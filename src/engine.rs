//! A RAM-only key-value engine over memtables and runs, with explicit
//! checkpoints and recovery.
//!
//! An [`Engine`] owns one active memtable, frozen memtables awaiting flush,
//! the published immutable runs, and the sequence counter:
//!
//! ```
//! use tessera::engine::{Engine, EngineOptions, EngineWorkspace};
//!
//! let engine = Engine::new(EngineOptions::default())?;
//! let mut workspace = EngineWorkspace::new();
//! engine.put(b"apple", b"red")?;
//! engine.flush()?;
//! engine.put(b"apricot", b"orange")?;
//!
//! assert_eq!(engine.get(b"apple", &mut workspace)?.unwrap().value, b"red");
//!
//! // A snapshot holds no lock, so scanning it delays no writer.
//! let snapshot = engine.snapshot()?;
//! let mut scan = snapshot.scan_prefix(b"ap", &mut workspace);
//! assert_eq!(scan.next_record()?.unwrap().key, b"apple");
//! assert_eq!(scan.next_record()?.unwrap().key, b"apricot");
//! assert_eq!(scan.next_record()?, None);
//! drop(scan);
//!
//! // Stops the background worker; a RAM-only engine's data goes with it.
//! engine.close()?;
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! ```
//!
//! # Consistency
//!
//! All state sits behind one reader-writer lock. A write holds it exclusively
//! while taking its sequence number and installing the update. Sequence order
//! is visibility order, and each successful write is visible before it returns.
//! A read holds the lock shared, so reads and writes of a key are linearizable.
//!
//! There are two ways to make several reads agree:
//!
//! - A [`ReadGuard`] holds the lock for as long as it lives, so everything
//!   read through it sees one state. **Writers and publication wait while a
//!   guard is held**, so use one only for a few reads. A waiting writer holds
//!   back new readers, so readers cannot starve it, and each write waits for
//!   at most the guards already held.
//! - A [`Snapshot`] holds no lock. See [Snapshots](self#snapshots).
//!
//! [`Engine::get`] holds the lock only to search the memtables and take
//! shared references to the published runs, which it then reads without the
//! lock, so decoding a block delays no writer. It sees every write that
//! returned before it began. A snapshot's reads see the state when it
//! was taken, which grows older as writes continue.
//!
//! Do not call a method of the [`Engine`] on a thread that holds a guard: a
//! write would deadlock, and so can a read once a writer is waiting. Use the
//! guard's own reads instead.
//!
//! Writer priority rests on one rule: a writer holds an internal gate while it
//! waits for the lock, and every reader takes that gate before the lock. Tests
//! check that a waiting writer holds the gate. They cannot check how the
//! operating system wakes the threads that queue behind it.
//!
//! # Panics
//!
//! A panic while the state lock is held for writing poisons it, and every later
//! operation then panics too. No write section is expected to panic short of a
//! bug, and the engine does not try to recover from one. A panic anywhere else,
//! including in a caller holding a [`ReadGuard`] or while a flush or
//! compaction builds without the lock, leaves the engine usable, and so does
//! a panic in the background worker, which stops it and is kept for
//! [`Engine::wait_for_compaction`] and [`Engine::close`]. No constructor
//! panics: a worker thread that cannot be created is [`OpenError::Worker`].
//!
//! # Snapshots
//!
//! [`Engine::snapshot`] takes, under the exclusive lock, a snapshot of the
//! active memtable, a shared reference to each frozen table, and one shared
//! reference to the list of published runs, so its cost does not grow with
//! the number of runs. The memtable is a
//! copy-on-write tree, so its snapshot shares every node, and later writes
//! copy only the nodes they change that a snapshot still shares. Nothing is
//! frozen or flushed for a snapshot, and taking one costs the same whatever
//! the size of the table.
//!
//! Every source a snapshot holds is immutable, so it reads without the lock,
//! in the same newest-first order as the engine, and sees exactly the writes
//! up to its [`last_seq`](Snapshot::last_seq), tombstones included, while
//! writes, flushes and compactions continue. The acquisition point is the
//! moment it holds the lock, between two writes.
//!
//! A snapshot keeps the sources it saw rather than asking the engine for
//! versions later, so flushes and compactions keep only the latest version of
//! each key: no reader of an older state ever reads their output. A flush or
//! compaction that removes a source from the engine leaves it to the
//! snapshots holding it, and it is freed with the last of them. A snapshot
//! taken with nothing written since the previous one shares that one's view.
//!
//! [`SnapshotLimits`] bound the live snapshots and the bytes kept only for
//! them; see [Memory](self#memory). A refused snapshot changes nothing.
//!
//! # Flushing
//!
//! A dedicated thread builds and publishes flushes. Writers only freeze the
//! active table and replace it under the lock, then signal that thread. Runs
//! are built without the state lock and replace their frozen tables atomically.
//! [`Engine::flush`] freezes everything written before the call and waits for
//! its tables to appear in runs. Flushes run one at a time.
//!
//! A flush's runs end at the run length and also at the smallest keys of
//! level 1's runs, taken in stage 1, so that each later level-0 job meets
//! fewer of them: a run ends there only if it holds an eighth of a run
//! length of input and the rest of the flush does too, and one run length
//! is cut into at most eight runs. Level 0 may hold any overlap, so cuts
//! that compaction has made stale by stage 3 cost nothing but fit; they
//! change how records are grouped, never which are written.
//!
//! Frozen tables stay readable throughout, and no reader sees their runs
//! until they replace them. Flushes run one at a time. A flush refused by the
//! run limits, or interrupted by a panic while building, leaves its tables
//! frozen, and the next flush merges them with the active table into one set
//! of runs.
//!
//! # Automatic flushing
//!
//! A writer rotates a full active table without building runs. It waits on a
//! condition variable when [`EngineOptions::max_frozen_memtables`] (default 2)
//! is reached, or waits for compaction at the level-0 stop. Readers and writes
//! that fit keep going during a flush build. A refused flush keeps its tables
//! readable; the next writer needing room and [`Engine::flush`] report the
//! existing [`UpdateError::FlushRefused`] / [`RunLimitError`]. Raising run limits
//! or successful compaction enables a retry. A worker panic is carried in the
//! refusal's [`RunLimitError::background`] and reported by [`Engine::close`].
//!
//! # Compaction
//!
//! [Background compaction](self#background-compaction) is on by default;
//! with [`EngineOptions::background_compaction`] set to `None`, compaction is
//! explicit. [`Engine::compact`] takes one step in three stages, like a
//! flush: inputs are selected under the lock, merged without any lock, and
//! replaced by the output in one step under the exclusive lock. Readers see
//! either all of the inputs or all of the output. Compactions run one at a
//! time; flushes, reads and writes continue beside them.
//!
//! Compaction is leveled. Every published run belongs to one level. Level 0
//! holds the runs of recent flushes in publication order, and they may
//! overlap; each deeper level is sorted by key and its runs are strictly
//! disjoint. Levels are compaction's bookkeeping only: they are kept in
//! memory, never persisted, and never consulted by reads, which go by
//! sequence number through the [run directory](self#reading-runs), so a run
//! returns the same answers whatever its level. Recovery puts a checkpoint's
//! runs, which are disjoint, in the last level. Each level below 0 is sized
//! using [`EngineOptions::level_ratio`] (10 by default), from the last level's
//! size, and empty levels are
//! added under level 0 as the data grows. One step is the first that applies:
//!
//! - **A level over its size**, the most over first: the run after the
//!   level's last job, in key order, merged with the runs of the next level
//!   it overlaps, found by binary search. Outputs are cut at the run length
//!   and where they would overlap more than `level_ratio` runs of the level below.
//! - **Level 0**, once it holds runs of
//!   [`CompactionOptions::max_overlap`] flushes and no deeper level is over
//!   its size: its oldest run, with the other level-0 runs that overlap the
//!   job while it fits, merged with the level-1 runs they overlap. However
//!   many flushes level 0 holds, a deeper level over its size goes first:
//!   level 0 waiting lets each of its jobs take more flushes at once, while
//!   a level 1 left over its size makes every one of them rewrite more.
//! - A run that overlaps nothing in the next level **moves** there: nothing
//!   is read or written.
//! - A job that does not fit the [`CompactionOptions`] limits **splits** its
//!   first run, rewriting it alone at the boundaries of the runs below it, if
//!   that leaves at least two pieces whose own jobs fit; the next job from
//!   that level takes the first piece. Otherwise the step is refused with
//!   [`CompactionError::JobLimit`], before anything is read.
//! - Two adjacent runs of one level, both under half a full run, are merged
//!   into one, if the merged range overlaps at most `level_ratio` full runs' bytes
//!   in the next level.
//!
//! A selected job builds up to [`EngineOptions::background_threads`] disjoint
//! key ranges in parallel, then publishes all output together. Job selection
//! and publication remain serialized. Parallel builders divide the segment
//! buffer allowance and share the configured workspace and output caps; small
//! jobs, neighbour merges and tight workspace budgets use fewer builders.
//!
//! Each step either lowers a level's excess, moves data down, or lowers the
//! run count, so repeated compaction without new flushes stops. A step reads
//! level sizes and key ranges only, never the runs' contents, and no step
//! compares every pair of runs.
//!
//! Only the newest version of each key is kept, which is correct because no
//! reader can ask for an older one. A tombstone is dropped only if no run
//! outside the inputs, in any level, overlaps its key and so could hold an
//! older value under it. Runs published after selection come from flushes
//! and are newer than every input, and the memtables are newer still, so
//! neither can be harmed by the replacement. Publication checks that every
//! input is still published where selection found it and that the output
//! fits its level, and a compaction that fails at any stage changes nothing.
//!
//! # Reading runs
//!
//! Every publication of runs, by a flush, a compaction or recovery, also
//! publishes a directory of them in the same step under the exclusive lock.
//! It partitions the runs into the fewest groups whose key ranges are
//! strictly disjoint, which is as many groups as the most runs whose
//! inclusive ranges share a key ([`EngineStats::max_overlap`]). The directory
//! is immutable: a lookup or scan reads one directory from start to finish,
//! and a snapshot keeps the one published when it was taken.
//!
//! A point lookup not decided by the memtables visits the groups in
//! descending order of the largest sequence number any of their runs holds.
//! In each it binary-searches for the one run whose range could hold the key,
//! and skips it without decoding anything if the key is outside its range,
//! its Bloom filter rules the key out, or it holds only versions older than
//! one already found. It stops once no group left holds a version as new.
//! When runs overlap heavily, groups are single runs and this is the order
//! of the runs' own sequence bounds.
//!
//! A scan merges one input per group: each reads its runs in key order
//! through one decoded block and one decompression context, after a binary
//! search for where to start. The memory a scan holds therefore grows with
//! the overlap, not with the number of runs.
//!
//! # Background compaction
//!
//! With [`EngineOptions::background_compaction`] set, the engine owns one
//! worker thread. Each flush publication and each change to the run limits
//! asks it for a pass; requests made while it is busy are counted as one, so
//! nothing queues. A pass runs [`Engine::compact`] with those options until
//! nothing is eligible, so the selection, limits and publication checks are
//! the synchronous ones, and it competes with manual compactions only for
//! the same one-at-a-time slot. The worker sleeps on a condition variable
//! between passes.
//!
//! A failed compaction ends its pass. It is recorded in
//! [`Engine::background_status`] and not retried until the next request, so
//! a lasting failure is reported once per flush, never in a loop. A flush
//! refused by the run limits also requests a pass, so a writer that retries
//! flushes recovers once compaction can run again, for example after the
//! snapshots that blocked it are dropped.
//! [`Engine::wait_for_compaction`] requests a pass, waits for it and returns
//! its failure. A compaction refused this way is not terminal: the next
//! request tries again. A panic in the worker stops it for good and is
//! reported as [`BackgroundError::Panicked`] with its message; the engine
//! stays usable without it.
//!
//! # Lifecycle
//!
//! An engine is open from the moment a constructor returns it until
//! [`Engine::close`] or drop, which do the same: tell the worker to stop,
//! wait for the compaction it is running, which is published at its usual
//! step (within [`CompactionOptions::max_input_bytes`]), start no other, join
//! the thread and free the engine. Both consume the
//! engine, so no other call can be in progress or follow; `close` also
//! returns a worker panic, where a drop discards it. Closing holds no lock
//! while it waits, so the worker can always finish.
//!
//! The worker thread holds the engine's shared core, not the engine, so
//! nothing keeps the engine alive, and nothing else holds the core.
//! [`Snapshot`]s own what they read and stay readable after the engine is
//! gone; a [`ReadGuard`] borrows the engine, so it ends first.
//!
//! Closing writes nothing: no flush or checkpoint. Data is lost unless
//! checkpointed first.
//!
//! # Configuration
//!
//! [`Engine::new`] and [`Engine::recover`] check every option before they
//! allocate or start the worker, returning [`OpenError::Config`] on invalid
//! options. [`Engine::compact`] and [`Engine::set_run_limits`] check theirs
//! the same way. Run limits refuse flushes ([`UpdateError::FlushRefused`]);
//! snapshot limits refuse snapshots and compactions; compaction limits
//! refuse a merge ([`CompactionError::JobLimit`] and its neighbours).
//!
//! Compression and block length choose how new runs are built, nothing
//! else. Every run records its own codec, block lengths and dictionary, so
//! runs and checkpoints written under other settings are read and recovered
//! as they are, and are rebuilt with the current settings only when a
//! compaction or checkpoint rewrites their records.
//!
//! # Memory
//!
//! Each memtable has [`EngineOptions::memtable_budget`] charged bytes, including
//! snapshot-pinned history. The active table and up to
//! [`EngineOptions::max_frozen_memtables`] frozen tables may coexist. An atomic
//! batch exceeding one budget uses the ingestion path (unsorted input is
//! staged in a private table sized for that batch).
//! Long-lived snapshots make rotations come sooner.
//!
//! Published runs are limited by a byte total, [`EngineOptions::max_run_bytes`],
//! the physical limit on what the engine holds, and by a count,
//! [`EngineOptions::max_runs`], a safety valve on their metadata. With a
//! background worker, a flush is also refused while level 0 holds runs of
//! 12 flushes (the level-0 stop), so that lookups stay bounded when the
//! worker falls behind; without one, the application compacts when it
//! chooses and only the run limits bound level 0. Once a flush
//! would pass any of these it is rejected, and writes needing room fail with
//! [`UpdateError::FlushRefused`]. Compaction lifts the level-0 stop by
//! itself, and the byte limit when the runs hold overwritten or deleted
//! versions it can drop; a database of live data at the byte limit needs
//! [`Engine::set_run_limits`] (deletes are writes, and are refused too).
//!
//! A compaction adds, until it publishes, its output and workspace, each
//! limited by [`CompactionOptions`]. Only one compaction builds at a time,
//! whether in the background or not. Its inputs stay published until it
//! publishes, and are freed as it releases the lock: a reader holding the lock
//! delays publication rather than keeping old runs. An [`Engine::get`] in
//! progress keeps the runs it is reading until it returns, a few blocks'
//! decoding, and until then they count as retained. Run bytes can therefore
//! briefly be the published total plus one output, beside two memtable
//! budgets and one workspace. A compaction is refused only if it would leave
//! more runs or bytes than both the limits and what is published now: its
//! growth is measured on its built output, so a compaction that shrinks
//! what is published, the one that reclaims space, is never blocked by the
//! limit that refuses a flush.
//!
//! When flushes outpace compaction, level 0 reaches its stop. A flush, or a
//! write needing room, then waits for the background worker before building
//! anything: it asks for a pass and sleeps, holding no lock that reads,
//! fitting writes or compaction need, until a step the worker publishes
//! leaves level 0 room, the pass ends, or the worker stops. It waits for
//! that one pass at most. A pass ends only when nothing is due or a step is
//! refused, and level 0 at its stop is always due, so a pass that leaves no
//! room was refused, and waiting longer would not help:
//!
//! - **A backlog**: the worker drains level 0 and the flush goes on. This
//!   is the only case that waits, and it is the common one.
//! - **Snapshots retaining too much**: merges are refused
//!   ([`CompactionError::Retained`]) while moves go on. The flush is refused
//!   with [`UpdateError::FlushRefused`] at the stop; once the snapshots are
//!   dropped, the next write needing room waits for a pass that succeeds.
//! - **A job over the [`CompactionOptions`] limits**
//!   ([`CompactionError::JobLimit`], or [`CompactionError::OutputLimit`]
//!   when an output outgrows its estimate, as a change of codec can make
//!   it): refused the same way, and again on every retry until the limits
//!   are raised. Each refused write asks for one pass, never more.
//! - **The byte or run limit** ([`EngineOptions::max_run_bytes`],
//!   [`EngineOptions::max_runs`]): refused at once, without waiting, and the
//!   worker is asked for a pass in case compaction can reclaim space.
//! - **A stopped worker** (it panicked): nothing waits; flushes at the stop
//!   are refused until [`Engine::compact`] drains level 0.
//!
//! A refusal at the stop carries why the pass it waited for made no room,
//! in [`RunLimitError::background`]: the pass's failure, which says whether
//! anything but a change of snapshots or limits will lift it, or the
//! worker's panic. [`Engine::background_status`] reports the latest pass,
//! which may be a later one. Without a worker there is no stop and nothing
//! waits. Flushes themselves still run
//! on the writer that needs room, so a write that fills the memtable stalls
//! for the flush, and for level 0 as above.
//!
//! Snapshots keep memory in two ways, both reported as
//! [`EngineStats::retained_bytes`]: the history pinned in the engine's
//! memtables, within their budgets, and the memtables and runs the engine
//! has released but snapshots still hold. A released memtable is charged in
//! full while a snapshot holds it; one that only snapshots of the earlier,
//! still writable table share is charged the newest such snapshot's charge
//! plus the history pinned since the oldest, fixed when it is released and
//! dropped once none lives. Each live snapshot holds at most the
//! state it saw, so the total depends on what is flushed and compacted while
//! it lives. While it exceeds [`SnapshotLimits::max_retained_bytes`], new
//! snapshots are refused and so are compactions, whose output would be held
//! beside inputs that snapshots keep (a move, which retires nothing, still
//! goes on). That is the backpressure: runs then accumulate in level 0
//! until its stop refuses flushes and so writes, until snapshots are
//! dropped. Each reader's [`EngineWorkspace`] adds two decoded blocks for
//! point lookups and, for scans, one per group of key-disjoint runs it has
//! read (see [Reading runs](self#reading-runs)).
//!
//! The run directory and the levels are not charged against any limit. The
//! directory holds a pointer per run and a few words per group, the levels a
//! pointer per run and a key per level, and a snapshot or a lookup in
//! progress keeps the directory it read, with the runs in it, until it is
//! dropped. [`EngineOptions::max_runs`] bounds both.
//!
//! These limits use the memtable's estimated charges, stored run lengths and
//! an estimate of workspace. They are not a limit on process memory.
//!
//! # Checkpoints
//!
//! [`Engine::checkpoint`] writes the state at one snapshot, its **ceiling**,
//! to a directory while writes continue, and [`Engine::recover`] restores the
//! committed checkpoint into a new RAM-only engine. A checkpoint holds every
//! key live at the ceiling with its newest value and nothing written after
//! it. A RAM-only engine therefore loses every write since its latest
//! checkpoint when it is closed or dropped or its process ends. No
//! checkpoint is ever taken automatically.
//!
//! A checkpoint is committed when the directory's `CURRENT` file is renamed
//! to name it, after its runs and manifest are synced; the previous one
//! stays committed until then and is removed afterwards. A failed or
//! interrupted checkpoint leaves the previous one intact. Recovery checks
//! every file of exactly the committed checkpoint, with no fallback to an
//! older one, before it returns an engine. One engine or process uses a
//! directory at a time, through an advisory lock on its `LOCK` file, which
//! the kernel releases if the process dies.

mod batch;
mod checkpoint;
mod directory;
mod flusher;
mod ingest;
mod levels;
mod multi;
mod worker;

#[cfg(test)]
mod capacity_tests;
#[cfg(test)]
mod leveled_tests;
#[cfg(test)]
mod leveling_tests;
#[cfg(test)]
mod staged_tests;

pub use batch::WriteBatch;
pub use checkpoint::{CheckpointError, CheckpointOptions, CheckpointReport, Damage, OpenError};
pub use worker::{BackgroundError, BackgroundStatus};

pub use crate::block::{Corruption as BlockCorruption, MAX_KEY_LEN, MAX_VALUE_LEN};
pub use crate::memtable::{ENTRY_OVERHEAD, MemoryUsage};
use crate::run::CompactError;
pub use crate::run::{
    CompactStats, ConflictingCopies, CorruptRun, LiveRecord, LiveValue, MergeWorkspace, ReadError,
    RunCorruption,
};

use std::fmt;
use std::sync::{
    Arc, Mutex, MutexGuard, PoisonError, RwLock, RwLockReadGuard, RwLockWriteGuard, Weak,
};

use crate::block::{Record, Value, Version};
use crate::memtable::{
    FlushCuts, FrozenMemtable, Memtable, Records, TARGET_RUN_LEN, TableError, flush_merged,
};
use crate::run::{
    Codec, CompactLimits, LogicalCursor, LogicalWorkspace, MAX_TARGET_RUN_LEN, OutputCuts,
    RAW_BLOCK_CAP, RECORD_INPUT_OVERHEAD, Run, RunView, TARGET_BLOCK_COMPRESSED, compact_parallel,
    latest_from,
};

use directory::RunDirectory;
use levels::{Dest, Fit, LEVEL0_STOP, Levels, Plan};

type SharedRun = Arc<Published>;

/// A published run.
#[derive(Debug)]
struct Published {
    run: Run<Vec<u8>>,
    /// The decoded length of its largest block, for sizing a compaction's
    /// workspace before reading anything.
    largest_block: usize,
}

/// Wraps the runs of one flush or compaction, in key order, for publication.
fn published(runs: Vec<Run<Vec<u8>>>) -> impl Iterator<Item = SharedRun> {
    runs.into_iter().map(|run| {
        // A run that cannot report its blocks is damaged, which the merge
        // that reads it reports; until then it is sized as one empty block.
        let largest_block = run.view().largest_block_len().unwrap_or(0);
        Arc::new(Published { run, largest_block })
    })
}

/// The configuration of an [`Engine`], fixed when it starts except for the
/// run limits, which [`Engine::set_run_limits`] replaces.
///
/// Every limit is an admission check on accounted bytes or counts: none
/// reserves memory or disk up front, and none bounds the process's resident
/// memory, which also holds snapshots, workspaces and allocator overhead. See
/// [Memory](self#memory).
#[derive(Clone, Copy, Debug)]
pub struct EngineOptions {
    /// Charged bytes each memtable may hold, from [`ENTRY_OVERHEAD`] (the
    /// charge of an empty record) to half of `usize::MAX`. The active table
    /// and each frozen table have this budget. A record charged more
    /// is refused with [`WriteError::RecordExceedsBudget`].
    pub memtable_budget: usize,
    /// Maximum frozen tables awaiting background flush, at least one (default 2).
    pub max_frozen_memtables: usize,
    /// Minimum charged input bytes for automatic sorted ingestion. `None`
    /// selects half the memtable budget; `Some(0)` ingests any sorted batch.
    pub bulk_ingest_min: Option<usize>,
    /// Bytes of memtable records per run a flush writes, so that a large
    /// flush writes several runs with disjoint key ranges: from 1 to 1 GiB.
    /// A run holds at least one record.
    pub target_run_len: usize,
    /// The most runs that may be published, at least 1: a safety valve on
    /// the metadata runs need (a few hundred bytes each, not charged
    /// against `max_run_bytes`), not a capacity limit. Leveled compaction
    /// keeps runs at about the target length, so the run count grows with
    /// the data and `max_run_bytes` decides how much fits. A flush that
    /// would exceed it is refused, and the writes waiting for it with
    /// [`UpdateError::FlushRefused`], until compaction merges small runs or
    /// the limit is raised. The default, 65,536, is the default
    /// `max_run_bytes` over 64 KiB; scale it with `max_run_bytes`.
    pub max_runs: usize,
    /// The most bytes of published runs, at least 1: their stored bytes, key
    /// filters and segment metadata together. This is the physical
    /// limit on how much the engine holds. Exceeding it refuses flushes as
    /// for `max_runs`; a compaction may exceed it only by publishing less
    /// than it replaces.
    pub max_run_bytes: usize,
    /// The sequence number a new engine continues after: the first write
    /// uses the next. [`Engine::recover`] continues after the larger of it and the
    /// checkpoint's ceiling.
    pub last_seq: u64,
    /// Starts a background worker that compacts with these options whenever
    /// a flush publishes runs; on by default with
    /// [`CompactionOptions::default`]. `None` runs no worker, and then the
    /// application must call [`Engine::compact`], or runs accumulate until
    /// flushes, and the writes that need them, are refused. See
    /// [Background compaction](self#background-compaction).
    pub background_compaction: Option<CompactionOptions>,
    /// Maximum parallel builders within one compaction job, at least 1.
    /// Defaults to half the available CPUs, capped at 4 and at least 1.
    /// Small jobs and tight workspace budgets use fewer builders.
    pub background_threads: usize,
    /// Size multiplier between adjacent levels, at least 2 (default 10).
    /// Dynamic level targets and the bounds on overlap use this ratio.
    pub level_ratio: usize,
    /// Limits on [`Engine::snapshot`]. Zero is valid for both and refuses
    /// every snapshot (`max_live`) or every snapshot and compaction while a
    /// snapshot retains anything (`max_retained_bytes`).
    pub snapshots: SnapshotLimits,
    /// How the runs this engine builds store their blocks. See
    /// [`Compression`].
    pub compression: Compression,
    /// Estimated compressed bytes per Zstandard block, 1024 by default, from
    /// 1 to 256 KiB. Each segment estimates its ratio from a 64 KiB sample.
    /// Blocks end at record boundaries, with a 256 KiB raw cap; a larger
    /// record has its own block. Uncompressed runs use a fixed 16 KiB target.
    pub target_block_compressed: usize,
}

impl Default for EngineOptions {
    /// 64 MiB memtables, 4 GiB of runs (and at most 65,536 of them),
    /// background compaction
    /// with [`CompactionOptions::default`], the default snapshot limits, and
    /// Zstandard level 1 with segment dictionaries and a 1 KiB compressed target.
    fn default() -> Self {
        Self {
            memtable_budget: 64 << 20,
            max_frozen_memtables: 2,
            bulk_ingest_min: None,
            target_run_len: TARGET_RUN_LEN,
            max_runs: DEFAULT_MAX_RUNS,
            max_run_bytes: DEFAULT_MAX_RUN_BYTES,
            last_seq: 0,
            background_compaction: Some(CompactionOptions::default()),
            level_ratio: 10,
            background_threads: std::thread::available_parallelism()
                .map_or(1, |cpus| (cpus.get() / 2).clamp(1, 4)),
            snapshots: SnapshotLimits::default(),
            compression: Compression::default(),
            target_block_compressed: TARGET_BLOCK_COMPRESSED,
        }
    }
}

/// The default [`EngineOptions::max_run_bytes`].
const DEFAULT_MAX_RUN_BYTES: usize = 4 << 30;
/// The default [`EngineOptions::max_runs`]: one run per 64 KiB of the
/// default `max_run_bytes`.
const DEFAULT_MAX_RUNS: usize = DEFAULT_MAX_RUN_BYTES / (64 << 10);

impl EngineOptions {
    /// The one check of these options, made before an engine starts a
    /// worker, allocates or touches a file. See [`ConfigError`].
    fn validate(&self) -> Result<(), ConfigError> {
        ConfigError::check(
            "EngineOptions::memtable_budget",
            self.memtable_budget as u64,
            ENTRY_OVERHEAD as u64,
            (usize::MAX / 2) as u64,
            "it must admit the smallest record, and twice it must not overflow",
        )?;
        ConfigError::check(
            "EngineOptions::target_run_len",
            self.target_run_len as u64,
            1,
            MAX_TARGET_RUN_LEN as u64,
            "runs are at most 1 GiB",
        )?;
        ConfigError::check(
            "EngineOptions::max_frozen_memtables",
            self.max_frozen_memtables as u64,
            1,
            usize::MAX as u64,
            "at least one frozen table must be allowed",
        )?;
        ConfigError::check(
            "EngineOptions::background_threads",
            self.background_threads as u64,
            1,
            usize::MAX as u64,
            "at least one builder is required",
        )?;
        ConfigError::check(
            "EngineOptions::level_ratio",
            self.level_ratio as u64,
            2,
            usize::MAX as u64,
            "level sizes must grow by at least two",
        )?;
        ConfigError::check_run_limits(self.max_runs, self.max_run_bytes)?;
        ConfigError::check(
            "EngineOptions::target_block_compressed",
            self.target_block_compressed as u64,
            1,
            RAW_BLOCK_CAP as u64,
            "compressed targets are from 1 byte to the 256 KiB raw cap",
        )?;
        if let Some(compaction) = &self.background_compaction {
            compaction.validate(self.target_run_len, self.target_block_compressed)?;
        }
        Ok(())
    }
}

/// Why options were refused: a setting outside the range the engine
/// supports, or one that contradicts another. Nothing was started,
/// allocated or written.
///
/// Every setting is checked, and none is adjusted: a value is accepted as
/// given or refused here. Settings that are valid can still meet runtime
/// limits, which are reported by the operation they stop, such as
/// [`UpdateError::FlushRefused`] or [`CompactionError::JobLimit`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct ConfigError {
    /// The setting, as `Type::field`.
    pub setting: &'static str,
    /// Its value.
    pub value: u64,
    /// The least value accepted, given the other settings.
    pub min: u64,
    /// The greatest value accepted.
    pub max: u64,
    /// Why the range is what it is.
    pub reason: &'static str,
}

impl ConfigError {
    /// Refuses `value` outside `min..=max`. Callers widen `usize` settings,
    /// which fit in `u64` on every target.
    fn check(
        setting: &'static str,
        value: u64,
        min: u64,
        max: u64,
        reason: &'static str,
    ) -> Result<(), Self> {
        if (min..=max).contains(&value) {
            return Ok(());
        }
        Err(Self {
            setting,
            value,
            min,
            max,
            reason,
        })
    }

    /// [`EngineOptions::max_runs`] and [`EngineOptions::max_run_bytes`], as
    /// given or as [`Engine::set_run_limits`] replaces them.
    fn check_run_limits(max_runs: usize, max_run_bytes: usize) -> Result<(), Self> {
        Self::check(
            "EngineOptions::max_runs",
            max_runs as u64,
            1,
            usize::MAX as u64,
            "with no runs allowed, no flush could publish",
        )?;
        Self::check(
            "EngineOptions::max_run_bytes",
            max_run_bytes as u64,
            1,
            usize::MAX as u64,
            "with no run bytes allowed, no flush could publish",
        )
    }
}

impl fmt::Display for ConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let Self {
            setting,
            value,
            min,
            max,
            reason,
        } = self;
        if *max == u64::MAX {
            write!(
                f,
                "{setting} is {value} but must be at least {min}: {reason}"
            )
        } else {
            write!(
                f,
                "{setting} is {value} but must be from {min} to {max}: {reason}"
            )
        }
    }
}

impl std::error::Error for ConfigError {}

/// How runs store their blocks. Zstandard uses a 16 KiB raw-content dictionary
/// per roughly 4 MiB segment, with independently decoded adaptive blocks.
/// Every decode loads the dictionary by reference into the reader's context.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum Compression {
    /// Zstandard level 1, with segment dictionaries and adaptive blocks.
    #[default]
    Zstd,
    /// Fixed 16 KiB raw blocks, read in place.
    Uncompressed,
}
impl Compression {
    fn codec(self) -> Codec {
        match self {
            Self::Zstd => Codec::Zstd,
            Self::Uncompressed => Codec::None,
        }
    }
}

/// Admission limits for [`Engine::snapshot`]. A snapshot that would exceed
/// one is refused with a [`SnapshotError`]; none is ever weakened.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SnapshotLimits {
    /// The most snapshots alive at once, including any that outlived the
    /// engine.
    pub max_live: usize,
    /// No snapshot is taken, and no compaction, while snapshots keep more
    /// than this many bytes: memtable history they pin, and memtables and
    /// runs the engine has released. See [Memory](self#memory).
    pub max_retained_bytes: usize,
}

impl Default for SnapshotLimits {
    /// 16 live snapshots and 1 GiB retained.
    fn default() -> Self {
        Self {
            max_live: 16,
            max_retained_bytes: 1 << 30,
        }
    }
}

/// Why [`Engine::snapshot`] refused. Nothing was changed.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SnapshotError {
    /// [`SnapshotLimits::max_live`] snapshots are alive. Drop one.
    Readers {
        /// Snapshots alive.
        live: usize,
        /// The limit.
        max: usize,
    },
    /// Snapshots retain more than [`SnapshotLimits::max_retained_bytes`].
    /// Dropping old snapshots frees it.
    Retained {
        /// Bytes retained.
        bytes: usize,
        /// The limit.
        max: usize,
    },
}

impl fmt::Display for SnapshotError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match *self {
            Self::Readers { live, max } => {
                write!(f, "{live} snapshots are alive, the limit is {max}")
            }
            Self::Retained { bytes, max } => {
                write!(f, "snapshots retain {bytes} bytes, over the limit of {max}")
            }
        }
    }
}

impl std::error::Error for SnapshotError {}

/// Why a write can never be applied with the engine's current options. A
/// write that only needs room flushes instead; see
/// [Automatic flushing](self#automatic-flushing).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WriteError {
    /// Sorted ingestion requires strictly ascending keys without duplicates.
    BatchNotSorted,
    /// The key exceeds [`MAX_KEY_LEN`].
    KeyTooLong {
        /// Length of the rejected key.
        len: usize,
    },
    /// The value exceeds [`MAX_VALUE_LEN`].
    ValueTooLong {
        /// Length of the rejected value.
        len: usize,
    },
    /// The record alone would be charged more than a whole memtable budget
    /// ([`EngineOptions::memtable_budget`]), so no memtable can hold it.
    RecordExceedsBudget {
        /// What the record would be charged: its key and value and
        /// [`ENTRY_OVERHEAD`].
        charge: usize,
        /// The budget.
        budget: usize,
    },
}

impl fmt::Display for WriteError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match *self {
            Self::BatchNotSorted => f.write_str("ingestion requires strictly sorted, unique keys"),
            Self::KeyTooLong { len } => {
                write!(f, "key of {len} bytes exceeds the limit of {MAX_KEY_LEN}")
            }
            Self::ValueTooLong { len } => {
                write!(
                    f,
                    "value of {len} bytes exceeds the limit of {MAX_VALUE_LEN}"
                )
            }
            Self::RecordExceedsBudget { charge, budget } => write!(
                f,
                "record charged {charge} bytes exceeds the memtable budget of {budget}"
            ),
        }
    }
}

impl std::error::Error for WriteError {}

/// Why a write was refused. It was not applied, used no sequence number,
/// and changed no reads. A flush it started may have frozen the active table.
/// A rejected write cannot succeed unchanged; a refused flush can be retried
/// after maintenance; exhausted sequence numbers prevent further writes.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum UpdateError {
    /// The write can never be applied: its key or value is too long, or its
    /// record exceeds a whole memtable budget.
    Rejected(WriteError),
    /// The memtables are full and the flush that would make room is refused
    /// by the run limits, now or since they or the runs last changed.
    /// Compaction or [`Engine::set_run_limits`] can make room, after which
    /// the write can be retried.
    FlushRefused(RunLimitError),
    /// Every sequence number has been used, which only an engine started
    /// near `u64::MAX` ([`EngineOptions::last_seq`]) can reach.
    SequencesExhausted,
}

impl fmt::Display for UpdateError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Rejected(error) => error.fmt(f),
            Self::FlushRefused(error) => write!(
                f,
                "the memtables are full and {error}; compaction can make room"
            ),
            Self::SequencesExhausted => f.write_str("sequence numbers are exhausted"),
        }
    }
}

impl std::error::Error for UpdateError {}

/// Publishing would exceed the run limits ([`EngineOptions::max_runs`] and
/// [`EngineOptions::max_run_bytes`]), or a flush would leave level 0 with
/// more flushes than the background worker lets it hold (the level-0 stop,
/// 12; unlimited without a worker). A
/// refused flush keeps its frozen table readable, and a later flush retries
/// it; a refused compaction publishes nothing.
///
/// Compaction lifts a refusal at the level-0 stop by itself, and one at
/// `max_run_bytes` when it can reclaim overwritten or deleted versions; a
/// database of live data at `max_run_bytes` needs
/// [`Engine::set_run_limits`]. `max_runs` is a safety valve on run metadata,
/// not a capacity limit: see [`EngineOptions::max_runs`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RunLimitError {
    /// Runs that would be published.
    pub runs: usize,
    /// The run count limit.
    pub max_runs: usize,
    /// Stored, filter and segment metadata bytes of the runs that would be published.
    pub run_bytes: usize,
    /// The limit on those bytes.
    pub max_run_bytes: usize,
    /// Flushes level 0 would hold.
    pub level0_flushes: usize,
    /// The level-0 stop: 12 with a background worker, `usize::MAX`
    /// without.
    pub max_level0_flushes: usize,
    /// At the level-0 stop, why the worker could not make room while the
    /// flush waited for it: the failure that ended the pass it waited for
    /// ([`BackgroundError::Failed`]), which retrying meets again until what
    /// it names changes (snapshots dropped, limits raised), or the worker's
    /// panic ([`BackgroundError::Panicked`]), after which only
    /// [`Engine::compact`] makes room. Also carries a flush-worker panic;
    /// its frozen tables remain readable, and that worker is not restarted.
    pub background: Option<Box<BackgroundError>>,
}

impl RunLimitError {
    /// Whether only the level-0 stop refused, which the worker lifts by
    /// itself unless its compactions are refused.
    fn level0_only(&self) -> bool {
        self.level0_flushes > self.max_level0_flushes
            && self.runs <= self.max_runs
            && self.run_bytes <= self.max_run_bytes
    }
}

impl fmt::Display for RunLimitError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.level0_flushes > self.max_level0_flushes {
            write!(
                f,
                "publishing would leave {} flushes in level 0, over the stop of {}",
                self.level0_flushes, self.max_level0_flushes
            )?;
            return match &self.background {
                Some(error) => write!(f, ", and {error}"),
                None => f.write_str("; compaction drains it"),
            };
        }
        if self.runs <= self.max_runs
            && self.run_bytes <= self.max_run_bytes
            && let Some(error) = &self.background
        {
            return write!(f, "background flush failed: {error}");
        }
        write!(
            f,
            "publishing would leave {} runs of {} bytes, over the limits of {} runs and {} bytes",
            self.runs, self.run_bytes, self.max_runs, self.max_run_bytes
        )
    }
}

impl std::error::Error for RunLimitError {}

/// What a flush published.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct FlushReport {
    /// Memtables flushed.
    pub tables: usize,
    /// Records written, including tombstones.
    pub records: usize,
    /// Runs published.
    pub runs: usize,
    /// Stored bytes published.
    pub run_bytes: usize,
    /// Key filters and physical segment metadata held beside stored bytes.
    /// Segment dictionaries are included in `run_bytes`.
    pub held_bytes: usize,
    /// High-water bytes one flush run's filter construction held for key hashes,
    /// freed when that run finished. Aggregated flush reports retain the
    /// engine's high-water mark; an empty report returns zero.
    pub filter_build_bytes: usize,
}

/// A summary of an engine's state.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct EngineStats {
    /// The last sequence number used.
    pub last_seq: u64,
    /// The active memtable's accounting.
    pub active: MemoryUsage,
    /// The frozen memtables' accounting, summed, if any await flush.
    pub frozen: Option<MemoryUsage>,
    /// Frozen memtables awaiting flush.
    pub frozen_tables: usize,
    /// Published runs.
    pub runs: usize,
    /// Stored bytes of the published runs.
    pub run_bytes: usize,
    /// Key filters and physical segment metadata beside `run_bytes`. Dictionaries are
    /// stored inside the runs. [`EngineOptions::max_run_bytes`] limits both.
    pub held_bytes: usize,
    /// The most published runs whose key ranges share one key: the most
    /// blocks a point read of a key absent from the memtables decodes, and
    /// the number of groups in the run directory, each one input of a scan.
    pub max_overlap: usize,
    /// Live snapshots.
    pub snapshots: usize,
    /// Memtables and runs the engine has released that snapshots still hold,
    /// or that an [`Engine::get`] in progress is still reading.
    pub retained_sources: usize,
    /// Bytes kept only for snapshots: the charged or stored bytes of those
    /// sources with any runs' filters and segment metadata, and history pinned in the engine's
    /// memtables, which `active` and `frozen` also report as
    /// [`MemoryUsage::pinned`].
    pub retained_bytes: usize,
    /// The part of `retained_bytes` that is runs: stored bytes of runs that
    /// compaction replaced and snapshots still hold.
    pub retained_run_bytes: usize,
    /// The part of `retained_bytes` that is runs' filters and segment metadata.
    pub retained_held_bytes: usize,
    /// Flushes whose runs level 0 holds: at most 12, the level-0 stop, with
    /// a background worker.
    pub level0_flushes: usize,
    /// Runs in level 0.
    pub level0_runs: usize,
    /// Levels below level 0, including empty ones, which compaction adds
    /// as the data grows.
    pub levels: usize,
    /// Flushes published since the engine started, whether a write or the
    /// caller started them.
    pub flushes: u64,
    /// Stored bytes those flushes published.
    pub flushed_bytes: u64,
}

struct State {
    last_seq: u64,
    active: Memtable,
    /// Oldest first. Each table's sequence numbers are above the runs' and
    /// those of the tables before it, and below `active`'s. Only flushes
    /// freeze tables, so there is more than one only after a flush was
    /// refused or panicked.
    frozen: Vec<Arc<FrozenMemtable>>,
    /// Shared so that a compaction can read its inputs without the lock while
    /// flushes publish beside them.
    runs: Vec<SharedRun>,
    /// The same runs, indexed for reading: grouped into key-disjoint runs,
    /// newest group first. Rebuilt by [`State::order_runs`] in the same write
    /// section as every change to `runs`, so readers only ever see the two
    /// agree; `runs` keeps publication order, which compaction selection uses
    /// to break ties. Immutable once built, so a lookup, a scan or a snapshot
    /// shares it with one reference count.
    directory: Arc<RunDirectory>,
    run_bytes: usize,
    held_bytes: usize,
    /// The same runs as compaction sees them, in levels. Changed in the same
    /// write section as `runs`, and never read by lookups or scans.
    levels: Levels,
    /// Level 0 flushes beyond which a flush is refused: [`LEVEL0_STOP`]
    /// with a background worker, otherwise unlimited; tests may change it.
    level0_stop: usize,
    /// Whether flushes are split at level 1's boundaries; tests turn it off
    /// to compare.
    #[cfg(test)]
    split_flushes: bool,
    max_runs: usize,
    max_run_bytes: usize,
    /// Sources removed from the state, kept track of until no snapshot holds
    /// them. Pruned whenever a source is added.
    retired: Vec<Retired>,
    /// The latest snapshot of the active table and the `last_seq` it was
    /// taken at. Every write raises `last_seq`, so while it is unchanged the
    /// snapshot still equals the active table and can be shared.
    last_view: Option<(u64, Weak<FrozenMemtable>)>,
    /// The last flush's refusal, until a flush or compaction publishes or
    /// the run limits change: the same flush would be refused again, so a
    /// write needing room does not build it.
    refused: Option<RunLimitError>,
    /// Why the last wait at the level-0 stop ended without room, until
    /// level 0 has room: the cause a refusal at the stop reports.
    blocked: Option<BackgroundError>,
    /// Flushes published, and their stored bytes.
    flushes: u64,
    flushed_bytes: u64,
    flush_totals: FlushReport,
}

/// A source the engine released, with the bytes it accounts for.
struct Retired {
    source: Weak<dyn Send + Sync>,
    bytes: usize,
    /// For a run, key filter and physical segment metadata bytes.
    held_bytes: Option<usize>,
}

/// What snapshots alone keep.
#[derive(Clone, Copy, Default)]
struct Retained {
    sources: usize,
    bytes: usize,
    run_bytes: usize,
    held_bytes: usize,
}

impl State {
    /// Rebuilds the directory from `runs`. Publication is rare, so this
    /// builds a fresh one rather than maintaining one, in O(n log n) for n
    /// runs, and readers holding the previous directory keep it.
    fn order_runs(&mut self) {
        self.directory = Arc::new(RunDirectory::build(&self.runs));
        if cfg!(any(test, debug_assertions)) {
            self.levels.check(&self.runs);
        }
    }

    /// Where a flush beginning now splits its runs.
    fn flush_cuts(&self, target_run_len: usize) -> FlushCuts {
        #[cfg(test)]
        if !self.split_flushes {
            return FlushCuts::default();
        }
        self.levels.flush_cuts(target_run_len)
    }

    /// The refusal of anything that would leave `runs` runs of `bytes`
    /// bytes and `flushes` level-0 flushes.
    fn run_limit(&self, runs: usize, bytes: usize, flushes: usize) -> RunLimitError {
        RunLimitError {
            runs,
            max_runs: self.max_runs,
            run_bytes: bytes,
            max_run_bytes: self.max_run_bytes,
            level0_flushes: flushes,
            max_level0_flushes: self.level0_stop,
            background: (flushes > self.level0_stop)
                .then(|| self.blocked.clone().map(Box::new))
                .flatten(),
        }
    }

    /// `held_bytes` is `Some` for a run.
    fn retire(&mut self, source: Weak<dyn Send + Sync>, bytes: usize, held_bytes: Option<usize>) {
        self.retired
            .retain(|retired| retired.source.strong_count() > 0);
        if source.strong_count() > 0 {
            self.retired.push(Retired {
                source,
                bytes,
                held_bytes,
            });
        }
    }

    /// Released sources still held, each counted once however many snapshots
    /// hold it, and the history pinned in the engine's memtables.
    fn retained(&self) -> Retained {
        let pinned = self.active.memory().pinned
            + self
                .frozen
                .iter()
                .map(|table| table.memory().pinned)
                .sum::<usize>();
        self.retired
            .iter()
            .filter(|retired| retired.source.strong_count() > 0)
            .fold(
                Retained {
                    sources: 0,
                    bytes: pinned,
                    run_bytes: 0,
                    held_bytes: 0,
                },
                |sum, retired| {
                    let filter = retired.held_bytes.unwrap_or(0);
                    let run = if retired.held_bytes.is_some() {
                        retired.bytes
                    } else {
                        0
                    };
                    Retained {
                        sources: sum.sources + 1,
                        bytes: sum.bytes + retired.bytes + filter,
                        run_bytes: sum.run_bytes + run,
                        held_bytes: sum.held_bytes + filter,
                    }
                },
            )
    }
}

/// Everything an engine shares with its background worker.
struct Core {
    state: RwLock<State>,
    /// Gives writers priority. A writer holds it while waiting for the state
    /// lock, and a reader passes through it first, so new readers cannot keep
    /// overtaking a waiting writer. The standard lock does not promise this.
    gate: Mutex<()>,
    /// Held by a flush from start to end, so that flushes run one at a time.
    flushing: Mutex<()>,
    /// Held by a compaction from selection to publication, so that runs are
    /// removed by one compaction at a time.
    compacting: Mutex<()>,
    background_threads: usize,
    level_ratio: usize,
    /// Held by a checkpoint, so that checkpoints run one at a time.
    checkpointing: Mutex<()>,
    memtable_budget: usize,
    max_frozen_memtables: usize,
    flush_signal: flusher::Signal,
    bulk_ingest_min: usize,
    target_run_len: usize,
    /// The codec of the runs this engine builds.
    codec: Codec,
    /// The estimated compressed block target of the runs this engine builds.
    target_block_compressed: usize,
    snapshot_limits: SnapshotLimits,
    /// Cloned by every snapshot, so that its count is the live snapshots.
    snapshots: Arc<()>,
    /// Present when a background worker compacts this engine.
    background: Option<worker::Signal>,
    /// Makes the worker panic at the start of its next pass.
    #[cfg(test)]
    panic_worker: std::sync::atomic::AtomicBool,
    /// Makes the next compaction panic once it holds the compaction lock,
    /// before it takes the state lock.
    #[cfg(test)]
    panic_step: std::sync::atomic::AtomicBool,
    /// Flushes that have waited for level 0, so that tests can tell when a
    /// writer is blocked.
    #[cfg(test)]
    level0_waits: std::sync::atomic::AtomicUsize,
    /// Runs between building a compaction and returning it.
    #[cfg(test)]
    after_build: Mutex<Option<Arc<dyn Fn() + Send + Sync>>>,
    #[cfg(test)]
    before_flush_build: Mutex<Option<Arc<dyn Fn() + Send + Sync>>>,
    #[cfg(test)]
    after_ingest_build: Mutex<Option<Arc<dyn Fn() + Send + Sync>>>,
    /// Merges that failed at the output limit after reading their inputs.
    #[cfg(test)]
    overflows: std::sync::atomic::AtomicUsize,
}

/// A key-value engine. See the [module documentation](self) for its
/// consistency, flushing, compaction and persistence rules.
pub struct Engine {
    core: Arc<Core>,
    /// Joined when the engine is dropped.
    worker: Option<std::thread::JoinHandle<()>>,
    flush_worker: Option<std::thread::JoinHandle<()>>,
}

impl Engine {
    /// Creates an empty RAM-only engine, starting a background compaction
    /// worker if [`EngineOptions::background_compaction`] is set (the
    /// default). Fails with [`OpenError::Config`] if any option is outside
    /// its range, before anything is allocated or started, or with
    /// [`OpenError::Worker`] if the worker's thread cannot be created.
    pub fn new(options: EngineOptions) -> Result<Self, OpenError> {
        options.validate().map_err(OpenError::Config)?;
        Self::start(Arc::new(Core::new(&options)), &options)
    }

    /// Wraps `core`, starting its worker if `options` ask for one. On failure
    /// the core is dropped with nothing running.
    fn start(core: Arc<Core>, options: &EngineOptions) -> Result<Self, OpenError> {
        let worker = options
            .background_compaction
            .map(|compaction| worker::start(Arc::clone(&core), compaction))
            .transpose()
            .map_err(OpenError::Worker)?;
        let mut engine = Self {
            core,
            worker,
            flush_worker: None,
        };
        engine.flush_worker =
            Some(flusher::start(Arc::clone(&engine.core)).map_err(OpenError::Worker)?);
        Ok(engine)
    }

    /// Shuts the engine down: stops the background worker after the
    /// compaction it is running, if any, which is published as usual, joins
    /// its thread and releases the engine's memory.
    /// No new compaction starts once this begins.
    ///
    /// It consumes the engine, so no read, write, [`ReadGuard`] or other
    /// call can be in progress or follow; an engine shared through an `Arc`
    /// is closed once it is the last reference ([`Arc::into_inner`]).
    /// [`Snapshot`]s are unaffected and stay readable.
    ///
    /// Closing writes nothing: no flush or checkpoint. The engine's data
    /// goes with it unless checkpointed first.
    /// Dropping the engine does the same and discards the result.
    ///
    /// Fails only with [`BackgroundError::Panicked`] if the worker had
    /// stopped by panicking, at any time; the engine is shut down all the
    /// same. A compaction that the worker found refused is not a failure
    /// here: it is reported by [`Engine::background_status`] while the
    /// engine is open.
    ///
    /// Nothing can use the engine after it is closed, or close it while a
    /// [`ReadGuard`] is held:
    ///
    /// ```compile_fail,E0382
    /// # use tessera::engine::{Engine, EngineOptions};
    /// let engine = Engine::new(EngineOptions::default()).unwrap();
    /// engine.close().unwrap();
    /// engine.put(b"k", b"v").unwrap();
    /// ```
    ///
    /// ```compile_fail,E0505
    /// # use tessera::engine::{Engine, EngineOptions};
    /// let engine = Engine::new(EngineOptions::default()).unwrap();
    /// let guard = engine.read();
    /// engine.close().unwrap();
    /// drop(guard);
    /// ```
    pub fn close(mut self) -> Result<(), BackgroundError> {
        self.stop_worker()
    }

    fn stop_worker(&mut self) -> Result<(), BackgroundError> {
        self.core.flush_signal.stop();
        if let Some(handle) = self.flush_worker.take() {
            let _ = handle.join();
        }
        let flush_error = self.core.flush_signal.failure();
        let Some(handle) = self.worker.take() else {
            return flush_error.map_or(Ok(()), Err);
        };
        let signal = self.core.background.as_ref().expect(WORKER);
        signal.stop();
        // The thread catches its own panics, so joining cannot fail.
        let _ = handle.join();
        match signal.status().panic {
            Some(message) => Err(BackgroundError::Panicked(message)),
            None => flush_error.map_or(Ok(()), Err),
        }
    }

    /// Sets `key` to `value` and returns the update's sequence number.
    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<u64, UpdateError> {
        self.core.put(key, value)
    }

    /// Deletes `key` and returns the update's sequence number.
    pub fn delete(&self, key: &[u8]) -> Result<u64, UpdateError> {
        self.core.delete(key)
    }

    /// Applies every operation atomically and returns the last sequence number.
    /// Later operations on the same key win. An empty batch returns the current
    /// sequence number without changing state. Batches built directly into runs
    /// take one publication sequence; memtable batches take one per operation.
    pub fn write(&self, batch: &WriteBatch) -> Result<u64, UpdateError> {
        self.core.write_batch(batch)
    }

    /// Builds a strictly key-sorted, duplicate-free batch directly into runs
    /// outside the state lock and publishes it atomically. Returns the single
    /// publication sequence of the whole batch; an empty batch returns the
    /// current ceiling. Rejects unsorted or repeated keys without changes.
    /// Tables sharing an actual batch key are flushed before publication.
    /// Runs are built once; concurrent writes to other keys cause no retries.
    /// Writes completed before publication are older than the ingested data.
    pub fn ingest_sorted(&self, batch: &WriteBatch) -> Result<u64, UpdateError> {
        if !batch.is_sorted() {
            return Err(UpdateError::Rejected(WriteError::BatchNotSorted));
        }
        batch.validate(self.memtable_budget())?;
        self.core.ingest_batch(batch, true)
    }

    fn memtable_budget(&self) -> usize {
        self.core.memtable_budget
    }

    /// Returns the latest value of `key`, or `None` if it is absent or
    /// deleted. The value is copied into `workspace`, once per call.
    pub fn get<'w>(
        &self,
        key: &[u8],
        workspace: &'w mut EngineWorkspace,
    ) -> Result<Option<LiveValue<'w>>, ReadError> {
        self.core.get(key, workspace)
    }

    /// Takes the shared lock and returns a stable view of the engine, for a
    /// few reads that must agree.
    ///
    /// Writers and flush publication wait until it is dropped, so use a
    /// [`Snapshot`] for a long scan. See the [module documentation](self) for
    /// what must not be done while holding it.
    pub fn read(&self) -> ReadGuard<'_> {
        self.core.read()
    }

    /// Returns a view of the current state that holds no lock.
    ///
    /// Takes the exclusive lock to snapshot the active memtable, in constant
    /// time, to copy one pointer per frozen table, and to share the run list
    /// with one reference count; see
    /// [Snapshots](self#snapshots). Snapshots taken with nothing written
    /// between them share the same view of the active table.
    ///
    /// Refused, changing nothing, when a [`SnapshotLimits`] would be exceeded.
    pub fn snapshot(&self) -> Result<Snapshot, SnapshotError> {
        self.core.snapshot()
    }

    /// Waits for the flush thread to put everything written before this call
    /// into runs, including any tables left by a refused flush
    /// and published in place of them. Waits for any flush in progress. An
    /// engine with nothing in memory does nothing.
    ///
    /// Writes never need this to make room. Refused with a [`RunLimitError`],
    /// leaving the tables frozen and readable, if publishing would exceed
    /// the run limits.
    pub fn flush(&self) -> Result<FlushReport, RunLimitError> {
        self.core.flush()
    }

    /// Takes one compaction step, building in `workspace`, or returns `None`
    /// if nothing is due. Waits for any compaction in progress, including
    /// the background worker's.
    ///
    /// The step is the first of those described under
    /// [Compaction](self#compaction) that applies: a merge of level 0 into
    /// level 1, a merge from the level most over its size into the next, a
    /// move, a split, or a merge of two small neighbours. A move or a level
    /// insertion changes only levels, which reads never consult, and returns
    /// a [`CompactionKind::Move`] report with nothing read or written.
    ///
    /// Each step lowers a level's excess, moves data down or lowers the run
    /// count, so compacting repeatedly with the same options and no new
    /// flushes stops. Afterwards a lookup visits at most one run per level
    /// and one per level-0 flush ([`EngineStats::max_overlap`]).
    ///
    /// The merge runs without the state lock: reads, writes and flushes
    /// proceed beside it, and its output is published in one step. Inputs a
    /// snapshot holds stay allocated until it is dropped.
    ///
    /// Refused with [`CompactionError::JobLimit`] if a level needs a job
    /// that no split brings within the limits, which only larger limits
    /// resolve; with [`CompactionError::Retained`] while snapshots retain
    /// too much, which dropping them resolves (a move still goes on).
    pub fn compact(
        &self,
        options: CompactionOptions,
        workspace: &mut MergeWorkspace,
    ) -> Result<Option<CompactionReport>, CompactionError> {
        self.core.compact(options, workspace)
    }

    /// Replaces the limits on published runs, as for
    /// [`EngineOptions::max_runs`] and [`EngineOptions::max_run_bytes`].
    /// Values below what is published now are accepted: flushes are then
    /// refused until compaction makes room. Refused, changing nothing, if
    /// either is 0.
    pub fn set_run_limits(&self, max_runs: usize, max_run_bytes: usize) -> Result<(), ConfigError> {
        ConfigError::check_run_limits(max_runs, max_run_bytes)?;
        self.core.set_run_limits(max_runs, max_run_bytes);
        Ok(())
    }

    /// Returns a summary of the current state.
    pub fn stats(&self) -> EngineStats {
        self.core.stats()
    }

    /// Asks the background worker to compact everything now eligible and
    /// waits until a pass that started after this call has finished.
    ///
    /// Use it to drain pending compaction, or after a flush rejected at the
    /// run limits: on success the flush can be retried, and a failure says
    /// compaction cannot make room. The wait lasts one pass, which ends once
    /// nothing is eligible; flushes during it can extend it, until the run
    /// limits stop them. Do not call it while holding a [`ReadGuard`] or a
    /// compaction job, which the worker would wait for.
    pub fn wait_for_compaction(&self) -> Result<(), BackgroundError> {
        match &self.core.background {
            Some(signal) => signal.wait(),
            None => Err(BackgroundError::Disabled),
        }
    }

    /// Returns what the background worker has done, or `None` if the engine
    /// has none.
    pub fn background_status(&self) -> Option<BackgroundStatus> {
        self.core.background.as_ref().map(worker::Signal::status)
    }
}

impl Core {
    fn new(options: &EngineOptions) -> Self {
        Self {
            state: RwLock::new(State {
                last_seq: options.last_seq,
                active: Memtable::new(options.memtable_budget, Some(options.last_seq)),
                frozen: Vec::new(),
                runs: Vec::new(),
                directory: Arc::new(RunDirectory::empty()),
                run_bytes: 0,
                held_bytes: 0,
                levels: Levels::new(options.level_ratio),
                // The stop is the worker's backpressure. Without one, the
                // application compacts when it chooses, and only the run
                // limits bound level 0.
                level0_stop: if options.background_compaction.is_some() {
                    LEVEL0_STOP
                } else {
                    usize::MAX
                },
                #[cfg(test)]
                split_flushes: true,
                max_runs: options.max_runs,
                max_run_bytes: options.max_run_bytes,
                retired: Vec::new(),
                last_view: None,
                refused: None,
                blocked: None,
                flushes: 0,
                flushed_bytes: 0,
                flush_totals: FlushReport::default(),
            }),
            gate: Mutex::new(()),
            flushing: Mutex::new(()),
            compacting: Mutex::new(()),
            background_threads: options.background_threads,
            level_ratio: options.level_ratio,
            checkpointing: Mutex::new(()),
            memtable_budget: options.memtable_budget,
            max_frozen_memtables: options.max_frozen_memtables,
            flush_signal: flusher::Signal::new(),
            bulk_ingest_min: options
                .bulk_ingest_min
                .unwrap_or(options.memtable_budget / 2),
            target_run_len: options.target_run_len,
            codec: options.compression.codec(),
            target_block_compressed: options.target_block_compressed,
            snapshot_limits: options.snapshots,
            snapshots: Arc::new(()),
            background: options.background_compaction.map(|_| worker::Signal::new()),
            #[cfg(test)]
            panic_worker: std::sync::atomic::AtomicBool::new(false),
            #[cfg(test)]
            panic_step: std::sync::atomic::AtomicBool::new(false),
            #[cfg(test)]
            level0_waits: std::sync::atomic::AtomicUsize::new(0),
            #[cfg(test)]
            after_build: Mutex::new(None),
            #[cfg(test)]
            before_flush_build: Mutex::new(None),
            #[cfg(test)]
            after_ingest_build: Mutex::new(None),
            #[cfg(test)]
            overflows: std::sync::atomic::AtomicUsize::new(0),
        }
    }

    fn put(&self, key: &[u8], value: &[u8]) -> Result<u64, UpdateError> {
        self.update(key, Value::Bytes(value))
    }

    fn delete(&self, key: &[u8]) -> Result<u64, UpdateError> {
        self.update(key, Value::Tombstone)
    }

    fn update(&self, key: &[u8], value: Value<'_>) -> Result<u64, UpdateError> {
        let mut state = self.write_state();
        loop {
            match self.write(&mut state, key, value) {
                Err(Refusal::Full) => state = self.make_room(state)?,
                written => return written.map_err(Refusal::into_error),
            }
        }
    }

    /// Waits only for backpressure, then swaps the active table. Never builds.
    fn make_room<'s>(
        &'s self,
        mut state: RwLockWriteGuard<'s, State>,
    ) -> Result<RwLockWriteGuard<'s, State>, UpdateError> {
        state = self.wait_for_level0(state);
        if state.levels.level0_flushes() >= state.level0_stop {
            return Err(UpdateError::FlushRefused(state.run_limit(
                state.runs.len(),
                state.run_bytes + state.held_bytes,
                state.levels.level0_flushes() + 1,
            )));
        }
        while state.frozen.len() >= self.max_frozen_memtables {
            if let Some(error) = self.flush_error(&state) {
                return Err(UpdateError::FlushRefused(error));
            }
            state = self.wait_for_flush(state);
        }
        if !state.active.is_empty() {
            self.freeze_active(&mut state);
        }
        self.flush_signal.request();
        Ok(state)
    }

    fn flush_error(&self, state: &State) -> Option<RunLimitError> {
        if let Some(error) = self.flush_signal.failure() {
            let mut refusal = state.run_limit(
                state.runs.len(),
                state.run_bytes + state.held_bytes,
                state.levels.level0_flushes(),
            );
            refusal.background = Some(Box::new(error));
            Some(refusal)
        } else {
            state.refused.clone()
        }
    }

    fn wait_for_flush<'s>(
        &'s self,
        state: RwLockWriteGuard<'s, State>,
    ) -> RwLockWriteGuard<'s, State> {
        // Acquire the notification mutex before releasing state: completion
        // cannot slip between checking the predicate and entering the condvar.
        let progress = self.flush_signal.lock();
        drop(state);
        drop(self.flush_signal.wait(progress));
        self.write_state()
    }

    /// The level-0 backpressure, before a flush is built: while level 0 is
    /// at its stop and a running worker can drain it, waits for the worker
    /// without the state lock until
    /// a step it publishes leaves room or a pass begun after this call ends.
    /// One pass is all it waits for: a pass ends only once nothing is due or
    /// a step is refused, and level 0 at its stop is always due, so a pass
    /// that leaves no room ended on a refusal (snapshots retaining too much,
    /// a job over the limits) that waiting longer would not lift. That
    /// failure, or the worker's panic, is kept in `State::blocked` for the
    /// refusal at publication to carry. Callers recheck their predicates
    /// after this wait because ingestion can publish concurrently.
    ///
    /// A refusal at the level-0 stop is forgotten once level 0 has room
    /// again, so the next write needing room tries a flush; any other
    /// standing refusal is returned at once by the caller, without waiting.
    fn wait_for_level0<'s>(
        &'s self,
        state: RwLockWriteGuard<'s, State>,
    ) -> RwLockWriteGuard<'s, State> {
        let full = |state: &State| state.levels.level0_flushes() >= state.level0_stop;
        let lift = |mut state: RwLockWriteGuard<'s, State>| {
            if !full(&state) {
                state.blocked = None;
                if state
                    .refused
                    .as_ref()
                    .is_some_and(RunLimitError::level0_only)
                {
                    state.refused = None;
                }
            }
            state
        };
        let Some(signal) = &self.background else {
            return lift(state);
        };
        if !full(&state)
            || state
                .refused
                .as_ref()
                .is_some_and(|refused| !refused.level0_only())
        {
            return lift(state);
        }
        drop(state);
        #[cfg(test)]
        self.level0_waits
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let waited = signal.wait_for(|| !full(&self.read_state()));
        let mut state = self.write_state();
        if full(&state) {
            state.blocked = waited.err();
        }
        lift(state)
    }

    /// Waits for any flush in progress and holds off others until the guard
    /// is dropped. The mutex guards no data, so a panic elsewhere leaves
    /// nothing to distrust, and the flush stays retryable.
    fn serialize_flush(&self) -> MutexGuard<'_, ()> {
        self.flushing.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Applies a RAM-only write to the active table.
    fn write(&self, state: &mut State, key: &[u8], value: Value<'_>) -> Result<u64, Refusal> {
        let seq = state
            .last_seq
            .checked_add(1)
            .ok_or(UpdateError::SequencesExhausted)?;
        let written = match value {
            Value::Bytes(bytes) => state.active.put(key, seq, bytes),
            Value::Tombstone => state.active.delete(key, seq),
        };
        written.map_err(|error| self.refusal(error))?;
        // Set only now: a rejected write must not use up a number.
        state.last_seq = seq;
        Ok(seq)
    }

    /// Distinguishes invalid records from an active table needing rotation.
    fn refusal(&self, error: TableError) -> Refusal {
        let rejected = |error| Refusal::Error(UpdateError::Rejected(error));
        match error {
            TableError::KeyTooLong { len } => rejected(WriteError::KeyTooLong { len }),
            TableError::ValueTooLong { len } => rejected(WriteError::ValueTooLong { len }),
            TableError::RecordExceedsBudget { charge, .. } => {
                rejected(WriteError::RecordExceedsBudget {
                    charge,
                    budget: self.memtable_budget,
                })
            }
            TableError::BudgetExceeded { .. } => Refusal::Full,
            TableError::SequenceNotIncreasing { .. } => {
                unreachable!("the engine assigns, and the log checks, increasing numbers")
            }
        }
    }

    /// Freezes the active table and replaces it, in constant time.
    fn freeze_active(&self, state: &mut State) {
        let successor = Memtable::new(self.memtable_budget, Some(state.last_seq));
        let table = std::mem::replace(&mut state.active, successor).freeze();
        state.frozen.push(Arc::new(table));
    }

    fn get<'w>(
        &self,
        key: &[u8],
        workspace: &'w mut EngineWorkspace,
    ) -> Result<Option<LiveValue<'w>>, ReadError> {
        let EngineWorkspace { logical, value, .. } = workspace;
        let state = self.read_state();
        let in_memory = state
            .active
            .get(key)
            .or_else(|| state.frozen.iter().rev().find_map(|table| table.get(key)));
        if let Some(version) = in_memory {
            let Value::Bytes(bytes) = version.value else {
                return Ok(None);
            };
            value.clear();
            value.extend_from_slice(bytes);
            return Ok(Some(LiveValue {
                seq: version.seq,
                value,
            }));
        }
        // The runs are immutable and shared, so they are read after the lock
        // is released: the result is the state at the lock, and a writer or
        // publication waits only for the memtable lookup, not for decoding.
        let directory = Arc::clone(&state.directory);
        drop(state);
        let found = latest_from(directory.lookup(), key, logical)?;
        Ok(found.map(|found| {
            value.clear();
            value.extend_from_slice(found.value);
            LiveValue {
                seq: found.seq,
                value,
            }
        }))
    }

    fn read(&self) -> ReadGuard<'_> {
        ReadGuard(self.read_state())
    }

    fn snapshot(&self) -> Result<Snapshot, SnapshotError> {
        let limits = self.snapshot_limits;
        let mut state = self.write_state();
        // Clones are made only under the exclusive lock, so this count is
        // exact; drops can only lower it.
        let live = Arc::strong_count(&self.snapshots) - 1;
        if live >= limits.max_live {
            return Err(SnapshotError::Readers {
                live,
                max: limits.max_live,
            });
        }
        let bytes = state.retained().bytes;
        if bytes > limits.max_retained_bytes {
            return Err(SnapshotError::Retained {
                bytes,
                max: limits.max_retained_bytes,
            });
        }
        let mut tables =
            Vec::with_capacity(state.frozen.len() + usize::from(!state.active.is_empty()));
        if !state.active.is_empty() {
            let reusable = state
                .last_view
                .as_ref()
                .filter(|(seq, _)| *seq == state.last_seq)
                .and_then(|(_, view)| view.upgrade());
            let view = reusable.unwrap_or_else(|| {
                let view = Arc::new(state.active.snapshot());
                state.last_view = Some((state.last_seq, Arc::downgrade(&view)));
                view
            });
            tables.push(view);
        }
        tables.extend(state.frozen.iter().rev().cloned());
        Ok(Snapshot {
            tables,
            runs: Arc::clone(&state.directory),
            last_seq: state.last_seq,
            _live: Arc::clone(&self.snapshots),
        })
    }

    fn flush(&self) -> Result<FlushReport, RunLimitError> {
        let mut state = self.write_state();
        let before = state.flush_totals;
        while !state.active.is_empty() && state.frozen.len() >= self.max_frozen_memtables {
            if let Some(error) = self.flush_error(&state) {
                return Err(error);
            }
            self.flush_signal.request();
            state = self.wait_for_flush(state);
        }
        if !state.active.is_empty() {
            self.freeze_active(&mut state);
        }
        let target = state.frozen.last().and_then(|table| table.last_seq());
        self.flush_signal.request();
        while target.is_some_and(|target| {
            state
                .frozen
                .first()
                .is_some_and(|table| table.last_seq().is_some_and(|seq| seq <= target))
        }) {
            if let Some(error) = self.flush_error(&state) {
                return Err(error);
            }
            state = self.wait_for_flush(state);
        }
        Ok(state.flush_totals.since(before))
    }

    /// Freezes the active table, unless it is empty, and starts a flush of
    /// every frozen table. Returns `None` if there is nothing to flush.
    #[cfg(test)]
    fn begin_flush(&self) -> Option<FlushJob<'_>> {
        let serial = self.serialize_flush();
        let mut state = self.wait_for_level0(self.write_state());
        if !state.active.is_empty() {
            self.freeze_active(&mut state);
        }
        if state.frozen.is_empty() {
            return None;
        }
        let tables = state.frozen.clone();
        let cuts = state.flush_cuts(self.target_run_len);
        drop(state);
        Some(FlushJob {
            engine: self,
            _serial: serial,
            tables,
            cuts,
        })
    }

    fn compact(
        &self,
        options: CompactionOptions,
        workspace: &mut MergeWorkspace,
    ) -> Result<Option<CompactionReport>, CompactionError> {
        options
            .validate(self.target_run_len, self.target_block_compressed)
            .map_err(CompactionError::Config)?;
        match self.begin_compaction(options)? {
            Some(Begun::Job(job)) => job.build(workspace)?.publish().map(Some),
            Some(Begun::Done(report)) => Ok(Some(report)),
            None => Ok(None),
        }
    }

    /// Chooses the next step from the levels, under the exclusive lock
    /// briefly, so that level insertions and moves, which change only
    /// levels, are done in the same section. Selection reads metadata only:
    /// level sizes, key ranges by binary search and the runs of level 0.
    fn begin_compaction(
        &self,
        options: CompactionOptions,
    ) -> Result<Option<Begun<'_>>, CompactionError> {
        // As for flushing, this mutex guards no data.
        let serial = self
            .compacting
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        #[cfg(test)]
        if self
            .panic_step
            .swap(false, std::sync::atomic::Ordering::SeqCst)
        {
            panic!("injected compaction failure");
        }
        let mut state = self.write_state();
        let bytes = state.retained().bytes;
        let max = self.snapshot_limits.max_retained_bytes;
        let fit = options.fit(self.target_run_len, state.level0_stop);
        state.levels.insert_levels(&fit);
        let plan = state.levels.plan(&fit);
        // Snapshots retaining too much refuse every step that would retire
        // runs they hold. A move retires nothing, so it goes on, and can
        // drain level 0 while the snapshots last.
        if bytes > max && !matches!(plan, Plan::Idle | Plan::Move { .. }) {
            return Err(CompactionError::Retained { bytes, max });
        }
        let refusal = |refusal: levels::Refusal| match refusal.outgrown {
            // Refused under a source's allowance: what stops it is the
            // output limit, which an earlier job from it reached.
            Some(output_bytes) => CompactionError::OutputLimit {
                output_bytes,
                max_output_bytes: options.max_output_bytes,
            },
            None => CompactionError::JobLimit {
                level: refusal.level,
                inputs: refusal.inputs,
                input_bytes: refusal.bytes,
                workspace_bytes: refusal.workspace_bytes,
                max_inputs: options.max_inputs,
                max_input_bytes: options.max_input_bytes,
                max_output_bytes: options.max_output_bytes,
                max_workspace_bytes: options.max_workspace_bytes,
            },
        };
        let (kind, level, inputs, dest, cuts, cursor, split) = match plan {
            Plan::Idle => return Ok(None),
            Plan::Refuse(refused) => return Err(refusal(refused)),
            Plan::Move { level, run } => {
                if state.levels.move_down(level, &run).is_err() {
                    return Err(CompactionError::Stale);
                }
                // A move from level 0 can lift the level-0 stop, so a flush
                // refused only for it may succeed now. It changes no run
                // count or byte, so any other refusal stands.
                if state
                    .refused
                    .as_ref()
                    .is_some_and(RunLimitError::level0_only)
                {
                    state.refused = None;
                }
                if cfg!(any(test, debug_assertions)) {
                    state.levels.check(&state.runs);
                }
                self.flush_signal.request();
                return Ok(Some(Begun::Done(CompactionReport {
                    kind: CompactionKind::Move,
                    level,
                    inputs: 0,
                    input_bytes: 0,
                    input_held_bytes: 0,
                    outputs: 0,
                    output_bytes: 0,
                    output_held_bytes: 0,
                    filter_build_bytes: 0,
                    complete: false,
                    stats: CompactStats::default(),
                    workspace_bytes: 0,
                })));
            }
            Plan::Merge {
                level,
                sources,
                dests,
                below,
            } => {
                let cursor =
                    Cursor::After(sources[0].run.key_range().expect(NOT_EMPTY).max.to_vec());
                let cuts = OutputCuts {
                    overlapped: below,
                    // A run reaching `level_ratio` runs below has crossed the
                    // starts of all but the first.
                    max_overlapped: self.level_ratio - 1,
                    ..OutputCuts::default()
                };
                let inputs = sources.into_iter().chain(dests).collect();
                let dest = Dest::Level(level + 1);
                (
                    CompactionKind::Merge,
                    level,
                    inputs,
                    dest,
                    cuts,
                    Some(cursor),
                    None,
                )
            }
            Plan::Split {
                level,
                source,
                cuts,
                refused,
            } => {
                let cursor = Cursor::At(source.run.key_range().expect(NOT_EMPTY).min.to_vec());
                let dest = if level == 0 {
                    Dest::InPlace
                } else {
                    Dest::Level(level)
                };
                let cuts = OutputCuts {
                    at: cuts,
                    ..OutputCuts::default()
                };
                let split = Some(refusal(refused));
                (
                    CompactionKind::Split,
                    level,
                    vec![source],
                    dest,
                    cuts,
                    Some(cursor),
                    split,
                )
            }
            Plan::Neighbours { level, runs } => {
                let cuts = OutputCuts {
                    single: true,
                    ..OutputCuts::default()
                };
                let inputs = runs.into_iter().collect();
                (
                    CompactionKind::Neighbours,
                    level,
                    inputs,
                    Dest::Level(level),
                    cuts,
                    None,
                    None,
                )
            }
        };
        let others = Self::others(&state, &inputs);
        // Logical reads resolve overlapping runs by sequence. Preserve it if
        // any unselected run could compete, even above the bottom level.
        // A snapshot taken after selection retains the original input runs.
        let zero_seq = matches!(dest, Dest::Level(level) if level == state.levels.depth())
            && others.is_empty()
            && Arc::strong_count(&self.snapshots) == 1;
        let estimated_run_bytes = state.levels.segment(&fit);
        drop(state);
        Ok(Some(Begun::Job(CompactionJob {
            engine: self,
            _serial: serial,
            inputs,
            others,
            options,
            kind,
            level,
            dest,
            zero_seq,
            cuts,
            cursor,
            refusal: split,
            estimated_run_bytes,
        })))
    }

    /// Begins the next compaction that reads and writes, taking any moves
    /// before it: for tests that stage one merge.
    #[cfg(test)]
    fn begin_job(
        &self,
        options: CompactionOptions,
    ) -> Result<Option<CompactionJob<'_>>, CompactionError> {
        loop {
            match self.begin_compaction(options)? {
                Some(Begun::Job(job)) => return Ok(Some(job)),
                Some(Begun::Done(_)) => {}
                None => return Ok(None),
            }
        }
    }

    /// The runs outside `inputs` that overlap their span, for the
    /// tombstone rule.
    fn others(state: &State, inputs: &[SharedRun]) -> Vec<SharedRun> {
        let ranges = inputs
            .iter()
            .map(|run| run.run.key_range().expect(NOT_EMPTY));
        let min = ranges.clone().map(|range| range.min).min();
        let max = ranges.map(|range| range.max).max();
        match (min, max) {
            (Some(min), Some(max)) => state.levels.others(inputs, min, max),
            _ => Vec::new(),
        }
    }

    /// Begins a compaction of the runs `picks` names, by index into the run
    /// list, whatever their levels: the tests' arbitrary merges. The output
    /// goes into the deepest level of the inputs if it fits there, and
    /// otherwise into level 0 as a new flush.
    #[cfg(test)]
    fn begin_picked(
        &self,
        options: CompactionOptions,
        picks: Vec<usize>,
    ) -> Result<CompactionJob<'_>, CompactionError> {
        let serial = self
            .compacting
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        let state = self.write_state();
        let bytes = state.retained().bytes;
        let max = self.snapshot_limits.max_retained_bytes;
        if bytes > max {
            return Err(CompactionError::Retained { bytes, max });
        }
        let inputs: Vec<SharedRun> = picks
            .iter()
            .map(|&index| Arc::clone(&state.runs[index]))
            .collect();
        let deepest = inputs
            .iter()
            .filter_map(|run| state.levels.level_of(run))
            .max()
            .unwrap_or(0);
        let others = Self::others(&state, &inputs);
        let estimated_run_bytes = state
            .levels
            .segment(&options.fit(self.target_run_len, state.level0_stop));
        drop(state);
        Ok(CompactionJob {
            engine: self,
            _serial: serial,
            inputs,
            others,
            options,
            kind: CompactionKind::Merge,
            level: deepest,
            dest: Dest::Anywhere(deepest),
            zero_seq: false,
            cuts: OutputCuts::default(),
            cursor: None,
            refusal: None,
            estimated_run_bytes,
        })
    }

    fn set_run_limits(&self, max_runs: usize, max_run_bytes: usize) {
        let mut state = self.write_state();
        state.max_runs = max_runs;
        state.max_run_bytes = max_run_bytes;
        state.refused = None;
        drop(state);
        self.notify_worker();
    }

    /// Asks the worker, if any, to look for eligible compactions.
    fn notify_worker(&self) {
        self.flush_signal.request();
        if let Some(signal) = &self.background {
            signal.request();
        }
    }

    fn stats(&self) -> EngineStats {
        let state = self.read_state();
        let retained = state.retained();
        EngineStats {
            last_seq: state.last_seq,
            active: state.active.memory(),
            frozen: state
                .frozen
                .iter()
                .map(|table| table.memory())
                .reduce(|sum, table| MemoryUsage {
                    payload: sum.payload + table.payload,
                    charged: sum.charged + table.charged,
                    pinned: sum.pinned + table.pinned,
                    budget: sum.budget + table.budget,
                }),
            frozen_tables: state.frozen.len(),
            runs: state.runs.len(),
            run_bytes: state.run_bytes,
            held_bytes: state.held_bytes,
            max_overlap: state.directory.group_count(),
            snapshots: Arc::strong_count(&self.snapshots) - 1,
            retained_sources: retained.sources,
            retained_bytes: retained.bytes,
            retained_run_bytes: retained.run_bytes,
            retained_held_bytes: retained.held_bytes,
            level0_flushes: state.levels.level0_flushes(),
            level0_runs: state.levels.shape()[0].0,
            levels: state.levels.depth(),
            flushes: state.flushes,
            flushed_bytes: state.flushed_bytes,
        }
    }

    fn read_state(&self) -> RwLockReadGuard<'_, State> {
        drop(self.pass_gate());
        self.state.read().expect(LOCK)
    }

    fn write_state(&self) -> RwLockWriteGuard<'_, State> {
        let _gate = self.pass_gate();
        self.state.write().expect(LOCK)
    }

    fn pass_gate(&self) -> MutexGuard<'_, ()> {
        // The gate guards no data, so a panic elsewhere leaves nothing wrong.
        self.gate.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

/// Why a write did not land.
#[derive(Clone, Debug)]
enum Refusal {
    /// The memtables have no room for the record until a flush makes some.
    /// Never returned to a caller.
    Full,
    /// A refusal for the caller.
    Error(UpdateError),
}

impl Refusal {
    /// The caller's error, for a refusal that cannot be [`Refusal::Full`].
    fn into_error(self) -> UpdateError {
        match self {
            Self::Error(error) => error,
            Self::Full => unreachable!("a write with no room flushes"),
        }
    }
}

impl From<UpdateError> for Refusal {
    fn from(error: UpdateError) -> Self {
        Self::Error(error)
    }
}

/// Returns the most runs whose inclusive key ranges share a key, which the
/// directory's group count equals; kept to check that.
#[cfg(test)]
fn deepest_overlap(runs: &[SharedRun]) -> usize {
    let mut bounds: Vec<(&[u8], bool)> = runs
        .iter()
        .flat_map(|run| {
            let range = run.run.key_range().expect(NOT_EMPTY);
            [(range.min, false), (range.max, true)]
        })
        .collect();
    // At one key, starts sort before ends, since ranges include both bounds.
    bounds.sort_unstable();
    let (mut depth, mut deepest) = (0usize, 0);
    for (_, end) in bounds {
        if end {
            depth -= 1;
        } else {
            depth += 1;
            deepest = deepest.max(depth);
        }
    }
    deepest
}

const LOCK: &str = "a write section holding the state lock panicked";
/// Flushes publish only non-empty tables, and compaction builds a run only
/// once it has a record.
const NOT_EMPTY: &str = "published runs are not empty";

impl Drop for Engine {
    /// As [`Engine::close`], discarding its result.
    fn drop(&mut self) {
        let _ = self.stop_worker();
    }
}

const WORKER: &str = "an engine with a worker has its signal";

impl fmt::Debug for Engine {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Engine")
            .field("stats", &self.stats())
            .finish_non_exhaustive()
    }
}

/// A flush whose tables are frozen and whose runs are not built yet: the
/// staging that lets a flush compress without the state lock.
///
/// Dropping it abandons the flush and keeps the tables frozen. Holding it
/// blocks other flushes, including a write's that needs room, but not reads,
/// writes that fit, or snapshots.
struct FlushJob<'e> {
    engine: &'e Core,
    _serial: MutexGuard<'e, ()>,
    /// Oldest first: a prefix of the state's frozen tables, since only
    /// flushes, one at a time, add or remove them.
    tables: Vec<Arc<FrozenMemtable>>,
    /// Where to split the runs, from level 1 when the flush began. Level 1
    /// may change before the flush publishes; level 0 allows any overlap,
    /// so stale cuts change only how well the runs fit, never a read.
    cuts: FlushCuts,
}

impl<'e> FlushJob<'e> {
    /// Builds the runs, compressing without the state lock. Several tables
    /// are merged as one, keeping each key's newest update.
    fn build(self) -> BuiltFlush<'e> {
        let tables: Vec<&FrozenMemtable> = self.tables.iter().map(Arc::as_ref).collect();
        let runs = flush_merged(
            &tables,
            self.engine.target_run_len,
            self.engine.codec,
            self.engine.target_block_compressed,
            &self.cuts,
        );
        BuiltFlush { job: self, runs }
    }

    /// The records in the tables, counting a key once per table.
    fn records(&self) -> usize {
        self.tables.iter().map(|table| table.len()).sum()
    }
}

impl fmt::Debug for FlushJob<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FlushJob")
            .field("tables", &self.tables.len())
            .field("records", &self.records())
            .finish_non_exhaustive()
    }
}

/// A flush with its runs built but not visible to anyone.
struct BuiltFlush<'e> {
    job: FlushJob<'e>,
    runs: Vec<Run<Vec<u8>>>,
}

impl BuiltFlush<'_> {
    /// Publishes the runs and removes the frozen tables in one step.
    ///
    /// On a [`RunLimitError`] the built runs are dropped and the tables stay
    /// frozen and readable.
    fn publish(self) -> Result<FlushReport, RunLimitError> {
        let Self { job, runs } = self;
        let report = FlushReport {
            tables: job.tables.len(),
            records: job.records(),
            runs: runs.len(),
            run_bytes: runs.iter().map(|run| run.stored_len()).sum(),
            held_bytes: runs.iter().map(Run::held_bytes).sum(),
            filter_build_bytes: runs.iter().map(Run::filter_build_bytes).max().unwrap_or(0),
        };
        let mut state = job.engine.write_state();
        let total_runs = state.runs.len() + report.runs;
        let total_bytes = state.run_bytes + state.held_bytes + report.run_bytes + report.held_bytes;
        let flushes = state.levels.level0_flushes() + 1;
        if total_runs > state.max_runs
            || total_bytes > state.max_run_bytes
            || flushes > state.level0_stop
        {
            let error = state.run_limit(total_runs, total_bytes, flushes);
            state.refused = Some(error.clone());
            drop(state);
            // Compaction is what makes room. A pass that failed earlier, say
            // while snapshots retained too much, is otherwise not retried.
            // At the level-0 stop alone, this flush has just waited for a
            // whole pass and nothing has flushed since, so another would
            // only repeat the refused job.
            if !error.level0_only() {
                job.engine.notify_worker();
            }
            return Err(error);
        }
        let flushed = job.tables.len();
        assert!(
            state.frozen[..flushed]
                .iter()
                .zip(&job.tables)
                .all(|(held, flushed)| Arc::ptr_eq(held, flushed)),
            "a flush's tables are the oldest frozen ones"
        );
        let runs: Vec<SharedRun> = published(runs).collect();
        let logical = job
            .tables
            .iter()
            .map(|table| table.memory().payload + table.len() * RECORD_INPUT_OVERHEAD)
            .sum();
        state.levels.push_flush(&runs, logical);
        state.runs.extend(runs);
        state.order_runs();
        state.run_bytes += report.run_bytes;
        state.held_bytes += report.held_bytes;
        state.refused = None;
        state.flushes += 1;
        state.flushed_bytes += report.run_bytes as u64;
        state.flush_totals.add(report);
        for table in state.frozen.drain(..flushed).collect::<Vec<_>>() {
            // This and the job's are the engine's references, and snapshots
            // are taken only under the lock, so any other is a snapshot's,
            // which keeps the whole table. Otherwise only views of it can
            // remain, which share its family, so this follows all of them.
            let bytes = if Arc::strong_count(&table) > 2 {
                table.memory().charged
            } else {
                table.held_by_snapshots()
            };
            state.retire(table.family() as Weak<dyn Send + Sync>, bytes, None);
        }
        // Unless a snapshot holds them, `job` has the last references, so the
        // tables are freed after this releases the lock.
        drop(state);
        job.engine.notify_worker();
        job.engine.flush_signal.changed();
        Ok(report)
    }
}

impl fmt::Debug for BuiltFlush<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BuiltFlush")
            .field("runs", &self.runs.len())
            .finish_non_exhaustive()
    }
}

/// Bounds and policy for one compaction. See [`Engine::compact`].
///
/// Each limit is checked against a job before anything is read: a job that
/// would exceed one is not formed. Every option is checked on its own, as
/// below, and the byte and workspace limits also against the engine's
/// [`EngineOptions::target_run_len`] and
/// [`EngineOptions::target_block_compressed`], so that a job of two full runs
/// always fits (a [`ConfigError`] otherwise).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CompactionOptions {
    /// The most runs merged at once, at least 2.
    pub max_inputs: usize,
    /// The most bytes of input runs, stored bytes and what they hold beside
    /// them (key filters and segment metadata), at least twice
    /// [`EngineOptions::target_run_len`].
    pub max_input_bytes: usize,
    /// The most bytes of output, stored bytes, key filters and segment metadata,
    /// built and held until publication, as for
    /// [`EngineOptions::max_run_bytes`]; at least twice
    /// [`EngineOptions::target_run_len`]. A job's output is estimated by its
    /// input before it starts, and checked as it is written; a job is formed
    /// only if its input leaves a thirty-second of this limit free for what
    /// output runs add (footers, filters, blocks packed differently), so
    /// that a job chosen is not refused here and then chosen again. It counts bytes
    /// written in exact segment allocations; the builder's reusable encoding
    /// scratch is charged to `max_workspace_bytes`.
    pub max_output_bytes: usize,
    /// The most estimated workspace bytes: one block and a decompression
    /// context per input, plus the builder's segment and block buffers and
    /// compression context. Parallel builders share this cap: segment buffers
    /// shrink with their count, and high fan-in jobs use fewer builders.
    /// A job exceeding this is refused before decoding.
    /// Remaining room bounds key hashes; an output needing more hashes gets
    /// no filter. At least the estimate for two 256 KiB input blocks.
    pub max_workspace_bytes: usize,
    /// Level 0 is compacted once it holds runs of this many flushes, at
    /// least 1, which compacts every flush (level 0 is compacted at 12
    /// whatever this is). After compaction a point read of a key absent
    /// from the memtables visits at most one run per flush left in level 0,
    /// fewer than this, and one per level below it.
    ///
    /// Before leveled compaction this bounded every key's overlap; level
    /// 0's flushes are what remains of that overlap, and the default is the
    /// same.
    pub max_overlap: usize,
}

impl Default for CompactionOptions {
    /// 32 inputs, 256 MiB in and out, 64 MiB of workspace, and level 0
    /// compacted at 4 flushes.
    fn default() -> Self {
        Self {
            max_inputs: 32,
            max_input_bytes: 256 << 20,
            max_output_bytes: 256 << 20,
            max_workspace_bytes: 64 << 20,
            max_overlap: 4,
        }
    }
}

/// The share of [`CompactionOptions::max_output_bytes`] that selection keeps
/// free, as one part in this many, for what a merge's output adds to its
/// inputs' bytes: run footers, filters and blocks packed differently.
/// Measured growth is at most about a kilobyte for jobs of tens to
/// hundreds of kilobytes; a change of codec or dictionary can add more, and
/// then the merge fails at the output limit as before.
const OUTPUT_HEADROOM: usize = 32;

impl CompactionOptions {
    /// The one check of these options, made when an engine is built with
    /// them for its worker and by every [`Engine::compact`], against the
    /// engine's run and block lengths.
    fn validate(
        &self,
        target_run_len: usize,
        _target_block_compressed: usize,
    ) -> Result<(), ConfigError> {
        ConfigError::check(
            "CompactionOptions::max_inputs",
            self.max_inputs as u64,
            2,
            usize::MAX as u64,
            "a merge takes at least two runs",
        )?;
        let two_runs = target_run_len.saturating_mul(2);
        for (setting, value) in [
            ("CompactionOptions::max_input_bytes", self.max_input_bytes),
            ("CompactionOptions::max_output_bytes", self.max_output_bytes),
        ] {
            ConfigError::check(
                setting,
                value as u64,
                two_runs as u64,
                usize::MAX as u64,
                "a job of two full runs must fit, twice EngineOptions::target_run_len",
            )?;
        }
        ConfigError::check(
            "CompactionOptions::max_workspace_bytes",
            self.max_workspace_bytes as u64,
            crate::run::workspace_estimate([RAW_BLOCK_CAP; 2]) as u64,
            usize::MAX as u64,
            "a merge of two runs of EngineOptions::target_block_compressed blocks must fit",
        )?;
        ConfigError::check(
            "CompactionOptions::max_overlap",
            self.max_overlap as u64,
            1,
            usize::MAX as u64,
            "level 0 holds at least one flush",
        )
    }

    fn fit(&self, target_run_len: usize, level0_stop: usize) -> Fit {
        Fit {
            max_inputs: self.max_inputs,
            max_bytes: self
                .max_input_bytes
                .min(self.max_output_bytes - self.max_output_bytes / OUTPUT_HEADROOM),
            max_workspace_bytes: self.max_workspace_bytes,
            trigger: self.max_overlap,
            level0_stop,
            target_run_len,
        }
    }
}

/// Why a compaction changed nothing.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CompactionError {
    /// A level needs compacting, but the smallest job that would make
    /// progress there exceeds a limit: its inputs, their bytes (as input,
    /// and as the estimate of its output), or its workspace estimate.
    /// Nothing was read. Every job of two full runs fits the limits
    /// [`CompactionOptions`] accepts, so this follows runs longer than
    /// [`EngineOptions::target_run_len`] (a single record longer than it,
    /// or runs built under a larger setting) or blocks longer than
    /// [`EngineOptions::target_block_compressed`]; raising the limit named lets it
    /// proceed, and retrying unchanged meets it again.
    JobLimit {
        /// The level whose job it is.
        level: usize,
        /// Runs the job would merge.
        inputs: usize,
        /// Their stored and held bytes.
        input_bytes: usize,
        /// The job's workspace estimate.
        workspace_bytes: usize,
        /// [`CompactionOptions::max_inputs`].
        max_inputs: usize,
        /// [`CompactionOptions::max_input_bytes`].
        max_input_bytes: usize,
        /// [`CompactionOptions::max_output_bytes`].
        max_output_bytes: usize,
        /// [`CompactionOptions::max_workspace_bytes`].
        max_workspace_bytes: usize,
    },
    /// The output would exceed [`CompactionOptions::max_output_bytes`],
    /// the limit on all of one job's output runs together. Nothing was
    /// published; a larger limit lets it proceed. A job is admitted by its
    /// inputs' bytes, so this follows output that outgrew them by more than
    /// the limit's headroom, as a change of codec or dictionary can make it.
    /// Selection then halves what a job from the same source may take, each
    /// time this happens to it, but never below the source with the largest
    /// run below it, and splits the source if need be; once even that job
    /// has failed, the source's steps are refused with this error, until
    /// the runs below it change, before
    /// anything is read, naming the output that crossed the limit, and
    /// other levels go on.
    OutputLimit {
        /// Bytes of output, stored and filters, when the limit was crossed.
        output_bytes: usize,
        /// The limit.
        max_output_bytes: usize,
    },
    /// The merge's workspace estimate exceeds
    /// [`CompactionOptions::max_workspace_bytes`]. Nothing was decoded.
    WorkspaceLimit {
        /// The estimate.
        workspace_bytes: usize,
        /// The limit.
        max_workspace_bytes: usize,
    },
    /// An input run is damaged, or two runs disagree about one update. The
    /// runs are unchanged; this is corruption, not a limit, and retrying
    /// meets it again.
    Read(ReadError),
    /// Publishing would leave more runs or run bytes than both the engine's
    /// limit and what is published now.
    RunLimit(RunLimitError),
    /// Snapshots retain more than [`SnapshotLimits::max_retained_bytes`], so
    /// the output could only add to it. Dropping snapshots lifts this. Moves,
    /// which write and retire nothing, are not refused.
    Retained {
        /// Bytes retained.
        bytes: usize,
        /// The limit.
        max: usize,
    },
    /// The runs the job chose were no longer where it found them when it
    /// was to publish. Nothing was published; compactions are serialized,
    /// so this is a defect, reported rather than published.
    Stale,
    /// The options were refused before anything was selected.
    Config(ConfigError),
}

impl From<CompactError> for CompactionError {
    fn from(error: CompactError) -> Self {
        match error {
            CompactError::Read(error) => Self::Read(error),
            CompactError::OutputLimit {
                output_bytes,
                max_output_bytes,
            } => Self::OutputLimit {
                output_bytes,
                max_output_bytes,
            },
            CompactError::WorkspaceLimit {
                workspace_bytes,
                max_workspace_bytes,
            } => Self::WorkspaceLimit {
                workspace_bytes,
                max_workspace_bytes,
            },
        }
    }
}

impl fmt::Display for CompactionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::JobLimit {
                level,
                inputs,
                input_bytes,
                workspace_bytes,
                max_inputs,
                max_input_bytes,
                max_output_bytes,
                max_workspace_bytes,
            } => write!(
                f,
                "level {level} needs a compaction of {inputs} runs, {input_bytes} bytes and \
                 {workspace_bytes} workspace bytes, over the limits of {max_inputs} runs, \
                 {max_input_bytes} bytes in, {max_output_bytes} bytes out and \
                 {max_workspace_bytes} workspace bytes; raise them to proceed"
            ),
            Self::OutputLimit {
                output_bytes,
                max_output_bytes,
            } => write!(
                f,
                "compaction output reached {output_bytes} bytes, over the limit of {max_output_bytes}"
            ),
            Self::WorkspaceLimit {
                workspace_bytes,
                max_workspace_bytes,
            } => write!(
                f,
                "compaction needs {workspace_bytes} workspace bytes, over the limit of {max_workspace_bytes}"
            ),
            Self::Read(error) => error.fmt(f),
            Self::RunLimit(error) => error.fmt(f),
            Self::Retained { bytes, max } => write!(
                f,
                "snapshots retain {bytes} bytes, over the limit of {max}; compaction waits"
            ),
            Self::Stale => f.write_str("compaction found its runs moved and published nothing"),
            Self::Config(error) => error.fmt(f),
        }
    }
}

impl std::error::Error for CompactionError {}

/// What kind of step a compaction took.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum CompactionKind {
    /// Runs of one level merged with the runs of the next that they
    /// overlap, into the next.
    Merge,
    /// A run relabelled to the next level, which it overlaps nowhere:
    /// nothing was read or written.
    Move,
    /// A run rewritten alone, in its level, in pieces whose own merges fit
    /// the limits where its whole merge did not.
    Split,
    /// Two adjacent small runs of one level merged into one.
    Neighbours,
}

/// What a compaction replaced.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CompactionReport {
    /// What kind of step it was. A move replaces nothing, and every count
    /// below is 0.
    pub kind: CompactionKind,
    /// The level the step took its runs from.
    pub level: usize,
    /// Runs removed.
    pub inputs: usize,
    /// Their stored bytes.
    pub input_bytes: usize,
    /// What they held beside that: key filters and physical segment metadata.
    pub input_held_bytes: usize,
    /// Runs published in their place.
    pub outputs: usize,
    /// Their stored bytes.
    pub output_bytes: usize,
    /// What they hold beside that: key filters and physical segment metadata.
    pub output_held_bytes: usize,
    /// The most bytes one output's filter construction held for key hashes,
    /// freed when that run was finished.
    pub filter_build_bytes: usize,
    /// Whether no other run overlapped the inputs, so that every tombstone
    /// could be dropped.
    pub complete: bool,
    /// What the merge read and wrote.
    pub stats: CompactStats,
    /// The workspace estimate checked before merging.
    pub workspace_bytes: usize,
}

/// A compaction whose inputs are chosen and whose output is not built yet.
///
/// It holds its inputs and the other runs that overlap them, which stay
/// allocated until it is dropped even if no longer published. Holding it
/// blocks other compactions, including the background worker's, but not
/// reads, writes or flushes. Dropping it abandons the compaction and changes
/// nothing.
struct CompactionJob<'e> {
    engine: &'e Core,
    _serial: MutexGuard<'e, ()>,
    inputs: Vec<SharedRun>,
    /// Unselected runs whose key ranges overlap the inputs' span. They may
    /// hold older versions that the inputs' tombstones hide.
    others: Vec<SharedRun>,
    options: CompactionOptions,
    kind: CompactionKind,
    level: usize,
    /// Where the output goes.
    dest: Dest,
    /// Settled output with no competing unselected run or pre-existing snapshot.
    zero_seq: bool,
    cuts: OutputCuts,
    /// Where the source level's next job starts once this publishes.
    cursor: Option<Cursor>,
    /// For a split, the job it stands in for: refused if the split leaves
    /// fewer than two pieces, so that a split always makes progress.
    refusal: Option<CompactionError>,
    /// Stored size of a full target-length run, using the planner estimate.
    estimated_run_bytes: usize,
}

/// How a published job moves its source level's cursor.
#[derive(Debug)]
enum Cursor {
    /// Past this key, the source's largest.
    After(Vec<u8>),
    /// To this key, a split source's smallest, so that its first piece is
    /// the level's next source.
    At(Vec<u8>),
}

/// A compaction step chosen under the lock: a job to build, or a move,
/// already done.
enum Begun<'e> {
    Job(CompactionJob<'e>),
    Done(CompactionReport),
}

impl<'e> CompactionJob<'e> {
    fn input_bytes(&self) -> usize {
        self.inputs.iter().map(|run| run.run.stored_len()).sum()
    }

    /// Returns whether no other run overlaps the inputs, so that every
    /// tombstone can be dropped.
    fn is_complete(&self) -> bool {
        self.others.is_empty()
    }

    /// Merges the inputs without the state lock.
    ///
    /// A tombstone is kept if any other run overlapping the inputs has a key
    /// range covering its key, since that run may hold an older value; runs
    /// published later come from flushes and hold only newer updates. On an
    /// error nothing is built and the engine is unchanged.
    fn build(self, workspace: &mut MergeWorkspace) -> Result<BuiltCompaction<'e>, CompactionError> {
        let views: Vec<RunView<'_>> = self.inputs.iter().map(|run| run.run.view()).collect();
        let ranges: Vec<_> = self
            .others
            .iter()
            .map(|run| run.run.key_range().expect(NOT_EMPTY))
            .collect();
        let keep_tombstone = |key: &[u8]| {
            ranges
                .iter()
                .any(|range| range.min <= key && key <= range.max)
        };
        let limits = CompactLimits {
            target_run_len: self.engine.target_run_len,
            max_output_bytes: self.options.max_output_bytes,
            max_workspace_bytes: self.options.max_workspace_bytes,
            codec: self.engine.codec,
            target_block_compressed: self.engine.target_block_compressed,
            zero_seq: self.zero_seq,
        };
        let output = match compact_parallel(
            &views,
            keep_tombstone,
            limits,
            &self.cuts,
            self.engine.background_threads,
            self.estimated_run_bytes,
            workspace,
        ) {
            Ok(output) => output,
            Err(error) => {
                if let CompactError::OutputLimit { output_bytes, .. } = error {
                    // So that selection does not choose this job again.
                    #[cfg(test)]
                    self.engine
                        .overflows
                        .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    let mut state = self.engine.write_state();
                    state.levels.outgrown(&self.inputs, output_bytes);
                }
                return Err(error.into());
            }
        };
        #[cfg(test)]
        {
            let hook = self.engine.after_build.lock().unwrap().clone();
            if let Some(hook) = hook {
                hook();
            }
        }
        drop(ranges);
        drop(views);
        Ok(BuiltCompaction { job: self, output })
    }
}

impl fmt::Debug for CompactionJob<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CompactionJob")
            .field("kind", &self.kind)
            .field("level", &self.level)
            .field("inputs", &self.inputs.len())
            .field("input_bytes", &self.input_bytes())
            .field("others", &self.others.len())
            .finish_non_exhaustive()
    }
}

/// A compaction with its output built but not visible to anyone.
struct BuiltCompaction<'e> {
    job: CompactionJob<'e>,
    output: crate::run::Compacted,
}

impl BuiltCompaction<'_> {
    /// Replaces the inputs with the output in one step under the lock.
    ///
    /// Every input must still be published where selection found it, and
    /// the output must fit its level beside the runs there. Runs published
    /// since selection, all flushes into level 0, are kept. A result over
    /// the run limits is accepted if it is no larger than what it replaces.
    /// On an error the output is dropped and the engine is unchanged. The
    /// inputs are freed after the lock is released, once no reader holds
    /// them.
    fn publish(self) -> Result<CompactionReport, CompactionError> {
        let Self { job, output } = self;
        let unchanged =
            output.runs.len() == 1 && output.stats.records_written == output.stats.records_read;
        if job.kind == CompactionKind::Split && unchanged {
            // The source rewritten whole, dropping nothing: no progress. A
            // split that dropped records (tombstones or older versions) has
            // fewer to rewrite next time, so it publishes even as one run.
            return Err(job.refusal.clone().expect("a split carries its job"));
        }
        let report = CompactionReport {
            kind: job.kind,
            level: job.level,
            inputs: job.inputs.len(),
            input_bytes: job.input_bytes(),
            input_held_bytes: job.inputs.iter().map(|run| run.run.held_bytes()).sum(),
            outputs: output.runs.len(),
            output_bytes: output.output_bytes,
            output_held_bytes: output.held_bytes,
            filter_build_bytes: output
                .runs
                .iter()
                .map(Run::filter_build_bytes)
                .max()
                .unwrap_or(0),
            complete: job.is_complete(),
            stats: output.stats,
            workspace_bytes: output.workspace_bytes,
        };
        let is_input = |run: &SharedRun| job.inputs.iter().any(|input| Arc::ptr_eq(input, run));
        let mut state = job.engine.write_state();
        if state.runs.iter().filter(|run| is_input(run)).count() != job.inputs.len() {
            drop(state);
            unreachable!("compactions are serialized, so only this one removes runs");
        }
        let total_runs = state.runs.len() - report.inputs + report.outputs;
        let stored = state.run_bytes - report.input_bytes + report.output_bytes;
        let filters = state.held_bytes - report.input_held_bytes + report.output_held_bytes;
        let (total_bytes, published_bytes) = (stored + filters, state.run_bytes + state.held_bytes);
        // Shrinking is always allowed, so that compaction can make room even
        // when the limits were lowered below what is published. Growth is
        // what this output adds, measured, not estimated.
        let grows_runs = total_runs > state.max_runs.max(state.runs.len());
        let grows_bytes = total_bytes > state.max_run_bytes.max(published_bytes);
        if grows_runs || grows_bytes {
            let flushes = state.levels.level0_flushes();
            return Err(CompactionError::RunLimit(state.run_limit(
                total_runs,
                total_bytes,
                flushes,
            )));
        }
        let outputs: Vec<SharedRun> = published(output.runs).collect();
        if state
            .levels
            .replace(&job.inputs, &outputs, job.dest)
            .is_err()
        {
            return Err(CompactionError::Stale);
        }
        match &job.cursor {
            Some(Cursor::After(key)) => state.levels.advance(job.level, key),
            Some(Cursor::At(key)) => state.levels.rewind(job.level, key),
            None => {}
        }
        state.runs.retain(|run| !is_input(run));
        state.runs.extend(outputs);
        state.order_runs();
        state.run_bytes = stored;
        state.held_bytes = filters;
        state.refused = None;
        for input in &job.inputs {
            let (bytes, filter) = (input.run.stored_len(), input.run.held_bytes());
            state.retire(
                Arc::downgrade(input) as Weak<dyn Send + Sync>,
                bytes,
                Some(filter),
            );
        }
        drop(state);
        job.engine.flush_signal.request();
        drop(job);
        Ok(report)
    }
}

impl fmt::Debug for BuiltCompaction<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BuiltCompaction")
            .field("runs", &self.output.runs.len())
            .field("output_bytes", &self.output.output_bytes)
            .finish_non_exhaustive()
    }
}

/// Reusable buffers for reading an engine, created once per reader.
#[derive(Debug, Default)]
pub struct EngineWorkspace {
    logical: LogicalWorkspace,
    value: Vec<u8>,
    prefix: Vec<u8>,
    multi: multi::Workspace,
}

impl EngineWorkspace {
    /// Creates an empty workspace, which allocates nothing until first used.
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns the decoded-block bytes retained across all runs.
    ///
    /// Point lookups keep two blocks whatever the number of runs. Scans keep
    /// one per group of key-disjoint runs they have merged, at most the
    /// largest [`EngineStats::max_overlap`] they have read.
    pub fn capacity(&self) -> usize {
        self.logical.capacity() + self.multi.capacity()
    }

    /// Returns the bytes held by Zstandard decompression contexts, as
    /// Zstandard reports them: one context for point lookups, and one per
    /// group of runs a scan has decoded Zstandard blocks from. A context is created on
    /// first use and kept for the workspace's lifetime.
    ///
    /// These bytes are allocated outside the Rust heap, are not part of
    /// [`capacity`](Self::capacity), and are charged against no limit.
    pub fn context_bytes(&self) -> usize {
        self.logical.context_bytes() + self.multi.context_bytes()
    }

    /// Returns how many run blocks reads through this workspace have loaded.
    pub fn blocks_loaded(&self) -> u64 {
        self.logical.blocks_loaded() + self.multi.blocks_loaded()
    }
}

/// A stable view of an [`Engine`], holding its state lock shared until it
/// is dropped, from [`Engine::read`].
///
/// Every read through it sees one state, and it copies and retains nothing.
/// In exchange, **writers, flushes and compaction publication wait while it
/// is held**, and a waiting writer holds back new readers. Keep it to a few
/// reads that must agree, and drop it promptly. For a long scan, or a view
/// kept while writes continue, take a [`Snapshot`]: it holds no lock, at the
/// cost of retaining what the engine releases while it lives.
///
/// Do not call [`Engine`] methods on a thread that holds a guard: a write
/// would deadlock. The guard borrows the engine, so the engine cannot be
/// closed while it is held.
///
/// ```
/// use tessera::engine::{Engine, EngineOptions, EngineWorkspace};
///
/// let engine = Engine::new(EngineOptions::default())?;
/// engine.put(b"from", b"10")?;
/// engine.put(b"to", b"5")?;
/// let mut workspace = EngineWorkspace::new();
/// let (from, to) = {
///     let guard = engine.read();
///     let from = guard.get(b"from", &mut workspace)?.map(|v| v.value.to_vec());
///     let to = guard.get(b"to", &mut workspace)?.map(|v| v.value.to_vec());
///     (from, to)
/// }; // Writers wait until the guard is dropped here.
/// assert_eq!((from.as_deref(), to.as_deref()), (Some(&b"10"[..]), Some(&b"5"[..])));
/// engine.close()?;
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
pub struct ReadGuard<'e>(RwLockReadGuard<'e, State>);

impl ReadGuard<'_> {
    /// Returns the latest value of `key`, or `None` if it is absent or
    /// deleted, without copying it.
    pub fn get<'r>(
        &'r self,
        key: &[u8],
        workspace: &'r mut EngineWorkspace,
    ) -> Result<Option<LiveValue<'r>>, ReadError> {
        lookup_state(&self.0, key, &mut workspace.logical)
    }

    /// Returns a scan of every live key.
    pub fn scan<'r>(&'r self, workspace: &'r mut EngineWorkspace) -> Scan<'r> {
        self.scan_prefix(&[], workspace)
    }

    /// Returns a scan of every live key starting with `prefix`.
    pub fn scan_prefix<'r>(
        &'r self,
        prefix: &[u8],
        workspace: &'r mut EngineWorkspace,
    ) -> Scan<'r> {
        let state = &*self.0;
        let tables = std::iter::once(state.active.iter_from(prefix)).chain(
            state
                .frozen
                .iter()
                .rev()
                .map(|table| table.iter_from(prefix)),
        );
        Scan::new(tables, &state.directory, prefix, workspace)
    }
}

impl fmt::Debug for ReadGuard<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ReadGuard").finish_non_exhaustive()
    }
}

/// The engine's state when [`Engine::snapshot`] returned, holding no lock.
///
/// It owns shared references to the frozen memtables and runs that made up
/// that state, which never change, so reads through it agree with each other
/// and are unaffected by later writes, flushes and compactions. Each source
/// is freed when neither the engine nor any snapshot holds it.
///
/// A snapshot is `Send` and `Sync`, may be read from several threads with a
/// workspace each, and may outlive its engine: it stays readable after
/// [`Engine::close`]. It cannot be refreshed: take a
/// new one to see later writes. While it lives it keeps whatever the engine
/// releases from its view, and the memtable history later writes copy away
/// from it, reported in [`EngineStats::retained_bytes`], so drop it once read.
/// Nothing bounds how long it lives, but while it retains too much, new
/// snapshots and compactions are refused; see [Memory](self#memory).
///
/// It does not implement `Clone`, so that each one counts against
/// [`SnapshotLimits::max_live`]. Share one by reference or in an `Arc`:
///
/// ```compile_fail,E0599
/// # let engine = tessera::engine::Engine::new(Default::default()).unwrap();
/// let snapshot = engine.snapshot().unwrap();
/// let second = snapshot.clone();
/// ```
pub struct Snapshot {
    /// Newest first.
    tables: Vec<Arc<FrozenMemtable>>,
    runs: Arc<RunDirectory>,
    last_seq: u64,
    _live: Arc<()>,
}

impl Snapshot {
    /// The last sequence number of the state this sees. Every update up to
    /// it is visible and no later one is.
    pub fn last_seq(&self) -> u64 {
        self.last_seq
    }

    /// Returns the value `key` had, or `None` if it was absent or deleted,
    /// without copying it.
    pub fn get<'r>(
        &'r self,
        key: &[u8],
        workspace: &'r mut EngineWorkspace,
    ) -> Result<Option<LiveValue<'r>>, ReadError> {
        let in_memory = self.tables.iter().find_map(|table| table.get(key));
        resolve(in_memory, &self.runs, key, &mut workspace.logical)
    }

    /// Returns a scan of every live key.
    pub fn scan<'r>(&'r self, workspace: &'r mut EngineWorkspace) -> Scan<'r> {
        self.scan_prefix(&[], workspace)
    }

    /// Returns a scan of every live key starting with `prefix`.
    pub fn scan_prefix<'r>(
        &'r self,
        prefix: &[u8],
        workspace: &'r mut EngineWorkspace,
    ) -> Scan<'r> {
        let tables = self.tables.iter().map(|table| table.iter_from(prefix));
        Scan::new(tables, &self.runs, prefix, workspace)
    }
}

impl fmt::Debug for Snapshot {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Snapshot")
            .field("last_seq", &self.last_seq)
            .field("tables", &self.tables.len())
            .field("runs", &self.runs.len())
            .finish_non_exhaustive()
    }
}

/// Newest first: every key's updates are in `active`, then the frozen tables
/// from newest to oldest, then the runs, so the first source holding a key
/// decides it.
fn lookup_state<'r>(
    state: &'r State,
    key: &[u8],
    logical: &'r mut LogicalWorkspace,
) -> Result<Option<LiveValue<'r>>, ReadError> {
    let in_memory = state
        .active
        .get(key)
        .or_else(|| state.frozen.iter().rev().find_map(|table| table.get(key)));
    resolve(in_memory, &state.directory, key, logical)
}

/// Decides `key` from the newest table update, or else from the runs.
fn resolve<'r>(
    in_memory: Option<Version<'r>>,
    runs: &'r RunDirectory,
    key: &[u8],
    logical: &'r mut LogicalWorkspace,
) -> Result<Option<LiveValue<'r>>, ReadError> {
    match in_memory {
        Some(Version {
            seq,
            value: Value::Bytes(value),
        }) => Ok(Some(LiveValue { seq, value })),
        Some(_) => Ok(None),
        None => latest_from(runs.lookup(), key, logical),
    }
}

fn next_within<'a>(records: &mut Records<'a>, prefix: &[u8]) -> Option<Record<'a>> {
    records
        .next()
        .filter(|record| record.key.starts_with(prefix))
}

/// Iterates live keys in ascending order, each once with its latest value.
///
/// This merges the memtables and the live keys of the runs. A table's update
/// beats anything in older tables or the runs, so a tombstone in a table hides
/// the key whatever they hold, and values are not copied.
pub struct Scan<'r> {
    /// Each table's records and the next of them, newest table first.
    tables: Vec<(Records<'r>, Option<Record<'r>>)>,
    runs: LogicalCursor<'r>,
    /// Whether the record the runs stand on has been used.
    runs_stale: bool,
    prefix: &'r [u8],
}

impl<'r> Scan<'r> {
    /// Starts a scan of `tables`, newest first and each positioned at
    /// `prefix`, over `runs`.
    fn new(
        tables: impl Iterator<Item = Records<'r>>,
        runs: &'r RunDirectory,
        prefix: &[u8],
        workspace: &'r mut EngineWorkspace,
    ) -> Self {
        let EngineWorkspace {
            logical,
            prefix: bound,
            ..
        } = workspace;
        bound.clear();
        bound.extend_from_slice(prefix);
        let prefix: &'r [u8] = bound;

        let mut cursor = LogicalCursor::grouped(runs, runs.groups(), logical);
        // A failed seek is repeated by the first read of the cursor.
        let _ = cursor.seek_prefix(prefix);
        let tables = tables
            .map(|mut records| {
                let head = next_within(&mut records, prefix);
                (records, head)
            })
            .collect();
        Self {
            tables,
            runs: cursor,
            runs_stale: true,
            prefix,
        }
    }
}

impl Scan<'_> {
    /// Advances to the next live key, or returns `None` when none is left.
    pub fn next_record(&mut self) -> Result<Option<LiveRecord<'_>>, ReadError> {
        loop {
            if self.runs_stale {
                self.runs.next_record()?;
                self.runs_stale = false;
            }
            let run_key = self.runs.live().map(|record| record.key);
            let smallest = self
                .tables
                .iter()
                .filter_map(|(_, head)| head.map(|record| record.key))
                .chain(run_key)
                .min();
            let Some(key) = smallest else {
                return Ok(None);
            };

            self.runs_stale = run_key == Some(key);
            // The newest table holding the key decides it; every table
            // holding it moves past it.
            let mut decided = None;
            for (records, head) in &mut self.tables {
                if let Some(record) = head.filter(|record| record.key == key) {
                    decided.get_or_insert(record);
                    *head = next_within(records, self.prefix);
                }
            }

            match decided {
                Some(Record {
                    key,
                    seq,
                    value: Value::Bytes(value),
                }) => return Ok(Some(LiveRecord { key, seq, value })),
                Some(_) => {}
                None => return Ok(self.runs.live()),
            }
        }
    }
}

impl fmt::Debug for Scan<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Scan").finish_non_exhaustive()
    }
}

/// Helpers for compaction correctness tests. Picks are indices into the
/// published runs, valid while nothing else changes them.
#[cfg(test)]
impl Engine {
    /// Compacts `picks` through the production job: the same tombstone
    /// rule, limits, build and publication, with the output placed as
    /// [`Core::begin_picked`] says.
    pub(crate) fn compact_picked(
        &self,
        options: CompactionOptions,
        picks: Vec<usize>,
        workspace: &mut MergeWorkspace,
    ) -> Result<CompactionReport, CompactionError> {
        self.core
            .begin_picked(options, picks)?
            .build(workspace)?
            .publish()
    }

    /// Changes the level-0 stop for backpressure correctness tests.
    pub(crate) fn set_level0_stop(&self, stop: usize) {
        self.core.write_state().level0_stop = stop;
    }

    /// Turns flush splitting on or off for correctness comparisons.
    pub(crate) fn set_split_flushes(&self, split: bool) {
        self.core.write_state().split_flushes = split;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A panic while holding the state lock poisons it. Closing takes no
    /// state lock, so it still stops the worker, reports the worker's own
    /// panic if the poison reached it, and releases the core.
    #[test]
    fn closing_with_a_poisoned_state_lock_still_releases_the_core() {
        for worker in [false, true] {
            let options = if worker {
                EngineOptions::default()
            } else {
                manual()
            };
            let engine = Engine::new(options).unwrap();
            engine.put(b"k", b"v").unwrap();
            std::thread::scope(|scope| {
                let poisoned = scope.spawn(|| {
                    let _state = engine.core.write_state();
                    panic!("poison the state lock");
                });
                assert!(poisoned.join().is_err());
            });
            assert!(engine.core.state.is_poisoned());
            let core = Arc::clone(&engine.core);
            if worker {
                // The worker's next compaction meets the poisoned lock and
                // panics; that panic is what closing reports.
                let error = engine.wait_for_compaction().unwrap_err();
                let crate::engine::BackgroundError::Panicked(message) = &error else {
                    panic!("{error:?}");
                };
                assert!(message.starts_with(crate::engine::LOCK), "{message}");
                assert_eq!(engine.close(), Err(error));
            } else {
                assert_eq!(engine.close(), Ok(()));
            }
            assert_eq!(Arc::strong_count(&core), 1);
            drop(core);
        }
    }

    /// The defaults without the background worker: these tests flush and
    /// compact by hand and count what each step leaves.
    fn manual() -> EngineOptions {
        EngineOptions {
            background_compaction: None,
            ..EngineOptions::default()
        }
    }

    fn frozen_handle(engine: &Engine) -> Arc<FrozenMemtable> {
        let state = engine.core.read_state();
        assert_eq!(state.frozen.len(), 1);
        Arc::clone(&state.frozen[0])
    }

    /// The writer-priority rule: from the moment a writer waits for the state
    /// lock until it has it, the gate every reader must pass is closed.
    #[test]
    fn a_waiting_writer_holds_the_gate_until_it_has_the_lock() {
        let engine = Engine::new(manual()).unwrap();
        engine.put(b"k", b"old").unwrap();
        let held = engine.read();
        std::thread::scope(|scope| {
            let writer = scope.spawn(|| engine.put(b"k", b"new").unwrap());
            // The guard keeps the writer waiting, so once it has the gate it
            // keeps it until the guard is dropped.
            while engine.core.gate.try_lock().is_ok() {
                std::thread::yield_now();
            }
            for _ in 0..100 {
                assert!(engine.core.gate.try_lock().is_err());
                assert!(!writer.is_finished());
                std::thread::yield_now();
            }
            drop(held);
            writer.join().unwrap();
        });
        assert!(engine.core.gate.try_lock().is_ok());
        let mut workspace = EngineWorkspace::new();
        assert_eq!(
            engine.get(b"k", &mut workspace).unwrap().unwrap().value,
            b"new"
        );
    }

    #[test]
    fn publication_releases_every_engine_reference_to_the_frozen_table() {
        let engine = Engine::new(manual()).unwrap();
        engine.put(b"k", b"v").unwrap();
        let job = engine.core.begin_flush().unwrap();
        let held = frozen_handle(&engine);
        assert_eq!(Arc::strong_count(&held), 3);
        job.build().publish().unwrap();
        assert_eq!(Arc::strong_count(&held), 1);
    }

    #[test]
    fn an_abandoned_flush_releases_only_its_own_reference() {
        let engine = Engine::new(manual()).unwrap();
        engine.put(b"k", b"v").unwrap();
        let job = engine.core.begin_flush().unwrap();
        let held = frozen_handle(&engine);
        drop(job);
        assert_eq!(Arc::strong_count(&held), 2);
    }

    #[test]
    fn a_rejected_publication_keeps_the_frozen_table_in_the_engine() {
        let engine = Engine::new(EngineOptions {
            max_run_bytes: 1,
            ..manual()
        })
        .unwrap();
        engine.put(b"k", b"v").unwrap();
        let job = engine.core.begin_flush().unwrap();
        let held = frozen_handle(&engine);
        job.build().publish().unwrap_err();
        assert_eq!(Arc::strong_count(&held), 2);
        assert!(Arc::ptr_eq(&held, &frozen_handle(&engine)));
    }

    fn options(max_inputs: usize, max_input_bytes: usize) -> CompactionOptions {
        CompactionOptions {
            max_inputs,
            max_input_bytes,
            max_overlap: 1,
            ..CompactionOptions::default()
        }
    }

    fn overlapping_runs(engine: &Engine) {
        for value in [&b"1"[..], b"2", b"3"] {
            engine.put(b"a", value).unwrap();
            engine.put(b"z", value).unwrap();
            engine.flush().unwrap();
        }
    }

    /// A lookup's value and the run blocks it decoded, through each read
    /// path that reaches the runs.
    fn read_counting(engine: &Engine, key: &[u8]) -> [(Option<Vec<u8>>, u64); 2] {
        let mut workspace = EngineWorkspace::new();
        let got = engine
            .get(key, &mut workspace)
            .unwrap()
            .map(|v| v.value.to_vec());
        let direct = (got, workspace.blocks_loaded());
        let mut workspace = EngineWorkspace::new();
        let guard = engine.read();
        let got = guard
            .get(key, &mut workspace)
            .unwrap()
            .map(|v| v.value.to_vec());
        [direct, (got, workspace.blocks_loaded())]
    }

    #[test]
    fn a_lookup_decodes_only_the_newest_run_holding_the_key() {
        let engine = Engine::new(manual()).unwrap();
        overlapping_runs(&engine);
        engine.put(b"m", b"live").unwrap();
        engine.delete(b"z").unwrap();
        engine.flush().unwrap();
        assert_eq!(engine.stats().max_overlap, 4);
        // "a" is in three runs, the newest decides; "z" is deleted in a
        // fourth; "m" is in one.
        for (key, expected) in [
            (&b"a"[..], Some(&b"3"[..])),
            (b"z", None),
            (b"m", Some(b"live")),
        ] {
            for (value, blocks) in read_counting(&engine, key) {
                assert_eq!(value.as_deref(), expected);
                assert_eq!(blocks, 1, "{key:?}");
            }
        }
        // A snapshot keeps the runs it saw, and still reads one block.
        let snapshot = engine.snapshot().unwrap();
        engine.put(b"a", b"4").unwrap();
        engine.flush().unwrap();
        let mut workspace = EngineWorkspace::new();
        let seen = snapshot
            .get(b"a", &mut workspace)
            .unwrap()
            .unwrap()
            .value
            .to_vec();
        assert_eq!((seen, workspace.blocks_loaded()), (b"3".to_vec(), 1));
        assert_eq!(value(&engine, b"a").as_deref(), Some(&b"4"[..]));
    }

    /// A merge that leaves a newer level-0 run out leaves it over the
    /// output's range: here the output holds "j" and an older "k", and the
    /// run left out holds the newest "k", and reads decide by sequence.
    #[test]
    fn a_partial_compaction_leaves_interleaved_runs_that_read_correctly() {
        let engine = Engine::new(manual()).unwrap();
        engine.put(b"j", b"j1").unwrap();
        engine.put(b"k", b"k1").unwrap();
        engine.flush().unwrap();
        engine.put(b"j", b"j2").unwrap();
        engine.put(b"k", b"k2").unwrap();
        for i in 0..200u32 {
            engine.put(format!("x{i:04}").as_bytes(), &[7; 64]).unwrap();
        }
        engine.flush().unwrap();
        engine.put(b"k", b"k3").unwrap();
        engine.flush().unwrap();
        let before = engine.snapshot().unwrap();

        let options = CompactionOptions {
            max_overlap: 2,
            max_inputs: 2,
            ..CompactionOptions::default()
        };
        let mut workspace = MergeWorkspace::new();
        // The oldest run moves to level 1; the next merges with it, and the
        // newest is left in level 0.
        let report = engine.compact(options, &mut workspace).unwrap().unwrap();
        assert_eq!(report.kind, CompactionKind::Move);
        let report = engine.compact(options, &mut workspace).unwrap().unwrap();
        assert_eq!(report.kind, CompactionKind::Merge);
        assert_eq!((report.inputs, report.outputs), (2, 1));
        assert_eq!(engine.stats().runs, 2);
        // "k": the newer run decides, before the output's older version.
        // "j": only the output's range holds it.
        for (value, blocks) in read_counting(&engine, b"k") {
            assert_eq!((value.as_deref(), blocks), (Some(&b"k3"[..]), 1));
        }
        for (value, blocks) in read_counting(&engine, b"j") {
            assert_eq!((value.as_deref(), blocks), (Some(&b"j2"[..]), 1));
        }
        let mut workspace = EngineWorkspace::new();
        for (key, expected) in [(&b"j"[..], &b"j2"[..]), (b"k", b"k3")] {
            let seen = before
                .get(key, &mut workspace)
                .unwrap()
                .unwrap()
                .value
                .to_vec();
            assert_eq!(seen, expected);
        }
    }

    /// Compactions are serialized, so in the engine nothing else removes a
    /// run. Publication still checks rather than resurrect a removed run's
    /// records.
    #[test]
    #[should_panic(expected = "compactions are serialized")]
    fn publication_checks_that_its_inputs_are_published() {
        let engine = Engine::new(manual()).unwrap();
        overlapping_runs(&engine);
        let built = engine
            .core
            .begin_job(options(8, usize::MAX))
            .unwrap()
            .unwrap()
            .build(&mut MergeWorkspace::new())
            .unwrap();
        engine.core.write_state().runs.remove(1);
        let _ = built.publish();
    }

    /// Building takes no lock: it completes while a waiting writer holds the
    /// gate that every reader must pass, which would block it forever if it
    /// tried to read the state. A failure shows as a hang, which the
    /// validation timeout stops.
    #[test]
    fn building_a_compaction_takes_no_lock() {
        let engine = Engine::new(manual()).unwrap();
        overlapping_runs(&engine);
        let job = engine
            .core
            .begin_job(options(8, usize::MAX))
            .unwrap()
            .unwrap();
        let (held, release) = (std::sync::Barrier::new(2), std::sync::Barrier::new(2));
        std::thread::scope(|scope| {
            let reader = scope.spawn(|| {
                let guard = engine.read();
                held.wait();
                release.wait();
                drop(guard);
            });
            held.wait();
            let writer = scope.spawn(|| engine.put(b"m", b"new").unwrap());
            while engine.core.gate.try_lock().is_ok() {
                std::thread::yield_now();
            }
            let built = job.build(&mut MergeWorkspace::new()).unwrap();
            assert!(!writer.is_finished());
            release.wait();
            reader.join().unwrap();
            writer.join().unwrap();
            let report = built.publish().unwrap();
            assert_eq!((report.inputs, report.outputs), (3, 1));
        });
        assert_eq!(engine.stats().runs, 1);
    }

    #[test]
    fn publication_frees_the_inputs_once_nothing_holds_them() {
        let engine = Engine::new(manual()).unwrap();
        overlapping_runs(&engine);
        let inputs: Vec<SharedRun> = engine.core.read_state().runs.clone();
        let mut workspace = MergeWorkspace::new();
        while engine
            .compact(options(8, usize::MAX), &mut workspace)
            .unwrap()
            .is_some()
        {}
        assert_eq!(engine.stats().runs, 1);
        assert!(inputs.iter().all(|run| Arc::strong_count(run) == 1));
    }

    #[test]
    fn publication_refuses_growth_past_the_run_limits_but_allows_shrinking() {
        let engine = Engine::new(manual()).unwrap();
        overlapping_runs(&engine);
        engine.set_run_limits(1, 1).unwrap();
        let begin = || {
            engine
                .core
                .begin_job(options(8, usize::MAX))
                .unwrap()
                .unwrap()
        };
        // Beginning may first move a run down, which changes no bytes.
        let job = begin();
        let stats = engine.stats();
        let mut built = job.build(&mut MergeWorkspace::new()).unwrap();
        built.output.output_bytes = stats.run_bytes + stats.held_bytes + 1;
        assert!(matches!(built.publish(), Err(CompactionError::RunLimit(_))));
        assert_eq!(engine.stats(), stats);

        let report = begin()
            .build(&mut MergeWorkspace::new())
            .unwrap()
            .publish()
            .unwrap();
        assert!(report.output_bytes < stats.run_bytes);
        assert_eq!(engine.stats().runs, 1);
    }

    // Background compaction.

    use std::sync::Barrier;
    use std::sync::atomic::Ordering;

    fn background(max_overlap: usize) -> EngineOptions {
        EngineOptions {
            background_compaction: Some(CompactionOptions {
                max_overlap,
                ..CompactionOptions::default()
            }),
            ..manual()
        }
    }

    fn value(engine: &Engine, key: &[u8]) -> Option<Vec<u8>> {
        let mut workspace = EngineWorkspace::new();
        engine
            .get(key, &mut workspace)
            .unwrap()
            .map(|found| found.value.to_vec())
    }

    /// Pauses the next compaction build after merging, until released.
    fn pause_after_build(engine: &Engine) -> (Arc<Barrier>, Arc<Barrier>) {
        let (built, release) = (Arc::new(Barrier::new(2)), Arc::new(Barrier::new(2)));
        let (b, r) = (Arc::clone(&built), Arc::clone(&release));
        *engine.core.after_build.lock().unwrap() = Some(Arc::new(move || {
            b.wait();
            r.wait();
        }));
        (built, release)
    }

    fn stop_requested(engine: &Core) -> bool {
        engine.background.as_ref().unwrap().stop_requested()
    }

    #[test]
    fn a_flush_wakes_the_worker_which_compacts_and_goes_idle() {
        let engine = Engine::new(background(1)).unwrap();
        assert_eq!(engine.background_status().unwrap().passes, 0);
        overlapping_runs(&engine);
        engine.wait_for_compaction().unwrap();
        let status = engine.background_status().unwrap();
        assert!(status.running && !status.busy);
        assert!(status.compactions >= 1);
        assert!(status.passes <= 4, "{status:?}");
        assert_eq!(engine.stats().runs, 1);
        assert_eq!(engine.stats().max_overlap, 1);
        assert_eq!(value(&engine, b"a").unwrap(), b"3");
        // With nothing requested, nothing runs.
        for _ in 0..1000 {
            std::thread::yield_now();
        }
        assert_eq!(engine.background_status().unwrap(), status);
    }

    /// Requests made while the worker cannot run count as one more pass.
    #[test]
    fn requests_coalesce_while_the_worker_is_busy() {
        let engine = Engine::new(background(1)).unwrap();
        let slot = engine.core.compacting.lock().unwrap();
        for round in 0..10 {
            engine.put(b"a", &[round]).unwrap();
            engine.put(b"z", &[round]).unwrap();
            engine.flush().unwrap();
        }
        drop(slot);
        engine.wait_for_compaction().unwrap();
        let status = engine.background_status().unwrap();
        // Eleven requests: at most the pass held up by the slot, one for the
        // flushes after it started, and one for the wait. A worker that wakes
        // late serves them all in one.
        assert!((1..=3).contains(&status.passes), "{status:?}");
        assert_eq!(engine.stats().runs, 1);
    }

    /// While the worker holds a built compaction, reads see the inputs and a
    /// flush publishes beside them; publication keeps the flushed run.
    #[test]
    fn reads_and_flushes_proceed_while_the_worker_compacts() {
        let engine = Engine::new(background(2)).unwrap();
        let (built, release) = pause_after_build(&engine);
        overlapping_runs(&engine);
        built.wait();
        assert_eq!(engine.stats().runs, 3);
        assert_eq!(value(&engine, b"z").unwrap(), b"3");
        engine.delete(b"a").unwrap();
        engine.put(b"m", b"new").unwrap();
        engine.flush().unwrap();
        assert_eq!(engine.stats().runs, 4);
        *engine.core.after_build.lock().unwrap() = None;
        release.wait();
        engine.wait_for_compaction().unwrap();

        assert_eq!(value(&engine, b"a"), None);
        assert_eq!(value(&engine, b"m").unwrap(), b"new");
        assert_eq!(value(&engine, b"z").unwrap(), b"3");
        assert!(engine.stats().max_overlap <= 2);
    }

    #[test]
    fn dropping_an_idle_engine_joins_the_worker_and_frees_the_core() {
        let engine = Engine::new(background(1)).unwrap();
        overlapping_runs(&engine);
        engine.wait_for_compaction().unwrap();
        let core = Arc::clone(&engine.core);
        assert_eq!(Arc::strong_count(&core), 4);
        drop(engine);
        assert_eq!(Arc::strong_count(&core), 1);
        let status = core.background.as_ref().unwrap().status();
        assert!(!status.running && status.panic.is_none());
    }

    /// Dropping the engine mid-compaction waits for that compaction, which is
    /// published, and starts no other.
    #[test]
    fn dropping_during_a_compaction_publishes_it_and_then_stops() {
        let engine = Engine::new(background(1)).unwrap();
        let (built, release) = pause_after_build(&engine);
        // Held while the runs are flushed, so that one compaction takes all.
        let slot = engine.core.compacting.lock().unwrap();
        overlapping_runs(&engine);
        drop(slot);
        built.wait();
        let core = Arc::clone(&engine.core);
        std::thread::scope(|scope| {
            let dropper = scope.spawn(move || drop(engine));
            while !stop_requested(&core) {
                std::thread::yield_now();
            }
            release.wait();
            dropper.join().unwrap();
        });
        assert_eq!(Arc::strong_count(&core), 1);
        let status = core.background.as_ref().unwrap().status();
        // The oldest run's move, then the merge that was held.
        assert_eq!((status.running, status.compactions), (false, 2));
        assert_eq!(core.stats().runs, 1);
    }

    /// Closing waits for the compaction in progress, which is published,
    /// starts no other, reports success and releases the core; a snapshot
    /// taken before stays readable.
    #[test]
    fn closing_during_a_compaction_publishes_it_and_succeeds() {
        let engine = Engine::new(background(1)).unwrap();
        let (built, release) = pause_after_build(&engine);
        let slot = engine.core.compacting.lock().unwrap();
        overlapping_runs(&engine);
        let snapshot = engine.snapshot().unwrap();
        drop(slot);
        built.wait();
        let core = Arc::clone(&engine.core);
        std::thread::scope(|scope| {
            let closer = scope.spawn(move || engine.close());
            while !stop_requested(&core) {
                std::thread::yield_now();
            }
            assert!(!closer.is_finished());
            release.wait();
            assert_eq!(closer.join().unwrap(), Ok(()));
        });
        assert_eq!(Arc::strong_count(&core), 1);
        let status = core.background.as_ref().unwrap().status();
        // The oldest run's move, then the merge that was held.
        assert_eq!((status.running, status.compactions), (false, 2));
        assert_eq!(core.stats().runs, 1);
        let mut workspace = EngineWorkspace::new();
        let found = snapshot.get(b"a", &mut workspace).unwrap().unwrap();
        assert_eq!(found.value, b"3");
    }

    /// A worker that panicked is reported by `close` with its message, and
    /// the engine is released all the same; dropping one does not panic.
    #[test]
    fn closing_after_a_worker_panic_reports_it_and_releases_the_engine() {
        for close in [true, false] {
            let engine = Engine::new(background(1)).unwrap();
            engine.core.panic_worker.store(true, Ordering::Relaxed);
            overlapping_runs(&engine);
            let panicked = Err(BackgroundError::Panicked("injected worker failure".into()));
            assert_eq!(engine.wait_for_compaction(), panicked);
            let core = Arc::clone(&engine.core);
            if close {
                assert_eq!(engine.close(), panicked);
            } else {
                drop(engine);
            }
            assert_eq!(Arc::strong_count(&core), 1);
        }
    }

    /// A worker that cannot start fails construction with the operating
    /// system's error and leaves nothing running; an engine without a worker
    /// never starts one, so it is unaffected.
    #[test]
    fn a_worker_that_cannot_start_fails_construction() {
        worker::tests::FAIL_SPAWN.with(|fail| fail.set(true));
        let engine = Engine::new(manual()).unwrap();
        assert_eq!(engine.background_status(), None);
        drop(engine);
        let refused = Engine::new(EngineOptions::default()).map(drop);
        let Err(OpenError::Worker(error)) = refused else {
            panic!("{refused:?}");
        };
        assert!(
            error.to_string().contains("injected spawn failure"),
            "{error}"
        );
        // The failure was injected once; the next engine starts its worker.
        let engine = Engine::new(EngineOptions::default()).unwrap();
        assert!(engine.background_status().unwrap().running);
        assert_eq!(engine.close(), Ok(()));
    }

    /// A lasting failure ends the pass and waits for the next request: it is
    /// reported, counted once per request, and cleared by a later success.
    #[test]
    fn a_failed_compaction_is_reported_once_per_request_and_recovers() {
        let engine = Engine::new(EngineOptions {
            target_run_len: 15 << 10,
            background_compaction: Some(CompactionOptions {
                max_overlap: 1,
                max_input_bytes: 30 << 10,
                max_output_bytes: 30 << 10,
                ..CompactionOptions::default()
            }),
            ..manual()
        })
        .unwrap();
        let mut seed = 7u64;
        let mut noise = || {
            (0..20 << 10)
                .map(|_| {
                    seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
                    (seed >> 56) as u8
                })
                .collect::<Vec<u8>>()
        };
        // Records longer than the run length: two such runs of one key
        // exceed the job limits, which no split can help.
        for _ in 0..3 {
            for key in 0..2u8 {
                engine.put(&[key], &noise()).unwrap();
            }
            engine.flush().unwrap();
        }
        let error = engine.wait_for_compaction().unwrap_err();
        assert!(
            matches!(
                error,
                BackgroundError::Failed(CompactionError::JobLimit { .. })
            ),
            "{error:?}"
        );
        let status = engine.background_status().unwrap();
        // Three flushes and one wait made four requests.
        assert!(status.failures >= 1, "{status:?}");
        assert!(
            status.failures <= status.passes && status.passes <= 4,
            "{status:?}"
        );
        let runs = engine.stats().runs;
        assert_eq!(runs, 6);
        for _ in 0..1000 {
            std::thread::yield_now();
        }
        assert_eq!(engine.background_status().unwrap(), status);

        // Larger limits, given to a manual compaction, resolve it; the next
        // pass succeeds and clears the failure.
        let mut workspace = MergeWorkspace::new();
        let wide = CompactionOptions {
            max_overlap: 1,
            ..CompactionOptions::default()
        };
        while engine.compact(wide, &mut workspace).unwrap().is_some() {}
        assert_eq!(engine.stats().runs, 2);
        engine.put(&[9], b"small").unwrap();
        engine.flush().unwrap();
        engine.wait_for_compaction().unwrap();
        let status = engine.background_status().unwrap();
        assert_eq!(status.last_failure, None);
        assert_eq!(engine.stats().level0_flushes, 0);
    }

    /// A panic stops the worker for good and is reported; the engine keeps
    /// working without it and is still dropped cleanly.
    #[test]
    fn a_panicked_worker_is_reported_and_the_engine_stays_usable() {
        let engine = Engine::new(background(1)).unwrap();
        engine.core.panic_worker.store(true, Ordering::Relaxed);
        overlapping_runs(&engine);
        assert_eq!(
            engine.wait_for_compaction(),
            Err(BackgroundError::Panicked("injected worker failure".into()))
        );
        let status = engine.background_status().unwrap();
        assert!(!status.running);
        assert_eq!(status.panic.as_deref(), Some("injected worker failure"));

        engine.put(b"k", b"v").unwrap();
        engine.flush().unwrap();
        assert_eq!(
            engine.wait_for_compaction(),
            Err(BackgroundError::Panicked("injected worker failure".into()))
        );
        engine
            .compact(options(8, usize::MAX), &mut MergeWorkspace::new())
            .unwrap()
            .unwrap();
        assert_eq!(value(&engine, b"k").unwrap(), b"v");
        let core = Arc::clone(&engine.core);
        drop(engine);
        assert_eq!(Arc::strong_count(&core), 1);
    }

    #[test]
    fn an_engine_without_a_worker_says_so() {
        let engine = Engine::new(manual()).unwrap();
        assert_eq!(engine.background_status(), None);
        assert_eq!(engine.wait_for_compaction(), Err(BackgroundError::Disabled));
    }
}

#[cfg(test)]
mod subcompaction_tests;
