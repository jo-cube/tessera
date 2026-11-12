//! A RAM-first key-value engine over memtables and runs, RAM-only unless
//! opened with a checkpoint or a write-ahead log.
//!
//! An [`Engine`] owns one active memtable, frozen memtables awaiting flush,
//! the published immutable runs, and the sequence counter:
//!
//! ```
//! use tessera::engine::{Engine, EngineOptions, EngineWorkspace};
//!
//! let engine = Engine::new(EngineOptions::default());
//! let mut workspace = EngineWorkspace::new();
//! engine.put(b"apple", b"red")?;
//! engine.flush()?;
//! engine.put(b"apricot", b"orange")?;
//!
//! assert_eq!(engine.get(b"apple", &mut workspace)?.unwrap().value, b"red");
//!
//! let reader = engine.read();
//! let mut scan = reader.scan_prefix(b"ap", &mut workspace);
//! assert_eq!(scan.next_record()?.unwrap().key, b"apple");
//! assert_eq!(scan.next_record()?.unwrap().key, b"apricot");
//! assert_eq!(scan.next_record()?, None);
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! ```
//!
//! # Consistency
//!
//! All state sits behind one reader-writer lock. A RAM-only write holds it
//! exclusively while taking its sequence number and installing the update. A
//! durable write reserves its number under the lock, logs and syncs without
//! it, and installs it under the lock again; batches are installed whole and
//! in order. In both modes sequence order is visibility order and each
//! successful write has one position in it, and a write is visible before it
//! returns. A read holds the lock shared, so reads and writes of a key are
//! linearizable.
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
//! compaction builds without the lock, leaves the engine usable, and so does a panic in the background worker, which stops
//! it. [`Engine::new`] panics if the worker's thread cannot be created.
//!
//! # Snapshots
//!
//! [`Engine::snapshot`] takes, under the exclusive lock and in constant time
//! per source, a snapshot of the active memtable and shared
//! references to the frozen tables and the published runs. The memtable is a
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
//! A write the memtables have no room for flushes them itself; see
//! [Automatic flushing](self#automatic-flushing). [`Engine::flush`] flushes
//! everything written before it on demand, for example before a checkpoint
//! or to bound what the memtables hold. Either way a flush has three stages:
//!
//! 1. The active memtable is frozen and replaced, under the lock.
//! 2. Runs are built from every frozen table without the lock, so reads and
//!    writes that fit continue.
//! 3. The runs are published and the tables they came from are removed in one
//!    step under the lock.
//!
//! Frozen tables stay readable throughout, and no reader sees their runs
//! until they replace them. Flushes run one at a time. A flush refused by the
//! run limits, or interrupted by a panic while building, leaves its tables
//! frozen, and the next flush merges them with the active table into one set
//! of runs.
//!
//! # Automatic flushing
//!
//! A write refused only because the memtables are full makes room and is
//! retried within the call, so callers never flush for room, and no error
//! means "no room yet": only a record charged more than a whole budget is
//! refused, with [`WriteError::RecordExceedsBudget`]. The write waits for any flush in progress
//! and retries; if the active table is still full it freezes it and retries
//! in the successor in the same hold of the lock, then builds and publishes
//! the flush itself before returning. So writers that run out of room
//! together wait for one flush, and only the first to find the table full
//! again starts the next. The successful path is unchanged: a write that
//! fits takes the lock once.
//!
//! A write applied once is never applied again: a retry follows only a
//! refusal, which changes nothing and uses no sequence number. Every retry
//! follows a flush that published, and a flush the run limits refuse ends
//! the write with [`UpdateError::FlushRefused`], so a write never spins
//! without progress. Writers running beside it can fill each new table
//! first, so the number of flushes one write waits for is not bounded.
//!
//! A refused flush (see [Memory](self#memory)) is remembered until a flush
//! or compaction publishes or the run limits change, which is when it could
//! succeed; until then a write needing room fails at once with
//! [`UpdateError::FlushRefused`] instead of building the same flush again.
//! Writes are not held waiting for compaction: the caller can
//! [`Engine::wait_for_compaction`] or [`Engine::compact`] and retry. A record
//! larger than a whole budget ([`WriteError::RecordExceedsBudget`]) and every
//! other refusal are returned as they are.
//!
//! Durable writes do the same in their batch's leader, which freezes for room
//! while it holds the log and builds the flush after handing leadership on;
//! see `src/engine/group.rs`.
//!
//! # Compaction
//!
//! Compaction is explicit unless [background
//! compaction](self#background-compaction) is configured. [`Engine::compact`]
//! does one in three stages, like a flush: inputs are selected under the
//! shared lock, merged without any lock, and replaced by the output in one
//! step under the exclusive lock. Readers see either all of the inputs or all
//! of the output. Compactions run one at a time; flushes, reads and writes
//! continue beside them.
//!
//! Only the newest version of each key is kept, which is correct because no
//! reader can ask for an older one. A tombstone is dropped only if no run
//! outside the inputs could hold an older value under its key. Runs published
//! after selection come from flushes and are newer than every input, and the
//! memtables are newer still, so neither can be harmed by the replacement.
//! Publication checks that every input is still published, and a compaction
//! that fails at any stage changes nothing.
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
//! its failure. A panic in the worker stops it for good and is reported the
//! same way; the engine stays usable without it.
//!
//! Dropping the engine stops the worker after the compaction it is running,
//! which is published, and joins the thread. The thread holds the engine's
//! shared core, not the engine, so nothing keeps the engine alive.
//!
//! # Memory
//!
//! Memtables are limited to two budgets in all: the active table and at most
//! one frozen table, or several left by refused flushes, whose charge lowers
//! the active table's budget until a flush removes them. When the active
//! table is full a write flushes it. A table's charge includes the nodes and values
//! its writes copied or replaced while a snapshot still shares them
//! ([`MemoryUsage::pinned`]), so the two budgets bound the memtables together
//! with the history their snapshots pin, and a long-lived snapshot makes
//! flushes come sooner rather than letting memory grow.
//!
//! Published runs are limited by a count and a byte total. Once a flush would
//! exceed either limit it is rejected, and writes needing room fail with
//! [`UpdateError::FlushRefused`]. Compaction can make room, and
//! [`Engine::set_run_limits`] raises the limits.
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
//! more runs or bytes than both the limits, or what it replaces.
//!
//! When flushes outpace compaction, the run limits reject flushes, the
//! memtables fill and writes are rejected with [`UpdateError::FlushRefused`].
//! No write waits for compaction on its own: a refused flush asks the worker
//! for a pass, and a caller can [`Engine::wait_for_compaction`] and retry.
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
//! beside inputs that snapshots keep. That is the backpressure: runs then
//! accumulate until flushes and so writes are refused, until snapshots
//! are dropped. Each reader's [`EngineWorkspace`] adds one decoded block per
//! run it reads.
//!
//! These limits use the memtable's estimated charges, stored run lengths and
//! an estimate of workspace. They are not a limit on process memory.
//!
//! # Checkpoints
//!
//! [`Engine::checkpoint`] writes one snapshot's state to a directory and
//! [`Engine::recover`] restores it. See [`CheckpointOptions`] and the format,
//! publication and recovery contract in the source of `checkpoint.rs`.
//!
//! # Durability
//!
//! An engine from [`Engine::create`] or [`Engine::open`] logs every write to
//! a write-ahead log in its directory, synced before the write is applied,
//! and [`Engine::durable_checkpoint`] bounds the log. Other engines touch no
//! file. See [`DurableOptions`] and the contract in the source of `wal.rs`.
//!
//! # Shutdown
//!
//! There is no close call: dropping the engine is shutdown. It stops the
//! background worker after the compaction it is running and waits for it,
//! then releases the directory's lock. Nothing is written on drop. A RAM-only
//! engine's state is gone unless a checkpoint holds it; a durable engine
//! needs nothing more, as every acknowledged write is already in the log,
//! though a [`Engine::durable_checkpoint`] first shortens the replay of the
//! next [`Engine::open`]. Snapshots may outlive the engine and keep what
//! they read alive until dropped.

mod checkpoint;
mod group;
mod wal;
mod worker;

#[cfg(test)]
mod staged_tests;

pub use checkpoint::{CheckpointError, CheckpointOptions, CheckpointReport, Damage, RecoveryError};
pub use wal::{
    DurableOptions, Inspection, LogDamage, LogPosition, LogStatus, OpenReport, TornTail, Verdict,
};
pub use worker::{BackgroundError, BackgroundStatus};

pub use crate::block::{Corruption as BlockCorruption, MAX_KEY_LEN, MAX_VALUE_LEN};
pub use crate::memtable::{ENTRY_OVERHEAD, MemoryUsage};
pub use crate::run::{
    CompactError, CompactStats, ConflictingCopies, CorruptRun, DictionaryChoice, LiveRecord,
    LiveValue, MergeWorkspace, ReadError, RunCorruption,
};

use std::fmt;
use std::sync::{
    Arc, Mutex, MutexGuard, PoisonError, RwLock, RwLockReadGuard, RwLockWriteGuard, Weak,
};

use crate::block::{Record, Value, Version};
use crate::memtable::{
    FrozenMemtable, Memtable, Records, TARGET_RUN_LEN, TableError, flush_merged,
};
use crate::run::{
    Codec, CompactLimits, DICTIONARY_LEN, DICTIONARY_WORKSPACE, LogicalCursor, LogicalWorkspace,
    MAX_ENGINE_BLOCK_LEN, MIN_DICTIONARY_INPUT, MIN_SAVING_PERCENT, Run, RunView, TARGET_BLOCK_LEN,
    compact, latest_in,
};

type SharedRun = Arc<Published>;

/// A published run.
#[derive(Debug)]
struct Published {
    run: Run<Vec<u8>>,
    /// Whether this was the last run of its flush or compaction, the only one
    /// that can be below the target length.
    tail: bool,
}

/// Wraps the runs of one flush or compaction, in key order, for publication.
fn published(runs: Vec<Run<Vec<u8>>>) -> impl Iterator<Item = SharedRun> {
    let last = runs.len().saturating_sub(1);
    runs.into_iter().enumerate().map(move |(index, run)| {
        Arc::new(Published {
            run,
            tail: index == last,
        })
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
    /// Charged bytes each memtable may hold. The active table and the tables
    /// being flushed share twice this. A record charged more is refused with
    /// [`WriteError::RecordExceedsBudget`].
    pub memtable_budget: usize,
    /// Bytes of memtable records per run a flush writes, so that a large
    /// flush writes several runs with disjoint key ranges. Values above
    /// 1 GiB are treated as 1 GiB; a run holds at least one record.
    pub target_run_len: usize,
    /// The most runs that may be published.
    pub max_runs: usize,
    /// The most bytes of published runs: their stored bytes, key filters
    /// and prepared dictionaries together.
    pub max_run_bytes: usize,
    /// The sequence number a new engine continues after: the first write
    /// uses the next. [`Engine::new`] and [`Engine::create`] use it;
    /// [`Engine::open`] ignores it, as the checkpoint and log decide; and
    /// [`Engine::recover`] continues after the larger of it and the
    /// checkpoint's ceiling.
    pub last_seq: u64,
    /// Starts a background worker that compacts with these options whenever
    /// a flush publishes runs. See [Background compaction](self#background-compaction).
    pub background_compaction: Option<CompactionOptions>,
    /// Limits on [`Engine::snapshot`].
    pub snapshots: SnapshotLimits,
    /// How the runs this engine builds store their blocks. See
    /// [`Compression`].
    pub compression: Compression,
    /// Bytes of records per block, before compression, in the runs that
    /// flushes, compactions and checkpoints build: 16 KiB by default. Every
    /// run read decodes one block, so smaller blocks make lookups and the
    /// first record of a scan cheaper, and cost memory: each block compresses
    /// on its own and adds an index entry and a frame header (see
    /// `docs/benchmarks.md`). Blocks are cut at record boundaries, so a
    /// record larger than this is a block of its own. Values above 64 KiB are
    /// treated as 64 KiB, which keeps compression contexts within the
    /// compaction workspace estimate. Any smaller value is valid, down to a
    /// block per record, at about 20 bytes of index and frame header a
    /// block.
    ///
    /// Each block records its own length, so an engine reads runs of every
    /// block length whatever this is set to, as with [`Self::compression`]:
    /// existing runs and checkpoints keep their blocks until compaction or a
    /// checkpoint rewrites their records.
    pub target_block_len: usize,
}

impl Default for EngineOptions {
    /// Provisional limits: 64 MiB memtables, 64 runs and 4 GiB of runs, no
    /// background compaction, the default snapshot limits, and Zstandard in
    /// 16 KiB blocks.
    fn default() -> Self {
        Self {
            memtable_budget: 64 << 20,
            target_run_len: TARGET_RUN_LEN,
            max_runs: 64,
            max_run_bytes: 4 << 30,
            last_seq: 0,
            background_compaction: None,
            snapshots: SnapshotLimits::default(),
            compression: Compression::default(),
            target_block_len: TARGET_BLOCK_LEN,
        }
    }
}

/// How the runs that flushes, compactions and checkpoints build store their
/// blocks: the trade between memory and the cost of reading a run.
///
/// Every read of a run, a lookup or a scan's first record in it, decodes a
/// block ([`EngineOptions::target_block_len`], 16 KiB by default), so the
/// codec sets most of a run read's cost; reading from
/// the memtables costs the same under all three. In the measurements in
/// `docs/benchmarks.md`, a lookup in a run took about 8 µs
/// with Zstandard, 2 µs with LZ4 and 0.5 µs uncompressed, and runs of
/// JSON-like values held 26%, 45% and 100% of their logical size.
///
/// Each block records its codec, so an engine reads runs of every codec
/// whatever this is set to: a checkpoint or log written under one setting
/// recovers under another, and compaction rewrites runs it merges with the
/// current one. [`EngineOptions::max_run_bytes`] and
/// [`CheckpointOptions::max_bytes`] count stored bytes, so less compression
/// reaches them sooner: after a switch to a less compact setting, a
/// compaction whose rewritten output would exceed `max_run_bytes` is
/// refused with [`CompactionError::RunLimit`] and changes nothing.
///
/// # Dictionaries
///
/// [`Self::ZstdDictionary`] is Zstandard whose rewrites train a
/// dictionary for their output. Small blocks compress poorly alone, and a
/// dictionary learned from the data restores most of what they lose, so it
/// is meant to pair with small blocks: it was measured with
/// [`EngineOptions::target_block_len`] at 4 KiB, which is not set for you.
/// On JSON-like values through the engine (`docs/benchmarks.md`), 4 KiB
/// blocks with a dictionary held 10% less run memory than 16 KiB plain
/// blocks and 27% less than 4 KiB plain blocks, and lookups that reached a
/// run took 3.9–5.7 µs, against 13.8 µs at 16 KiB and 5.9–6.0 µs at 4 KiB
/// plain. Within that range a lookup is slower the more distinct
/// dictionaries the runs use (each compaction trains its own) and the less
/// of their tables is in cache. On small repetitive records they gained
/// nothing, and the check below leaves those plain.
///
/// Only runs that are rewritten get a dictionary. Runs that no compaction
/// merges, such as the disjoint runs of keys written in ascending order,
/// stay plain until a checkpoint rewrites them.
///
/// - Flushes and the memtable's records are always plain Zstandard: there is
///   no rewrite to learn from.
/// - A compaction, and a checkpoint, first sample decoded blocks of the runs
///   they rewrite (about 1.6 MiB, read evenly across them) and train one
///   16 KiB dictionary, outside the engine's state lock. One block in eight
///   is held back, and the dictionary is used for every run the rewrite
///   builds only if it compresses those blocks at least 10% smaller than
///   plain Zstandard at the same block length. Otherwise the output is plain.
///   Rewrites of less than about 6.4 MiB decoded are not sampled at all.
///   This is decided before any output is built and is never retried.
/// - A compaction reserves about 21 MiB for training beside the merge
///   (about 13 MiB was measured), which
///   [`CompactionOptions::max_workspace_bytes`] must admit; a compaction
///   whose limit leaves less room builds plain output rather than failing.
///   A checkpoint applies no such limit. Each dictionary run stores its
///   16 KiB dictionary and holds a prepared form of about 43 KiB beside it,
///   which counts towards [`EngineOptions::max_run_bytes`] like a key
///   filter: the runs of one rewrite share it but each counts it in full,
///   and a recovered run prepares its own. Training takes roughly 0.05 s
///   per rewrite.
/// - [`CompactStats::dictionary`] and [`CheckpointReport::dictionary`] report
///   each choice.
///
/// A dictionary run is written in a newer run format, which builds before it
/// cannot read; plain runs are unchanged, and runs of every kind recover and
/// mix whatever this is set to.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum Compression {
    /// Zstandard at level 3: the densest, and the most CPU per block read.
    #[default]
    Zstd,
    /// LZ4: about 60% of Zstandard's density on structured values, at about
    /// a quarter of its decode cost.
    Lz4,
    /// Blocks are stored as they are and read in place, with no decode. A
    /// block's checksum is then verified only when its run is loaded from a
    /// checkpoint, not on every read.
    Uncompressed,
    /// Zstandard, and compactions and checkpoints compress the runs they
    /// build with a dictionary trained for them when that saves enough. See
    /// [Dictionaries](Self#dictionaries).
    ZstdDictionary,
}

impl Compression {
    fn codec(self) -> Codec {
        match self {
            Self::Zstd | Self::ZstdDictionary => Codec::Zstd,
            Self::Lz4 => Codec::Lz4,
            Self::Uncompressed => Codec::None,
        }
    }
}

// The documentation above states these.
const _: () = assert!(DICTIONARY_LEN == 16 << 10 && MIN_SAVING_PERCENT == 10);
const _: () = assert!(MIN_DICTIONARY_INPUT == 6_553_600 && DICTIONARY_WORKSPACE <= 22 << 20);

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

/// Why a write was refused. It was not applied and used no sequence number,
/// and reads see no change, though after [`Self::LogFailed`] the record may
/// survive in the log. A flush the write started may have frozen the active
/// table.
///
/// The variants fall into three kinds:
///
/// - **Permanent for this write**: [`Self::Rejected`]. Retrying the same
///   write cannot succeed.
/// - **Backpressure**, which clears once the engine is maintained:
///   [`Self::FlushRefused`] (compact, or raise the run limits),
///   [`Self::LogFull`] (take a [durable checkpoint](Engine::durable_checkpoint)).
///   Nothing was applied, and the write can be retried.
/// - **Terminal for this engine**: [`Self::LogFailed`] and [`Self::LogClosed`]
///   (reopen the database) and [`Self::SequencesExhausted`]. Reads keep
///   working.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum UpdateError {
    /// The write can never be applied: its key or value is too long, or its
    /// record exceeds a whole memtable budget.
    Rejected(WriteError),
    /// The memtables are full and the flush that would make room is refused
    /// by the run limits, now or since they or the runs last changed.
    /// Compaction or [`Engine::set_run_limits`] can make room, after which
    /// the write can be retried.
    FlushRefused(FlushError),
    /// Every sequence number has been used, which only an engine started
    /// near `u64::MAX` ([`EngineOptions::last_seq`]) can reach.
    SequencesExhausted,
    /// Logging this write failed (its append or sync, or the thread logging
    /// its batch panicked: [`std::io::ErrorKind::Other`]), so it was not
    /// applied, but its record may survive and be recovered: its outcome is
    /// uncertain until the database is reopened. The log is now closed. See
    /// "Durable writes" in `src/engine/wal.rs`.
    LogFailed(std::io::ErrorKind),
    /// An earlier log failure closed the log: nothing was logged. Reopen the
    /// database to write again.
    LogClosed,
    /// The log would exceed [`DurableOptions::max_log_bytes`]: nothing was
    /// logged. [`Engine::durable_checkpoint`] frees segments.
    LogFull {
        /// Bytes the log would reach.
        bytes: u64,
        /// The limit.
        max_bytes: u64,
    },
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
            Self::LogFailed(kind) => write!(
                f,
                "logging the write failed ({kind}), so its outcome is uncertain; \
                 the log is closed until the database is reopened"
            ),
            Self::LogClosed => {
                f.write_str("the log closed after an earlier failure; reopen the database to write")
            }
            Self::LogFull { bytes, max_bytes } => write!(
                f,
                "the log would reach {bytes} bytes, over the limit of {max_bytes}; \
                 a durable checkpoint frees space"
            ),
        }
    }
}

impl std::error::Error for UpdateError {}

/// Publishing a flush would exceed the run limits. Its frozen table remains
/// readable, and a later flush retries it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FlushError {
    /// Runs that would be published.
    pub runs: usize,
    /// The run count limit.
    pub max_runs: usize,
    /// Stored and filter bytes of the runs that would be published.
    pub run_bytes: usize,
    /// The limit on those bytes.
    pub max_run_bytes: usize,
}

impl fmt::Display for FlushError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "flush would publish {} runs of {} bytes, over the limits of {} runs and {} bytes",
            self.runs, self.run_bytes, self.max_runs, self.max_run_bytes
        )
    }
}

impl std::error::Error for FlushError {}

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
    /// Bytes the published runs hold beside their stored bytes: their key
    /// filters. A flush builds no dictionary.
    pub held_bytes: usize,
    /// The most bytes one run's filter construction held for key hashes,
    /// freed when that run was finished.
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
    /// Bytes the published runs hold in memory beside `run_bytes`: their key
    /// filters and any prepared dictionaries. [`EngineOptions::max_run_bytes`]
    /// limits the two together. The runs one rewrite builds share a prepared
    /// dictionary and each counts it in full, so this can overstate the
    /// memory held by one dictionary (about 43 KiB) per such run.
    pub held_bytes: usize,
    /// The most published runs whose key ranges share one key: the most
    /// blocks a point read of a key absent from the memtables decodes.
    pub max_overlap: usize,
    /// Live snapshots.
    pub snapshots: usize,
    /// Memtables and runs the engine has released that snapshots still hold,
    /// or that an [`Engine::get`] in progress is still reading.
    pub retained_sources: usize,
    /// Bytes kept only for snapshots: the charged or stored bytes of those
    /// sources with any runs' filters, and the history pinned in the engine's
    /// memtables, which `active` and `frozen` also report as
    /// [`MemoryUsage::pinned`].
    pub retained_bytes: usize,
    /// The part of `retained_bytes` that is runs: stored bytes of runs that
    /// compaction replaced and snapshots still hold.
    pub retained_run_bytes: usize,
    /// The part of `retained_bytes` that is those runs' filters and prepared
    /// dictionaries.
    pub retained_held_bytes: usize,
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
    /// The same runs in the order lookups read them: largest sequence number
    /// first, so that a version found early lets `latest_in` skip the runs
    /// holding only older ones. Rebuilt by [`State::order_runs`] whenever
    /// `runs` changes; `runs` keeps publication order, which compaction
    /// selection uses to break ties.
    lookup_order: Vec<SharedRun>,
    run_bytes: usize,
    held_bytes: usize,
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
    refused: Option<FlushError>,
    /// Flushes published, and their stored bytes.
    flushes: u64,
    flushed_bytes: u64,
}

/// A source the engine released, with the bytes it accounts for.
struct Retired {
    source: Weak<dyn Send + Sync>,
    bytes: usize,
    /// For a run, its filter and prepared dictionary bytes.
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
    /// Rebuilds `lookup_order` from `runs`. Publication is rare and runs are
    /// few, so this sorts a fresh copy rather than maintaining one.
    fn order_runs(&mut self) {
        self.lookup_order.clone_from(&self.runs);
        self.lookup_order
            .sort_by_key(|run| std::cmp::Reverse(run.run.max_seq()));
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

    fn frozen_charged(&self) -> usize {
        self.frozen.iter().map(|table| table.memory().charged).sum()
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
    /// Held by a checkpoint, so that checkpoints run one at a time.
    checkpointing: Mutex<()>,
    /// The log and directory lock of a durable engine, set once it is open.
    /// Unset, writes touch no file.
    durable: std::sync::OnceLock<wal::Durable>,
    memtable_budget: usize,
    target_run_len: usize,
    /// The codec of the runs this engine builds.
    codec: Codec,
    /// Whether compactions and checkpoints may train dictionaries.
    dictionary: bool,
    /// The block length, before compression, of the runs this engine builds.
    target_block_len: usize,
    snapshot_limits: SnapshotLimits,
    /// Cloned by every snapshot, so that its count is the live snapshots.
    snapshots: Arc<()>,
    /// Present when a background worker compacts this engine.
    background: Option<worker::Signal>,
    /// Makes the worker panic at the start of its next pass.
    #[cfg(test)]
    panic_worker: std::sync::atomic::AtomicBool,
    /// Runs between building a compaction and returning it.
    #[cfg(test)]
    after_build: Mutex<Option<Arc<dyn Fn() + Send + Sync>>>,
}

/// A key-value engine. See the [module documentation](self) for its
/// consistency, flushing, compaction and persistence rules.
pub struct Engine {
    core: Arc<Core>,
    /// Joined when the engine is dropped.
    worker: Option<std::thread::JoinHandle<()>>,
}

impl Engine {
    /// Creates an empty engine, starting a background compaction worker if
    /// [`EngineOptions::background_compaction`] is set.
    pub fn new(options: EngineOptions) -> Self {
        Self::start(Arc::new(Core::new(&options)), &options)
    }

    /// Wraps `core`, starting its worker if `options` ask for one.
    fn start(core: Arc<Core>, options: &EngineOptions) -> Self {
        let worker = options
            .background_compaction
            .map(|compaction| worker::start(Arc::clone(&core), compaction));
        Self { core, worker }
    }

    /// Sets `key` to `value` and returns the update's sequence number.
    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<u64, UpdateError> {
        self.core.put(key, value)
    }

    /// Deletes `key` and returns the update's sequence number.
    pub fn delete(&self, key: &[u8]) -> Result<u64, UpdateError> {
        self.core.delete(key)
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
    /// time, and to copy one pointer per run and frozen table; see
    /// [Snapshots](self#snapshots). Snapshots taken with nothing written
    /// between them share the same view of the active table.
    ///
    /// Refused, changing nothing, when a [`SnapshotLimits`] would be exceeded.
    pub fn snapshot(&self) -> Result<Snapshot, SnapshotError> {
        self.core.snapshot()
    }

    /// Flushes everything written before this call: the active table and any
    /// frozen tables left by a refused flush are merged into one set of runs
    /// and published in place of them. Waits for any flush in progress. An
    /// engine with nothing in memory does nothing.
    ///
    /// Writes never need this to make room. Refused with a [`FlushError`],
    /// leaving the tables frozen and readable, if publishing would exceed
    /// the run limits.
    pub fn flush(&self) -> Result<FlushReport, FlushError> {
        self.core.flush()
    }

    /// Compacts once, building in `workspace`, or returns `None` if nothing
    /// needs it. Waits for any compaction in progress, including the
    /// background worker's.
    ///
    /// If some key lies in the ranges of more than
    /// [`CompactionOptions::max_overlap`] runs, the runs covering the most
    /// deeply covered key are the candidates. The fewest smallest of them
    /// that bring the overlap there within the allowance are taken, then each
    /// next smallest no larger than all taken so far, within the input limits.
    /// A large run is so rewritten only once comparable data has accumulated
    /// over it, not each time a small run arrives. The runs taken all share
    /// that key, so the merge lowers the overlap there and, since its output
    /// is disjoint and stays within their combined range, raises it nowhere.
    ///
    /// If no two runs overlap and there are more than
    /// [`CompactionOptions::disjoint_runs_above`], neighbouring tails are
    /// merged instead: a tail is the last run of a flush or compaction, the
    /// only kind that can be below the target length. The window of adjacent
    /// tails with the most runs that fits, then the fewest bytes, is taken.
    /// It leaves one tail in place of several. Full-length disjoint runs are
    /// never rewritten.
    ///
    /// Each kind of merge reduces a count that the other cannot raise, so
    /// compacting repeatedly with the same options and no new flushes stops.
    ///
    /// The merge runs without the state lock: reads, writes and flushes
    /// proceed beside it, and its output is published in one step. Inputs a
    /// snapshot holds stay allocated until it is dropped.
    ///
    /// Repeated until `None` with a [`CompactionOptions::max_overlap`] of 1,
    /// this consolidates after ingestion slows: then no key lies in more than
    /// one run and a lookup reads at most one run block, within the same input
    /// limits (a merge beyond them fails instead). Runs that overlap no other
    /// are rewritten only by the disjoint-tail rule.
    pub fn compact(
        &self,
        options: CompactionOptions,
        workspace: &mut MergeWorkspace,
    ) -> Result<Option<CompactionReport>, CompactionError> {
        self.core.compact(options, workspace)
    }

    /// Replaces the limits on published runs.
    pub fn set_run_limits(&self, max_runs: usize, max_run_bytes: usize) {
        self.core.set_run_limits(max_runs, max_run_bytes)
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
                lookup_order: Vec::new(),
                run_bytes: 0,
                held_bytes: 0,
                max_runs: options.max_runs,
                max_run_bytes: options.max_run_bytes,
                retired: Vec::new(),
                last_view: None,
                refused: None,
                flushes: 0,
                flushed_bytes: 0,
            }),
            gate: Mutex::new(()),
            flushing: Mutex::new(()),
            compacting: Mutex::new(()),
            checkpointing: Mutex::new(()),
            durable: std::sync::OnceLock::new(),
            memtable_budget: options.memtable_budget,
            target_run_len: options.target_run_len,
            codec: options.compression.codec(),
            dictionary: options.compression == Compression::ZstdDictionary,
            target_block_len: options.target_block_len.min(MAX_ENGINE_BLOCK_LEN),
            snapshot_limits: options.snapshots,
            snapshots: Arc::new(()),
            background: options.background_compaction.map(|_| worker::Signal::new()),
            #[cfg(test)]
            panic_worker: std::sync::atomic::AtomicBool::new(false),
            #[cfg(test)]
            after_build: Mutex::new(None),
        }
    }

    fn put(&self, key: &[u8], value: &[u8]) -> Result<u64, UpdateError> {
        self.update(key, Value::Bytes(value))
    }

    fn delete(&self, key: &[u8]) -> Result<u64, UpdateError> {
        self.update(key, Value::Tombstone)
    }

    fn update(&self, key: &[u8], value: Value<'_>) -> Result<u64, UpdateError> {
        if let Some(durable) = self.durable.get() {
            return self.update_durable(durable, key, value);
        }
        // Bound first, so the state lock is released before making room.
        let written = self.write(&mut self.write_state(), key, value);
        match written {
            Err(Refusal::Full) => self.write_making_room(key, value),
            written => written.map_err(Refusal::into_error),
        }
    }

    /// A RAM-only write the memtables had no room for. See
    /// [Automatic flushing](self#automatic-flushing).
    #[cold]
    fn write_making_room(&self, key: &[u8], value: Value<'_>) -> Result<u64, UpdateError> {
        loop {
            let serial = self.serialize_flush();
            let mut state = self.write_state();
            // A flush that finished while this waited may have made room.
            match self.write(&mut state, key, value) {
                Err(Refusal::Full) => {}
                written => return written.map_err(Refusal::into_error),
            }
            let job = match self.begin_making_room(serial, &mut state) {
                Ok(Some(job)) => job,
                Ok(None) => {
                    drop(state);
                    unreachable!("an empty memtable has room for any record within its budget")
                }
                Err(refused) => return Err(UpdateError::FlushRefused(refused)),
            };
            // Fits the new active table unless tables were already frozen.
            let written = self.write(&mut state, key, value);
            drop(state);
            let flushed = job.build().publish();
            match written {
                Err(Refusal::Full) => flushed.map_err(UpdateError::FlushRefused)?,
                written => return written.map_err(Refusal::into_error),
            };
        }
    }

    /// Freezes the active table for durable writes the memtables refused,
    /// returning the flush of every frozen table, which its batch's leader
    /// publishes after committing the batch. See
    /// [Automatic flushing](self#automatic-flushing).
    fn freeze_for_room(&self) -> Result<Option<FlushJob<'_>>, FlushError> {
        let serial = self.serialize_flush();
        self.begin_making_room(serial, &mut self.write_state())
    }

    /// Waits for any flush in progress and holds off others until the guard
    /// is dropped. The mutex guards no data, so a panic elsewhere leaves
    /// nothing to distrust, and the flush stays retryable.
    fn serialize_flush(&self) -> MutexGuard<'_, ()> {
        self.flushing.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Holding the flush guard `serial`, freezes the active table and returns a
    /// flush of every frozen table, or `None` if nothing is in memory. Returns
    /// the last refusal instead if it still stands.
    fn begin_making_room<'e>(
        &'e self,
        serial: MutexGuard<'e, ()>,
        state: &mut State,
    ) -> Result<Option<FlushJob<'e>>, FlushError> {
        if let Some(refused) = state.refused {
            return Err(refused);
        }
        if !state.active.is_empty() {
            self.freeze_active(state);
        }
        Ok((!state.frozen.is_empty()).then(|| FlushJob {
            engine: self,
            _serial: serial,
            tables: state.frozen.clone(),
        }))
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

    /// Sorts a memtable's refusal into a lack of room, which a flush makes,
    /// and a rejection for the caller. While frozen tables hold memory, the
    /// active table's budget is lowered so that all of them stay within two
    /// budgets, so a record refused only by the lowered budget fits once a
    /// flush frees it.
    fn refusal(&self, error: TableError) -> Refusal {
        let rejected = |error| Refusal::Error(UpdateError::Rejected(error));
        match error {
            TableError::KeyTooLong { len } => rejected(WriteError::KeyTooLong { len }),
            TableError::ValueTooLong { len } => rejected(WriteError::ValueTooLong { len }),
            TableError::RecordExceedsBudget { charge, .. } if charge > self.memtable_budget => {
                rejected(WriteError::RecordExceedsBudget {
                    charge,
                    budget: self.memtable_budget,
                })
            }
            TableError::RecordExceedsBudget { .. } | TableError::BudgetExceeded { .. } => {
                Refusal::Full
            }
            TableError::SequenceNotIncreasing { .. } => {
                unreachable!("the engine assigns, and the log checks, increasing numbers")
            }
        }
    }

    /// The active table's budget beside frozen tables charged `frozen` bytes.
    fn active_budget(&self, frozen: usize) -> usize {
        let total = self.memtable_budget.saturating_mul(2);
        self.memtable_budget.min(total.saturating_sub(frozen))
    }

    /// Freezes the active table and replaces it, in constant time.
    fn freeze_active(&self, state: &mut State) {
        let frozen = state.frozen_charged() + state.active.memory().charged;
        let successor = Memtable::new(self.active_budget(frozen), Some(state.last_seq));
        let table = std::mem::replace(&mut state.active, successor).freeze();
        state.frozen.push(Arc::new(table));
    }

    fn get<'w>(
        &self,
        key: &[u8],
        workspace: &'w mut EngineWorkspace,
    ) -> Result<Option<LiveValue<'w>>, ReadError> {
        let EngineWorkspace {
            logical,
            value,
            runs,
            ..
        } = workspace;
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
        runs.clear();
        runs.extend(state.lookup_order.iter().cloned());
        drop(state);
        let found = latest_in(
            runs.iter()
                .map(|run| (run.run.view(), run.run.filter(), run.run.max_seq())),
            key,
            logical,
        )
        .map(|found| {
            found.map(|found| {
                value.clear();
                value.extend_from_slice(found.value);
                found.seq
            })
        });
        // Releases the runs at once, so that no workspace keeps one alive.
        runs.clear();
        Ok(found?.map(|seq| LiveValue { seq, value }))
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
            runs: state.lookup_order.clone(),
            last_seq: state.last_seq,
            _live: Arc::clone(&self.snapshots),
        })
    }

    fn flush(&self) -> Result<FlushReport, FlushError> {
        match self.begin_flush() {
            Some(job) => job.build().publish(),
            None => Ok(FlushReport::default()),
        }
    }

    /// Freezes the active table, unless it is empty, and starts a flush of
    /// every frozen table. Returns `None` if there is nothing to flush.
    fn begin_flush(&self) -> Option<FlushJob<'_>> {
        let serial = self.serialize_flush();
        let mut state = self.write_state();
        if !state.active.is_empty() {
            self.freeze_active(&mut state);
        }
        if state.frozen.is_empty() {
            return None;
        }
        let tables = state.frozen.clone();
        drop(state);
        Some(FlushJob {
            engine: self,
            _serial: serial,
            tables,
        })
    }

    fn compact(
        &self,
        options: CompactionOptions,
        workspace: &mut MergeWorkspace,
    ) -> Result<Option<CompactionReport>, CompactionError> {
        match self.begin_compaction(options)? {
            Some(job) => job.build(workspace)?.publish().map(Some),
            None => Ok(None),
        }
    }

    fn begin_compaction(
        &self,
        options: CompactionOptions,
    ) -> Result<Option<CompactionJob<'_>>, CompactionError> {
        // As for flushing, this mutex guards no data.
        let serial = self
            .compacting
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        let state = self.read_state();
        let bytes = state.retained().bytes;
        let max = self.snapshot_limits.max_retained_bytes;
        if bytes > max {
            return Err(CompactionError::Retained { bytes, max });
        }
        let candidates: Vec<Candidate<'_>> = state
            .runs
            .iter()
            .map(|run| {
                let range = run.run.key_range().expect(BUILT).expect(NOT_EMPTY);
                Candidate {
                    min: range.min,
                    max: range.max,
                    bytes: run.run.as_bytes().len(),
                    tail: run.tail,
                }
            })
            .collect();
        let Some(selected) = select(&candidates, options)? else {
            return Ok(None);
        };
        let min = selected.iter().map(|&index| candidates[index].min).min();
        let max = selected.iter().map(|&index| candidates[index].max).max();
        let (min, max) = (min.expect("two inputs"), max.expect("two inputs"));
        let inputs: Vec<SharedRun> = selected
            .iter()
            .map(|&index| Arc::clone(&state.runs[index]))
            .collect();
        let others = state
            .runs
            .iter()
            .zip(&candidates)
            .enumerate()
            .filter(|(index, (_, candidate))| {
                !selected.contains(index) && candidate.min <= max && candidate.max >= min
            })
            .map(|(_, (run, _))| Arc::clone(run))
            .collect();
        drop(state);
        Ok(Some(CompactionJob {
            engine: self,
            _serial: serial,
            input_bytes: inputs.iter().map(|run| run.run.as_bytes().len()).sum(),
            inputs,
            others,
            options,
        }))
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
            max_overlap: deepest_overlap(&state.runs),
            snapshots: Arc::strong_count(&self.snapshots) - 1,
            retained_sources: retained.sources,
            retained_bytes: retained.bytes,
            retained_run_bytes: retained.run_bytes,
            retained_held_bytes: retained.held_bytes,
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
#[derive(Clone, Copy, Debug)]
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

/// Returns the most runs whose inclusive key ranges share a key.
fn deepest_overlap(runs: &[SharedRun]) -> usize {
    let mut bounds: Vec<(&[u8], bool)> = runs
        .iter()
        .flat_map(|run| {
            let range = run.run.key_range().expect(BUILT).expect(NOT_EMPTY);
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
const BUILT: &str = "published runs were built by the engine";
/// Flushes publish only non-empty tables, and compaction builds a run only
/// once it has a record.
const NOT_EMPTY: &str = "published runs are not empty";

impl Drop for Engine {
    /// Stops the worker after the compaction it is running, if any, and waits
    /// for it to exit.
    fn drop(&mut self) {
        if let Some(handle) = self.worker.take() {
            self.core.background.as_ref().expect(WORKER).stop();
            // A panic has been recorded in the status already; dropping must
            // not raise it again.
            let _ = handle.join();
        }
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
            self.engine.target_block_len,
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
    /// On a [`FlushError`] the built runs are dropped and the tables stay
    /// frozen and readable.
    fn publish(self) -> Result<FlushReport, FlushError> {
        let Self { job, runs } = self;
        let report = FlushReport {
            tables: job.tables.len(),
            records: job.records(),
            runs: runs.len(),
            run_bytes: runs.iter().map(|run| run.as_bytes().len()).sum(),
            held_bytes: runs.iter().map(Run::held_bytes).sum(),
            filter_build_bytes: runs.iter().map(Run::filter_build_bytes).max().unwrap_or(0),
        };
        let mut state = job.engine.write_state();
        let total_runs = state.runs.len() + report.runs;
        let total_bytes = state.run_bytes + state.held_bytes + report.run_bytes + report.held_bytes;
        if total_runs > state.max_runs || total_bytes > state.max_run_bytes {
            let error = FlushError {
                runs: total_runs,
                max_runs: state.max_runs,
                run_bytes: total_bytes,
                max_run_bytes: state.max_run_bytes,
            };
            state.refused = Some(error);
            drop(state);
            // Compaction is what makes room. A pass that failed earlier, say
            // while snapshots retained too much, is otherwise not retried.
            job.engine.notify_worker();
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
        state.runs.extend(published(runs));
        state.order_runs();
        state.run_bytes += report.run_bytes;
        state.held_bytes += report.held_bytes;
        state.refused = None;
        state.flushes += 1;
        state.flushed_bytes += report.run_bytes as u64;
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
        let budget = job.engine.active_budget(state.frozen_charged());
        state.active.raise_budget(budget);
        // Unless a snapshot holds them, `job` has the last references, so the
        // tables are freed after this releases the lock.
        drop(state);
        job.engine.notify_worker();
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
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CompactionOptions {
    /// The most runs merged at once.
    pub max_inputs: usize,
    /// The most stored bytes of input runs.
    pub max_input_bytes: usize,
    /// The most bytes of output, stored bytes and key filters together,
    /// built and held until publication, as for
    /// [`EngineOptions::max_run_bytes`]. It counts bytes written, not
    /// allocated: the run being built grows its buffer by doubling, so it may
    /// hold up to its own size again in unused capacity until it is finished.
    pub max_output_bytes: usize,
    /// The most estimated workspace bytes: per input, a decoded copy of its
    /// largest block and a decompression context, and for the output, two
    /// copies of the largest block and a compression context. A merge whose
    /// estimate exceeds it fails before decoding anything. What the estimate
    /// leaves bounds the buffer of key hashes for the output's key filter;
    /// an output run that would need more is published without a filter.
    pub max_workspace_bytes: usize,
    /// The most runs whose key ranges may share a key. A point read decodes
    /// at most this many blocks once compaction has caught up. Values below 1
    /// are treated as 1, which merges any overlap.
    pub max_overlap: usize,
    /// How many runs are tolerated, when none overlap, before neighbouring
    /// tails are merged.
    pub disjoint_runs_above: usize,
}

impl Default for CompactionOptions {
    /// Provisional limits: 8 inputs, 256 MiB in and out, 64 MiB of
    /// workspace, an overlap of 4, and up to 16 disjoint runs.
    fn default() -> Self {
        Self {
            max_inputs: 8,
            max_input_bytes: 256 << 20,
            max_output_bytes: 256 << 20,
            max_workspace_bytes: 64 << 20,
            max_overlap: 4,
            disjoint_runs_above: 16,
        }
    }
}

/// Why a compaction changed nothing.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CompactionError {
    /// Runs need compacting, but no two fit in the input limits.
    InputLimit {
        /// Stored bytes of the two smallest runs that would be merged.
        input_bytes: usize,
        /// The limit.
        max_input_bytes: usize,
    },
    /// Merging failed or would exceed a limit. See [`CompactError`].
    Compact(CompactError),
    /// Publishing would leave more runs or run bytes than both the engine's
    /// limit and what is published now.
    RunLimit(FlushError),
    /// Snapshots retain more than [`SnapshotLimits::max_retained_bytes`], so
    /// the output could only add to it. Dropping snapshots lifts this.
    Retained {
        /// Bytes retained.
        bytes: usize,
        /// The limit.
        max: usize,
    },
}

impl From<CompactError> for CompactionError {
    fn from(error: CompactError) -> Self {
        Self::Compact(error)
    }
}

impl fmt::Display for CompactionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InputLimit {
                input_bytes,
                max_input_bytes,
            } => write!(
                f,
                "compaction needs {input_bytes} input bytes, over the limit of {max_input_bytes}"
            ),
            Self::Compact(error) => error.fmt(f),
            Self::RunLimit(error) => error.fmt(f),
            Self::Retained { bytes, max } => write!(
                f,
                "snapshots retain {bytes} bytes, over the limit of {max}; compaction waits"
            ),
        }
    }
}

impl std::error::Error for CompactionError {}

/// What a compaction replaced.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CompactionReport {
    /// Runs removed.
    pub inputs: usize,
    /// Their stored bytes.
    pub input_bytes: usize,
    /// What they held beside that: filters and prepared dictionaries.
    pub input_held_bytes: usize,
    /// Runs published in their place.
    pub outputs: usize,
    /// Their stored bytes.
    pub output_bytes: usize,
    /// What they hold beside that: filters and prepared dictionaries.
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

struct Candidate<'a> {
    min: &'a [u8],
    max: &'a [u8],
    bytes: usize,
    tail: bool,
}

/// Chooses run indices to compact, as documented on
/// [`Engine::compact`]. Quadratic in the number of runs.
fn select(
    runs: &[Candidate<'_>],
    options: CompactionOptions,
) -> Result<Option<Vec<usize>>, CompactionError> {
    let bytes = |set: &[usize]| set.iter().map(|&index| runs[index].bytes).sum::<usize>();
    // The deepest overlap is at some run's smallest key, so the stacks of runs
    // covering those keys include every deepest one.
    let mut stacks: Vec<(&[u8], Vec<usize>)> = runs
        .iter()
        .map(|point| {
            let covering = (0..runs.len())
                .filter(|&index| runs[index].min <= point.min && point.min <= runs[index].max)
                .collect();
            (point.min, covering)
        })
        .collect();
    let deepest = stacks
        .iter()
        .map(|(_, stack)| stack.len())
        .max()
        .unwrap_or(0);

    let limit = options.max_overlap.max(1);
    if deepest > limit {
        stacks.retain(|(_, stack)| stack.len() > limit);
        stacks.sort_by(|(a, x), (b, y)| (y.len(), bytes(x), *a).cmp(&(x.len(), bytes(y), *b)));
        let mut smallest_pair = usize::MAX;
        for (_, mut stack) in stacks {
            stack.sort_by_key(|&index| (runs[index].bytes, index));
            let mut picked = fill(&stack, runs, options);
            if picked.len() >= 2 {
                // Merging k runs lowers this stack by k - 1. Take the fewest
                // that bring it within the allowance, then each next run no
                // larger than all taken so far.
                let need = (stack.len() - limit + 1).clamp(2, picked.len());
                let mut total = bytes(&picked[..need]);
                let mut keep = need;
                while keep < picked.len() && runs[picked[keep]].bytes <= total {
                    total += runs[picked[keep]].bytes;
                    keep += 1;
                }
                picked.truncate(keep);
                return Ok(Some(picked));
            }
            smallest_pair = smallest_pair.min(bytes(&stack[..2]));
        }
        return Err(input_limit(smallest_pair, options));
    }
    if deepest > 1 || runs.len() <= options.disjoint_runs_above {
        return Ok(None);
    }

    // No two runs overlap. Stretches of tails with no full run between them,
    // in key order.
    let mut order: Vec<usize> = (0..runs.len()).collect();
    order.sort_by_key(|&index| runs[index].min);
    let stretches = order
        .split(|&index| !runs[index].tail)
        .filter(|stretch| stretch.len() >= 2);
    let mut best: Option<Vec<usize>> = None;
    let mut smallest_pair = None;
    for stretch in stretches {
        for start in 0..stretch.len() - 1 {
            let window = fill(&stretch[start..], runs, options);
            let key = |window: &[usize]| (window.len(), std::cmp::Reverse(bytes(window)));
            if window.len() >= 2 && best.as_deref().is_none_or(|best| key(&window) > key(best)) {
                best = Some(window);
            }
        }
        let pair = stretch.windows(2).map(bytes).min().expect("two tails");
        smallest_pair = Some(smallest_pair.map_or(pair, |smallest: usize| smallest.min(pair)));
    }
    match (best, smallest_pair) {
        (Some(window), _) => Ok(Some(window)),
        (None, Some(pair)) => Err(input_limit(pair, options)),
        (None, None) => Ok(None),
    }
}

/// Takes runs from the front of `candidates` while they fit the limits.
fn fill(candidates: &[usize], runs: &[Candidate<'_>], options: CompactionOptions) -> Vec<usize> {
    let mut picked = Vec::new();
    let mut total = 0;
    for &index in candidates {
        if picked.len() == options.max_inputs || total + runs[index].bytes > options.max_input_bytes
        {
            break;
        }
        total += runs[index].bytes;
        picked.push(index);
    }
    picked
}

fn input_limit(input_bytes: usize, options: CompactionOptions) -> CompactionError {
    CompactionError::InputLimit {
        input_bytes,
        max_input_bytes: options.max_input_bytes,
    }
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
    input_bytes: usize,
    options: CompactionOptions,
}

impl<'e> CompactionJob<'e> {
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
            .map(|run| run.run.key_range().expect(BUILT).expect(NOT_EMPTY))
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
            target_block_len: self.engine.target_block_len,
            dictionary: self.engine.dictionary,
        };
        let output = compact(&views, keep_tombstone, limits, workspace)?;
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
            .field("inputs", &self.inputs.len())
            .field("input_bytes", &self.input_bytes)
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
    /// Every input must still be published. Runs published since selection
    /// are kept. A result over the run limits is accepted if it is no larger
    /// than what it replaces. On an error the output is dropped and the engine is
    /// unchanged. The inputs are freed after the lock is released, once no
    /// reader holds them.
    fn publish(self) -> Result<CompactionReport, CompactionError> {
        let Self { job, output } = self;
        let report = CompactionReport {
            inputs: job.inputs.len(),
            input_bytes: job.input_bytes,
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
        // when the limits were lowered below what is published.
        let grows_runs = total_runs > state.max_runs.max(state.runs.len());
        let grows_bytes = total_bytes > state.max_run_bytes.max(published_bytes);
        if grows_runs || grows_bytes {
            return Err(CompactionError::RunLimit(FlushError {
                runs: total_runs,
                max_runs: state.max_runs,
                run_bytes: total_bytes,
                max_run_bytes: state.max_run_bytes,
            }));
        }
        state.runs.retain(|run| !is_input(run));
        state.runs.extend(published(output.runs));
        state.order_runs();
        state.run_bytes = stored;
        state.held_bytes = filters;
        state.refused = None;
        for input in &job.inputs {
            let (bytes, filter) = (input.run.as_bytes().len(), input.run.held_bytes());
            state.retire(
                Arc::downgrade(input) as Weak<dyn Send + Sync>,
                bytes,
                Some(filter),
            );
        }
        drop(state);
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
    /// The runs an [`Engine::get`] reads after releasing the lock; empty
    /// between calls.
    runs: Vec<SharedRun>,
}

impl EngineWorkspace {
    /// Creates an empty workspace, which allocates nothing until first used.
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns the decoded-block bytes retained across all runs.
    pub fn capacity(&self) -> usize {
        self.logical.capacity()
    }

    /// Returns how many run blocks reads through this workspace have loaded.
    pub fn blocks_loaded(&self) -> u64 {
        self.logical.blocks_loaded()
    }
}

/// A stable view of an [`Engine`], holding its lock shared.
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
        Scan::new(tables, &state.runs, prefix, workspace)
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
/// workspace each, and may outlive its engine. It cannot be refreshed: take a
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
/// # let engine = tessera::engine::Engine::new(Default::default());
/// let snapshot = engine.snapshot().unwrap();
/// let second = snapshot.clone();
/// ```
pub struct Snapshot {
    /// Newest first.
    tables: Vec<Arc<FrozenMemtable>>,
    runs: Vec<SharedRun>,
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
    resolve(in_memory, &state.lookup_order, key, logical)
}

/// Decides `key` from the newest table update, or else from the runs.
fn resolve<'r>(
    in_memory: Option<Version<'r>>,
    runs: &'r [SharedRun],
    key: &[u8],
    logical: &'r mut LogicalWorkspace,
) -> Result<Option<LiveValue<'r>>, ReadError> {
    match in_memory {
        Some(Version {
            seq,
            value: Value::Bytes(value),
        }) => Ok(Some(LiveValue { seq, value })),
        Some(_) => Ok(None),
        None => latest_in(
            runs.iter()
                .map(|run| (run.run.view(), run.run.filter(), run.run.max_seq())),
            key,
            logical,
        ),
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
        runs: &'r [SharedRun],
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

        let mut cursor = LogicalCursor::over(runs.iter().map(|run| run.run.view()), logical);
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

/// For the tests' dictionary measurements (5AG).
#[cfg(test)]
impl Engine {
    /// The published runs that store a dictionary, the distinct prepared
    /// dictionaries they hold, and the distinct dictionaries by content.
    pub(crate) fn dictionary_counts(&self) -> (usize, usize, usize) {
        let state = self.core.read_state();
        let parts: Vec<_> = state
            .runs
            .iter()
            .filter_map(|run| run.run.dictionary_parts())
            .collect();
        let mut prepared: Vec<_> = parts.iter().map(|(_, p)| Arc::as_ptr(p)).collect();
        prepared.sort_unstable();
        prepared.dedup();
        let mut contents: Vec<&[u8]> = parts.iter().map(|(bytes, _)| *bytes).collect();
        contents.sort_unstable();
        contents.dedup();
        (parts.len(), prepared.len(), contents.len())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn frozen_handle(engine: &Engine) -> Arc<FrozenMemtable> {
        let state = engine.core.read_state();
        assert_eq!(state.frozen.len(), 1);
        Arc::clone(&state.frozen[0])
    }

    /// The writer-priority rule: from the moment a writer waits for the state
    /// lock until it has it, the gate every reader must pass is closed.
    #[test]
    fn a_waiting_writer_holds_the_gate_until_it_has_the_lock() {
        let engine = Engine::new(EngineOptions::default());
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
        let engine = Engine::new(EngineOptions::default());
        engine.put(b"k", b"v").unwrap();
        let job = engine.core.begin_flush().unwrap();
        let held = frozen_handle(&engine);
        assert_eq!(Arc::strong_count(&held), 3);
        job.build().publish().unwrap();
        assert_eq!(Arc::strong_count(&held), 1);
    }

    #[test]
    fn an_abandoned_flush_releases_only_its_own_reference() {
        let engine = Engine::new(EngineOptions::default());
        engine.put(b"k", b"v").unwrap();
        let job = engine.core.begin_flush().unwrap();
        let held = frozen_handle(&engine);
        drop(job);
        assert_eq!(Arc::strong_count(&held), 2);
    }

    #[test]
    fn a_rejected_publication_keeps_the_frozen_table_in_the_engine() {
        let engine = Engine::new(EngineOptions {
            max_runs: 0,
            ..EngineOptions::default()
        });
        engine.put(b"k", b"v").unwrap();
        let job = engine.core.begin_flush().unwrap();
        let held = frozen_handle(&engine);
        job.build().publish().unwrap_err();
        assert_eq!(Arc::strong_count(&held), 2);
        assert!(Arc::ptr_eq(&held, &frozen_handle(&engine)));
    }

    fn candidate(min: &'static str, max: &'static str, bytes: usize) -> Candidate<'static> {
        Candidate {
            min: min.as_bytes(),
            max: max.as_bytes(),
            bytes,
            tail: true,
        }
    }

    fn options(max_inputs: usize, max_input_bytes: usize) -> CompactionOptions {
        CompactionOptions {
            max_inputs,
            max_input_bytes,
            max_overlap: 1,
            disjoint_runs_above: 2,
            ..CompactionOptions::default()
        }
    }

    #[test]
    fn selection_takes_the_smallest_runs_covering_the_deepest_key() {
        let runs = [
            candidate("a", "z", 50),
            candidate("b", "c", 1),
            candidate("m", "p", 2),
            candidate("n", "o", 3),
            candidate("o", "q", 4),
            candidate("x", "y", 5),
        ];
        // "o" lies in runs 0, 2, 3 and 4; everything else in at most two.
        assert_eq!(select(&runs, options(8, 100)), Ok(Some(vec![2, 3, 4, 0])));
        assert_eq!(select(&runs, options(2, 100)), Ok(Some(vec![2, 3])));
        assert_eq!(select(&runs, options(8, 49)), Ok(Some(vec![2, 3, 4])));
        // Within the overlap allowed, or the deepest stack does not fit.
        let tolerant = CompactionOptions {
            max_overlap: 4,
            ..options(8, 100)
        };
        assert_eq!(select(&runs, tolerant), Ok(None));
        let narrow = CompactionOptions {
            max_overlap: 3,
            ..options(8, 2)
        };
        assert_eq!(
            select(&runs, narrow),
            Err(CompactionError::InputLimit {
                input_bytes: 5,
                max_input_bytes: 2
            })
        );
    }

    #[test]
    fn selection_rewrites_a_large_run_only_once_comparable_data_covers_it() {
        let allow = |max_overlap| CompactionOptions {
            max_overlap,
            ..options(8, 1_000)
        };
        // Five runs over one key: a large one and four small. Two inputs bring
        // the stack within 4; the other small runs are no larger than those
        // taken, but the large one is.
        let mut runs = vec![
            candidate("a", "z", 100),
            candidate("a", "z", 1),
            candidate("b", "y", 1),
            candidate("c", "x", 1),
            candidate("d", "w", 1),
        ];
        assert_eq!(select(&runs, allow(4)), Ok(Some(vec![1, 2, 3, 4])));
        // Deeper, more inputs are needed, but the large run still waits until
        // those taken add up to it.
        runs.extend((0..3).map(|_| candidate("e", "v", 3)));
        assert_eq!(select(&runs, allow(4)), Ok(Some(vec![1, 2, 3, 4, 5, 6, 7])));
        runs.truncate(5);
        runs[0].bytes = 4;
        assert_eq!(select(&runs, allow(4)), Ok(Some(vec![1, 2, 3, 4, 0])));
        // The fewest needed are always taken, even when larger than the rest.
        runs[0].bytes = 100;
        assert_eq!(select(&runs, allow(1)), Ok(Some(vec![1, 2, 3, 4, 0])));
        assert_eq!(select(&runs, allow(2)), Ok(Some(vec![1, 2, 3, 4])));
        runs[4].bytes = 50;
        assert_eq!(select(&runs, allow(3)), Ok(Some(vec![1, 2, 3])));
        assert_eq!(select(&runs, allow(5)), Ok(None));
    }

    #[test]
    fn selection_merges_adjacent_tails_only_above_the_disjoint_threshold() {
        let mut runs = vec![
            candidate("a", "b", 5),
            candidate("c", "d", 1),
            candidate("e", "f", 1),
            candidate("g", "h", 1),
        ];
        assert_eq!(select(&runs[..2], options(8, 100)), Ok(None));
        assert_eq!(select(&runs, options(8, 100)), Ok(Some(vec![0, 1, 2, 3])));
        // Prefer more runs, then fewer bytes.
        assert_eq!(select(&runs, options(2, 100)), Ok(Some(vec![1, 2])));
        // A full run separates tails, and is never rewritten.
        runs[2].tail = false;
        assert_eq!(select(&runs, options(8, 100)), Ok(Some(vec![0, 1])));
        runs[0].tail = false;
        assert_eq!(select(&runs, options(8, 100)), Ok(None));
        // Any overlap within the allowance disables tail merging.
        runs[0].tail = true;
        runs[2] = candidate("a", "a", 1);
        let tolerant = CompactionOptions {
            max_overlap: 2,
            ..options(8, 100)
        };
        assert_eq!(select(&runs, tolerant), Ok(None));
        assert_eq!(select(&runs[..1], options(8, 100)), Ok(None));
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
        let engine = Engine::new(EngineOptions::default());
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

    /// Compaction selects the smallest runs, not adjacent ones, so its output
    /// can span a run it left: here the output holds the oldest and newest
    /// versions of "k" and the oldest of "j", and the run left between them
    /// holds the newest of "j".
    #[test]
    fn a_partial_compaction_leaves_interleaved_runs_that_read_correctly() {
        let engine = Engine::new(EngineOptions::default());
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

        let report = engine
            .compact(
                CompactionOptions {
                    max_overlap: 2,
                    max_inputs: 2,
                    ..CompactionOptions::default()
                },
                &mut MergeWorkspace::new(),
            )
            .unwrap()
            .unwrap();
        assert_eq!((report.inputs, report.outputs), (2, 1));
        assert_eq!(engine.stats().runs, 2);
        // "k": the output decides alone. "j": the output's version is older
        // than the run it spans, which is still read.
        for (value, blocks) in read_counting(&engine, b"k") {
            assert_eq!((value.as_deref(), blocks), (Some(&b"k3"[..]), 1));
        }
        for (value, blocks) in read_counting(&engine, b"j") {
            assert_eq!((value.as_deref(), blocks), (Some(&b"j2"[..]), 2));
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
        let engine = Engine::new(EngineOptions::default());
        overlapping_runs(&engine);
        let built = engine
            .core
            .begin_compaction(options(8, usize::MAX))
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
        let engine = Engine::new(EngineOptions::default());
        overlapping_runs(&engine);
        let job = engine
            .core
            .begin_compaction(options(8, usize::MAX))
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
        let engine = Engine::new(EngineOptions::default());
        overlapping_runs(&engine);
        let inputs: Vec<SharedRun> = engine.core.read_state().runs.clone();
        engine
            .compact(options(8, usize::MAX), &mut MergeWorkspace::new())
            .unwrap()
            .unwrap();
        assert!(inputs.iter().all(|run| Arc::strong_count(run) == 1));
    }

    #[test]
    fn publication_refuses_growth_past_the_run_limits_but_allows_shrinking() {
        let engine = Engine::new(EngineOptions::default());
        overlapping_runs(&engine);
        let stats = engine.stats();
        engine.set_run_limits(1, 1);
        let begin = || {
            engine
                .core
                .begin_compaction(options(8, usize::MAX))
                .unwrap()
                .unwrap()
        };
        let mut built = begin().build(&mut MergeWorkspace::new()).unwrap();
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
            ..EngineOptions::default()
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
        let engine = Engine::new(background(1));
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
        let engine = Engine::new(background(1));
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
        let engine = Engine::new(background(2));
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
        let engine = Engine::new(background(1));
        overlapping_runs(&engine);
        engine.wait_for_compaction().unwrap();
        let core = Arc::clone(&engine.core);
        assert_eq!(Arc::strong_count(&core), 3);
        drop(engine);
        assert_eq!(Arc::strong_count(&core), 1);
        let status = core.background.as_ref().unwrap().status();
        assert!(!status.running && !status.panicked);
    }

    /// Dropping the engine mid-compaction waits for that compaction, which is
    /// published, and starts no other.
    #[test]
    fn dropping_during_a_compaction_publishes_it_and_then_stops() {
        let engine = Engine::new(background(1));
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
        assert_eq!((status.running, status.compactions), (false, 1));
        assert_eq!(core.stats().runs, 1);
    }

    /// A lasting failure ends the pass and waits for the next request: it is
    /// reported, counted once per request, and cleared by a later success.
    #[test]
    fn a_failed_compaction_is_reported_once_per_request_and_recovers() {
        let engine = Engine::new(EngineOptions {
            background_compaction: Some(CompactionOptions {
                max_overlap: 1,
                max_output_bytes: 30 << 10,
                ..CompactionOptions::default()
            }),
            ..EngineOptions::default()
        });
        let mut seed = 7u64;
        let mut noise = || {
            (0..2048)
                .map(|_| {
                    seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
                    (seed >> 56) as u8
                })
                .collect::<Vec<u8>>()
        };
        for _ in 0..3 {
            for key in 0..20u8 {
                engine.put(&[key], &noise()).unwrap();
            }
            engine.flush().unwrap();
        }
        let error = engine.wait_for_compaction().unwrap_err();
        assert!(
            matches!(
                error,
                BackgroundError::Failed(CompactionError::Compact(CompactError::OutputLimit { .. }))
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
        assert_eq!(engine.stats().runs, 3);
        for _ in 0..1000 {
            std::thread::yield_now();
        }
        assert_eq!(engine.background_status().unwrap(), status);

        for key in 0..20u8 {
            engine.delete(&[key]).unwrap();
        }
        engine.flush().unwrap();
        engine.wait_for_compaction().unwrap();
        let status = engine.background_status().unwrap();
        assert_eq!(status.last_failure, None);
        assert_eq!(engine.stats().runs, 0);
    }

    /// A panic stops the worker for good and is reported; the engine keeps
    /// working without it and is still dropped cleanly.
    #[test]
    fn a_panicked_worker_is_reported_and_the_engine_stays_usable() {
        let engine = Engine::new(background(1));
        engine.core.panic_worker.store(true, Ordering::Relaxed);
        overlapping_runs(&engine);
        assert_eq!(engine.wait_for_compaction(), Err(BackgroundError::Stopped));
        let status = engine.background_status().unwrap();
        assert!(!status.running && status.panicked);

        engine.put(b"k", b"v").unwrap();
        engine.flush().unwrap();
        assert_eq!(engine.wait_for_compaction(), Err(BackgroundError::Stopped));
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
        let engine = Engine::new(EngineOptions::default());
        assert_eq!(engine.background_status(), None);
        assert_eq!(engine.wait_for_compaction(), Err(BackgroundError::Disabled));
    }
}
