//! A RAM-first key-value engine with explicit checkpoints and recovery.
//!
//! ```
//! use tessera::engine::{Engine, EngineOptions, EngineWorkspace};
//! let engine = Engine::new(EngineOptions::default())?;
//! engine.put(b"apple", b"red")?;
//! let snapshot = engine.snapshot();
//! engine.put(b"apple", b"green")?;
//! let mut workspace = EngineWorkspace::new();
//! assert_eq!(snapshot.get(b"apple", &mut workspace)?.unwrap().value, b"red");
//! engine.close()?;
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! ```
//!
//! # Consistency
//!
//! Writes and publication use one exclusive state lock; sequence order is
//! visibility order. A read takes immutable run references under the shared
//! lock and decodes after releasing it. A [`ReadGuard`] holds the shared lock
//! for several consistent reads. Drop it before calling an engine method:
//! writers and publication must acquire that lock. Writers have priority over
//! new readers. Caller-owned read workspaces are outside engine accounting.
//!
//! # Snapshots
//!
//! [`Engine::snapshot`] is infallible and holds no lock after acquisition.
//! It shares the copy-on-write memtable and immutable run directory. Later
//! writes copy only shared nodes and partial arena chunks. Snapshots retain
//! their sources through flushes, compactions, and engine closure; maintenance
//! keeps only the newest version because older readers own their own sources.
//!
//! # Flushing
//!
//! Writers rotate full memtables and a dedicated worker builds runs outside
//! the state lock. At most two frozen tables await flush. [`Engine::flush`]
//! waits for everything written before the call. Flushes always publish;
//! readers see either the tables or their replacement runs atomically.
//! Outputs end at the target run length and selected level-1 boundaries.
//!
//! # Compaction
//!
//! Background compaction is always enabled. Level 0 holds overlapping flushes;
//! deeper levels have disjoint key ranges. Dynamic level targets use
//! [`EngineOptions::level_ratio`]. Selection takes the most overfull level,
//! then level 0 at four flushes, then small neighbours. Outputs are cut at the
//! target run length and the overlap bound. Nonoverlapping runs move without
//! rewriting. Optional level-0 inputs fit an internal transient allowance;
//! the first source and its overlaps always proceed. Large level-0 jobs into
//! the bottom build disjoint pieces of about one target stored run, aligned
//! to bottom-run boundaries. Up to `background_threads` pieces build at once
//! and publish independently, in any order. Output reservations share a bound
//! of that many piece budgets (at least 128 KiB each for one block and metadata).
//! A piece reserves its inputs' stored bytes plus an eighth and per-run
//! metadata; one larger than the whole bound runs alone. A piece that outgrows
//! its reservation retries with at least the whole bound, doubling each time,
//! so the output allowance never fails a job.
//! Bounded views keep only unmerged level-0 ranges visible and share the
//! original immutable storage. Each piece retires its bottom inputs; later
//! failure preserves earlier publications. Other jobs may build output ranges
//! in parallel and publish together. No snapshot or size limit refuses a
//! compaction. Only the latest version survives; tombstones survive while an
//! unselected run could hold an older value.
//!
//! # Memory
//!
//! [`EngineOptions::memory_limit`] defaults to unlimited. [`EngineStats::used`]
//! sums charged memtables, stored runs and their held metadata, pool reserve
//! and unused checked-out capacity, retired sources, and estimates of build
//! output and workspace. Pinned history is charged once in memtables, then in
//! retired sources after publication. Shared sources are counted once.
//! Estimates and allocator overhead mean this is an admission budget, not an
//! operating-system RSS cap. An admitted record or maintenance build can
//! temporarily cross it; flushes and compactions must still finish.
//!
//! Below 90% writers proceed. Above that they wait briefly on publication;
//! at 100% they flush and request reclamation while it can make progress.
//! Reclamation drains overlapping versions and isolated tombstones even when
//! ordinary level targets are satisfied. Live data and held snapshots that
//! cannot shrink cause [`UpdateError::MemoryLimit`] without waiting for their
//! owners. Writers also wait at the internal level-0 stop (12 flushes).
//! Reads, snapshots, checkpoints and maintenance never wait on the budget.
//!
//! # Background compaction
//!
//! Publication wakes the compaction worker. [`Engine::wait_for_compaction`]
//! requests a complete pass; [`Engine::background_status`] reports progress,
//! read failures and a worker panic. A failed pass sleeps until requested
//! again. Closing joins both workers and reports a panic; it writes nothing.
//! Snapshots remain usable after closure.
//!
//! # Thread safety
//!
//! [`Engine`] and [`Snapshot`] are `Send + Sync`: share them across threads and
//! use a separate [`EngineWorkspace`] for each concurrent read. A workspace is
//! mutably borrowed for an operation and its returned values. A [`ReadGuard`]
//! is not `Send`; it holds its shared lock until dropped. Scans borrow their
//! view and workspace, and return records valid until the next cursor step.
//!
//! # Panics
//!
//! A panic under the exclusive state lock poisons the engine; subsequent
//! operations acquiring that lock panic. Build panics leave published sources
//! readable. A stopped flush worker is replaced by synchronous flushing when
//! needed; a stopped compaction worker can likewise be serviced synchronously.
//! Corrupt runs are reported by reads and maintenance.
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
//! Checkpoints use the normal flush builder, then stream immutable run chunks
//! and persisted Bloom filters once per stable run ID. Later checkpoints reuse
//! those files and report written/reused bytes and runs, plus deleted run files.
//! Levels, bounded views and sequence overrides are restored without
//! compaction. Use [`Engine::checkpoint_with_metadata`] and
//! [`Engine::recover_with_metadata`]
//! to store and recover opaque application metadata (up to 16 MiB).
//! Run format 6 stores magicless Zstandard frames and prefix-compressed index
//! keys; checkpoint manifests use version 5. Recovery validates file lengths,
//! checksums, all records, filter membership and cross-run copies before
//! starting workers. CRC-32 detects damage but does not authenticate files.
//!
//! A checkpoint is committed when the directory's `CURRENT` file is renamed
//! to name it, after its runs and manifest are synced. The previous generation
//! stays readable; GC keeps files referenced by either generation. A failed or
//! interrupted checkpoint leaves the previous one intact. Recovery checks
//! every file of exactly the committed checkpoint, with no fallback to an
//! older one, before it returns an engine. One engine or process uses a
//! directory at a time, through an advisory lock on its `LOCK` file, which
//! the kernel releases if the process dies.
//! Recovery leaves checkpoint data unchanged and may create the lock file.

mod batch;
mod checkpoint;
mod compaction;
#[cfg(feature = "diagnostics")]
#[doc(hidden)]
pub mod diagnostics;
mod directory;
mod errors;
mod flush_job;
mod flusher;
mod ingest;
mod levels;
mod multi;
mod options;
mod pieces;
mod read;
mod resources;
mod stats;
mod worker;

#[cfg(test)]
mod compaction_model_tests;
#[cfg(test)]
mod copy_forward_tests;
#[cfg(test)]
mod level_invariants_tests;
#[cfg(test)]
mod staged_tests;

#[cfg(test)]
mod stress_tests;

pub use batch::WriteBatch;
pub use checkpoint::{CheckpointError, CheckpointOptions, CheckpointReport, Damage, OpenError};
pub use compaction::CompactionError;
use compaction::{Begun, BuiltCompaction, CompactionJob, Cursor};
pub(crate) use compaction::{CompactionKind, CompactionPolicy, CompactionReport};
pub use errors::{UpdateError, WriteError};
#[cfg(test)]
use flush_job::BuiltFlush;
use flush_job::FlushJob;
pub use options::{Compression, ConfigError, EngineOptions};
pub use read::{EngineWorkspace, ReadGuard, Scan, Snapshot};
pub use stats::{EngineStats, FlushReport};
pub use worker::{BackgroundError, BackgroundStatus};

pub use crate::block::{Corruption as BlockCorruption, MAX_KEY_LEN, MAX_VALUE_LEN};
pub use crate::chunk::BuildCounters;
pub use crate::memtable::{ENTRY_OVERHEAD, MemoryUsage};
pub(crate) use crate::run::{CompactStats, MergeWorkspace};
pub use crate::run::{
    ConflictingCopies, CorruptRun, LiveRecord, LiveValue, ReadError, RunCorruption,
};

use std::collections::HashSet;
use std::fmt;
use std::sync::{
    Arc, Mutex, MutexGuard, PoisonError, RwLock, RwLockReadGuard, RwLockWriteGuard, Weak,
};

use crate::block::{Record, Value, Version};
use crate::chunk::{ChunkPool, RESERVE_BYTES};
use crate::memtable::{FlushCuts, FrozenMemtable, Memtable, Records, TableError, flush_merged};
use crate::run::{
    Codec, CompactLimits, LogicalCursor, LogicalWorkspace, OutputCuts, RAW_BLOCK_CAP,
    RECORD_INPUT_OVERHEAD, Run, RunView, SeqFloor, TARGET_BLOCK_COMPRESSED, compact_parallel_in,
    latest_from,
};

use directory::RunDirectory;
use levels::{Dest, Fit, Levels, Plan};

type SharedRun = Arc<Published>;

/// A published run.
#[derive(Debug)]
struct Published {
    id: u128,
    checkpoint_crc: std::sync::OnceLock<u32>,
    run: Run<Vec<u8>>,
    /// The decoded length of its largest block, for sizing a compaction's
    /// workspace before reading anything.
    largest_block: usize,
}

/// Physical storage is counted once even when disjoint views share it.
fn physical_runs(runs: &[SharedRun]) -> impl Iterator<Item = &SharedRun> {
    let mut seen = HashSet::with_capacity(runs.len());
    runs.iter()
        .filter(move |run| seen.insert(run.run.storage_id()))
}

/// Wraps the runs of one flush or compaction, in key order, for publication.
fn published(runs: Vec<Run<Vec<u8>>>) -> impl Iterator<Item = SharedRun> {
    runs.into_iter().map(|run| {
        let largest_block = run.largest_block();
        Arc::new(Published {
            id: checkpoint::new_run_id(),
            checkpoint_crc: std::sync::OnceLock::new(),
            run,
            largest_block,
        })
    })
}

// Internal scheduling defaults. None is an admission/refusal limit.
const TARGET_RUN_LEN: usize = crate::memtable::TARGET_RUN_LEN; // 64 MiB logical input per run.
const LEVEL0_STOP: usize = 12; // Bound read amplification while writers wait.
const _: () = assert!(
    LEVEL0_STOP == 12,
    "EngineStats::level0_flushes documents the stop"
);
const BULK_INGEST_DIVISOR: usize = 2; // Sorted ingestion at half a memtable.
const MAX_FROZEN_MEMTABLES: usize = 2; // Bound the flush backlog.
const MAX_COMPACTION_INPUTS: usize = 32; // Optional level-0 fan-in.
const COMPACTION_INPUT_BYTES: usize = 256 << 20; // Optional level-0 input.
const COMPACTION_WORKSPACE_BYTES: usize = 64 << 20; // Parallel builder allowance.
const COMPACTION_TRIGGER: usize = 4; // Flush groups that trigger level 0.
const SOFT_WAIT: std::time::Duration = std::time::Duration::from_millis(2);

struct State {
    last_seq: u64,
    active: Memtable,
    /// Oldest first. Each table's sequence numbers are above the runs' and
    /// those of the tables before it, and below `active`'s. Writers rotate
    /// tables while the flush worker builds.
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
    /// Level-0 writer stop; deterministic tests may change it.
    level0_stop: usize,
    /// Whether flushes are split at level 1's boundaries; tests turn it off
    /// to compare.
    #[cfg(test)]
    split_flushes: bool,
    /// Sources removed from the state, kept track of until no snapshot holds
    /// them. Pruned whenever a source is added.
    retired: Vec<Retired>,
    /// The latest snapshot of the active table and the `last_seq` it was
    /// taken at. Every write raises `last_seq`, so while it is unchanged the
    /// snapshot still equals the active table and can be shared.
    last_view: Option<(u64, Weak<FrozenMemtable>)>,
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

    fn recount_runs(&mut self) {
        self.run_bytes = physical_runs(&self.runs).map(|r| r.run.stored_len()).sum();
        self.held_bytes = physical_runs(&self.runs).map(|r| r.run.held_bytes()).sum();
    }

    fn retire_runs(&mut self, inputs: &[SharedRun]) {
        let live: HashSet<_> = self.runs.iter().map(|r| r.run.storage_id()).collect();
        for input in physical_runs(inputs).filter(|r| !live.contains(&r.run.storage_id())) {
            self.retire(
                input.run.retirement(),
                input.run.stored_len(),
                Some(input.run.held_bytes()),
            );
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

#[cfg(test)]
type PieceHook = Arc<dyn Fn(usize) + Send + Sync>;

/// Everything an engine shares with its background worker.
struct Core {
    chunks: ChunkPool,
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
    /// Cloned by every snapshot, so that its count is the live snapshots.
    snapshots: Arc<()>,
    memory_limit: Option<usize>,
    policy: CompactionPolicy,
    resources: resources::Progress,
    reclaim: std::sync::atomic::AtomicBool,
    /// Test fixtures can suspend the compaction worker.
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
    /// Cleared by equivalence tests to rewrite every block.
    #[cfg(test)]
    copy_forward: std::sync::atomic::AtomicBool,
    #[cfg(test)]
    piece_budget: std::sync::atomic::AtomicUsize,
    #[cfg(test)]
    fail_piece: std::sync::atomic::AtomicUsize,
    #[cfg(test)]
    before_piece: Mutex<Option<PieceHook>>,
    #[cfg(test)]
    piece_output_budget: std::sync::atomic::AtomicUsize,
    /// Replaces each concurrent piece's first request, if nonzero.
    #[cfg(test)]
    piece_request: std::sync::atomic::AtomicUsize,
    #[cfg(test)]
    before_flush_build: Mutex<Option<Arc<dyn Fn() + Send + Sync>>>,
    #[cfg(test)]
    after_ingest_build: Mutex<Option<Arc<dyn Fn() + Send + Sync>>>,
}

/// A key-value engine. See the [module documentation](self) for its
/// consistency, flushing, compaction and persistence rules.
/// It is `Send + Sync`; use one workspace per concurrent reader.
/// Methods that acquire the state lock panic if it is poisoned; see
/// [Panics](self#panics). Drop any [`ReadGuard`] before calling those methods.
pub struct Engine {
    core: Arc<Core>,
    /// Joined when the engine is dropped.
    worker: Option<std::thread::JoinHandle<()>>,
    flush_worker: Option<std::thread::JoinHandle<()>>,
}

impl Engine {
    /// Creates an empty engine and starts flush and compaction workers.
    /// Invalid options return [`OpenError::Config`]; thread creation failures
    /// return [`OpenError::Worker`].
    pub fn new(options: EngineOptions) -> Result<Self, OpenError> {
        options.validate().map_err(OpenError::Config)?;
        Self::start(Arc::new(Core::new(&options)), &options)
    }

    /// Wraps `core`, starting its worker if `options` ask for one. On failure
    /// the core is dropped with nothing running.
    fn start(core: Arc<Core>, options: &EngineOptions) -> Result<Self, OpenError> {
        #[cfg(test)]
        let enabled = options.background_compaction.is_some();
        #[cfg(not(test))]
        let enabled = {
            let _ = options;
            true
        };
        let worker = enabled
            .then(|| worker::start(Arc::clone(&core)))
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

    /// Shuts the engine down, joins its flush and compaction workers and
    /// releases its memory. In-flight builds may publish before workers stop.
    /// No new compaction starts once this begins.
    ///
    /// It consumes the engine, so no read, write, [`ReadGuard`] or other
    /// call can be in progress or follow; an engine shared through an `Arc`
    /// is closed once it is the last reference ([`Arc::into_inner`]).
    /// [`Snapshot`]s are unaffected and stay readable.
    ///
    /// Closing starts no flush or checkpoint; an in-flight build may finish.
    /// It writes nothing to disk, so the engine's data goes with it unless
    /// checkpointed first. Dropping the engine does the same and discards the
    /// result.
    ///
    /// Fails only with [`BackgroundError::Panicked`] if either worker
    /// stopped by panicking, at any time; the engine is shut down all the
    /// same. A compaction read failure is reported by [`Engine::background_status`].
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
    ///
    /// # Errors
    /// Returns [`UpdateError`] for an invalid record, memory admission failure
    /// or exhausted sequence space, without publishing the write.
    ///
    /// # Panics
    /// Panics if the state lock is poisoned or a synchronous flush build panics.
    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<u64, UpdateError> {
        self.core.put(key, value)
    }

    /// Deletes `key` and returns the update's sequence number.
    /// A missing key still receives a tombstone and a sequence.
    ///
    /// # Errors
    /// Returns [`UpdateError`] for an invalid key or record charge, memory
    /// admission failure or exhausted sequences. Rejection publishes nothing.
    ///
    /// # Panics
    /// Panics if the state lock is poisoned or a synchronous flush build panics.
    pub fn delete(&self, key: &[u8]) -> Result<u64, UpdateError> {
        self.core.delete(key)
    }

    /// Applies every operation atomically and returns the last sequence number.
    /// Later operations on the same key win. An empty batch returns the current
    /// sequence number without changing state. Batches built directly into runs
    /// take one publication sequence; memtable batches take one per operation.
    ///
    /// # Errors
    /// Returns [`UpdateError`] if any record is invalid, admission fails or
    /// sequences are exhausted. Rejection publishes none of the batch.
    ///
    /// # Panics
    /// Panics if the engine state lock is poisoned or a synchronous build panics.
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
    ///
    /// # Errors
    /// Returns [`UpdateError::Rejected`] for unsorted, duplicate or invalid
    /// records, or another [`UpdateError`] for admission or sequence exhaustion.
    /// Rejection publishes nothing.
    ///
    /// # Panics
    /// Panics if the state lock is poisoned or the private run build panics;
    /// a build panic leaves the published sources readable.
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
    ///
    /// # Errors
    /// Returns [`ReadError`] for touched corrupt runs or conflicting copies.
    ///
    /// # Panics
    /// Panics if the engine state lock is poisoned.
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
    ///
    /// # Panics
    /// Panics if the engine state lock is poisoned.
    pub fn read(&self) -> ReadGuard<'_> {
        self.core.read()
    }

    /// Takes a consistent copy-on-write view without retaining a lock.
    /// Snapshots keep their own immutable sources through maintenance and closure.
    ///
    /// # Panics
    /// Panics if the engine state lock is poisoned.
    pub fn snapshot(&self) -> Snapshot {
        self.core.snapshot()
    }

    /// Waits for everything written before this call to appear in runs.
    /// Publication is unconditional. A stopped flush worker reports its panic;
    /// its tables stay readable and writers can flush them synchronously.
    ///
    /// # Errors
    /// Returns [`BackgroundError::Panicked`] if the flush worker has failed.
    ///
    /// # Panics
    /// Panics if the engine state lock is poisoned.
    pub fn flush(&self) -> Result<FlushReport, BackgroundError> {
        self.core.flush()
    }

    /// Performs one eager compaction step, or returns `None` if none is eligible.
    /// Uses the caller's workspace and serializes with other compactions.
    ///
    /// # Errors
    /// Returns [`CompactionError`] for a read failure or invalidated selection.
    ///
    /// # Panics
    /// Panics if the state lock is poisoned or a synchronous builder panics.
    #[cfg(any(test, feature = "diagnostics"))]
    pub(crate) fn compact(
        &self,
        workspace: &mut MergeWorkspace,
    ) -> Result<Option<CompactionReport>, CompactionError> {
        self.core.compact(
            CompactionPolicy {
                max_overlap: 1,
                ..self.core.policy
            },
            workspace,
        )
    }

    /// Returns a summary of the current state.
    ///
    /// # Panics
    /// Panics if the engine state lock is poisoned.
    pub fn stats(&self) -> EngineStats {
        self.core.stats()
    }

    /// Requests a complete compaction pass and waits for it, or reports a
    /// read failure or worker panic. Do not hold a [`ReadGuard`] while waiting.
    pub fn wait_for_compaction(&self) -> Result<(), BackgroundError> {
        match &self.core.background {
            Some(signal) => signal.wait(),
            None => Ok(()),
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
        Self::with_pool(options, ChunkPool::new(RESERVE_BYTES))
    }

    fn with_pool(options: &EngineOptions, chunks: ChunkPool) -> Self {
        #[cfg(test)]
        let (last_seq, target_run_len, max_frozen_memtables, bulk_ingest_min, policy, background) = (
            options.last_seq,
            options.target_run_len,
            options.max_frozen_memtables,
            options
                .bulk_ingest_min
                .unwrap_or(options.memtable_budget / BULK_INGEST_DIVISOR),
            options.background_compaction.unwrap_or_default(),
            options.background_compaction.map(|_| worker::Signal::new()),
        );
        #[cfg(not(test))]
        let (last_seq, target_run_len, max_frozen_memtables, bulk_ingest_min, policy, background) = (
            0,
            TARGET_RUN_LEN,
            MAX_FROZEN_MEMTABLES,
            options.memtable_budget / BULK_INGEST_DIVISOR,
            CompactionPolicy::for_memory(options.memory_limit),
            Some(worker::Signal::new()),
        );
        let active = Memtable::in_pool(options.memtable_budget, Some(last_seq), &chunks);
        Self {
            chunks,
            state: RwLock::new(State {
                last_seq,
                active,
                frozen: Vec::new(),
                runs: Vec::new(),
                directory: Arc::new(RunDirectory::empty()),
                run_bytes: 0,
                held_bytes: 0,
                levels: Levels::new(options.level_ratio),
                level0_stop: if background.is_some() {
                    LEVEL0_STOP
                } else {
                    usize::MAX
                },
                #[cfg(test)]
                split_flushes: true,
                retired: Vec::new(),
                last_view: None,
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
            max_frozen_memtables,
            flush_signal: flusher::Signal::new(),
            bulk_ingest_min,
            target_run_len,
            codec: options.compression.codec(),
            target_block_compressed: options.target_block_compressed,
            snapshots: Arc::new(()),
            background,
            memory_limit: options.memory_limit,
            policy: CompactionPolicy::for_memory(options.memory_limit).with_test_policy(policy),
            resources: resources::Progress::default(),
            reclaim: std::sync::atomic::AtomicBool::new(false),
            #[cfg(test)]
            panic_worker: std::sync::atomic::AtomicBool::new(false),
            #[cfg(test)]
            panic_step: std::sync::atomic::AtomicBool::new(false),
            #[cfg(test)]
            level0_waits: std::sync::atomic::AtomicUsize::new(0),
            #[cfg(test)]
            after_build: Mutex::new(None),
            #[cfg(test)]
            copy_forward: std::sync::atomic::AtomicBool::new(true),
            #[cfg(test)]
            piece_budget: std::sync::atomic::AtomicUsize::new(0),
            #[cfg(test)]
            fail_piece: std::sync::atomic::AtomicUsize::new(0),
            #[cfg(test)]
            before_piece: Mutex::new(None),
            #[cfg(test)]
            piece_output_budget: std::sync::atomic::AtomicUsize::new(0),
            #[cfg(test)]
            piece_request: std::sync::atomic::AtomicUsize::new(0),
            #[cfg(test)]
            before_flush_build: Mutex::new(None),
            #[cfg(test)]
            after_ingest_build: Mutex::new(None),
        }
    }

    fn put(&self, key: &[u8], value: &[u8]) -> Result<u64, UpdateError> {
        self.update(key, Value::Bytes(value))
    }

    fn delete(&self, key: &[u8]) -> Result<u64, UpdateError> {
        self.update(key, Value::Tombstone)
    }

    fn update(&self, key: &[u8], value: Value<'_>) -> Result<u64, UpdateError> {
        self.validate_record(key, value)?;
        let mut state = self.admit(self.write_state())?;
        loop {
            match self.write(&mut state, key, value) {
                Err(Refusal::Full) => state = self.admit(self.make_room(state))?,
                written => return written.map_err(Refusal::into_error),
            }
        }
    }

    /// Waits only for backpressure, then swaps the active table. Never builds.
    fn make_room<'s>(
        &'s self,
        mut state: RwLockWriteGuard<'s, State>,
    ) -> RwLockWriteGuard<'s, State> {
        state = self.wait_for_level0(state);
        while state.frozen.len() >= self.max_frozen_memtables {
            #[cfg(feature = "diagnostics")]
            let _waiting = self.resources.waiting();
            state = self.wait_for_flush(state);
        }
        if !state.active.is_empty() {
            self.freeze_active(&mut state);
        }
        self.flush_signal.request();
        state
    }

    fn wait_for_flush<'s>(
        &'s self,
        state: RwLockWriteGuard<'s, State>,
    ) -> RwLockWriteGuard<'s, State> {
        let progress = self.flush_signal.lock();
        if self.flush_signal.failed(&progress) {
            drop(progress);
            drop(state);
            let serial = self.serialize_flush();
            let state = self.write_state();
            let job = FlushJob {
                engine: self,
                _serial: serial,
                tables: state.frozen.clone(),
                cuts: state.flush_cuts(self.target_run_len),
            };
            drop(state);
            if !job.tables.is_empty() {
                job.build().publish();
            }
            return self.write_state();
        }
        // Acquire the notification mutex before releasing state: completion
        // cannot slip between checking the predicate and entering the condvar.
        drop(state);
        drop(self.flush_signal.wait(progress));
        self.write_state()
    }

    /// Wait for compaction publication without holding the state lock.
    fn wait_for_level0<'s>(
        &'s self,
        mut state: RwLockWriteGuard<'s, State>,
    ) -> RwLockWriteGuard<'s, State> {
        while state.levels.level0_flushes() >= state.level0_stop {
            #[cfg(feature = "diagnostics")]
            let _waiting = self.resources.waiting();
            #[cfg(feature = "diagnostics")]
            let _level0 = self.resources.level0_waiting();
            drop(state);
            #[cfg(test)]
            self.level0_waits
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            let signal = self.background.as_ref().expect(WORKER);
            if signal
                .wait_for(|| {
                    let state = self.read_state();
                    state.levels.level0_flushes() < state.level0_stop
                })
                .is_err()
            {
                // A worker panic does not prevent synchronous maintenance.
                self.compact(self.policy, &mut MergeWorkspace::new())
                    .expect("compaction cannot read its inputs while writers wait at level 0");
            }
            state = self.write_state();
        }
        state
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
        self.chunks.ingested(
            key.len()
                + match value {
                    Value::Bytes(bytes) => bytes.len(),
                    Value::Tombstone => 0,
                },
        );
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
                unreachable!("the engine assigns increasing sequence numbers")
            }
        }
    }

    /// Freezes the active table and replaces it, in constant time.
    fn freeze_active(&self, state: &mut State) {
        let successor = Memtable::in_pool(self.memtable_budget, Some(state.last_seq), &self.chunks);
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

    fn snapshot(&self) -> Snapshot {
        let mut state = self.write_state();
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
        Snapshot {
            tables,
            runs: Arc::clone(&state.directory),
            last_seq: state.last_seq,
            _live: Arc::clone(&self.snapshots),
        }
    }

    fn flush(&self) -> Result<FlushReport, BackgroundError> {
        let mut state = self.write_state();
        let before = state.flush_totals;
        if let Some(error) = self.flush_signal.failure() {
            return Err(error);
        }
        while !state.active.is_empty() && state.frozen.len() >= self.max_frozen_memtables {
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
            state = self.wait_for_flush(state);
            if let Some(error) = self.flush_signal.failure() {
                return Err(error);
            }
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
        options: CompactionPolicy,
        workspace: &mut MergeWorkspace,
    ) -> Result<Option<CompactionReport>, CompactionError> {
        match self.begin_compaction(options)? {
            Some(Begun::Job(job)) => job.execute(workspace).map(Some),
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
        options: CompactionPolicy,
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
        let fit = options.fit(self.target_run_len, state.level0_stop);
        state.levels.insert_levels(&fit);
        let plan = if options.reclaim {
            state.levels.plan_reclaim(&fit)
        } else {
            state.levels.plan(&fit)
        };
        let mut source_count = 0;
        let (kind, level, inputs, dest, mut cuts, cursor) = match plan {
            Plan::Idle => return Ok(None),
            Plan::Move { level, run } => {
                if state.levels.move_down(level, &run).is_err() {
                    return Err(CompactionError::Stale);
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
                if level == 0 {
                    source_count = sources.len();
                }
                let inputs = sources.into_iter().chain(dests).collect();
                let dest = Dest::Level(level + 1);
                (
                    CompactionKind::Merge,
                    level,
                    inputs,
                    dest,
                    cuts,
                    Some(cursor),
                )
            }
            Plan::Clean { level, run } => (
                CompactionKind::Merge,
                level,
                vec![run],
                Dest::Level(level),
                OutputCuts::default(),
                None,
            ),
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
                )
            }
        };
        cuts.omit_filter = matches!(dest, Dest::Level(level) if level == state.levels.depth());
        let others = Self::others(&state, &inputs);
        // Output into the bottom level erases the sequences no unselected
        // run can contest; `build` decides record by record. Snapshots and
        // checkpoints keep the directories, and so the runs, they started from.
        let zero_seq = matches!(dest, Dest::Level(level) if level == state.levels.depth());
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
            estimated_run_bytes,
            source_count,
        })))
    }

    /// Begins the next compaction that reads and writes, taking any moves
    /// before it: for tests that stage one merge.
    #[cfg(test)]
    fn begin_job(
        &self,
        options: CompactionPolicy,
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
        options: CompactionPolicy,
        picks: Vec<usize>,
    ) -> Result<CompactionJob<'_>, CompactionError> {
        let serial = self
            .compacting
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        let state = self.write_state();
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
            estimated_run_bytes,
            source_count: 0,
        })
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
        let usage = self.usage(&state);
        let physical: Vec<_> = physical_runs(&state.runs).collect();

        EngineStats {
            build: self.chunks.counters(),
            filter_bytes: physical.iter().map(|run| run.run.filter_bytes()).sum(),
            index_bytes: physical.iter().map(|run| run.run.index_bytes()).sum(),
            chunk_bytes_held: physical.iter().map(|run| run.run.chunk_capacity()).sum(),
            block_bytes_live: physical.iter().map(|run| run.run.frame_bytes()).sum(),
            used: usage.used,
            memory_limit: self.memory_limit,
            memtable_bytes: usage.memtables,
            snapshot_bytes: usage.snapshots,
            pool_unaccounted_bytes: usage.pool_unaccounted,
            build_output_bytes: usage.build_output,
            build_workspace_bytes: usage.build_workspace,
            pool_reserve_bytes: usage.pool_reserve,
            pool_in_use_bytes: usage.pool_in_use,
            pool_reserve_limit: usage.pool_cap,
            last_seq: state.last_seq,
            active: state.active.memory(),
            frozen: state
                .frozen
                .iter()
                .map(|table| table.memory())
                .reduce(|sum, table| MemoryUsage {
                    payload: sum.payload + table.payload,
                    arena_bytes: sum.arena_bytes + table.arena_bytes,
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

/// Helpers for compaction correctness tests. Picks are indices into the
/// published runs, valid while nothing else changes them.
#[cfg(test)]
impl Engine {
    /// Compacts `picks` through the engine job: the same tombstone
    /// rule, build and publication, with the output placed as
    /// [`Core::begin_picked`] says.
    pub(crate) fn compact_picked(
        &self,
        options: CompactionPolicy,
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

    #[test]
    fn scans_reuse_workspace_cursor_capacity_after_early_drop_and_source_release() {
        let engine = Engine::new(manual()).unwrap();
        engine.put(b"a", b"old").unwrap();
        engine.put(b"b", b"deleted").unwrap();
        let job = engine.core.begin_flush().unwrap();
        engine.put(b"a", b"new").unwrap();
        engine.delete(b"b").unwrap();
        engine.put(b"c", b"live").unwrap();
        let snapshot = engine.snapshot();
        let mut ws = EngineWorkspace::new();
        let mut scan = snapshot.scan(&mut ws);
        assert_eq!(scan.tables.len(), 2);
        let allocation = scan.tables.as_ptr().addr();
        let capacity = scan.tables.capacity();
        assert_eq!(scan.next_record().unwrap().unwrap().value, b"new");
        drop(scan); // Leave another live key unread.
        assert!(ws.scan_tables.is_empty());
        assert_eq!(ws.scan_tables.as_ptr().addr(), allocation);
        for _ in 0..3 {
            let mut scan = snapshot.scan_prefix(b"c", &mut ws);
            assert_eq!(scan.tables.as_ptr().addr(), allocation);
            assert_eq!(scan.next_record().unwrap().unwrap().value, b"live");
            assert!(scan.next_record().unwrap().is_none());
        }
        drop(snapshot);
        job.build().publish();
        engine.flush().unwrap();
        engine.close().unwrap();
        // No cursor or borrowed source survives in the workspace.
        let empty = Engine::new(manual()).unwrap();
        let guard = empty.read();
        let mut scan = guard.scan(&mut ws);
        assert_eq!(scan.tables.as_ptr().addr(), allocation);
        assert!(scan.next_record().unwrap().is_none());
        drop(scan);
        assert_eq!(ws.scan_tables.capacity(), capacity);
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
        job.build().publish();
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

    fn options(max_inputs: usize, max_input_bytes: usize) -> CompactionPolicy {
        CompactionPolicy {
            max_inputs,
            max_input_bytes,
            max_overlap: 1,
            ..CompactionPolicy::default()
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
        let snapshot = engine.snapshot();
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
        let before = engine.snapshot();

        let options = CompactionPolicy {
            max_overlap: 2,
            max_inputs: 2,
            ..CompactionPolicy::default()
        };
        let mut workspace = MergeWorkspace::new();
        // The oldest run moves to level 1; the next merges with it, and the
        // newest is left in level 0.
        let report = engine
            .core
            .compact(options, &mut workspace)
            .unwrap()
            .unwrap();
        assert_eq!(report.kind, CompactionKind::Move);
        let report = engine
            .core
            .compact(options, &mut workspace)
            .unwrap()
            .unwrap();
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
            .core
            .compact(options(8, usize::MAX), &mut workspace)
            .unwrap()
            .is_some()
        {}
        assert_eq!(engine.stats().runs, 1);
        crate::test_support::wait_until(|| inputs.iter().all(|run| Arc::strong_count(run) == 1));
        assert!(inputs.iter().all(|run| Arc::strong_count(run) == 1));
    }

    // Background compaction.

    use std::sync::Barrier;
    use std::sync::atomic::Ordering;

    fn background(max_overlap: usize) -> EngineOptions {
        EngineOptions {
            background_compaction: Some(CompactionPolicy {
                max_overlap,
                ..CompactionPolicy::default()
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
        let snapshot = engine.snapshot();
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
            .core
            .compact(options(8, usize::MAX), &mut MergeWorkspace::new())
            .unwrap()
            .unwrap();
        assert_eq!(value(&engine, b"k").unwrap(), b"v");
        let core = Arc::clone(&engine.core);
        drop(engine);
        assert_eq!(Arc::strong_count(&core), 1);
    }
}

#[cfg(test)]
mod subcompaction_tests;

#[cfg(test)]
#[path = "../tests/batch.rs"]
mod api_batch_tests;
#[cfg(test)]
#[path = "../tests/checkpoint.rs"]
mod api_checkpoint_tests;
#[cfg(test)]
#[path = "../tests/compaction.rs"]
mod api_compaction_tests;
#[cfg(test)]
#[path = "../tests/engine.rs"]
mod api_engine_tests;
#[cfg(test)]
#[path = "../tests/snapshot.rs"]
mod api_snapshot_tests;
