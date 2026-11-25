//! The engine: memtables, runs in levels, flushes, compaction, checkpoints
//! and the public handles over them.
//!
//! Writes and publication use one exclusive state lock; sequence order is
//! visibility order. A read takes immutable run references under the shared
//! lock and decodes after releasing it. Writers have priority over new
//! readers.
//!
//! Writers rotate full memtables and a dedicated worker builds runs outside
//! the state lock; at most two frozen tables await flush. Level 0 holds
//! overlapping flushes; deeper levels have disjoint key ranges, with dynamic
//! targets from [`Options::level_ratio`]. Every publication of runs also
//! publishes a directory that groups them into the fewest key-disjoint
//! groups, which point lookups and scans read.

mod batch;
mod checkpoint;
mod combiner;
mod compaction;
#[cfg(feature = "diagnostics")]
#[doc(hidden)]
pub mod diagnostics;
mod directory;
mod error;
mod errors;
mod flush_job;
mod flusher;
mod handle;
mod ingest;
mod keyspaces;
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
#[cfg(feature = "diagnostics")]
use checkpoint::CheckpointReport;
use checkpoint::{CheckpointError, OpenError};
use compaction::{Begun, BuiltCompaction, CompactionError, CompactionJob, Cursor};
pub(crate) use compaction::{CompactionKind, CompactionPolicy, CompactionReport};
pub use error::{Error, ErrorKind, Result};
use errors::{UpdateError, WriteError};
#[cfg(test)]
use flush_job::BuiltFlush;
use flush_job::FlushJob;
pub use handle::{Engine, Keyspace, KeyspaceStats, MAX_KEYSPACE_NAME_LEN, MAX_METADATA_LEN, Stats};
use options::ConfigError;
pub use options::{Compression, Options};
use read::View;
pub use read::{KeyRange, MultiGet, Reader, Scan, Snapshot};
pub(crate) use stats::{EngineStats, FlushReport, KeyspaceDetail};
use worker::BackgroundError;
#[cfg(test)]
use worker::BackgroundStatus;

pub use crate::block::{MAX_KEY_LEN, MAX_VALUE_LEN};
pub(crate) use crate::chunk::BuildCounters;
pub(crate) use crate::memtable::{ENTRY_OVERHEAD, MemoryUsage};
pub(crate) use crate::run::{CompactStats, MergeWorkspace};
use crate::run::{ConflictingCopies, LiveRecord, LiveValue, ReadError};

use std::collections::{BTreeMap, HashSet};
use std::fmt;
use std::sync::{
    Arc, Mutex, MutexGuard, PoisonError, RwLock, RwLockReadGuard, RwLockWriteGuard, Weak,
};

use crate::block::{Record, Value, Version};
use crate::chunk::{ChunkPool, RESERVE_BYTES};
use crate::memtable::{
    FlushCuts, FlushSpace, FrozenMemtable, Key, Memtable, Records, TableError, flush_merged,
};
use crate::run::{
    Codec, CompactLimits, Encoding, LogicalCursor, LogicalWorkspace, OutputCuts, RAW_BLOCK_CAP,
    Run, RunView, SeqFloor, TARGET_BLOCK_COMPRESSED, compact_parallel_in, latest_from,
};

use directory::RunDirectory;
use keyspaces::{Directories, KeyPrefix, KeyspaceState, MAX_PREFIX_LEN};
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
const COMPACTION_INPUT_BYTES: usize = 256 << 20; // Optional level-0 source bytes.
const COMPACTION_WORKSPACE_BYTES: usize = 64 << 20; // Parallel builder allowance.
const COMPACTION_TRIGGER: usize = 4; // Flush groups that trigger level 0.
/// Fraction of the memtable budget the active table must reach before a quiet
/// period flushes it. Below one eighth, the saving is under an eighth of one
/// memtable while promoting the flushed run can rewrite a full bottom run, so
/// a trickle of small writes never causes a rewrite every quiet period.
const QUIET_TAIL_DIVISOR: usize = 8;
const SOFT_WAIT: std::time::Duration = std::time::Duration::from_millis(2);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct WriteActivity {
    sequence: u64,
    flushes: u64,
}

struct State {
    last_seq: u64,
    /// The live keyspaces by ID. Created and dropped under this lock, so
    /// every write, read and snapshot agrees on them.
    keyspaces: BTreeMap<u32, KeyspaceState>,
    /// The live keyspaces' IDs by name.
    names: BTreeMap<Arc<str>, u32>,
    /// The ID the next keyspace created gets. IDs are never reused.
    next_keyspace: u32,
    /// Every live keyspace's directory, rebuilt with any of them, so that a
    /// snapshot takes them all with one reference count.
    directories: Directories,
    active: Memtable,
    /// Oldest first. Each table's sequence numbers are above the runs' and
    /// those of the tables before it, and below `active`'s. Writers rotate
    /// tables while the flush worker builds.
    frozen: Vec<Arc<FrozenMemtable>>,
    /// Stored and held bytes of every keyspace's runs.
    run_bytes: usize,
    held_bytes: usize,
    /// The most flushes any keyspace's level 0 holds runs of.
    level0_flushes: usize,
    /// Level-0 writer stop; deterministic tests may change it.
    level0_stop: usize,
    /// The keyspace compaction served last, where ties resume after.
    compaction_cursor: u32,
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
    /// lock, and a reader passes through it only when a writer is waiting, so
    /// new readers cannot keep overtaking a waiting writer while uncontended
    /// reads never serialize on this mutex.
    gate: Mutex<()>,
    /// Number of writers currently waiting to acquire `state` exclusively.
    waiting_writers: std::sync::atomic::AtomicUsize,
    /// Combines concurrent small writes so one lock acquisition serves a group.
    combiner: combiner::WriteCombiner,
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
    /// How the runs this engine builds encode their blocks.
    encoding: Encoding,
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
    /// Writers that have waited for a flush to clear frozen tables.
    #[cfg(test)]
    flush_waits: std::sync::atomic::AtomicUsize,
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
    /// Runs once after a flush releases the state lock, while its job
    /// still holds the flushed tables.
    #[cfg(test)]
    after_flush_publish: Mutex<Option<Arc<dyn Fn() + Send + Sync>>>,
    #[cfg(test)]
    after_ingest_build: Mutex<Option<Arc<dyn Fn() + Send + Sync>>>,
    #[cfg(test)]
    before_combine: Mutex<Option<Arc<dyn Fn() + Send + Sync>>>,
}

impl Core {
    fn new(options: &Options) -> Self {
        Self::with_pool(options, ChunkPool::new(RESERVE_BYTES))
    }

    fn with_pool(options: &Options, chunks: ChunkPool) -> Self {
        #[cfg(test)]
        let (last_seq, target_run_len, max_frozen_memtables, bulk_ingest_min, policy, background) = (
            options.last_seq,
            options.target_run_len,
            options.max_frozen_memtables,
            options
                .bulk_ingest_min
                .unwrap_or(options.memtable_size / BULK_INGEST_DIVISOR),
            options.background_compaction.unwrap_or_default(),
            options.background_compaction.map(|_| worker::Signal::new()),
        );
        #[cfg(not(test))]
        let (last_seq, target_run_len, max_frozen_memtables, bulk_ingest_min, policy, background) = (
            0,
            TARGET_RUN_LEN,
            MAX_FROZEN_MEMTABLES,
            options.memtable_size / BULK_INGEST_DIVISOR,
            CompactionPolicy::for_memory(options.memory_limit),
            Some(worker::Signal::new()),
        );
        let active = Memtable::in_pool(options.memtable_size, Some(last_seq), &chunks);
        Self {
            chunks,
            state: RwLock::new(State {
                last_seq,
                keyspaces: BTreeMap::new(),
                names: BTreeMap::new(),
                next_keyspace: 0,
                directories: Arc::new([]),
                active,
                frozen: Vec::new(),
                run_bytes: 0,
                held_bytes: 0,
                level0_flushes: 0,
                compaction_cursor: 0,
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
            waiting_writers: std::sync::atomic::AtomicUsize::new(0),
            combiner: combiner::WriteCombiner::default(),
            flushing: Mutex::new(()),
            compacting: Mutex::new(()),
            background_threads: options.background_threads,
            level_ratio: options.level_ratio,
            checkpointing: Mutex::new(()),
            memtable_budget: options.memtable_size,
            max_frozen_memtables,
            flush_signal: flusher::Signal::new(),
            bulk_ingest_min,
            target_run_len,
            encoding: options.compression.encoding(),
            target_block_compressed: options.block_size,
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
            flush_waits: std::sync::atomic::AtomicUsize::new(0),
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
            after_flush_publish: Mutex::new(None),
            #[cfg(test)]
            after_ingest_build: Mutex::new(None),
            #[cfg(test)]
            before_combine: Mutex::new(None),
        }
    }

    /// Writes to keyspace "test", creating it if needed: for tests of the
    /// core alone.
    #[cfg(test)]
    fn put(&self, key: &[u8], value: &[u8]) -> Result<u64, UpdateError> {
        self.update(self.test_keyspace(), key, Value::Bytes(value))
    }

    #[cfg(test)]
    fn delete(&self, key: &[u8]) -> Result<u64, UpdateError> {
        self.update(self.test_keyspace(), key, Value::Tombstone)
    }

    /// Keyspace "test"'s ID, creating it if needed.
    #[cfg(test)]
    fn test_keyspace(&self) -> u32 {
        let mut state = self.write_state();
        match state.names.get("test") {
            Some(&id) => id,
            None => state.create_keyspace("test", self.level_ratio).unwrap(),
        }
    }

    /// Applies one update to keyspace `keyspace` and returns its sequence
    /// number.
    fn update(&self, keyspace: u32, key: &[u8], value: Value<'_>) -> Result<u64, UpdateError> {
        self.validate_record(KeyPrefix::new(keyspace), key, value)?;
        self.combine_single(keyspace, key, value)
    }

    fn update_direct(
        &self,
        keyspace: u32,
        key: &[u8],
        value: Value<'_>,
    ) -> Result<u64, UpdateError> {
        let state = self.admit(self.write_state())?;
        self.apply_single(state, keyspace, key, value).0
    }

    fn apply_single<'s>(
        &'s self,
        mut state: RwLockWriteGuard<'s, State>,
        keyspace: u32,
        key: &[u8],
        value: Value<'_>,
    ) -> (
        Result<u64, UpdateError>,
        Option<RwLockWriteGuard<'s, State>>,
    ) {
        loop {
            let prefix = match state.keyspace(keyspace) {
                Ok(space) => space.prefix,
                Err(error) => return (Err(error), Some(state)),
            };
            match self.write(&mut state, prefix.key(key), value) {
                Err(Refusal::Full) => match self.admit(self.make_room(state)) {
                    Ok(next) => state = next,
                    Err(error) => return (Err(error), None),
                },
                written => return (written.map_err(Refusal::into_error), Some(state)),
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
            #[cfg(feature = "diagnostics")]
            let _flush = self.resources.flush_waiting();
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
            let job = FlushJob::new(self, serial, &state);
            drop(state);
            if !job.tables.is_empty() {
                job.build().publish();
            }
            return self.write_state();
        }
        // Acquire the notification mutex before releasing state: completion
        // cannot slip between checking the predicate and entering the condvar.
        drop(state);
        #[cfg(test)]
        self.flush_waits
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        drop(self.flush_signal.wait(progress));
        self.write_state()
    }

    /// Wait for compaction publication without holding the state lock.
    fn wait_for_level0<'s>(
        &'s self,
        mut state: RwLockWriteGuard<'s, State>,
    ) -> RwLockWriteGuard<'s, State> {
        while state.level0_flushes >= state.level0_stop {
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
                    state.level0_flushes < state.level0_stop
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
    fn write(&self, state: &mut State, key: Key<'_>, value: Value<'_>) -> Result<u64, Refusal> {
        let was_below = !self.quiet_tail_ready(state);
        let seq = state.last_seq.checked_add(1).expect(SEQUENCES);
        state
            .active
            .write(key, seq, value)
            .map_err(|error| self.refusal(error))?;
        // Set only now: a rejected write must not use up a number.
        state.last_seq = seq;
        self.chunks.ingested(
            key.key.len()
                + match value {
                    Value::Bytes(bytes) => bytes.len(),
                    Value::Tombstone => 0,
                },
        );
        if was_below && self.quiet_tail_ready(state) {
            self.arm_quiet_tail(state);
        }
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

    /// The latest value of `key` in keyspace `keyspace`, copied into `value`.
    fn get<'w>(
        &self,
        keyspace: u32,
        key: &[u8],
        logical: &'w mut LogicalWorkspace,
        value: &'w mut Vec<u8>,
    ) -> Result<Option<&'w [u8]>> {
        let state = self.read_state();
        let space = state.keyspace(keyspace)?;
        let stored = space.prefix.key(key);
        let in_memory = state.active.get_in(stored).or_else(|| {
            state
                .frozen
                .iter()
                .rev()
                .find_map(|table| table.get_in(stored))
        });
        if let Some(version) = in_memory {
            let Value::Bytes(bytes) = version.value else {
                return Ok(None);
            };
            value.clear();
            value.extend_from_slice(bytes);
            return Ok(Some(value));
        }
        // The runs are immutable and shared, so they are read after the lock
        // is released: the result is the state at the lock, and a writer or
        // publication waits only for the memtable lookup, not for decoding.
        let directory = Arc::clone(&space.directory);
        drop(state);
        let found = latest_from(directory.lookup(), key, logical)?;
        Ok(found.map(|found| {
            value.clear();
            value.extend_from_slice(found.value);
            &value[..]
        }))
    }

    /// A consistent view of the state now, sharing every source.
    fn snapshot(&self) -> View {
        // Taking a snapshot mutates state only to cache a new active-table
        // view; when the active table is empty or the cached view still
        // matches `last_seq`, a shared read lock suffices.
        {
            let state = self.read_state();
            if state.active.is_empty() {
                return View {
                    tables: state.frozen.iter().rev().cloned().collect(),
                    directories: Arc::clone(&state.directories),
                    last_seq: state.last_seq,
                    next_keyspace: state.next_keyspace,
                    _live: Arc::clone(&self.snapshots),
                };
            }
            if let Some(view) = state
                .last_view
                .as_ref()
                .filter(|(seq, _)| *seq == state.last_seq)
                .and_then(|(_, view)| view.upgrade())
            {
                let mut tables = Vec::with_capacity(state.frozen.len() + 1);
                tables.push(view);
                tables.extend(state.frozen.iter().rev().cloned());
                return View {
                    tables,
                    directories: Arc::clone(&state.directories),
                    last_seq: state.last_seq,
                    next_keyspace: state.next_keyspace,
                    _live: Arc::clone(&self.snapshots),
                };
            }
        }
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
        View {
            tables,
            directories: Arc::clone(&state.directories),
            last_seq: state.last_seq,
            next_keyspace: state.next_keyspace,
            _live: Arc::clone(&self.snapshots),
        }
    }

    /// Drops keyspace `name` and retires its runs, and returns whether it
    /// was live. Snapshots keep what they hold. Its memtable records stay
    /// charged until the next flush leaves them out.
    ///
    /// Holding the flush and compaction locks keeps any build from
    /// publishing the keyspace's runs after this; a batch built outside the
    /// lock checks its keyspaces when it publishes.
    fn drop_keyspace(&self, name: &str) -> bool {
        if !self.read_state().names.contains_key(name) {
            return false;
        }
        let _flushing = self.serialize_flush();
        let _compacting = self
            .compacting
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        let mut state = self.write_state();
        if !state.remove_keyspace(name) {
            return false;
        }
        drop(state);
        self.flush_signal.changed();
        self.resources.changed();
        self.notify_worker();
        true
    }

    #[cfg_attr(not(any(test, feature = "diagnostics")), allow(dead_code))]
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
        let job = FlushJob::new(self, serial, &state);
        drop(state);
        Some(job)
    }

    fn compact(
        &self,
        options: CompactionPolicy,
        workspace: &mut MergeWorkspace,
    ) -> Result<Option<CompactionReport>, CompactionError> {
        self.compact_when(options, workspace, None)
    }

    fn quiet_tail_min(&self) -> usize {
        self.memtable_budget.div_ceil(QUIET_TAIL_DIVISOR)
    }

    fn quiet_tail_ready(&self, state: &State) -> bool {
        state.active.memory().charged >= self.quiet_tail_min()
    }

    fn arm_quiet_tail(&self, state: &State) {
        if state.frozen.is_empty()
            && let Some(signal) = &self.background
        {
            signal.arm_quiet_if_idle(state.write_activity());
        }
    }

    fn idle_activity(&self) -> Option<WriteActivity> {
        let state = self.read_state();
        (state.frozen.is_empty() && (state.level0_flushes != 0 || self.quiet_tail_ready(&state)))
            .then(|| state.write_activity())
    }

    /// Freezes and flushes a large active-memtable tail before a quiet settle,
    /// using the normal flush path and serialization outside the state lock.
    /// Returns the activity for the settle that follows, or `None` if writes
    /// or frozen tables intervened.
    fn flush_quiet_tail(&self, activity: WriteActivity) -> Option<WriteActivity> {
        {
            let state = self.read_state();
            if !state.frozen.is_empty() || activity != state.write_activity() {
                return None;
            }
            if !self.quiet_tail_ready(&state) || state.level0_flushes >= state.level0_stop {
                return Some(activity);
            }
        }
        let serial = self.serialize_flush();
        let mut state = self.write_state();
        if !state.frozen.is_empty() || activity != state.write_activity() {
            return None;
        }
        if !self.quiet_tail_ready(&state) || state.level0_flushes >= state.level0_stop {
            return Some(activity);
        }
        self.freeze_active(&mut state);
        let job = FlushJob::new_quiet(self, serial, &state);
        drop(state);
        #[cfg(test)]
        if let Some(hook) = self.before_flush_build.lock().unwrap().take() {
            hook();
        }
        job.build().publish_quiet();
        Some(WriteActivity {
            flushes: activity.flushes + 1,
            ..activity
        })
    }

    fn compact_when(
        &self,
        options: CompactionPolicy,
        workspace: &mut MergeWorkspace,
        quiet: Option<WriteActivity>,
    ) -> Result<Option<CompactionReport>, CompactionError> {
        match self.begin_compaction(options, quiet)? {
            Some(Begun::Job(job)) => job.execute(workspace).map(Some),
            Some(Begun::Done(report)) => Ok(Some(report)),
            None => Ok(None),
        }
    }

    /// The keyspace compaction serves next, after adding the levels each
    /// keyspace's size calls for: the highest [`levels::Score`], or under
    /// reclaim the most reclaimable bytes among keyspaces with work. When
    /// quiet, keyspaces with any level-0 runs are eligible below every
    /// ordinary score. Ties rotate in keyspace ID order.
    fn choose_keyspace(
        state: &mut State,
        fit: &Fit,
        reclaim: bool,
        quiet: bool,
    ) -> Option<(u32, bool)> {
        let cursor = state.compaction_cursor;
        // IDs after the cursor first, then the rest: a key that wraps.
        let mut best = None;
        for (&id, space) in &mut state.keyspaces {
            let turn = (id <= cursor, id);
            space.levels.insert_levels(fit);
            let rank = if reclaim {
                if matches!(space.levels.plan_reclaim(fit), Plan::Idle) {
                    continue;
                }
                (
                    Some(levels::Score::default()),
                    space.levels.reclaimable_bytes(),
                )
            } else {
                let score = space.levels.score(fit);
                if score.is_none() && !(quiet && space.levels.has_level0()) {
                    continue;
                }
                (score, 0)
            };
            let better = best
                .as_ref()
                .is_none_or(|(first, top)| rank > *top || rank == *top && turn < *first);
            if better {
                best = Some((turn, rank));
            }
        }
        let ((_, id), (score, _)) = best?;
        state.compaction_cursor = id;
        Some((id, score.is_none()))
    }

    /// Chooses the next step from the levels, under the exclusive lock
    /// briefly, so that level insertions and moves, which change only
    /// levels, are done in the same section. Selection reads metadata only:
    /// level sizes, key ranges by binary search and the runs of level 0.
    fn begin_compaction(
        &self,
        options: CompactionPolicy,
        quiet: Option<WriteActivity>,
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
        // Pending flushes can join the same ranges when they publish.
        if quiet
            .is_some_and(|activity| !state.frozen.is_empty() || activity != state.write_activity())
        {
            return Ok(None);
        }
        let fit = options.fit(self.target_run_len, state.level0_stop);
        let Some((keyspace, residue)) =
            Self::choose_keyspace(&mut state, &fit, options.reclaim, quiet.is_some())
        else {
            return Ok(None);
        };
        let space = state.space_mut(keyspace);
        let plan = if options.reclaim {
            space.levels.plan_reclaim(&fit)
        } else if residue {
            space.levels.plan_idle(&fit)
        } else {
            space.levels.plan(&fit)
        };
        let mut source_count = 0;
        let (kind, level, inputs, dest, mut cuts, cursor) = match plan {
            Plan::Idle => return Ok(None),
            Plan::Move { level, run } => {
                if space.levels.move_down(level, &run).is_err() {
                    return Err(CompactionError::Stale);
                }
                if cfg!(any(test, debug_assertions)) {
                    space.levels.check(&space.runs);
                }
                state.levels_changed();
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
        let space = state.space_mut(keyspace);
        cuts.omit_filter = matches!(dest, Dest::Level(level) if level == space.levels.depth());
        let others = Self::others(space, &inputs);
        // Output into the bottom level erases the sequences no unselected
        // run can contest; `build` decides record by record. Snapshots and
        // checkpoints keep the directories, and so the runs, they started from.
        let zero_seq = matches!(dest, Dest::Level(level) if level == space.levels.depth());
        let estimated_run_bytes = space.levels.segment(&fit);
        drop(state);
        Ok(Some(Begun::Job(CompactionJob {
            engine: self,
            _serial: serial,
            keyspace,
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
            match self.begin_compaction(options, None)? {
                Some(Begun::Job(job)) => return Ok(Some(job)),
                Some(Begun::Done(_)) => {}
                None => return Ok(None),
            }
        }
    }

    /// The runs of `space` outside `inputs` that overlap their span, for
    /// the tombstone rule.
    fn others(space: &KeyspaceState, inputs: &[SharedRun]) -> Vec<SharedRun> {
        let ranges = inputs
            .iter()
            .map(|run| run.run.key_range().expect(NOT_EMPTY));
        let min = ranges.clone().map(|range| range.min).min();
        let max = ranges.map(|range| range.max).max();
        match (min, max) {
            (Some(min), Some(max)) => space.levels.others(inputs, min, max),
            _ => Vec::new(),
        }
    }

    /// Begins a compaction of the runs `picks` names, by index into the run
    /// list of the one live keyspace, whatever their levels: the tests'
    /// arbitrary merges. The output goes into the deepest level of the
    /// inputs if it fits there, and otherwise into level 0 as a flush of its own.
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
        let keyspace = state.only_id();
        let space = state.only();
        let inputs: Vec<SharedRun> = picks
            .iter()
            .map(|&index| Arc::clone(&space.runs[index]))
            .collect();
        let deepest = inputs
            .iter()
            .filter_map(|run| space.levels.level_of(run))
            .max()
            .unwrap_or(0);
        let others = Self::others(space, &inputs);
        let estimated_run_bytes = space
            .levels
            .segment(&options.fit(self.target_run_len, state.level0_stop));
        drop(state);
        Ok(CompactionJob {
            engine: self,
            _serial: serial,
            keyspace,
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

    #[cfg_attr(not(any(test, feature = "diagnostics")), allow(dead_code))]
    fn stats(&self) -> EngineStats {
        let state = self.read_state();
        let retained = state.retained();
        let usage = self.usage(&state);
        let spaces = self.keyspace_stats_in(&state);
        let sum = |field: fn(&KeyspaceDetail) -> usize| spaces.iter().map(field).sum::<usize>();
        let most =
            |field: fn(&KeyspaceDetail) -> usize| spaces.iter().map(field).max().unwrap_or(0);

        EngineStats {
            build: self.chunks.counters(),
            filter_bytes: sum(|space| space.filter_bytes),
            index_bytes: sum(|space| space.index_bytes),
            chunk_bytes_held: sum(|space| space.chunk_bytes_held),
            block_bytes_live: sum(|space| space.block_bytes_live),
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
            runs: sum(|space| space.runs),
            run_bytes: state.run_bytes,
            held_bytes: state.held_bytes,
            max_overlap: most(|space| space.max_overlap),
            snapshots: Arc::strong_count(&self.snapshots) - 1,
            retained_sources: retained.sources,
            retained_bytes: retained.bytes,
            retained_run_bytes: retained.run_bytes,
            retained_held_bytes: retained.held_bytes,
            level0_flushes: state.level0_flushes,
            level0_runs: sum(|space| space.level0_runs),
            levels: most(|space| space.levels),
            flushes: state.flushes,
            flushed_bytes: state.flushed_bytes,
            keyspaces: spaces.len(),
        }
    }

    /// Each live keyspace's runs and levels, in name order.
    #[cfg_attr(not(any(test, feature = "diagnostics")), allow(dead_code))]
    fn keyspace_stats(&self) -> Vec<KeyspaceDetail> {
        self.keyspace_stats_in(&self.read_state())
    }

    #[cfg_attr(not(any(test, feature = "diagnostics")), allow(dead_code))]
    fn keyspace_stats_in(&self, state: &State) -> Vec<KeyspaceDetail> {
        state
            .names
            .values()
            .map(|id| {
                let space = &state.keyspaces[id];
                let physical: Vec<_> = physical_runs(&space.runs).collect();
                KeyspaceDetail {
                    name: space.name.to_string(),
                    runs: space.runs.len(),
                    run_bytes: space.run_bytes,
                    held_bytes: space.held_bytes,
                    filter_bytes: physical.iter().map(|run| run.run.filter_bytes()).sum(),
                    index_bytes: physical.iter().map(|run| run.run.index_bytes()).sum(),
                    chunk_bytes_held: physical.iter().map(|run| run.run.chunk_capacity()).sum(),
                    block_bytes_live: physical.iter().map(|run| run.run.frame_bytes()).sum(),
                    max_overlap: space.directory.group_count(),
                    level0_flushes: space.levels.level0_flushes(),
                    level0_runs: space.levels.shape()[0].0,
                    levels: space.levels.depth(),
                }
            })
            .collect()
    }

    fn read_state(&self) -> RwLockReadGuard<'_, State> {
        if self
            .waiting_writers
            .load(std::sync::atomic::Ordering::Acquire)
            != 0
        {
            #[cfg(any(test, feature = "diagnostics"))]
            self.resources
                .gate_waits
                .fetch_add(1, std::sync::atomic::Ordering::Release);
            drop(self.pass_gate());
        }
        self.state.read().expect(LOCK)
    }

    fn write_state(&self) -> RwLockWriteGuard<'_, State> {
        struct WaitingWriter<'a>(&'a std::sync::atomic::AtomicUsize);
        impl Drop for WaitingWriter<'_> {
            fn drop(&mut self) {
                self.0.fetch_sub(1, std::sync::atomic::Ordering::Release);
            }
        }
        self.waiting_writers
            .fetch_add(1, std::sync::atomic::Ordering::AcqRel);
        let _waiting = WaitingWriter(&self.waiting_writers);
        let _gate = self.pass_gate();
        let guard = self.state.write().expect(LOCK);
        #[cfg(any(test, feature = "diagnostics"))]
        self.resources
            .write_locks
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        guard
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
/// At one update per nanosecond, 2^64 sequence numbers last 584 years.
const SEQUENCES: &str = "sequence numbers are exhausted";
/// Flushes publish only non-empty tables, and compaction builds a run only
/// once it has a record.
const NOT_EMPTY: &str = "published runs are not empty";

const WORKER: &str = "an engine with a worker has its signal";

/// Helpers for compaction correctness tests. Picks are indices into the
/// published runs, valid while nothing else changes them.
#[cfg(any(test, feature = "diagnostics"))]
impl Core {
    /// Performs one eager compaction step with the caller's workspace, or
    /// returns `None` if none is eligible.
    fn compact_step(
        &self,
        workspace: &mut MergeWorkspace,
    ) -> Result<Option<CompactionReport>, CompactionError> {
        self.compact(
            CompactionPolicy {
                max_overlap: 1,
                ..self.policy
            },
            workspace,
        )
    }
}

#[cfg(test)]
impl Engine {
    /// As [`Core::compact_step`].
    pub(crate) fn compact(
        &self,
        workspace: &mut MergeWorkspace,
    ) -> Result<Option<CompactionReport>, CompactionError> {
        self.core.compact_step(workspace)
    }

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
            let options = if worker { Options::default() } else { manual() };
            let engine = Engine::in_memory(options).unwrap();
            engine.ks().put(b"k", b"v").unwrap();
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
                let closed = engine.close().unwrap_err();
                assert_eq!(closed.kind(), ErrorKind::Background);
                assert!(closed.to_string().contains(message.as_str()), "{closed}");
            } else {
                engine.close().unwrap();
            }
            assert_eq!(Arc::strong_count(&core), 1);
            drop(core);
        }
    }

    /// The defaults without the background worker: these tests flush and
    /// compact by hand and count what each step leaves.
    fn manual() -> Options {
        Options {
            background_compaction: None,
            ..Options::default()
        }
    }

    fn frozen_handle(engine: &Engine) -> Arc<FrozenMemtable> {
        let state = engine.core.read_state();
        assert_eq!(state.frozen.len(), 1);
        Arc::clone(&state.frozen[0])
    }

    #[test]
    fn scans_reuse_reader_cursor_capacity_after_early_drop_and_source_release() {
        let engine = Engine::in_memory(manual()).unwrap();
        let ks = engine.ks();
        ks.put(b"a", b"old").unwrap();
        ks.put(b"b", b"deleted").unwrap();
        let job = engine.core.begin_flush().unwrap();
        ks.put(b"a", b"new").unwrap();
        ks.delete(b"b").unwrap();
        ks.put(b"c", b"live").unwrap();
        let snapshot = engine.snapshot();
        let mut reader = snapshot.reader();
        let mut scan = reader.range(&ks, ..);
        assert_eq!(scan.tables.len(), 2);
        let allocation = scan.tables.as_ptr().addr();
        let capacity = scan.tables.capacity();
        assert_eq!(scan.next().unwrap().unwrap().1, b"new");
        drop(scan); // Leave another live key unread.
        assert!(reader.buffers.scan_tables.is_empty());
        assert_eq!(reader.buffers.scan_tables.as_ptr().addr(), allocation);
        for _ in 0..3 {
            let mut scan = reader.prefix(&ks, b"c");
            assert_eq!(scan.tables.as_ptr().addr(), allocation);
            assert_eq!(scan.next().unwrap().unwrap().1, b"live");
            assert!(scan.next().unwrap().is_none());
        }
        drop(reader);
        drop(snapshot);
        job.build().publish();
        engine.flush().unwrap();
        // No cursor or borrowed source survives in the pooled buffers.
        let mut reader = engine.reader();
        let mut scan = reader.range(&ks, ..);
        assert_eq!(scan.tables.as_ptr().addr(), allocation);
        assert!(scan.next().unwrap().is_some());
        drop(scan);
        assert_eq!(reader.buffers.scan_tables.capacity(), capacity);
    }

    /// The writer-priority rule: from the moment a writer waits for the state
    /// lock until it has it, the gate every reader must pass is closed.
    #[test]
    fn a_waiting_writer_holds_the_gate_until_it_has_the_lock() {
        let engine = Engine::in_memory(manual()).unwrap();
        let ks = engine.ks();
        ks.put(b"k", b"old").unwrap();
        let held = engine.core.read_state();
        std::thread::scope(|scope| {
            let writer = scope.spawn(|| ks.put(b"k", b"new").unwrap());
            // The read lock keeps the writer waiting, so once it has the gate it
            // keeps it until the guard is dropped.
            crate::test_support::wait_until(|| {
                engine.core.waiting_writers.load(Ordering::Acquire) == 1
                    && engine.core.gate.try_lock().is_err()
            });
            let reader = scope.spawn(|| ks.get(b"k").unwrap());
            crate::test_support::wait_until(|| {
                engine.core.resources.gate_waits.load(Ordering::Acquire) == 1
            });
            assert!(engine.core.gate.try_lock().is_err());
            assert!(!writer.is_finished());
            assert!(!reader.is_finished());
            drop(held);
            writer.join().unwrap();
            assert_eq!(reader.join().unwrap().as_deref(), Some(&b"new"[..]));
        });
        assert!(engine.core.gate.try_lock().is_ok());
        assert_eq!(ks.get(b"k").unwrap().unwrap(), b"new");
    }

    #[test]
    fn repeated_snapshot_without_writes_takes_no_exclusive_lock() {
        let engine = Engine::in_memory(manual()).unwrap();
        let ks = engine.ks();
        let extra = engine.keyspace("extra").unwrap();
        let write_locks = || engine.core.resources.write_locks.load(Ordering::Relaxed);

        // Empty active table needs only the shared read lock.
        let before = write_locks();
        let empty = engine.snapshot();
        assert_eq!(write_locks(), before);
        drop(empty);

        ks.put(b"k1", b"v1").unwrap();
        extra.put(b"e1", b"ev1").unwrap();

        // First snapshot after a write takes the exclusive lock once to cache `last_view`.
        let before = write_locks();
        let snap1 = engine.snapshot();
        assert_eq!(write_locks(), before + 1);

        // Repeated snapshots without intervening writes reuse `last_view` under the read lock.
        let snap2 = engine.snapshot();
        let held_read = engine.core.read_state();
        let snap3 = engine.snapshot();
        drop(held_read);
        assert_eq!(write_locks(), before + 1);
        assert_eq!(snap2.reader().get(&ks, b"k1").unwrap(), Some(&b"v1"[..]));
        assert_eq!(
            snap3.reader().get(&extra, b"e1").unwrap(),
            Some(&b"ev1"[..])
        );

        // Dropping a keyspace leaves `active` and `last_seq` unchanged, so reusing
        // `last_view` under the read lock still observes the updated directory.
        assert!(engine.drop_keyspace("extra").unwrap());
        // Hold `flushing` so the flush worker woken by `drop_keyspace` cannot
        // take `write_state` during the lock-count assertions below.
        let _flushing = engine.core.serialize_flush();
        let after_drop = write_locks();
        let snap_after_drop = engine.snapshot();
        assert_eq!(write_locks(), after_drop);
        assert_eq!(
            snap1.reader().get(&extra, b"e1").unwrap(),
            Some(&b"ev1"[..])
        );
        assert_eq!(
            snap_after_drop
                .reader()
                .get(&extra, b"e1")
                .unwrap_err()
                .kind(),
            ErrorKind::KeyspaceDropped
        );
        assert_eq!(
            snap_after_drop.reader().get(&ks, b"k1").unwrap(),
            Some(&b"v1"[..])
        );

        // Once every snapshot holding `last_view` drops, the next snapshot recreates it.
        drop((snap1, snap2, snap3, snap_after_drop));
        let before = write_locks();
        let snap4 = engine.snapshot();
        assert_eq!(write_locks(), before + 1);

        // A new write advances `last_seq`, so the next snapshot creates a new view once.
        ks.put(b"k2", b"v2").unwrap();
        let before = write_locks();
        let snap5 = engine.snapshot();
        assert_eq!(write_locks(), before + 1);
        let snap6 = engine.snapshot();
        assert_eq!(write_locks(), before + 1);
        assert_eq!(snap4.reader().get(&ks, b"k2").unwrap(), None);
        assert_eq!(snap5.reader().get(&ks, b"k2").unwrap(), Some(&b"v2"[..]));
        assert_eq!(snap6.reader().get(&ks, b"k2").unwrap(), Some(&b"v2"[..]));
        drop(_flushing);
    }

    #[test]
    fn publication_releases_every_engine_reference_to_the_frozen_table() {
        let engine = Engine::in_memory(manual()).unwrap();
        engine.ks().put(b"k", b"v").unwrap();
        let job = engine.core.begin_flush().unwrap();
        let held = frozen_handle(&engine);
        assert_eq!(Arc::strong_count(&held), 3);
        job.build().publish();
        assert_eq!(Arc::strong_count(&held), 1);
    }

    #[test]
    fn an_abandoned_flush_releases_only_its_own_reference() {
        let engine = Engine::in_memory(manual()).unwrap();
        engine.ks().put(b"k", b"v").unwrap();
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
            engine.ks().put(b"a", value).unwrap();
            engine.ks().put(b"z", value).unwrap();
            engine.flush().unwrap();
        }
    }

    /// A lookup's value and the run blocks it decoded, through each read
    /// path that reaches the runs.
    fn read_counting(engine: &Engine, key: &[u8]) -> [(Option<Vec<u8>>, u64); 2] {
        let ks = engine.ks();
        let mut reader = engine.reader();
        let got = reader.get(&ks, key).unwrap().map(<[u8]>::to_vec);
        let latest = (got, reader.counters().blocks_loaded);
        let snapshot = engine.snapshot();
        let mut reader = snapshot.reader();
        let got = reader.get(&ks, key).unwrap().map(<[u8]>::to_vec);
        [latest, (got, reader.counters().blocks_loaded)]
    }

    #[test]
    fn a_lookup_decodes_only_the_newest_run_holding_the_key() {
        let engine = Engine::in_memory(manual()).unwrap();
        overlapping_runs(&engine);
        engine.ks().put(b"m", b"live").unwrap();
        engine.ks().delete(b"z").unwrap();
        engine.flush().unwrap();
        assert_eq!(engine.engine_stats().max_overlap, 4);
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
        engine.ks().put(b"a", b"4").unwrap();
        engine.flush().unwrap();
        let mut reader = snapshot.reader();
        let seen = reader.get(&engine.ks(), b"a").unwrap().unwrap().to_vec();
        assert_eq!((seen, reader.counters().blocks_loaded), (b"3".to_vec(), 1));
        assert_eq!(value(&engine, b"a").as_deref(), Some(&b"4"[..]));
    }

    /// A merge that leaves a newer level-0 run out leaves it over the
    /// output's range: here the output holds "j" and an older "k", and the
    /// run left out holds the newest "k", and reads decide by sequence.
    #[test]
    fn a_partial_compaction_leaves_interleaved_runs_that_read_correctly() {
        let engine = Engine::in_memory(manual()).unwrap();
        engine.ks().put(b"j", b"j1").unwrap();
        engine.ks().put(b"k", b"k1").unwrap();
        engine.flush().unwrap();
        engine.ks().put(b"j", b"j2").unwrap();
        engine.ks().put(b"k", b"k2").unwrap();
        for i in 0..200u32 {
            engine
                .ks()
                .put(format!("x{i:04}").as_bytes(), &[7; 64])
                .unwrap();
        }
        engine.flush().unwrap();
        engine.ks().put(b"k", b"k3").unwrap();
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
        assert_eq!(engine.engine_stats().runs, 2);
        // "k": the newer run decides, before the output's older version.
        // "j": only the output's range holds it.
        for (value, blocks) in read_counting(&engine, b"k") {
            assert_eq!((value.as_deref(), blocks), (Some(&b"k3"[..]), 1));
        }
        for (value, blocks) in read_counting(&engine, b"j") {
            assert_eq!((value.as_deref(), blocks), (Some(&b"j2"[..]), 1));
        }
        for (key, expected) in [(&b"j"[..], &b"j2"[..]), (b"k", b"k3")] {
            assert_eq!(before.value(key).unwrap(), expected);
        }
    }

    /// Compactions are serialized, so in the engine nothing else removes a
    /// run. Publication still checks rather than resurrect a removed run's
    /// records.
    #[test]
    #[should_panic(expected = "compactions are serialized")]
    fn publication_checks_that_its_inputs_are_published() {
        let engine = Engine::in_memory(manual()).unwrap();
        overlapping_runs(&engine);
        let built = engine
            .core
            .begin_job(options(8, usize::MAX))
            .unwrap()
            .unwrap()
            .build(&mut MergeWorkspace::new())
            .unwrap();
        engine.core.write_state().only_mut().runs.remove(1);
        let _ = built.publish();
    }

    /// Building takes no lock: it completes while a waiting writer holds the
    /// gate that every reader must pass, which would block it forever if it
    /// tried to read the state. A failure shows as a hang, which the
    /// validation timeout stops.
    #[test]
    fn building_a_compaction_takes_no_lock() {
        let engine = Engine::in_memory(manual()).unwrap();
        overlapping_runs(&engine);
        let job = engine
            .core
            .begin_job(options(8, usize::MAX))
            .unwrap()
            .unwrap();
        let ks = engine.ks();
        let (held, release) = (std::sync::Barrier::new(2), std::sync::Barrier::new(2));
        std::thread::scope(|scope| {
            let reader = scope.spawn(|| {
                let guard = engine.core.read_state();
                held.wait();
                release.wait();
                drop(guard);
            });
            held.wait();
            let writer = scope.spawn(|| ks.put(b"m", b"new").unwrap());
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
        assert_eq!(engine.engine_stats().runs, 1);
    }

    #[test]
    fn publication_frees_the_inputs_once_nothing_holds_them() {
        let engine = Engine::in_memory(manual()).unwrap();
        overlapping_runs(&engine);
        let inputs: Vec<SharedRun> = engine.core.read_state().only().runs.clone();
        let mut workspace = MergeWorkspace::new();
        while engine
            .core
            .compact(options(8, usize::MAX), &mut workspace)
            .unwrap()
            .is_some()
        {}
        assert_eq!(engine.engine_stats().runs, 1);
        crate::test_support::wait_until(|| inputs.iter().all(|run| Arc::strong_count(run) == 1));
        assert!(inputs.iter().all(|run| Arc::strong_count(run) == 1));
    }

    // Background compaction.

    use std::sync::Barrier;
    use std::sync::atomic::Ordering;

    fn background(max_overlap: usize) -> Options {
        Options {
            background_compaction: Some(CompactionPolicy {
                max_overlap,
                ..CompactionPolicy::default()
            }),
            ..manual()
        }
    }

    fn value(engine: &Engine, key: &[u8]) -> Option<Vec<u8>> {
        engine.ks().get(key).unwrap()
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
        let engine = Engine::in_memory(background(1)).unwrap();
        assert_eq!(engine.background_status().unwrap().passes, 0);
        overlapping_runs(&engine);
        engine.wait_for_compaction().unwrap();
        let status = engine.background_status().unwrap();
        assert!(status.running && !status.busy);
        assert!(status.compactions >= 1);
        assert!(status.passes <= 4, "{status:?}");
        assert_eq!(engine.engine_stats().runs, 1);
        assert_eq!(engine.engine_stats().max_overlap, 1);
        assert_eq!(value(&engine, b"a").unwrap(), b"3");
        // With nothing requested, the worker parks idle and runs no more passes.
        let core = Arc::clone(&engine.core);
        crate::test_support::wait_until(|| core.background.as_ref().unwrap().is_idle_parked());
        assert_eq!(engine.background_status().unwrap(), status);
        engine.close().unwrap();
        let stopped = core.background.as_ref().unwrap().status();
        assert!(!stopped.running && !stopped.busy);
        assert_eq!(stopped.passes, status.passes);
        assert_eq!(stopped.compactions, status.compactions);
    }

    /// Requests made while the worker cannot run count as one more pass.
    #[test]
    fn requests_coalesce_while_the_worker_is_busy() {
        let engine = Engine::in_memory(background(1)).unwrap();
        let slot = engine.core.compacting.lock().unwrap();
        for round in 0..10 {
            engine.ks().put(b"a", &[round]).unwrap();
            engine.ks().put(b"z", &[round]).unwrap();
            engine.flush().unwrap();
        }
        drop(slot);
        engine.wait_for_compaction().unwrap();
        let status = engine.background_status().unwrap();
        // Eleven requests: at most the pass held up by the slot, one for the
        // flushes after it started, and one for the wait. A worker that wakes
        // late serves them all in one.
        assert!((1..=3).contains(&status.passes), "{status:?}");
        assert_eq!(engine.engine_stats().runs, 1);
    }

    /// While the worker holds a built compaction, reads see the inputs and a
    /// flush publishes beside them; publication keeps the flushed run.
    #[test]
    fn reads_and_flushes_proceed_while_the_worker_compacts() {
        let engine = Engine::in_memory(background(2)).unwrap();
        let (built, release) = pause_after_build(&engine);
        overlapping_runs(&engine);
        built.wait();
        assert_eq!(engine.engine_stats().runs, 3);
        assert_eq!(value(&engine, b"z").unwrap(), b"3");
        engine.ks().delete(b"a").unwrap();
        engine.ks().put(b"m", b"new").unwrap();
        engine.flush().unwrap();
        assert_eq!(engine.engine_stats().runs, 4);
        *engine.core.after_build.lock().unwrap() = None;
        release.wait();
        engine.wait_for_compaction().unwrap();

        assert_eq!(value(&engine, b"a"), None);
        assert_eq!(value(&engine, b"m").unwrap(), b"new");
        assert_eq!(value(&engine, b"z").unwrap(), b"3");
        assert!(engine.engine_stats().max_overlap <= 2);
    }

    #[test]
    fn dropping_an_idle_engine_joins_the_worker_and_frees_the_core() {
        let engine = Engine::in_memory(background(1)).unwrap();
        overlapping_runs(&engine);
        engine.wait_for_compaction().unwrap();
        // The engine, its shared state, both workers and this test.
        let core = Arc::clone(&engine.core);
        assert_eq!(Arc::strong_count(&core), 5);
        drop(engine);
        assert_eq!(Arc::strong_count(&core), 1);
        let status = core.background.as_ref().unwrap().status();
        assert!(!status.running && status.panic.is_none());
    }

    /// Dropping the engine mid-compaction waits for that compaction, which is
    /// published, and starts no other.
    #[test]
    fn dropping_during_a_compaction_publishes_it_and_then_stops() {
        let engine = Engine::in_memory(background(1)).unwrap();
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
        let engine = Engine::in_memory(background(1)).unwrap();
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
            closer.join().unwrap().unwrap();
        });
        assert_eq!(Arc::strong_count(&core), 1);
        let status = core.background.as_ref().unwrap().status();
        // The oldest run's move, then the merge that was held.
        assert_eq!((status.running, status.compactions), (false, 2));
        assert_eq!(core.stats().runs, 1);
        assert_eq!(snapshot.value(b"a").unwrap(), b"3");
    }

    /// A worker that panicked is reported by `close` with its message, and
    /// the engine is released all the same; dropping one does not panic.
    #[test]
    fn closing_after_a_worker_panic_reports_it_and_releases_the_engine() {
        for close in [true, false] {
            let engine = Engine::in_memory(background(1)).unwrap();
            engine.core.panic_worker.store(true, Ordering::Relaxed);
            overlapping_runs(&engine);
            let panicked = Err(BackgroundError::Panicked("injected worker failure".into()));
            assert_eq!(engine.wait_for_compaction(), panicked);
            let core = Arc::clone(&engine.core);
            if close {
                let error = engine.close().unwrap_err();
                assert_eq!(error.kind(), ErrorKind::Background);
                assert!(
                    error.to_string().contains("injected worker failure"),
                    "{error}"
                );
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
        let engine = Engine::in_memory(manual()).unwrap();
        assert_eq!(engine.background_status(), None);
        drop(engine);
        let refused = Engine::in_memory(Options::default()).map(drop);
        let Err(error) = refused else {
            panic!("{refused:?}");
        };
        assert_eq!(error.kind(), ErrorKind::Io);
        let source = std::error::Error::source(&error).unwrap();
        assert!(
            source.to_string().contains("injected spawn failure"),
            "{error}: {source}"
        );
        // The failure was injected once; the next engine starts its worker.
        let engine = Engine::in_memory(Options::default()).unwrap();
        assert!(engine.background_status().unwrap().running);
        engine.close().unwrap();
    }

    /// A panic stops the worker for good and is reported; the engine keeps
    /// working without it and is still dropped cleanly.
    #[test]
    fn a_panicked_worker_is_reported_and_the_engine_stays_usable() {
        let engine = Engine::in_memory(background(1)).unwrap();
        engine.core.panic_worker.store(true, Ordering::Relaxed);
        overlapping_runs(&engine);
        assert_eq!(
            engine.wait_for_compaction(),
            Err(BackgroundError::Panicked("injected worker failure".into()))
        );
        let status = engine.background_status().unwrap();
        assert!(!status.running);
        assert_eq!(status.panic.as_deref(), Some("injected worker failure"));

        engine.ks().put(b"k", b"v").unwrap();
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

    #[test]
    fn pooled_readers_trim_large_decoding_and_result_buffers() {
        let engine = Engine::in_memory(Options {
            memtable_size: 4 << 20,
            compression: Compression::Zstd,
            ..manual()
        })
        .unwrap();
        let ks = engine.ks();
        // A >1 MiB compressible value produces a >1 MiB decoded block.
        let big = vec![b'x'; (1 << 20) + 4096];
        ks.put(b"big", &big).unwrap();
        ks.put(b"small", b"tiny").unwrap();
        engine.flush().unwrap();

        {
            let mut reader = engine.reader();
            assert_eq!(reader.get(&ks, b"big").unwrap(), Some(big.as_slice()));
            assert!(reader.buffers.logical.capacity() > (1 << 20));
            assert!(reader.value_buffer().1 > (1 << 20));

            let got = reader.multi_get(&ks, &[b"big"]).unwrap();
            assert_eq!(got[0], Some(big.as_slice()));
            drop(got);
            assert!(reader.buffers.multi.capacity() > (1 << 20));

            let mut scan = reader.range(&ks, ..);
            assert_eq!(scan.next().unwrap(), Some((&b"big"[..], big.as_slice())));
            drop(scan);
        }

        // The returned buffers must have trimmed every >1 MiB allocation.
        let mut reused = engine.reader();
        assert_eq!(reused.buffers.logical.capacity(), 0);
        assert_eq!(reused.buffers.multi.capacity(), 0);
        assert_eq!(reused.value_buffer().1, 0);

        // Normal-sized buffers stay pooled across readers.
        assert_eq!(reused.get(&ks, b"small").unwrap(), Some(&b"tiny"[..]));
        let warm_cap = reused.buffers.logical.capacity();
        assert!(warm_cap > 0);
        drop(reused);
        let reused_again = engine.reader();
        assert_eq!(reused_again.buffers.logical.capacity(), warm_cap);
    }

    #[test]
    fn debug_format_engine_does_not_block_or_double_panic() {
        let engine = Engine::in_memory(manual()).unwrap();
        let _ks = engine.ks();
        assert!(format!("{engine:?}").contains("test"));

        let guard = engine.core.write_state();
        assert!(format!("{engine:?}").contains("<locked>"));
        drop(guard);

        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _guard = engine.core.write_state();
            panic!("poison state lock");
        }));
        assert!(format!("{engine:?}").contains("<poisoned>"));
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
#[path = "../tests/keyspaces.rs"]
mod api_keyspaces_tests;
#[cfg(test)]
#[path = "../tests/snapshot.rs"]
mod api_snapshot_tests;
#[cfg(test)]
#[path = "../tests/api.rs"]
mod api_tests;
