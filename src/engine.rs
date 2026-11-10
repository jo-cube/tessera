//! A minimal in-memory key-value engine over memtables and runs.
//!
//! An [`Engine`] owns one active memtable, at most one frozen memtable awaiting
//! flush, the published immutable runs, and the sequence counter:
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
//! All state sits behind one reader-writer lock. A write holds it exclusively
//! while taking its sequence number and installing the update, so sequence
//! order is visibility order and each successful write has one position in it.
//! A read holds the lock shared, so reads and writes of a key are linearizable.
//!
//! A [`ReadGuard`] holds the lock for as long as it lives, so everything read
//! through it, including a whole scan, sees one state. **Writers and flush
//! publication wait while a guard is held**, so a long scan blocks them. A
//! waiting writer holds back new readers, so readers cannot starve it, and
//! each write waits for at most the guards already held.
//! Memtables keep only the latest update per key, so a sequence number could
//! not recreate an earlier state; holding the lock is what makes a scan
//! consistent.
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
//! including in a caller holding a [`ReadGuard`] or a flush job, leaves the
//! engine usable.
//!
//! # Flushing
//!
//! Flushing is explicit. [`Engine::flush`] does the whole job, and
//! [`Engine::begin_flush`] exposes its stages:
//!
//! 1. The active memtable is frozen and replaced, under the lock.
//! 2. Runs are built without the lock, so reads and writes continue.
//! 3. The runs are published and the frozen table is removed in one step under
//!    the lock.
//!
//! The frozen table stays readable throughout, and no reader sees its runs
//! until they replace it. A flush abandoned or rejected at any stage leaves the
//! frozen table in place, and the next flush retries it.
//!
//! # Compaction
//!
//! Compaction is explicit and synchronous. [`Engine::compact`] does one, and
//! [`Engine::begin_compaction`] exposes the same stages as a flush: inputs are
//! selected under the shared lock, merged without any lock, and replaced by the
//! output in one step under the exclusive lock. Readers see either all of the
//! inputs or all of the output. Compactions run one at a time; flushes, reads
//! and writes continue beside them.
//!
//! Only the newest version of each key is kept, which is correct because no
//! reader can ask for an older one. A tombstone is dropped only if no run
//! outside the inputs could hold an older value under its key. Runs published
//! after selection come from flushes and are newer than every input, and the
//! memtables are newer still, so neither can be harmed by the replacement.
//! Publication checks that every input is still published, and a compaction
//! that fails at any stage changes nothing.
//!
//! # Memory
//!
//! Mutable memory is limited by two memtable budgets, the active and at most
//! one frozen table. When the active table is full a write is rejected with
//! [`WriteError::BudgetExceeded`] and the caller should flush.
//!
//! Published runs are limited by a count and a byte total. Once a flush would
//! exceed either limit it is rejected, and writes stop when the memtables are
//! full. Compaction can make room, and [`Engine::set_run_limits`] raises the
//! limits.
//!
//! A compaction adds, until it publishes, its output and workspace, each
//! limited by [`CompactionOptions`]. Its inputs stay allocated until it
//! publishes and any reader holding them is done, so the run bytes can briefly
//! be the published total plus the output. Run limits are checked against the
//! total after replacement.
//!
//! These limits use the memtable's estimated charges, stored run lengths and
//! an estimate of workspace. They are not a limit on process memory.

use std::fmt;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError, RwLock, RwLockReadGuard, RwLockWriteGuard};

use crate::block::{Record, Value, Version};
use crate::memtable::{FrozenMemtable, MemoryUsage, Memtable, Records, TARGET_RUN_LEN, WriteError};
use crate::run::{
    CompactError, CompactLimits, CompactStats, LiveRecord, LiveValue, LogicalCursor,
    LogicalWorkspace, MergeWorkspace, ReadError, Run, RunView, compact, latest_in,
};

type SharedRun = Arc<Published>;

/// A published run.
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

/// The configuration of an [`Engine`].
#[derive(Clone, Copy, Debug)]
pub struct EngineOptions {
    /// Charged bytes each memtable may hold.
    pub memtable_budget: usize,
    /// Input per run when flushing. See [`FrozenMemtable::flush`].
    pub target_run_len: usize,
    /// The most runs that may be published.
    pub max_runs: usize,
    /// The most stored run bytes that may be published.
    pub max_run_bytes: usize,
    /// The sequence number to continue after. The first write uses the next.
    pub last_seq: u64,
}

impl Default for EngineOptions {
    /// Provisional limits: 64 MiB memtables, 64 runs and 4 GiB of runs.
    fn default() -> Self {
        Self {
            memtable_budget: 64 << 20,
            target_run_len: TARGET_RUN_LEN,
            max_runs: 64,
            max_run_bytes: 4 << 30,
            last_seq: 0,
        }
    }
}

/// Why a write was rejected. The engine is unchanged, and no sequence number
/// was used.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum UpdateError {
    /// The memtable refused the update. If it is
    /// [`WriteError::BudgetExceeded`], flushing makes room.
    Rejected(WriteError),
    /// Every sequence number has been used.
    SequencesExhausted,
}

impl fmt::Display for UpdateError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Rejected(error) => error.fmt(f),
            Self::SequencesExhausted => f.write_str("sequence numbers are exhausted"),
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
    /// Stored bytes that would be published.
    pub run_bytes: usize,
    /// The stored byte limit.
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
}

/// A summary of an engine's state.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct EngineStats {
    /// The last sequence number used.
    pub last_seq: u64,
    /// The active memtable's accounting.
    pub active: MemoryUsage,
    /// The frozen memtable's accounting, if one is awaiting flush.
    pub frozen: Option<MemoryUsage>,
    /// Published runs.
    pub runs: usize,
    /// Stored bytes of the published runs.
    pub run_bytes: usize,
}

struct State {
    last_seq: u64,
    active: Memtable,
    /// Its sequence numbers are all above the runs' and below `active`'s.
    frozen: Option<Arc<FrozenMemtable>>,
    /// Shared so that a compaction can read its inputs without the lock while
    /// flushes publish beside them.
    runs: Vec<SharedRun>,
    run_bytes: usize,
    max_runs: usize,
    max_run_bytes: usize,
}

/// A minimal key-value engine. See the [module documentation](self) for its
/// consistency and flushing rules.
pub struct Engine {
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
    memtable_budget: usize,
    target_run_len: usize,
}

impl Engine {
    /// Creates an empty engine.
    pub fn new(options: EngineOptions) -> Self {
        Self {
            state: RwLock::new(State {
                last_seq: options.last_seq,
                active: Memtable::new(options.memtable_budget, Some(options.last_seq)),
                frozen: None,
                runs: Vec::new(),
                run_bytes: 0,
                max_runs: options.max_runs,
                max_run_bytes: options.max_run_bytes,
            }),
            gate: Mutex::new(()),
            flushing: Mutex::new(()),
            compacting: Mutex::new(()),
            memtable_budget: options.memtable_budget,
            target_run_len: options.target_run_len,
        }
    }

    /// Sets `key` to `value` and returns the update's sequence number.
    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<u64, UpdateError> {
        self.update(key, Value::Bytes(value))
    }

    /// Deletes `key` and returns the update's sequence number.
    pub fn delete(&self, key: &[u8]) -> Result<u64, UpdateError> {
        self.update(key, Value::Tombstone)
    }

    fn update(&self, key: &[u8], value: Value<'_>) -> Result<u64, UpdateError> {
        let mut state = self.write_state();
        let seq = state
            .last_seq
            .checked_add(1)
            .ok_or(UpdateError::SequencesExhausted)?;
        let written = match value {
            Value::Bytes(bytes) => state.active.put(key, seq, bytes),
            Value::Tombstone => state.active.delete(key, seq),
        };
        written.map_err(UpdateError::Rejected)?;
        // Set only now: a rejected write must not use up a number.
        state.last_seq = seq;
        Ok(seq)
    }

    /// Returns the latest value of `key`, or `None` if it is absent or
    /// deleted. The value is copied into `workspace`, once per call.
    pub fn get<'w>(
        &self,
        key: &[u8],
        workspace: &'w mut EngineWorkspace,
    ) -> Result<Option<LiveValue<'w>>, ReadError> {
        let EngineWorkspace { logical, value, .. } = workspace;
        let state = self.read_state();
        let Some(found) = lookup(&state, key, logical)? else {
            return Ok(None);
        };
        let seq = found.seq;
        value.clear();
        value.extend_from_slice(found.value);
        Ok(Some(LiveValue { seq, value }))
    }

    /// Takes the shared lock and returns a stable view of the engine.
    ///
    /// Writers and flush publication wait until it is dropped. See the
    /// [module documentation](self) for what must not be done while holding it.
    pub fn read(&self) -> ReadGuard<'_> {
        ReadGuard(self.read_state())
    }

    /// Flushes until everything written before this call is in runs.
    ///
    /// A frozen table left by an earlier flush is flushed first, then the
    /// active table. An empty engine does nothing.
    pub fn flush(&self) -> Result<FlushReport, FlushError> {
        let mut total = FlushReport::default();
        while let Some(job) = self.begin_flush() {
            let includes_active = !job.retried;
            let report = job.build().publish()?;
            total.tables += report.tables;
            total.records += report.records;
            total.runs += report.runs;
            total.run_bytes += report.run_bytes;
            if includes_active {
                break;
            }
        }
        Ok(total)
    }

    /// Starts a flush, freezing the active table unless a frozen table is
    /// waiting to be retried. Returns `None` if there is nothing to flush.
    ///
    /// Waits for any flush already in progress, including one this thread
    /// holds, so drop or publish a job before starting another.
    pub fn begin_flush(&self) -> Option<FlushJob<'_>> {
        // This mutex guards no data, so a panic elsewhere leaves nothing to
        // distrust, and the flush must stay retryable.
        let serial = self.flushing.lock().unwrap_or_else(PoisonError::into_inner);
        let mut state = self.write_state();
        let (frozen, retried) = match &state.frozen {
            Some(frozen) => (Arc::clone(frozen), true),
            None if state.active.is_empty() => return None,
            None => {
                let successor = Memtable::new(self.memtable_budget, Some(state.last_seq));
                let frozen = Arc::new(std::mem::replace(&mut state.active, successor).freeze());
                state.frozen = Some(Arc::clone(&frozen));
                (frozen, false)
            }
        };
        drop(state);
        Some(FlushJob {
            engine: self,
            _serial: serial,
            frozen,
            retried,
        })
    }

    /// Compacts once with [`Engine::begin_compaction`]'s policy, building in
    /// `workspace`. Returns `None` if the policy finds nothing to do.
    pub fn compact(
        &self,
        options: CompactionOptions,
        workspace: &mut MergeWorkspace,
    ) -> Result<Option<CompactionReport>, CompactionError> {
        match self.begin_compaction(options)? {
            Some(job) => job.build(workspace)?.publish().map(Some),
            None => Ok(None),
        }
    }

    /// Selects runs to compact, or returns `None` if nothing needs it.
    ///
    /// If some key lies in the ranges of more than
    /// [`CompactionOptions::max_overlap`] runs, the runs covering the most
    /// deeply covered key are the candidates, and the smallest of them are
    /// taken within the input limits. They all share that key, so the merge
    /// lowers the overlap there and, since its output is disjoint and stays
    /// within their combined range, raises it nowhere.
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
    /// Waits for any compaction in progress, including one this thread holds.
    pub fn begin_compaction(
        &self,
        options: CompactionOptions,
    ) -> Result<Option<CompactionJob<'_>>, CompactionError> {
        // As for flushing, this mutex guards no data.
        let serial = self
            .compacting
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        let state = self.read_state();
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

    /// Replaces the limits on published runs.
    pub fn set_run_limits(&self, max_runs: usize, max_run_bytes: usize) {
        let mut state = self.write_state();
        state.max_runs = max_runs;
        state.max_run_bytes = max_run_bytes;
    }

    /// Returns a summary of the current state.
    pub fn stats(&self) -> EngineStats {
        let state = self.read_state();
        EngineStats {
            last_seq: state.last_seq,
            active: state.active.memory(),
            frozen: state.frozen.as_ref().map(|frozen| frozen.memory()),
            runs: state.runs.len(),
            run_bytes: state.run_bytes,
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

const LOCK: &str = "a write section holding the state lock panicked";
const BUILT: &str = "published runs were built by the engine";
/// Flushes publish only non-empty tables, and compaction builds a run only
/// once it has a record.
const NOT_EMPTY: &str = "published runs are not empty";

impl fmt::Debug for Engine {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Engine")
            .field("stats", &self.stats())
            .finish_non_exhaustive()
    }
}

/// A flush whose table is frozen and whose runs are not built yet.
///
/// Dropping it abandons the flush and keeps the table frozen. Holding it
/// blocks other flushes, but not reads or writes.
pub struct FlushJob<'e> {
    engine: &'e Engine,
    _serial: MutexGuard<'e, ()>,
    frozen: Arc<FrozenMemtable>,
    retried: bool,
}

impl<'e> FlushJob<'e> {
    /// Builds the runs, compressing without the state lock.
    pub fn build(self) -> BuiltFlush<'e> {
        let runs = self.frozen.flush(self.engine.target_run_len).collect();
        BuiltFlush { job: self, runs }
    }

    /// Returns the frozen table being flushed.
    pub fn table(&self) -> &FrozenMemtable {
        &self.frozen
    }
}

impl fmt::Debug for FlushJob<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FlushJob")
            .field("records", &self.frozen.len())
            .field("retried", &self.retried)
            .finish_non_exhaustive()
    }
}

/// A flush with its runs built but not visible to anyone.
pub struct BuiltFlush<'e> {
    job: FlushJob<'e>,
    runs: Vec<Run<Vec<u8>>>,
}

impl BuiltFlush<'_> {
    /// Returns the runs built, in key order.
    pub fn runs(&self) -> &[Run<Vec<u8>>] {
        &self.runs
    }

    /// Publishes the runs and removes the frozen table in one step.
    ///
    /// On a [`FlushError`] the built runs are dropped and the table stays
    /// frozen and readable.
    pub fn publish(self) -> Result<FlushReport, FlushError> {
        let Self { job, runs } = self;
        let report = FlushReport {
            tables: 1,
            records: job.frozen.len(),
            runs: runs.len(),
            run_bytes: runs.iter().map(|run| run.as_bytes().len()).sum(),
        };
        let mut state = job.engine.write_state();
        let total_runs = state.runs.len() + report.runs;
        let total_bytes = state.run_bytes + report.run_bytes;
        if total_runs > state.max_runs || total_bytes > state.max_run_bytes {
            return Err(FlushError {
                runs: total_runs,
                max_runs: state.max_runs,
                run_bytes: total_bytes,
                max_run_bytes: state.max_run_bytes,
            });
        }
        state.runs.extend(published(runs));
        state.run_bytes = total_bytes;
        state.frozen = None;
        // `job` holds the last reference, so the table is freed after this
        // releases the lock.
        drop(state);
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

/// Bounds and policy for one compaction. See [`Engine::begin_compaction`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CompactionOptions {
    /// The most runs merged at once.
    pub max_inputs: usize,
    /// The most stored bytes of input runs.
    pub max_input_bytes: usize,
    /// The most stored bytes of output, built and held until publication.
    pub max_output_bytes: usize,
    /// The most estimated decoded-block, builder and codec-context bytes.
    /// See [`crate::run::workspace_bytes`].
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
    /// An input was no longer published when the output was ready.
    InputsChanged,
    /// Publishing would leave more runs or run bytes than both the engine's
    /// limit and what is published now.
    RunLimit(FlushError),
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
            Self::InputsChanged => f.write_str("compaction inputs changed before publication"),
            Self::RunLimit(error) => error.fmt(f),
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
    /// Runs published in their place.
    pub outputs: usize,
    /// Their stored bytes.
    pub output_bytes: usize,
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
/// [`Engine::begin_compaction`]. Quadratic in the number of runs.
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

    if deepest > options.max_overlap.max(1) {
        stacks.retain(|(_, stack)| stack.len() > options.max_overlap.max(1));
        stacks.sort_by(|(a, x), (b, y)| (y.len(), bytes(x), *a).cmp(&(x.len(), bytes(y), *b)));
        let mut smallest_pair = usize::MAX;
        for (_, mut stack) in stacks {
            stack.sort_by_key(|&index| (runs[index].bytes, index));
            let picked = fill(&stack, runs, options);
            if picked.len() >= 2 {
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
/// blocks other compactions, but not reads, writes or flushes. Dropping it
/// abandons the compaction and changes nothing.
pub struct CompactionJob<'e> {
    engine: &'e Engine,
    _serial: MutexGuard<'e, ()>,
    inputs: Vec<SharedRun>,
    /// Unselected runs whose key ranges overlap the inputs' span. They may
    /// hold older versions that the inputs' tombstones hide.
    others: Vec<SharedRun>,
    input_bytes: usize,
    options: CompactionOptions,
}

impl<'e> CompactionJob<'e> {
    /// Returns the runs selected.
    pub fn inputs(&self) -> impl ExactSizeIterator<Item = &Run<Vec<u8>>> {
        self.inputs.iter().map(|run| &run.run)
    }

    /// Returns whether no other run overlaps the inputs, so that every
    /// tombstone can be dropped.
    pub fn is_complete(&self) -> bool {
        self.others.is_empty()
    }

    /// Merges the inputs without the state lock.
    ///
    /// A tombstone is kept if any other run overlapping the inputs has a key
    /// range covering its key, since that run may hold an older value; runs
    /// published later come from flushes and hold only newer updates. On an
    /// error nothing is built and the engine is unchanged.
    pub fn build(
        self,
        workspace: &mut MergeWorkspace,
    ) -> Result<BuiltCompaction<'e>, CompactionError> {
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
        };
        let output = compact(&views, keep_tombstone, limits, workspace)?;
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
pub struct BuiltCompaction<'e> {
    job: CompactionJob<'e>,
    output: crate::run::Compacted,
}

impl BuiltCompaction<'_> {
    /// Returns the runs built, in key order.
    pub fn runs(&self) -> &[Run<Vec<u8>>] {
        &self.output.runs
    }

    /// Replaces the inputs with the output in one step under the lock.
    ///
    /// Every input must still be published. Runs published since selection
    /// are kept. A result over the run limits is accepted if it is no larger
    /// than what it replaces. On an error the output is dropped and the engine is
    /// unchanged. The inputs are freed after the lock is released, once no
    /// reader holds them.
    pub fn publish(self) -> Result<CompactionReport, CompactionError> {
        let Self { job, output } = self;
        let report = CompactionReport {
            inputs: job.inputs.len(),
            input_bytes: job.input_bytes,
            outputs: output.runs.len(),
            output_bytes: output.output_bytes,
            complete: job.is_complete(),
            stats: output.stats,
            workspace_bytes: output.workspace_bytes,
        };
        let is_input = |run: &SharedRun| job.inputs.iter().any(|input| Arc::ptr_eq(input, run));
        let mut state = job.engine.write_state();
        if state.runs.iter().filter(|run| is_input(run)).count() != job.inputs.len() {
            return Err(CompactionError::InputsChanged);
        }
        let total_runs = state.runs.len() - report.inputs + report.outputs;
        let total_bytes = state.run_bytes - report.input_bytes + report.output_bytes;
        // Shrinking is always allowed, so that compaction can make room even
        // when the limits were lowered below what is published.
        let grows_runs = total_runs > state.max_runs.max(state.runs.len());
        let grows_bytes = total_bytes > state.max_run_bytes.max(state.run_bytes);
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
        state.run_bytes = total_bytes;
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
        lookup(&self.0, key, &mut workspace.logical)
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
        let EngineWorkspace {
            logical,
            prefix: bound,
            ..
        } = workspace;
        bound.clear();
        bound.extend_from_slice(prefix);
        let prefix: &'r [u8] = bound;

        let state = &*self.0;
        let mut runs = LogicalCursor::over(state.runs.iter().map(|run| run.run.view()), logical);
        // A failed seek is repeated by the first read of the cursor.
        let _ = runs.seek_prefix(prefix);

        let mut active = state.active.iter_from(prefix);
        let mut frozen = state.frozen.as_deref().map(|table| table.iter_from(prefix));
        Scan {
            active_head: next_within(&mut active, prefix),
            frozen_head: frozen.as_mut().and_then(|table| next_within(table, prefix)),
            active,
            frozen,
            runs,
            runs_stale: true,
            prefix,
        }
    }
}

impl fmt::Debug for ReadGuard<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ReadGuard").finish_non_exhaustive()
    }
}

/// Newest first: every key's updates are in `active`, then `frozen`, then the
/// runs, so the first source holding a key decides it.
fn lookup<'r>(
    state: &'r State,
    key: &[u8],
    logical: &'r mut LogicalWorkspace,
) -> Result<Option<LiveValue<'r>>, ReadError> {
    let in_memory = state
        .active
        .get(key)
        .or_else(|| state.frozen.as_deref()?.get(key));
    match in_memory {
        Some(Version {
            seq,
            value: Value::Bytes(value),
        }) => Ok(Some(LiveValue { seq, value })),
        Some(_) => Ok(None),
        None => latest_in(state.runs.iter().map(|run| run.run.view()), key, logical),
    }
}

fn next_within<'a>(records: &mut Records<'a>, prefix: &[u8]) -> Option<Record<'a>> {
    records
        .next()
        .filter(|record| record.key.starts_with(prefix))
}

/// Iterates live keys in ascending order, each once with its latest value.
///
/// This merges the active table, the frozen table and the live keys of the
/// runs. A table's update beats anything in the runs, so a tombstone in a table
/// hides the key whatever the runs hold, and values are not copied.
pub struct Scan<'r> {
    active: Records<'r>,
    frozen: Option<Records<'r>>,
    active_head: Option<Record<'r>>,
    frozen_head: Option<Record<'r>>,
    runs: LogicalCursor<'r>,
    /// Whether the record the runs stand on has been used.
    runs_stale: bool,
    prefix: &'r [u8],
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
            let smallest = [
                self.active_head.map(|record| record.key),
                self.frozen_head.map(|record| record.key),
                run_key,
            ]
            .into_iter()
            .flatten()
            .min();
            let Some(key) = smallest else {
                return Ok(None);
            };

            let active = self.active_head.filter(|record| record.key == key);
            let frozen = self.frozen_head.filter(|record| record.key == key);
            self.runs_stale = run_key == Some(key);
            if active.is_some() {
                self.active_head = next_within(&mut self.active, self.prefix);
            }
            if frozen.is_some() {
                self.frozen_head = self
                    .frozen
                    .as_mut()
                    .and_then(|table| next_within(table, self.prefix));
            }

            match active.or(frozen) {
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

#[cfg(test)]
mod tests {
    use super::*;

    fn frozen_handle(engine: &Engine) -> Arc<FrozenMemtable> {
        Arc::clone(engine.read_state().frozen.as_ref().unwrap())
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
            while engine.gate.try_lock().is_ok() {
                std::thread::yield_now();
            }
            for _ in 0..100 {
                assert!(engine.gate.try_lock().is_err());
                assert!(!writer.is_finished());
                std::thread::yield_now();
            }
            drop(held);
            writer.join().unwrap();
        });
        assert!(engine.gate.try_lock().is_ok());
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
        let job = engine.begin_flush().unwrap();
        let held = frozen_handle(&engine);
        assert_eq!(Arc::strong_count(&held), 3);
        job.build().publish().unwrap();
        assert_eq!(Arc::strong_count(&held), 1);
    }

    #[test]
    fn an_abandoned_flush_releases_only_its_own_reference() {
        let engine = Engine::new(EngineOptions::default());
        engine.put(b"k", b"v").unwrap();
        let job = engine.begin_flush().unwrap();
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
        let job = engine.begin_flush().unwrap();
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

    /// Compactions are serialized, so in the engine nothing else removes a
    /// run. Publication still checks, and must change nothing if one went.
    #[test]
    fn publication_fails_if_an_input_is_no_longer_published() {
        let engine = Engine::new(EngineOptions::default());
        overlapping_runs(&engine);
        let built = engine
            .begin_compaction(options(8, usize::MAX))
            .unwrap()
            .unwrap()
            .build(&mut MergeWorkspace::new())
            .unwrap();
        let removed = engine.write_state().runs.remove(1);
        let stats = engine.stats();
        assert_eq!(built.publish(), Err(CompactionError::InputsChanged));
        assert_eq!(engine.stats(), stats);
        assert_eq!(Arc::strong_count(&removed), 1);
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
            while engine.gate.try_lock().is_ok() {
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
        let inputs: Vec<SharedRun> = engine.read_state().runs.clone();
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
                .begin_compaction(options(8, usize::MAX))
                .unwrap()
                .unwrap()
        };
        let mut built = begin().build(&mut MergeWorkspace::new()).unwrap();
        built.output.output_bytes = stats.run_bytes + 1;
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
}
