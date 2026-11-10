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
//! # Memory
//!
//! Mutable memory is limited by two memtable budgets, the active and at most
//! one frozen table. When the active table is full a write is rejected with
//! [`WriteError::BudgetExceeded`] and the caller should flush.
//!
//! Published runs are limited by a count and a byte total. Nothing removes a
//! run yet, so once a flush would exceed either limit it is rejected, and
//! writes stop when the memtables are full. [`Engine::set_run_limits`] raises
//! the limits.
//!
//! These limits use the memtable's estimated charges and stored run lengths.
//! They are not a limit on process memory.

use std::fmt;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError, RwLock, RwLockReadGuard, RwLockWriteGuard};

use crate::block::{Record, Value, Version};
use crate::memtable::{FrozenMemtable, MemoryUsage, Memtable, Records, TARGET_RUN_LEN, WriteError};
use crate::run::{
    LiveRecord, LiveValue, LogicalCursor, LogicalWorkspace, ReadError, Run, latest_in,
};

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
    runs: Vec<Run<Vec<u8>>>,
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
        state.runs.extend(runs);
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
        let mut runs = LogicalCursor::over(state.runs.iter().map(Run::view), logical);
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
        None => latest_in(state.runs.iter().map(Run::view), key, logical),
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
}
