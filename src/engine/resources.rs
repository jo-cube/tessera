//! Writer admission and accounting of temporary maintenance allocations.
//! Shared sources are charged once; build estimates reserve transient room.
//! Maintenance can finish above the budget so it can reclaim writer space.

use super::*;
use std::sync::Condvar;
use std::sync::atomic::Ordering;

#[derive(Default)]
pub(super) struct Progress {
    event: Mutex<Estimates>,
    changed: Condvar,
    pub(super) outputs: Arc<crate::run::OutputCounters>,
    #[cfg(feature = "diagnostics")]
    pub(super) writer_wait_ns: std::sync::atomic::AtomicU64,
    #[cfg(feature = "diagnostics")]
    pub(super) level0_waits: std::sync::atomic::AtomicU64,
    #[cfg(feature = "diagnostics")]
    pub(super) level0_wait_ns: std::sync::atomic::AtomicU64,
    #[cfg(feature = "diagnostics")]
    pub(super) flush_wait_ns: std::sync::atomic::AtomicU64,
    #[cfg(feature = "diagnostics")]
    pub(super) memory_wait_ns: std::sync::atomic::AtomicU64,
    #[cfg(any(test, feature = "diagnostics"))]
    pub(super) gate_waits: std::sync::atomic::AtomicU64,
    #[cfg(any(test, feature = "diagnostics"))]
    pub(super) write_locks: std::sync::atomic::AtomicU64,
    #[cfg(any(test, feature = "diagnostics"))]
    pub(super) write_groups: std::sync::atomic::AtomicU64,
    #[cfg(any(test, feature = "diagnostics"))]
    pub(super) grouped_writes: std::sync::atomic::AtomicU64,
    #[cfg(any(test, feature = "diagnostics"))]
    pub(super) write_group_sizes: [std::sync::atomic::AtomicU64; combiner::GROUP_SIZE_BUCKETS],
    #[cfg(feature = "diagnostics")]
    pub(super) output_limit: std::sync::atomic::AtomicUsize,
    #[cfg(feature = "diagnostics")]
    pub(super) peak_reserved: std::sync::atomic::AtomicUsize,
    #[cfg(feature = "diagnostics")]
    pub(super) peak_active: std::sync::atomic::AtomicUsize,
    #[cfg(feature = "diagnostics")]
    pub(super) piece_retries: std::sync::atomic::AtomicUsize,
}

#[derive(Default)]
struct Estimates {
    output: usize,
    workspace: usize,
}

#[cfg(feature = "diagnostics")]
pub(super) struct WaitCharge<'a> {
    progress: &'a Progress,
    started: std::time::Instant,
}
#[cfg(feature = "diagnostics")]
impl Drop for WaitCharge<'_> {
    fn drop(&mut self) {
        let nanos = self.started.elapsed().as_nanos().min(u128::from(u64::MAX)) as u64;
        self.progress
            .writer_wait_ns
            .fetch_add(nanos, Ordering::Relaxed);
    }
}

#[cfg(feature = "diagnostics")]
pub(super) struct Level0Charge<'a> {
    progress: &'a Progress,
    started: std::time::Instant,
}
#[cfg(feature = "diagnostics")]
impl Drop for Level0Charge<'_> {
    fn drop(&mut self) {
        let nanos = self.started.elapsed().as_nanos().min(u128::from(u64::MAX)) as u64;
        self.progress.level0_waits.fetch_add(1, Ordering::Relaxed);
        self.progress
            .level0_wait_ns
            .fetch_add(nanos, Ordering::Relaxed);
    }
}

#[cfg(feature = "diagnostics")]
pub(super) struct AtomicWaitCharge<'a> {
    counter: &'a std::sync::atomic::AtomicU64,
    started: std::time::Instant,
}
#[cfg(feature = "diagnostics")]
impl Drop for AtomicWaitCharge<'_> {
    fn drop(&mut self) {
        let nanos = self.started.elapsed().as_nanos().min(u128::from(u64::MAX)) as u64;
        self.counter.fetch_add(nanos, Ordering::Relaxed);
    }
}

impl Progress {
    #[cfg(any(test, feature = "diagnostics"))]
    pub(super) fn record_write_group(&self, size: usize) {
        self.write_groups.fetch_add(1, Ordering::Relaxed);
        self.grouped_writes
            .fetch_add(size as u64, Ordering::Relaxed);
        self.write_group_sizes[combiner::group_size_bucket(size)].fetch_add(1, Ordering::Relaxed);
    }

    /// Charges one writer wait at the level-0 stop, counted and timed.
    #[cfg(feature = "diagnostics")]
    pub(super) fn level0_waiting(&self) -> Level0Charge<'_> {
        Level0Charge {
            progress: self,
            started: std::time::Instant::now(),
        }
    }
    #[cfg(feature = "diagnostics")]
    pub(super) fn flush_waiting(&self) -> AtomicWaitCharge<'_> {
        AtomicWaitCharge {
            counter: &self.flush_wait_ns,
            started: std::time::Instant::now(),
        }
    }
    #[cfg(feature = "diagnostics")]
    pub(super) fn memory_waiting(&self) -> AtomicWaitCharge<'_> {
        AtomicWaitCharge {
            counter: &self.memory_wait_ns,
            started: std::time::Instant::now(),
        }
    }
    #[cfg(feature = "diagnostics")]
    pub(super) fn waiting(&self) -> WaitCharge<'_> {
        WaitCharge {
            progress: self,
            started: std::time::Instant::now(),
        }
    }
    pub(super) fn changed(&self) {
        drop(self.event.lock().unwrap_or_else(PoisonError::into_inner));
        self.changed.notify_all();
    }

    pub(super) fn build(&self, output: usize, workspace: usize) -> BuildCharge<'_> {
        let mut estimates = self.event.lock().unwrap_or_else(PoisonError::into_inner);
        estimates.output = estimates.output.saturating_add(output);
        estimates.workspace = estimates.workspace.saturating_add(workspace);
        BuildCharge {
            progress: self,
            output,
            workspace,
        }
    }

    fn estimates(&self) -> (usize, usize) {
        let estimates = self.event.lock().unwrap_or_else(PoisonError::into_inner);
        (estimates.output, estimates.workspace)
    }
}

pub(super) struct BuildCharge<'a> {
    progress: &'a Progress,
    output: usize,
    workspace: usize,
}

impl BuildCharge<'_> {
    /// Replaces the build estimates with the output and workspace still held.
    pub(super) fn finished(&mut self, output: usize, workspace: usize) {
        let mut estimates = self
            .progress
            .event
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        estimates.output = estimates
            .output
            .saturating_sub(self.output)
            .saturating_add(output);
        estimates.workspace = estimates
            .workspace
            .saturating_sub(self.workspace)
            .saturating_add(workspace);
        self.output = output;
        self.workspace = workspace;
    }
}

impl Drop for BuildCharge<'_> {
    fn drop(&mut self) {
        let mut estimates = self
            .progress
            .event
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        estimates.output = estimates.output.saturating_sub(self.output);
        estimates.workspace = estimates.workspace.saturating_sub(self.workspace);
        drop(estimates);
        self.progress.changed.notify_all();
    }
}

#[cfg_attr(not(any(test, feature = "diagnostics")), allow(dead_code))]
#[derive(Default)]
pub(super) struct Usage {
    pub used: usize,
    pub memtables: usize,
    pub snapshots: usize,
    pub pool_unaccounted: usize,
    pub pool_reserve: usize,
    pub pool_in_use: usize,
    pub pool_cap: usize,
    pub build_output: usize,
    pub build_workspace: usize,
}

impl Core {
    pub(super) fn usage(&self, state: &State) -> Usage {
        let active = state.active.memory();
        let (memtables, pinned) = state.frozen.iter().fold(
            (active.charged, active.pinned),
            |(charged, pinned), table| {
                let memory = table.memory();
                (
                    charged.saturating_add(memory.charged),
                    pinned.saturating_add(memory.pinned),
                )
            },
        );
        let snapshots = state.retained().bytes.saturating_sub(pinned);
        let (pool_reserve, pool_in_use, pool_cap, pool_unaccounted) = self.chunks.accounting();
        let (build_output, build_workspace) = self.resources.estimates();
        let used = [
            memtables,
            state.run_bytes,
            state.held_bytes,
            snapshots,
            pool_reserve,
            pool_unaccounted,
            build_output,
            build_workspace,
        ]
        .into_iter()
        .fold(0usize, usize::saturating_add);
        Usage {
            used,
            memtables,
            snapshots,
            pool_unaccounted,
            pool_reserve,
            pool_in_use,
            pool_cap,
            build_output,
            build_workspace,
        }
    }

    pub(super) fn validate_record(
        &self,
        prefix: KeyPrefix,
        key: &[u8],
        value: Value<'_>,
    ) -> Result<(), UpdateError> {
        let len = match value {
            Value::Bytes(bytes) => bytes.len(),
            Value::Tombstone => 0,
        };
        let error = if key.len() > MAX_KEY_LEN {
            Some(WriteError::KeyTooLong { len: key.len() })
        } else if len > MAX_VALUE_LEN {
            Some(WriteError::ValueTooLong { len })
        } else {
            let charge = prefix.as_bytes().len() + key.len() + len + ENTRY_OVERHEAD;
            (charge > self.memtable_budget).then_some(WriteError::RecordExceedsBudget {
                charge,
                budget: self.memtable_budget,
            })
        };
        error.map_or(Ok(()), |error| Err(UpdateError::Rejected(error)))
    }

    /// State -> notification mutex is the lock order for every publisher.
    /// Taking it before releasing state closes the check/wait race.
    fn wait_for_publication<'s>(
        &'s self,
        state: RwLockWriteGuard<'s, State>,
        briefly: bool,
    ) -> RwLockWriteGuard<'s, State> {
        let event = self
            .resources
            .event
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        if !briefly && event.output == 0 && event.workspace == 0 {
            return state;
        }
        drop(state);
        #[cfg(feature = "diagnostics")]
        let _waiting = self.resources.waiting();
        #[cfg(feature = "diagnostics")]
        let _memory = self.resources.memory_waiting();
        if briefly {
            drop(
                self.resources
                    .changed
                    .wait_timeout(event, SOFT_WAIT)
                    .unwrap_or_else(PoisonError::into_inner),
            );
        } else {
            drop(
                self.resources
                    .changed
                    .wait(event)
                    .unwrap_or_else(PoisonError::into_inner),
            );
        }
        self.write_state()
    }

    pub(super) fn admit<'s>(
        &'s self,
        mut state: RwLockWriteGuard<'s, State>,
    ) -> Result<RwLockWriteGuard<'s, State>, UpdateError> {
        state = self.wait_for_level0(state);
        let Some(limit) = self.memory_limit else {
            return Ok(state);
        };
        let soft = limit - limit / 10;
        let used = self.usage(&state).used;
        if used < soft {
            return Ok(state);
        }
        if used < limit {
            if let Some(signal) = &self.background {
                signal.request();
            }
            state = self.wait_for_publication(state, true);
        }
        loop {
            state = self.wait_for_level0(state);
            // Free reserve is immediately reclaimable and must never cause refusal.
            self.chunks.trim();
            let usage = self.usage(&state);
            if usage.used < limit {
                return Ok(state);
            }
            if !state.active.is_empty() {
                state = self.make_room(state);
            }
            if !state.frozen.is_empty() {
                self.flush_signal.request();
                #[cfg(feature = "diagnostics")]
                let _waiting = self.resources.waiting();
                #[cfg(feature = "diagnostics")]
                let _flush = self.resources.flush_waiting();
                state = self.wait_for_flush(state);
                continue;
            }
            if usage.build_output != 0 || usage.build_workspace != 0 {
                state = self.wait_for_publication(state, false);
                continue;
            }
            let options = CompactionPolicy {
                reclaim: true,
                max_overlap: 1,
                ..self.policy
            };
            let fit = options.fit(self.target_run_len, state.level0_stop);
            if state
                .keyspaces
                .values()
                .all(|space| matches!(space.levels.plan_reclaim(&fit), Plan::Idle))
            {
                return Err(UpdateError::MemoryLimit {
                    used: usage.used,
                    limit,
                });
            }
            drop(state);
            #[cfg(feature = "diagnostics")]
            let waiting = self.resources.waiting();
            #[cfg(feature = "diagnostics")]
            let memory_waiting = self.resources.memory_waiting();
            self.reclaim.store(true, Ordering::SeqCst);
            let waited = self
                .background
                .as_ref()
                .map(|signal| signal.wait_for(|| self.usage(&self.read_state()).used < limit));
            if waited.is_none() || waited.is_some_and(|result| result.is_err()) {
                // Maintenance can still run after a worker panic. A corrupt
                // input cannot reclaim memory and must not leave a writer waiting.
                if self.compact(options, &mut MergeWorkspace::new()).is_err() {
                    let used = self.usage(&self.read_state()).used;
                    return Err(UpdateError::MemoryLimit { used, limit });
                }
            }
            state = self.write_state();
            #[cfg(feature = "diagnostics")]
            drop((memory_waiting, waiting));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicUsize;
    use std::sync::{Barrier, mpsc};
    use std::time::{Duration, Instant};

    /// Estimates as large as an unlimited allowance saturate instead of
    /// overflowing, and release back to zero.
    #[test]
    fn build_estimates_saturate() {
        let progress = Progress::default();
        let large = progress.build(usize::MAX, usize::MAX);
        let small = progress.build(1, 1);
        assert_eq!(progress.estimates(), (usize::MAX, usize::MAX));
        drop(large);
        assert_eq!(progress.estimates(), (0, 0));
        drop(small);
        assert_eq!(progress.estimates(), (0, 0));
    }

    #[test]
    fn build_charge_finished_saturates_after_saturated_build() {
        let progress = Progress::default();
        let large = progress.build(usize::MAX, usize::MAX);
        let mut small = progress.build(1, 1);
        drop(large);
        small.finished(0, 0);
        assert_eq!(progress.estimates(), (0, 0));
    }

    fn fixture(rounds: u8) -> Core {
        let core = Core::new(&Options {
            memtable_size: 64 << 10,
            target_run_len: 8 << 10,
            compression: Compression::Uncompressed,
            ..Options::default()
        });
        for value in 0..rounds {
            core.put(b"key", &[value; 4096]).unwrap();
            core.begin_flush().unwrap().build().publish();
        }
        core.chunks.trim();
        core
    }

    fn start(mut core: Core, limit: usize) -> Engine {
        core.memory_limit = Some(limit);
        core.policy = CompactionPolicy::for_memory(Some(limit));
        Engine::start(core, &Options::default(), None, None).unwrap()
    }

    fn settle(core: &Core) {
        let policy = CompactionPolicy {
            max_overlap: 1,
            ..core.policy
        };
        while core
            .compact(policy, &mut MergeWorkspace::new())
            .unwrap()
            .is_some()
        {}
        core.chunks.trim();
    }

    fn assert_formula(stats: EngineStats) {
        assert_eq!(
            stats.used,
            stats.memtable_bytes
                + stats.run_bytes
                + stats.held_bytes
                + stats.pool_reserve_bytes
                + stats.pool_unaccounted_bytes
                + stats.snapshot_bytes
                + stats.build_output_bytes
                + stats.build_workspace_bytes
        );
    }

    #[test]
    fn writes_below_the_limit_proceed_and_account_for_pool_padding() {
        let engine = Engine::in_memory(Options {
            memory_limit: Some(16 << 20),
            ..Options::default()
        })
        .unwrap();
        for value in 0..100u32 {
            engine.ks().put(b"key", &value.to_be_bytes()).unwrap();
        }
        let stats = engine.engine_stats();
        assert!(stats.used < 16 << 20);
        assert!(stats.pool_unaccounted_bytes > stats.active.charged);
        assert_eq!(stats.memtable_bytes, stats.active.charged);
        assert_formula(stats);
    }

    #[test]
    fn soft_pressure_wait_is_bounded_even_while_compaction_is_blocked() {
        let core = fixture(1);
        settle(&core);
        let used = core.stats().used;
        let engine = start(core, used + used / 10);
        let held = engine.core.compacting.lock().unwrap();
        std::thread::scope(|scope| {
            let (tx, rx) = mpsc::channel();
            let engine = &engine;
            scope.spawn(move || tx.send(engine.ks().put(b"new", b"value")).unwrap());
            assert!(rx.recv_timeout(Duration::from_secs(2)).unwrap().is_ok());
        });
        drop(held);
    }

    #[test]
    fn a_writer_at_the_limit_waits_for_stale_versions_and_resumes() {
        let core = fixture(3);
        let limit = core.stats().used;
        let (built_tx, built_rx) = mpsc::channel();
        let (resume_tx, resume_rx) = mpsc::channel();
        let resume_rx = Mutex::new(resume_rx);
        let paused = std::sync::atomic::AtomicBool::new(false);
        *core.after_build.lock().unwrap() = Some(Arc::new(move || {
            if paused.swap(true, Ordering::SeqCst) {
                return;
            }
            built_tx.send(()).unwrap();
            resume_rx
                .lock()
                .unwrap()
                .recv_timeout(Duration::from_secs(5))
                .unwrap();
        }));
        let engine = start(core, limit);
        std::thread::scope(|scope| {
            let (tx, rx) = mpsc::channel();
            let engine = &engine;
            scope.spawn(move || tx.send(engine.ks().put_seq(b"key", b"latest")).unwrap());
            built_rx.recv_timeout(Duration::from_secs(2)).unwrap();
            let stats = engine.engine_stats();
            assert_eq!(stats.last_seq, 3);
            assert!(stats.used >= limit);
            assert!(stats.build_output_bytes > 0 && stats.build_workspace_bytes > 0);
            assert!(engine.background_status().unwrap().busy);
            assert!(rx.try_recv().is_err());
            assert_formula(stats);
            // The one pressure merge consumes every overlapping version.
            resume_tx.send(()).unwrap();
            assert_eq!(rx.recv_timeout(Duration::from_secs(2)).unwrap(), Ok(4));
        });
        assert_eq!(engine.ks().get(b"key").unwrap().unwrap(), b"latest");
    }

    #[test]
    fn live_data_at_the_limit_returns_promptly_without_a_sequence() {
        let core = fixture(1);
        settle(&core);
        let limit = core.stats().used;
        let engine = start(core, limit);
        let before = engine.engine_stats().last_seq;
        let began = Instant::now();
        assert!(
            matches!(engine.ks().put_seq(b"new", b"value"), Err(UpdateError::MemoryLimit { used, limit: found }) if used >= limit && found == limit)
        );
        let error = engine.ks().put(b"new", b"value").unwrap_err();
        assert_eq!(error.kind(), ErrorKind::MemoryLimit);
        assert!(began.elapsed() < Duration::from_secs(1));
        assert_eq!(engine.engine_stats().last_seq, before);
        assert_eq!(engine.write_seq(&WriteBatch::new()), Ok(before));
    }

    #[test]
    fn snapshot_sources_raise_used_and_release_relaxes_writer_pressure() {
        let core = fixture(1);
        let snapshot = core.snapshot();
        core.put(b"key", &[9; 4096]).unwrap();
        core.begin_flush().unwrap().build().publish();
        settle(&core);
        let held = core.stats();
        assert!(held.snapshot_bytes > 0);
        assert_eq!(held.snapshot_bytes, held.retained_bytes);
        let limit = held.used - held.snapshot_bytes / 2;
        let engine = start(core, limit);
        let error = engine.ks().put(b"new", b"value").unwrap_err();
        assert_eq!(error.kind(), ErrorKind::MemoryLimit);
        assert_formula(engine.engine_stats());
        let mut logical = crate::run::LogicalWorkspace::default();
        let found = snapshot
            .get(snapshot.only().unwrap(), b"key", &mut logical)
            .unwrap()
            .unwrap();
        assert_eq!(found.value, [0; 4096]);
        drop(snapshot);
        let released = engine.engine_stats();
        assert_eq!(released.snapshot_bytes, 0);
        assert!(released.used < held.used);
        assert_eq!(engine.ks().put_seq(b"new", b"value"), Ok(3));
    }

    /// The publisher stops after releasing the state lock, while its job
    /// still holds the flushed table.
    #[test]
    fn a_published_flush_counts_its_tables_until_they_are_freed() {
        let core = fixture(0);
        core.put(b"key", &[1; 4096]).unwrap();
        let charged = core.read_state().active.memory().charged;
        let (parked, park) = mpsc::channel();
        let (resume, resumed) = mpsc::channel::<()>();
        let resumed = std::sync::Mutex::new(resumed);
        *core.after_flush_publish.lock().unwrap() = Some(Arc::new(move || {
            parked.send(()).unwrap();
            resumed.lock().unwrap().recv().unwrap();
        }));
        let during = std::thread::scope(|scope| {
            let publisher = scope.spawn(|| core.begin_flush().unwrap().build().publish());
            park.recv().unwrap();
            let usage = core.usage(&core.read_state());
            resume.send(()).unwrap();
            publisher.join().unwrap();
            usage
        });
        assert_eq!(
            during.snapshots, charged,
            "a live flushed table is counted whole: memtables {} + snapshots {} vs charged {charged}",
            during.memtables, during.snapshots,
        );
        assert_eq!(core.stats().snapshot_bytes, 0, "the freed table is not");
    }

    /// Snapshots of the active table are views that share its family and
    /// never reference the frozen table, so only a snapshot taken after the
    /// table froze holds it whole.
    #[test]
    fn only_a_snapshot_taken_after_freezing_holds_a_flushed_table_whole() {
        let core = fixture(0);
        core.put(b"key", &[1; 4096]).unwrap();
        let first = core.snapshot();
        let reused = core.snapshot();
        assert!(Arc::ptr_eq(&first.tables[0], &reused.tables[0]));
        core.put(b"other", &[2; 4096]).unwrap();
        let job = core.begin_flush().unwrap();
        let table = Arc::clone(&core.read_state().frozen[0]);
        assert_eq!(Arc::strong_count(&table), 3, "the state, the job and this");
        let (held, charged) = (table.held_by_snapshots(), table.memory().charged);
        assert!(0 < held && held < charged, "held {held} of {charged}");
        drop(table);
        job.build().publish();
        assert_eq!(core.stats().snapshot_bytes, held);
        drop((first, reused));
        assert_eq!(core.stats().snapshot_bytes, 0);

        core.put(b"key", &[3; 4096]).unwrap();
        let job = core.begin_flush().unwrap();
        let whole = core.snapshot();
        let charged = core.read_state().frozen[0].memory().charged;
        job.build().publish();
        assert_eq!(core.stats().snapshot_bytes, charged);
        drop(whole);
        assert_eq!(core.stats().snapshot_bytes, 0);
    }

    /// A merge workspace's decode buffers and contexts are charged with its
    /// build, so they are released with it, before publication.
    #[test]
    fn a_merge_workspace_holds_nothing_past_its_charged_build() {
        // Compressed blocks are decoded into the workspace's buffers.
        let core = Core::new(&Options {
            memtable_size: 64 << 10,
            target_run_len: 8 << 10,
            compression: Compression::Zstd,
            ..Options::default()
        });
        for round in 0..3u32 {
            for key in 0..64u32 {
                core.put(
                    &key.to_be_bytes(),
                    format!("{round}-{key}-{}", "v".repeat(64)).as_bytes(),
                )
                .unwrap();
            }
            core.begin_flush().unwrap().build().publish();
        }
        let policy = CompactionPolicy {
            max_overlap: 1,
            ..core.policy
        };
        let job = core.begin_job(policy).unwrap().unwrap();
        let mut workspace = MergeWorkspace::new();
        let built = job.build_with_threads(&mut workspace, 1).unwrap();
        assert!(built.output.workspace_bytes > 0, "the merge decoded blocks");
        let held = workspace.capacity() + workspace.context_bytes();
        let charged = core.stats().build_workspace_bytes;
        built.publish().unwrap();
        let after = core.stats().build_workspace_bytes;
        assert!(
            held <= charged && held <= after,
            "workspace holds {held} bytes; charged {charged} before and {after} after publication"
        );
        assert_eq!(held, 0);
    }

    #[test]
    fn pinned_history_is_counted_once_and_build_charges_unwind() {
        let core = fixture(0);
        core.put(b"key", b"old").unwrap();
        let snapshot = core.snapshot();
        core.put(b"key", b"new").unwrap();
        let stats = core.stats();
        assert!(stats.active.pinned > 0);
        assert_eq!(stats.memtable_bytes, stats.active.charged);
        assert_eq!(stats.snapshot_bytes, 0);
        let before = stats.used;
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _charge = core.resources.build(1024, 2048);
            assert_eq!(core.stats().used, before + 3072);
            panic!("abandoned build");
        }));
        assert_eq!(core.stats().used, before);
        assert_eq!(core.stats().build_workspace_bytes, 0);
        assert_formula(core.stats());
        drop(snapshot);
    }

    #[test]
    fn pressure_reclaims_isolated_tombstones_below_normal_targets() {
        let core = fixture(0);
        core.delete(b"deleted").unwrap();
        core.begin_flush().unwrap().build().publish();
        settle(&core);
        assert_eq!(core.stats().runs, 1);
        let limit = core.stats().used;
        let engine = start(core, limit);
        assert_eq!(engine.ks().put_seq(b"live", b"value"), Ok(2));
        assert_eq!(engine.engine_stats().runs, 0);
    }

    #[test]
    fn remaining_options_validate_their_ranges() {
        let cases = [
            (
                Options {
                    memory_limit: Some(options::MIN_MEMORY_LIMIT - 1),
                    ..Options::default()
                },
                "Options::memory_limit",
            ),
            (
                Options {
                    memtable_size: ENTRY_OVERHEAD + MAX_PREFIX_LEN - 1,
                    ..Options::default()
                },
                "Options::memtable_size",
            ),
            (
                Options {
                    memtable_size: usize::MAX,
                    ..Options::default()
                },
                "Options::memtable_size",
            ),
            (
                Options {
                    background_threads: 0,
                    ..Options::default()
                },
                "Options::background_threads",
            ),
            (
                Options {
                    level_ratio: 1,
                    ..Options::default()
                },
                "Options::level_ratio",
            ),
            (
                Options {
                    block_size: 0,
                    ..Options::default()
                },
                "Options::block_size",
            ),
            (
                Options {
                    block_size: RAW_BLOCK_CAP + 1,
                    ..Options::default()
                },
                "Options::block_size",
            ),
        ];
        for (options, setting) in cases {
            let error = Engine::in_memory(options).unwrap_err();
            assert_eq!(error.kind(), ErrorKind::Config, "{error}");
            assert!(error.to_string().contains(setting), "{error}");
        }
        assert_eq!(Options::default().memory_limit, None);
        Engine::in_memory(Options {
            memory_limit: Some(options::MIN_MEMORY_LIMIT),
            ..Options::default()
        })
        .unwrap();
        // Below the minimum only through the core: nothing fits, so even an
        // empty engine refuses a write and an empty batch uses no sequence.
        let engine = start(Core::new(&Options::default()), 0);
        assert_eq!(engine.write_seq(&WriteBatch::new()), Ok(0));
        let ks = engine.keyspace("limited").unwrap();
        assert_eq!(
            ks.put_seq(b"a", b"v"),
            Err(UpdateError::MemoryLimit { used: 0, limit: 0 })
        );
        assert_eq!(
            ks.put(b"a", b"v").unwrap_err().kind(),
            ErrorKind::MemoryLimit
        );
    }

    #[test]
    fn recovered_live_data_needs_no_speculative_rewrite_to_refuse_a_write() {
        let core = fixture(1);
        settle(&core);
        let engine = Engine::start(core, &Options::default(), None, None).unwrap();
        let directory = std::env::temp_dir().join(format!(
            "tessera-resources-live-recovery-{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&directory);
        engine.checkpoint_to(&directory).unwrap();
        engine.close().unwrap();
        // Recovered as `Engine::open` does, under a limit below the minimum
        // option, so that the live data alone exceeds it.
        let (core, _) = checkpoint::recover(&directory, &Options::default())
            .unwrap()
            .unwrap();
        let recovered = start(core, 1);
        assert!(!recovered.core.read_state().only().runs[0].run.reclaimable());
        let error = recovered.ks().put(b"new", b"value").unwrap_err();
        assert_eq!(error.kind(), ErrorKind::MemoryLimit);
        assert_eq!(recovered.background_status().unwrap().compactions, 0);
        recovered.close().unwrap();
        std::fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn writers_readers_snapshots_and_checkpoints_finish_at_the_limit() {
        let (tx, rx) = mpsc::channel();
        let supervisor = std::thread::spawn(move || {
            let core = fixture(3);
            let limit = core.stats().used;
            let engine = Arc::new(start(core, limit));
            let barrier = Barrier::new(5);
            let directory = std::env::temp_dir().join(format!(
                "tessera-resources-concurrent-{}",
                std::process::id()
            ));
            let _ = std::fs::remove_dir_all(&directory);
            let rejected = AtomicUsize::new(0);
            std::thread::scope(|scope| {
                for writer in 0..2u32 {
                    let (engine, barrier, rejected) = (&engine, &barrier, &rejected);
                    scope.spawn(move || {
                        barrier.wait();
                        for value in 0..80u32 {
                            match engine.ks().put(&writer.to_be_bytes(), &value.to_be_bytes()) {
                                Ok(()) => {}
                                Err(error) if error.kind() == ErrorKind::MemoryLimit => {
                                    rejected.fetch_add(1, Ordering::Relaxed);
                                }
                                Err(error) => panic!("{error}"),
                            }
                        }
                    });
                }
                let (engine, barrier, directory) = (&engine, &barrier, &directory);
                scope.spawn(move || {
                    barrier.wait();
                    for _ in 0..8 {
                        engine.checkpoint_to(directory).unwrap();
                        let recovered = Engine::open(directory, Options::default()).unwrap();
                        recovered.close().unwrap();
                    }
                });
                scope.spawn(|| {
                    barrier.wait();
                    let ks = engine.ks();
                    for _ in 0..160 {
                        ks.get(b"key").unwrap();
                        let snapshot = engine.snapshot();
                        let mut reader = snapshot.reader();
                        let mut scan = reader.range(&ks, ..);
                        let mut previous = None::<Vec<u8>>;
                        while let Some((key, _)) = scan.next().unwrap() {
                            if let Some(previous) = &previous {
                                assert!(previous.as_slice() < key);
                            }
                            previous = Some(key.to_vec());
                        }
                    }
                });
                barrier.wait();
            });
            assert_formula(engine.engine_stats());
            drop(engine);
            std::fs::remove_dir_all(directory).unwrap();
            tx.send(()).unwrap();
        });
        rx.recv_timeout(Duration::from_secs(15))
            .expect("resource-pressure operations must not deadlock");
        supervisor.join().unwrap();
    }

    #[test]
    fn admit_rechecks_level0_stop_after_wait_for_flush() {
        let core = Core::new(&Options {
            memtable_size: 64 << 10,
            target_run_len: 8 << 10,
            compression: Compression::Zstd,
            ..Options::default()
        });
        for i in 0..3u8 {
            core.put(&[i], b"v").unwrap();
            core.begin_flush().unwrap().build().publish();
        }
        // Leave compressible data in `active`: flushing it pushes level 0 to
        // `level0_stop` (4) while compression drops `used` below `limit`.
        core.put(b"compressible", &[0u8; 32 << 10]).unwrap();
        core.chunks.trim();
        let limit = core.stats().used;
        core.write_state().level0_stop = 4;
        let engine = start(core, limit);
        let ks = engine.ks();
        let compact_hold = engine.core.compacting.lock().unwrap();

        std::thread::scope(|scope| {
            let (tx, rx) = mpsc::channel();
            let ks = &ks;
            let writer = scope.spawn(move || {
                tx.send(ks.put_seq(b"past-stop", b"x")).unwrap();
            });
            crate::test_support::wait_until(|| {
                assert!(
                    rx.try_recv().is_err(),
                    "writer slipped past level0_stop without waiting"
                );
                engine.core.level0_waits.load(Ordering::SeqCst) > 0
            });
            assert_eq!(engine.engine_stats().level0_flushes, 4);
            assert!(rx.try_recv().is_err());
            drop(compact_hold);
            assert!(rx.recv_timeout(Duration::from_secs(5)).unwrap().is_ok());
            writer.join().unwrap();
        });
        assert_eq!(ks.get(b"past-stop").unwrap().as_deref(), Some(&b"x"[..]));
    }
}
