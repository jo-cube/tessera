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
}

#[derive(Default)]
struct Estimates {
    output: usize,
    workspace: usize,
}

impl Progress {
    pub(super) fn changed(&self) {
        drop(self.event.lock().unwrap_or_else(PoisonError::into_inner));
        self.changed.notify_all();
    }

    pub(super) fn build(&self, output: usize, workspace: usize) -> BuildCharge<'_> {
        let mut estimates = self.event.lock().unwrap_or_else(PoisonError::into_inner);
        estimates.output += output;
        estimates.workspace += workspace;
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
    pub(super) fn finished(&mut self, output: usize) {
        let mut estimates = self
            .progress
            .event
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        estimates.output = estimates.output - self.output + output;
        estimates.workspace -= self.workspace;
        self.output = output;
        self.workspace = 0;
    }
}

impl Drop for BuildCharge<'_> {
    fn drop(&mut self) {
        let mut estimates = self
            .progress
            .event
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        estimates.output -= self.output;
        estimates.workspace -= self.workspace;
        drop(estimates);
        self.progress.changed();
    }
}

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

    pub(super) fn validate_record(&self, key: &[u8], value: Value<'_>) -> Result<(), UpdateError> {
        let len = match value {
            Value::Bytes(bytes) => bytes.len(),
            Value::Tombstone => 0,
        };
        let error = if key.len() > MAX_KEY_LEN {
            Some(WriteError::KeyTooLong { len: key.len() })
        } else if len > MAX_VALUE_LEN {
            Some(WriteError::ValueTooLong { len })
        } else {
            let charge = key.len() + len + ENTRY_OVERHEAD;
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
            if matches!(state.levels.plan_reclaim(&fit), Plan::Idle) {
                return Err(UpdateError::MemoryLimit {
                    used: usage.used,
                    limit,
                });
            }
            drop(state);
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
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicUsize;
    use std::sync::{Barrier, mpsc};
    use std::time::{Duration, Instant};

    fn fixture(rounds: u8) -> Core {
        let core = Core::new(&EngineOptions {
            memtable_budget: 64 << 10,
            target_run_len: 8 << 10,
            compression: Compression::Uncompressed,
            ..EngineOptions::default()
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
        Engine::start(Arc::new(core), &EngineOptions::default()).unwrap()
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
        let engine = Engine::new(EngineOptions {
            memory_limit: Some(16 << 20),
            ..EngineOptions::default()
        })
        .unwrap();
        for value in 0..100u32 {
            engine.put(b"key", &value.to_be_bytes()).unwrap();
        }
        let stats = engine.stats();
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
            scope.spawn(move || tx.send(engine.put(b"new", b"value")).unwrap());
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
            scope.spawn(move || tx.send(engine.put(b"key", b"latest")).unwrap());
            built_rx.recv_timeout(Duration::from_secs(2)).unwrap();
            assert!(rx.recv_timeout(Duration::from_millis(20)).is_err());
            let stats = engine.stats();
            assert!(stats.used >= limit);
            assert!(stats.build_output_bytes > 0 && stats.build_workspace_bytes > 0);
            assert_formula(stats);
            // The one pressure merge consumes every overlapping version.
            resume_tx.send(()).unwrap();
            assert_eq!(rx.recv_timeout(Duration::from_secs(2)).unwrap(), Ok(4));
        });
        let mut workspace = EngineWorkspace::new();
        assert_eq!(
            engine.get(b"key", &mut workspace).unwrap().unwrap().value,
            b"latest"
        );
    }

    #[test]
    fn live_data_at_the_limit_returns_promptly_without_a_sequence() {
        let core = fixture(1);
        settle(&core);
        let limit = core.stats().used;
        let engine = start(core, limit);
        let before = engine.stats().last_seq;
        let began = Instant::now();
        assert!(
            matches!(engine.put(b"new", b"value"), Err(UpdateError::MemoryLimit { used, limit: found }) if used >= limit && found == limit)
        );
        assert!(began.elapsed() < Duration::from_secs(1));
        assert_eq!(engine.stats().last_seq, before);
        assert_eq!(engine.write(&WriteBatch::new()), Ok(before));
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
        assert!(matches!(
            engine.put(b"new", b"value"),
            Err(UpdateError::MemoryLimit { .. })
        ));
        assert_formula(engine.stats());
        assert_eq!(
            snapshot
                .get(b"key", &mut EngineWorkspace::new())
                .unwrap()
                .unwrap()
                .value,
            &[0; 4096]
        );
        drop(snapshot);
        let released = engine.stats();
        assert_eq!(released.snapshot_bytes, 0);
        assert!(released.used < held.used);
        assert_eq!(engine.put(b"new", b"value"), Ok(3));
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
        assert_eq!(engine.put(b"live", b"value"), Ok(2));
        assert_eq!(engine.stats().runs, 0);
    }

    #[test]
    fn remaining_options_validate_their_ranges() {
        let cases = [
            (
                EngineOptions {
                    memtable_budget: ENTRY_OVERHEAD - 1,
                    ..EngineOptions::default()
                },
                "EngineOptions::memtable_budget",
            ),
            (
                EngineOptions {
                    memtable_budget: usize::MAX,
                    ..EngineOptions::default()
                },
                "EngineOptions::memtable_budget",
            ),
            (
                EngineOptions {
                    background_threads: 0,
                    ..EngineOptions::default()
                },
                "EngineOptions::background_threads",
            ),
            (
                EngineOptions {
                    level_ratio: 1,
                    ..EngineOptions::default()
                },
                "EngineOptions::level_ratio",
            ),
            (
                EngineOptions {
                    target_block_compressed: 0,
                    ..EngineOptions::default()
                },
                "EngineOptions::target_block_compressed",
            ),
            (
                EngineOptions {
                    target_block_compressed: RAW_BLOCK_CAP + 1,
                    ..EngineOptions::default()
                },
                "EngineOptions::target_block_compressed",
            ),
        ];
        for (options, setting) in cases {
            assert!(
                matches!(Engine::new(options), Err(OpenError::Config(error)) if error.setting == setting)
            );
        }
        assert_eq!(EngineOptions::default().memory_limit, None);
        let engine = Engine::new(EngineOptions {
            memory_limit: Some(0),
            ..EngineOptions::default()
        })
        .unwrap();
        assert_eq!(engine.write(&WriteBatch::new()), Ok(0));
        assert_eq!(
            engine.put(b"a", b"v"),
            Err(UpdateError::MemoryLimit { used: 0, limit: 0 })
        );
    }

    #[test]
    fn recovered_live_data_needs_no_speculative_rewrite_to_refuse_a_write() {
        let core = fixture(1);
        settle(&core);
        let engine = Engine::start(Arc::new(core), &EngineOptions::default()).unwrap();
        let directory = std::env::temp_dir().join(format!(
            "tessera-resources-live-recovery-{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&directory);
        engine
            .checkpoint(&directory, CheckpointOptions::default())
            .unwrap();
        engine.close().unwrap();
        let recovered = Engine::recover(
            &directory,
            EngineOptions {
                memory_limit: Some(1),
                ..EngineOptions::default()
            },
        )
        .unwrap();
        assert!(!recovered.core.read_state().runs[0].run.reclaimable());
        assert!(matches!(
            recovered.put(b"new", b"value"),
            Err(UpdateError::MemoryLimit { .. })
        ));
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
                            match engine.put(&writer.to_be_bytes(), &value.to_be_bytes()) {
                                Ok(_) => {}
                                Err(UpdateError::MemoryLimit { .. }) => {
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
                        engine
                            .checkpoint(directory, CheckpointOptions::default())
                            .unwrap();
                        let recovered =
                            Engine::recover(directory, EngineOptions::default()).unwrap();
                        recovered.close().unwrap();
                    }
                });
                scope.spawn(|| {
                    barrier.wait();
                    let mut workspace = EngineWorkspace::new();
                    for _ in 0..160 {
                        engine.get(b"key", &mut workspace).unwrap();
                        let snapshot = engine.snapshot();
                        let mut scan = snapshot.scan(&mut workspace);
                        let mut previous = None::<Vec<u8>>;
                        while let Some(record) = scan.next_record().unwrap() {
                            if let Some(previous) = &previous {
                                assert!(previous.as_slice() < record.key);
                            }
                            previous = Some(record.key.to_vec());
                        }
                    }
                });
                barrier.wait();
            });
            assert_formula(engine.stats());
            drop(engine);
            std::fs::remove_dir_all(directory).unwrap();
            tx.send(()).unwrap();
        });
        rx.recv_timeout(Duration::from_secs(15))
            .expect("resource-pressure operations must not deadlock");
        supervisor.join().unwrap();
    }
}
