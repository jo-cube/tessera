//! Dedicated flush worker and condition-variable notifications.
use super::*;
use std::panic::{self, AssertUnwindSafe};
use std::sync::Condvar;

pub(super) struct Signal {
    progress: Mutex<Progress>,
    changed: Condvar,
}
#[derive(Default)]
pub(super) struct Progress {
    requested: bool,
    stop: bool,
    failure: Option<BackgroundError>,
}
impl Signal {
    pub(super) fn new() -> Self {
        Self {
            progress: Mutex::new(Progress::default()),
            changed: Condvar::new(),
        }
    }
    pub(super) fn lock(&self) -> MutexGuard<'_, Progress> {
        self.progress.lock().unwrap_or_else(PoisonError::into_inner)
    }
    pub(super) fn failed(&self, progress: &Progress) -> bool {
        progress.failure.is_some()
    }
    pub(super) fn wait<'a>(&self, guard: MutexGuard<'a, Progress>) -> MutexGuard<'a, Progress> {
        self.changed
            .wait(guard)
            .unwrap_or_else(PoisonError::into_inner)
    }
    pub(super) fn request(&self) {
        self.lock().requested = true;
        self.changed.notify_all();
    }
    pub(super) fn changed(&self) {
        drop(self.lock());
        self.changed.notify_all();
    }
    pub(super) fn stop(&self) {
        self.lock().stop = true;
        self.changed.notify_all();
    }
    pub(super) fn failure(&self) -> Option<BackgroundError> {
        self.lock().failure.clone()
    }
}

pub(super) fn start(core: Arc<Core>) -> std::io::Result<std::thread::JoinHandle<()>> {
    std::thread::Builder::new()
        .name("tessera-flush".into())
        .spawn(move || {
            let result = panic::catch_unwind(AssertUnwindSafe(|| run(&core)));
            if let Err(payload) = result {
                let message = payload
                    .downcast_ref::<&str>()
                    .map(|s| (*s).to_owned())
                    .or_else(|| payload.downcast_ref::<String>().cloned())
                    .unwrap_or_else(|| "a panic without a message".into());
                core.flush_signal.lock().failure = Some(BackgroundError::Panicked(message));
            }
            core.flush_signal.changed();
        })
}

fn run(core: &Core) {
    loop {
        {
            let mut progress = core.flush_signal.lock();
            while !progress.requested && !progress.stop {
                progress = core.flush_signal.wait(progress);
            }
            if progress.stop {
                return;
            }
            progress.requested = false;
        }
        let serial = core.serialize_flush();
        let state = core.write_state();
        if state.frozen.is_empty() {
            continue;
        }
        let job = FlushJob {
            engine: core,
            _serial: serial,
            tables: state.frozen.clone(),
            cuts: state.flush_cuts(core.target_run_len),
        };
        drop(state);
        #[cfg(test)]
        if let Some(hook) = core.before_flush_build.lock().unwrap().take() {
            hook();
        }
        job.build().publish();
        core.flush_signal.changed();
    }
}

impl FlushReport {
    pub(super) fn add(&mut self, report: Self) {
        self.tables += report.tables;
        self.records += report.records;
        self.runs += report.runs;
        self.run_bytes += report.run_bytes;
        self.held_bytes += report.held_bytes;
        self.filter_build_bytes = self.filter_build_bytes.max(report.filter_build_bytes);
    }
    pub(super) fn since(self, before: Self) -> Self {
        Self {
            tables: self.tables - before.tables,
            records: self.records - before.records,
            runs: self.runs - before.runs,
            run_bytes: self.run_bytes - before.run_bytes,
            held_bytes: self.held_bytes - before.held_bytes,
            filter_build_bytes: if self.tables == before.tables {
                0
            } else {
                self.filter_build_bytes
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Barrier, mpsc};
    use std::time::Duration;

    #[test]
    fn writers_continue_during_build_until_the_frozen_limit() {
        let engine = Engine::new(EngineOptions {
            memtable_budget: ENTRY_OVERHEAD + 2,
            background_compaction: None,
            ..EngineOptions::default()
        })
        .unwrap();
        let started = Arc::new(Barrier::new(2));
        let resume = Arc::new(Barrier::new(2));
        let (a, b) = (Arc::clone(&started), Arc::clone(&resume));
        *engine.core.before_flush_build.lock().unwrap() = Some(Arc::new(move || {
            a.wait();
            b.wait();
        }));
        engine.put(b"a", b"v").unwrap();
        engine.put(b"b", b"v").unwrap();
        started.wait();
        std::thread::scope(|scope| {
            let (tx, rx) = mpsc::channel();
            let engine = &engine;
            let writer = scope.spawn(move || {
                engine.put(b"c", b"v").unwrap();
                tx.send(()).unwrap();
                engine.put(b"d", b"v").unwrap();
                tx.send(()).unwrap();
            });
            rx.recv_timeout(Duration::from_secs(2)).unwrap();
            assert!(rx.recv_timeout(Duration::from_millis(20)).is_err());
            assert_eq!(engine.core.read_state().frozen.len(), 2);
            resume.wait();
            rx.recv_timeout(Duration::from_secs(2)).unwrap();
            writer.join().unwrap();
        });
        engine.flush().unwrap();
    }

    #[test]
    fn a_failed_background_build_is_readable_and_writers_flush_synchronously() {
        let engine = Engine::new(EngineOptions {
            memtable_budget: ENTRY_OVERHEAD + 2,
            max_frozen_memtables: 1,
            background_compaction: None,
            ..EngineOptions::default()
        })
        .unwrap();
        *engine.core.before_flush_build.lock().unwrap() =
            Some(Arc::new(|| panic!("injected flush failure")));
        engine.put(b"a", b"v").unwrap();
        engine.put(b"b", b"v").unwrap();
        engine.put(b"c", b"v").unwrap();
        assert!(matches!(engine.flush(), Err(BackgroundError::Panicked(_))));
        let mut workspace = EngineWorkspace::new();
        assert_eq!(
            engine.get(b"a", &mut workspace).unwrap().unwrap().value,
            b"v"
        );
        assert_eq!(
            engine.get(b"b", &mut workspace).unwrap().unwrap().value,
            b"v"
        );
        assert!(engine.close().is_err());
    }
}
