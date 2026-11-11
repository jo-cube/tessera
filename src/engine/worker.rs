//! The background compaction worker: one thread per engine that compacts
//! whenever it is asked to, and sleeps otherwise.
//!
//! Requests are counted, not queued. A request made while a pass is running
//! causes one more pass after it, however many requests arrive.

use std::fmt;
use std::sync::{Arc, Condvar, Mutex, MutexGuard, PoisonError};
use std::thread::{self, JoinHandle};

use crate::run::MergeWorkspace;

use super::{CompactionError, CompactionOptions, Core};

/// What the background worker has done since the engine was created.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct BackgroundStatus {
    /// Whether the worker thread is running.
    pub running: bool,
    /// Whether it is in a pass now.
    pub busy: bool,
    /// Passes finished. A pass compacts until nothing is eligible, a
    /// compaction fails, or the engine is dropped.
    pub passes: u64,
    /// Compactions published.
    pub compactions: u64,
    /// Stored bytes of the runs those compactions replaced.
    pub input_bytes: u64,
    /// Stored bytes of the runs they published.
    pub output_bytes: u64,
    /// Passes that ended on a failure.
    pub failures: u64,
    /// The failure that ended the most recent pass, if it failed.
    pub last_failure: Option<CompactionError>,
    /// Whether the worker stopped by panicking. It is not restarted.
    pub panicked: bool,
}

/// Why waiting for background compaction did not end in success.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum BackgroundError {
    /// The engine has no background worker.
    Disabled,
    /// The worker is not running, because it panicked.
    Stopped,
    /// The pass waited for ended on this failure. Nothing is retried until
    /// the next flush, refused flush, limit change or wait.
    Failed(CompactionError),
}

impl fmt::Display for BackgroundError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Disabled => f.write_str("the engine has no background compaction"),
            Self::Stopped => f.write_str("the background compaction worker has stopped"),
            Self::Failed(error) => write!(f, "background compaction failed: {error}"),
        }
    }
}

impl std::error::Error for BackgroundError {}

/// The worker's mailbox, shared through the engine's core.
pub(super) struct Signal {
    progress: Mutex<Progress>,
    changed: Condvar,
}

#[derive(Default)]
struct Progress {
    /// Requests made. A pass starts while this is ahead of `started`.
    requested: u64,
    /// The value of `requested` when the latest pass started.
    started: u64,
    /// The value of `requested` when the latest finished pass started.
    finished: u64,
    stop: bool,
    status: BackgroundStatus,
}

impl Signal {
    pub(super) fn new() -> Self {
        Self {
            progress: Mutex::new(Progress::default()),
            changed: Condvar::new(),
        }
    }

    /// Asks for a pass after any in progress. Never blocks on compaction.
    pub(super) fn request(&self) -> u64 {
        let mut progress = self.lock();
        progress.requested += 1;
        self.changed.notify_all();
        progress.requested
    }

    /// Tells the worker to exit once the compaction it is running, if any,
    /// is published or fails.
    pub(super) fn stop(&self) {
        self.lock().stop = true;
        self.changed.notify_all();
    }

    pub(super) fn status(&self) -> BackgroundStatus {
        self.lock().status.clone()
    }

    #[cfg(test)]
    pub(super) fn stop_requested(&self) -> bool {
        self.lock().stop
    }

    /// Requests a pass and waits until one started after the request has
    /// finished, or the worker has stopped.
    pub(super) fn wait(&self) -> Result<(), BackgroundError> {
        let target = self.request();
        let mut progress = self.lock();
        while progress.status.running && progress.finished < target {
            progress = self.wait_on(progress);
        }
        if progress.finished < target {
            return Err(BackgroundError::Stopped);
        }
        match &progress.status.last_failure {
            Some(error) => Err(BackgroundError::Failed(error.clone())),
            None => Ok(()),
        }
    }

    fn lock(&self) -> MutexGuard<'_, Progress> {
        // Every update to the progress is a few assignments that cannot panic
        // part way, so a poisoned lock still holds consistent counters.
        self.progress.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn wait_on<'a>(&self, progress: MutexGuard<'a, Progress>) -> MutexGuard<'a, Progress> {
        self.changed
            .wait(progress)
            .unwrap_or_else(PoisonError::into_inner)
    }
}

/// Starts the worker. Its thread holds the core, not the engine, so dropping
/// the engine can stop and join it.
///
/// Panics if the operating system cannot create the thread.
pub(super) fn start(core: Arc<Core>, options: CompactionOptions) -> JoinHandle<()> {
    signal(&core).lock().status.running = true;
    thread::Builder::new()
        .name("tessera-compaction".into())
        .spawn(move || run(&core, options))
        .expect("the compaction worker thread can be created")
}

fn signal(core: &Core) -> &Signal {
    core.background
        .as_ref()
        .expect("an engine with a worker has its signal")
}

/// Marks the worker stopped however it exits, so that waiters return.
struct Exit<'a>(&'a Signal);

impl Drop for Exit<'_> {
    fn drop(&mut self) {
        let mut progress = self.0.lock();
        progress.status.running = false;
        progress.status.busy = false;
        progress.status.panicked = thread::panicking();
        self.0.changed.notify_all();
    }
}

fn run(core: &Core, options: CompactionOptions) {
    let signal = signal(core);
    let _exit = Exit(signal);
    let mut workspace = MergeWorkspace::new();
    loop {
        let generation = {
            let mut progress = signal.lock();
            while !progress.stop && progress.requested == progress.started {
                progress = signal.wait_on(progress);
            }
            if progress.stop {
                return;
            }
            progress.started = progress.requested;
            progress.status.busy = true;
            progress.started
        };
        let failure = pass(core, signal, options, &mut workspace).err();
        let mut progress = signal.lock();
        progress.finished = generation;
        progress.status.busy = false;
        progress.status.passes += 1;
        progress.status.failures += u64::from(failure.is_some());
        progress.status.last_failure = failure;
        signal.changed.notify_all();
    }
}

/// Compacts until nothing is eligible, a compaction fails, or the worker is
/// told to stop. Each compaction lowers a count that only new flushes raise,
/// so without them the pass ends; with them it ends once flushes stop, which
/// the run limits force if compaction falls behind.
fn pass(
    core: &Core,
    signal: &Signal,
    options: CompactionOptions,
    workspace: &mut MergeWorkspace,
) -> Result<(), CompactionError> {
    #[cfg(test)]
    tests::fault_point(core);
    while !signal.lock().stop {
        let Some(report) = core.compact(options, workspace)? else {
            break;
        };
        let mut progress = signal.lock();
        progress.status.compactions += 1;
        progress.status.input_bytes += report.input_bytes as u64;
        progress.status.output_bytes += report.output_bytes as u64;
    }
    Ok(())
}

#[cfg(test)]
pub(super) mod tests {
    use std::sync::atomic::Ordering;

    use super::Core;

    /// Panics the worker at the start of a pass when the test asks.
    pub(super) fn fault_point(core: &Core) {
        if core.panic_worker.load(Ordering::Relaxed) {
            panic!("injected worker failure");
        }
    }
}
