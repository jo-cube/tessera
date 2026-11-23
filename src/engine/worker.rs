//! The background compaction worker: one thread per engine that compacts
//! when requested and promotes level-0 residue after a quiet period.
//!
//! Requests are counted, not queued. A request made while a pass is running
//! causes one more pass after it, however many requests arrive.

use std::any::Any;
use std::fmt;
use std::io;
use std::panic::{self, AssertUnwindSafe};
use std::sync::{Arc, Condvar, Mutex, MutexGuard, PoisonError};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use crate::run::MergeWorkspace;

use super::{CompactionError, CompactionPolicy, Core, WriteActivity};

/// Allow short read-only intervals between write phases to pass while
/// their pending runs can still collect more updates.
const QUIET_PERIOD: Duration = Duration::from_secs(5);

/// What the background worker has done since the engine was created.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct BackgroundStatus {
    /// Whether the worker thread is running.
    pub running: bool,
    /// Whether it is in a pass now.
    pub busy: bool,
    /// Passes finished. A pass compacts until nothing is eligible, a
    /// compaction fails, or the engine is closed or dropped.
    pub passes: u64,
    /// Compaction steps published, moves included.
    pub compactions: u64,
    /// Stored bytes of the runs those compactions replaced.
    pub input_bytes: u64,
    /// Stored bytes of the runs they published.
    pub output_bytes: u64,
    /// Passes that ended on a failure.
    pub failures: u64,
    /// The failure that ended the most recent pass, if it failed. A
    /// read failure is not terminal: the next request starts another pass.
    pub(crate) last_failure: Option<CompactionError>,
    /// The message of the panic that stopped the worker, if it panicked. A
    /// stopped worker is not restarted; the engine works on without it.
    pub panic: Option<String>,
}

/// Why waiting for background compaction did not end in success.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub(crate) enum BackgroundError {
    /// The worker panicked and has stopped for good, with this message.
    Panicked(String),
    /// The pass waited for ended on this failure. Nothing is retried until
    /// the next publication or explicit wait.
    Failed(CompactionError),
}

impl fmt::Display for BackgroundError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Panicked(message) => {
                write!(f, "the background worker panicked: {message}")
            }
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
    quiet: Option<(Instant, WriteActivity)>,
    quiet_pending: Option<WriteActivity>,
    #[cfg(test)]
    clock: Option<Instant>,
}

impl Progress {
    fn now(&self) -> Instant {
        #[cfg(test)]
        return self.clock.expect("the worker has a test clock");
        #[cfg(not(test))]
        Instant::now()
    }

    fn arm_quiet(&mut self, activity: Option<WriteActivity>) {
        self.quiet = activity.map(|activity| (self.now() + QUIET_PERIOD, activity));
    }
}

impl Signal {
    pub(super) fn new() -> Self {
        Self {
            progress: Mutex::new(Progress {
                #[cfg(test)]
                clock: Some(Instant::now()),
                ..Progress::default()
            }),
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

    /// Arms the quiet deadline when the worker is idle with no deadline set,
    /// or records the crossing for the end of an in-flight pass.
    pub(super) fn arm_quiet_if_idle(&self, activity: WriteActivity) {
        let mut progress = self.lock();
        if progress.stop || progress.quiet.is_some() {
            return;
        }
        if !progress.status.busy && progress.requested == progress.started {
            progress.arm_quiet(Some(activity));
            self.changed.notify_all();
        } else {
            progress.quiet_pending = Some(activity);
        }
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

    /// Requests a pass and waits until `room` holds, a pass started after
    /// the request has finished, or the worker has stopped, returning
    /// `room`'s last answer. `room` is asked first, after every step the
    /// worker publishes, and once more after the pass, each time without
    /// this signal's lock, so it may take the engine's: a step published
    /// between two answers is seen by the next, never missed.
    ///
    /// Without room at the end, the error is why, read with the end it
    /// explains: the worker's panic, or the failure of the pass that
    /// ended the wait. A pass that ended with nothing due leaves `Ok(false)`.
    pub(super) fn wait_for(&self, mut room: impl FnMut() -> bool) -> Result<bool, BackgroundError> {
        let ended = |progress: &Progress| match &progress.status.panic {
            _ if progress.status.running => progress
                .status
                .last_failure
                .clone()
                .map(BackgroundError::Failed),
            Some(message) => Some(BackgroundError::Panicked(message.clone())),
            None => Some(BackgroundError::Panicked(String::new())),
        };
        let stopped = {
            let progress = self.lock();
            (!progress.status.running)
                .then(|| ended(&progress))
                .flatten()
        };
        if let Some(error) = stopped {
            return if room() { Ok(true) } else { Err(error) };
        }
        let target = self.request();
        loop {
            let (steps, over) = {
                let progress = self.lock();
                let over = (!progress.status.running || progress.finished >= target)
                    .then(|| ended(&progress));
                (progress.status.compactions, over)
            };
            if room() {
                return Ok(true);
            }
            if let Some(ended) = over {
                return ended.map_or(Ok(false), Err);
            }
            let mut progress = self.lock();
            while progress.status.running
                && progress.finished < target
                && progress.status.compactions == steps
            {
                progress = self.wait_on(progress);
            }
        }
    }

    /// Requests a pass and waits until one started after the request has
    /// finished, or the worker has stopped.
    #[cfg_attr(not(any(test, feature = "diagnostics")), allow(dead_code))]
    pub(super) fn wait(&self) -> Result<(), BackgroundError> {
        let target = self.request();
        let mut progress = self.lock();
        while progress.status.running && progress.finished < target {
            progress = self.wait_on(progress);
        }
        if progress.finished < target {
            let message = progress.status.panic.clone().unwrap_or_default();
            return Err(BackgroundError::Panicked(message));
        }
        match &progress.status.last_failure {
            Some(error) => Err(BackgroundError::Failed(error.clone())),
            None => Ok(()),
        }
    }

    fn start_pass(&self) -> Option<(u64, Option<WriteActivity>)> {
        let mut progress = self.lock();
        let quiet = loop {
            if progress.stop {
                return None;
            }
            if progress.requested != progress.started {
                break None;
            }
            let Some((deadline, activity)) = progress.quiet else {
                progress = self.wait_on(progress);
                continue;
            };
            let remaining = deadline.saturating_duration_since(progress.now());
            if remaining.is_zero() {
                break Some(activity);
            }
            #[cfg(not(test))]
            {
                progress = self
                    .changed
                    .wait_timeout(progress, remaining)
                    .unwrap_or_else(PoisonError::into_inner)
                    .0;
            }
            #[cfg(test)]
            {
                progress = self.wait_on(progress);
            }
        };
        progress.started = progress.requested;
        progress.quiet = None;
        progress.status.busy = true;
        Some((progress.started, quiet))
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

/// Starts the worker. Its thread holds the core, not the engine, so closing
/// or dropping the engine can stop and join it. Fails, leaving the worker
/// marked as not running, if the operating system cannot create the thread.
pub(super) fn start(core: Arc<Core>) -> io::Result<JoinHandle<()>> {
    signal(&core).lock().status.running = true;
    let thread = Arc::clone(&core);
    #[cfg(test)]
    let spawned = if tests::FAIL_SPAWN.with(|fail| fail.replace(false)) {
        Err(io::Error::other("injected spawn failure"))
    } else {
        spawn(thread)
    };
    #[cfg(not(test))]
    let spawned = spawn(thread);
    if spawned.is_err() {
        signal(&core).lock().status.running = false;
    }
    spawned
}

fn spawn(core: Arc<Core>) -> io::Result<JoinHandle<()>> {
    thread::Builder::new()
        .name("tessera-compaction".into())
        .spawn(move || {
            // A panic is caught here, after unwinding released every lock
            // it held, so that its message is kept for `wait` and `close`.
            let outcome = panic::catch_unwind(AssertUnwindSafe(|| run(&core)));
            let signal = signal(&core);
            let mut progress = signal.lock();
            progress.status.running = false;
            progress.status.busy = false;
            progress.status.panic = outcome.err().map(|payload| message(&*payload));
            signal.changed.notify_all();
        })
}

/// A panic payload's message, as `std` prints it.
fn message(payload: &(dyn Any + Send)) -> String {
    match (
        payload.downcast_ref::<&str>(),
        payload.downcast_ref::<String>(),
    ) {
        (Some(message), _) => (*message).to_owned(),
        (_, Some(message)) => message.clone(),
        _ => String::from("a panic without a message"),
    }
}

fn signal(core: &Core) -> &Signal {
    core.background
        .as_ref()
        .expect("an engine with a worker has its signal")
}

fn run(core: &Core) {
    let signal = signal(core);
    // Recovered engines can start with published level-0 runs.
    let activity = core.idle_activity();
    {
        let mut progress = signal.lock();
        if progress.quiet.is_none() {
            progress.arm_quiet(activity);
        }
    }
    while let Some((generation, quiet)) = signal.start_pass() {
        let failure = pass(core, signal, quiet).err();
        signal.lock().quiet_pending = None;
        let activity = if failure.is_none() {
            core.idle_activity()
        } else {
            None
        };
        let mut progress = signal.lock();
        let activity = activity.or(if failure.is_none() {
            progress.quiet_pending.take()
        } else {
            None
        });
        progress.arm_quiet(activity);
        progress.finished = generation;
        progress.status.busy = false;
        progress.status.passes += 1;
        progress.status.failures += u64::from(failure.is_some());
        progress.status.last_failure = failure;
        signal.changed.notify_all();
        drop(progress);
        core.resources.changed();
    }
}

/// Compacts until nothing is eligible, a compaction fails, or the worker is
/// told to stop. Each step lowers a level's excess, moves data down or lowers
/// the run count, which only new flushes undo, so without them the pass
/// ends; with them it ends once flushes stop, which the level-0 stop forces
/// if compaction falls behind. A read failure ends the pass and is retried
/// only after another publication or request.
fn pass(
    core: &Core,
    signal: &Signal,
    mut quiet: Option<WriteActivity>,
) -> Result<(), CompactionError> {
    #[cfg(test)]
    tests::fault_point(core);
    if let Some(activity) = quiet {
        let progress = signal.lock();
        if progress.stop || progress.requested != progress.started {
            return Ok(());
        }
        drop(progress);
        let Some(next) = core.flush_quiet_tail(activity) else {
            return Ok(());
        };
        quiet = Some(next);
    }
    let reclaim = quiet.is_none()
        && core
            .reclaim
            .swap(false, std::sync::atomic::Ordering::SeqCst);
    let options = CompactionPolicy {
        reclaim,
        ..core.policy
    };
    loop {
        let progress = signal.lock();
        if progress.stop || quiet.is_some() && progress.requested != progress.started {
            break;
        }
        drop(progress);
        // Keep no uncharged decode buffers or native contexts while idle.
        let mut workspace = MergeWorkspace::new();
        let result = core.compact_when(options, &mut workspace, quiet);
        drop(workspace);
        let Some(report) = result? else {
            break;
        };
        let mut progress = signal.lock();
        progress.status.compactions += 1;
        progress.status.input_bytes += report.input_bytes as u64;
        progress.status.output_bytes += report.output_bytes as u64;
        // A writer waiting for level 0 checks again after every step.
        signal.changed.notify_all();
        drop(progress);
        core.resources.changed();
    }
    Ok(())
}

#[cfg(test)]
pub(super) mod tests {
    use std::cell::Cell;
    use std::sync::atomic::Ordering;

    use super::Core;

    thread_local! {
        /// Makes the next worker start on this thread fail as if the
        /// operating system had refused the thread.
        pub(in crate::engine) static FAIL_SPAWN: Cell<bool> = const { Cell::new(false) };
    }

    /// Panics the worker at the start of a pass when the test asks.
    pub(super) fn fault_point(core: &Core) {
        if core.panic_worker.load(Ordering::Relaxed) {
            panic!("injected worker failure");
        }
    }
}

#[cfg(test)]
mod idle_tests;
