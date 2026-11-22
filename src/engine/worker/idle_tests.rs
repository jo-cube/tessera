use super::*;
use crate::engine::{Compression, Engine, Options, WriteBatch};
use std::sync::mpsc::{self, Receiver, Sender};

const TEST_TIMEOUT: Duration = Duration::from_secs(5);

fn advance_clock(signal: &Signal, elapsed: Duration) {
    let mut progress = signal.lock();
    *progress.clock.as_mut().unwrap() += elapsed;
    signal.changed.notify_all();
}

fn wait_for(signal: &Signal, ready: impl Fn(&Progress) -> bool) {
    let (progress, _) = signal
        .changed
        .wait_timeout_while(signal.lock(), TEST_TIMEOUT, |progress| !ready(progress))
        .unwrap();
    assert!(ready(&progress), "worker did not reach the expected state");
}

fn signal_for(engine: &Engine) -> &Signal {
    signal(&engine.core)
}

fn drain(engine: &Engine) {
    engine.flush().unwrap();
    engine.wait_for_compaction().unwrap();
}

fn settle(engine: &Engine) {
    let signal = signal_for(engine);
    let passes = signal.status().passes;
    advance_clock(signal, QUIET_PERIOD);
    wait_for(signal, |progress| progress.status.passes > passes);
    assert_eq!(engine.engine_stats().level0_runs, 0);
}

fn pause_build(engine: &Engine) -> (Receiver<()>, Sender<()>) {
    let (started, built) = mpsc::channel();
    let (release, resume) = mpsc::channel();
    let resume = Mutex::new(Some(resume));
    *engine.core.after_build.lock().unwrap() = Some(Arc::new(move || {
        if let Some(resume) = resume.lock().unwrap().take() {
            started.send(()).unwrap();
            resume.recv_timeout(TEST_TIMEOUT).unwrap();
        }
    }));
    (built, release)
}

fn assert_accounting(engine: &Engine) {
    let s = engine.engine_stats();
    assert_eq!(
        engine.stats().memory_used,
        s.memtable_bytes
            + s.run_bytes
            + s.held_bytes
            + s.snapshot_bytes
            + s.pool_reserve_bytes
            + s.pool_unaccounted_bytes
            + s.build_output_bytes
            + s.build_workspace_bytes
    );
}

#[test]
fn level_0_settles_once_after_each_quiet_period() {
    let engine = Engine::in_memory(Options::default()).unwrap();
    let ks = engine.ks();
    let signal = signal_for(&engine);
    for value in [b"alpha", b"bravo"] {
        ks.put(b"key", value).unwrap();
        drain(&engine);
        let passes = signal.status().passes;
        assert_eq!(engine.engine_stats().level0_runs, 1);
        advance_clock(signal, QUIET_PERIOD - Duration::from_millis(1));
        {
            let progress = signal.lock();
            assert!(progress.quiet.unwrap().0 > progress.now());
            assert_eq!(progress.status.passes, passes);
        }
        assert_eq!(engine.engine_stats().level0_runs, 1);
        advance_clock(signal, Duration::from_millis(1));
        wait_for(signal, |progress| progress.status.passes > passes);
        assert_eq!(engine.engine_stats().level0_runs, 0);
        assert_eq!(engine.engine_stats().filter_bytes, 0);
        assert_eq!(ks.get(b"key").unwrap().as_deref(), Some(value.as_slice()));
        let done = signal.status();
        advance_clock(signal, QUIET_PERIOD * 3);
        assert!(signal.lock().quiet.is_none());
        assert_eq!(signal.status(), done);
    }
}

#[test]
fn successful_writes_defer_level_0_settling_without_a_flush() {
    let engine = Engine::in_memory(Options::default()).unwrap();
    let ks = engine.ks();
    ks.put(b"base", b"value").unwrap();
    drain(&engine);
    let signal = signal_for(&engine);
    let compactions = signal.status().compactions;

    for index in 0..6u8 {
        advance_clock(signal, QUIET_PERIOD - Duration::from_millis(1));
        if index % 2 == 0 {
            ks.put(b"point", &[index]).unwrap();
        } else {
            let mut batch = WriteBatch::new();
            batch.put(&ks, b"point", &[index]);
            batch.put(&ks, b"second", &[index]);
            engine.write(&batch).unwrap();
        }
        let passes = signal.status().passes;
        advance_clock(signal, Duration::from_millis(1));
        wait_for(signal, |progress| progress.status.passes > passes);
        assert_eq!(signal.status().compactions, compactions);
        assert_eq!(engine.engine_stats().level0_runs, 1);
    }

    settle(&engine);
    assert_eq!(engine.engine_stats().flushes, 1);
    assert!(engine.engine_stats().active.charged > 0);
    assert_eq!(ks.get(b"point").unwrap().unwrap(), [5]);
}

#[test]
fn frozen_tables_defer_idle_compaction_until_they_publish() {
    let engine = Engine::in_memory(Options::default()).unwrap();
    let ks = engine.ks();
    ks.put(b"key", b"base").unwrap();
    drain(&engine);
    ks.put(b"key", b"tail").unwrap();
    engine.wait_for_compaction().unwrap();

    let signal = signal_for(&engine);
    let flush = engine.core.begin_flush().unwrap();
    let before = signal.status();
    advance_clock(signal, QUIET_PERIOD);
    wait_for(signal, |progress| progress.status.passes > before.passes);
    assert!(signal.lock().quiet.is_none());
    assert_eq!(signal.status().compactions, before.compactions);

    flush.build().publish();
    engine.wait_for_compaction().unwrap();
    settle(&engine);
    assert_eq!(ks.get(b"key").unwrap().unwrap(), b"tail");
}

#[test]
fn close_wakes_a_worker_waiting_for_quiet() {
    let engine = Engine::in_memory(Options::default()).unwrap();
    engine.ks().put(b"key", b"value").unwrap();
    drain(&engine);
    let core = Arc::clone(&engine.core);
    let signal = signal(&core);
    let before = signal.status();
    assert!(signal.lock().quiet.is_some());
    engine.close().unwrap();
    let after = signal.status();
    assert!(!after.running);
    assert_eq!(after.passes, before.passes);
    assert_eq!(after.compactions, before.compactions);
}

#[test]
fn close_finishes_the_selected_idle_job_and_stops() {
    let engine = Engine::in_memory(Options::default()).unwrap();
    for name in ["left", "right"] {
        engine
            .keyspace(name)
            .unwrap()
            .put(b"key", b"value")
            .unwrap();
    }
    drain(&engine);
    assert_eq!(engine.engine_stats().level0_runs, 2);
    let core = Arc::clone(&engine.core);
    let signal = signal(&core);
    let (built, release) = pause_build(&engine);
    advance_clock(signal, QUIET_PERIOD);
    built.recv_timeout(TEST_TIMEOUT).unwrap();
    thread::scope(|scope| {
        let close = scope.spawn(|| engine.close());
        wait_for(signal, |progress| progress.stop);
        release.send(()).unwrap();
        close.join().unwrap().unwrap();
    });
    assert_eq!(signal.status().compactions, 1);
    assert!(!signal.status().running);
}

#[test]
fn a_write_during_an_idle_job_defers_the_next_job() {
    let engine = Engine::in_memory(Options::default()).unwrap();
    let left = engine.keyspace("left").unwrap();
    let right = engine.keyspace("right").unwrap();
    left.put(b"key", b"value").unwrap();
    right.put(b"key", b"value").unwrap();
    drain(&engine);
    let signal = signal_for(&engine);
    let (built, release) = pause_build(&engine);
    let passes = signal.status().passes;
    advance_clock(signal, QUIET_PERIOD);
    built.recv_timeout(TEST_TIMEOUT).unwrap();
    left.put(b"key", b"overwritten").unwrap();
    release.send(()).unwrap();
    wait_for(signal, |progress| progress.status.passes > passes);
    assert_eq!(signal.status().compactions, 1);
    assert_eq!(engine.engine_stats().level0_runs, 1);
    assert_eq!(left.get(b"key").unwrap().unwrap(), b"overwritten");
    settle(&engine);
}

#[test]
fn dropping_a_keyspace_during_idle_compaction_retires_its_runs() {
    let engine = Engine::in_memory(Options::default()).unwrap();
    let dropped = engine.keyspace("discard").unwrap();
    dropped.put(b"key", b"value").unwrap();
    drain(&engine);
    let snapshot = engine.snapshot();
    drop(engine.core.serialize_flush());
    let (built, release) = pause_build(&engine);
    advance_clock(signal_for(&engine), QUIET_PERIOD);
    built.recv_timeout(TEST_TIMEOUT).unwrap();
    thread::scope(|scope| {
        let dropper = scope.spawn(|| engine.drop_keyspace("discard"));
        // Drop holds the flush lock while waiting for the selected job.
        crate::test_support::wait_until(|| engine.core.flushing.try_lock().is_err());
        release.send(()).unwrap();
        assert!(dropper.join().unwrap().unwrap());
    });
    engine.wait_for_compaction().unwrap();
    assert_eq!(engine.engine_stats().runs, 0);
    let replacement = engine.keyspace("discard").unwrap();
    assert!(replacement.get(b"key").unwrap().is_none());
    assert_eq!(
        snapshot.reader().get(&dropped, b"key").unwrap(),
        Some(b"value".as_slice())
    );
}

#[test]
fn idle_compaction_keeps_snapshot_inputs_charged_until_release() {
    let engine = Engine::in_memory(Options::default()).unwrap();
    let ks = engine.ks();
    ks.put(b"key", b"initial").unwrap();
    ks.put(b"deleted", b"visible").unwrap();
    drain(&engine);
    let initial = engine.snapshot();
    ks.put(b"key", b"replacement").unwrap();
    ks.delete(b"deleted").unwrap();
    drain(&engine);
    let before = engine.engine_stats();
    let pending = engine.snapshot();
    let (built, release) = pause_build(&engine);
    let signal = signal_for(&engine);
    let passes = signal.status().passes;
    advance_clock(signal, QUIET_PERIOD);
    built.recv_timeout(TEST_TIMEOUT).unwrap();
    assert!(engine.engine_stats().build_output_bytes > 0);
    assert_accounting(&engine);
    release.send(()).unwrap();
    wait_for(signal, |progress| progress.status.passes > passes);
    let after = engine.engine_stats();
    assert_eq!(after.level0_runs, 0);
    assert_eq!(after.build_output_bytes + after.build_workspace_bytes, 0);
    assert_eq!(after.snapshot_bytes, before.run_bytes + before.held_bytes);
    assert_accounting(&engine);
    assert_eq!(
        initial.reader().get(&ks, b"deleted").unwrap(),
        Some(b"visible".as_slice())
    );
    assert_eq!(
        pending.reader().get(&ks, b"key").unwrap(),
        Some(b"replacement".as_slice())
    );
    assert!(ks.get(b"deleted").unwrap().is_none());
    drop(initial);
    drop(pending);
    crate::test_support::wait_until(|| engine.stats().snapshot_bytes == 0);
    assert_accounting(&engine);
}

#[test]
fn a_checkpoint_during_idle_compaction_recovers_its_captured_runs() {
    let engine = Engine::in_memory(Options::default()).unwrap();
    let ks = engine.ks();
    ks.put(b"key", b"value").unwrap();
    drain(&engine);
    let dir = crate::test_support::Scratch::new("checkpoint-during-idle");
    let (built, release) = pause_build(&engine);
    advance_clock(signal_for(&engine), QUIET_PERIOD);
    built.recv_timeout(TEST_TIMEOUT).unwrap();
    ks.put(b"tail", b"checkpointed").unwrap();
    engine.checkpoint_to(&dir.0).unwrap();
    release.send(()).unwrap();

    engine.wait_for_compaction().unwrap();
    assert_eq!(engine.engine_stats().level0_runs, 1);
    settle(&engine);
    let recovered = Engine::open(&dir.0, Options::default()).unwrap();
    let ks = recovered.ks();
    assert_eq!(recovered.engine_stats().level0_runs, 2);
    assert_eq!(ks.get(b"key").unwrap().unwrap(), b"value");
    assert_eq!(ks.get(b"tail").unwrap().unwrap(), b"checkpointed");
    crate::test_support::wait_until(|| signal_for(&recovered).lock().quiet.is_some());
    settle(&recovered);
    recovered.close().unwrap();
}

#[test]
fn quiet_compaction_reclaims_split_flush_remainders_at_each_builder_count() {
    let run = |background_threads| {
        let engine = Engine::in_memory(Options {
            target_run_len: 4 << 10,
            background_threads,
            compression: Compression::Uncompressed,
            ..Options::default()
        })
        .unwrap();
        let ks = engine.ks();
        let mut workspace = MergeWorkspace::new();
        for keys in [b"abcdef", b"pqrstz"] {
            for &key in keys {
                ks.put(&[key], &[key; 400]).unwrap();
            }
            drain(&engine);
            assert!(engine.core.compact_step(&mut workspace).unwrap().is_some());
        }
        for value in [3, 4] {
            for &key in b"bcdqrs" {
                ks.put(&[key], &[value; 200]).unwrap();
            }
            ks.delete(b"t").unwrap();
            drain(&engine);
        }
        assert_eq!(engine.engine_stats().level0_runs, 4);
        let step = engine.core.compact_step(&mut workspace).unwrap().unwrap();
        assert_eq!(step.level, 0);
        assert_eq!(
            (
                engine.engine_stats().level0_runs,
                engine.engine_stats().level0_flushes
            ),
            (2, 2)
        );
        engine.wait_for_compaction().unwrap();
        settle(&engine);

        let actual: Vec<_> = b"abcdefpqrstz"
            .iter()
            .map(|&key| ks.get(&[key]).unwrap())
            .collect();
        let expected: Vec<_> = b"abcdefpqrstz"
            .iter()
            .map(|&key| {
                if key == b't' {
                    None
                } else if b"bcdqrs".contains(&key) {
                    Some(vec![4; 200])
                } else {
                    Some(vec![key; 400])
                }
            })
            .collect();
        assert_eq!(actual, expected);
        let stats = engine.engine_stats();
        assert_eq!(stats.filter_bytes, 0);
        assert_accounting(&engine);
        (stats.runs, stats.run_bytes)
    };
    let expected = run(1);
    assert_eq!(run(2), expected);
    assert_eq!(run(4), expected);
}
