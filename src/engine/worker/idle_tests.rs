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

fn pause_flush(engine: &Engine) -> (Receiver<()>, Sender<()>) {
    let (started, paused) = mpsc::channel();
    let (release, resume) = mpsc::channel();
    let resume = Mutex::new(Some(resume));
    *engine.core.before_flush_build.lock().unwrap() = Some(Arc::new(move || {
        if let Some(resume) = resume.lock().unwrap().take() {
            started.send(()).unwrap();
            resume.recv_timeout(TEST_TIMEOUT).unwrap();
        }
    }));
    (paused, release)
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

#[test]
fn quiet_tail_at_or_above_threshold_flushes_and_settles_in_one_period() {
    let engine = Engine::in_memory(Options {
        memtable_size: 8 << 10,
        ..Options::default()
    })
    .unwrap();
    let ks = engine.ks();
    let signal = signal_for(&engine);

    // Cold start with no level-0 runs: crossing 1/8 of the budget arms quiet.
    let mut batch = WriteBatch::new();
    let value_a = vec![b'a'; 450];
    let value_b = vec![b'b'; 450];
    batch.put(&ks, b"k1", &value_a);
    batch.put(&ks, b"k2", &value_b);
    engine.write(&batch).unwrap();
    assert_eq!(engine.engine_stats().flushes, 0);
    assert!(engine.engine_stats().active.charged >= 1 << 10);
    assert!(signal.lock().quiet.is_some());

    let passes = signal.status().passes;
    advance_clock(signal, QUIET_PERIOD - Duration::from_millis(1));
    assert_eq!(signal.status().passes, passes);
    assert_eq!(engine.engine_stats().flushes, 0);

    advance_clock(signal, Duration::from_millis(1));
    wait_for(signal, |progress| progress.status.passes > passes);
    let stats = engine.engine_stats();
    assert_eq!(stats.flushes, 1);
    assert_eq!(stats.active.charged, 0);
    assert_eq!(stats.level0_runs, 0);
    assert!(stats.runs >= 1);
    assert!(signal.lock().quiet.is_none());
    assert_eq!(ks.get(b"k1").unwrap().as_deref(), Some(value_a.as_slice()));
    assert_eq!(ks.get(b"k2").unwrap().as_deref(), Some(value_b.as_slice()));

    // With level-0 residue already present, one quiet period covers both.
    ks.put(b"k1", b"intermediate").unwrap();
    drain(&engine);
    assert_eq!(engine.engine_stats().level0_runs, 1);
    let value_c = vec![b'c'; 500];
    let value_d = vec![b'd'; 500];
    batch.clear();
    batch.put(&ks, b"k1", &value_c);
    batch.put(&ks, b"k3", &value_d);
    engine.write(&batch).unwrap();
    engine.wait_for_compaction().unwrap();
    let passes = signal.status().passes;
    advance_clock(signal, QUIET_PERIOD);
    wait_for(signal, |progress| progress.status.passes > passes);
    let stats = engine.engine_stats();
    assert_eq!(stats.flushes, 3);
    assert_eq!(stats.active.charged, 0);
    assert_eq!(stats.level0_runs, 0);
    assert_eq!(ks.get(b"k1").unwrap().as_deref(), Some(value_c.as_slice()));
    assert_eq!(ks.get(b"k2").unwrap().as_deref(), Some(value_b.as_slice()));
    assert_eq!(ks.get(b"k3").unwrap().as_deref(), Some(value_d.as_slice()));
}

#[test]
fn quiet_tail_below_threshold_stays_in_active_memtable() {
    let engine = Engine::in_memory(Options {
        memtable_size: 8 << 10,
        ..Options::default()
    })
    .unwrap();
    let ks = engine.ks();
    let signal = signal_for(&engine);

    ks.put(b"small", b"trickle").unwrap();
    assert!(engine.engine_stats().active.charged < (1 << 10));
    assert!(signal.lock().quiet.is_none());
    let before = signal.status();
    advance_clock(signal, QUIET_PERIOD * 3);
    assert_eq!(signal.status(), before);
    assert_eq!(engine.engine_stats().flushes, 0);
    assert!(engine.engine_stats().active.charged > 0);
    assert_eq!(ks.get(b"small").unwrap().unwrap(), b"trickle");

    // Level-0 residue still settles while a below-threshold tail stays active.
    drain(&engine);
    ks.put(b"tiny", b"stays").unwrap();
    engine.wait_for_compaction().unwrap();
    assert!(engine.engine_stats().active.charged < (1 << 10));
    settle(&engine);
    assert_eq!(engine.engine_stats().flushes, 1);
    assert!(engine.engine_stats().active.charged > 0);
    assert!(signal.lock().quiet.is_none());
    assert_eq!(ks.get(b"small").unwrap().unwrap(), b"trickle");
    assert_eq!(ks.get(b"tiny").unwrap().unwrap(), b"stays");
}

#[test]
fn a_write_during_a_quiet_tail_flush_defers_settling() {
    let engine = Engine::in_memory(Options {
        memtable_size: 8 << 10,
        ..Options::default()
    })
    .unwrap();
    let ks = engine.ks();
    let signal = signal_for(&engine);

    let mut batch = WriteBatch::new();
    let value = vec![b'x'; 500];
    batch.put(&ks, b"a", &value);
    batch.put(&ks, b"b", &value);
    engine.write(&batch).unwrap();

    let (paused, release) = pause_flush(&engine);
    let passes = signal.status().passes;
    advance_clock(signal, QUIET_PERIOD);
    paused.recv_timeout(TEST_TIMEOUT).unwrap();
    // Building holds no state lock, so writes proceed and defer the settle.
    ks.put(b"a", b"overwritten").unwrap();
    release.send(()).unwrap();
    wait_for(signal, |progress| progress.status.passes > passes);

    assert_eq!(engine.engine_stats().flushes, 1);
    assert_eq!(signal.status().compactions, 0);
    assert_eq!(engine.engine_stats().level0_runs, 1);
    assert_eq!(ks.get(b"a").unwrap().unwrap(), b"overwritten");
    assert_eq!(ks.get(b"b").unwrap().as_deref(), Some(value.as_slice()));

    settle(&engine);
    assert_eq!(ks.get(b"a").unwrap().unwrap(), b"overwritten");
    assert_eq!(ks.get(b"b").unwrap().as_deref(), Some(value.as_slice()));
}

#[test]
fn close_during_a_quiet_tail_flush_stops_cleanly() {
    let engine = Engine::in_memory(Options {
        memtable_size: 8 << 10,
        ..Options::default()
    })
    .unwrap();
    let ks = engine.ks();
    let mut batch = WriteBatch::new();
    batch.put(&ks, b"a", &[b'v'; 500]);
    batch.put(&ks, b"b", &[b'w'; 500]);
    engine.write(&batch).unwrap();

    let core = Arc::clone(&engine.core);
    let signal = signal(&core);
    let (paused, release) = pause_flush(&engine);
    advance_clock(signal, QUIET_PERIOD);
    paused.recv_timeout(TEST_TIMEOUT).unwrap();
    thread::scope(|scope| {
        let close = scope.spawn(|| engine.close());
        wait_for(signal, |progress| progress.stop);
        release.send(()).unwrap();
        close.join().unwrap().unwrap();
    });
    let status = signal.status();
    assert!(!status.running);
    assert_eq!(status.compactions, 0);
    assert!(status.panic.is_none());
    assert_eq!(core.stats().flushes, 1);
}

#[test]
fn quiet_tail_flush_clears_active_charge_and_lowers_memory_used() {
    let engine = Engine::in_memory(Options {
        memtable_size: 2 << 20,
        ..Options::default()
    })
    .unwrap();
    let ks = engine.ks();
    let signal = signal_for(&engine);
    let value = br#"{"group":"quiet","status":"active","payload":"repeated-json-fields"}"#;

    // Warm the chunk pool so the build borrows from reserve rather than
    // adding a fresh 1 MiB chunk to the reserve during this measurement.
    ks.put(b"warm", value).unwrap();
    drain(&engine);
    settle(&engine);

    let mut batch = WriteBatch::new();
    for i in 0..4000u32 {
        batch.put(&ks, format!("k{i:04}").as_bytes(), value);
    }
    engine.write(&batch).unwrap();
    let before_stats = engine.stats();
    assert!(engine.engine_stats().active.charged >= (256 << 10));
    assert_accounting(&engine);

    let passes = signal.status().passes;
    advance_clock(signal, QUIET_PERIOD);
    wait_for(signal, |progress| progress.status.passes > passes);

    let after_engine = engine.engine_stats();
    let after_stats = engine.stats();
    assert_eq!(after_engine.active.charged, 0);
    assert_eq!(after_engine.level0_runs, 0);
    assert!(
        after_stats.memory_used < before_stats.memory_used,
        "memory_used did not fall: before={}, after={}",
        before_stats.memory_used,
        after_stats.memory_used
    );
    assert_accounting(&engine);
    assert_eq!(ks.get(b"k0000").unwrap().as_deref(), Some(value.as_slice()));
    assert_eq!(ks.get(b"k3999").unwrap().as_deref(), Some(value.as_slice()));
}

#[test]
fn quiet_tail_flush_aligns_cuts_to_oldest_level_0_flush_group() {
    let engine = Engine::in_memory(Options {
        memtable_size: 16 << 10,
        target_run_len: 4 << 10,
        compression: Compression::Uncompressed,
        ..Options::default()
    })
    .unwrap();
    let ks = engine.ks();
    // Build three disjoint bottom runs so subsequent flushes split on them.
    for prefix in *b"amz" {
        for i in 0..8u8 {
            ks.put(&[prefix, i], &[prefix; 500]).unwrap();
        }
        drain(&engine);
        settle(&engine);
    }
    assert_eq!(engine.engine_stats().level0_runs, 0);
    assert!(engine.engine_stats().runs >= 3);

    // Flush one level-0 group split across the bottom runs, then write a tail
    // above the 1/8 threshold (2 KiB) spanning the same key range.
    let mut batch = WriteBatch::new();
    for prefix in *b"amz" {
        for i in 0..4u8 {
            batch.put(&ks, &[prefix, i], &[1u8; 300]);
        }
    }
    engine.write(&batch).unwrap();
    drain(&engine);
    let l0_before = engine.engine_stats().level0_runs;
    assert!(l0_before >= 2, "expected split L0 flush, got {l0_before}");

    batch.clear();
    for prefix in *b"amz" {
        batch.put(&ks, &[prefix, 0], &[2u8; 700]);
    }
    engine.write(&batch).unwrap();
    engine.wait_for_compaction().unwrap();
    assert!(engine.engine_stats().active.charged >= (2 << 10));

    let (paused, release) = pause_build(&engine);
    let signal = signal_for(&engine);
    let passes = signal.status().passes;
    advance_clock(signal, QUIET_PERIOD);
    paused.recv_timeout(TEST_TIMEOUT).unwrap();
    // Quiet tail flush has published aligned slices before the first merge.
    assert_eq!(engine.engine_stats().active.charged, 0);
    assert!(engine.engine_stats().level0_runs >= l0_before * 2);
    release.send(()).unwrap();
    wait_for(signal, |progress| progress.status.passes > passes);

    assert_eq!(engine.engine_stats().level0_runs, 0);
    for prefix in *b"amz" {
        assert_eq!(ks.get(&[prefix, 0]).unwrap().unwrap(), vec![2u8; 700]);
        assert_eq!(ks.get(&[prefix, 1]).unwrap().unwrap(), vec![1u8; 300]);
    }
}

#[test]
fn rotation_during_quiet_tail_flush_re_signals_flusher_and_loses_nothing() {
    let engine = Engine::in_memory(Options {
        memtable_size: 4 << 10,
        ..Options::default()
    })
    .unwrap();
    let ks = engine.ks();
    let signal = signal_for(&engine);

    let quiet_val = vec![b'q'; 600];
    ks.put(b"q1", &quiet_val).unwrap();
    assert!(signal.lock().quiet.is_some());

    let (paused, release) = pause_flush(&engine);
    let passes = signal.status().passes;
    advance_clock(signal, QUIET_PERIOD);
    paused.recv_timeout(TEST_TIMEOUT).unwrap();

    // Fill and rotate `active` into `frozen[1]` while `flush_quiet_tail` holds `flushing`.
    let val = vec![b'w'; 1200];
    for i in 0..4u8 {
        ks.put(format!("k{i}").as_bytes(), &val).unwrap();
    }
    assert_eq!(engine.engine_stats().frozen_tables, 2);

    release.send(()).unwrap();
    wait_for(signal, |progress| progress.status.passes > passes);
    crate::test_support::wait_until(|| engine.engine_stats().frozen_tables == 0);

    assert_eq!(engine.engine_stats().flushes, 2);
    assert_eq!(
        ks.get(b"q1").unwrap().as_deref(),
        Some(quiet_val.as_slice())
    );
    for i in 0..4u8 {
        assert_eq!(
            ks.get(format!("k{i}").as_bytes()).unwrap().as_deref(),
            Some(val.as_slice())
        );
    }
    engine.close().unwrap();
}
