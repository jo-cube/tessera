use super::*;
use crate::test_support::Scratch;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc;
use std::time::Duration;

const KEYS: u16 = 256;
fn options() -> Options {
    Options {
        memtable_size: 1 << 20,
        target_run_len: 64 << 10,
        background_threads: 1,
        background_compaction: None,
        ..Options::default()
    }
}
fn fixture() -> Engine {
    fixture_with_threads(1)
}
fn fixture_with_threads(background_threads: usize) -> Engine {
    let engine = Engine::in_memory(Options {
        background_threads,
        ..options()
    })
    .unwrap();
    let runs: Vec<_> = (0..8)
        .map(|group| {
            let mut builder =
                crate::run::RunBuilder::in_pool(Codec::Zstd, 128, &engine.core.chunks);
            for key in group * 32..(group + 1) * 32u16 {
                builder
                    .add(&key.to_be_bytes(), 0, Value::Bytes(&[0; 128]))
                    .unwrap();
            }
            builder.finish()
        })
        .collect();
    engine.ks();
    {
        let mut state = engine.core.write_state();
        let id = state.only_id();
        let space = state.only_mut();
        space.runs = published(runs).collect();
        let recovered: Vec<_> = space.runs.iter().map(|r| (1, 0, r.clone())).collect();
        space.levels = Levels::recovered(&recovered, 1, 10);
        state.published([id]);
        state.split_flushes = false;
    }
    for key in 0..KEYS {
        engine.ks().put(&key.to_be_bytes(), &[1; 128]).unwrap();
    }
    engine.flush().unwrap();
    engine.core.piece_budget.store(1, Ordering::Relaxed);
    engine
}
fn check(snapshot: &Snapshot, ks: &Keyspace, expected: impl Fn(u16) -> Option<Vec<u8>>) {
    let mut reader = snapshot.reader();
    let mut scan = reader.range(ks, ..);
    for key in 0..KEYS {
        if let Some(value) = expected(key) {
            let record = scan.next().unwrap().unwrap();
            assert_eq!(record, (&key.to_be_bytes()[..], value.as_slice()));
        }
    }
    assert!(scan.next().unwrap().is_none());
    drop(scan);
    let keys: Vec<_> = (0..KEYS).map(u16::to_be_bytes).collect();
    for (at, value) in reader.multi_get(ks, &keys).unwrap().iter().enumerate() {
        assert_eq!(*value, expected(at as u16).as_deref());
    }
    for key in 0..KEYS {
        assert_eq!(
            reader.get(ks, &key.to_be_bytes()).unwrap(),
            expected(key).as_deref()
        );
    }
    for prefix in [vec![], vec![0], vec![0, 31], vec![255]] {
        let mut scan = reader.prefix(ks, &prefix);
        let mut expected = (0..KEYS)
            .filter(|k| k.to_be_bytes().starts_with(&prefix))
            .filter_map(|k| expected(k).map(|v| (k, v)));
        while let Some(record) = scan.next().unwrap() {
            let (key, value) = expected.next().unwrap();
            assert_eq!(record, (&key.to_be_bytes()[..], value.as_slice()));
        }
        assert!(expected.next().is_none());
    }
}
fn ones(_: u16) -> Option<Vec<u8>> {
    Some(vec![1; 128])
}

#[test]
fn readers_snapshots_and_checkpoints_cross_piece_publications() {
    let engine = fixture();
    let before = engine.snapshot();
    let (ready_tx, ready_rx) = mpsc::channel();
    let (resume_tx, resume_rx) = mpsc::channel();
    let resume_rx = Mutex::new(resume_rx);
    *engine.core.after_build.lock().unwrap() = Some(Arc::new(move || {
        ready_tx.send(()).unwrap();
        resume_rx
            .lock()
            .unwrap()
            .recv_timeout(Duration::from_secs(10))
            .unwrap();
    }));
    let scratch = Scratch::new("piece-checkpoint");
    let mut pins = Vec::new();
    std::thread::scope(|scope| {
        let job = scope.spawn(|| engine.compact(&mut MergeWorkspace::new()).unwrap().unwrap());
        ready_rx.recv_timeout(Duration::from_secs(10)).unwrap();
        let held = engine.snapshot();
        let guard = engine.core.read_state();
        resume_tx.send(()).unwrap();
        assert!(
            ready_rx.recv_timeout(Duration::from_millis(20)).is_err(),
            "publication waits for the state lock"
        );
        drop(guard);
        let ks = engine.ks();
        let mut reader = held.reader();
        let mut scan = reader.range(&ks, ..);
        for key in 0..KEYS {
            assert_eq!(scan.next().unwrap().unwrap().0, key.to_be_bytes());
        }
        assert!(scan.next().unwrap().is_none());
        drop(scan);
        ready_rx.recv_timeout(Duration::from_secs(10)).unwrap();
        let middle = engine.snapshot();
        check(&middle, &engine.ks(), ones);
        assert_eq!(before.get_seq(&0u16.to_be_bytes()).unwrap().0, 1);
        assert_eq!(middle.get_seq(&0u16.to_be_bytes()).unwrap().0, 0);
        assert_eq!(middle.get_seq(&32u16.to_be_bytes()).unwrap().0, 33);
        let state = engine.core.read_state();
        assert_eq!(
            state.only().directory.group_count(),
            deepest_overlap(&state.only().runs)
        );
        assert_eq!(state.only().directory.group_count(), 2);
        drop(state);
        assert!(
            engine.core.read_state().only().levels.runs()[0]
                .iter()
                .any(|r| r.run.slice().is_some())
        );
        engine.checkpoint_to(&scratch.0).unwrap();
        let recovered = Engine::open(&scratch.0, options()).unwrap();
        check(&recovered.snapshot(), &recovered.ks(), ones);
        assert_eq!(
            recovered
                .core
                .read_state()
                .only()
                .levels
                .checkpoint_runs()
                .iter()
                .map(|(_, _, r)| r.run.slice().map(|(a, b)| (a.to_vec(), b.to_vec())))
                .collect::<Vec<_>>(),
            engine
                .core
                .read_state()
                .only()
                .levels
                .checkpoint_runs()
                .iter()
                .map(|(_, _, r)| r.run.slice().map(|(a, b)| (a.to_vec(), b.to_vec())))
                .collect::<Vec<_>>()
        );
        let reader = scope.spawn(|| {
            for _ in 0..20 {
                check(&engine.snapshot(), &engine.ks(), ones);
            }
        });
        resume_tx.send(()).unwrap();
        for _ in 2..8 {
            ready_rx.recv_timeout(Duration::from_secs(10)).unwrap();
            resume_tx.send(()).unwrap();
        }
        let report = job.join().unwrap();
        assert_eq!(report.stats.records_written, usize::from(KEYS));
        reader.join().unwrap();
        pins.push(middle);
    });
    let after = engine.snapshot();
    assert_eq!(after.get_seq(&0u16.to_be_bytes()).unwrap().0, 0);
    assert_eq!(engine.engine_stats().max_overlap, 1);
    for pin in [&before, &pins[0], &after] {
        check(pin, &engine.ks(), ones);
    }
    assert!(engine.engine_stats().retained_run_bytes > 0);
    drop((pins, before, after));
    assert_eq!(engine.engine_stats().retained_run_bytes, 0);
}

#[test]
fn a_middle_piece_refusal_keeps_published_ranges_and_releases_charges() {
    let engine = fixture();
    let retired = engine.core.read_state().only().levels.runs()[1][0]
        .run
        .retirement();
    let original_ids: Vec<_> = engine.core.read_state().only().levels.runs()[1]
        .iter()
        .map(|r| r.id)
        .collect();
    engine.core.fail_piece.store(3, Ordering::Relaxed);
    assert_eq!(
        engine.compact(&mut MergeWorkspace::new()),
        Err(CompactionError::Stale)
    );
    assert_eq!(
        retired.strong_count(),
        0,
        "completed bottom input was freed"
    );
    let state = engine.core.read_state();
    assert!(
        state.only().levels.runs()[1]
            .iter()
            .all(|r| !original_ids[..2].contains(&r.id))
    );
    assert!(
        state.only().levels.runs()[1]
            .iter()
            .any(|r| r.id == original_ids[2])
    );
    drop(state);
    check(&engine.snapshot(), &engine.ks(), ones);
    assert_eq!(engine.engine_stats().build_output_bytes, 0);
    assert_eq!(engine.engine_stats().build_workspace_bytes, 0);
    let scratch = Scratch::new("piece-refused");
    engine.checkpoint_to(&scratch.0).unwrap();
    let recovered = Engine::open(&scratch.0, options()).unwrap();
    check(&recovered.snapshot(), &recovered.ks(), ones);
    engine.core.fail_piece.store(0, Ordering::Relaxed);
    while engine
        .compact(&mut MergeWorkspace::new())
        .unwrap()
        .is_some()
    {}
    check(&engine.snapshot(), &engine.ks(), ones);
}

#[test]
fn interleaved_skipped_run_contests_erasure_in_every_piece() {
    let engine = fixture();
    // A skipped, interleaved L0 update is older than a selected update but
    // newer than bottom. It contests only its present keys through its filter.
    for key in (0..KEYS).step_by(3) {
        engine.ks().put(&key.to_be_bytes(), b"skipped").unwrap();
    }
    engine.flush().unwrap();
    for key in (0..KEYS).step_by(2) {
        engine.ks().put(&key.to_be_bytes(), b"newest").unwrap();
    }
    engine.flush().unwrap();
    let state = engine.core.read_state();
    let l0 = state.only().levels.runs()[0].clone();
    drop(state);
    let mut job = engine
        .core
        .begin_job(CompactionPolicy {
            max_overlap: 1,
            ..CompactionPolicy::default()
        })
        .unwrap()
        .unwrap();
    // Explicitly select the first and third sources, leaving the second.
    job.inputs.retain(|r| !Arc::ptr_eq(r, &l0[1]));
    job.source_count = 2;
    job.others.push(l0[1].clone());
    let before = engine.snapshot();
    job.execute(&mut MergeWorkspace::new()).unwrap();
    let expected = |key| {
        if key % 2 == 0 {
            Some(b"newest".to_vec())
        } else if key % 3 == 0 {
            Some(b"skipped".to_vec())
        } else {
            ones(key)
        }
    };
    check(&engine.snapshot(), &engine.ks(), expected);
    check(&before, &engine.ks(), expected);
    let state = engine.core.read_state();
    for run in &state.only().levels.runs()[1] {
        let mut ws = crate::run::ReadWorkspace::new();
        let mut cursor = run.run.cursor(&mut ws);
        while let Some(record) = cursor.next_record().unwrap() {
            let key = u16::from_be_bytes(record.key.try_into().unwrap());
            if key % 6 == 0 {
                assert!(record.seq > 0, "older skipped version must contest");
            }
            if key % 2 == 0 && key % 3 != 0 {
                assert_eq!(record.seq, 0);
            }
        }
    }
}

#[test]
fn randomized_model_forces_many_pieces_per_job() {
    randomized_pieces(1);
}
#[test]
fn randomized_model_forces_many_concurrent_pieces_per_job() {
    randomized_pieces(4);
}
fn randomized_pieces(threads: usize) {
    let engine = fixture_with_threads(threads);
    engine
        .core
        .piece_output_budget
        .store(32 << 10, Ordering::Relaxed);
    let publications = Arc::new(AtomicUsize::new(0));
    let count = publications.clone();
    *engine.core.after_build.lock().unwrap() = Some(Arc::new(move || {
        count.fetch_add(1, Ordering::Relaxed);
    }));
    let mut model = vec![Some(vec![1; 128]); usize::from(KEYS)];
    let mut rng = super::super::compaction_model_tests::Rng(0x32);
    for round in 0..12u8 {
        for _ in 0..100 {
            let key = rng.below(u64::from(KEYS)) as u16;
            if rng.below(4) == 0 {
                engine.ks().delete(&key.to_be_bytes()).unwrap();
                model[usize::from(key)] = None;
            } else {
                let value = vec![round; 128];
                engine.ks().put(&key.to_be_bytes(), &value).unwrap();
                model[usize::from(key)] = Some(value);
            }
        }
        engine.flush().unwrap();
        while engine
            .compact(&mut MergeWorkspace::new())
            .unwrap()
            .is_some()
        {}
        check(&engine.snapshot(), &engine.ks(), |key| {
            model[usize::from(key)].clone()
        });
    }
    assert!(publications.load(Ordering::Relaxed) > 20);
}

#[test]
fn corruption_in_a_middle_piece_preserves_completed_publications() {
    let engine = fixture();
    let (clean, damaged, first) = {
        let state = engine.core.read_state();
        let clean = state.only().levels.runs()[1][2].clone();
        let damaged = published(vec![clean.run.damaged_at(12)]).next().unwrap();
        let first = state.only().levels.runs()[1][0].run.retirement();
        (clean, damaged, first)
    };
    let replace = |old: &SharedRun, new: &SharedRun| {
        let mut state = engine.core.write_state();
        let id = state.only_id();
        let space = state.only_mut();
        space
            .levels
            .replace(
                std::slice::from_ref(old),
                std::slice::from_ref(new),
                Dest::Level(1),
            )
            .unwrap();
        let at = space.runs.iter().position(|r| Arc::ptr_eq(r, old)).unwrap();
        space.runs[at] = new.clone();
        state.published([id]);
    };
    replace(&clean, &damaged);
    assert!(matches!(
        engine.compact(&mut MergeWorkspace::new()),
        Err(CompactionError::Read(_))
    ));
    assert_eq!(first.strong_count(), 0);
    assert_eq!(engine.engine_stats().build_output_bytes, 0);
    assert_eq!(engine.engine_stats().build_workspace_bytes, 0);
    // Newer source records still hide the damaged older versions in point reads.
    for key in 0..KEYS {
        assert_eq!(engine.get_seq(&key.to_be_bytes()).unwrap().1, &[1; 128]);
    }
    // Repair only the damaged fixture, then verify scans and a resumed job.
    replace(&damaged, &clean);
    check(&engine.snapshot(), &engine.ks(), ones);
    while engine
        .compact(&mut MergeWorkspace::new())
        .unwrap()
        .is_some()
    {}
    check(&engine.snapshot(), &engine.ks(), ones);
}

#[test]
fn pieces_publish_out_of_order_and_recover_holes_with_shared_storage() {
    let engine = fixture_with_threads(4);
    engine
        .core
        .piece_output_budget
        .store(32 << 10, Ordering::Relaxed);
    let before = engine.snapshot();
    let permission = Arc::new((Mutex::new(1u64 << 1), std::sync::Condvar::new()));
    let permits = permission.clone();
    *engine.core.before_piece.lock().unwrap() = Some(Arc::new(move |index| {
        let (lock, changed) = &*permits;
        let mut allowed = lock.lock().unwrap();
        while *allowed & (1 << index) == 0 {
            allowed = changed.wait(allowed).unwrap();
        }
    }));
    let scratch = Scratch::new("out-of-order-piece-holes");
    std::thread::scope(|scope| {
        let job = scope.spawn(|| engine.compact(&mut MergeWorkspace::new()).unwrap().unwrap());
        crate::test_support::wait_until(|| engine.get_seq(&32u16.to_be_bytes()).unwrap().0 == 0);
        let middle = engine.snapshot();
        assert_eq!(middle.get_seq(&0u16.to_be_bytes()).unwrap().0, 1);
        check(&middle, &engine.ks(), ones);
        let expected_bytes = engine.engine_stats().run_bytes;
        {
            let state = engine.core.read_state();
            assert_eq!(state.only().levels.runs()[0].len(), 7);
            assert_eq!(physical_runs(&state.only().levels.runs()[0]).count(), 1);
            assert_eq!(
                state.only().directory.group_count(),
                deepest_overlap(&state.only().runs)
            );
        }
        engine.checkpoint_to(&scratch.0).unwrap();
        let recovered = Engine::open(&scratch.0, options()).unwrap();
        assert_eq!(
            recovered.engine_stats().run_bytes,
            expected_bytes,
            "recovery shares one physical file among views"
        );
        check(&recovered.snapshot(), &recovered.ks(), ones);
        while recovered
            .compact(&mut MergeWorkspace::new())
            .unwrap()
            .is_some()
        {}
        check(&recovered.snapshot(), &recovered.ks(), ones);
        // A flush remains independent while all remaining builders are paused.
        engine.ks().put(&10u16.to_be_bytes(), b"late").unwrap();
        engine.flush().unwrap();
        *permission.0.lock().unwrap() = u64::MAX;
        permission.1.notify_all();
        assert_eq!(job.join().unwrap().stats.records_written, usize::from(KEYS));
        check(&before, &engine.ks(), ones);
        check(&middle, &engine.ks(), ones);
    });
    check(&engine.snapshot(), &engine.ks(), |key| {
        if key == 10 {
            Some(b"late".to_vec())
        } else {
            ones(key)
        }
    });
    drop(before);
    assert_eq!(engine.engine_stats().retained_run_bytes, 0);
    #[cfg(feature = "diagnostics")]
    {
        let m = diagnostics::maintenance(&engine);
        assert_eq!(m.unpublished_bytes, 0);
        assert!(m.peak_active_pieces >= 2 && m.peak_active_pieces <= 4);
        assert!(m.peak_unpublished_bytes <= m.output_limit);
        assert!(m.peak_reserved_bytes <= m.output_limit);
    }
}

#[test]
fn an_underestimated_concurrent_piece_retries_alone_instead_of_failing() {
    let engine = fixture_with_threads(4);
    // Every first request is far below the output, and no allowance is forced.
    engine.core.piece_request.store(1, Ordering::Relaxed);
    while engine
        .compact(&mut MergeWorkspace::new())
        .unwrap()
        .is_some()
    {}
    check(&engine.snapshot(), &engine.ks(), ones);
    assert_eq!(engine.engine_stats().build_output_bytes, 0);
    #[cfg(feature = "diagnostics")]
    {
        let m = diagnostics::maintenance(&engine);
        assert!(m.piece_retries > 0);
        assert_eq!(m.unpublished_bytes, 0);
    }
}

#[test]
fn concurrent_output_refusal_keeps_every_view_readable_and_checkpointable() {
    let engine = fixture_with_threads(4);
    engine.core.piece_output_budget.store(1, Ordering::Relaxed);
    assert!(matches!(
        engine.compact(&mut MergeWorkspace::new()),
        Err(CompactionError::OutputLimit { limit: 4, .. })
    ));
    check(&engine.snapshot(), &engine.ks(), ones);
    assert_eq!(engine.engine_stats().build_output_bytes, 0);
    assert_eq!(engine.engine_stats().build_workspace_bytes, 0);
    let scratch = Scratch::new("concurrent-piece-refusal");
    engine.checkpoint_to(&scratch.0).unwrap();
    let recovered = Engine::open(&scratch.0, options()).unwrap();
    assert_eq!(
        recovered.engine_stats().run_bytes,
        engine.engine_stats().run_bytes
    );
    check(&recovered.snapshot(), &recovered.ks(), ones);
    while recovered
        .compact(&mut MergeWorkspace::new())
        .unwrap()
        .is_some()
    {}
    check(&recovered.snapshot(), &recovered.ks(), ones);
}

#[test]
fn one_builder_publishes_small_bottom_ranges_before_building_the_next() {
    let engine = fixture_with_threads(1);
    // Use the real planner estimate, rather than the forced-piece test mode.
    engine.core.piece_budget.store(0, Ordering::Relaxed);
    let first = engine.core.read_state().only().levels.runs()[1][0]
        .run
        .retirement();
    let builds = Arc::new(AtomicUsize::new(0));
    let count = builds.clone();
    *engine.core.after_build.lock().unwrap() = Some(Arc::new(move || {
        if count.fetch_add(1, Ordering::Relaxed) > 0 {
            assert_eq!(first.strong_count(), 0, "the prior small range has retired");
        }
    }));
    engine.compact(&mut MergeWorkspace::new()).unwrap().unwrap();
    assert!(builds.load(Ordering::Relaxed) > 1);
    check(&engine.snapshot(), &engine.ks(), ones);
}

#[cfg(feature = "diagnostics")]
#[test]
fn piecewise_compaction_and_ingestion_record_diagnostics() {
    let engine = fixture_with_threads(1);
    let bottom_runs = engine.core.read_state().only().levels.runs()[1].clone();
    let expected_blocks: u64 = bottom_runs
        .iter()
        .map(|r| r.run.view().block_count() as u64)
        .sum();
    let expected_raw: u64 = bottom_runs
        .iter()
        .map(|r| r.run.merged_raw_len() as u64)
        .sum();
    let expected_segments: u64 = bottom_runs
        .iter()
        .map(|r| r.run.view().segment_count() as u64)
        .sum();

    engine.compact(&mut MergeWorkspace::new()).unwrap().unwrap();
    let copy = engine.core.chunks.copy_counters();
    assert_eq!(copy.input_blocks, expected_blocks);
    assert_eq!(copy.input_raw, expected_raw);
    assert_eq!(copy.input_segments, expected_segments);

    let before = engine.core.chunks.work_counters()[0];
    let mut batch = WriteBatch::new();
    batch.put(&engine.ks(), b"ingest-a", b"value-a");
    batch.put(&engine.ks(), b"ingest-b", b"value-b");
    engine.ingest_sorted(&batch).unwrap();
    let after = engine.core.chunks.work_counters()[0];
    assert!(after[1] > before[1], "ingestion should record raw output");
    assert!(
        after[2] > before[2],
        "ingestion should record stored output"
    );
}
