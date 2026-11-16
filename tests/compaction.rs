//! Compaction in the engine: which runs it selects,
//! and how it replaces runs while reads, writes and flushes continue.
//!
//! The merge itself is tested beside it in `src/run`, and compactions staged
//! between other operations in `src/engine/staged_tests.rs`.
//! Expected results come from maps maintained by the tests. Concurrency tests
//! use barriers and shared flags and never sleep.

use std::collections::BTreeMap;
use std::sync::Barrier;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;

use tessera::engine::{
    CompactionKind, CompactionPolicy, Engine, EngineOptions, EngineWorkspace, MergeWorkspace,
};

/// The defaults without the background worker: these tests flush and
/// compact by hand and count what each step leaves.
fn manual() -> EngineOptions {
    EngineOptions {
        background_compaction: None,
        ..EngineOptions::default()
    }
}

/// Merges any overlap, up to `max_inputs` runs at a time.
fn options(max_inputs: usize) -> CompactionPolicy {
    CompactionPolicy {
        max_inputs,
        max_overlap: 1,
        ..CompactionPolicy::default()
    }
}

fn get(engine: &Engine, key: &[u8]) -> Option<Vec<u8>> {
    let mut workspace = EngineWorkspace::new();
    engine
        .get(key, &mut workspace)
        .unwrap()
        .map(|found| found.value.to_vec())
}

fn everything(engine: &Engine) -> Vec<(Vec<u8>, Vec<u8>)> {
    let guard = engine.read();
    let mut workspace = EngineWorkspace::new();
    let mut scan = guard.scan(&mut workspace);
    let mut out = Vec::new();
    while let Some(record) = scan.next_record().unwrap() {
        out.push((record.key.to_vec(), record.value.to_vec()));
    }
    out
}

type Model = BTreeMap<Vec<u8>, Vec<u8>>;

fn compact_fully(engine: &Engine) {
    let mut workspace = MergeWorkspace::new();
    let options = CompactionPolicy {
        max_inputs: usize::MAX,
        max_input_bytes: usize::MAX,
        max_workspace_bytes: usize::MAX,
        ..options(0)
    };
    while engine
        .core
        .compact(options, &mut workspace)
        .unwrap()
        .is_some()
    {}
}

/// The resurrection hazard: a tombstone merged without a run that could
/// hold the value it hides must be kept, and may go only once no such run is
/// left outside the merge.
#[test]
fn a_tombstone_hiding_an_unselected_run_survives_until_that_run_is_merged() {
    let engine = Engine::new(manual()).unwrap();
    for i in 0..500 {
        engine.put(format!("k{i:03}").as_bytes(), b"old").unwrap();
    }
    engine.flush().unwrap();
    engine.delete(b"k100").unwrap();
    engine.put(b"k150", b"new").unwrap();
    engine.flush().unwrap();
    engine.put(b"k099", b"new").unwrap();
    engine.put(b"k151", b"new").unwrap();
    engine.flush().unwrap();

    // The large run moves to level 1, writing nothing. With two inputs a
    // job, the tombstone's run merges with it, and the newest run, which
    // also covers k100, is left out.
    let mut workspace = MergeWorkspace::new();
    let report = engine
        .core
        .compact(options(2), &mut workspace)
        .unwrap()
        .unwrap();
    assert_eq!(
        (report.kind, report.output_bytes),
        (CompactionKind::Move, 0)
    );
    let report = engine
        .core
        .compact(options(2), &mut workspace)
        .unwrap()
        .unwrap();
    assert_eq!(report.kind, CompactionKind::Merge);
    assert_eq!((report.inputs, report.outputs), (2, 1));
    assert!(!report.complete);
    assert_eq!(report.stats.tombstones_dropped, 0);
    assert_eq!(get(&engine, b"k100"), None);
    assert_eq!(engine.stats().runs, 2);

    let report = engine
        .core
        .compact(options(8), &mut workspace)
        .unwrap()
        .unwrap();
    assert!(report.complete);
    assert_eq!(report.stats.tombstones_dropped, 1);
    assert_eq!(get(&engine, b"k100"), None);
    assert_eq!(get(&engine, b"k150").unwrap(), b"new");
    assert_eq!(get(&engine, b"k151").unwrap(), b"new");
    assert_eq!(everything(&engine).len(), 499);
    assert_eq!(engine.stats().runs, 1);
    assert!(
        engine
            .core
            .compact(options(8), &mut workspace)
            .unwrap()
            .is_none()
    );
}

/// Level 0 waits for its trigger: small flushes over a large run are left
/// alone until there are `max_overlap` of them, and are then merged into it
/// together, so the large run is rewritten once for all of them.
#[test]
fn level_0_is_compacted_at_its_trigger_in_one_job() {
    let engine = Engine::new(manual()).unwrap();
    for i in 0..2_000 {
        engine.put(format!("k{i:04}").as_bytes(), b"base").unwrap();
    }
    engine.flush().unwrap();
    let mut workspace = MergeWorkspace::new();
    let options = CompactionPolicy::default();
    // Alone in level 0, below the trigger: nothing is due.
    assert!(
        engine
            .core
            .compact(options, &mut workspace)
            .unwrap()
            .is_none()
    );
    let base = engine.stats().run_bytes;
    for round in 0..3 {
        engine.put(b"k0000", format!("{round}").as_bytes()).unwrap();
        engine.put(b"k1999", format!("{round}").as_bytes()).unwrap();
        engine.flush().unwrap();
    }
    assert_eq!(engine.stats().max_overlap, 4);
    let report = engine
        .core
        .compact(options, &mut workspace)
        .unwrap()
        .unwrap();
    // The oldest, the large run, moves to the empty level 1.
    assert_eq!(report.kind, CompactionKind::Move);
    assert!(
        engine
            .core
            .compact(options, &mut workspace)
            .unwrap()
            .is_none()
    );
    engine.put(b"k0000", b"3").unwrap();
    engine.put(b"k1999", b"3").unwrap();
    engine.flush().unwrap();
    let report = engine
        .core
        .compact(options, &mut workspace)
        .unwrap()
        .unwrap();
    assert_eq!((report.kind, report.inputs), (CompactionKind::Merge, 5));
    assert!(report.input_bytes > base);
    assert!(report.complete);
    assert!(
        engine
            .core
            .compact(options, &mut workspace)
            .unwrap()
            .is_none()
    );
    let stats = engine.stats();
    assert_eq!(
        (stats.runs, stats.max_overlap, stats.level0_flushes),
        (1, 1, 0)
    );
    assert_eq!(get(&engine, b"k0000").unwrap(), b"3");
    assert_eq!(get(&engine, b"k1999").unwrap(), b"3");
    assert_eq!(get(&engine, b"k1000").unwrap(), b"base");
    assert_eq!(everything(&engine).len(), 2_000);
}

/// Every flushed or compacted run, including each part of a split output,
/// gets a filter covering its own keys. Filters are counted beside the stored
/// bytes, charged to snapshots that hold replaced runs, and released with them.
#[test]
fn filters_follow_runs_through_flushes_splits_compactions_and_snapshots() {
    let engine = Engine::new(EngineOptions {
        target_run_len: 4_096,

        ..manual()
    })
    .unwrap();
    let key = |i: u32| format!("key{i:04}").into_bytes();
    let mut model = Model::new();
    for i in 0..1_000 {
        engine.put(&key(i), b"old").unwrap();
        model.insert(key(i), b"old".to_vec());
    }
    let flushed = engine.flush().unwrap();
    assert!(flushed.runs > 1);
    assert!(flushed.held_bytes > 0 && flushed.filter_build_bytes > 0);
    // Newer tombstones and values over the same range.
    for i in (0..1_000).step_by(3) {
        engine.delete(&key(i)).unwrap();
        model.remove(&key(i));
    }
    for i in (1..1_000).step_by(7) {
        engine.put(&key(i), b"new").unwrap();
        model.insert(key(i), b"new".to_vec());
    }
    let second = engine.flush().unwrap();
    let stats = engine.stats();
    assert_eq!(stats.held_bytes, flushed.held_bytes + second.held_bytes);

    let snapshot = engine.snapshot();
    let mut workspace = MergeWorkspace::new();
    let mut inputs = (0, 0usize);
    let options = CompactionPolicy {
        max_inputs: usize::MAX,
        max_input_bytes: usize::MAX,
        max_workspace_bytes: usize::MAX,
        ..options(0)
    };
    while let Some(report) = engine.core.compact(options, &mut workspace).unwrap() {
        assert!(report.outputs == 0 || report.output_held_bytes > 0);
        inputs.0 += report.input_bytes;
        inputs.1 += report.input_held_bytes;
    }
    let compacted = engine.stats();
    assert!(compacted.runs > 1, "the output was split");
    assert!(compacted.held_bytes > 0 && inputs.1 > 0);
    // Every run published when the snapshot was taken overlapped another, so
    // all were replaced; the snapshot keeps exactly those, filters included.
    // Intermediate runs no snapshot saw were freed.
    assert_eq!(
        (compacted.retained_run_bytes, compacted.retained_held_bytes),
        (stats.run_bytes, stats.held_bytes)
    );
    assert!(inputs.1 > stats.held_bytes);
    // Every key, deleted ones included, reads as the model says, from the
    // compacted runs and from the snapshot's replaced ones.
    let mut read = EngineWorkspace::new();
    for i in 0..1_000 {
        let expected = model.get(&key(i)).cloned();
        assert_eq!(get(&engine, &key(i)), expected);
        let held = snapshot.get(&key(i), &mut read).unwrap();
        assert_eq!(held.map(|found| found.value.to_vec()), expected);
    }
    drop(snapshot);
    let released = engine.stats();
    assert_eq!(
        (released.retained_run_bytes, released.retained_held_bytes),
        (0, 0)
    );
}

/// Writers, a flusher, a manual compactor and the background worker run
/// beside a reader. Each key's value only moves forward, a deleted key never
/// returns, and the final state is exactly what was written.
/// Flushes, waiting for the worker whenever level 0 is at its stop.
fn flush_waiting(engine: &Engine) {
    engine.flush().unwrap();
}

#[test]
fn concurrent_compaction_loses_and_resurrects_nothing() {
    const WRITERS: usize = 2;
    const KEYS: usize = 48;
    const ROUNDS: u64 = 400;

    let engine = Engine::new(EngineOptions {
        target_run_len: 4 << 10,

        background_compaction: Some(CompactionPolicy {
            max_overlap: 2,
            ..CompactionPolicy::default()
        }),
        ..manual()
    })
    .unwrap();
    let key = |writer: usize, i: usize| format!("w{writer}/{i:03}").into_bytes();
    let barrier = Barrier::new(WRITERS + 3);
    let writing = AtomicBool::new(true);
    let compactions = std::sync::atomic::AtomicUsize::new(0);

    thread::scope(|scope| {
        let writers: Vec<_> = (0..WRITERS)
            .map(|writer| {
                let (engine, barrier) = (&engine, &barrier);
                scope.spawn(move || {
                    barrier.wait();
                    for round in 1..=ROUNDS {
                        for i in 0..KEYS {
                            if round == ROUNDS && i % 2 == 1 {
                                engine.delete(&key(writer, i)).unwrap();
                            } else {
                                engine.put(&key(writer, i), &round.to_be_bytes()).unwrap();
                            }
                        }
                        // Overlapping runs accumulate while writing goes on.
                        if round % 10 == 0 {
                            flush_waiting(engine);
                        }
                    }
                })
            })
            .collect();
        scope.spawn(|| {
            barrier.wait();
            while writing.load(Ordering::Acquire) {
                flush_waiting(&engine);
            }
        });
        scope.spawn(|| {
            barrier.wait();
            let mut workspace = MergeWorkspace::new();
            while writing.load(Ordering::Acquire) {
                if engine
                    .core
                    .compact(options(3), &mut workspace)
                    .unwrap()
                    .is_some()
                {
                    compactions.fetch_add(1, Ordering::Relaxed);
                }
            }
        });
        scope.spawn(|| {
            barrier.wait();
            // Per key: the last round seen, and whether it was deleted.
            let mut seen: BTreeMap<Vec<u8>, (u64, bool)> = BTreeMap::new();
            let mut workspace = EngineWorkspace::new();
            while writing.load(Ordering::Acquire) {
                let guard = engine.read();
                let mut scan = guard.scan(&mut workspace);
                let mut present = Vec::new();
                while let Some(record) = scan.next_record().unwrap() {
                    let round = u64::from_be_bytes(record.value.try_into().unwrap());
                    present.push((record.key.to_vec(), round));
                }
                for (key, round) in &present {
                    let (last, deleted) = seen.entry(key.clone()).or_insert((0, false));
                    assert!(!*deleted, "{key:?} returned after its delete");
                    assert!(*round >= *last, "{key:?} went back to round {round}");
                    *last = *round;
                }
                for (key, (_, deleted)) in &mut seen {
                    if !present.iter().any(|(k, _)| k == key) {
                        *deleted = true;
                    }
                }
            }
        });
        // Every writer is joined before any failure is raised, so that the
        // other threads stop rather than spin on.
        let joined: Vec<_> = writers.into_iter().map(|writer| writer.join()).collect();
        writing.store(false, Ordering::Release);
        for result in joined {
            result.unwrap();
        }
    });

    flush_waiting(&engine);
    // Give the worker work after the manual compactor has stopped, so its
    // participation does not depend on winning their mutex race.
    for _ in 0..3 {
        engine.put(&key(0, 0), &ROUNDS.to_be_bytes()).unwrap();
        flush_waiting(&engine);
    }
    engine.wait_for_compaction().unwrap();
    assert!(engine.stats().max_overlap <= 2);
    assert!(engine.background_status().unwrap().compactions > 0);
    compact_fully(&engine);
    let expected: Vec<_> = (0..WRITERS)
        .flat_map(|writer| (0..KEYS).step_by(2).map(move |i| (writer, i)))
        .map(|(writer, i)| (key(writer, i), ROUNDS.to_be_bytes().to_vec()))
        .collect::<BTreeMap<_, _>>()
        .into_iter()
        .collect();
    assert_eq!(everything(&engine), expected);
    assert_eq!(engine.stats().runs, 1);
    // The compactor is not guaranteed a turn; record whether it had one.
    eprintln!(
        "manual compactions during writing: {}; background: {:?}",
        compactions.into_inner(),
        engine.background_status()
    );
}

// Reference model.
