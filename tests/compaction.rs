//! Compaction in the engine: which runs it selects, the limits that stop it,
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
    CompactionOptions, Engine, EngineOptions, EngineWorkspace, MergeWorkspace, UpdateError,
};

/// Merges any overlap, up to `max_inputs` runs at a time.
fn options(max_inputs: usize) -> CompactionOptions {
    CompactionOptions {
        max_inputs,
        max_overlap: 1,
        disjoint_runs_above: 0,
        ..CompactionOptions::default()
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
    let options = CompactionOptions {
        max_inputs: usize::MAX,
        max_input_bytes: usize::MAX,
        max_workspace_bytes: usize::MAX,
        ..options(0)
    };
    while engine.compact(options, &mut workspace).unwrap().is_some() {}
}

/// The resurrection hazard: a tombstone merged without the older run holding
/// the value it hides must be kept, and may go only once that run is merged.
#[test]
fn a_tombstone_hiding_an_unselected_run_survives_until_that_run_is_merged() {
    let engine = Engine::new(EngineOptions::default());
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

    // k100 lies in all three runs. The two small ones are chosen; the large
    // one, left out, holds k100's value.
    let report = engine
        .compact(options(2), &mut MergeWorkspace::new())
        .unwrap()
        .unwrap();
    assert_eq!((report.inputs, report.outputs), (2, 1));
    assert!(!report.complete);
    assert_eq!(report.stats.tombstones_dropped, 0);
    assert_eq!(get(&engine, b"k100"), None);
    assert_eq!(engine.stats().runs, 2);

    let report = engine
        .compact(options(8), &mut MergeWorkspace::new())
        .unwrap()
        .unwrap();
    assert!(report.complete);
    assert_eq!(report.stats.tombstones_dropped, 1);
    assert_eq!(get(&engine, b"k100"), None);
    assert_eq!(get(&engine, b"k150").unwrap(), b"new");
    assert_eq!(get(&engine, b"k151").unwrap(), b"new");
    assert_eq!(everything(&engine).len(), 499);
    assert_eq!(engine.stats().runs, 1);
}

/// The waste the size rule removes: a large run covering the same keys as a
/// few small ones is left in place rather than rewritten to absorb them.
#[test]
fn small_runs_are_merged_without_rewriting_a_large_run_over_them() {
    let engine = Engine::new(EngineOptions::default());
    for i in 0..2_000 {
        engine.put(format!("k{i:04}").as_bytes(), b"base").unwrap();
    }
    engine.flush().unwrap();
    let base = engine.stats().run_bytes;
    for round in 0..4 {
        engine.put(b"k0000", format!("{round}").as_bytes()).unwrap();
        engine.put(b"k1999", format!("{round}").as_bytes()).unwrap();
        engine.flush().unwrap();
    }
    assert_eq!(engine.stats().max_overlap, 5);

    let mut workspace = MergeWorkspace::new();
    let options = CompactionOptions::default();
    let report = engine.compact(options, &mut workspace).unwrap().unwrap();
    assert_eq!(report.inputs, 4);
    assert!(report.input_bytes < base / 10);
    assert!(!report.complete);
    // Nothing more is eligible, so compacting again rewrites nothing.
    assert!(engine.compact(options, &mut workspace).unwrap().is_none());
    let stats = engine.stats();
    assert_eq!((stats.runs, stats.max_overlap), (2, 2));
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
        max_runs: usize::MAX,
        max_run_bytes: usize::MAX,
        ..EngineOptions::default()
    });
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

    let snapshot = engine.snapshot().unwrap();
    let mut workspace = MergeWorkspace::new();
    let mut inputs = (0, 0usize);
    let options = CompactionOptions {
        max_inputs: usize::MAX,
        max_input_bytes: usize::MAX,
        max_workspace_bytes: usize::MAX,
        ..options(0)
    };
    while let Some(report) = engine.compact(options, &mut workspace).unwrap() {
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

/// Every limit refuses before anything changes, and the same compaction
/// succeeds once the limit allows it.
#[test]
fn a_compaction_over_any_limit_changes_nothing() {
    let engine = Engine::new(EngineOptions {
        target_run_len: 64,
        ..EngineOptions::default()
    });
    for round in 0..4 {
        for i in 0..20 {
            engine
                .put(
                    format!("k{i:02}").as_bytes(),
                    format!("v{round}").as_bytes(),
                )
                .unwrap();
        }
        engine.flush().unwrap();
    }
    let stats = engine.stats();
    let contents = everything(&engine);
    let mut workspace = MergeWorkspace::new();
    let refusals = [
        CompactionOptions {
            max_input_bytes: 1,
            ..options(8)
        },
        CompactionOptions {
            max_output_bytes: 1,
            ..options(8)
        },
        CompactionOptions {
            max_workspace_bytes: 1,
            ..options(8)
        },
    ];
    for refusal in refusals {
        assert!(engine.compact(refusal, &mut workspace).is_err());
        assert_eq!(engine.stats(), stats);
        assert_eq!(everything(&engine), contents);
    }
    // Over lowered run limits, a compaction that shrinks storage is still
    // allowed, so that it can make room for flushes.
    engine.set_run_limits(1, stats.run_bytes / 2);
    let report = engine.compact(options(8), &mut workspace).unwrap().unwrap();
    assert!(report.complete);
    assert_eq!(everything(&engine), contents);
}

/// Writers, a flusher, a manual compactor and the background worker run
/// beside a reader. Each key's value only moves forward, a deleted key never
/// returns, and the final state is exactly what was written.
#[test]
fn concurrent_compaction_loses_and_resurrects_nothing() {
    const WRITERS: usize = 2;
    const KEYS: usize = 48;
    const ROUNDS: u64 = 400;

    let engine = Engine::new(EngineOptions {
        target_run_len: 4 << 10,
        max_runs: usize::MAX,
        max_run_bytes: usize::MAX,
        background_compaction: Some(CompactionOptions {
            max_overlap: 2,
            ..CompactionOptions::default()
        }),
        ..EngineOptions::default()
    });
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
                            let result = if round == ROUNDS && i % 2 == 1 {
                                engine.delete(&key(writer, i))
                            } else {
                                engine.put(&key(writer, i), &round.to_be_bytes())
                            };
                            assert!(!matches!(result, Err(UpdateError::SequencesExhausted)));
                            result.unwrap();
                        }
                        // Overlapping runs accumulate while writing goes on.
                        if round % 10 == 0 {
                            engine.flush().unwrap();
                        }
                    }
                })
            })
            .collect();
        scope.spawn(|| {
            barrier.wait();
            while writing.load(Ordering::Acquire) {
                engine.flush().unwrap();
            }
        });
        scope.spawn(|| {
            barrier.wait();
            let mut workspace = MergeWorkspace::new();
            while writing.load(Ordering::Acquire) {
                if engine
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
        for writer in writers {
            writer.join().unwrap();
        }
        writing.store(false, Ordering::Release);
    });

    engine.flush().unwrap();
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
