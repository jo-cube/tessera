//! Compaction: what a merge keeps, the limits that stop it, and how the engine
//! replaces runs while reads, writes and flushes continue.
//!
//! Block, run and merge behaviour have their own tests and are not repeated.
//! Expected results come from maps maintained by the tests. Concurrency tests
//! use barriers and shared flags and never sleep.

use std::collections::BTreeMap;
use std::sync::Barrier;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;

use tessera::block::Value;
use tessera::engine::{
    CompactionOptions, Engine, EngineOptions, EngineWorkspace, Snapshot, UpdateError,
};
use tessera::run::{
    CONTEXT_ALLOWANCE, CompactError, CompactLimits, Compacted, ConflictingCopies, MergeWorkspace,
    ReadError, ReadWorkspace, Run, RunBuilder, compact,
};

// Mechanism.

type Physical = (Vec<u8>, u64, Option<Vec<u8>>);

fn run(records: &[(&str, u64, Option<&str>)]) -> Run<Vec<u8>> {
    let mut builder = RunBuilder::default();
    for &(key, seq, value) in records {
        let value = value.map_or(Value::Tombstone, |v| Value::Bytes(v.as_bytes()));
        builder.add(key.as_bytes(), seq, value).unwrap();
    }
    builder.finish()
}

fn records(run: &Run<Vec<u8>>) -> Vec<Physical> {
    let mut workspace = ReadWorkspace::new();
    let mut cursor = run.cursor(&mut workspace);
    let mut out = Vec::new();
    while let Some(record) = cursor.next_record().unwrap() {
        let value = match record.value {
            Value::Bytes(bytes) => Some(bytes.to_vec()),
            Value::Tombstone => None,
        };
        out.push((record.key.to_vec(), record.seq, value));
    }
    out
}

fn physical(key: &str, seq: u64, value: Option<&str>) -> Physical {
    (key.into(), seq, value.map(Into::into))
}

const UNLIMITED: CompactLimits = CompactLimits {
    target_run_len: usize::MAX,
    max_output_bytes: usize::MAX,
    max_workspace_bytes: usize::MAX,
};

fn merge(
    inputs: &[&Run<Vec<u8>>],
    keep_tombstone: impl FnMut(&[u8]) -> bool,
    limits: CompactLimits,
) -> Result<Compacted, CompactError> {
    let views: Vec<_> = inputs.iter().map(|run| run.view()).collect();
    compact(&views, keep_tombstone, limits, &mut MergeWorkspace::new())
}

fn merged(compacted: &Compacted) -> Vec<Physical> {
    compacted.runs.iter().flat_map(records).collect()
}

/// The non-MVCC rule: only the newest version of a key survives, and an
/// identical copy counts once wherever it sits in the version chain.
#[test]
fn only_the_newest_version_survives_and_copies_count_once() {
    let older = run(&[
        ("a", 1, Some("a1")),
        ("b", 2, Some("b2")),
        ("c", 3, Some("c3")),
    ]);
    let newer = run(&[("a", 5, Some("a5")), ("b", 2, Some("b2")), ("d", 6, None)]);
    let out = merge(&[&older, &newer], |_| true, UNLIMITED).unwrap();
    assert_eq!(
        merged(&out),
        [
            physical("a", 5, Some("a5")),
            physical("b", 2, Some("b2")),
            physical("c", 3, Some("c3")),
            physical("d", 6, None),
        ]
    );
    let stats = out.stats;
    assert_eq!((stats.records_read, stats.records_written), (6, 4));
    assert_eq!((stats.versions_dropped, stats.copies_collapsed), (1, 1));
    assert_eq!(stats.tombstones_dropped, 0);
    assert_eq!(
        out.output_bytes,
        out.runs.iter().map(|r| r.as_bytes().len()).sum::<usize>()
    );
}

/// Dropping an obsolete version must not hide that the inputs disagree about
/// it: no later read could tell which was meant.
#[test]
fn differing_copies_fail_the_merge_even_when_obsolete() {
    let left = run(&[("k", 9, Some("new")), ("k", 4, Some("x"))]);
    let right = run(&[("k", 4, Some("y"))]);
    let error = merge(&[&left, &right], |_| true, UNLIMITED).unwrap_err();
    assert_eq!(
        error,
        CompactError::Read(ReadError::ConflictingCopies(ConflictingCopies {
            key: b"k".to_vec(),
            seq: 4,
            inputs: (0, 1),
        }))
    );
}

#[test]
fn a_newest_tombstone_is_written_only_when_the_caller_keeps_it() {
    let older = run(&[("drop", 2, Some("v")), ("keep", 1, Some("v"))]);
    let newer = run(&[("drop", 4, None), ("keep", 3, None)]);
    let out = merge(&[&older, &newer], |key| key == b"keep", UNLIMITED).unwrap();
    assert_eq!(merged(&out), [physical("keep", 3, None)]);
    assert_eq!(out.stats.tombstones_dropped, 1);
    assert_eq!(out.stats.versions_dropped, 2);

    // Everything deleted: nothing is built at all.
    let out = merge(&[&older, &newer], |_| false, UNLIMITED).unwrap();
    assert!(out.runs.is_empty());
    assert_eq!(out.output_bytes, 0);
}

#[test]
fn output_is_split_into_ascending_disjoint_runs() {
    let records: Vec<(String, u64, Option<String>)> = (0..400)
        .map(|i| (format!("key{i:04}"), i + 1, Some(format!("value{i}"))))
        .collect();
    let borrowed: Vec<_> = records
        .iter()
        .map(|(k, s, v)| (k.as_str(), *s, v.as_deref()))
        .collect();
    let (left, right) = borrowed.split_at(200);
    let (left, right) = (run(left), run(right));
    let limits = CompactLimits {
        target_run_len: 1_000,
        ..UNLIMITED
    };
    let out = merge(&[&right, &left], |_| true, limits).unwrap();
    assert!(out.runs.len() > 10, "{}", out.runs.len());
    let ranges: Vec<_> = out
        .runs
        .iter()
        .map(|r| r.key_range().unwrap().unwrap())
        .collect();
    assert!(ranges.windows(2).all(|pair| pair[0].max < pair[1].min));
    assert_eq!(merged(&out).len(), 400);
}

/// Each limit fails with a precise error and nothing is returned. The
/// workspace limit fails before any block is decoded.
#[test]
fn limits_fail_the_merge_with_the_amount_needed() {
    let older = run(&[("a", 1, Some("one")), ("b", 2, Some("two"))]);
    let newer = run(&[("a", 3, Some("three"))]);
    let full = merge(&[&older, &newer], |_| true, UNLIMITED).unwrap();

    // Output counts the stored bytes and the filters together.
    let output = full.output_bytes + full.filter_bytes;
    assert!(full.filter_bytes > 0);
    let error = merge(
        &[&older, &newer],
        |_| true,
        CompactLimits {
            max_output_bytes: output - 1,
            ..UNLIMITED
        },
    )
    .unwrap_err();
    assert_eq!(
        error,
        CompactError::OutputLimit {
            output_bytes: output,
            max_output_bytes: output - 1
        }
    );
    let at_limit = CompactLimits {
        max_output_bytes: output,
        ..UNLIMITED
    };
    merge(&[&older, &newer], |_| true, at_limit).unwrap();

    let views = [older.view(), newer.view()];
    let mut workspace = MergeWorkspace::new();
    let limits = CompactLimits {
        max_workspace_bytes: full.workspace_bytes - 1,
        ..UNLIMITED
    };
    let error = compact(&views, |_| true, limits, &mut workspace).unwrap_err();
    assert_eq!(
        error,
        CompactError::WorkspaceLimit {
            workspace_bytes: full.workspace_bytes,
            max_workspace_bytes: full.workspace_bytes - 1,
        }
    );
    assert_eq!(workspace.blocks_loaded(), 0);

    // What the estimate leaves of the workspace limit holds the key hashes:
    // none left means output without a filter, which reads the same.
    for (spare, filtered) in [(0, false), (15, false), (16, true)] {
        let limits = CompactLimits {
            max_workspace_bytes: full.workspace_bytes + spare,
            ..UNLIMITED
        };
        let out = merge(&[&older, &newer], |_| true, limits).unwrap();
        assert_eq!(out.filter_bytes > 0, filtered, "{spare}");
        assert_eq!(merged(&out), merged(&full));
        let key = b"a".as_slice();
        assert_eq!(
            out.runs[0].get(key, &mut ReadWorkspace::new()).unwrap(),
            full.runs[0].get(key, &mut ReadWorkspace::new()).unwrap()
        );
    }
}

/// The workspace estimate allows a fixed amount per codec context; the real
/// contexts, measured by Zstandard itself, must fit in it.
#[test]
fn measured_codec_contexts_fit_their_allowance() {
    let input: Vec<(String, u64, Option<String>)> = (0..3_000)
        .map(|i| (format!("key{i:05}"), i + 1, Some(format!("{:064}", i * 7))))
        .collect();
    let mut builder = RunBuilder::default();
    for (key, seq, value) in &input {
        let value = Value::Bytes(value.as_deref().unwrap().as_bytes());
        builder.add(key.as_bytes(), *seq, value).unwrap();
    }
    assert!(builder.context_bytes() > 0);
    assert!(
        builder.context_bytes() <= CONTEXT_ALLOWANCE,
        "{}",
        builder.context_bytes()
    );
    let run = builder.finish();

    let views = [run.view()];
    let mut workspace = MergeWorkspace::new();
    compact(&views, |_| true, UNLIMITED, &mut workspace).unwrap();
    assert!(workspace.context_bytes() > 0);
    assert!(workspace.context_bytes() <= CONTEXT_ALLOWANCE);
}

// Engine.

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

fn snapshot_contents(snapshot: &Snapshot) -> Vec<(Vec<u8>, Vec<u8>)> {
    let mut workspace = EngineWorkspace::new();
    let mut scan = snapshot.scan(&mut workspace);
    let mut out = Vec::new();
    while let Some(record) = scan.next_record().unwrap() {
        out.push((record.key.to_vec(), record.value.to_vec()));
    }
    out
}

/// A snapshot equals the model as it was when the snapshot was taken.
fn check_snapshot(snapshot: &Snapshot, then: &Model, pool: &[Vec<u8>]) {
    let expected: Vec<_> = then.clone().into_iter().collect();
    assert_eq!(snapshot_contents(snapshot), expected);
    let mut workspace = EngineWorkspace::new();
    for key in pool {
        let found = snapshot.get(key, &mut workspace).unwrap();
        assert_eq!(
            found.map(|found| found.value.to_vec()),
            then.get(key).cloned()
        );
    }
}

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
    let job = engine.begin_compaction(options(2)).unwrap().unwrap();
    assert!(!job.is_complete());
    let report = job
        .build(&mut MergeWorkspace::new())
        .unwrap()
        .publish()
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
    assert!(flushed.filter_bytes > 0 && flushed.filter_build_bytes > 0);
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
    assert_eq!(
        stats.filter_bytes,
        flushed.filter_bytes + second.filter_bytes
    );

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
        assert!(report.outputs == 0 || report.output_filter_bytes > 0);
        inputs.0 += report.input_bytes;
        inputs.1 += report.input_filter_bytes;
    }
    let compacted = engine.stats();
    assert!(compacted.runs > 1, "the output was split");
    assert!(compacted.filter_bytes > 0 && inputs.1 > 0);
    // Every run published when the snapshot was taken overlapped another, so
    // all were replaced; the snapshot keeps exactly those, filters included.
    // Intermediate runs no snapshot saw were freed.
    assert_eq!(
        (
            compacted.retained_run_bytes,
            compacted.retained_filter_bytes
        ),
        (stats.run_bytes, stats.filter_bytes)
    );
    assert!(inputs.1 > stats.filter_bytes);
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
        (released.retained_run_bytes, released.retained_filter_bytes),
        (0, 0)
    );
}

/// Runs flushed between selection and publication, and the memtables, are
/// newer than every input. They stay published and keep winning.
#[test]
fn publication_keeps_what_was_flushed_and_written_meanwhile() {
    let engine = Engine::new(EngineOptions::default());
    for round in 0..3 {
        engine.put(b"a", format!("a{round}").as_bytes()).unwrap();
        engine.put(b"b", b"b").unwrap();
        engine.flush().unwrap();
    }
    let job = engine.begin_compaction(options(8)).unwrap().unwrap();
    engine.delete(b"a").unwrap();
    engine.flush().unwrap();
    let built = job.build(&mut MergeWorkspace::new()).unwrap();
    engine.put(b"c", b"c").unwrap();
    engine.flush().unwrap();
    engine.put(b"b", b"memtable").unwrap();

    let before = engine.stats();
    let report = built.publish().unwrap();
    assert_eq!((report.inputs, report.outputs), (3, 1));
    let after = engine.stats();
    assert_eq!(after.runs, before.runs - 3 + 1);
    assert_eq!(
        after.run_bytes,
        before.run_bytes - report.input_bytes + report.output_bytes
    );
    assert_eq!(
        everything(&engine),
        [
            (b"b".to_vec(), b"memtable".to_vec()),
            (b"c".to_vec(), b"c".to_vec())
        ]
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

struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^ (z >> 31)
    }

    fn below(&mut self, bound: usize) -> usize {
        (self.next() % bound as u64) as usize
    }
}

enum Stage<'e> {
    Idle,
    Begun(tessera::engine::CompactionJob<'e>),
    Built(tessera::engine::BuiltCompaction<'e>),
}

/// Writes, flushes, snapshots and partial compactions, staged so that writes
/// and flushes land between selection, build and publication. After every
/// step reads equal the latest-update map, every report adds up, and a
/// retained snapshot still equals the map as it was when it was taken.
#[test]
fn randomized_compactions_match_an_independent_model() {
    const SEEDS: u64 = 30;
    const OPERATIONS: usize = 400;
    const BACKGROUND_SEEDS: u64 = 10;

    let pool: Vec<Vec<u8>> = (0..40u8)
        .map(|i| vec![b'k', i / 4, i])
        .chain([Vec::new(), vec![0xFF; 3]])
        .collect();
    let (mut compactions, mut incomplete, mut dropped_tombstones) = (0, 0, 0);
    let (mut snapshots, mut outlived) = (0, 0);

    // Then a few seeds again with a background worker compacting beside the
    // staged manual compactions. Counts come from the manual seeds only.
    let runs = (0..SEEDS).map(|seed| (seed, false));
    for (seed, background) in runs.chain((0..BACKGROUND_SEEDS).map(|seed| (seed, true))) {
        let mut rng = Rng(seed);
        let engine = Engine::new(EngineOptions {
            target_run_len: [64, 1_024, 1 << 20][rng.below(3)],
            max_runs: usize::MAX,
            max_run_bytes: usize::MAX,
            background_compaction: background.then(|| CompactionOptions {
                max_overlap: 1 + seed as usize % 3,
                ..CompactionOptions::default()
            }),
            // Odd seeds use small memtables, which the history pinned by
            // snapshots fills, so writes flush them.
            memtable_budget: if seed % 2 == 0 { 64 << 20 } else { 4 << 10 },
            ..EngineOptions::default()
        });
        let mut model: BTreeMap<Vec<u8>, Vec<u8>> = BTreeMap::new();
        let mut stage = Stage::Idle;
        let mut workspace = MergeWorkspace::new();
        let mut held: Vec<(Snapshot, Model)> = Vec::new();

        for _ in 0..OPERATIONS {
            match rng.below(100) {
                55..=59 if held.len() >= 6 || (!held.is_empty() && rng.below(3) == 0) => {
                    held.swap_remove(rng.below(held.len()));
                }
                55..=59 => match engine.snapshot() {
                    Ok(snapshot) => {
                        assert_eq!(snapshot.last_seq(), engine.stats().last_seq);
                        held.push((snapshot, model.clone()));
                        snapshots += 1;
                    }
                    Err(error) => panic!("seed {seed}: {error}"),
                },
                0..=54 => {
                    let key = pool[rng.below(pool.len())].clone();
                    let value = (rng.below(4) != 0).then(|| {
                        let len = if rng.below(10) == 0 { 3_000 } else { 12 };
                        (0..rng.below(len))
                            .map(|_| rng.next() as u8)
                            .collect::<Vec<u8>>()
                    });
                    let write = |engine: &Engine| match &value {
                        Some(value) => engine.put(&key, value),
                        None => engine.delete(&key),
                    };
                    // A write with no room flushes first.
                    if let Err(error) = write(&engine) {
                        panic!("seed {seed}: {error}");
                    }
                    match value {
                        Some(value) => model.insert(key, value),
                        None => model.remove(&key),
                    };
                }
                60..=74 => {
                    engine.flush().unwrap();
                }
                _ => {
                    let options = CompactionOptions {
                        max_inputs: 2 + rng.below(4),
                        max_overlap: 1 + rng.below(3),
                        disjoint_runs_above: rng.below(4),
                        ..CompactionOptions::default()
                    };
                    stage = match stage {
                        Stage::Idle => match engine.begin_compaction(options).unwrap() {
                            Some(job) => Stage::Begun(job),
                            None => Stage::Idle,
                        },
                        Stage::Begun(job) => Stage::Built(job.build(&mut workspace).unwrap()),
                        Stage::Built(built) => {
                            let ranges: Vec<_> = built
                                .runs()
                                .iter()
                                .map(|run| run.key_range().unwrap().unwrap())
                                .map(|range| (range.min.to_vec(), range.max.to_vec()))
                                .collect();
                            assert!(ranges.windows(2).all(|pair| pair[0].1 < pair[1].0));
                            let before = engine.stats();
                            let report = built.publish().unwrap();
                            let after = engine.stats();
                            assert_eq!(after.runs, before.runs - report.inputs + report.outputs);
                            assert_eq!(
                                after.run_bytes,
                                before.run_bytes - report.input_bytes + report.output_bytes
                            );
                            let stats = report.stats;
                            assert_eq!(
                                stats.records_read,
                                stats.records_written
                                    + stats.versions_dropped
                                    + stats.copies_collapsed
                                    + stats.tombstones_dropped,
                                "seed {seed}, background {background}"
                            );
                            if !background {
                                compactions += 1;
                                incomplete += usize::from(!report.complete);
                                dropped_tombstones += stats.tombstones_dropped;
                            }
                            Stage::Idle
                        }
                    };
                }
            }
            let expected: Vec<_> = model.clone().into_iter().collect();
            assert_eq!(
                everything(&engine),
                expected,
                "seed {seed}, background {background}"
            );
            for key in &pool {
                assert_eq!(
                    get(&engine, key),
                    model.get(key).cloned(),
                    "seed {seed}, background {background}"
                );
            }
            if !held.is_empty() {
                let (snapshot, then) = &held[rng.below(held.len())];
                check_snapshot(snapshot, then, &pool);
            }
        }
        drop(stage);
        engine.flush().unwrap();
        if background {
            engine.wait_for_compaction().unwrap();
            assert!(engine.background_status().unwrap().compactions > 0);
        }
        compact_fully(&engine);
        // Fully compacted: nothing overlaps and no two tails are adjacent.
        let done = options(usize::MAX);
        assert!(engine.compact(done, &mut workspace).unwrap().is_none());
        let expected: Vec<_> = model.into_iter().collect();
        assert_eq!(
            everything(&engine),
            expected,
            "seed {seed}, background {background}"
        );
        // Everything a snapshot saw has since been flushed and compacted
        // away, and the snapshot still reads it.
        for (snapshot, then) in &held {
            check_snapshot(snapshot, then, &pool);
        }
        outlived += held.len();
        let stats = engine.stats();
        assert_eq!(stats.snapshots, held.len());
        assert!(held.is_empty() <= (stats.retained_bytes == 0));
        drop(held);
        let stats = engine.stats();
        assert_eq!((stats.snapshots, stats.retained_bytes), (0, 0));
    }
    assert!(snapshots > 300, "{snapshots}");
    assert!(outlived > 20, "{outlived}");
    assert!(compactions > 300, "{compactions}");
    assert!(incomplete > 50, "{incomplete}");
    assert!(dropped_tombstones > 50, "{dropped_tombstones}");
}
