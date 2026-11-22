//! Leveled compaction invariants: level membership and
//! disjointness after every publication, reads unaffected by levels,
//! recovered level membership, moves, splits, neighbour merges and
//! growth across many runs.
//!
//! Every publication in a test build checks the level invariants
//! (`Levels::check`, from `State::order_runs` and after each move), so every
//! test in the crate that flushes or compacts checks them too.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::Ordering;

use super::compaction_model_tests::Rng;
use super::*;

const TARGET: usize = 4 << 10;

fn options() -> Options {
    Options {
        memtable_size: 16 << 10,
        target_run_len: TARGET,
        background_compaction: None,
        ..Options::default()
    }
}

/// The defaults, with byte limits of 32 runs as the defaults are of four
/// default runs.
fn compaction() -> CompactionPolicy {
    CompactionPolicy {
        max_input_bytes: 32 * TARGET,

        ..CompactionPolicy::default()
    }
}

fn key(index: u64) -> Vec<u8> {
    format!("key/{index:08}").into_bytes()
}

fn value(seed: u64) -> Vec<u8> {
    super::compaction_model_tests::value_for(seed)
}

/// What compaction did until it had nothing to do or refused.
#[derive(Debug, Default)]
struct Steps {
    kinds: BTreeMap<String, usize>,
    written: usize,
    refusal: Option<CompactionError>,
}

fn settle_with(engine: &Engine, options: CompactionPolicy, steps: &mut Steps) {
    let mut workspace = MergeWorkspace::new();
    for _ in 0..100_000 {
        match engine.core.compact(options, &mut workspace) {
            Ok(Some(report)) => {
                *steps.kinds.entry(format!("{:?}", report.kind)).or_default() += 1;
                steps.written += report.output_bytes;
            }
            Ok(None) => return,
            Err(error) => {
                steps.refusal = Some(error);
                return;
            }
        }
    }
    panic!("compaction did not settle: {steps:?}");
}

fn settle(engine: &Engine, steps: &mut Steps) {
    settle_with(engine, compaction(), steps);
}

/// Each level's runs by identity, level 0 first.
fn level_ids(engine: &Engine) -> Vec<Vec<usize>> {
    let state = engine.core.read_state();
    state.only().levels.check(&state.only().runs);
    state
        .only()
        .levels
        .runs()
        .iter()
        .map(|level| level.iter().map(|run| Arc::as_ptr(run) as usize).collect())
        .collect()
}

fn get(engine: &Engine, key: &[u8]) -> Option<Vec<u8>> {
    engine.ks().get(key).unwrap()
}

fn scan(snapshot: &Snapshot) -> BTreeMap<Vec<u8>, Vec<u8>> {
    snapshot
        .records(b"")
        .into_iter()
        .map(|(key, _, value)| (key, value))
        .collect()
}

/// Random puts and deletes, flushed and compacted step by step: every read
/// matches a model, tombstones never let an older value back, snapshots keep
/// what they saw across compactions, and each kind of step happens.
#[test]
fn reads_match_a_model_through_leveled_compaction() {
    let engine = Engine::in_memory(options()).unwrap();
    let mut model: BTreeMap<Vec<u8>, Vec<u8>> = BTreeMap::new();
    let mut rng = Rng(5);
    let mut steps = Steps::default();
    let mut snapshots = Vec::new();
    for round in 0..60u64 {
        for write in 0..300 {
            let key = key(rng.below(3_000));
            if rng.below(4) == 0 {
                engine.ks().delete(&key).unwrap();
                model.remove(&key);
            } else {
                let value = value(round * 1_000 + write);
                engine.ks().put(&key, &value).unwrap();
                model.insert(key, value);
            }
        }
        engine.flush().unwrap();
        if round % 15 == 7 {
            snapshots.push((engine.snapshot(), model.clone()));
        }
        // One step at a time, checking reads between steps.
        let mut workspace = MergeWorkspace::new();
        while let Some(report) = engine.core.compact(compaction(), &mut workspace).unwrap() {
            *steps.kinds.entry(format!("{:?}", report.kind)).or_default() += 1;
            for _ in 0..20 {
                let key = key(rng.below(3_000));
                assert_eq!(get(&engine, &key), model.get(&key).cloned());
            }
        }
    }
    let now = engine.snapshot();
    assert_eq!(scan(&now), model, "nothing lost or resurrected");
    for (snapshot, seen) in &snapshots {
        assert_eq!(&scan(snapshot), seen, "a snapshot keeps what it saw");
    }
    let stats = engine.engine_stats();
    assert!(stats.levels >= 2, "{stats:?}");
    for kind in ["Merge", "Move"] {
        assert!(steps.kinds.contains_key(kind), "{steps:?}");
    }
    assert!(stats.level0_flushes <= 4, "{stats:?}");
    drop((now, snapshots));
    crate::test_support::wait_until(|| engine.engine_stats().retained_run_bytes == 0);
    assert_eq!(engine.engine_stats().retained_run_bytes, 0);
}

/// A merge replaces only its inputs: every other run stays the same shared
/// run in the same level, and a move keeps the run itself.
#[test]
fn a_merge_replaces_only_its_range_and_a_move_keeps_its_run() {
    let engine = Engine::in_memory(options()).unwrap();
    let mut workspace = MergeWorkspace::new();
    // Ascending loads overlap nothing: every flush moves down unchanged.
    for index in 0..6_000 {
        engine.ks().put(&key(index), &value(index)).unwrap();
        if index % 500 == 499 {
            engine.flush().unwrap();
            loop {
                let before = level_ids(&engine);
                let Some(report) = engine.core.compact(compaction(), &mut workspace).unwrap()
                else {
                    break;
                };
                let after = level_ids(&engine);
                let all = |levels: &[Vec<usize>]| -> BTreeSet<usize> {
                    levels.iter().flatten().copied().collect()
                };
                if report.kind == CompactionKind::Move {
                    assert_eq!(all(&before), all(&after), "a move rewrites nothing");
                    assert_eq!(report.output_bytes, 0);
                }
            }
        }
    }
    let runs = engine.engine_stats().runs;
    assert!(runs > 64, "{runs} runs, expected a multi-level fixture");
    assert!(engine.engine_stats().levels >= 2);

    // A database of one level: overwrite a narrow range and compact level
    // 0 at once. Only the runs that range meets are replaced.
    let engine = Engine::in_memory(options()).unwrap();
    for index in 0..400 {
        engine.ks().put(&key(index), &value(index)).unwrap();
        if index % 100 == 99 {
            engine.flush().unwrap();
        }
    }
    let eager = CompactionPolicy {
        max_overlap: 1,
        ..compaction()
    };
    let mut steps = Steps::default();
    settle_with(&engine, eager, &mut steps);
    assert_eq!(steps.written, 0, "ascending flushes only move");
    let before = level_ids(&engine);
    assert_eq!(before.len(), 2);
    assert!(before[1].len() > 8, "{before:?}");
    for index in 200..210 {
        engine.ks().put(&key(index), b"updated").unwrap();
    }
    engine.flush().unwrap();
    let mut workspace = MergeWorkspace::new();
    let mut replaced = 0;
    while let Some(report) = engine.core.compact(eager, &mut workspace).unwrap() {
        assert_eq!(report.kind, CompactionKind::Merge);
        replaced += report.inputs - 1;
    }
    let after: BTreeSet<usize> = level_ids(&engine).into_iter().flatten().collect();
    let kept = before[1].iter().filter(|id| after.contains(id)).count();
    assert!((1..=3).contains(&replaced), "{replaced} runs replaced");
    assert_eq!(
        kept,
        before[1].len() - replaced,
        "untouched runs stay shared"
    );
    assert_eq!(get(&engine, &key(205)), Some(b"updated".to_vec()));
    assert_eq!(get(&engine, &key(199)), Some(value(199)));
}

/// Recovery preserves every ID and level, including overlapping level-0
/// runs, without compacting at open; later writes and compaction stay valid.
#[test]
fn recovery_preserves_run_ids_and_levels() {
    let dir = std::env::temp_dir().join(format!("tessera-leveled-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let mut model = BTreeMap::new();
    let expected;
    {
        let engine = Engine::in_memory(options()).unwrap();
        for index in 0..8_000u64 {
            let key = key(index * 7 % 8_000);
            engine.ks().put(&key, &value(index)).unwrap();
            model.insert(key, value(index));
        }
        let mut steps = Steps::default();
        settle(&engine, &mut steps);
        engine.checkpoint_to(&dir).unwrap();
        expected = engine
            .core
            .read_state()
            .only()
            .levels
            .runs()
            .iter()
            .map(|level| level.iter().map(|run| run.id).collect::<Vec<_>>())
            .collect::<Vec<_>>();
        engine.close().unwrap();
    }
    let engine = Engine::open(&dir, options()).unwrap();
    let _ = std::fs::remove_dir_all(&dir);
    level_ids(&engine);
    let stats = engine.engine_stats();
    let actual: Vec<Vec<_>> = engine
        .core
        .read_state()
        .only()
        .levels
        .runs()
        .iter()
        .map(|level| level.iter().map(|run| run.id).collect())
        .collect();
    assert_eq!(actual, expected);
    assert_eq!(stats.levels, expected.len() - 1);
    assert_eq!(stats.level0_runs, expected[0].len());
    assert!(stats.runs > 64, "{} runs", stats.runs);

    let mut steps = Steps::default();
    settle(&engine, &mut steps);
    assert!(steps.refusal.is_none(), "{steps:?}");
    level_ids(&engine);
    assert_eq!(scan(&engine.snapshot()), model);

    let mut rng = Rng(3);
    for write in 0..4_000 {
        let key = key(rng.below(8_000));
        engine.ks().put(&key, &value(write + 100_000)).unwrap();
        model.insert(key, value(write + 100_000));
    }
    engine.flush().unwrap();
    settle(&engine, &mut steps);
    assert!(steps.refusal.is_none(), "{steps:?}");
    assert_eq!(scan(&engine.snapshot()), model);
}

/// A checkpoint whose runs occupy level 1 supplies split boundaries for
/// the first flushes after recovery. Puts and deletes at those boundaries
/// read back exactly, held snapshots stay stable, and a second checkpoint
/// recovers the same contents.
#[test]
fn flushes_after_recovery_split_at_the_recovered_runs() {
    // Keep the fixture in level 1 so its recovered boundaries split flushes.
    let original_options = options();
    let options = || Options {
        level_ratio: 1000,
        ..original_options
    };
    let dir = std::env::temp_dir().join(format!("tessera-split-recovery-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let mut model = BTreeMap::new();
    {
        let engine = Engine::in_memory(options()).unwrap();
        for index in 0..6_000u64 {
            let key = key(index * 7 % 6_000);
            engine.ks().put(&key, &value(index)).unwrap();
            model.insert(key, value(index));
        }
        settle(&engine, &mut Steps::default());
        engine.checkpoint_to(&dir).unwrap();
        engine.close().unwrap();
    }
    let checkpointed = model.clone();
    let writes = |engine: &Engine, model: &mut BTreeMap<Vec<u8>, Vec<u8>>| {
        let mut rng = Rng(21);
        for write in 0..1_500u64 {
            let key = key(rng.below(6_000));
            if write % 5 == 0 {
                engine.ks().delete(&key).unwrap();
                model.remove(&key);
            } else {
                engine.ks().put(&key, &value(write + 50_000)).unwrap();
                model.insert(key, value(write + 50_000));
            }
        }
        engine.flush().unwrap();
        engine.engine_stats().level0_runs
    };
    let whole = Engine::open(&dir, options()).unwrap();
    whole.set_split_flushes(false);
    let unsplit = writes(&whole, &mut model.clone());
    drop(whole);
    let engine = Engine::open(&dir, options()).unwrap();
    let recovered = engine.engine_stats().runs;
    let existing_level0 = engine.engine_stats().level0_runs;
    let before = engine.snapshot();
    let level0 = writes(&engine, &mut model);
    assert!(
        level0 > unsplit,
        "the flushes were cut at recovered boundaries: {level0} level-0 runs, {unsplit} unsplit"
    );
    assert_eq!(engine.engine_stats().levels, 1, "nothing has compacted yet");
    assert_eq!(scan(&engine.snapshot()), model);
    assert_eq!(
        engine.engine_stats().runs,
        recovered + level0 - existing_level0
    );
    settle(&engine, &mut Steps::default());
    level_ids(&engine);
    assert_eq!(scan(&engine.snapshot()), model);
    assert_eq!(scan(&before), checkpointed);
    drop(before);

    let again = dir.with_extension("again");
    let _ = std::fs::remove_dir_all(&again);
    engine.checkpoint_to(&again).unwrap();
    engine.close().unwrap();
    let engine = Engine::open(&again, options()).unwrap();
    assert_eq!(scan(&engine.snapshot()), model);
    let _ = std::fs::remove_dir_all(&dir);
    let _ = std::fs::remove_dir_all(&again);
}

/// Small disjoint flushes move down unchanged and leave small adjacent
/// runs, which are merged in pairs until no two neighbours are both small.
#[test]
fn small_neighbours_merge() {
    let engine = Engine::in_memory(options()).unwrap();
    let mut steps = Steps::default();
    for index in 0..3_000u64 {
        engine.ks().put(&key(index), &value(index)).unwrap();
        if index % 3 == 2 {
            engine.flush().unwrap();
            settle(&engine, &mut steps);
        }
    }
    assert!(
        steps.kinds.get("Neighbours").copied().unwrap_or(0) > 100,
        "{steps:?}"
    );
    let stats = engine.engine_stats();
    // 1,000 tiny flushes of about 400 logical bytes; segments of about
    // 4 KiB logical. Pairs of small neighbours merge until one is at least
    // half a segment, so at most about four runs per segment remain.
    assert!(stats.runs < 250, "{stats:?}");
    for index in (0..3_000).step_by(37) {
        assert_eq!(get(&engine, &key(index)), Some(value(index)));
    }
}

/// A growing dataset spans many runs and levels while directory overlap
/// stays bounded by level-0 flushes and disjoint deeper levels.
#[test]
fn growing_data_spans_many_runs_with_bounded_read_overlap() {
    let engine = Engine::in_memory(Options {
        background_compaction: Some(compaction()),
        ..options()
    })
    .unwrap();
    for index in 0..20_000u64 {
        let key = key(index * 104_729 % 20_000);
        engine.ks().put(&key, &value(index)).unwrap();
    }
    engine.wait_for_compaction().unwrap();
    let stats = engine.engine_stats();
    assert!(stats.runs > 64, "{stats:?}");
    assert!(stats.levels >= 2, "{stats:?}");
    // A lookup visits a run per level-0 flush and one per level.
    assert!(
        stats.max_overlap <= stats.level0_flushes + stats.levels,
        "{stats:?}"
    );
    for index in (0..20_000).step_by(997) {
        assert!(get(&engine, &key(index)).is_some());
    }
    engine.close().unwrap();
}

// Flush splitting and level-0 backpressure.

/// The same writes into an engine that splits flushes at level 1's
/// boundaries and one that does not: reads agree with a model in both, and
/// each flush is cut into at most eight runs per run of the unsplit flush.
#[test]
fn flushes_split_at_level_1_boundaries_and_stay_bounded() {
    let engines = [true, false].map(|split| {
        let engine = Engine::in_memory(options()).unwrap();
        engine.set_split_flushes(split);
        engine
    });
    let mut model = BTreeMap::new();
    let mut rng = Rng(17);
    let mut runs = [0usize; 2];
    let mut steps = [Steps::default(), Steps::default()];
    for round in 0..60u64 {
        // Under one memtable, so that only these flushes happen.
        for write in 0..50 {
            let key = key(rng.below(4_000));
            let value = value(round * 1_000 + write);
            for engine in &engines {
                engine.ks().put(&key, &value).unwrap();
            }
            model.insert(key, value);
        }
        let flushed = engines
            .each_ref()
            .map(|engine| engine.flush().unwrap().runs);
        assert_eq!(engines[0].engine_stats().flushes, round + 1);
        assert!(flushed[0] <= 8 * flushed[1], "{flushed:?}");
        for (at, engine) in engines.iter().enumerate() {
            runs[at] += flushed[at];
            settle(engine, &mut steps[at]);
            assert!(steps[at].refusal.is_none(), "{:?}", steps[at]);
        }
    }
    assert!(runs[0] > runs[1], "splitting cut some flushes: {runs:?}");
    for engine in &engines {
        assert_eq!(scan(&engine.snapshot()), model);
        level_ids(engine);
    }
}

/// A flush prepared before compaction changes level 1 publishes with the
/// cuts it took: they only group its records, so reads and levels stay
/// right, and the next compactions take the pieces as any level-0 runs.
#[test]
fn a_flush_split_at_stale_boundaries_publishes_correctly() {
    let engine = Engine::in_memory(options()).unwrap();
    let mut model = BTreeMap::new();
    let eager = CompactionPolicy {
        max_overlap: 1,
        ..compaction()
    };
    let mut steps = Steps::default();
    let mut put = |engine: &Engine, index: u64, seed: u64| {
        engine.ks().put(&key(index), &value(seed)).unwrap();
        model.insert(key(index), value(seed));
    };
    for index in 0..240 {
        put(&engine, index * 10, index);
        if index % 40 == 39 {
            engine.flush().unwrap();
        }
    }
    settle_with(&engine, eager, &mut steps);
    let before = level_ids(&engine);
    // Level 0 holds a flush that the next compaction merges into level 1.
    for index in 0..40 {
        put(&engine, index * 61, 10_000 + index);
    }
    engine.flush().unwrap();
    // This flush takes its cuts now...
    for index in 0..40 {
        put(&engine, index * 59 + 3, 20_000 + index);
    }
    let job = engine.core.begin_flush().unwrap();
    assert!(!job.spaces[0].2.at.is_empty());
    // ... then compaction rewrites level 1 under it.
    let mut workspace = MergeWorkspace::new();
    let report = engine.core.compact(eager, &mut workspace).unwrap().unwrap();
    assert_eq!(report.kind, CompactionKind::Merge);
    assert_ne!(level_ids(&engine)[1..], before[1..]);
    job.build().publish();
    level_ids(&engine);
    assert_eq!(scan(&engine.snapshot()), model);
    settle_with(&engine, eager, &mut steps);
    assert!(steps.refusal.is_none(), "{steps:?}");
    assert_eq!(scan(&engine.snapshot()), model);
}

/// Writes with a worker held back by the test: once level 0 reaches its
/// stop, the writer's flush waits rather than failing, without the state
/// lock, and completes as soon as the worker can compact.
#[test]
fn a_flush_at_the_level_0_stop_waits_for_the_worker() {
    let engine = Engine::in_memory(Options {
        background_compaction: Some(compaction()),
        ..options()
    })
    .unwrap();
    engine.set_level0_stop(2);
    let held = engine.core.compacting.lock().unwrap();
    std::thread::scope(|scope| {
        let writer = scope.spawn(|| {
            let mut model = BTreeMap::new();
            for flush in 0..4u64 {
                for index in 0..40 {
                    let key = key(index * 7 + flush);
                    engine.ks().put(&key, &value(flush)).unwrap();
                    model.insert(key, value(flush));
                }
                engine.flush().unwrap();
            }
            model
        });
        while engine.core.level0_waits.load(Ordering::SeqCst) == 0 && !writer.is_finished() {
            std::thread::yield_now();
        }
        // Blocked at the stop, holding no lock a reader needs.
        assert_eq!(engine.engine_stats().level0_flushes, 2);
        assert!(!writer.is_finished());
        drop(held);
        let model = writer.join().unwrap();
        assert_eq!(scan(&engine.snapshot()), model);
    });
    engine.wait_for_compaction().unwrap();
    assert!(engine.engine_stats().level0_flushes < 2);
    engine.close().unwrap();
}

#[test]
fn settled_sequences_are_zero_and_a_snapshot_during_build_keeps_its_sequences() {
    let engine = Engine::in_memory(options()).unwrap();
    let limits = CompactionPolicy {
        max_overlap: 1,
        ..compaction()
    };
    engine.ks().put(b"a", b"first").unwrap();
    engine.ks().put(b"stable", b"base").unwrap();
    engine.ks().put(b"deleted", b"live").unwrap();
    engine.flush().unwrap();
    settle_with(&engine, limits, &mut Steps::default());
    engine.ks().put(b"a", b"second").unwrap();
    engine.ks().delete(b"deleted").unwrap();
    engine.flush().unwrap();
    let ceiling = engine.engine_stats().last_seq;
    let original = engine.get_seq(b"a").unwrap().0;
    assert!(original > 0);
    let job = engine.core.begin_job(limits).unwrap().unwrap();
    assert!(job.zero_seq);
    // This snapshot is taken after eligibility was decided. Its own runs
    // keep the original sequences even though the new output erases them.
    let snapshot = engine.snapshot();
    job.build(&mut MergeWorkspace::new())
        .unwrap()
        .publish()
        .unwrap();
    assert_eq!(engine.get_seq(b"a"), Some((0, b"second".to_vec())));
    assert_eq!(engine.get_seq(b"stable").unwrap().0, 0);
    assert_eq!(engine.ks().get(b"deleted").unwrap(), None);
    assert_eq!(snapshot.get_seq(b"a"), Some((original, b"second".to_vec())));
    assert_eq!(snapshot.get_seq(b"stable").unwrap().0, 2);
    assert_eq!(snapshot.last_seq(), ceiling);
    drop(snapshot);
    let dir = std::env::temp_dir().join(format!("tessera-seq-zero-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    engine.checkpoint_to(&dir).unwrap();
    let recovered = Engine::open(&dir, options()).unwrap();
    assert_eq!(recovered.engine_stats().last_seq, ceiling);
    assert_eq!(recovered.get_seq(b"a").unwrap().0, 0);
    recovered.ks().put(b"a", b"third").unwrap();
    assert_eq!(recovered.get_seq(b"a").unwrap().0, ceiling + 1);
    assert_eq!(
        scan(&recovered.snapshot()).get(&b"a"[..]),
        Some(&b"third".to_vec())
    );
    std::fs::remove_dir_all(&dir).unwrap();
}

#[test]
fn snapshots_before_compaction_and_snapshots_of_seq_zero_data_stay_visible() {
    let engine = Engine::in_memory(options()).unwrap();
    let limits = CompactionPolicy {
        max_overlap: 1,
        ..compaction()
    };
    let drain = || settle_with(&engine, limits, &mut Steps::default());
    engine.ks().put(b"key", b"one").unwrap();
    engine.flush().unwrap();
    drain();
    let before = engine.snapshot();
    engine.ks().put(b"key", b"two").unwrap();
    engine.flush().unwrap();
    drain();
    assert_eq!(before.get_seq(b"key"), Some((1, b"one".to_vec())));
    assert_eq!(engine.get_seq(b"key").unwrap().0, 0);
    drop(before);
    engine.ks().put(b"key", b"three").unwrap();
    engine.flush().unwrap();
    drain();
    assert_eq!(engine.get_seq(b"key").unwrap().0, 0);
    let settled = engine.snapshot();
    engine.ks().delete(b"key").unwrap();
    engine.flush().unwrap();
    drain();
    assert_eq!(engine.ks().get(b"key").unwrap(), None);
    assert_eq!(settled.get_seq(b"key"), Some((0, b"three".to_vec())));
    assert_eq!(scan(&settled).get(&b"key"[..]), Some(&b"three".to_vec()));
    drop(settled);
    engine.ks().put(b"key", b"four").unwrap();
    engine.flush().unwrap();
    drain();
    assert_eq!(get(&engine, b"key"), Some(b"four".to_vec()));
}

#[test]
fn an_unselected_overlapping_run_keeps_sequences_until_the_final_merge() {
    let engine = Engine::in_memory(options()).unwrap();
    let limits = CompactionPolicy {
        max_inputs: 2,
        max_overlap: 1,
        ..compaction()
    };
    engine.ks().put(b"key", b"one").unwrap();
    engine.flush().unwrap();
    settle_with(&engine, limits, &mut Steps::default());
    engine.ks().put(b"key", b"two").unwrap();
    engine.flush().unwrap();
    engine.ks().put(b"key", b"three").unwrap();
    engine.flush().unwrap();
    let job = engine.core.begin_job(limits).unwrap().unwrap();
    assert!(!job.others.is_empty());
    assert!(job.zero_seq);
    job.build(&mut MergeWorkspace::new())
        .unwrap()
        .publish()
        .unwrap();
    assert_eq!(get(&engine, b"key"), Some(b"three".to_vec()));
    settle_with(&engine, limits, &mut Steps::default());
    assert_eq!(engine.get_seq(b"key"), Some((0, b"three".to_vec())));
}

/// The bottom level's records by key, with their stored sequences.
fn bottom_seqs(engine: &Engine) -> BTreeMap<Vec<u8>, u64> {
    let state = engine.core.read_state();
    let bottom = state.only().levels.runs().last().unwrap();
    let mut out = BTreeMap::new();
    for run in bottom {
        let mut workspace = crate::run::ReadWorkspace::new();
        let mut cursor = run.run.cursor(&mut workspace);
        while let Some(record) = cursor.next_record().unwrap() {
            out.insert(record.key.to_vec(), record.seq);
        }
    }
    out
}

/// A level-0 run left out of a job into the bottom while a newer one is
/// taken contests only the keys it may hold: the output run mixes erased and
/// kept sequences. Reads match the model before and after the skipped run is
/// merged, through a snapshot from before the job and checkpoints taken
/// across it and after it.
#[test]
fn a_skipped_interleaved_run_keeps_only_the_sequences_it_may_contest() {
    let engine = Engine::in_memory(Options {
        compression: Compression::Uncompressed,
        ..options()
    })
    .unwrap();
    let mut model = BTreeMap::new();
    let mut flush = |records: &[(&[u8], usize)], tag: &str| {
        for &(key, len) in records {
            let value = format!("{tag}-{}", "x".repeat(len)).into_bytes();
            engine.ks().put(key, &value).unwrap();
            model.insert(key.to_vec(), value);
        }
        engine.flush().unwrap();
        engine.engine_stats().last_seq
    };
    let eager = CompactionPolicy {
        max_overlap: 1,
        ..compaction()
    };
    // Two bottom runs, each over half a segment so that they stay apart.
    let wide = 400;
    flush(
        &[b"a", b"b", b"c", b"d", b"e", b"f"].map(|key| (&key[..], wide)),
        "r1",
    );
    settle_with(&engine, eager, &mut Steps::default());
    flush(&[b"p", b"q", b"z"].map(|key| (&key[..], 2 * wide)), "r2");
    settle_with(&engine, eager, &mut Steps::default());
    assert_eq!(
        engine
            .core
            .read_state()
            .only()
            .levels
            .runs()
            .last()
            .unwrap()
            .len(),
        2
    );
    // Level 0, oldest first: the source, a run reaching both bottom runs,
    // and a newer run over the source's bottom run only.
    flush(&[(b"b", 1), (b"d", 1)], "o1");
    let skipped_min = engine.engine_stats().last_seq + 1;
    flush(&[(b"c", 1), (b"e", 1), (b"q", 1)], "o2");
    let newest = flush(&[(b"b", 1), (b"e", 1)], "o3");
    let job = engine
        .core
        .begin_job(CompactionPolicy {
            max_inputs: 3,
            ..eager
        })
        .unwrap()
        .unwrap();
    assert!(job.zero_seq);
    assert_eq!(job.inputs.len(), 3);
    assert_eq!(job.others.len(), 1);
    assert_eq!(job.others[0].run.min_seq(), skipped_min);
    assert_eq!(job.seq_floors().len(), 1);
    let before = engine.snapshot();
    let before_seqs = bottom_seqs(&engine);
    let base = std::env::temp_dir().join(format!("tessera-mixed-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&base);
    let built = job.build(&mut MergeWorkspace::new()).unwrap();
    engine.checkpoint_to(&base.join("across")).unwrap();
    built.publish().unwrap();
    // Only `e` keeps its sequence: the skipped run holds an older `e`. Its
    // `c` is newer than the output's, and it does not cover `b` or `f`.
    let seqs = bottom_seqs(&engine);
    // The other bottom run was not rewritten.
    let kept: Vec<&[u8]> = seqs
        .iter()
        .filter(|&(_, &seq)| seq != 0)
        .map(|(key, _)| key.as_slice())
        .collect();
    assert_eq!(kept, [&b"e"[..], b"p", b"q", b"z"]);
    assert_eq!(seqs[&b"e"[..]], newest);
    assert_eq!(seqs[&b"a"[..]], 0);
    assert_eq!(scan(&engine.snapshot()), model);
    assert_eq!(engine.get_seq(b"e").unwrap().0, newest);
    assert_eq!(engine.get_seq(b"b").unwrap().0, 0);
    // The snapshot from before the job still reads its own sequences.
    assert_eq!(before.get_seq(b"b").unwrap().0, newest - 1);
    assert_eq!(before.get_seq(b"d").unwrap().0, skipped_min - 1);
    assert_eq!(scan(&before), model);
    drop(before);
    engine.checkpoint_to(&base.join("after")).unwrap();
    for (name, bottom) in [("across", before_seqs), ("after", seqs)] {
        let recovered = Engine::open(base.join(name), options()).unwrap();
        assert_eq!(scan(&recovered.snapshot()), model, "{name}");
        assert_eq!(bottom_seqs(&recovered), bottom, "{name}");
        let state = recovered.core.read_state();
        for run in &state.only().runs {
            let records = bottom_seqs_of(&run.run);
            assert_eq!(run.run.min_seq(), records.iter().copied().min().unwrap());
            assert_eq!(run.run.max_seq(), records.iter().copied().max().unwrap());
        }
    }
    // Once the skipped run is merged, nothing contests any key.
    settle_with(&engine, eager, &mut Steps::default());
    assert_eq!(scan(&engine.snapshot()), model);
    assert_eq!(engine.core.read_state().only().levels.runs()[0].len(), 0);
    assert!(bottom_seqs(&engine).values().all(|&seq| seq == 0));
    std::fs::remove_dir_all(&base).unwrap();
}

fn bottom_seqs_of(run: &Run<Vec<u8>>) -> Vec<u64> {
    let mut workspace = crate::run::ReadWorkspace::new();
    let mut cursor = run.cursor(&mut workspace);
    let mut out = Vec::new();
    while let Some(record) = cursor.next_record().unwrap() {
        out.push(record.seq);
    }
    out
}

/// Level-0 compaction expands from the earliest level-0 run to other runs that
/// overlap its key span. When every flush in level 0 is split at level-1
/// boundaries, one step merges only one key-range slice and leaves the other
/// slices in level 0; when the earliest flush spans both slices without a cut,
/// one step sweeps every overlapping level-0 run across both slices.
#[test]
fn split_level0_flushes_compact_by_overlapping_slice_unlike_unsplit() {
    let run_case = |split_first_flush: bool| {
        let engine = Engine::in_memory(Options {
            compression: Compression::Uncompressed,
            ..options()
        })
        .unwrap();
        let eager = CompactionPolicy {
            max_overlap: 1,
            ..compaction()
        };
        // Seed two disjoint bottom runs so later flushes split between them.
        for key in [b"a", b"b", b"c", b"d", b"e", b"f"] {
            engine.ks().put(key, &[b'1'; 400]).unwrap();
        }
        engine.flush().unwrap();
        settle_with(&engine, eager, &mut Steps::default());
        for key in [b"p", b"q", b"r", b"s", b"t", b"z"] {
            engine.ks().put(key, &[b'2'; 400]).unwrap();
        }
        engine.flush().unwrap();
        settle_with(&engine, eager, &mut Steps::default());
        assert_eq!(
            engine
                .core
                .read_state()
                .only()
                .levels
                .runs()
                .last()
                .unwrap()
                .len(),
            2
        );

        // Publish four flushes spanning both bottom runs.
        for round in 0..4u8 {
            engine.core.write_state().split_flushes = split_first_flush || round > 0;
            for key in [b"b", b"c", b"d", b"q", b"r", b"s"] {
                engine.ks().put(key, &[round; 200]).unwrap();
            }
            engine.flush().unwrap();
        }
        let before_l0_runs = engine.core.read_state().only().levels.runs()[0].len();
        let trigger_four = CompactionPolicy {
            max_overlap: 4,
            ..compaction()
        };
        let mut workspace = MergeWorkspace::new();
        let report = engine
            .core
            .compact(trigger_four, &mut workspace)
            .unwrap()
            .unwrap();
        assert_eq!(report.kind, CompactionKind::Merge);
        let state = engine.core.read_state();
        let after_l0_runs = state.only().levels.runs()[0].len();
        let after_l0_flushes = state.only().levels.level0_flushes();
        (
            before_l0_runs,
            report.inputs,
            after_l0_runs,
            after_l0_flushes,
        )
    };

    let (split_before, split_inputs, split_after_runs, split_after_flushes) = run_case(true);
    assert_eq!(split_before, 8);
    assert_eq!(split_inputs, 5); // 4 lower-slice L0 runs + 1 lower bottom run
    assert_eq!(split_after_runs, 4);
    assert_eq!(split_after_flushes, 4);

    let (unsplit_before, unsplit_inputs, unsplit_after_runs, unsplit_after_flushes) =
        run_case(false);
    assert_eq!(unsplit_before, 7); // 1 full-span L0 run + 3 split flushes (6 runs)
    assert_eq!(unsplit_inputs, 9); // all 7 L0 runs + both bottom runs
    assert_eq!(unsplit_after_runs, 0);
    assert_eq!(unsplit_after_flushes, 0);
}
