//! Leveled compaction in production (5AT): level membership and
//! disjointness after every publication, reads unaffected by levels,
//! recovery into one level, moves, splits, neighbour merges, refusals and
//! growth past the old run ceiling.
//!
//! Every publication in a test build checks the level invariants
//! (`Levels::check`, from `State::order_runs` and after each move), so every
//! test in the crate that flushes or compacts checks them too.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::Ordering;

use super::capacity_tests::Rng;
use super::*;

const TARGET: usize = 4 << 10;

fn options() -> EngineOptions {
    EngineOptions {
        memtable_budget: 16 << 10,
        target_run_len: TARGET,
        background_compaction: None,
        ..EngineOptions::default()
    }
}

/// The defaults, with byte limits of 32 runs as the defaults are of four
/// default runs.
fn compaction() -> CompactionOptions {
    CompactionOptions {
        max_input_bytes: 32 * TARGET,
        max_output_bytes: 32 * TARGET,
        ..CompactionOptions::default()
    }
}

fn key(index: u64) -> Vec<u8> {
    format!("key/{index:08}").into_bytes()
}

fn value(seed: u64) -> Vec<u8> {
    super::capacity_tests::value_for(seed)
}

/// What compaction did until it had nothing to do or refused.
#[derive(Debug, Default)]
struct Steps {
    kinds: BTreeMap<String, usize>,
    written: usize,
    refusal: Option<CompactionError>,
}

fn settle_with(engine: &Engine, options: CompactionOptions, steps: &mut Steps) {
    let mut workspace = MergeWorkspace::new();
    for _ in 0..100_000 {
        match engine.compact(options, &mut workspace) {
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
    state.levels.check(&state.runs);
    state
        .levels
        .runs()
        .iter()
        .map(|level| level.iter().map(|run| Arc::as_ptr(run) as usize).collect())
        .collect()
}

fn get(engine: &Engine, key: &[u8]) -> Option<Vec<u8>> {
    engine
        .get(key, &mut EngineWorkspace::new())
        .unwrap()
        .map(|live| live.value.to_vec())
}

fn scan(snapshot: &Snapshot) -> BTreeMap<Vec<u8>, Vec<u8>> {
    let mut workspace = EngineWorkspace::new();
    let mut scan = snapshot.scan(&mut workspace);
    let mut out = BTreeMap::new();
    while let Some(record) = scan.next_record().unwrap() {
        out.insert(record.key.to_vec(), record.value.to_vec());
    }
    out
}

/// Random puts and deletes, flushed and compacted step by step: every read
/// matches a model, tombstones never let an older value back, snapshots keep
/// what they saw across compactions, and each kind of step happens.
#[test]
fn reads_match_a_model_through_leveled_compaction() {
    let engine = Engine::new(options()).unwrap();
    let mut model: BTreeMap<Vec<u8>, Vec<u8>> = BTreeMap::new();
    let mut rng = Rng(5);
    let mut steps = Steps::default();
    let mut snapshots = Vec::new();
    for round in 0..60u64 {
        for write in 0..300 {
            let key = key(rng.below(3_000));
            if rng.below(4) == 0 {
                engine.delete(&key).unwrap();
                model.remove(&key);
            } else {
                let value = value(round * 1_000 + write);
                engine.put(&key, &value).unwrap();
                model.insert(key, value);
            }
        }
        engine.flush().unwrap();
        if round % 15 == 7 {
            snapshots.push((engine.snapshot().unwrap(), model.clone()));
        }
        // One step at a time, checking reads between steps.
        let mut workspace = MergeWorkspace::new();
        while let Some(report) = engine.compact(compaction(), &mut workspace).unwrap() {
            *steps.kinds.entry(format!("{:?}", report.kind)).or_default() += 1;
            for _ in 0..20 {
                let key = key(rng.below(3_000));
                assert_eq!(get(&engine, &key), model.get(&key).cloned());
            }
        }
    }
    let now = engine.snapshot().unwrap();
    assert_eq!(scan(&now), model, "nothing lost or resurrected");
    for (snapshot, seen) in &snapshots {
        assert_eq!(&scan(snapshot), seen, "a snapshot keeps what it saw");
    }
    let stats = engine.stats();
    assert!(stats.levels >= 2, "{stats:?}");
    for kind in ["Merge", "Move"] {
        assert!(steps.kinds.contains_key(kind), "{steps:?}");
    }
    assert!(stats.level0_flushes <= 4, "{stats:?}");
    drop((now, snapshots));
    assert_eq!(engine.stats().retained_run_bytes, 0);
}

/// A merge replaces only its inputs: every other run stays the same shared
/// run in the same level, and a move keeps the run itself.
#[test]
fn a_merge_replaces_only_its_range_and_a_move_keeps_its_run() {
    let engine = Engine::new(options()).unwrap();
    let mut workspace = MergeWorkspace::new();
    // Ascending loads overlap nothing: every flush moves down unchanged.
    for index in 0..6_000 {
        engine.put(&key(index), &value(index)).unwrap();
        if index % 500 == 499 {
            engine.flush().unwrap();
            loop {
                let before = level_ids(&engine);
                let Some(report) = engine.compact(compaction(), &mut workspace).unwrap() else {
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
    let runs = engine.stats().runs;
    assert!(runs > 64, "{runs} runs, past the old limit of 64");
    assert!(engine.stats().levels >= 2);

    // A database of one level: overwrite a narrow range and compact level
    // 0 at once. Only the runs that range meets are replaced.
    let engine = Engine::new(options()).unwrap();
    for index in 0..400 {
        engine.put(&key(index), &value(index)).unwrap();
        if index % 100 == 99 {
            engine.flush().unwrap();
        }
    }
    let eager = CompactionOptions {
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
        engine.put(&key(index), b"updated").unwrap();
    }
    engine.flush().unwrap();
    let mut workspace = MergeWorkspace::new();
    let mut replaced = 0;
    while let Some(report) = engine.compact(eager, &mut workspace).unwrap() {
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

/// A move that drains level 0 lifts a flush refused at the level-0 stop,
/// as a merge does: ascending data is compacted by moves alone, so a
/// refusal that only publications lifted would stand for good.
#[test]
fn a_move_lifts_a_refusal_at_the_level_0_stop() {
    let engine = Engine::new(options()).unwrap();
    engine.set_level0_stop(2);
    let mut refused = None;
    for flush in 0..3 {
        for index in flush * 20..(flush + 1) * 20 {
            engine.put(&key(index), &value(index)).unwrap();
        }
        if let Err(error) = engine.flush() {
            refused = Some(error);
        }
    }
    let refused = refused.expect("the third flush passes the stop");
    assert_eq!((refused.level0_flushes, refused.max_level0_flushes), (3, 2));
    let mut steps = Steps::default();
    settle_with(
        &engine,
        CompactionOptions {
            max_overlap: 1,
            ..compaction()
        },
        &mut steps,
    );
    assert_eq!(steps.written, 0, "{steps:?}");
    assert!(steps.kinds.contains_key("Move"), "{steps:?}");
    // A write needing room flushes rather than failing on the old refusal.
    let flushes = engine.stats().flushes;
    let mut index = 60;
    while engine.stats().flushes == flushes {
        engine.put(&key(index), &value(index)).unwrap();
        index += 1;
    }
    assert_eq!(get(&engine, &key(59)), Some(value(59)));
}

/// Recovery puts a checkpoint's runs in one level with level 0 empty;
/// compaction then only adds levels above them, writing nothing, and
/// later writes keep the levels valid.
#[test]
fn recovery_puts_runs_in_one_level() {
    let dir = std::env::temp_dir().join(format!("tessera-leveled-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let mut model = BTreeMap::new();
    {
        let engine = Engine::new(options()).unwrap();
        for index in 0..8_000u64 {
            let key = key(index * 7 % 8_000);
            engine.put(&key, &value(index)).unwrap();
            model.insert(key, value(index));
        }
        let mut steps = Steps::default();
        settle(&engine, &mut steps);
        engine
            .checkpoint(&dir, CheckpointOptions::default())
            .unwrap();
        engine.close().unwrap();
    }
    let engine = Engine::recover(&dir, options()).unwrap();
    let _ = std::fs::remove_dir_all(&dir);
    let levels = level_ids(&engine);
    let stats = engine.stats();
    assert_eq!((levels.len(), stats.level0_runs, stats.levels), (2, 0, 1));
    assert_eq!(levels[1].len(), stats.runs);
    assert!(stats.runs > 64, "{} runs", stats.runs);

    let mut steps = Steps::default();
    settle(&engine, &mut steps);
    assert_eq!(steps.written, 0, "{steps:?}");
    assert!(steps.refusal.is_none(), "{steps:?}");
    let stats = engine.stats();
    assert!(stats.levels > 1, "levels are inserted above recovered runs");
    let last = level_ids(&engine).pop().unwrap();
    assert_eq!(last, levels[1], "recovered runs stay where they are");

    let mut rng = Rng(3);
    for write in 0..4_000 {
        let key = key(rng.below(8_000));
        engine.put(&key, &value(write + 100_000)).unwrap();
        model.insert(key, value(write + 100_000));
    }
    engine.flush().unwrap();
    settle(&engine, &mut steps);
    assert!(steps.refusal.is_none(), "{steps:?}");
    assert_eq!(scan(&engine.snapshot().unwrap()), model);
}

/// Until compaction inserts levels above them, recovered runs are level 1,
/// so the first flushes after recovery are split at the checkpoint's run
/// boundaries: puts and deletes split there read back exactly, a snapshot
/// taken before them is unchanged, and a second checkpoint of the result
/// recovers the same contents.
#[test]
fn flushes_after_recovery_split_at_the_recovered_runs() {
    let dir = std::env::temp_dir().join(format!("tessera-split-recovery-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let mut model = BTreeMap::new();
    {
        let engine = Engine::new(options()).unwrap();
        for index in 0..6_000u64 {
            let key = key(index * 7 % 6_000);
            engine.put(&key, &value(index)).unwrap();
            model.insert(key, value(index));
        }
        settle(&engine, &mut Steps::default());
        engine
            .checkpoint(&dir, CheckpointOptions::default())
            .unwrap();
        engine.close().unwrap();
    }
    let checkpointed = model.clone();
    let writes = |engine: &Engine, model: &mut BTreeMap<Vec<u8>, Vec<u8>>| {
        let mut rng = Rng(21);
        for write in 0..1_500u64 {
            let key = key(rng.below(6_000));
            if write % 5 == 0 {
                engine.delete(&key).unwrap();
                model.remove(&key);
            } else {
                engine.put(&key, &value(write + 50_000)).unwrap();
                model.insert(key, value(write + 50_000));
            }
        }
        engine.flush().unwrap();
        engine.stats().level0_runs
    };
    let whole = Engine::recover(&dir, options()).unwrap();
    whole.set_split_flushes(false);
    let unsplit = writes(&whole, &mut model.clone());
    drop(whole);
    let engine = Engine::recover(&dir, options()).unwrap();
    let recovered = engine.stats().runs;
    let before = engine.snapshot().unwrap();
    let level0 = writes(&engine, &mut model);
    assert!(
        level0 > unsplit,
        "the flushes were cut at recovered boundaries: {level0} level-0 runs, {unsplit} unsplit"
    );
    assert_eq!(engine.stats().levels, 1, "nothing has compacted yet");
    assert_eq!(scan(&engine.snapshot().unwrap()), model);
    assert_eq!(engine.stats().runs, recovered + level0);
    settle(&engine, &mut Steps::default());
    level_ids(&engine);
    assert_eq!(scan(&engine.snapshot().unwrap()), model);
    assert_eq!(scan(&before), checkpointed);
    drop(before);

    let again = dir.with_extension("again");
    let _ = std::fs::remove_dir_all(&again);
    engine
        .checkpoint(&again, CheckpointOptions::default())
        .unwrap();
    engine.close().unwrap();
    let engine = Engine::recover(&again, options()).unwrap();
    assert_eq!(scan(&engine.snapshot().unwrap()), model);
    let _ = std::fs::remove_dir_all(&dir);
    let _ = std::fs::remove_dir_all(&again);
}

/// A level-0 run over more runs below than one job may take is split at
/// their boundaries into pieces whose own merges fit, which then proceed:
/// compaction settles, no piece is split again, and nothing is lost.
#[test]
fn a_split_lets_a_wide_run_proceed() {
    let engine = Engine::new(options()).unwrap();
    let mut model = BTreeMap::new();
    for index in 0..6_000u64 {
        engine.put(&key(index), &value(index)).unwrap();
        model.insert(key(index), value(index));
    }
    let mut steps = Steps::default();
    settle(&engine, &mut steps);
    let below = engine.stats().runs;
    assert!(below > 40, "{below} runs");
    // One flush touching every run below, five times over.
    let narrow = CompactionOptions {
        max_inputs: 6,
        max_overlap: 1,
        ..compaction()
    };
    let mut steps = Steps::default();
    for round in 0..5 {
        for index in (round..6_000).step_by(97) {
            engine.put(&key(index), b"wide").unwrap();
            model.insert(key(index), b"wide".to_vec());
        }
        engine.flush().unwrap();
        settle_with(&engine, narrow, &mut steps);
        assert!(steps.refusal.is_none(), "{steps:?}");
    }
    let splits = steps.kinds.get("Split").copied().unwrap_or(0);
    assert!(splits > 0, "{steps:?}");
    // Each wide flush is split once per level it passes through, at most.
    assert!(splits <= 5 * engine.stats().levels, "{steps:?}");
    assert_eq!(scan(&engine.snapshot().unwrap()), model);
}

/// Small disjoint flushes move down unchanged and leave small adjacent
/// runs, which are merged in pairs until no two neighbours are both small.
#[test]
fn small_neighbours_merge() {
    let engine = Engine::new(options()).unwrap();
    let mut steps = Steps::default();
    for index in 0..3_000u64 {
        engine.put(&key(index), &value(index)).unwrap();
        if index % 3 == 2 {
            engine.flush().unwrap();
            settle(&engine, &mut steps);
        }
    }
    assert!(
        steps.kinds.get("Neighbours").copied().unwrap_or(0) > 100,
        "{steps:?}"
    );
    let stats = engine.stats();
    // 1,000 tiny flushes of about 400 logical bytes; segments of about
    // 4 KiB logical. Pairs of small neighbours merge until one is at least
    // half a segment, so at most about four runs per segment remain.
    assert!(stats.runs < 200, "{stats:?}");
    for index in (0..3_000).step_by(37) {
        assert_eq!(get(&engine, &key(index)), Some(value(index)));
    }
}

/// A job beyond the limits is refused before anything is read, naming its
/// size, and leaves every run and level as it was; raising the limit lets
/// it proceed.
#[test]
fn a_job_over_the_limits_is_refused_and_changes_nothing() {
    let engine = Engine::new(options()).unwrap();
    // Records longer than the run length are runs of their own; two
    // overlapping flushes of them need a job of two such runs.
    let mut noise = 0x2545_f491_4f6c_dd1d_u64;
    let big: Vec<u8> = (0..3 * TARGET)
        .map(|_| {
            noise ^= noise << 13;
            noise ^= noise >> 7;
            noise ^= noise << 17;
            noise as u8
        })
        .collect();
    for round in 0..6 {
        engine.put(&key(1), &big).unwrap();
        engine.put(&key(2), &big).unwrap();
        engine.put(&key(3 + round), b"small").unwrap();
        engine.flush().unwrap();
    }
    let tight = CompactionOptions {
        max_input_bytes: 2 * TARGET,
        max_output_bytes: 2 * TARGET,
        max_overlap: 1,
        ..CompactionOptions::default()
    };
    // The oldest flush moves down; the next meets it and is refused.
    let mut steps = Steps::default();
    settle_with(&engine, tight, &mut steps);
    let error = steps.refusal.expect("refused");
    let CompactionError::JobLimit {
        level,
        inputs,
        input_bytes,
        max_input_bytes,
        ..
    } = error
    else {
        panic!("{error:?}");
    };
    assert_eq!(level, 0);
    assert!(inputs >= 2 && input_bytes > max_input_bytes, "{error}");
    assert!(error.to_string().contains("raise them"), "{error}");
    let before = level_ids(&engine);
    let stats = engine.stats();
    assert_eq!(
        engine.compact(tight, &mut MergeWorkspace::new()),
        Err(error)
    );
    assert_eq!(level_ids(&engine), before);
    assert_eq!(engine.stats(), stats);
    // Larger limits proceed.
    let mut steps = Steps::default();
    settle_with(
        &engine,
        CompactionOptions {
            max_overlap: 1,
            ..compaction()
        },
        &mut steps,
    );
    assert!(steps.refusal.is_none(), "{steps:?}");
    assert_eq!(get(&engine, &key(1)), Some(big));
    assert_eq!(engine.stats().level0_flushes, 0);
}

/// While snapshots retain too much, compaction is refused and level 0
/// fills to its stop, after which flushes are refused naming it; once the
/// snapshot is dropped, compaction drains level 0 and flushes resume.
#[test]
fn a_retention_refusal_recovers_once_snapshots_end() {
    let engine = Engine::new(EngineOptions {
        snapshots: SnapshotLimits {
            max_retained_bytes: 0,
            ..SnapshotLimits::default()
        },
        background_compaction: Some(compaction()),
        ..options()
    })
    .unwrap();
    let mut rng = Rng(9);
    let mut write = 0u64;
    let mut put = |engine: &Engine| {
        write += 1;
        engine.put(&key(rng.below(500)), &value(write))
    };
    // Without a snapshot, a writer that outpaces the worker waits for it.
    let waiting = |engine: &Engine, put: &mut dyn FnMut(&Engine) -> Result<u64, UpdateError>| loop {
        match put(engine) {
            Err(UpdateError::FlushRefused(_)) => engine.wait_for_compaction().unwrap(),
            written => return written.unwrap(),
        }
    };
    for _ in 0..2_000 {
        waiting(&engine, &mut put);
    }
    engine.wait_for_compaction().unwrap();
    engine.flush().unwrap();
    let snapshot = engine.snapshot().unwrap();
    let mut refused = None;
    for _ in 0..20_000 {
        if let Err(error) = put(&engine) {
            refused = Some(error);
            break;
        }
    }
    assert!(
        matches!(
            engine.wait_for_compaction(),
            Err(BackgroundError::Failed(CompactionError::Retained { .. }))
        ),
        "{:?}",
        engine.background_status()
    );
    let Some(UpdateError::FlushRefused(limit)) = refused else {
        panic!("{refused:?}");
    };
    assert_eq!(limit.max_level0_flushes, levels::LEVEL0_STOP);
    assert!(limit.level0_flushes > limit.max_level0_flushes, "{limit}");
    assert!(limit.to_string().contains("level 0"), "{limit}");
    // The refusal names why the worker made no room.
    assert!(
        matches!(
            limit.background.as_deref(),
            Some(BackgroundError::Failed(CompactionError::Retained { .. }))
        ),
        "{limit:?}"
    );
    assert!(limit.to_string().contains("retain"), "{limit}");
    // A job the worker began before anything was retained may publish
    // after the refusal, draining part of level 0; the next is refused.
    assert!(engine.stats().level0_flushes <= levels::LEVEL0_STOP);

    // Nothing tells the engine the snapshot is gone: the next write that
    // needs room waits for a pass, which now succeeds, and writes go on.
    drop(snapshot);
    for _ in 0..2_000 {
        put(&engine).unwrap();
    }
    engine.wait_for_compaction().unwrap();
    assert!(engine.stats().level0_flushes < 4);
    engine.close().unwrap();
}

/// Overwrites of a fixed key set under a byte limit just above its live
/// size: flushes are refused when the limit is reached, and compaction
/// reclaims the overwritten versions so that writes and deletes continue,
/// without raising any limit.
#[test]
fn overwrites_and_deletes_continue_under_a_byte_limit() {
    let engine = Engine::new(options()).unwrap();
    for index in 0..2_000 {
        engine.put(&key(index), &value(index)).unwrap();
    }
    engine.flush().unwrap();
    let mut steps = Steps::default();
    settle(&engine, &mut steps);
    let live = engine.stats();
    let limit = (live.run_bytes + live.held_bytes) * 2;
    engine.set_run_limits(usize::MAX, limit).unwrap();
    let mut rng = Rng(1);
    let mut refusals = 0;
    for write in 0..30_000u64 {
        let key = key(rng.below(2_000));
        loop {
            let done = if write % 5 == 0 {
                engine.delete(&key)
            } else {
                engine.put(&key, &value(write))
            };
            match done {
                Ok(_) => break,
                Err(UpdateError::FlushRefused(_)) => {
                    refusals += 1;
                    assert!(refusals < 10_000, "no progress");
                    settle(&engine, &mut steps);
                    assert!(steps.refusal.is_none(), "{steps:?}");
                    engine.flush().ok();
                }
                Err(error) => panic!("{error}"),
            }
        }
    }
    let stats = engine.stats();
    assert!(stats.run_bytes + stats.held_bytes <= limit, "{stats:?}");
    assert!(stats.flushes > 20, "{stats:?}");
}

/// The run count passes the old ceiling of 64 with the default options
/// scaled to small runs: `max_runs` is a safety valve, not capacity.
#[test]
fn runs_grow_past_the_old_ceiling() {
    let engine = Engine::new(EngineOptions {
        background_compaction: Some(compaction()),
        ..options()
    })
    .unwrap();
    for index in 0..20_000u64 {
        let key = key(index * 104_729 % 20_000);
        loop {
            match engine.put(&key, &value(index)) {
                Ok(_) => break,
                Err(UpdateError::FlushRefused(_)) => {
                    engine.wait_for_compaction().unwrap();
                }
                Err(error) => panic!("{error}"),
            }
        }
    }
    engine.wait_for_compaction().unwrap();
    let stats = engine.stats();
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

/// Section 11 of the 5AT milestone: the flat policy that preceded levels
/// and leveled compaction, on the engine at the reduced scale of
/// `capacity_tests` and `leveling_tests` (64 KiB runs and memtables,
/// 15-byte keys, 100-byte text values, limits of four runs' length in and
/// out). The flat policy runs through the tests' copy of its selector,
/// once with its old 64-run limit and once without; leveled compaction
/// runs through [`Engine::compact`] with the defaults so scaled. Every
/// compaction is synchronous after each flush, so its time is the worker's
/// CPU. `PRODUCTION_WRITES` sets the writes (200,000 by default).
#[test]
#[ignore = "research: run with --release --ignored --nocapture"]
fn production_policies_compared() {
    use super::capacity_tests::{Workload, index};
    use std::time::Instant;

    const RUN: usize = 64 << 10;
    let writes: u64 = std::env::var("PRODUCTION_WRITES")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(200_000);
    let leveled = CompactionOptions {
        max_input_bytes: 4 * RUN,
        max_output_bytes: 4 * RUN,
        ..CompactionOptions::default()
    };
    let flat = CompactionOptions {
        max_inputs: 8,
        ..leveled
    };
    let options = |max_runs| EngineOptions {
        memtable_budget: RUN,
        target_run_len: RUN,
        max_runs,
        max_run_bytes: usize::MAX / 4,
        background_compaction: None,
        ..EngineOptions::default()
    };
    let key = |index: u64| format!("user/{index:010}").into_bytes();

    #[derive(Clone, Copy, Debug)]
    enum History {
        Engine(Workload),
        /// Half inserts of new keys, 30% overwrites, 20% deletes.
        Mixed,
    }
    #[derive(Clone, Copy, Debug)]
    enum Policy {
        Flat(usize),
        Leveled,
    }
    let histories = [
        ("ascending", History::Engine(Workload::Ascending)),
        ("uniform insert", History::Engine(Workload::UniformInsert)),
        (
            "uniform overwrite",
            History::Engine(Workload::UniformUpdate),
        ),
        ("hot range", History::Engine(Workload::Hot)),
        ("mixed with deletes", History::Mixed),
    ];
    println!(
        "PRODUCTION | workload | policy | writes completed | WA (flushed+compacted)/flushed \
         | stored written / logical | live stored MiB | space amp. | runs | levels | L0 flushes max \
         | lookup groups | merges / moves / splits / neighbours | compacted in / out MiB \
         | refusals (compaction / flush) | compaction s | point µs | prefix µs \
         | peak published MiB | RSS MiB |"
    );
    for (label, history) in histories {
        for policy in [Policy::Flat(64), Policy::Flat(1 << 20), Policy::Leveled] {
            let engine = Engine::new(options(match policy {
                Policy::Flat(max_runs) => max_runs,
                Policy::Leveled => DEFAULT_MAX_RUNS,
            }))
            .unwrap();
            if let Policy::Flat(_) = policy {
                engine.set_level0_stop(usize::MAX);
            }
            let mut workspace = MergeWorkspace::new();
            let (mut compacted_in, mut compacted_out) = (0u64, 0u64);
            let mut kinds = [0u64; 4];
            let mut refused_compactions = 0u64;
            let mut compaction_time = 0.0;
            let mut peak = 0usize;
            let mut compact = |engine: &Engine| {
                let start = Instant::now();
                loop {
                    let report = match policy {
                        Policy::Flat(_) => match engine.selection(flat) {
                            Ok(Some(picks)) => engine.compact_picked(flat, picks, &mut workspace),
                            Ok(None) => break,
                            Err(error) => Err(error),
                        },
                        Policy::Leveled => match engine.compact(leveled, &mut workspace) {
                            Ok(Some(report)) => Ok(report),
                            Ok(None) => break,
                            Err(error) => Err(error),
                        },
                    };
                    match report {
                        Ok(report) => {
                            compacted_in += report.input_bytes as u64;
                            compacted_out += report.output_bytes as u64;
                            kinds[match report.kind {
                                CompactionKind::Merge => 0,
                                CompactionKind::Move => 1,
                                CompactionKind::Split => 2,
                                _ => 3,
                            }] += 1;
                        }
                        Err(_) => {
                            refused_compactions += 1;
                            break;
                        }
                    }
                }
                compaction_time += start.elapsed().as_secs_f64();
                let stats = engine.stats();
                peak = peak.max(stats.run_bytes + stats.held_bytes);
            };
            let keys = match history {
                History::Engine(Workload::UniformUpdate | Workload::Hot) => writes / 4,
                _ => writes,
            };
            let mut rng = Rng(11);
            let mut live: Vec<u64> = Vec::new();
            let (mut logical, mut flushes, mut completed) = (0u64, 0u64, writes);
            let mut refused_flushes = 0u64;
            let mut l0_max = 0;
            for write in 0..writes {
                let (index, delete) = match history {
                    History::Engine(workload) => (index(workload, write, keys, &mut rng), false),
                    History::Mixed => {
                        let roll = rng.below(10);
                        if roll < 5 || live.is_empty() {
                            let index = rng.below(10_000_000_000);
                            live.push(index);
                            (index, false)
                        } else if roll < 8 {
                            (live[rng.below(live.len() as u64) as usize], false)
                        } else {
                            let at = rng.below(live.len() as u64) as usize;
                            (live.swap_remove(at), true)
                        }
                    }
                };
                let key = key(index);
                let value = value(write);
                let done = if delete {
                    logical += key.len() as u64;
                    engine.delete(&key)
                } else {
                    logical += (key.len() + value.len()) as u64;
                    engine.put(&key, &value)
                };
                if done.is_err() {
                    // The old ceiling: compaction cannot make room.
                    compact(&engine);
                    refused_flushes += 1;
                    completed = write;
                    break;
                }
                let stats = engine.stats();
                l0_max = l0_max.max(stats.level0_flushes);
                if stats.flushes != flushes {
                    flushes = stats.flushes;
                    compact(&engine);
                }
            }
            if completed == writes {
                engine.flush().unwrap();
                compact(&engine);
            }
            let stats = engine.stats();
            // Live bytes: the same records written once, in order.
            let live_stored = {
                let fresh = Engine::new(options(DEFAULT_MAX_RUNS)).unwrap();
                let snapshot = engine.snapshot().unwrap();
                let mut read = EngineWorkspace::new();
                let mut scan = snapshot.scan(&mut read);
                while let Some(record) = scan.next_record().unwrap() {
                    fresh.put(record.key, record.value).unwrap();
                }
                fresh.flush().unwrap();
                let stats = fresh.stats();
                stats.run_bytes + stats.held_bytes
            };
            // Reads: point lookups of keys written, and prefixes of ten keys.
            let mut read = EngineWorkspace::new();
            let probes = 20_000u64;
            let start = Instant::now();
            for probe in 0..probes {
                // Uniform over the keys written; the mixed history's are
                // drawn from those still live.
                let index = match history {
                    History::Mixed if !live.is_empty() => {
                        live[rng.below(live.len() as u64) as usize]
                    }
                    History::Engine(Workload::Ascending | Workload::UniformInsert) => index(
                        Workload::UniformInsert,
                        rng.below(completed.max(1)),
                        keys,
                        &mut rng,
                    ),
                    _ => rng.below(keys),
                };
                let _ = probe;
                std::hint::black_box(engine.get(&key(index), &mut read).unwrap().is_some());
            }
            let point = start.elapsed().as_secs_f64() * 1e6 / probes as f64;
            let prefixes = 2_000u64;
            let start = Instant::now();
            for _ in 0..prefixes {
                let index = match history {
                    History::Mixed if !live.is_empty() => {
                        live[rng.below(live.len() as u64) as usize]
                    }
                    _ => rng.below(keys),
                };
                let prefix = &key(index)[..14];
                let guard = engine.read();
                let mut scan = guard.scan_prefix(prefix, &mut read);
                while let Some(record) = scan.next_record().unwrap() {
                    std::hint::black_box(record.key);
                }
            }
            let prefix = start.elapsed().as_secs_f64() * 1e6 / prefixes as f64;
            let rss = std::fs::read_to_string("/proc/self/status")
                .ok()
                .and_then(|status| {
                    status
                        .lines()
                        .find(|line| line.starts_with("VmRSS:"))
                        .and_then(|line| line.split_whitespace().nth(1)?.parse::<f64>().ok())
                })
                .map_or(f64::NAN, |kib| kib / 1024.0);
            let mib = |bytes: f64| bytes / (1 << 20) as f64;
            let flushed = stats.flushed_bytes as f64;
            println!(
                "PRODUCTION | {label} | {policy:?} | {completed} | {:.2} | {:.3} | {:.1} | {:.2} | {} | {} | {l0_max} | {} | {} / {} / {} / {} | {:.0} / {:.0} | {refused_compactions} / {refused_flushes} | {compaction_time:.1} | {point:.2} | {prefix:.1} | {:.1} | {rss:.0} |",
                (flushed + compacted_out as f64) / flushed.max(1.0),
                (flushed + compacted_out as f64) / logical.max(1) as f64,
                mib(live_stored as f64),
                (stats.run_bytes + stats.held_bytes) as f64 / live_stored.max(1) as f64,
                stats.runs,
                stats.levels,
                stats.max_overlap,
                kinds[0],
                kinds[1],
                kinds[2],
                kinds[3],
                mib(compacted_in as f64),
                mib(compacted_out as f64),
                mib(peak as f64),
            );
            engine.close().unwrap();
        }
    }
}

// 5AU: flush splitting and level-0 backpressure.

/// The same writes into an engine that splits flushes at level 1's
/// boundaries and one that does not: reads agree with a model in both, and
/// each flush is cut into at most eight runs per run of the unsplit flush.
#[test]
fn flushes_split_at_level_1_boundaries_and_stay_bounded() {
    let engines = [true, false].map(|split| {
        let engine = Engine::new(options()).unwrap();
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
                engine.put(&key, &value).unwrap();
            }
            model.insert(key, value);
        }
        let flushed = engines
            .each_ref()
            .map(|engine| engine.flush().unwrap().runs);
        assert_eq!(engines[0].stats().flushes, round + 1);
        assert!(flushed[0] <= 8 * flushed[1], "{flushed:?}");
        for (at, engine) in engines.iter().enumerate() {
            runs[at] += flushed[at];
            settle(engine, &mut steps[at]);
            assert!(steps[at].refusal.is_none(), "{:?}", steps[at]);
        }
    }
    assert!(runs[0] > runs[1], "splitting cut some flushes: {runs:?}");
    for engine in &engines {
        assert_eq!(scan(&engine.snapshot().unwrap()), model);
        level_ids(engine);
    }
}

/// A flush prepared before compaction changes level 1 publishes with the
/// cuts it took: they only group its records, so reads and levels stay
/// right, and the next compactions take the pieces as any level-0 runs.
#[test]
fn a_flush_split_at_stale_boundaries_publishes_correctly() {
    let engine = Engine::new(options()).unwrap();
    let mut model = BTreeMap::new();
    let eager = CompactionOptions {
        max_overlap: 1,
        ..compaction()
    };
    let mut steps = Steps::default();
    let mut put = |engine: &Engine, index: u64, seed: u64| {
        engine.put(&key(index), &value(seed)).unwrap();
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
    assert!(!job.cuts.at.is_empty());
    // ... then compaction rewrites level 1 under it.
    let mut workspace = MergeWorkspace::new();
    let report = engine.compact(eager, &mut workspace).unwrap().unwrap();
    assert_eq!(report.kind, CompactionKind::Merge);
    assert_ne!(level_ids(&engine)[1..], before[1..]);
    job.build().publish().unwrap();
    level_ids(&engine);
    assert_eq!(scan(&engine.snapshot().unwrap()), model);
    settle_with(&engine, eager, &mut steps);
    assert!(steps.refusal.is_none(), "{steps:?}");
    assert_eq!(scan(&engine.snapshot().unwrap()), model);
}

/// Writes with a worker held back by the test: once level 0 reaches its
/// stop, the writer's flush waits rather than failing, without the state
/// lock, and completes as soon as the worker can compact.
#[test]
fn a_flush_at_the_level_0_stop_waits_for_the_worker() {
    let engine = Engine::new(EngineOptions {
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
                    engine.put(&key, &value(flush)).unwrap();
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
        assert_eq!(engine.stats().level0_flushes, 2);
        assert!(!writer.is_finished());
        drop(held);
        let model = writer.join().unwrap();
        assert_eq!(scan(&engine.snapshot().unwrap()), model);
    });
    engine.wait_for_compaction().unwrap();
    assert!(engine.stats().level0_flushes < 2);
    engine.close().unwrap();
}

/// A worker that panics while a writer waits for level 0 wakes the writer,
/// whose flush is refused at the stop instead of waiting for good; writes
/// needing room fail at once from then on, until manual compaction drains
/// level 0.
#[test]
fn a_worker_panic_ends_a_wait_for_level_0() {
    let engine = Engine::new(EngineOptions {
        background_compaction: Some(compaction()),
        ..options()
    })
    .unwrap();
    engine.set_level0_stop(2);
    let held = engine.core.compacting.lock().unwrap();
    std::thread::scope(|scope| {
        let writer = scope.spawn(|| {
            for flush in 0..3u64 {
                for index in 0..40 {
                    engine.put(&key(index * 7 + flush), &value(flush)).unwrap();
                }
                if let Err(refused) = engine.flush() {
                    return Some(refused);
                }
            }
            None
        });
        while engine.core.level0_waits.load(Ordering::SeqCst) == 0 && !writer.is_finished() {
            std::thread::yield_now();
        }
        assert!(!writer.is_finished());
        engine.core.panic_step.store(true, Ordering::SeqCst);
        drop(held);
        let refused = writer.join().unwrap().expect("the flush was refused");
        assert_eq!((refused.level0_flushes, refused.max_level0_flushes), (3, 2));
        assert_eq!(
            refused.background.as_deref(),
            Some(&BackgroundError::Panicked(
                "injected compaction failure".to_owned()
            ))
        );
    });
    let status = engine.background_status().unwrap();
    assert!(!status.running);
    assert_eq!(status.panic.as_deref(), Some("injected compaction failure"));
    assert!(engine.flush().is_err());
    let mut steps = Steps::default();
    settle(&engine, &mut steps);
    engine.flush().unwrap();
    assert_eq!(get(&engine, &key(7 * 39 + 2)), Some(value(2)));
}

/// A merge's output, stored bytes and filters, can exceed its inputs' by
/// some hundreds of bytes of run overhead, and the output limit counts it as
/// written. Selection keeps headroom for that: a merge whose inputs are
/// exactly at the byte limits is not admitted, so it cannot fail at the
/// output limit, where every later selection would choose it again and a
/// level at its stop would refuse flushes for good.
#[test]
fn a_merge_at_the_byte_limits_is_not_admitted_to_overflow() {
    let history = |engine: &Engine, flush: u64| {
        let mut rng = Rng(flush + 1);
        for _ in 0..80 {
            let index = rng.below(5_000);
            engine.put(&key(index), &value(index ^ flush)).unwrap();
        }
        engine.flush().unwrap();
    };
    let drain = |engine: &Engine| {
        while engine
            .compact(compaction(), &mut MergeWorkspace::new())
            .unwrap()
            .is_some()
        {}
    };
    // The first merge, compacting after every flush, whose output exceeds
    // its inputs and whose inputs a valid limit can equal.
    let reference = Engine::new(options()).unwrap();
    let mut found = None;
    'search: for flush in 0..400 {
        history(&reference, flush);
        for step in 0.. {
            let Some(report) = reference
                .compact(compaction(), &mut MergeWorkspace::new())
                .unwrap()
            else {
                break;
            };
            let input = report.input_bytes + report.input_held_bytes;
            let output = report.output_bytes + report.output_held_bytes;
            if report.kind == CompactionKind::Merge && output > input && input >= 2 * TARGET {
                found = Some((flush, step, input));
                break 'search;
            }
        }
    }
    let (flush, step, input) = found.expect("some merge writes more than it reads");
    let engine = Engine::new(options()).unwrap();
    for earlier in 0..flush {
        history(&engine, earlier);
        drain(&engine);
    }
    history(&engine, flush);
    for _ in 0..step {
        engine
            .compact(compaction(), &mut MergeWorkspace::new())
            .unwrap();
    }
    let exact = CompactionOptions {
        max_input_bytes: input,
        max_output_bytes: input,
        ..compaction()
    };
    let results: Vec<_> = (0..3)
        .map(|_| engine.compact(exact, &mut MergeWorkspace::new()))
        .collect();
    assert!(
        !results
            .iter()
            .any(|result| matches!(result, Err(CompactionError::OutputLimit { .. }))),
        "a merge of {input} bytes was admitted at that limit: {results:?}"
    );
}

/// Runs checkpointed with Zstandard and recovered uncompressed: a merge of
/// them writes several times its input, far past the output headroom, so a
/// job admitted by its input fails at the output limit. Selection must not
/// choose that job again and again: after a failure at the output limit,
/// jobs from that source are formed smaller (split if need be) until they
/// fit or are refused before reading anything, and compaction settles with
/// every record intact.
#[test]
fn output_growth_past_the_headroom_does_not_repeat_a_job() {
    let dir = std::env::temp_dir().join(format!("tessera-growth-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let mut model = BTreeMap::new();
    {
        let engine = Engine::new(EngineOptions {
            compression: Compression::Zstd,
            ..options()
        })
        .unwrap();
        for index in 0..6_000u64 {
            let key = key(index * 7 % 6_000);
            engine.put(&key, &value(index)).unwrap();
            model.insert(key, value(index));
        }
        settle(&engine, &mut Steps::default());
        engine
            .checkpoint(&dir, CheckpointOptions::default())
            .unwrap();
        engine.close().unwrap();
    }
    let engine = Engine::recover(
        &dir,
        EngineOptions {
            compression: Compression::Uncompressed,
            ..options()
        },
    )
    .unwrap();
    let _ = std::fs::remove_dir_all(&dir);
    let mut rng = Rng(5);
    for write in 0..3_000u64 {
        let key = key(rng.below(6_000));
        engine.put(&key, &value(write + 90_000)).unwrap();
        model.insert(key, value(write + 90_000));
    }
    engine.flush().unwrap();
    let shape = engine.core.read_state().levels.shape();
    // The smallest limits accepted: two full runs.
    let limits = CompactionOptions {
        max_input_bytes: 2 * TARGET,
        max_output_bytes: 2 * TARGET,
        ..compaction()
    };
    // Compact until nothing is due or a refusal repeats unchanged (it is
    // then decided before reading anything, and stands).
    let mut workspace = MergeWorkspace::new();
    let (mut steps, mut last) = (0usize, None);
    for _ in 0..10_000 {
        match engine.compact(limits, &mut workspace) {
            Ok(Some(_)) => {
                steps += 1;
                last = None;
            }
            Ok(None) => {
                last = None;
                break;
            }
            Err(error) if last.as_ref() == Some(&error) => break,
            Err(error) => last = Some(error),
        }
    }
    let overflows = engine.core.overflows.load(Ordering::SeqCst);
    assert!(overflows > 0, "the output outgrew the headroom: {shape:?}");
    let runs = engine.stats().runs;
    assert!(
        overflows <= 4 * runs,
        "{overflows} merges failed at the output limit, {steps} steps, {runs} runs"
    );
    assert!(
        matches!(last, None | Some(CompactionError::OutputLimit { .. })),
        "{last:?}"
    );
    level_ids(&engine);
    assert_eq!(scan(&engine.snapshot().unwrap()), model);
}

/// The allowance after a failure at the output limit never falls below
/// the source with the largest run below it, which is the smallest job
/// that makes progress on all of the source. Halved below that,
/// after a few large jobs of a recovered level outgrew jobs of three runs,
/// it refused every job of the source, including one of two runs that fits,
/// and compaction never drained (found by the alpha validation after 5AU:
/// `alpha_tests::alpha_output_growth_after_codec_switch` with a level-0
/// backlog). Now every source compacts, and compaction drains.
#[test]
fn an_outgrown_source_keeps_a_job_that_can_progress() {
    let dir = std::env::temp_dir().join(format!("tessera-floor-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    // Runs of 32 KiB, where a run's overhead beside its records is small
    // enough that a job of two uncompressed runs fits jobs of three.
    const RUN: usize = 32 << 10;
    let sized = |compression| EngineOptions {
        memtable_budget: 4 * RUN,
        target_run_len: RUN,
        compression,
        ..options()
    };
    let mut model = BTreeMap::new();
    {
        let engine = Engine::new(sized(Compression::Zstd)).unwrap();
        for index in 0..40_000u64 {
            let key = key(index * 7 % 40_000);
            engine.put(&key, &value(index)).unwrap();
            model.insert(key, value(index));
        }
        settle(&engine, &mut Steps::default());
        engine
            .checkpoint(&dir, CheckpointOptions::default())
            .unwrap();
        engine.close().unwrap();
    }
    let engine = Engine::recover(&dir, sized(Compression::Uncompressed)).unwrap();
    let _ = std::fs::remove_dir_all(&dir);
    let mut rng = Rng(9);
    for write in 0..20_000u64 {
        let key = key(rng.below(40_000));
        engine.put(&key, &value(write + 90_000)).unwrap();
        model.insert(key, value(write + 90_000));
    }
    engine.flush().unwrap();
    let limits = CompactionOptions {
        max_input_bytes: 3 * RUN,
        max_output_bytes: 3 * RUN,
        ..compaction()
    };
    let mut workspace = MergeWorkspace::new();
    let (mut steps, mut last) = (0usize, None);
    let drained = loop {
        match engine.compact(limits, &mut workspace) {
            Ok(Some(_)) => steps += 1,
            Ok(None) => break true,
            Err(error) if last.as_ref() == Some(&error) => break false,
            Err(error) => last = Some(error),
        }
        assert!(steps < 100_000);
    };
    let overflows = engine.core.overflows.load(Ordering::SeqCst);
    assert!(overflows > 0, "the output outgrew the headroom");
    assert!(
        drained,
        "{overflows} overflows, {steps} steps, ended on {last:?}"
    );
    let runs = engine.stats().runs;
    assert!(overflows <= 4 * runs, "{overflows} overflows, {runs} runs");
    level_ids(&engine);
    assert_eq!(scan(&engine.snapshot().unwrap()), model);
}

/// A valid configuration in which no level-0 job fits: each flush at the
/// stop waits for one pass, which is refused, and is then refused naming
/// the stop; the worker runs one pass per refused flush, not in a loop.
#[test]
fn an_impossible_job_refuses_flushes_after_one_pass_each() {
    let minimum = CompactionOptions {
        max_workspace_bytes: 0,
        ..compaction()
    };
    let Err(CompactionError::Config(config)) = Engine::new(options())
        .unwrap()
        .compact(minimum, &mut MergeWorkspace::new())
    else {
        panic!("a workspace limit of 0 is refused");
    };
    let restrictive = CompactionOptions {
        max_workspace_bytes: config.min as usize,
        ..minimum
    };
    let engine = Engine::new(EngineOptions {
        memtable_budget: 256 << 10,
        background_compaction: Some(restrictive),
        ..options()
    })
    .unwrap();
    // Every flush overlaps the others and holds a record of three blocks,
    // so every level-0 job needs more workspace than the limit allows.
    let large = vec![b'x'; 48 << 10];
    let mut refused = None;
    for flush in 0..40u64 {
        engine.put(b"large", &large).unwrap();
        engine.put(&key(flush), &value(flush)).unwrap();
        if let Err(error) = engine.flush() {
            refused = Some(error);
            break;
        }
    }
    let refused = refused.expect("level 0 filled");
    assert_eq!(refused.max_level0_flushes, levels::LEVEL0_STOP);
    assert!(
        matches!(
            refused.background.as_deref(),
            Some(BackgroundError::Failed(CompactionError::JobLimit { .. }))
        ),
        "{refused:?}"
    );
    let status = engine.background_status().unwrap();
    assert!(
        matches!(status.last_failure, Some(CompactionError::JobLimit { .. })),
        "{status:?}"
    );
    let passes = status.passes;
    for _ in 0..5 {
        assert!(engine.flush().is_err());
    }
    let status = engine.background_status().unwrap();
    assert!(status.passes <= passes + 5, "{status:?}");
    assert!(!status.busy);
    engine.close().unwrap();
}

/// 5AU's paired measurement: the same histories with flushes left whole
/// (as 5AT built them) and split at level 1's boundaries, compacted after
/// every flush, and split with the background worker, whose waits at the
/// level-0 stop are timed; with text and with incompressible values. Run
/// as [`production_policies_compared`]; `SPLIT_WRITES` sets the writes
/// (200,000 by default).
#[test]
#[ignore = "research: run with --release --ignored --nocapture"]
fn split_flushes_compared() {
    use super::capacity_tests::{Workload, index};
    use std::time::Instant;

    const RUN: usize = 64 << 10;
    let writes: u64 = std::env::var("SPLIT_WRITES")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(200_000);
    let leveled = CompactionOptions {
        max_input_bytes: 4 * RUN,
        max_output_bytes: 4 * RUN,
        ..CompactionOptions::default()
    };
    let options = |background: bool| EngineOptions {
        memtable_budget: RUN,
        target_run_len: RUN,
        max_run_bytes: usize::MAX / 4,
        background_compaction: background.then_some(leveled),
        ..EngineOptions::default()
    };
    let key = |index: u64| format!("user/{index:010}").into_bytes();
    let noise = |seed: u64| {
        let mut state = seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1;
        (0..100)
            .map(|_| {
                state ^= state << 13;
                state ^= state >> 7;
                state ^= state << 17;
                state as u8
            })
            .collect::<Vec<u8>>()
    };
    #[derive(Clone, Copy, Debug, PartialEq)]
    enum Policy {
        Unsplit,
        Split,
        SplitBackground,
    }
    let histories = [
        ("ascending", Some(Workload::Ascending)),
        ("uniform insert", Some(Workload::UniformInsert)),
        ("uniform overwrite", Some(Workload::UniformUpdate)),
        ("hot range", Some(Workload::Hot)),
        ("mixed with deletes", None),
    ];
    println!(
        "SPLIT | workload | values | policy | WA | stored written / logical | space amp. \
         | runs | levels (runs per level) | flush runs per flush (max) | merges / moves / splits / neighbours \
         | merge inputs mean (max) | largest merge input KiB | largest output KiB | L0 flushes max \
         | waits (s) | foreground s | compaction s | point µs | short prefix µs | long prefix µs \
         | decoder contexts KiB | refusals (compactions / writes) |"
    );
    for (label, workload) in histories {
        for incompressible in [false, true] {
            for policy in [Policy::Unsplit, Policy::Split, Policy::SplitBackground] {
                let values = if incompressible {
                    "incompressible"
                } else {
                    "text"
                };
                if let Ok(filter) = std::env::var("SPLIT_FILTER")
                    && !format!("{label}|{values}|{policy:?}").contains(&filter)
                {
                    continue;
                }
                let background = policy == Policy::SplitBackground;
                let engine = Engine::new(options(background)).unwrap();
                engine.set_split_flushes(policy != Policy::Unsplit);
                *engine.core.step_log.lock().unwrap() = Some(Vec::new());
                let mut workspace = MergeWorkspace::new();
                let mut kinds = [0u64; 4];
                let (mut compacted_out, mut merge_inputs, mut merges) = (0u64, 0usize, 0usize);
                let (mut max_inputs, mut max_input, mut max_output) = (0usize, 0usize, 0usize);
                let (mut compaction_time, mut refusals) = (0.0, 0u64);
                let mut compact = |engine: &Engine| {
                    let start = Instant::now();
                    loop {
                        match engine.compact(leveled, &mut workspace) {
                            Ok(Some(report)) => {
                                compacted_out += report.output_bytes as u64;
                                let kind = match report.kind {
                                    CompactionKind::Merge => 0,
                                    CompactionKind::Move => 1,
                                    CompactionKind::Split => 2,
                                    _ => 3,
                                };
                                kinds[kind] += 1;
                                if kind == 0 {
                                    merges += 1;
                                    merge_inputs += report.inputs;
                                    max_inputs = max_inputs.max(report.inputs);
                                    max_input =
                                        max_input.max(report.input_bytes + report.input_held_bytes);
                                }
                                max_output =
                                    max_output.max(report.output_bytes + report.output_held_bytes);
                            }
                            Ok(None) => break,
                            Err(_) => {
                                refusals += 1;
                                break;
                            }
                        }
                    }
                    compaction_time += start.elapsed().as_secs_f64();
                };
                let keys = match workload {
                    Some(Workload::UniformUpdate | Workload::Hot) => writes / 4,
                    _ => writes,
                };
                let mut rng = Rng(11);
                let mut live: Vec<u64> = Vec::new();
                let (mut logical, mut flushes, mut flush_runs) = (0u64, 0u64, 0usize);
                let (mut l0_max, mut max_flush_runs) = (0, 0);
                let mut foreground = 0.0;
                let mut refused_writes = 0u64;
                for write in 0..writes {
                    let (index, delete) = match workload {
                        Some(workload) => (index(workload, write, keys, &mut rng), false),
                        None => {
                            let roll = rng.below(10);
                            if roll < 5 || live.is_empty() {
                                let index = rng.below(10_000_000_000);
                                live.push(index);
                                (index, false)
                            } else if roll < 8 {
                                (live[rng.below(live.len() as u64) as usize], false)
                            } else {
                                let at = rng.below(live.len() as u64) as usize;
                                (live.swap_remove(at), true)
                            }
                        }
                    };
                    let key = key(index);
                    let value = if incompressible {
                        noise(write)
                    } else {
                        value(write)
                    };
                    let start = Instant::now();
                    let runs_before = engine.stats().runs;
                    let done = if delete {
                        logical += key.len() as u64;
                        engine.delete(&key)
                    } else {
                        logical += (key.len() + value.len()) as u64;
                        engine.put(&key, &value)
                    };
                    foreground += start.elapsed().as_secs_f64();
                    if let Err(error) = done {
                        if refused_writes == 0 {
                            eprintln!(
                                "SPLIT refused | {label} | {policy:?} | {error} | {:?}",
                                engine.background_status()
                            );
                        }
                        refused_writes += 1;
                        if background {
                            let _ = engine.wait_for_compaction();
                        } else {
                            compact(&engine);
                        }
                        continue;
                    }
                    let stats = engine.stats();
                    l0_max = l0_max.max(stats.level0_flushes);
                    if stats.flushes != flushes {
                        flushes = stats.flushes;
                        if !background {
                            // Before compaction, the flush's runs are the
                            // growth since the write began.
                            let added = stats.runs + 1 - runs_before.min(stats.runs + 1);
                            flush_runs += added;
                            max_flush_runs = max_flush_runs.max(added);
                            compact(&engine);
                        }
                    }
                }
                // The operating endpoint: what the writes left, before the
                // final flush and drain.
                let operating = {
                    let stats = engine.stats();
                    let log = engine.core.step_log.lock().unwrap();
                    let out: usize = log
                        .as_ref()
                        .unwrap()
                        .iter()
                        .map(|step| step.output_bytes)
                        .sum();
                    let shape = engine.core.read_state().levels.shape();
                    (
                        (stats.flushed_bytes as f64 + out as f64)
                            / (stats.flushed_bytes as f64).max(1.0),
                        stats.level0_flushes,
                        shape,
                    )
                };
                let start = Instant::now();
                for attempt in 0.. {
                    match engine.flush() {
                        Ok(_) => break,
                        Err(error) => {
                            assert!(attempt < 3, "{error}");
                            eprintln!(
                                "SPLIT final flush refused | {label} | {policy:?} | {error} | {:?}",
                                engine.background_status()
                            );
                            refused_writes += 1;
                            let _ = engine.wait_for_compaction();
                        }
                    }
                }
                foreground += start.elapsed().as_secs_f64();
                if background {
                    let start = Instant::now();
                    engine.wait_for_compaction().unwrap();
                    compaction_time += start.elapsed().as_secs_f64();
                    let status = engine.background_status().unwrap();
                    compacted_out = status.output_bytes;
                } else {
                    compact(&engine);
                }
                let stats = engine.stats();
                let live_stored = {
                    let fresh = Engine::new(options(false)).unwrap();
                    let snapshot = engine.snapshot().unwrap();
                    let mut read = EngineWorkspace::new();
                    let mut scan = snapshot.scan(&mut read);
                    while let Some(record) = scan.next_record().unwrap() {
                        fresh.put(record.key, record.value).unwrap();
                    }
                    fresh.flush().unwrap();
                    let stats = fresh.stats();
                    stats.run_bytes + stats.held_bytes
                };
                let mut read = EngineWorkspace::new();
                let probe = |rng: &mut Rng| match workload {
                    None if !live.is_empty() => live[rng.below(live.len() as u64) as usize],
                    Some(Workload::Ascending | Workload::UniformInsert) => {
                        index(Workload::UniformInsert, rng.below(writes), keys, rng)
                    }
                    _ => rng.below(keys),
                };
                let probes = 20_000u64;
                let start = Instant::now();
                for _ in 0..probes {
                    let index = probe(&mut rng);
                    std::hint::black_box(engine.get(&key(index), &mut read).unwrap().is_some());
                }
                let point = start.elapsed().as_secs_f64() * 1e6 / probes as f64;
                // Prefixes of 14 bytes (ten indices) and 11 (ten thousand).
                let prefix_time =
                    |length: usize, count: u64, rng: &mut Rng, read: &mut EngineWorkspace| {
                        let start = Instant::now();
                        for _ in 0..count {
                            let index = probe(rng);
                            let prefix = &key(index)[..length];
                            let guard = engine.read();
                            let mut scan = guard.scan_prefix(prefix, read);
                            while let Some(record) = scan.next_record().unwrap() {
                                std::hint::black_box(record.key);
                            }
                        }
                        start.elapsed().as_secs_f64() * 1e6 / count as f64
                    };
                let loaded = read.blocks_loaded();
                let short = prefix_time(14, 2_000, &mut rng, &mut read);
                let short_blocks = (read.blocks_loaded() - loaded) as f64 / 2_000.0;
                let loaded = read.blocks_loaded();
                let long = prefix_time(11, 200, &mut rng, &mut read);
                let long_blocks = (read.blocks_loaded() - loaded) as f64 / 200.0;
                // Short scans at the next four flush phases, so that the
                // point of level 0's compaction cycle averages out:
                // (blocks per scan, µs per scan, level-0 flushes) each.
                let mut phases = Vec::new();
                if !background && std::env::var("SPLIT_PHASES").is_ok() {
                    for phase in 0..4u64 {
                        let flushes = engine.stats().flushes;
                        let mut extra = 0u64;
                        while engine.stats().flushes == flushes {
                            let index = rng.below(10_000_000_000);
                            let key = key(index);
                            let value = if incompressible {
                                noise(extra)
                            } else {
                                value(extra)
                            };
                            engine.put(&key, &value).unwrap();
                            extra += 1;
                        }
                        let mut drain = MergeWorkspace::new();
                        while let Ok(Some(_)) = engine.compact(leveled, &mut drain) {}
                        let loaded = read.blocks_loaded();
                        let time = prefix_time(14, 1_000, &mut rng, &mut read);
                        let blocks = (read.blocks_loaded() - loaded) as f64 / 1_000.0;
                        phases.push(format!(
                            "{phase}: {blocks:.2} blocks {time:.1} µs L0 {}",
                            engine.stats().level0_flushes
                        ));
                    }
                    println!("PHASES | {label} | {policy:?} | {} |", phases.join(" | "));
                }
                {
                    let state = engine.core.read_state();
                    let level0: Vec<usize> = state.levels.runs()[0]
                        .iter()
                        .map(|run| run.run.as_bytes().len() >> 10)
                        .collect();
                    println!(
                        "SCAN | {label} | {} | {policy:?} | groups {} | level-0 runs {} KiB {level0:?} | level-0 flushes {} | blocks per short scan {short_blocks:.2}, long {long_blocks:.1} | short {short:.1} µs |",
                        if incompressible {
                            "incompressible"
                        } else {
                            "text"
                        },
                        stats.max_overlap,
                        level0.len(),
                        stats.level0_flushes,
                    );
                }
                let levels: Vec<String> = engine
                    .core
                    .read_state()
                    .levels
                    .shape()
                    .iter()
                    .map(|(runs, _)| runs.to_string())
                    .collect();
                let waits = engine.core.level0_waits.load(Ordering::SeqCst);
                let waited = engine.core.level0_wait_nanos.load(Ordering::SeqCst) as f64 / 1e9;
                let flushed = stats.flushed_bytes as f64;
                let per_flush = if background {
                    "-".to_string()
                } else {
                    format!(
                        "{:.2} ({max_flush_runs})",
                        flush_runs as f64 / flushes.max(1) as f64
                    )
                };
                println!(
                    "SPLIT | {label} | {} | {policy:?} | {:.2} | {:.3} | {:.2} | {} | {} ({}) | {per_flush} | {} / {} / {} / {} | {:.1} ({max_inputs}) | {} | {} | {l0_max} | {waits} ({waited:.2}) | {foreground:.2} | {compaction_time:.2} | {point:.2} | {short:.1} | {long:.0} | {} | {refusals} / {refused_writes} |",
                    if incompressible {
                        "incompressible"
                    } else {
                        "text"
                    },
                    (flushed + compacted_out as f64) / flushed.max(1.0),
                    (flushed + compacted_out as f64) / logical.max(1) as f64,
                    (stats.run_bytes + stats.held_bytes) as f64 / live_stored.max(1) as f64,
                    stats.runs,
                    stats.levels,
                    levels.join("/"),
                    kinds[0],
                    kinds[1],
                    kinds[2],
                    kinds[3],
                    merge_inputs as f64 / merges.max(1) as f64,
                    max_input >> 10,
                    max_output >> 10,
                    workspace.context_bytes() >> 10,
                );
                let log = engine.core.step_log.lock().unwrap().take().unwrap();
                print_steps(
                    label,
                    values,
                    &format!("{policy:?}"),
                    &log,
                    &operating,
                    &engine,
                );
                engine.close().unwrap();
            }
        }
    }
}

/// Prints [`split_flushes_compared`]'s breakdown of one history's steps.
fn print_steps(
    label: &str,
    values: &str,
    policy: &str,
    log: &[super::StepRecord],
    operating: &(f64, usize, Vec<(usize, usize)>),
    engine: &Engine,
) {
    let kib = |bytes: usize| bytes >> 10;
    let shape = |shape: &[(usize, usize)]| {
        shape
            .iter()
            .map(|(runs, bytes)| format!("{runs}:{}", kib(*bytes)))
            .collect::<Vec<_>>()
            .join("/")
    };
    let mut groups: std::collections::BTreeMap<String, (usize, usize, usize)> = Default::default();
    for step in log.iter().filter(|step| step.kind.is_some()) {
        let entry = groups
            .entry(format!("{:?}{}", step.kind.unwrap(), step.level))
            .or_default();
        entry.0 += 1;
        entry.1 += step.input_bytes;
        entry.2 += step.output_bytes;
    }
    let jobs: Vec<String> = groups
        .iter()
        .map(|(name, (count, input, output))| {
            format!("{name} {count}x {}>{}", kib(*input), kib(*output))
        })
        .collect();
    let merges0: Vec<_> = log
        .iter()
        .filter(|step| step.kind == Some(CompactionKind::Merge) && step.level == 0)
        .collect();
    let n = merges0.len().max(1) as f64;
    let mean = |f: &dyn Fn(&super::StepRecord) -> usize| {
        merges0.iter().map(|step| f(step)).sum::<usize>() as f64 / n
    };
    let level0_bytes: usize = merges0
        .iter()
        .map(|step| step.input_bytes - step.lower_bytes)
        .sum();
    let lower_bytes: usize = merges0.iter().map(|step| step.lower_bytes).sum();
    let errors: Vec<_> = log
        .iter()
        .filter(|step| step.error.is_some() && step.level != usize::MAX)
        .collect();
    let failed_waits: Vec<_> = log.iter().filter(|step| step.level == usize::MAX).collect();
    let final_shape = engine.core.read_state().levels.shape();
    // Output growth: how far a step's output (stored and held) came above
    // its inputs', and the largest output against the 256 KiB limit.
    let growth = log
        .iter()
        .filter(|step| step.kind == Some(CompactionKind::Merge))
        .map(|step| {
            (step.output_bytes + step.output_held) as i64
                - (step.input_bytes + step.input_held) as i64
        })
        .max()
        .unwrap_or(0);
    let stored_growth = log
        .iter()
        .filter(|step| step.kind == Some(CompactionKind::Merge))
        .map(|step| step.output_bytes as i64 - (step.input_bytes + step.input_held) as i64)
        .max()
        .unwrap_or(0);
    let largest = log.iter().map(|step| step.output_bytes).max().unwrap_or(0);
    let merges: Vec<_> = log
        .iter()
        .filter(|step| step.kind == Some(CompactionKind::Merge))
        .collect();
    let ppm = merges
        .iter()
        .map(|step| {
            let input = (step.input_bytes + step.input_held) as i64;
            ((step.output_bytes + step.output_held) as i64 - input) * 1_000_000 / input.max(1)
        })
        .max()
        .unwrap_or(0);
    let near = merges
        .iter()
        .filter(|step| step.input_bytes + step.input_held + 1024 > 256 << 10)
        .count();
    let grew_near = merges
        .iter()
        .filter(|step| {
            let input = step.input_bytes + step.input_held;
            input + 1024 > 256 << 10 && step.output_bytes + step.output_held > input
        })
        .count();
    println!(
        "NEAR | {label} | {values} | {policy} | merges {}, max growth {ppm} ppm, within 1 KiB of the limit {near}, of which grew {grew_near} |",
        merges.len()
    );
    println!(
        "GROWTH | {label} | {values} | {policy} | max merge growth {growth} B, stored over estimate {stored_growth} B, largest stored output {largest} B |"
    );
    println!(
        "STEPS | {label} | {values} | {policy} | operating WA {:.2}, L0 {} flushes, {} | drained {} | L0 merges {}: L0 in {:.1}, L1 in {:.1}, L1/L0 bytes {:.2}, left out {:.0}% , at L0 {:.1} flushes, L1 {:.0} KiB | {} | errors {} {:?} | failed waits {} {:?} |",
        operating.0,
        operating.1,
        shape(&operating.2),
        shape(&final_shape),
        merges0.len(),
        mean(&|step| step.level0_inputs),
        mean(&|step| step.lower_inputs),
        lower_bytes as f64 / level0_bytes.max(1) as f64,
        100.0 * merges0.iter().filter(|step| step.level0_left > 0).count() as f64 / n,
        mean(&|step| step.level0_flushes),
        mean(&|step| step.shape.get(1).map_or(0, |level| level.1)) / 1024.0,
        jobs.join(", "),
        errors.len(),
        errors
            .first()
            .map(|step| (step.error.clone(), step.level0_flushes, shape(&step.shape))),
        failed_waits.len(),
        failed_waits.first().map(|step| (
            step.error.clone(),
            step.level0_flushes,
            shape(&step.shape)
        )),
    );
}
