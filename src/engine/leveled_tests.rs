//! Leveled compaction in production (5AT): level membership and
//! disjointness after every publication, reads unaffected by levels,
//! recovery into one level, moves, splits, neighbour merges, refusals and
//! growth past the old run ceiling.
//!
//! Every publication in a test build checks the level invariants
//! (`Levels::check`, from `State::order_runs` and after each move), so every
//! test in the crate that flushes or compacts checks them too.

use std::collections::{BTreeMap, BTreeSet};

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
    // A job the worker began before anything was retained may publish
    // after the refusal, draining part of level 0; the next is refused.
    assert!(engine.stats().level0_flushes <= levels::LEVEL0_STOP);

    drop(snapshot);
    engine.wait_for_compaction().unwrap();
    assert!(engine.stats().level0_flushes < 4);
    for _ in 0..2_000 {
        waiting(&engine, &mut put);
    }
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
