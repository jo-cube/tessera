//! A model checks production compaction over recovered runs.

use std::collections::BTreeMap;

use super::capacity_tests::Rng;
use super::*;

fn engine_key(index: u64) -> Vec<u8> {
    format!("user/{index:010}").into_bytes()
}

fn json(index: u64) -> Vec<u8> {
    format!(
        r#"{{"id":{index},"user":"user-{}","email":"u{}@example.com","score":{},"tags":["alpha","beta"],"active":true,"created":"2026-11-{:02}T10:{:02}:00Z"}}"#,
        index % 9_973,
        index % 9_973,
        index * 37 % 1_000,
        index % 28 + 1,
        index % 60,
    )
    .into_bytes()
}

/// Section 7: recovery yields one disjoint level, and a leveled schedule
/// over it, through the production compaction job, keeps every deeper
/// level sorted and disjoint after each publication, shares untouched runs,
/// keeps snapshots reading what they saw, and agrees with a model whatever
/// the level tags.
#[test]
fn a_leveled_schedule_over_recovered_runs_keeps_levels_valid() {
    let dir = std::env::temp_dir().join(format!("tessera-leveling-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let options = EngineOptions {
        memtable_budget: 16 << 10,
        target_run_len: 4 << 10,

        background_compaction: None,
        ..EngineOptions::default()
    };
    let mut rng = Rng(21);
    let mut model: BTreeMap<Vec<u8>, Vec<u8>> = BTreeMap::new();
    {
        let engine = Engine::new(options).unwrap();
        for _ in 0..3_000 {
            let key = engine_key(rng.below(2_000));
            let value = json(rng.below(1 << 30));
            engine.put(&key, &value).unwrap();
            model.insert(key, value);
        }
        engine.flush().unwrap();
        // Consolidate all versions once to seed the independent schedule
        // with disjoint runs, which checkpoints now preserve as-is.
        let count = engine.stats().runs;
        engine
            .compact_picked(
                CompactionPolicy {
                    max_inputs: count.max(2),
                    max_workspace_bytes: 256 << 20,
                    ..CompactionPolicy::default()
                },
                (0..count).collect(),
                &mut MergeWorkspace::new(),
            )
            .unwrap();
        engine
            .checkpoint(&dir, CheckpointOptions::default())
            .unwrap();
        engine.close().unwrap();
    }
    let engine = Engine::recover(&dir, options).unwrap();
    let _ = std::fs::remove_dir_all(&dir);

    let range = |run: &SharedRun| {
        let range = run.run.key_range().unwrap();
        (range.min.to_vec(), range.max.to_vec())
    };
    let disjoint = |level: &[SharedRun]| {
        level
            .windows(2)
            .all(|pair| range(&pair[0]).1 < range(&pair[1]).0)
    };
    // The fixture is settled before checkpointing: all runs are disjoint.
    let mut recovered = engine.core.read_state().runs.clone();
    recovered.sort_by_key(|run| range(run).0);
    assert!(recovered.len() > 4 && disjoint(&recovered));
    assert_eq!(engine.core.read_state().directory.group_count(), 1);
    // Start the independent schedule model with all settled runs in level 2.
    // The production engine has restored its recorded levels.
    let mut levels: Vec<Vec<SharedRun>> = vec![Vec::new(), Vec::new(), recovered];

    let check_reads = |engine: &Engine, model: &BTreeMap<Vec<u8>, Vec<u8>>| {
        let mut workspace = EngineWorkspace::new();
        for index in 0..2_100 {
            let key = engine_key(index);
            let found = engine
                .get(&key, &mut workspace)
                .unwrap()
                .map(|live| live.value.to_vec());
            assert_eq!(found.as_ref(), model.get(&key), "key {index}");
        }
    };
    let position = |engine: &Engine, run: &SharedRun| {
        engine
            .core
            .read_state()
            .runs
            .iter()
            .position(|other| Arc::ptr_eq(other, run))
            .unwrap()
    };
    // Compacts `sources` of level `from` with what they overlap in the next
    // level, and checks the publication.
    let job = |engine: &Engine, levels: &mut Vec<Vec<SharedRun>>, from: usize, source: usize| {
        let run = levels[from][source].clone();
        let (lo, hi) = range(&run);
        let below = &levels[from + 1];
        let start = below.partition_point(|other| range(other).1 < lo);
        let end = below
            .partition_point(|other| range(other).0 <= hi)
            .max(start);
        if start == end {
            // A trivial move: relabelled, nothing rewritten.
            levels[from].remove(source);
            levels[from + 1].insert(start, run);
            return;
        }
        let mut picks = vec![position(engine, &run)];
        picks.extend(
            below[start..end]
                .iter()
                .map(|other| position(engine, other)),
        );
        let before = engine.core.read_state().runs.clone();
        let report = engine
            .compact_picked(
                CompactionPolicy::default(),
                picks.clone(),
                &mut MergeWorkspace::new(),
            )
            .unwrap();
        // What publication adds to the published bytes: the reserve a
        // compaction needs under the byte limit.
        let growth = (report.output_bytes + report.output_held_bytes) as i64
            - (report.input_bytes + report.input_held_bytes) as i64;
        let per_output = growth as f64 / report.outputs.max(1) as f64;
        GROWTH.with(|cell| {
            let (largest, per_run) = cell.get();
            cell.set((largest.max(growth), per_run.max(per_output)));
        });
        let after = engine.core.read_state().runs.clone();
        for (at, old) in before.iter().enumerate() {
            let kept = after.iter().any(|new| Arc::ptr_eq(new, old));
            assert_eq!(
                kept,
                !picks.contains(&at),
                "only the job's runs are replaced"
            );
        }
        let outputs: Vec<SharedRun> = after
            .iter()
            .filter(|new| !before.iter().any(|old| Arc::ptr_eq(old, new)))
            .cloned()
            .collect();
        levels[from].remove(source);
        let replaced: Vec<SharedRun> = levels[from + 1].splice(start..end, outputs).collect();
        assert_eq!(replaced.len(), end - start);
        assert!(
            disjoint(&levels[from + 1]),
            "level {} stays disjoint",
            from + 1
        );
        let state = engine.core.read_state();
        assert_eq!(state.directory.len(), state.runs.len());
        assert_eq!(
            levels.iter().map(Vec::len).sum::<usize>(),
            state.runs.len(),
            "every run in exactly one level"
        );
    };

    thread_local! {
        static GROWTH: std::cell::Cell<(i64, f64)> = const { std::cell::Cell::new((i64::MIN, f64::MIN)) };
    }
    let mut snapshot = None;
    let mut inserted = 0;
    for round in 0..12 {
        for _ in 0..400 {
            let key = engine_key(rng.below(2_100));
            if rng.below(5) == 0 {
                engine.delete(&key).unwrap();
                model.remove(&key);
            } else {
                let value = json(rng.below(1 << 30));
                engine.put(&key, &value).unwrap();
                model.insert(key, value);
            }
        }
        engine.flush().unwrap();
        let known: Vec<SharedRun> = levels.iter().flatten().cloned().collect();
        let mut flushed: Vec<SharedRun> = engine
            .core
            .read_state()
            .runs
            .iter()
            .filter(|run| !known.iter().any(|other| Arc::ptr_eq(run, other)))
            .cloned()
            .collect();
        flushed.sort_by_key(|run| range(run).0);
        levels[0].extend(flushed);
        if round == 3 {
            snapshot = Some((engine.snapshot(), model.clone()));
        }
        // Level 0 drains oldest first; level i keeps at most 4·4^(i−1) runs
        // by pushing runs down; a level is inserted under level 0 when the
        // last holds more than four times the level above it may.
        while !levels[0].is_empty() {
            job(&engine, &mut levels, 0, 0);
        }
        for level in 1..levels.len() - 1 {
            while levels[level].len() > 4 * 4usize.pow(level as u32 - 1) {
                let at = rng.below(levels[level].len() as u64) as usize;
                job(&engine, &mut levels, level, at);
            }
        }
        let last = levels.len() - 1;
        if levels[last].len() > 16 * 4usize.pow(last as u32 - 1) {
            levels.insert(1, Vec::new());
            inserted += 1;
        }
        check_reads(&engine, &model);
        if let Some((snapshot, seen)) = &snapshot {
            let mut workspace = EngineWorkspace::new();
            for (key, value) in seen.iter().take(200) {
                let found = snapshot
                    .get(key, &mut workspace)
                    .unwrap()
                    .map(|live| live.value.to_vec());
                assert_eq!(found.as_ref(), Some(value));
            }
            assert!(engine.stats().retained_run_bytes > 0);
        }
    }
    drop(snapshot);
    assert_eq!(engine.stats().retained_run_bytes, 0);
    for level in &levels[1..] {
        assert!(disjoint(level));
    }
    assert!(inserted > 0, "a level was inserted");
    let (largest, per_run) = GROWTH.with(std::cell::Cell::get);
    println!(
        "SCHEDULE | levels {:?} | largest published growth of a job {largest} B, {per_run:.0} B per output run |",
        levels.iter().map(Vec::len).collect::<Vec<_>>()
    );
}
