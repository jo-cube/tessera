//! Range-local replacement, sharing, retention and a leveled reference model.
//! Every replacement must preserve untouched sources and agree with independent
//! reads, including snapshots and recovered run sets.

use super::*;
use std::collections::BTreeMap;

const RUN_LEN: usize = 64 << 10;

fn engine_options() -> Options {
    Options {
        memtable_size: RUN_LEN,
        target_run_len: RUN_LEN,

        background_compaction: None,
        ..Options::default()
    }
}

/// The defaults with their byte limits scaled as the runs are.
pub(super) fn compaction_options() -> CompactionPolicy {
    CompactionPolicy {
        max_input_bytes: 4 * RUN_LEN,

        ..CompactionPolicy::default()
    }
}

fn key(index: u64) -> Vec<u8> {
    format!("user/{index:010}").into_bytes()
}

/// [`value`], shared with the compaction correctness tests.
pub(super) fn value_for(seed: u64) -> Vec<u8> {
    value(seed)
}

/// A 100-byte value from a small vocabulary, so that it compresses about
/// as text does.
fn value(seed: u64) -> Vec<u8> {
    const WORDS: [&[u8]; 8] = [
        b"alpha ",
        b"bravo ",
        b"charlie ",
        b"delta ",
        b"echo ",
        b"foxtrot ",
        b"golf ",
        b"hotel ",
    ];
    let mut state = seed.wrapping_mul(0x9e37_79b9_7f4a_7c15) | 1;
    let mut out = Vec::with_capacity(100);
    while out.len() < 100 {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        out.extend_from_slice(WORDS[(state % 8) as usize]);
    }
    out.truncate(100);
    out
}

pub(super) struct Rng(pub(super) u64);

impl Rng {
    pub(super) fn below(&mut self, bound: u64) -> u64 {
        self.0 = self.0.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        (z ^ (z >> 31)) % bound
    }
}

/// Range-local compaction replaces only the inputs it selects. The
/// runs a small update overlaps are found by binary search over the sorted
/// disjoint runs, merged with it, and replaced; every other run stays the
/// same shared run, a snapshot keeps reading what it saw, and the replaced
/// runs are retained only while it lives.
#[test]
fn a_range_local_replacement_leaves_other_runs_shared() {
    let engine = Engine::in_memory(engine_options()).unwrap();
    for index in 0..3_000 {
        engine.ks().put(&key(index), &value(index)).unwrap();
    }
    engine.flush().unwrap();
    let disjoint = engine.core.read_state().runs.len();
    assert!(disjoint >= 5, "{disjoint} runs");
    let snapshot = engine.snapshot();
    for index in 1_500..1_520 {
        engine.ks().put(&key(index), b"updated").unwrap();
    }
    engine.flush().unwrap();

    // Selection: the update's run, and the older runs its range meets.
    let (picks, before, update_bytes) = {
        let state = engine.core.read_state();
        let range = |run: &SharedRun| {
            let range = run.run.key_range().unwrap();
            (range.min.to_vec(), range.max.to_vec())
        };
        let newest = (0..state.runs.len())
            .max_by_key(|&at| state.runs[at].run.max_seq())
            .unwrap();
        let (low, high) = range(&state.runs[newest]);
        let mut older: Vec<usize> = (0..state.runs.len()).filter(|&at| at != newest).collect();
        older.sort_by_key(|&at| range(&state.runs[at]).0);
        let first = older.partition_point(|&at| range(&state.runs[at]).1 < low);
        let mut picks = vec![newest];
        picks.extend(
            older[first..]
                .iter()
                .take_while(|&&at| range(&state.runs[at]).0 <= high),
        );
        let update_bytes = state.runs[newest].run.as_bytes().len();
        (picks, state.runs.clone(), update_bytes)
    };
    assert!(
        picks.len() < before.len() - 1,
        "{} of {}",
        picks.len(),
        before.len()
    );

    let report = engine
        .compact_picked(
            compaction_options(),
            picks.clone(),
            &mut MergeWorkspace::new(),
        )
        .unwrap();
    let after = engine.core.read_state().runs.clone();
    for (at, run) in before.iter().enumerate() {
        let kept = after.iter().any(|other| Arc::ptr_eq(other, run));
        assert_eq!(kept, !picks.contains(&at), "run {at}");
    }
    assert_eq!(after.len(), before.len() - picks.len() + report.outputs);

    assert_eq!(
        engine.ks().get(&key(1_510)).unwrap(),
        Some(b"updated".to_vec())
    );
    assert_eq!(snapshot.value(&key(1_510)), Some(value(1_510)));
    assert!(engine.engine_stats().retained_run_bytes >= report.input_bytes);
    // The test's own references to the replaced runs count as a reader's.
    // Without them, the snapshot holds the older inputs; the update's run
    // was flushed after it, from a memtable it holds instead.
    drop((before, after));
    crate::test_support::wait_until(|| {
        engine.engine_stats().retained_run_bytes == report.input_bytes - update_bytes
    });
    assert_eq!(
        engine.engine_stats().retained_run_bytes,
        report.input_bytes - update_bytes
    );
    drop(snapshot);
    crate::test_support::wait_until(|| engine.engine_stats().retained_run_bytes == 0);
    assert_eq!(engine.engine_stats().retained_run_bytes, 0);
}

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

/// A leveled schedule over disjoint recovered runs keeps every deeper
/// level sorted and disjoint after each publication, shares untouched runs,
/// keeps snapshots reading what they saw, and agrees with a model whatever
/// the level tags.
#[test]
fn a_leveled_schedule_over_recovered_runs_keeps_levels_valid() {
    let dir = std::env::temp_dir().join(format!("tessera-leveling-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let options = Options {
        memtable_size: 16 << 10,
        target_run_len: 4 << 10,

        background_compaction: None,
        ..Options::default()
    };
    let mut rng = Rng(21);
    let mut model: BTreeMap<Vec<u8>, Vec<u8>> = BTreeMap::new();
    {
        let engine = Engine::in_memory(options).unwrap();
        for _ in 0..3_000 {
            let key = engine_key(rng.below(2_000));
            let value = json(rng.below(1 << 30));
            engine.ks().put(&key, &value).unwrap();
            model.insert(key, value);
        }
        engine.flush().unwrap();
        // Consolidate all versions once to seed the independent schedule
        // with disjoint runs, which checkpoints preserve as-is.
        let count = engine.engine_stats().runs;
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
        engine.checkpoint_to(&dir).unwrap();
        engine.close().unwrap();
    }
    let engine = Engine::open(&dir, options).unwrap();
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
    // Recovery restores the recorded levels.
    let mut levels: Vec<Vec<SharedRun>> = vec![Vec::new(), Vec::new(), recovered];

    let check_reads = |engine: &Engine, model: &BTreeMap<Vec<u8>, Vec<u8>>| {
        for index in 0..2_100 {
            let key = engine_key(index);
            let found = engine.ks().get(&key).unwrap();
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
                engine.ks().delete(&key).unwrap();
                model.remove(&key);
            } else {
                let value = json(rng.below(1 << 30));
                engine.ks().put(&key, &value).unwrap();
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
            for (key, value) in seen.iter().take(200) {
                let found = snapshot.value(key);
                assert_eq!(found.as_ref(), Some(value));
            }
            assert!(engine.engine_stats().retained_run_bytes > 0);
        }
    }
    drop(snapshot);
    crate::test_support::wait_until(|| engine.engine_stats().retained_run_bytes == 0);
    assert_eq!(engine.engine_stats().retained_run_bytes, 0);
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
