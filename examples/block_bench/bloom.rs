//! Milestone 5I: the engine's filtered point lookups against unfiltered
//! lookups over the same published runs (a `RunSet` of their views, which
//! carry no filters), on overlapping and settled run sets.

use std::hint::black_box;
use std::time::Instant;

use tessera::engine::{CompactionOptions, Engine, EngineWorkspace, UpdateError};
use tessera::memtable::WriteError;
use tessera::run::{LogicalWorkspace, MergeWorkspace, ReadWorkspace, Run, RunSet, RunView};

use super::{
    FLAT_TREE, FlushTally, Owned, Rng, allocations_during, mib, percentile, shuffled,
    snapshot_base, snapshot_options, tree_json, tree_key, tree_small,
};

const BUDGET: usize = 1 << 20;
const FLUSHES: usize = 16;
const PROBES: usize = 20_000;

fn compact_until(engine: &Engine, max_overlap: usize) {
    let options = CompactionOptions {
        max_overlap,
        ..CompactionOptions::default()
    };
    let mut workspace = MergeWorkspace::new();
    for _ in 0..10_000 {
        if engine.compact(options, &mut workspace).unwrap().is_none() {
            return;
        }
    }
    panic!("compaction did not finish");
}

/// Puts from `next` until the `count`th flush, leaving the memtable empty so
/// that the engine's answers come from the runs alone.
fn flush_puts(engine: &Engine, count: usize, mut next: impl FnMut() -> (Vec<u8>, Vec<u8>)) {
    let mut tally = FlushTally::default();
    for _ in 0..10_000_000 {
        let (key, value) = next();
        match engine.put(&key, &value) {
            Ok(_) => continue,
            Err(UpdateError::Rejected(WriteError::BudgetExceeded { .. })) => {
                tally.flush(engine);
                if tally.flushes == count {
                    return;
                }
                engine.put(&key, &value).unwrap();
            }
            Err(error) => panic!("{error}"),
        }
    }
    panic!("flushing did not finish");
}

/// The run sets measured, built from one dataset.
fn states(records: &[Owned]) -> Vec<(&'static str, Engine)> {
    let count = records.len();
    let base = || {
        let engine = snapshot_base(
            records,
            &shuffled(count, 900),
            snapshot_options(BUDGET, false),
        );
        compact_until(&engine, 1);
        engine
    };
    let value = |rng: &mut Rng| {
        records[(rng.next() % count as u64) as usize]
            .value
            .clone()
            .unwrap()
    };
    let overwrite = |engine: &Engine, flushes: usize, seed: u64| {
        let mut rng = Rng(seed);
        flush_puts(engine, flushes, || {
            let key = records[(rng.next() % count as u64) as usize].key.clone();
            (key, value(&mut rng))
        });
    };
    let mut states = Vec::new();

    let engine = base();
    overwrite(&engine, FLUSHES, 1);
    states.push(("overwrites, base and 16 flushes", engine));
    let engine = base();
    overwrite(&engine, FLUSHES, 1);
    compact_until(&engine, CompactionOptions::default().max_overlap);
    states.push(("overwrites, compacted (overlap 4)", engine));
    let engine = base();
    overwrite(&engine, 1, 2);
    states.push(("overwrites, base and 1 flush", engine));
    states.push(("overwrites, 1 run", base()));
    for (_, engine) in &states {
        let stats = engine.stats();
        assert!(
            stats.active.payload == 0 && stats.frozen.is_none(),
            "the runs hold everything"
        );
    }

    let engine = base();
    let order = shuffled(count * 2, 77);
    let mut at = 0;
    let mut rng = Rng(3);
    flush_puts(&engine, FLUSHES, || {
        let key = tree_key((count + order[at] % (count * 2)) as u64, FLAT_TREE);
        at += 1;
        (key, value(&mut rng))
    });
    states.push(("new keys, base and 16 flushes", engine));
    states
}

/// Present keys sampled from the published runs, and absent keys inside
/// their ranges: a present key with a zero byte appended (keys here have a
/// fixed length, so it is never stored).
fn probes(runs: &[&Run<Vec<u8>>], rng: &mut Rng) -> (Vec<Vec<u8>>, Vec<Vec<u8>>) {
    let mut keys = Vec::new();
    for run in runs {
        let mut read = ReadWorkspace::new();
        let mut cursor = run.cursor(&mut read);
        while let Some(record) = cursor.next_record().unwrap() {
            keys.push(record.key.to_vec());
        }
    }
    let present: Vec<Vec<u8>> = (0..PROBES)
        .map(|_| keys[(rng.next() % keys.len() as u64) as usize].clone())
        .collect();
    let absent = present
        .iter()
        .map(|key| {
            let mut key = key.clone();
            key.push(0);
            key
        })
        .collect();
    (present, absent)
}

/// Mean ns, p50 and p99 µs, and blocks per lookup of `get` over `keys`.
fn timed(
    keys: &[Vec<u8>],
    mut get: impl FnMut(&[u8]) -> (Option<u64>, u64),
) -> (f64, f64, f64, f64, Vec<Option<u64>>) {
    let mut latency = Vec::with_capacity(keys.len());
    let mut answers = Vec::with_capacity(keys.len());
    let mut blocks = 0;
    let begun = Instant::now();
    for key in keys {
        let start = Instant::now();
        let (answer, loaded) = get(key);
        latency.push(start.elapsed().as_nanos() as u64);
        answers.push(answer);
        blocks += loaded;
    }
    let mean = begun.elapsed().as_nanos() as f64 / keys.len() as f64;
    latency.sort_unstable();
    (
        mean,
        percentile(&latency, 0.5) as f64 / 1e3,
        percentile(&latency, 0.99) as f64 / 1e3,
        blocks as f64 / keys.len() as f64,
        answers,
    )
}

fn rows(dataset: &str, label: &str, engine: &Engine) {
    let stats = engine.stats();
    let guard = engine.read();
    let runs: Vec<&Run<Vec<u8>>> = guard.runs().collect();
    let views: Vec<RunView<'_>> = runs.iter().map(|run| run.view()).collect();
    let (present, absent) = probes(&runs, &mut Rng(9));
    let mut workspace = EngineWorkspace::new();
    let mut logical = LogicalWorkspace::new();
    let mut read = ReadWorkspace::new();
    for (kind, keys) in [("present", &present), ("absent, in range", &absent)] {
        // Candidates and the runs holding each key, read without filters.
        let (mut candidates, mut holding) = (0, 0);
        for key in keys {
            for view in &views {
                let range = view.key_range().unwrap().unwrap();
                if range.min <= key.as_slice() && key.as_slice() <= range.max {
                    candidates += 1;
                    holding += usize::from(view.get(key, &mut read).unwrap().is_some());
                }
            }
        }
        let filtered = timed(keys, |key| {
            let before = workspace.blocks_loaded();
            let found = guard
                .get(key, &mut workspace)
                .unwrap()
                .map(|found| found.seq);
            (found, workspace.blocks_loaded() - before)
        });
        let unfiltered = timed(keys, |key| {
            let before = logical.blocks_loaded();
            let found = RunSet::new(&views)
                .get(key, &mut logical)
                .unwrap()
                .map(|found| found.seq);
            (found, logical.blocks_loaded() - before)
        });
        assert_eq!(filtered.4, unfiltered.4, "filters changed an answer");
        let allocations = allocations_during(|| {
            for key in keys.iter().take(1_000) {
                black_box(guard.get(key, &mut workspace).unwrap());
            }
        });
        let per = |count: usize| count as f64 / keys.len() as f64;
        let without = per(candidates - holding);
        let passed = filtered.3 - per(holding);
        println!(
            "| {dataset} | {label} | {kind} | {} | {:.2} | {:.2} | {:.2} | {:.2} | {:.1}% | {:.0} | {:.0} | {:.1} | {:.1} | {:.1} | {:.1} | {:.0} | {:.0} | {:.2} | {:.1}% | {:.3} |",
            runs.len(),
            per(candidates),
            per(holding),
            unfiltered.3,
            filtered.3,
            if without == 0.0 {
                0.0
            } else {
                passed / without * 100.0
            },
            unfiltered.0,
            filtered.0,
            unfiltered.1,
            filtered.1,
            unfiltered.2,
            filtered.2,
            1e6 / unfiltered.0,
            1e6 / filtered.0,
            mib(stats.filter_bytes),
            stats.filter_bytes as f64 / stats.run_bytes as f64 * 100.0,
            allocations as f64 / 1_000.0,
        );
    }
}

pub(super) fn bloom_bench() {
    println!(
        "bloom: the engine's filtered lookup against an unfiltered RunSet over the same run views, {PROBES} probes per row; every answer compared"
    );
    println!();
    println!(
        "| dataset | state | probe | runs | candidates | holding | blocks unfiltered | blocks filtered | false positive | unfiltered ns | filtered ns | unfiltered p50 µs | filtered p50 µs | unfiltered p99 µs | filtered p99 µs | unfiltered k/s | filtered k/s | filter MiB | of stored | allocs/lookup |"
    );
    println!("|{}", "---|".repeat(20));
    for (dataset, records) in [
        ("json", tree_json(16 << 20)),
        ("8-byte", tree_small(8 << 20)),
    ] {
        for (label, engine) in states(&records) {
            rows(dataset, label, &engine);
        }
    }
}
