//! Compaction input selection (5AJ): how much obsolete data the selector's
//! legal choices eliminate per byte rewritten, under three write histories.
//!
//! An ignored measurement, not a check. Run it in release, one at a time:
//!
//! ```text
//! cargo test --release --lib run::tests::selection -- --ignored --nocapture
//! ```
//!
//! The engine runs no worker: after each flush the test compacts until
//! nothing is eligible, as the worker does, so that every decision is
//! deterministic and can be examined before it is made. Between flushes the
//! runs do not change, so each such state is weighted by the writes it
//! served, and its historical-version bytes are measured exactly: the run
//! bytes less what a full merge of every run, built and abandoned, would
//! store. Alternatives are built the same way and never published, except
//! under `SELECT_POLICY`, which publishes them through the production job.
//!
//! - `SELECT_HISTORY`: `uniform` (updates and 15% deletes of random keys,
//!   the 5AH workload), `hot` (nine writes in ten within the lowest eighth
//!   of the keys) or `append` (from the lowest quarter of the keys, eight
//!   writes in ten insert the next key and two update a random inserted key).
//! - `SELECT_WRITES` (4,000,000) on `json:SELECT_GROUPS` (64, or 256 for
//!   `append`); `SELECT_OVERLAP` is `max_overlap` (4).
//! - `SELECT_POLICY`: `current` (the production selector), `stack` (every run
//!   of the stack the selector would take, within the input limits) or
//!   `oracle` (of the legal choices with at most 1.25 times the current
//!   choice's input, the one that frees the most bytes, by building each).
//! - `SELECT_ENUMERATE`: the decisions, numbered from 1, at which every legal
//!   choice is built and listed (`3,8,15`).
//! - `SELECT_EXACT=0` skips the full merges.

use std::collections::BTreeSet;
use std::time::Instant;

use crate::engine::{
    CompactionOptions, Compression, DryCompaction, Engine, EngineOptions, EngineWorkspace, RunShape,
};
use crate::run::MergeWorkspace;

use super::costs::{Rng, compare};

const GROUP: u64 = 16_384;

fn env(name: &str, default: u64) -> u64 {
    std::env::var(name).map_or(default, |value| value.parse().unwrap())
}

fn mib(bytes: usize) -> f64 {
    bytes as f64 / f64::from(1 << 20)
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum History {
    Uniform,
    Hot,
    Append,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Policy {
    Current,
    Stack,
    Oracle,
}

struct Writer {
    groups: u64,
    history: History,
    rng: Rng,
    /// Each key's version, 0 while it is absent.
    versions: Vec<u32>,
    /// The next key to insert when appending.
    next: u64,
    logical: u64,
    inserts: u64,
    updates: u64,
    deletes: u64,
}

impl Writer {
    fn key(&self, index: u64) -> [u8; 8] {
        compare::key(self.groups, index)
    }

    fn write(&mut self, engine: &Engine, step: u64) {
        let records = self.versions.len() as u64;
        let index = match self.history {
            History::Uniform => self.rng.below(records),
            History::Hot if self.rng.below(10) < 9 => self.rng.below(records / 8),
            History::Hot => self.rng.below(records),
            History::Append if self.rng.below(10) < 8 => {
                self.next += 1;
                self.next - 1
            }
            History::Append => self.rng.below(self.next),
        };
        let key = self.key(index);
        let present = self.versions[index as usize] != 0;
        if present && self.history != History::Append && self.rng.below(100) < 15 {
            engine.delete(&key).unwrap();
            self.versions[index as usize] = 0;
            self.deletes += 1;
            self.logical += key.len() as u64;
        } else {
            let version = 2 + step as u32;
            let value = compare::json(index, u64::from(version));
            engine.put(&key, &value).unwrap();
            self.versions[index as usize] = version;
            *if present {
                &mut self.updates
            } else {
                &mut self.inserts
            } += 1;
            self.logical += (key.len() + value.len()) as u64;
        }
    }
}

/// The most runs whose ranges share a key, as the engine counts it.
fn deepest<'a>(ranges: impl Iterator<Item = (&'a [u8], &'a [u8])> + Clone) -> usize {
    ranges
        .clone()
        .map(|(point, _)| {
            ranges
                .clone()
                .filter(|&(min, max)| min <= point && point <= max)
                .count()
        })
        .max()
        .unwrap_or(0)
}

fn covering(shapes: &[RunShape], point: &[u8]) -> Vec<usize> {
    (0..shapes.len())
        .filter(|&index| shapes[index].min.as_slice() <= point && point <= &shapes[index].max)
        .collect()
}

fn bytes(shapes: &[RunShape], set: &[usize]) -> usize {
    set.iter().map(|&index| shapes[index].bytes).sum()
}

fn fits(shapes: &[RunShape], set: &[usize], options: CompactionOptions) -> bool {
    set.len() >= 2
        && set.len() <= options.max_inputs
        && bytes(shapes, set) <= options.max_input_bytes
}

/// Every choice the selector's invariants allow: two or more runs that share
/// a key covered by more than the allowance, or if nothing exceeds it and
/// there are enough runs, adjacent tails with no full run between them;
/// within the input limits. Each sorted, without repeats.
fn legal(shapes: &[RunShape], options: CompactionOptions) -> Vec<Vec<usize>> {
    let limit = options.max_overlap.max(1);
    let mut groups = BTreeSet::new();
    let ranges = shapes.iter().map(|s| (s.min.as_slice(), s.max.as_slice()));
    if deepest(ranges) > limit {
        for shape in shapes {
            let stack = covering(shapes, &shape.min);
            if stack.len() <= limit {
                continue;
            }
            let n = stack.len().min(14);
            for mask in 1_u32..(1 << n) {
                let set: Vec<usize> = (0..n)
                    .filter(|bit| mask & (1 << bit) != 0)
                    .map(|bit| stack[bit])
                    .collect();
                if fits(shapes, &set, options) {
                    groups.insert(set);
                }
            }
        }
    } else if shapes.len() > crate::engine::FLAT_DISJOINT_RUNS_ABOVE {
        let mut order: Vec<usize> = (0..shapes.len()).collect();
        order.sort_by(|&a, &b| shapes[a].min.cmp(&shapes[b].min));
        for stretch in order.split(|&index| !shapes[index].tail) {
            for start in 0..stretch.len() {
                for end in start + 2..=stretch.len() {
                    let mut set = stretch[start..end].to_vec();
                    if fits(shapes, &set, options) {
                        set.sort_unstable();
                        groups.insert(set);
                    }
                }
            }
        }
    }
    groups.into_iter().collect()
}

/// The stack the production selector takes from, all of it within the input
/// limits, smallest first; `None` if nothing exceeds the allowance.
fn whole_stack(shapes: &[RunShape], options: CompactionOptions) -> Option<Vec<usize>> {
    let limit = options.max_overlap.max(1);
    let mut stacks: Vec<(&[u8], Vec<usize>)> = shapes
        .iter()
        .map(|shape| (shape.min.as_slice(), covering(shapes, &shape.min)))
        .filter(|(_, stack)| stack.len() > limit)
        .collect();
    stacks.sort_by(|(a, x), (b, y)| {
        (y.len(), bytes(shapes, x), *a).cmp(&(x.len(), bytes(shapes, y), *b))
    });
    let (_, mut stack) = stacks.into_iter().next()?;
    stack.sort_by_key(|&index| (shapes[index].bytes, index));
    let mut picked = Vec::new();
    for index in stack {
        let mut next = picked.clone();
        next.push(index);
        if next.len() > options.max_inputs || bytes(shapes, &next) > options.max_input_bytes {
            break;
        }
        picked = next;
    }
    (picked.len() >= 2).then_some(picked)
}

/// The deepest overlap once `set` is replaced by `built`'s output.
fn overlap_after(shapes: &[RunShape], set: &[usize], built: &DryCompaction) -> usize {
    let kept = (0..shapes.len())
        .filter(|index| !set.contains(index))
        .map(|index| (shapes[index].min.as_slice(), shapes[index].max.as_slice()));
    let added = built
        .ranges
        .iter()
        .map(|(min, max)| (min.as_slice(), max.as_slice()));
    deepest(kept.chain(added))
}

fn describe(shapes: &[RunShape], set: &[usize]) -> String {
    let newest = shapes.iter().map(|s| s.max_seq).max().unwrap_or(0);
    set.iter()
        .map(|&index| {
            let shape = &shapes[index];
            format!(
                "{}{:.1}{}",
                if shape.dictionary { "d" } else { "p" },
                mib(shape.bytes),
                if shape.max_seq == newest { "*" } else { "" },
            )
        })
        .collect::<Vec<_>>()
        .join("+")
}

/// Builds every legal choice and lists the most efficient, the production
/// choice and the whole stack.
fn enumerate(
    engine: &Engine,
    options: CompactionOptions,
    decision: usize,
    current: &[usize],
    workspace: &mut MergeWorkspace,
) {
    let shapes = engine.run_shapes();
    let stack = whole_stack(&shapes, options);
    let groups = legal(&shapes, options);
    let ranges = shapes.iter().map(|s| (s.min.as_slice(), s.max.as_slice()));
    println!(
        "decision {decision}: {} runs, {:.1} MiB, overlap {}, {} legal choices; runs as d(ictionary)/p(lain) MiB, * newest",
        shapes.len(),
        mib(bytes(&shapes, &(0..shapes.len()).collect::<Vec<_>>())),
        deepest(ranges),
        groups.len(),
    );
    let mut rows = Vec::new();
    for set in &groups {
        let built = match engine.dry_compact(options, set.clone(), workspace) {
            Ok(built) => built,
            Err(error) => {
                println!("refused\t{}\t{error}", describe(&shapes, set));
                continue;
            }
        };
        rows.push((set.clone(), built));
    }
    let freed = |built: &DryCompaction| built.input_bytes as f64 - built.output_bytes as f64;
    rows.sort_by(|(_, a), (_, b)| {
        (freed(b) / b.input_bytes as f64).total_cmp(&(freed(a) / a.input_bytes as f64))
    });
    println!(
        "| choice | runs | in MiB | out MiB | freed MiB | freed / in | versions dropped | tombstones dropped | obsolete share | overlap after | note |"
    );
    println!("|---|---|---|---|---|---|---|---|---|---|---|");
    for (rank, (set, built)) in rows.iter().enumerate() {
        let mut sorted_current = current.to_vec();
        sorted_current.sort_unstable();
        let note = [
            (*set == sorted_current).then_some("current"),
            stack
                .as_ref()
                .is_some_and(|stack| {
                    let mut stack = stack.clone();
                    stack.sort_unstable();
                    *set == stack
                })
                .then_some("stack"),
        ]
        .into_iter()
        .flatten()
        .collect::<Vec<_>>()
        .join(", ");
        if rank >= 8 && note.is_empty() {
            continue;
        }
        let dropped = built.stats.versions_dropped + built.stats.tombstones_dropped;
        println!(
            "| {} | {} | {:.1} | {:.1} | {:.1} | {:.3} | {} | {} | {:.1}% | {} | {} |",
            describe(&shapes, set),
            set.len(),
            mib(built.input_bytes),
            mib(built.output_bytes),
            freed(built) / f64::from(1 << 20),
            freed(built) / built.input_bytes as f64,
            built.stats.versions_dropped,
            built.stats.tombstones_dropped,
            dropped as f64 / built.stats.records_read as f64 * 100.0,
            overlap_after(&shapes, set, built),
            note,
        );
    }
}

/// Of the legal choices whose input is at most 1.25 times the current
/// choice's, the one that frees the most stored bytes.
fn oracle(
    engine: &Engine,
    options: CompactionOptions,
    current: &[usize],
    workspace: &mut MergeWorkspace,
) -> Vec<usize> {
    let shapes = engine.run_shapes();
    let budget = bytes(&shapes, current) * 5 / 4;
    let mut best = (current.to_vec(), i64::MIN);
    for set in legal(&shapes, options) {
        if bytes(&shapes, &set) > budget {
            continue;
        }
        if let Ok(built) = engine.dry_compact(options, set.clone(), workspace) {
            let freed = built.input_bytes as i64 - built.output_bytes as i64;
            if freed > best.1 {
                best = (set, freed);
            }
        }
    }
    best.0
}

/// What every run would store merged into one consolidated set, built and
/// abandoned: the live records alone, without tombstones.
fn consolidated_bytes(engine: &Engine, workspace: &mut MergeWorkspace) -> usize {
    let shapes = engine.run_shapes();
    if shapes.len() < 2 {
        return bytes(&shapes, &(0..shapes.len()).collect::<Vec<_>>());
    }
    let options = CompactionOptions {
        max_inputs: usize::MAX,
        max_input_bytes: usize::MAX,
        max_output_bytes: usize::MAX,
        max_workspace_bytes: 256 << 20,
        ..CompactionOptions::default()
    };
    engine
        .dry_compact(options, (0..shapes.len()).collect(), workspace)
        .unwrap()
        .output_bytes
}

/// One state between flushes: its run bytes, historical bytes, runs and
/// overlap, weighted by the writes it served.
#[derive(Default)]
struct Weighted {
    writes: u64,
    run_bytes: f64,
    historical: f64,
    runs: f64,
    overlap: f64,
    plain: f64,
}

impl Weighted {
    fn add(&mut self, writes: u64, state: &(usize, Option<usize>, usize, usize, usize)) {
        let w = writes as f64;
        self.writes += writes;
        self.run_bytes += w * state.0 as f64;
        self.historical += w * state.1.unwrap_or(0) as f64;
        self.runs += w * state.2 as f64;
        self.overlap += w * state.3 as f64;
        self.plain += w * state.4 as f64;
    }

    fn mean(&self, total: f64) -> f64 {
        total / self.writes.max(1) as f64
    }
}

#[test]
#[ignore = "a measurement: run in release with --ignored --nocapture"]
fn compaction_selection() {
    let history = match std::env::var("SELECT_HISTORY").as_deref() {
        Ok("hot") => History::Hot,
        Ok("append") => History::Append,
        Ok("uniform") | Err(_) => History::Uniform,
        Ok(other) => panic!("unknown history {other}"),
    };
    let policy = match std::env::var("SELECT_POLICY").as_deref() {
        Ok("stack") => Policy::Stack,
        Ok("oracle") => Policy::Oracle,
        Ok("current") | Err(_) => Policy::Current,
        Ok(other) => panic!("unknown policy {other}"),
    };
    let groups = env(
        "SELECT_GROUPS",
        if history == History::Append { 256 } else { 64 },
    );
    let writes = env("SELECT_WRITES", 4_000_000);
    let max_overlap = env("SELECT_OVERLAP", 4) as usize;
    let exact = env("SELECT_EXACT", 1) != 0;
    let enumerate_at: Vec<usize> = std::env::var("SELECT_ENUMERATE")
        .map(|list| list.split(',').map(|n| n.parse().unwrap()).collect())
        .unwrap_or_default();
    let records = groups * GROUP;
    println!(
        "config\thistory={history:?}\tpolicy={policy:?}\twrites={writes}\trecords={records}\tmax_overlap={max_overlap}\texact={exact}"
    );
    let options = CompactionOptions {
        max_overlap,
        ..CompactionOptions::default()
    };
    let engine = Engine::new(EngineOptions {
        max_runs: 256,
        background_compaction: None,
        compression: Compression::ZstdDictionary,
        target_block_len: 4 << 10,
        ..EngineOptions::default()
    })
    .unwrap();

    // The sustained instrument's load: seven keys in eight, or when
    // appending the lowest quarter, consolidated.
    let mut writer = Writer {
        groups,
        history,
        rng: Rng::new(50_000),
        versions: vec![0; records as usize],
        next: records / 4,
        logical: 0,
        inserts: 0,
        updates: 0,
        deletes: 0,
    };
    if history == History::Append {
        assert!(
            writes * 8 / 10 + writes / 100 < records - records / 4,
            "appends past the last key"
        );
    }
    for j in 0..records {
        let index = (j.wrapping_mul(0x9E37_79B1).wrapping_add(0x7F4A_7C15)) & (records - 1);
        let loaded = match history {
            History::Append => index < records / 4,
            _ => index % 8 != 7,
        };
        if loaded {
            engine
                .put(&writer.key(index), &compare::json(index, 1))
                .unwrap();
            writer.versions[index as usize] = 1;
        }
    }
    engine.flush().unwrap();
    let consolidate = CompactionOptions {
        max_overlap: 1,
        max_inputs: usize::MAX,
        max_input_bytes: usize::MAX,
        max_output_bytes: usize::MAX,
        ..CompactionOptions::default()
    };
    let mut merge = MergeWorkspace::new();
    let mut dry = MergeWorkspace::new();
    while engine.compact(consolidate, &mut merge).unwrap().is_some() {}

    let state = |engine: &Engine, dry: &mut MergeWorkspace| {
        let stats = engine.stats();
        let historical =
            exact.then(|| stats.run_bytes - consolidated_bytes(engine, dry).min(stats.run_bytes));
        let plain = engine
            .run_shapes()
            .iter()
            .filter(|shape| !shape.dictionary)
            .map(|shape| shape.bytes)
            .sum::<usize>();
        (
            stats.run_bytes,
            historical,
            stats.runs,
            stats.max_overlap,
            plain,
        )
    };
    let mut current_state = state(&engine, &mut dry);
    println!(
        "base\truns {}\trun MiB {:.1}\thistorical MiB {:.1}",
        current_state.2,
        mib(current_state.0),
        mib(current_state.1.unwrap_or(0)),
    );
    println!(
        "| flush | write | runs before | overlap before | run MiB before | compactions | in MiB | out MiB | dropped | runs after | overlap after | run MiB after | plain MiB after | historical MiB after |"
    );
    println!("|---|---|---|---|---|---|---|---|---|---|---|---|---|---|");

    let mut weighted = Weighted::default();
    let (mut decision, mut compactions) = (0, 0);
    let (mut input_bytes, mut output_bytes) = (0usize, 0usize);
    let (mut versions_dropped, mut tombstones_dropped) = (0usize, 0usize);
    let (mut peak_runs, mut peak_transient) = (0usize, 0usize);
    let (mut compact_time, mut choose_time) = (0.0, 0.0);
    let mut flushes_seen = engine.stats().flushes;
    let flushes_base = flushes_seen;
    let flushed_base = engine.stats().flushed_bytes;
    let mut since = 0u64;
    for step in 0..writes {
        writer.write(&engine, step);
        since += 1;
        if step % 64 != 63 && step + 1 != writes {
            continue;
        }
        let stats = engine.stats();
        if stats.flushes == flushes_seen {
            continue;
        }
        flushes_seen = stats.flushes;
        weighted.add(since, &current_state);
        since = 0;
        let before = (stats.runs, stats.max_overlap, stats.run_bytes);
        peak_runs = peak_runs.max(stats.run_bytes);
        let (mut count, mut flush_in, mut flush_out, mut flush_dropped) = (0, 0, 0, 0);
        while let Some(current) = engine.selection(options).unwrap() {
            decision += 1;
            if enumerate_at.contains(&decision) {
                enumerate(&engine, options, decision, &current, &mut dry);
            }
            let choosing = Instant::now();
            let picks = match policy {
                Policy::Current => current,
                Policy::Stack => whole_stack(&engine.run_shapes(), options).unwrap_or(current),
                Policy::Oracle => oracle(&engine, options, &current, &mut dry),
            };
            choose_time += choosing.elapsed().as_secs_f64();
            let run_bytes = engine.stats().run_bytes;
            let compacting = Instant::now();
            let report = engine.compact_picked(options, picks, &mut merge).unwrap();
            compact_time += compacting.elapsed().as_secs_f64();
            peak_transient = peak_transient.max(run_bytes + report.output_bytes);
            count += 1;
            flush_in += report.input_bytes;
            flush_out += report.output_bytes;
            let dropped = report.stats.versions_dropped + report.stats.tombstones_dropped;
            flush_dropped += dropped;
            versions_dropped += report.stats.versions_dropped;
            tombstones_dropped += report.stats.tombstones_dropped;
        }
        compactions += count;
        input_bytes += flush_in;
        output_bytes += flush_out;
        current_state = state(&engine, &mut dry);
        println!(
            "| {} | {} | {} | {} | {:.1} | {count} | {:.1} | {:.1} | {flush_dropped} | {} | {} | {:.1} | {:.1} | {} |",
            flushes_seen - flushes_base,
            step + 1,
            before.0,
            before.1,
            mib(before.2),
            mib(flush_in),
            mib(flush_out),
            current_state.2,
            current_state.3,
            mib(current_state.0),
            mib(current_state.4),
            current_state
                .1
                .map_or(String::from("-"), |h| format!("{:.1}", mib(h))),
        );
    }
    weighted.add(since, &current_state);

    let flushed = (engine.stats().flushed_bytes - flushed_base) as usize;
    let logical = writer.logical as f64;
    println!(
        "writes\t{writes} ({} inserts, {} updates, {} deletes)\tlogical {:.1} MiB\tflushes {}",
        writer.inserts,
        writer.updates,
        writer.deletes,
        logical / f64::from(1 << 20),
        flushes_seen - flushes_base,
    );
    println!(
        "summary\t{history:?}\t{policy:?}\tcompactions {compactions}\tcompacted {:.1} -> {:.1} MiB\twrite amplification: flush {:.2}, compaction {:.2}, total {:.2}\tversions dropped {versions_dropped}, tombstones dropped {tombstones_dropped}\tmean run MiB {:.1}\tmean plain MiB {:.1}\tmean historical MiB {}\tmean runs {:.1}\tmean overlap {:.2}\tpeak run MiB {:.1}, during a merge {:.1}\tcompaction s {compact_time:.1}, choosing s {choose_time:.1}",
        mib(input_bytes),
        mib(output_bytes),
        flushed as f64 / logical,
        output_bytes as f64 / logical,
        (flushed + output_bytes) as f64 / logical,
        weighted.mean(weighted.run_bytes) / f64::from(1 << 20),
        weighted.mean(weighted.plain) / f64::from(1 << 20),
        if exact {
            format!(
                "{:.1}",
                weighted.mean(weighted.historical) / f64::from(1 << 20)
            )
        } else {
            String::from("-")
        },
        weighted.mean(weighted.runs),
        weighted.mean(weighted.overlap),
        mib(peak_runs),
        mib(peak_transient),
    );

    // Every live record, in key order, and nothing else.
    let mut live: Vec<u64> = (0..records)
        .filter(|&index| writer.versions[index as usize] != 0)
        .collect();
    live.sort_by_key(|&index| writer.key(index));
    let mut workspace = EngineWorkspace::new();
    let guard = engine.read();
    let mut scan = guard.scan(&mut workspace);
    let mut checked = 0usize;
    while let Some(record) = scan.next_record().unwrap() {
        let index = live[checked];
        assert_eq!(record.key, writer.key(index));
        let version = u64::from(writer.versions[index as usize]);
        assert_eq!(record.value, compare::json(index, version));
        checked += 1;
    }
    assert_eq!(checked, live.len());
    println!("full scan\t{checked} records verified");
}
