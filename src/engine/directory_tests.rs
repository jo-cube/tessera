//! Research (5AP, extended in 5AQ): the published runs indexed as groups of
//! mutually disjoint key ranges.
//!
//! Everything here is test-only. Each measured state is read through the
//! production directory (`super::directory`) and through the flat lookup the
//! engine used before 5AQ (`latest_in` over every run, newest bound first),
//! and scanned through the grouped merge and through the merge of every run
//! as its own input, on the same runs, in the same process, alternating, with
//! their results asserted equal. Grouping itself is tested deterministically
//! in `super::directory`.
//!
//! The tests are ignored. Run them with a release build:
//!
//! ```text
//! scripts/validate.sh exec 3000 cargo test --locked --release --lib \
//!   engine::directory_tests -- --ignored --nocapture --test-threads=1
//! ```
//!
//! `DIRECTORY_COUNTS` (comma-separated run counts, synthetic layouts),
//! `DIRECTORY_PROBES` (probes per class) and `DIRECTORY_SCALE` (writes in the
//! compacted histories, in thousands) change the sizes.

use std::cell::Cell;
use std::cmp::Reverse;
use std::collections::BTreeMap;
use std::time::Instant;

use super::*;
use crate::run::{KeyRange, LookupInput, LookupSource, latest_in};

/// The runs newest bound first: the flat lookup order before 5AQ.
pub(super) fn flat_order(runs: &[SharedRun]) -> Vec<SharedRun> {
    let mut order = runs.to_vec();
    order.sort_by_key(|run| Reverse(run.run.max_seq()));
    order
}

/// The flat lookup over the runs, without the memtables.
fn flat_get<'a, 'r>(
    runs: &'a [SharedRun],
    key: &[u8],
    logical: &'r mut LogicalWorkspace,
) -> Result<Option<LiveValue<'r>>, ReadError>
where
    'a: 'r,
{
    latest_in(runs.iter().map(|run| &run.run), key, logical)
}

// Instrumentation.

#[derive(Clone, Copy, Default)]
struct Counts {
    /// Runs whose range was compared with the key (not pruned by sequence).
    range: u64,
    /// Filter probes: runs whose range admitted the key.
    filter: u64,
    /// Runs opened, after range and filter admitted the key.
    opened: u64,
}

/// A run that counts what a lookup asks of it.
#[derive(Clone, Copy)]
struct Counted<'a> {
    run: &'a Run<Vec<u8>>,
    counts: &'a Cell<Counts>,
}

impl<'a> LookupInput<'a> for Counted<'a> {
    fn max_seq(self) -> u64 {
        self.run.max_seq()
    }

    fn excludes(self, key: &[u8], hash: &mut Option<u64>) -> bool {
        let mut counts = self.counts.get();
        counts.range += 1;
        let range = self.run.key_range().expect(NOT_EMPTY);
        if range.min <= key && key <= range.max && self.run.filter().is_some() {
            counts.filter += 1;
        }
        self.counts.set(counts);
        self.run.excludes(key, hash)
    }

    fn view(self) -> RunView<'a> {
        let mut counts = self.counts.get();
        counts.opened += 1;
        self.counts.set(counts);
        self.run.view()
    }
}

// Fixtures.

const STRIDE: u64 = 104_729;

fn key(index: u64) -> Vec<u8> {
    format!("user/{:010}", 2 * index).into_bytes()
}

fn missing_key(index: u64) -> Vec<u8> {
    format!("user/{:010}", 2 * index + 1).into_bytes()
}

/// 1,024 JSON-like values of about 150 to 500 bytes.
fn values() -> Vec<Vec<u8>> {
    const WORDS: [&str; 8] = [
        "alpha", "bravo", "charlie", "delta", "echo", "foxtrot", "golf", "hotel",
    ];
    let mut state = 0x9e37_79b9_7f4a_7c15u64;
    let mut next = move |bound: u64| {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        state % bound
    };
    (0..1024)
        .map(|index| {
            let bio: Vec<&str> = (0..20 + next(60))
                .map(|_| WORDS[next(8) as usize])
                .collect();
            format!(
                r#"{{"id":{index},"score":{},"plan":"{}","bio":"{}"}}"#,
                next(100_000),
                ["free", "pro", "team"][next(3) as usize],
                bio.join(" ")
            )
            .into_bytes()
        })
        .collect()
}

/// What the writes left for each key index.
#[derive(Clone, Copy, PartialEq)]
enum Fate {
    Unwritten,
    Present,
    Overwritten,
    Deleted,
}

struct Model {
    fates: Vec<Fate>,
}

impl Model {
    fn new(keys: u64) -> Self {
        Self {
            fates: vec![Fate::Unwritten; keys as usize],
        }
    }

    fn put(&mut self, index: u64) {
        let fate = &mut self.fates[index as usize];
        *fate = if *fate == Fate::Unwritten {
            Fate::Present
        } else {
            Fate::Overwritten
        };
    }

    fn delete(&mut self, index: u64) {
        self.fates[index as usize] = Fate::Deleted;
    }
}

struct Probes {
    name: &'static str,
    keys: Vec<Vec<u8>>,
    found: bool,
}

/// Up to `count` probes of each class the model holds.
fn probes(model: &Model, count: usize, seed: u64) -> Vec<Probes> {
    let mut rng = Rng(seed);
    let classes: [(&str, Fate, bool); 4] = [
        ("present", Fate::Present, true),
        ("overwritten", Fate::Overwritten, true),
        ("deleted", Fate::Deleted, false),
        ("missing", Fate::Unwritten, false),
    ];
    let written: Vec<u64> = (0..model.fates.len() as u64)
        .filter(|&index| model.fates[index as usize] != Fate::Unwritten)
        .collect();
    classes
        .into_iter()
        .filter_map(|(name, fate, found)| {
            let pool: Vec<u64> = if fate == Fate::Unwritten {
                written.clone()
            } else {
                written
                    .iter()
                    .copied()
                    .filter(|&index| model.fates[index as usize] == fate)
                    .collect()
            };
            if pool.is_empty() {
                return None;
            }
            let keys = (0..count)
                .map(|_| {
                    let index = pool[rng.below(pool.len() as u64) as usize];
                    if fate == Fate::Unwritten {
                        missing_key(index)
                    } else {
                        key(index)
                    }
                })
                .collect();
            Some(Probes { name, keys, found })
        })
        .collect()
}

struct Rng(u64);

impl Rng {
    fn below(&mut self, bound: u64) -> u64 {
        self.0 = self.0.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        (z ^ (z >> 31)) % bound
    }
}

fn env_or<T: std::str::FromStr>(name: &str, default: T) -> T {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

// Topology and measurement.

/// Runs that overlap no other run, with inclusive ranges.
fn isolated(runs: &[SharedRun]) -> usize {
    let mut ranges: Vec<(&[u8], &[u8])> = runs
        .iter()
        .map(|run| {
            let range = run.run.key_range().expect(NOT_EMPTY);
            (range.min, range.max)
        })
        .collect();
    ranges.sort();
    let mut count = 0;
    let mut reach: Option<&[u8]> = None;
    for (index, &(min, max)) in ranges.iter().enumerate() {
        let after = ranges.get(index + 1).is_none_or(|&(next, _)| max < next);
        let before = reach.is_none_or(|reach| reach < min);
        count += usize::from(before && after);
        reach = Some(reach.map_or(max, |reach| reach.max(max)));
    }
    count
}

/// Pairs of runs where one's largest key equals the other's smallest.
fn touching_pairs(runs: &[SharedRun]) -> usize {
    let ranges: Vec<KeyRange<'_>> = runs
        .iter()
        .map(|run| run.run.key_range().expect(NOT_EMPTY))
        .collect();
    let mut mins: Vec<&[u8]> = ranges.iter().map(|range| range.min).collect();
    mins.sort();
    ranges
        .iter()
        .map(|range| {
            let start = mins.partition_point(|&min| min < range.max);
            let end = mins.partition_point(|&min| min <= range.max);
            // A single-key run's own minimum is not a pair.
            (end - start) - usize::from(range.min == range.max)
        })
        .sum()
}

fn owned(
    found: Result<Option<LiveValue<'_>>, ReadError>,
) -> Result<Option<(u64, Vec<u8>)>, ReadError> {
    found.map(|found| found.map(|found| (found.seq, found.value.to_vec())))
}

fn percentile(sorted: &[f64], p: f64) -> f64 {
    sorted[((sorted.len() - 1) as f64 * p).round() as usize]
}

/// Times `get` once per probe over two rounds, and returns p50 and p99 in ns.
fn timed(keys: &[Vec<u8>], mut get: impl FnMut(&[u8]) -> bool, rounds: usize) -> Vec<f64> {
    let mut samples = Vec::with_capacity(keys.len() * rounds);
    for _ in 0..rounds {
        for key in keys {
            let start = Instant::now();
            std::hint::black_box(get(key));
            samples.push(start.elapsed().as_nanos() as f64);
        }
    }
    samples
}

/// The production directory's lookup, counting what it asks of each run.
struct CountedSource<'a> {
    inner: super::directory::Lookup<'a>,
    counts: &'a Cell<Counts>,
}

impl<'a> LookupSource<'a> for CountedSource<'a> {
    type Run = Counted<'a>;

    fn next_run(&mut self, key: &[u8], floor: Option<u64>) -> Option<(usize, Counted<'a>)> {
        let counts = self.counts;
        self.inner
            .next_run(key, floor)
            .map(|(position, run)| (position, Counted { run, counts }))
    }
}

/// Reads every live key with `prefix` through `cursor`, returning the count.
fn drain(cursor: &mut LogicalCursor<'_>, prefix: &[u8]) -> usize {
    cursor.seek_prefix(prefix).unwrap();
    let mut count = 0;
    while let Some(record) = cursor.next_record().unwrap() {
        std::hint::black_box(record);
        count += 1;
    }
    count
}

/// Measures one published state: topology, directory costs, and lookups and
/// scans through the flat order of 5AO and the production directory,
/// alternating, with their results checked against each other.
fn measure(label: &str, engine: &Engine, model: &Model, probe_count: usize) {
    // Nothing writes meanwhile, so the reads see one state.
    let (published, production) = {
        let state = engine.core.read_state();
        (state.runs.clone(), Arc::clone(&state.directory))
    };
    let runs = flat_order(&published);
    let stats = engine.stats();
    let probes = probes(model, probe_count, 17);

    // Directory and publication costs, each the best of five builds.
    let best = |f: &mut dyn FnMut()| {
        (0..5)
            .map(|_| {
                let start = Instant::now();
                f();
                start.elapsed().as_secs_f64() * 1e6
            })
            .fold(f64::INFINITY, f64::min)
    };
    let build_us = best(&mut || {
        std::hint::black_box(super::directory::RunDirectory::build(&published));
    });
    let order_us = best(&mut || {
        std::hint::black_box(Arc::<[SharedRun]>::from(flat_order(&published)));
    });
    let select_us = best(&mut || {
        let _ = std::hint::black_box(select(&candidates(&runs), CompactionOptions::default()));
    });
    assert_eq!(production.group_count(), deepest_overlap(&runs));
    let sizes: Vec<usize> = production.groups().map(|group| group.len()).collect();

    println!(
        "TOPOLOGY | {label} | {} | {} | {} | {} | {} | {:.1} | {} | {:.0} | {:.0} | {:.0} | {} | {} |",
        runs.len(),
        stats.max_overlap,
        isolated(&runs),
        production.group_count(),
        sizes.iter().max().unwrap_or(&0),
        runs.len() as f64 / production.group_count().max(1) as f64,
        touching_pairs(&runs),
        build_us,
        order_us,
        select_us,
        production.heap_bytes(),
        runs.len() * size_of::<SharedRun>(),
    );

    let mut flat_ws = LogicalWorkspace::default();
    let mut directory_ws = LogicalWorkspace::default();
    for class in &probes {
        // Correctness: both paths agree on every probe.
        let counts = Cell::new(Counts::default());
        let directory_counts = Cell::new(Counts::default());
        let mut blocks = [0u64; 2];
        for probe in &class.keys {
            let before = [flat_ws.blocks_loaded(), directory_ws.blocks_loaded()];
            let flat = owned(latest_in(
                runs.iter().map(|run| Counted {
                    run: &run.run,
                    counts: &counts,
                }),
                probe,
                &mut flat_ws,
            ));
            let source = CountedSource {
                inner: production.lookup(),
                counts: &directory_counts,
            };
            let grouped = owned(latest_from(source, probe, &mut directory_ws));
            assert_eq!(flat, grouped, "{label} {} {probe:02x?}", class.name);
            assert_eq!(
                flat.as_ref().unwrap().is_some(),
                class.found,
                "{label} {}",
                class.name
            );
            blocks[0] += flat_ws.blocks_loaded() - before[0];
            blocks[1] += directory_ws.blocks_loaded() - before[1];
        }

        // Timing: per call and over whole passes, alternating.
        let n = class.keys.len() as f64;
        let mut flat_samples = Vec::new();
        let mut directory_samples = Vec::new();
        let mut means = [0.0f64; 2];
        for _ in 0..2 {
            flat_samples.extend(timed(
                &class.keys,
                |key| flat_get(&runs, key, &mut flat_ws).unwrap().is_some(),
                1,
            ));
            directory_samples.extend(timed(
                &class.keys,
                |key| {
                    latest_from(production.lookup(), key, &mut directory_ws)
                        .unwrap()
                        .is_some()
                },
                1,
            ));
            let start = Instant::now();
            for key in &class.keys {
                std::hint::black_box(flat_get(&runs, key, &mut flat_ws).unwrap().is_some());
            }
            means[0] += start.elapsed().as_nanos() as f64 / n / 2.0;
            let start = Instant::now();
            for key in &class.keys {
                std::hint::black_box(
                    latest_from(production.lookup(), key, &mut directory_ws)
                        .unwrap()
                        .is_some(),
                );
            }
            means[1] += start.elapsed().as_nanos() as f64 / n / 2.0;
        }
        flat_samples.sort_by(f64::total_cmp);
        directory_samples.sort_by(f64::total_cmp);
        let (flat, grouped) = (counts.get(), directory_counts.get());
        println!(
            "LOOKUP | {label} | {} | {} | {:.0} / {:.0} / {:.0} | {:.0} / {:.0} / {:.0} | {:.1} / {:.1} | {:.2} / {:.2} | {:.2} / {:.2} | {:.3} / {:.3} |",
            runs.len(),
            class.name,
            means[0],
            percentile(&flat_samples, 0.5),
            percentile(&flat_samples, 0.99),
            means[1],
            percentile(&directory_samples, 0.5),
            percentile(&directory_samples, 0.99),
            flat.range as f64 / n,
            grouped.range as f64 / n,
            flat.filter as f64 / n,
            grouped.filter as f64 / n,
            flat.opened as f64 / n,
            grouped.opened as f64 / n,
            blocks[0] as f64 / n,
            blocks[1] as f64 / n,
        );
    }

    // Scans: short prefixes (5 keys written), long ones (500), each through a
    // fresh workspace first for its memory, then timed alternating.
    let mut rng = Rng(23);
    let keys = model.fates.len() as u64;
    for (name, len, count) in [("short", 14, 200), ("long", 12, 20)] {
        let prefixes: Vec<Vec<u8>> = (0..count)
            .map(|_| key(rng.below(keys))[..len].to_vec())
            .collect();
        let mut flat_ws = LogicalWorkspace::default();
        let mut directory_ws = LogicalWorkspace::default();
        let mut found = [0usize; 2];
        let mut blocks = [0u64; 2];
        let mut means = [0.0f64; 2];
        for round in 0..3 {
            let before = [flat_ws.blocks_loaded(), directory_ws.blocks_loaded()];
            let start = Instant::now();
            let mut cursor =
                LogicalCursor::over(runs.iter().map(|run| run.run.view()), &mut flat_ws);
            let flat: usize = prefixes
                .iter()
                .map(|prefix| drain(&mut cursor, prefix))
                .sum();
            drop(cursor);
            let flat_ns = start.elapsed().as_nanos() as f64;
            let start = Instant::now();
            let mut cursor =
                LogicalCursor::grouped(&*production, production.groups(), &mut directory_ws);
            let grouped: usize = prefixes
                .iter()
                .map(|prefix| drain(&mut cursor, prefix))
                .sum();
            drop(cursor);
            let directory_ns = start.elapsed().as_nanos() as f64;
            assert_eq!(flat, grouped, "{label} {name} scans agree");
            if round == 0 {
                found = [flat, grouped];
                blocks = [
                    flat_ws.blocks_loaded() - before[0],
                    directory_ws.blocks_loaded() - before[1],
                ];
            } else {
                means[0] += flat_ns / count as f64 / 2.0;
                means[1] += directory_ns / count as f64 / 2.0;
            }
        }
        // Per scan, a cursor is made once per batch here, as a scan makes one
        // per call; the per-call cost of making it is in the public A/B.
        println!(
            "SCAN | {label} | {} | {name} | {:.1} | {:.2} / {:.2} | {:.1} / {:.1} | {} / {} | {} / {} | {} |",
            runs.len(),
            found[0] as f64 / count as f64,
            means[0] / 1e3,
            means[1] / 1e3,
            blocks[0] as f64 / count as f64,
            blocks[1] as f64 / count as f64,
            flat_ws.context_bytes() >> 10,
            directory_ws.context_bytes() >> 10,
            flat_ws.capacity() >> 10,
            directory_ws.capacity() >> 10,
            production.group_count(),
        );
    }
}

fn print_headers() {
    println!(
        "TOPOLOGY | history | runs | max_overlap | isolated runs | groups | largest group | mean group | touching pairs | directory build µs | 5AO lookup order µs | select µs | directory bytes | 5AO lookup order bytes |"
    );
    println!(
        "LOOKUP | history | runs | class | 5AO flat mean / p50 / p99 ns | directory mean / p50 / p99 ns | range checks flat / directory | filter probes flat / directory | runs opened flat / directory | blocks flat / directory |"
    );
    println!(
        "SCAN | history | runs | length | keys per scan | flat / grouped µs per scan | blocks per scan flat / grouped | context KiB flat / grouped | decoded KiB flat / grouped | groups |"
    );
}

/// The options of the compacted histories: the defaults, scaled down so
/// that runs are small, with no run limits and compaction by hand.
fn scaled(run_len: usize) -> EngineOptions {
    EngineOptions {
        memtable_budget: run_len,
        target_run_len: run_len,
        max_runs: 1 << 20,
        max_run_bytes: usize::MAX / 4,
        background_compaction: None,
        ..EngineOptions::default()
    }
}

/// Compacts as the worker would, until nothing is selected or a limit
/// refuses. Returns the refusal, if any.
fn settle(engine: &Engine, workspace: &mut MergeWorkspace) -> Option<CompactionError> {
    loop {
        match engine.compact(CompactionOptions::default(), workspace) {
            Ok(Some(_)) => {}
            Ok(None) => return None,
            Err(error) => return Some(error),
        }
    }
}

#[derive(Clone, Copy, Debug)]
enum History {
    /// Every key once, in ascending order.
    Ascending,
    /// Every key once in a fixed permutation, then uniform overwrites and
    /// deletes: a mixed update history.
    Uniform,
    /// Every key once in ascending order, then overwrites and deletes
    /// within a hot 2% of the keys.
    Hot,
}

/// Runs a history under the selector, measuring it each time the run count
/// first reaches a mark, and reports the overlap the selector left after
/// every flush.
fn history(kind: History, writes: u64, probe_count: usize) {
    let run_len = 64 << 10;
    let keys = match kind {
        History::Ascending => writes,
        History::Uniform | History::Hot => writes * 3 / 4,
    };
    let engine = Engine::new(scaled(run_len)).unwrap();
    let values = values();
    let mut model = Model::new(keys);
    let mut workspace = MergeWorkspace::new();
    let mut rng = Rng(3);
    let mut flushes = 0;
    let mut overlaps: BTreeMap<usize, u64> = BTreeMap::new();
    let mut refusals = 0;
    let mut marks = vec![64usize, 256, 1024, 2048].into_iter().peekable();
    let label = format!("{kind:?}").to_lowercase();
    for write in 0..writes {
        let (index, delete) = if write < keys {
            let index = match kind {
                History::Uniform => write * STRIDE % keys,
                History::Ascending | History::Hot => write,
            };
            (index, false)
        } else {
            let span = match kind {
                History::Hot => keys / 50,
                _ => keys,
            };
            (rng.below(span), rng.below(5) == 0)
        };
        if delete {
            engine.delete(&key(index)).unwrap();
            model.delete(index);
        } else {
            let value = &values[(write % values.len() as u64) as usize];
            engine.put(&key(index), value).unwrap();
            model.put(index);
        }
        if engine.stats().flushes != flushes {
            flushes = engine.stats().flushes;
            refusals += u64::from(settle(&engine, &mut workspace).is_some());
            *overlaps.entry(engine.stats().max_overlap).or_default() += 1;
            if marks
                .peek()
                .is_some_and(|&mark| engine.stats().runs >= mark)
            {
                marks.next();
                engine.flush().unwrap();
                settle(&engine, &mut workspace);
                measure(&format!("{label} @{write}"), &engine, &model, probe_count);
            }
        }
    }
    engine.flush().unwrap();
    settle(&engine, &mut workspace);
    measure(&format!("{label} end"), &engine, &model, probe_count);
    println!(
        "OVERLAP | {label} | after each of {flushes} flushes and its compactions, max_overlap: {overlaps:?}; refusals {refusals}"
    );
    engine.close().unwrap();
}

/// The 5AO layouts: two write phases flushed into exactly `runs` runs with
/// no compaction, in key order (disjoint) or a fixed permutation
/// (overlapping).
fn synthetic(overlapping: bool, runs: u64, probe_count: usize) {
    const KEYS: u64 = 200_000;
    let engine = Engine::new(EngineOptions {
        memtable_budget: 1 << 30,
        ..scaled(64 << 20)
    })
    .unwrap();
    let values = values();
    let order = |j: u64| if overlapping { j * STRIDE % KEYS } else { j };
    let updates: Vec<u64> = (0..KEYS)
        .map(order)
        .filter(|index| index % 10 == 1 || index % 10 == 2)
        .collect();
    let writes = KEYS + updates.len() as u64;
    let mut model = Model::new(KEYS);
    let (mut written, mut flushed) = (0u64, 0u64);
    let mut wrote = |engine: &Engine| {
        written += 1;
        if written * runs >= (flushed + 1) * writes {
            engine.flush().unwrap();
            flushed += 1;
        }
    };
    for j in 0..KEYS {
        let index = order(j);
        engine
            .put(&key(index), &values[(index % 1024) as usize])
            .unwrap();
        model.put(index);
        wrote(&engine);
    }
    for &index in &updates {
        if index % 10 == 1 {
            engine
                .put(&key(index), &values[((index + 1) % 1024) as usize])
                .unwrap();
            model.put(index);
        } else {
            engine.delete(&key(index)).unwrap();
            model.delete(index);
        }
        wrote(&engine);
    }
    assert_eq!(engine.stats().runs as u64, runs);
    let label = if overlapping {
        "overlapping"
    } else {
        "disjoint"
    };
    measure(label, &engine, &model, probe_count);
    engine.close().unwrap();
}

#[test]
#[ignore = "research: run with --release --ignored --nocapture"]
fn grouped_directory_against_the_flat_lookup_order() {
    let probe_count = env_or("DIRECTORY_PROBES", 2_000);
    let counts: Vec<u64> = std::env::var("DIRECTORY_COUNTS")
        .unwrap_or_else(|_| "4,64,256,1024,2048".into())
        .split(',')
        .map(|count| count.parse().unwrap())
        .collect();
    let scale: u64 = env_or("DIRECTORY_SCALE", 300);
    print_headers();
    for overlapping in [false, true] {
        for &runs in &counts {
            synthetic(overlapping, runs, probe_count);
        }
    }
    for kind in [History::Ascending, History::Uniform, History::Hot] {
        history(kind, scale * 1_000, probe_count);
    }
}

// Capacity: where the default run limit stops writes, at a reduced scale.

#[test]
#[ignore = "research: run with --release --ignored --nocapture"]
fn the_run_limit_stops_writes_that_the_selector_cannot_relieve() {
    let run_len = 256 << 10;
    let values = values();
    for kind in [History::Ascending, History::Uniform] {
        let engine = Engine::new(EngineOptions {
            max_runs: EngineOptions::default().max_runs,
            ..scaled(run_len)
        })
        .unwrap();
        let mut workspace = MergeWorkspace::new();
        let mut rng = Rng(11);
        let (mut logical, mut flushes, mut deepest) = (0u64, 0, 0);
        let mut outcome = String::from("no refusal");
        for write in 0..2_000_000u64 {
            let index = match kind {
                History::Ascending => write,
                _ => rng.below(1 << 40),
            };
            let value = &values[(write % 1024) as usize];
            let key = format!("user/{index:013}").into_bytes();
            match engine.put(&key, value) {
                Ok(_) => logical += (key.len() + value.len()) as u64,
                Err(UpdateError::FlushRefused(error)) => {
                    let relief = settle(&engine, &mut workspace);
                    let retry = engine.put(&key, value).is_ok();
                    let state = engine.core.read_state();
                    let selected = select(&candidates(&state.runs), CompactionOptions::default());
                    outcome = format!(
                        "refused after {write} writes, {:.1} MiB logical ({:.1} run lengths): {error}; compaction then: {relief:?}; retry ok: {retry}; selector: {:?}",
                        logical as f64 / f64::from(1 << 20),
                        logical as f64 / run_len as f64,
                        selected.map(|picked| picked.map(|picked| picked.len()))
                    );
                    break;
                }
                Err(error) => panic!("{error}"),
            }
            if engine.stats().flushes != flushes {
                flushes = engine.stats().flushes;
                settle(&engine, &mut workspace);
                deepest = deepest.max(engine.stats().max_overlap);
            }
        }
        let stats = engine.stats();
        println!(
            "CAPACITY | {kind:?} | runs {} | max_overlap now {} (deepest seen after compaction {deepest}) | isolated {} | {outcome}",
            stats.runs,
            stats.max_overlap,
            isolated(&engine.core.read_state().runs),
        );
        engine.close().unwrap();
    }
}

// The overlap allowance across state transitions that refuse compaction.

#[test]
#[ignore = "research: run with --release --ignored --nocapture"]
fn overlap_exceeds_the_allowance_while_compaction_is_refused() {
    let values = values();
    // A held snapshot and a small retention limit: compactions are refused
    // once the snapshot pins enough replaced runs.
    let engine = Engine::new(EngineOptions {
        snapshots: SnapshotLimits {
            max_retained_bytes: 128 << 10,
            ..SnapshotLimits::default()
        },
        ..scaled(64 << 10)
    })
    .unwrap();
    let mut workspace = MergeWorkspace::new();
    let mut rng = Rng(13);
    let mut flushes = 0;
    let mut snapshot = None;
    let mut deepest = 0;
    let mut errors: BTreeMap<String, u64> = BTreeMap::new();
    let mut model = Model::new(20_000);
    for write in 0..60_000u64 {
        if write == 10_000 {
            snapshot = Some(engine.snapshot().unwrap());
        }
        let index = rng.below(20_000);
        engine
            .put(&key(index), &values[(write % 1024) as usize])
            .unwrap();
        model.put(index);
        if engine.stats().flushes != flushes {
            flushes = engine.stats().flushes;
            if let Some(error) = settle(&engine, &mut workspace) {
                let name = format!("{error:?}");
                *errors
                    .entry(name[..name.find([' ', '(', '{']).unwrap_or(name.len())].to_string())
                    .or_default() += 1;
            }
            deepest = deepest.max(engine.stats().max_overlap);
        }
    }
    let held = engine.stats();
    // Reads while the overlap is far above the allowance.
    engine.flush().unwrap();
    let probe_count = env_or("DIRECTORY_PROBES", 2_000);
    print_headers();
    measure("refused, snapshot held", &engine, &model, probe_count);
    drop(snapshot);
    let relief = settle(&engine, &mut workspace);
    println!(
        "TRANSITION | snapshot held, 128 KiB retention | deepest overlap after compaction {deepest} (allowance 4), {} runs, refusals {errors:?} | after release: overlap {}, runs {}, {relief:?}",
        held.runs,
        engine.stats().max_overlap,
        engine.stats().runs,
    );
    engine.close().unwrap();
}
