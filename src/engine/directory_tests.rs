//! Research (5AP): could the published runs be indexed as groups of mutually
//! disjoint key ranges, without changing how runs are built or compacted?
//!
//! Everything here is experimental and test-only. The engine's lookup path,
//! publication and compaction are unchanged: the directory is built beside
//! them from the published lookup order, and its lookups are compared with
//! the engine's own (`latest_in` over every run, as `Engine::get` reads the
//! runs) on the same runs, in the same process, alternating.
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
use std::collections::{BTreeMap, BinaryHeap};
use std::time::Instant;

use super::*;
use crate::run::{CorruptRun, KeyRange, LookupInput, ReadWorkspace, RunBuilder, RunCursor};

// The directory.

/// One run as the directory holds it: its key range, sequence bound and run,
/// all borrowed from the published run.
#[derive(Clone, Copy)]
struct Entry<'a> {
    min: &'a [u8],
    max: &'a [u8],
    max_seq: u64,
    run: &'a Run<Vec<u8>>,
}

/// Runs whose inclusive key ranges are pairwise disjoint, in key order.
struct Group<'a> {
    entries: Vec<Entry<'a>>,
    /// The largest sequence bound of any member.
    max_seq: u64,
}

/// The published runs partitioned into groups, ordered by their sequence
/// bounds, largest first.
struct Directory<'a> {
    groups: Vec<Group<'a>>,
    /// Peak temporary bytes the build held: its sort order and heap.
    build_bytes: usize,
}

/// Whether a run starting at `min` may follow one ending at `last` in a
/// group. Ranges include both bounds, so equal endpoints overlap; `touching`
/// admits them, for measuring what that would save, never for lookups.
fn follows(last: &[u8], min: &[u8], touching: bool) -> bool {
    if touching { last <= min } else { last < min }
}

impl<'a> Directory<'a> {
    /// Interval partitioning: runs in order of their smallest key (then
    /// largest key, then position), each into the group whose last run ends
    /// earliest if it ends before the run starts, else into a new group. This
    /// is deterministic and uses the fewest groups possible, which is the
    /// deepest overlap.
    fn build(runs: &'a [SharedRun], touching: bool) -> Self {
        let entries: Vec<Entry<'a>> = runs
            .iter()
            .map(|published| {
                let range = published.run.key_range().expect(NOT_EMPTY);
                Entry {
                    min: range.min,
                    max: range.max,
                    max_seq: published.run.max_seq(),
                    run: &published.run,
                }
            })
            .collect();
        let mut order: Vec<usize> = (0..entries.len()).collect();
        order.sort_by(|&a, &b| {
            let (x, y) = (&entries[a], &entries[b]);
            (x.min, x.max, a).cmp(&(y.min, y.max, b))
        });
        let mut ends: BinaryHeap<Reverse<(&[u8], usize)>> = BinaryHeap::new();
        let mut groups: Vec<Group<'a>> = Vec::new();
        for &index in &order {
            let entry = entries[index];
            let group = match ends.peek() {
                Some(&Reverse((last, group))) if follows(last, entry.min, touching) => {
                    ends.pop();
                    group
                }
                _ => {
                    groups.push(Group {
                        entries: Vec::new(),
                        max_seq: 0,
                    });
                    groups.len() - 1
                }
            };
            groups[group].entries.push(entry);
            groups[group].max_seq = groups[group].max_seq.max(entry.max_seq);
            ends.push(Reverse((entry.max, group)));
        }
        let build_bytes = entries.capacity() * size_of::<Entry<'_>>()
            + order.capacity() * size_of::<usize>()
            + ends.capacity() * size_of::<Reverse<(&[u8], usize)>>();
        for group in &mut groups {
            group.entries.shrink_to_fit();
        }
        groups.sort_by_key(|group| (Reverse(group.max_seq), group.entries[0].min));
        Self {
            groups,
            build_bytes,
        }
    }

    /// The bytes the directory keeps: groups and entries, not the runs.
    fn bytes(&self) -> usize {
        self.groups.capacity() * size_of::<Group<'_>>()
            + self
                .groups
                .iter()
                .map(|group| group.entries.capacity() * size_of::<Entry<'_>>())
                .sum::<usize>()
    }

    /// Collects the run of each group whose range holds `key`, at most one a
    /// group, as (sequence bound, group, position). `exact` orders them by
    /// their own sequence bounds, as the engine visits runs; otherwise they
    /// stay in group order, which is by the groups' coarser bounds.
    fn candidates(&self, key: &[u8], exact: bool, out: &mut Vec<(u64, u32, u32)>) {
        out.clear();
        for (index, group) in self.groups.iter().enumerate() {
            let position = group.entries.partition_point(|entry| entry.max < key);
            if let Some(entry) = group.entries.get(position)
                && entry.min <= key
            {
                out.push((entry.max_seq, index as u32, position as u32));
            }
        }
        if exact {
            out.sort_unstable_by_key(|&(max_seq, ..)| Reverse(max_seq));
        }
    }

    fn entry(&self, (_, group, position): (u64, u32, u32)) -> Entry<'a> {
        self.groups[group as usize].entries[position as usize]
    }

    /// Point lookup through the directory: the candidates, read by the
    /// engine's own lookup routine.
    fn get<'r>(
        &self,
        key: &[u8],
        exact: bool,
        candidates: &mut Vec<(u64, u32, u32)>,
        logical: &'r mut LogicalWorkspace,
    ) -> Result<Option<LiveValue<'r>>, ReadError>
    where
        'a: 'r,
    {
        self.candidates(key, exact, candidates);
        latest_in(
            candidates
                .iter()
                .map(|&candidate| self.entry(candidate).run),
            key,
            logical,
        )
    }
}

/// The engine's lookup over the runs, without the memtables.
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

/// Checks the directory against brute force: every run in exactly one group,
/// each group sorted and strictly disjoint, the fewest groups, and for each
/// probe exactly the runs whose ranges hold it.
fn check_directory(runs: &[SharedRun], directory: &Directory<'_>, probes: &[Probes]) {
    let mut seen: Vec<*const Run<Vec<u8>>> = Vec::new();
    for group in &directory.groups {
        for pair in group.entries.windows(2) {
            assert!(pair[0].max < pair[1].min, "a group's ranges overlap");
        }
        assert_eq!(
            group.max_seq,
            group.entries.iter().map(|e| e.max_seq).max().unwrap()
        );
        seen.extend(group.entries.iter().map(|entry| entry.run as *const _));
    }
    let mut published: Vec<*const Run<Vec<u8>>> =
        runs.iter().map(|run| &run.run as *const _).collect();
    seen.sort();
    published.sort();
    assert_eq!(seen, published, "every run once");
    assert_eq!(
        directory.groups.len(),
        deepest_overlap(runs),
        "fewest groups"
    );
    let mut candidates = Vec::new();
    for probe in probes.iter().flat_map(|class| &class.keys).step_by(7) {
        directory.candidates(probe, true, &mut candidates);
        let mut found: Vec<*const Run<Vec<u8>>> = candidates
            .iter()
            .map(|&candidate| directory.entry(candidate).run as *const _)
            .collect();
        let mut want: Vec<*const Run<Vec<u8>>> = runs
            .iter()
            .filter(|run| {
                let range = run.run.key_range().expect(NOT_EMPTY);
                range.min <= probe.as_slice() && probe.as_slice() <= range.max
            })
            .map(|run| &run.run as *const _)
            .collect();
        found.sort();
        want.sort();
        assert_eq!(found, want, "candidates of {probe:02x?}");
    }
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

/// Measures one published state: topology, directory costs, and lookups
/// through both paths, with their results checked against each other.
fn measure(label: &str, engine: &Engine, model: &Model, probe_count: usize) {
    // Nothing writes meanwhile, so the two reads see one state.
    let runs = Arc::clone(&engine.core.read_state().lookup_order);
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
        std::hint::black_box(Directory::build(&runs, false));
    });
    let order_us = best(&mut || {
        let mut order: Vec<SharedRun> = runs.to_vec();
        order.sort_by_key(|run| Reverse(run.run.max_seq()));
        std::hint::black_box(Arc::<[SharedRun]>::from(order));
    });
    let select_us = best(&mut || {
        let _ = std::hint::black_box(select(&candidates(&runs), CompactionOptions::default()));
    });
    let directory = Directory::build(&runs, false);
    let touching = Directory::build(&runs, true);
    check_directory(&runs, &directory, &probes);
    let sizes: Vec<usize> = directory.groups.iter().map(|g| g.entries.len()).collect();

    println!(
        "TOPOLOGY | {label} | {} | {} | {} | {} | {} | {:.1} | {} | {} | {:.0} | {:.0} | {:.0} | {} | {} | {} |",
        runs.len(),
        stats.max_overlap,
        isolated(&runs),
        directory.groups.len(),
        sizes.iter().max().unwrap_or(&0),
        runs.len() as f64 / directory.groups.len().max(1) as f64,
        touching.groups.len(),
        touching_pairs(&runs),
        build_us,
        order_us,
        select_us,
        directory.bytes(),
        directory.build_bytes,
        runs.len() * size_of::<SharedRun>(),
    );

    let mut flat_ws = LogicalWorkspace::default();
    let mut grouped_ws = LogicalWorkspace::default();
    let mut coarse_ws = LogicalWorkspace::default();
    let mut candidates = Vec::with_capacity(directory.groups.len());
    for class in &probes {
        // Correctness: the three paths agree on every probe, and the exact
        // order decodes exactly the blocks the engine's order does.
        let counts = Cell::new(Counts::default());
        let grouped_counts = Cell::new(Counts::default());
        let mut blocks = [0u64; 3];
        let mut candidate_total = 0usize;
        let mut candidate_max = 0usize;
        for probe in &class.keys {
            let before = [
                flat_ws.blocks_loaded(),
                grouped_ws.blocks_loaded(),
                coarse_ws.blocks_loaded(),
            ];
            let flat = owned(latest_in(
                runs.iter().map(|run| Counted {
                    run: &run.run,
                    counts: &counts,
                }),
                probe,
                &mut flat_ws,
            ));
            directory.candidates(probe, true, &mut candidates);
            candidate_total += candidates.len();
            candidate_max = candidate_max.max(candidates.len());
            let grouped = owned(latest_in(
                candidates.iter().map(|&candidate| Counted {
                    run: directory.entry(candidate).run,
                    counts: &grouped_counts,
                }),
                probe,
                &mut grouped_ws,
            ));
            let coarse = owned(directory.get(probe, false, &mut candidates, &mut coarse_ws));
            assert_eq!(flat, grouped, "{label} {} {probe:02x?}", class.name);
            assert_eq!(flat, coarse, "{label} {} {probe:02x?}", class.name);
            assert_eq!(
                flat.as_ref().unwrap().is_some(),
                class.found,
                "{label} {}",
                class.name
            );
            for (total, (ws, before)) in blocks
                .iter_mut()
                .zip([&flat_ws, &grouped_ws, &coarse_ws].into_iter().zip(before))
            {
                *total += ws.blocks_loaded() - before;
            }
        }
        assert_eq!(
            blocks[0], blocks[1],
            "{label} {}: exact order decodes alike",
            class.name
        );
        let warm = candidates.capacity();

        // Timing: the engine's path and the directory's, alternating.
        let n = class.keys.len() as f64;
        let mut flat_samples = Vec::new();
        let mut grouped_samples = Vec::new();
        let mut coarse_samples = Vec::new();
        for _ in 0..2 {
            flat_samples.extend(timed(
                &class.keys,
                |key| flat_get(&runs, key, &mut flat_ws).unwrap().is_some(),
                1,
            ));
            grouped_samples.extend(timed(
                &class.keys,
                |key| {
                    directory
                        .get(key, true, &mut candidates, &mut grouped_ws)
                        .unwrap()
                        .is_some()
                },
                1,
            ));
            coarse_samples.extend(timed(
                &class.keys,
                |key| {
                    directory
                        .get(key, false, &mut candidates, &mut coarse_ws)
                        .unwrap()
                        .is_some()
                },
                1,
            ));
        }
        // Means over whole passes, free of the clock's resolution (about
        // 42 ns here), alternating as above.
        let mut means = [0.0f64; 3];
        for _ in 0..2 {
            let start = Instant::now();
            for key in &class.keys {
                std::hint::black_box(flat_get(&runs, key, &mut flat_ws).unwrap().is_some());
            }
            means[0] += start.elapsed().as_nanos() as f64 / n / 2.0;
            for (slot, exact, ws) in [(1, true, &mut grouped_ws), (2, false, &mut coarse_ws)] {
                let start = Instant::now();
                for key in &class.keys {
                    std::hint::black_box(
                        directory
                            .get(key, exact, &mut candidates, ws)
                            .unwrap()
                            .is_some(),
                    );
                }
                means[slot] += start.elapsed().as_nanos() as f64 / n / 2.0;
            }
        }
        assert_eq!(
            candidates.capacity(),
            warm,
            "the candidate buffer did not grow"
        );
        for samples in [&mut flat_samples, &mut grouped_samples, &mut coarse_samples] {
            samples.sort_by(f64::total_cmp);
        }
        let (flat, grouped) = (counts.get(), grouped_counts.get());
        println!(
            "LOOKUP | {label} | {} | {} | {:.0} / {:.0} / {:.0} | {:.0} / {:.0} / {:.0} | {:.0} / {:.0} / {:.0} | {:.1} | {:.2} / {} | {:.2} / {:.2} | {:.2} / {:.2} | {:.3} / {:.3} / {:.3} |",
            runs.len(),
            class.name,
            means[0],
            percentile(&flat_samples, 0.5),
            percentile(&flat_samples, 0.99),
            means[1],
            percentile(&grouped_samples, 0.5),
            percentile(&grouped_samples, 0.99),
            means[2],
            percentile(&coarse_samples, 0.5),
            percentile(&coarse_samples, 0.99),
            flat.range as f64 / n,
            candidate_total as f64 / n,
            candidate_max,
            flat.filter as f64 / n,
            grouped.filter as f64 / n,
            flat.opened as f64 / n,
            grouped.opened as f64 / n,
            blocks[0] as f64 / n,
            blocks[1] as f64 / n,
            blocks[2] as f64 / n,
        );
    }
}

fn print_headers() {
    println!(
        "TOPOLOGY | history | runs | max_overlap | isolated runs | groups | largest group | mean group | groups if equal ends touched | touching pairs | build µs | lookup order µs | select µs | directory bytes | build temp bytes | lookup order bytes |"
    );
    println!(
        "LOOKUP | history | runs | class | engine mean / p50 / p99 ns | grouped mean / p50 / p99 ns | group-order mean / p50 / p99 ns | engine range checks | candidates mean / max | filter probes engine / grouped | runs opened engine / grouped | blocks engine / grouped / group-order |"
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

// Snapshots: a directory over a snapshot's runs reads what the snapshot does.

#[test]
#[ignore = "research: run with --release --ignored --nocapture"]
fn a_directory_over_a_snapshot_reads_what_the_snapshot_reads() {
    let engine = Engine::new(scaled(16 << 10)).unwrap();
    let values = values();
    let mut workspace = MergeWorkspace::new();
    let mut rng = Rng(5);
    for write in 0..20_000u64 {
        let index = rng.below(4_000);
        engine
            .put(&key(index), &values[(write % 1024) as usize])
            .unwrap();
    }
    engine.flush().unwrap();
    settle(&engine, &mut workspace);
    let snapshot = engine.snapshot().unwrap();
    // Later writes, deletes, flushes and compactions the snapshot must not see.
    for write in 0..20_000u64 {
        let index = rng.below(4_000);
        if write % 3 == 0 {
            engine.delete(&key(index)).unwrap();
        } else {
            engine
                .put(&key(index), &values[(write % 7) as usize])
                .unwrap();
        }
    }
    engine.flush().unwrap();
    settle(&engine, &mut workspace);
    let directory = Directory::build(&snapshot.runs, false);
    let mut engine_ws = EngineWorkspace::new();
    let mut logical = LogicalWorkspace::default();
    let mut candidates = Vec::new();
    let mut differ = 0;
    for index in 0..4_000 {
        for probe in [key(index), missing_key(index)] {
            let want = owned(snapshot.get(&probe, &mut engine_ws));
            let found = owned(directory.get(&probe, true, &mut candidates, &mut logical));
            assert_eq!(found, want, "{probe:02x?}");
            let now = owned(engine.get(&probe, &mut engine_ws));
            differ += usize::from(now != want);
        }
    }
    assert!(differ > 0, "the later writes changed what the engine sees");
    println!(
        "snapshot: {} runs in {} groups; {differ} keys differ from the live engine",
        snapshot.runs.len(),
        directory.groups.len()
    );
}

// Scans: a concatenating cursor over one group's runs.

/// A cursor over runs with pairwise disjoint key ranges, in key order, that
/// reads them one after another through a single workspace: one decoded
/// block and one decompression context however many runs it crosses.
struct Concat<'r> {
    runs: &'r [Run<Vec<u8>>],
    index: usize,
    cursor: Option<RunCursor<'r>>,
    spare: Option<&'r mut ReadWorkspace>,
    prefix: Option<Vec<u8>>,
}

impl<'r> Concat<'r> {
    fn new(runs: &'r [Run<Vec<u8>>], workspace: &'r mut ReadWorkspace) -> Self {
        let mut concat = Self {
            runs,
            index: 0,
            cursor: None,
            spare: Some(workspace),
            prefix: None,
        };
        concat.retarget(0);
        concat
    }

    /// Moves the workspace to the run at `index`, positioned before its first
    /// record, or parks it past the last run.
    fn retarget(&mut self, index: usize) {
        let workspace = match self.cursor.take() {
            Some(cursor) => cursor.into_workspace(),
            None => self.spare.take().expect("the workspace is parked"),
        };
        self.index = index;
        match self.runs.get(index) {
            Some(run) => self.cursor = Some(run.cursor(workspace)),
            None => self.spare = Some(workspace),
        }
    }

    /// Binary-searches for the first run that can hold `target`.
    fn seek(&mut self, target: &[u8]) -> Result<(), CorruptRun> {
        self.prefix = None;
        let index = self
            .runs
            .partition_point(|run| run.key_range().expect(NOT_EMPTY).max < target);
        self.retarget(index);
        self.cursor
            .as_mut()
            .map_or(Ok(()), |cursor| cursor.seek(target))
    }

    fn seek_prefix(&mut self, prefix: &[u8]) -> Result<(), CorruptRun> {
        let index = self
            .runs
            .partition_point(|run| run.key_range().expect(NOT_EMPTY).max < prefix);
        self.prefix = Some(prefix.to_vec());
        self.retarget(index);
        self.cursor
            .as_mut()
            .map_or(Ok(()), |cursor| cursor.seek_prefix(prefix))
    }

    fn next_record(&mut self) -> Result<Option<Record<'_>>, CorruptRun> {
        loop {
            let Some(cursor) = self.cursor.as_mut() else {
                return Ok(None);
            };
            if cursor.peek()?.is_some() {
                break;
            }
            // This run is done. The next one starts above every key so far,
            // so under a prefix it continues the scan only if its first key
            // has the prefix.
            let Some(next) = self.runs.get(self.index + 1) else {
                return Ok(None);
            };
            if let Some(prefix) = &self.prefix
                && !next.key_range().expect(NOT_EMPTY).min.starts_with(prefix)
            {
                return Ok(None);
            }
            self.retarget(self.index + 1);
            if let Some(prefix) = self.prefix.clone() {
                self.cursor
                    .as_mut()
                    .expect("retargeted")
                    .seek_prefix(&prefix)?;
            }
        }
        self.cursor.as_mut().expect("peeked").next_record()
    }
}

type Owned = (Vec<u8>, u64, Option<Vec<u8>>);

/// A record's payload in the model: bytes, or `None` for a tombstone.
type Payload = Option<Vec<u8>>;

fn own(record: Record<'_>) -> Owned {
    let value = match record.value {
        Value::Bytes(value) => Some(value.to_vec()),
        Value::Tombstone => None,
    };
    (record.key.to_vec(), record.seq, value)
}

#[test]
#[ignore = "research: run with --release --ignored --nocapture"]
fn a_concatenating_cursor_reads_a_group_like_its_records_in_order() {
    const ALPHABET: [u8; 5] = [0x00, 0x01, b'a', 0xFE, 0xFF];
    for seed in 0..40 {
        let mut rng = Rng(900 + seed);
        let mut records: BTreeMap<(Vec<u8>, Reverse<u64>), Payload> = BTreeMap::new();
        while records.len() < 600 {
            let key: Vec<u8> = (0..rng.below(6))
                .map(|_| ALPHABET[rng.below(5) as usize])
                .collect();
            for _ in 0..=rng.below(3) {
                let seq = rng.below(1_000);
                let value = (rng.below(5) != 0).then(|| vec![seq as u8; rng.below(40) as usize]);
                records.insert((key.clone(), Reverse(seq)), value);
            }
        }
        let all: Vec<Owned> = records
            .into_iter()
            .map(|((key, Reverse(seq)), value)| (key, seq, value))
            .collect();
        // Cut between user keys, so that ranges are strictly disjoint.
        let mut runs = Vec::new();
        let mut start = 0;
        while start < all.len() {
            let mut end = (start + 1 + rng.below(120) as usize).min(all.len());
            while end < all.len() && all[end].0 == all[end - 1].0 {
                end += 1;
            }
            let codec = [Codec::None, Codec::Lz4, Codec::Zstd][rng.below(3) as usize];
            let mut builder = RunBuilder::with_target_block_len(codec, 256);
            for (key, seq, value) in &all[start..end] {
                let value = value.as_deref().map_or(Value::Tombstone, Value::Bytes);
                builder.add(key, *seq, value).unwrap();
            }
            runs.push(builder.finish());
            start = end;
        }
        let mut workspace = ReadWorkspace::new();
        let mut concat = Concat::new(&runs, &mut workspace);
        let mut probes: Vec<Vec<u8>> = runs
            .iter()
            .flat_map(|run| {
                let range = run.key_range().unwrap();
                [range.min.to_vec(), range.max.to_vec()]
            })
            .collect();
        probes.extend((0..60).map(|_| {
            (0..rng.below(5))
                .map(|_| ALPHABET[rng.below(5) as usize])
                .collect::<Vec<u8>>()
        }));
        probes.push(Vec::new());
        probes.push(vec![0xFF]);
        for probe in &probes {
            for prefix in [false, true] {
                let want: Vec<&Owned> = all
                    .iter()
                    .filter(|record| {
                        if prefix {
                            record.0.starts_with(probe)
                        } else {
                            record.0 >= *probe
                        }
                    })
                    .collect();
                if prefix {
                    concat.seek_prefix(probe).unwrap();
                } else {
                    concat.seek(probe).unwrap();
                }
                let mut got = Vec::new();
                while let Some(record) = concat.next_record().unwrap() {
                    got.push(own(record));
                }
                let got: Vec<&Owned> = got.iter().collect();
                assert_eq!(got, want, "seed {seed} probe {probe:02x?} prefix {prefix}");
            }
        }
        drop(concat);
        if seed == 0 {
            let mut merge = MergeWorkspace::new();
            let views: Vec<RunView<'_>> = runs.iter().map(Run::view).collect();
            let mut cursor = crate::run::MergeCursor::new(views.iter().copied(), &mut merge);
            while cursor.next_record().unwrap().is_some() {}
            drop(cursor);
            println!(
                "concat over {} runs: {} decoded bytes, {} context bytes; a merge over them: {} and {}",
                runs.len(),
                workspace.capacity(),
                workspace.context_bytes(),
                merge.capacity(),
                merge.context_bytes()
            );
        }
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
    for write in 0..60_000u64 {
        if write == 10_000 {
            snapshot = Some(engine.snapshot().unwrap());
        }
        let index = rng.below(20_000);
        engine
            .put(&key(index), &values[(write % 1024) as usize])
            .unwrap();
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
