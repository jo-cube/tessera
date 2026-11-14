//! Research (5AR): what the current compaction policy does once the run
//! count no longer limits capacity, a model of it and of leveled
//! alternatives, directory and selection costs at thousands of runs, and a
//! prototype of range-local replacement. `docs/capacity-design.md` draws on
//! them.
//!
//! Everything is at reduced scale: 64 KiB memtables and runs instead of
//! 64 MiB, with the compaction byte limits scaled by the same factor (input
//! and output 4 run lengths, as 256 MiB is to 64 MiB), no run limit, and
//! compaction run to completion after every flush, as the worker would. Each
//! history reports, every time its flush count doubles: runs, overlap,
//! stored bytes, bytes written by flushes and by compactions, and the time
//! one selection takes.
//!
//! The tests are ignored. Run them with a release build:
//!
//! ```text
//! scripts/validate.sh exec 3000 cargo test --locked --release --lib \
//!   engine::capacity_tests -- --ignored --nocapture --test-threads=1
//! ```
//!
//! `CAPACITY_WRITES` (thousands of writes per history) changes the size.

use std::collections::BTreeMap;
use std::time::Instant;

use super::*;

const RUN_LEN: usize = 64 << 10;
const STRIDE: u64 = 104_729;

fn engine_options() -> EngineOptions {
    EngineOptions {
        memtable_budget: RUN_LEN,
        target_run_len: RUN_LEN,
        max_runs: 1 << 20,
        max_run_bytes: usize::MAX / 4,
        background_compaction: None,
        ..EngineOptions::default()
    }
}

/// The defaults with their byte limits scaled as the runs are.
pub(super) fn compaction_options() -> CompactionOptions {
    CompactionOptions {
        max_input_bytes: 4 * RUN_LEN,
        max_output_bytes: 4 * RUN_LEN,
        ..CompactionOptions::default()
    }
}

fn key(index: u64) -> Vec<u8> {
    format!("user/{index:010}").into_bytes()
}

/// [`value`], for other research modules.
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

#[derive(Clone, Copy, Debug)]
pub(super) enum Workload {
    /// New keys in ascending order.
    Ascending,
    /// New keys in a fixed permutation: the database grows and every flush
    /// spans the whole key space.
    UniformInsert,
    /// `keys` loaded in order, then uniform overwrites: a fixed size.
    UniformUpdate,
    /// `keys` loaded in order, then overwrites within a hot 2%.
    Hot,
}

/// The `write`th key a workload writes over `keys` keys.
pub(super) fn index(workload: Workload, write: u64, keys: u64, rng: &mut Rng) -> u64 {
    match workload {
        Workload::Ascending => write,
        Workload::UniformInsert => write * STRIDE % keys,
        Workload::UniformUpdate | Workload::Hot if write < keys => write,
        Workload::UniformUpdate => rng.below(keys),
        Workload::Hot => rng.below(keys / 50),
    }
}

#[derive(Default)]
struct Totals {
    compactions: u64,
    input_bytes: u64,
    output_bytes: u64,
    refusals: BTreeMap<String, u64>,
}

fn settle(engine: &Engine, workspace: &mut MergeWorkspace, totals: &mut Totals) {
    loop {
        match engine.compact(compaction_options(), workspace) {
            Ok(Some(report)) => {
                totals.compactions += 1;
                totals.input_bytes += report.input_bytes as u64;
                totals.output_bytes += report.output_bytes as u64;
            }
            Ok(None) => return,
            Err(error) => {
                let name = format!("{error:?}");
                let name = name[..name.find([' ', '(', '{']).unwrap_or(name.len())].to_string();
                *totals.refusals.entry(name).or_default() += 1;
                return;
            }
        }
    }
}

fn report(label: &str, engine: &Engine, totals: &Totals, logical: u64) {
    let stats = engine.stats();
    let (select_us, groups) = {
        let state = engine.core.read_state();
        let start = Instant::now();
        let picked = select(&candidates(&state.runs), compaction_options());
        let elapsed = start.elapsed().as_secs_f64() * 1e6;
        std::hint::black_box(picked.ok());
        (elapsed, state.directory.group_count())
    };
    let flushed = stats.flushed_bytes;
    println!(
        "CAPACITY | {label} | {} | {} | {} | {} | {:.1} | {:.1} | {:.2} | {:.2} | {} | {:.0} | {:?} |",
        stats.flushes,
        stats.runs,
        groups,
        stats.max_overlap,
        logical as f64 / (1 << 20) as f64,
        stats.run_bytes as f64 / (1 << 20) as f64,
        totals.output_bytes as f64 / flushed.max(1) as f64,
        (flushed + totals.output_bytes) as f64 / flushed.max(1) as f64,
        totals.compactions,
        select_us,
        totals.refusals,
    );
}

fn run(workload: Workload, writes: u64, keys: u64) {
    let engine = Engine::new(engine_options()).unwrap();
    let mut workspace = MergeWorkspace::new();
    let mut totals = Totals::default();
    let mut rng = Rng(11);
    let mut flushes = 0;
    let mut mark = 16;
    let mut logical = 0u64;
    let label = format!("{workload:?}");
    for write in 0..writes {
        let index = index(workload, write, keys, &mut rng);
        let key = key(index);
        let value = value(write);
        logical += (key.len() + value.len()) as u64;
        engine.put(&key, &value).unwrap();
        let now = engine.stats().flushes;
        if now != flushes {
            flushes = now;
            settle(&engine, &mut workspace, &mut totals);
            if flushes >= mark {
                mark *= 2;
                report(&label, &engine, &totals, logical);
            }
        }
    }
    engine.flush().unwrap();
    settle(&engine, &mut workspace, &mut totals);
    report(&format!("{label} end"), &engine, &totals, logical);
    engine.close().unwrap();
}

#[test]
#[ignore = "research: run with --release --ignored --nocapture"]
fn write_amplification_without_a_run_limit() {
    let writes: u64 = std::env::var("CAPACITY_WRITES")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(1_000)
        * 1_000;
    println!(
        "CAPACITY | history | flushes | runs | groups | max_overlap | logical MiB written | stored MiB | compaction bytes / flushed | write amplification | compactions | select µs | refusals |"
    );
    run(Workload::Ascending, writes, writes);
    run(Workload::UniformInsert, writes, writes);
    run(Workload::UniformUpdate, writes, writes / 2);
    run(Workload::Hot, writes, writes / 2);
}

// A metadata model: runs as key ranges with record counts, no data.
//
// Keys are positions in [0, 1). A run holds `keys` records spread evenly
// over [lo, hi]. Where several runs cover an interval, their union is
// estimated from their densities: under uniform updates of `universe` keys
// each run covers a fraction of the keys there, independently; under
// inserts every record is a distinct key. Output is cut into runs of
// `target` records, the last a tail, as compaction and flushes cut theirs.
// Sizes are in records; the selector sees 100 bytes a record.

mod model {
    use super::super::{Candidate, CompactionOptions, select};

    /// Records spread evenly over [lo, hi].
    #[derive(Clone, Copy, Debug)]
    pub struct Piece {
        lo: f64,
        hi: f64,
        keys: f64,
    }

    impl Piece {
        fn mass(&self, a: f64, b: f64) -> f64 {
            let (a, b) = (a.max(self.lo), b.min(self.hi));
            if b <= a {
                0.0
            } else {
                self.keys * (b - a) / (self.hi - self.lo).max(1e-15)
            }
        }
    }

    /// A run: its range, its records, and how they are spread, as pieces of
    /// even density, so that merges keep where records are.
    #[derive(Clone, Debug)]
    pub struct SimRun {
        pub lo: f64,
        pub hi: f64,
        pub keys: f64,
        pub tail: bool,
        pieces: Vec<Piece>,
    }

    impl SimRun {
        pub fn uniform(lo: f64, hi: f64, keys: f64, tail: bool) -> Self {
            Self {
                lo,
                hi,
                keys,
                tail,
                pieces: vec![Piece { lo, hi, keys }],
            }
        }

        /// Records in [a, b].
        pub fn mass(&self, a: f64, b: f64) -> f64 {
            if b <= self.lo || a >= self.hi {
                return 0.0;
            }
            self.pieces.iter().map(|piece| piece.mass(a, b)).sum()
        }

        /// The part of the run in [a, b], if it holds records.
        pub fn clip(&self, a: f64, b: f64) -> Option<Self> {
            let (lo, hi) = (a.max(self.lo), b.min(self.hi));
            let pieces: Vec<Piece> = self
                .pieces
                .iter()
                .filter_map(|piece| {
                    let keys = piece.mass(lo, hi);
                    (keys > 1e-9).then(|| Piece {
                        lo: piece.lo.max(lo),
                        hi: piece.hi.min(hi),
                        keys,
                    })
                })
                .collect();
            let keys: f64 = pieces.iter().map(|piece| piece.keys).sum();
            (hi > lo && keys > 1e-9).then_some(Self {
                lo,
                hi,
                keys,
                tail: self.tail,
                pieces,
            })
        }

        fn bounds(&self) -> impl Iterator<Item = f64> + '_ {
            self.pieces.iter().flat_map(|piece| [piece.lo, piece.hi])
        }
    }

    /// Appends a piece, joining it to the last if their densities are within
    /// 2%, which keeps runs from fragmenting into ever more pieces.
    fn push_piece(pieces: &mut Vec<Piece>, piece: Piece) {
        if piece.keys <= 1e-12 || piece.hi <= piece.lo {
            return;
        }
        if let Some(last) = pieces.last_mut() {
            let density = |piece: &Piece| piece.keys / (piece.hi - piece.lo).max(1e-15);
            let (a, b) = (density(last), density(&piece));
            if (a - b).abs() <= 0.02 * a.max(b) && (last.hi - piece.lo).abs() < 1e-12 {
                last.hi = piece.hi;
                last.keys += piece.keys;
                return;
            }
        }
        pieces.push(piece);
    }

    #[derive(Clone, Copy, Debug)]
    pub enum Keys {
        /// Every write a new key.
        Insert,
        /// Writes over this many keys.
        Update(f64),
    }

    /// The union of `runs` over [a, b], which must lie where they are
    /// piecewise uniform.
    fn union(runs: &[SimRun], a: f64, b: f64, keys: Keys) -> f64 {
        match keys {
            Keys::Insert => runs.iter().map(|run| run.mass(a, b)).sum(),
            Keys::Update(universe) => {
                let room = universe * (b - a);
                if room <= 0.0 {
                    return 0.0;
                }
                let absent: f64 = runs
                    .iter()
                    .map(|run| 1.0 - (run.mass(a, b) / room).min(1.0))
                    .product();
                room * (1.0 - absent)
            }
        }
    }

    /// Merges `inputs` over [from, to] into runs of `target` records, the
    /// last a tail. Returns them and the records written.
    pub fn merge(
        inputs: &[SimRun],
        from: f64,
        to: f64,
        target: f64,
        keys: Keys,
    ) -> (Vec<SimRun>, f64) {
        let mut points: Vec<f64> = inputs
            .iter()
            .flat_map(SimRun::bounds)
            .chain([from, to])
            .filter(|&point| (from..=to).contains(&point))
            .collect();
        points.sort_by(f64::total_cmp);
        points.dedup();
        let mut out = Vec::new();
        let (mut start, mut filled, mut total) = (from, 0.0, 0.0);
        let mut pieces = Vec::new();
        for pair in points.windows(2) {
            let (a, b) = (pair[0], pair[1]);
            let mass = union(inputs, a, b, keys);
            total += mass;
            let mut at = a;
            let mut left = mass;
            while filled + left >= target && left > 0.0 {
                let take = target - filled;
                let cut = at + (b - at) * take / left;
                push_piece(
                    &mut pieces,
                    Piece {
                        lo: at,
                        hi: cut,
                        keys: take,
                    },
                );
                out.push(SimRun {
                    lo: start,
                    hi: cut,
                    keys: target,
                    tail: false,
                    pieces: std::mem::take(&mut pieces),
                });
                left -= take;
                at = cut;
                start = cut;
                filled = 0.0;
            }
            push_piece(
                &mut pieces,
                Piece {
                    lo: at,
                    hi: b,
                    keys: left,
                },
            );
            filled += left;
        }
        if filled > 1e-9 {
            out.push(SimRun {
                lo: start,
                hi: to,
                keys: filled,
                tail: true,
                pieces,
            });
        } else if let Some(last) = out.last_mut() {
            last.tail = true;
        }
        (out, total)
    }

    /// The engine's selector over the modelled runs.
    pub fn select_runs(runs: &[SimRun], options: CompactionOptions) -> Option<Vec<usize>> {
        let bounds: Vec<([u8; 8], [u8; 8])> = runs
            .iter()
            // Runs that abut share no key: a run ends one position before
            // the next starts.
            .map(|run| {
                let min = u64::from_be_bytes(encode(run.lo));
                let max = u64::from_be_bytes(encode(run.hi))
                    .saturating_sub(1)
                    .max(min);
                (min.to_be_bytes(), max.to_be_bytes())
            })
            .collect();
        let candidates: Vec<Candidate<'_>> = runs
            .iter()
            .zip(&bounds)
            .map(|(run, (min, max))| Candidate {
                min,
                max,
                bytes: (run.keys * 100.0) as usize,
                tail: run.tail,
            })
            .collect();
        select(&candidates, options).ok().flatten()
    }

    fn encode(position: f64) -> [u8; 8] {
        ((position.clamp(0.0, 1.0) * (1u64 << 62) as f64) as u64).to_be_bytes()
    }

    /// The deepest overlap, with runs that abut not overlapping.
    pub fn depth<'a>(runs: impl Iterator<Item = &'a SimRun>) -> usize {
        let mut points: Vec<(f64, i32)> =
            runs.flat_map(|run| [(run.lo, -1), (run.hi, 1)]).collect();
        points.sort_by(|a, b| a.0.total_cmp(&b.0).then(b.1.cmp(&a.1)));
        let (mut depth, mut deepest) = (0i32, 0);
        for (_, step) in points {
            depth -= step;
            deepest = deepest.max(depth);
        }
        deepest as usize
    }

    /// What one modelled history wrote.
    #[derive(Debug, Default)]
    pub struct Outcome {
        pub flushed: f64,
        pub written: f64,
        pub jobs: u64,
        /// Runs moved to the next level without being rewritten.
        pub moves: u64,
        pub runs: usize,
        pub depth: usize,
        pub largest_job: f64,
        pub select_us: f64,
        /// Records held at the end.
        pub held: f64,
    }

    impl Outcome {
        pub fn amplification(&self) -> f64 {
            (self.flushed + self.written) / self.flushed
        }
    }

    /// The flushes of a workload: `writes` writes of which the first `load`
    /// are new keys in ascending order, in memtables of `memtable` writes.
    #[derive(Clone, Copy, Debug)]
    pub struct Flushes {
        pub keys: Keys,
        /// Every write a new key, in ascending order.
        pub ascending: bool,
        pub load: f64,
        pub writes: f64,
        pub memtable: f64,
    }

    impl Flushes {
        /// The run the `n`th flush publishes.
        pub fn run(&self, n: f64) -> SimRun {
            let written = n * self.memtable;
            if self.ascending || written < self.load {
                let total = if self.ascending {
                    self.writes
                } else {
                    self.load
                };
                let lo = written / total;
                // Consecutive flushes share no key.
                let hi = (lo + self.memtable / total * (1.0 - 1e-9)).min(1.0);
                return SimRun::uniform(lo, hi, self.memtable, true);
            }
            let keys = match self.keys {
                Keys::Insert => self.memtable,
                Keys::Update(universe) => universe * (1.0 - (-self.memtable / universe).exp()),
            };
            SimRun::uniform(0.0, 1.0, keys, true)
        }

        pub fn count(&self) -> u64 {
            (self.writes / self.memtable) as u64
        }
    }

    /// The current policy: the engine's selector after every flush.
    pub fn current(flushes: Flushes, target: f64, options: CompactionOptions) -> Outcome {
        let mut runs: Vec<SimRun> = Vec::new();
        let mut outcome = Outcome::default();
        for n in 0..flushes.count() {
            let run = flushes.run(n as f64);
            outcome.flushed += run.keys;
            runs.push(run);
            loop {
                let start = std::time::Instant::now();
                let picked = select_runs(&runs, options);
                outcome.select_us = start.elapsed().as_secs_f64() * 1e6;
                let Some(mut picked) = picked else { break };
                let inputs: Vec<SimRun> = picked.iter().map(|&index| runs[index].clone()).collect();
                let from = inputs
                    .iter()
                    .map(|run| run.lo)
                    .fold(f64::INFINITY, f64::min);
                let to = inputs.iter().map(|run| run.hi).fold(0.0, f64::max);
                let input: f64 = inputs.iter().map(|run| run.keys).sum();
                let (output, total) = merge(&inputs, from, to, target, flushes.keys);
                outcome.written += total;
                outcome.jobs += 1;
                outcome.largest_job = outcome.largest_job.max(input);
                picked.sort_unstable();
                for index in picked.into_iter().rev() {
                    runs.remove(index);
                }
                runs.extend(output);
            }
        }
        outcome.runs = runs.len();
        outcome.held = runs.iter().map(|run| run.keys).sum();
        outcome.depth = depth(runs.iter());
        outcome
    }

    /// A leveled policy: flushes enter level 0, which may overlap; every
    /// deeper level is disjoint. Targets are set from the last level, which
    /// has none: each level above it `ratio` times smaller, but at least
    /// `base`, and level 0 `l0` records. Once the first disjoint level's
    /// target would exceed `ratio` times `base`, a new empty level is
    /// inserted above it, so the last level holds most records and few are
    /// shadowed. When a level outgrows its target, a
    /// job merges the key range from that level's cursor holding `job`
    /// records of it and the next level into the next level, which then
    /// covers that range with fresh runs and nothing else. The range is cut
    /// wherever it falls, as a job over whole runs cut at the next level's
    /// boundaries would approximate.
    ///
    /// `job` is in logical records and is not checked against stored-byte
    /// limits: at the measured 28 stored bytes a record, `ratio + 2` runs
    /// fit the scaled limits; incompressible records would not.
    ///
    /// With `split`, a flush is cut at the boundaries of level 1's runs, and
    /// level 0 too is merged a range of `job` records at a time, as level 0
    /// runs that each overlap few runs below allow. Without it, level 0 is
    /// merged whole.
    #[allow(clippy::too_many_arguments)]
    pub fn leveled(
        flushes: Flushes,
        target: f64,
        l0: f64,
        base: f64,
        ratio: f64,
        job: f64,
        split: bool,
    ) -> Outcome {
        let mut levels: Vec<Vec<SimRun>> = vec![Vec::new(), Vec::new()];
        let mut cursors = vec![0.0f64; 2];
        let mut outcome = Outcome::default();
        let mass = |level: &[SimRun]| level.iter().map(|run| run.keys).sum::<f64>();
        let target_of = |levels: &[Vec<SimRun>], level: usize| {
            let last = levels.len() - 1;
            if level == 0 {
                l0
            } else if level == last {
                f64::INFINITY
            } else {
                let bottom = levels[last].iter().map(|run| run.keys).sum::<f64>();
                (bottom / ratio.powi((last - level) as i32)).max(base)
            }
        };
        for n in 0..flushes.count() {
            let run = flushes.run(n as f64);
            outcome.flushed += run.keys;
            if split {
                let mut cuts: Vec<f64> = levels[1]
                    .iter()
                    .map(|below| below.lo)
                    .filter(|&cut| cut > run.lo && cut < run.hi)
                    .chain([run.lo, run.hi])
                    .collect();
                cuts.sort_by(f64::total_cmp);
                cuts.dedup();
                levels[0].extend(
                    cuts.windows(2)
                        .filter_map(|pair| run.clip(pair[0], pair[1])),
                );
            } else {
                levels[0].push(run);
            }
            loop {
                let last = levels.len() - 1;
                let first = mass(&levels[last]) / ratio.powi(last as i32 - 1);
                if first > base * ratio {
                    levels.insert(1, Vec::new());
                    cursors.insert(1, 0.0);
                    continue;
                }
                let (level, score) = (0..last)
                    .map(|level| (level, mass(&levels[level]) / target_of(&levels, level)))
                    .fold(
                        (0, 0.0),
                        |best, next| if next.1 > best.1 { next } else { best },
                    );
                if score <= 1.0 {
                    break;
                }
                // A run that nothing in the next level overlaps moves down
                // without being rewritten; in level 0, only if no older run
                // there overlaps it, so that newer versions stay above.
                let overlaps = |run: &SimRun, below: &[SimRun]| {
                    below
                        .iter()
                        .any(|other| other.lo < run.hi && run.lo < other.hi)
                };
                if let Some(at) = levels[level]
                    .iter()
                    .position(|run| !overlaps(run, &levels[level + 1]))
                    .filter(|&at| level > 0 || !overlaps(&levels[0][at], &levels[0][..at]))
                {
                    let run = levels[level].remove(at);
                    levels[level + 1].push(run);
                    levels[level + 1].sort_by(|a, b| a.lo.total_cmp(&b.lo));
                    outcome.moves += 1;
                    continue;
                }
                // Level 0 is merged whole with what it overlaps below, which
                // its target keeps within a job; deeper levels a range at a
                // time.
                let (from, to) = if level == 0 && !split {
                    let from = levels[0].iter().map(|run| run.lo).fold(1.0, f64::min);
                    let to = levels[0].iter().map(|run| run.hi).fold(0.0, f64::max);
                    (from, to)
                } else {
                    let from = cursors[level];
                    (
                        from,
                        range_end(&levels[level], &levels[level + 1], from, job),
                    )
                };
                let mut inputs = Vec::new();
                for at in [level, level + 1] {
                    let mut kept = Vec::new();
                    for run in levels[at].drain(..) {
                        inputs.extend(run.clip(from, to));
                        kept.extend(run.clip(run.lo, from));
                        kept.extend(run.clip(to, run.hi));
                    }
                    levels[at] = kept;
                }
                let input: f64 = inputs.iter().map(|run| run.keys).sum();
                let (output, total) = merge(&inputs, from, to, target, flushes.keys);
                outcome.written += total;
                outcome.jobs += 1;
                outcome.largest_job = outcome.largest_job.max(input);
                levels[level + 1].extend(output);
                levels[level + 1].sort_by(|a, b| a.lo.total_cmp(&b.lo));
                cursors[level] = if to >= 1.0 { 0.0 } else { to };
            }
        }
        outcome.runs = levels.iter().map(Vec::len).sum();
        outcome.held = levels.iter().flatten().map(|run| run.keys).sum();
        outcome.depth =
            depth(levels[0].iter()) + levels[1..].iter().filter(|level| !level.is_empty()).count();
        outcome
    }

    /// Where a job starting at `from` reaches `job` records of the two levels,
    /// or 1.
    fn range_end(upper: &[SimRun], lower: &[SimRun], from: f64, job: f64) -> f64 {
        let runs: Vec<&SimRun> = upper
            .iter()
            .chain(lower)
            .filter(|run| run.hi > from)
            .collect();
        let mut points: Vec<f64> = runs
            .iter()
            .flat_map(|run| run.bounds())
            .filter(|&point| point > from)
            .chain([from, 1.0])
            .collect();
        points.sort_by(f64::total_cmp);
        points.dedup();
        let mut taken = 0.0;
        for pair in points.windows(2) {
            let (a, b) = (pair[0], pair[1]);
            let mass: f64 = runs.iter().map(|run| run.mass(a, b)).sum();
            if taken + mass >= job && mass > 0.0 {
                return a + (b - a) * (job - taken) / mass;
            }
            taken += mass;
        }
        1.0
    }
}

/// Records per run and per memtable, at the scale of the measurements above:
/// 132 bytes a record counted toward the run length, 64 KiB runs, and
/// flushes of 266 records (54% of a run), as measured. The selector sees 28
/// stored bytes a record, as measured.
const TARGET: f64 = 65_536.0 / 132.0;
const MEMTABLE: f64 = 266.0;

fn selector_options() -> CompactionOptions {
    // The scaled byte limits of `compaction_options`, in the model's stored
    // bytes of 28 a record rather than 100.
    CompactionOptions {
        max_input_bytes: (4.0 * 65_536.0 * 100.0 / 28.0) as usize,
        max_output_bytes: (4.0 * 65_536.0 * 100.0 / 28.0) as usize,
        ..CompactionOptions::default()
    }
}

fn print_outcome(label: &str, size_runs: f64, outcome: &model::Outcome) {
    println!(
        "MODEL | {label} | {size_runs:.0} | {:.2} | {} / {} | {} | {} | {:.1} | {:.0} | {:.0} | {:.2} |",
        outcome.amplification(),
        outcome.jobs,
        outcome.moves,
        outcome.runs,
        outcome.depth,
        outcome.largest_job / TARGET,
        outcome.select_us,
        outcome.held / TARGET,
        outcome.held / TARGET / size_runs,
    );
}

/// The model against the measured histories, with the engine's selector.
#[test]
#[ignore = "research: run with --release --ignored --nocapture"]
fn the_model_reproduces_the_measured_policy() {
    use model::{Flushes, Keys, current};
    let writes = 200_000.0;
    println!(
        "MODEL | history | size in runs | write amplification | jobs / moves | runs | depth | largest job (runs) | last select µs | held (runs) | space amplification |"
    );
    for (label, flushes) in [
        (
            "ascending",
            Flushes {
                keys: Keys::Insert,
                ascending: true,
                load: 0.0,
                writes,
                memtable: MEMTABLE,
            },
        ),
        (
            "uniform insert",
            Flushes {
                keys: Keys::Insert,
                ascending: false,
                load: 0.0,
                writes,
                memtable: MEMTABLE,
            },
        ),
        (
            "uniform update",
            Flushes {
                keys: Keys::Update(writes / 2.0),
                ascending: false,
                load: writes / 2.0,
                writes,
                memtable: MEMTABLE,
            },
        ),
    ] {
        let outcome = current(flushes, TARGET, selector_options());
        print_outcome(&format!("current, {label}"), writes / TARGET, &outcome);
    }
}

/// Projections: the current policy and leveled ones, as the database grows,
/// for uniform updates (a fixed key set loaded in order, then overwritten
/// twice over), uniform inserts and ascending inserts.
#[test]
#[ignore = "research: run with --release --ignored --nocapture"]
fn policies_as_the_database_grows() {
    use model::{Flushes, Keys, current, leveled};
    let largest: f64 = std::env::var("MODEL_RUNS")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(1600.0);
    let current_up_to: f64 = std::env::var("MODEL_CURRENT_RUNS")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(400.0);
    println!(
        "MODEL | policy, workload | size in runs | write amplification | jobs / moves | runs | depth | largest job (runs) | last select µs | held (runs) | space amplification |"
    );
    let mut size = 100.0;
    while size <= largest {
        let keys = size * TARGET;
        let workloads = [
            (
                "uniform update",
                Flushes {
                    keys: Keys::Update(keys),
                    ascending: false,
                    load: keys,
                    writes: 3.0 * keys,
                    memtable: MEMTABLE,
                },
            ),
            (
                "uniform insert",
                Flushes {
                    keys: Keys::Insert,
                    ascending: false,
                    load: 0.0,
                    writes: keys,
                    memtable: MEMTABLE,
                },
            ),
            (
                "ascending",
                Flushes {
                    keys: Keys::Insert,
                    ascending: true,
                    load: 0.0,
                    writes: keys,
                    memtable: MEMTABLE,
                },
            ),
        ];
        for (label, flushes) in workloads {
            if size <= current_up_to {
                let outcome = current(flushes, TARGET, selector_options());
                print_outcome(&format!("current, {label}"), size, &outcome);
            }
            for (ratio, split) in [(4.0, false), (8.0, false), (4.0, true), (8.0, true)] {
                let job = (ratio + 2.0) * TARGET;
                let outcome = leveled(
                    flushes,
                    TARGET,
                    4.0 * MEMTABLE,
                    4.0 * TARGET,
                    ratio,
                    job,
                    split,
                );
                let name = if split { "split" } else { "whole level 0" };
                print_outcome(&format!("leveled {ratio}, {name}, {label}"), size, &outcome);
            }
        }
        size *= 2.0;
    }
}

/// Metadata at scale: `n` small real runs laid out as `levels` disjoint
/// levels (each level's runs tile the key space; each deeper level holds
/// four times as many runs), and the costs that grow with the run count:
/// directory construction and memory, lookups through it, and one selection
/// by the current selector.
#[test]
#[ignore = "research: run with --release --ignored --nocapture"]
fn metadata_and_selection_at_thousands_of_runs() {
    use crate::run::{LogicalWorkspace, RunBuilder, latest_from};
    let counts: Vec<usize> = std::env::var("METADATA_RUNS")
        .unwrap_or_else(|_| "1024,4096,16384,65536".into())
        .split(',')
        .map(|count| count.parse().unwrap())
        .collect();
    let select_up_to: usize = std::env::var("METADATA_SELECT_RUNS")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(16_384);
    println!(
        "METADATA | runs | levels | groups | build µs | directory bytes | run struct bytes | present lookup ns | missing lookup ns | select µs |"
    );
    const SPACE: u64 = 1 << 40;
    for &count in &counts {
        for levels in [1u32, 4] {
            // Runs per level, deepest largest, in proportion 1 : 4 : 16 : 64.
            let weights: Vec<usize> = (0..levels).map(|level| 4usize.pow(level)).collect();
            let total: usize = weights.iter().sum();
            let mut runs: Vec<SharedRun> = Vec::with_capacity(count);
            let mut seq = 0;
            for (level, weight) in weights.iter().enumerate() {
                let here = (count * weight / total).max(1);
                let width = SPACE / here as u64;
                for index in 0..here as u64 {
                    // Two records at the ends of the run's slice, offset per
                    // level so that levels overlap but never share a key.
                    let base = index * width + level as u64;
                    let mut builder = RunBuilder::new(Codec::Zstd);
                    for offset in [0, width - 8] {
                        seq += 1;
                        let key = (base + offset).to_be_bytes();
                        builder.add(&key, seq, Value::Bytes(b"value")).unwrap();
                    }
                    runs.push(Arc::new(Published {
                        run: builder.finish(),
                        tail: false,
                        largest_block: 0,
                    }));
                }
            }
            let start = Instant::now();
            let directory = directory::RunDirectory::build(&runs);
            let build_us = start.elapsed().as_secs_f64() * 1e6;
            let run_struct = size_of::<Published>() + 2 * size_of::<usize>();

            let mut logical = LogicalWorkspace::new();
            let probes = 20_000u64;
            let mut time = |present: bool| {
                let start = Instant::now();
                for probe in 0..probes {
                    let run = (probe * 7919) as usize % runs.len();
                    let range = runs[run].run.key_range().unwrap();
                    let key: Vec<u8> = if present {
                        range.min.to_vec()
                    } else {
                        let mut key = range.min.to_vec();
                        key[7] ^= 0x80;
                        key
                    };
                    std::hint::black_box(
                        latest_from(directory.lookup(), &key, &mut logical)
                            .unwrap()
                            .is_some(),
                    );
                }
                start.elapsed().as_nanos() as f64 / probes as f64
            };
            let (present_ns, missing_ns) = (time(true), time(false));
            let select_us = if count <= select_up_to {
                let start = Instant::now();
                let picked = select(&candidates(&runs), CompactionOptions::default());
                std::hint::black_box(picked.ok());
                format!("{:.0}", start.elapsed().as_secs_f64() * 1e6)
            } else {
                "not run".to_string()
            };
            println!(
                "METADATA | {} | {levels} | {} | {build_us:.0} | {} | {run_struct} | {present_ns:.0} | {missing_ns:.0} | {select_us} |",
                runs.len(),
                directory.group_count(),
                directory.heap_bytes(),
            );
        }
    }
}

/// Prototype (5AR): a range-local compaction over the production job. The
/// runs a small update overlaps are found by binary search over the sorted
/// disjoint runs, merged with it, and replaced; every other run stays the
/// same shared run, a snapshot keeps reading what it saw, and the replaced
/// runs are retained only while it lives.
#[test]
fn a_range_local_replacement_leaves_other_runs_shared() {
    let engine = Engine::new(engine_options()).unwrap();
    let mut workspace = EngineWorkspace::new();
    for index in 0..3_000 {
        engine.put(&key(index), &value(index)).unwrap();
    }
    engine.flush().unwrap();
    let disjoint = engine.core.read_state().runs.len();
    assert!(disjoint >= 5, "{disjoint} runs");
    let snapshot = engine.snapshot().unwrap();
    for index in 1_500..1_520 {
        engine.put(&key(index), b"updated").unwrap();
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

    let read = |found: Option<LiveValue<'_>>| found.map(|live| live.value.to_vec());
    assert_eq!(
        read(engine.get(&key(1_510), &mut workspace).unwrap()),
        Some(b"updated".to_vec())
    );
    assert_eq!(
        read(snapshot.get(&key(1_510), &mut workspace).unwrap()),
        Some(value(1_510))
    );
    assert!(engine.stats().retained_run_bytes >= report.input_bytes);
    // The test's own references to the replaced runs count as a reader's.
    // Without them, the snapshot holds the older inputs; the update's run
    // was flushed after it, from a memtable it holds instead.
    drop((before, after));
    assert_eq!(
        engine.stats().retained_run_bytes,
        report.input_bytes - update_bytes
    );
    drop(snapshot);
    assert_eq!(engine.stats().retained_run_bytes, 0);
}
