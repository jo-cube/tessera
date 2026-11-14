//! Research (5AS): the invariants, progress and resource bounds of the
//! leveled organization `docs/capacity-design.md` proposes, before any of it
//! is built.
//!
//! [`model`] is a discrete simulation: exact keys, sequence numbers and
//! tombstones, segments with logical and stored sizes, snapshots that pin
//! replaced segments, and the production selector as a baseline. Its leveled
//! planner forms only jobs that fit the configured limits, splits a source
//! segment when its job does not, and refuses explicitly otherwise. Every
//! publication can be checked against the level invariants and every read
//! against a reference map.
//!
//! The model tests are ignored. Run them with a release build:
//!
//! ```text
//! scripts/validate.sh exec 3000 cargo test --locked --release --lib \
//!   engine::leveling_tests -- --ignored --nocapture --test-threads=1
//! ```
//!
//! The tests that drive the production compaction job over small real runs
//! (recovery into one level, scheduling after it) are not ignored.

use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::ops::Range;
use std::time::Instant;

use super::capacity_tests::{Rng, Workload, index};
use super::*;

pub(super) mod model {
    use super::*;

    /// One version of a key.
    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    pub struct Rec {
        pub key: u64,
        pub seq: u64,
        pub dead: bool,
    }

    /// How records and runs are sized.
    #[derive(Clone, Copy, Debug)]
    pub struct Sizes {
        /// Bytes counted toward the run length for a value and a tombstone:
        /// key, value and 16.
        pub live: u64,
        pub dead: u64,
        /// Stored bytes (blocks, index and filter) per logical byte.
        pub stored_per_logical: f64,
        /// Stored bytes of a run beside its records.
        pub run_overhead: u64,
    }

    #[derive(Clone, Debug)]
    pub struct Seg {
        pub id: u64,
        /// The flush that wrote it, for level 0's order and groups.
        pub flush: u64,
        pub recs: Vec<Rec>,
        pub logical: u64,
        pub stored: u64,
        /// Written by a split, to detect a split being split again.
        pub split: bool,
        /// The last run of an output, for the production selector.
        pub tail: bool,
    }

    impl Seg {
        pub fn min(&self) -> u64 {
            self.recs[0].key
        }
        pub fn max(&self) -> u64 {
            self.recs[self.recs.len() - 1].key
        }
        fn covers(&self, key: u64) -> bool {
            self.min() <= key && key <= self.max()
        }
        fn find(&self, key: u64) -> Option<Rec> {
            self.recs
                .binary_search_by_key(&key, |rec| rec.key)
                .ok()
                .map(|at| self.recs[at])
        }
    }

    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    pub enum Policy {
        /// The production selector over one flat set of runs.
        Current,
        Leveled,
    }

    /// Flush splitting at level 1's boundaries.
    #[derive(Clone, Copy, Debug, PartialEq)]
    pub struct Split {
        /// No piece smaller than this many logical bytes, as a fraction of
        /// a segment.
        pub min_piece: f64,
        /// No more pieces than this per flushed segment.
        pub max_pieces: usize,
    }

    #[derive(Clone, Copy, Debug)]
    pub struct Config {
        pub policy: Policy,
        /// Logical bytes a segment (`target_run_len`).
        pub target: u64,
        pub ratio: u64,
        /// Stored bytes of level 1 at least.
        pub base: u64,
        /// Flush groups in level 0 that call for compaction, and that stop
        /// flushes.
        pub l0_trigger: usize,
        pub l0_stop: usize,
        pub max_inputs: usize,
        /// Stored bytes of input and estimated output of one job.
        pub max_input: u64,
        pub max_output: u64,
        /// Published stored bytes, and the part of them flushes may not use.
        pub max_run_bytes: u64,
        pub reserve: u64,
        pub max_retained: u64,
        pub split: Option<Split>,
        /// Publications between a flush reading level 1's boundaries and
        /// publishing its pieces.
        pub stale: usize,
        /// Cut outputs where they would overlap more than `ratio` segments
        /// of the level below their own.
        pub grandparent: bool,
        /// Check the invariants after every publication and reads every so
        /// many.
        pub check_every: u64,
        /// Writes a flush holds.
        pub memtable: usize,
    }

    #[derive(Clone, Debug, PartialEq, Eq)]
    pub enum Plan {
        /// An empty level under level 0.
        Insert,
        Move {
            level: usize,
            at: usize,
        },
        Merge {
            level: usize,
            sources: Vec<usize>,
            dests: Range<usize>,
        },
        /// Two adjacent small segments of one level.
        Neighbours {
            level: usize,
            at: usize,
        },
        /// The source rewritten alone, cut before each key of `cuts`.
        Split {
            level: usize,
            at: usize,
            cuts: Vec<u64>,
        },
        /// A production selection over the flat set.
        Picked(Vec<usize>),
        Idle,
        Refuse(Refusal),
    }

    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    pub enum Refusal {
        /// Snapshots retain more than allowed; recovers when they end.
        Retained,
        /// One source segment and one destination do not fit the limits:
        /// the configuration must change.
        Job { stored: u64, inputs: usize },
    }

    #[derive(Clone, Debug, Default)]
    pub struct Stats {
        pub writes: u64,
        pub logical_in: u64,
        pub flushes: u64,
        pub flushed: u64,
        pub compacted: u64,
        pub jobs: u64,
        pub moves: u64,
        pub splits: u64,
        pub split_pieces: u64,
        pub resplits: u64,
        /// Stored bytes written into each level (splits into their own),
        /// and splits per level.
        pub written_into: Vec<u64>,
        pub splits_in: Vec<u64>,
        pub unproductive_splits: u64,
        pub neighbours: u64,
        pub inserts: u64,
        pub largest_job: u64,
        pub largest_inputs: usize,
        pub largest_output: u64,
        pub refused_retained: u64,
        pub refused_job: u64,
        pub refused_publish: u64,
        pub blocked_flushes: u64,
        pub stalls: u64,
        pub max_l0_groups: usize,
        pub max_l0_runs: usize,
        pub max_depth: usize,
        pub max_segments: usize,
        pub max_retained: u64,
        pub recovery_ops: Vec<u64>,
        pub flush_pieces: u64,
        pub max_flush_pieces: usize,
        pub smallest_piece: u64,
    }

    pub struct Tree {
        pub config: Config,
        pub sizes: Sizes,
        /// 0 is level 0 in flush order; the rest sorted and disjoint. Under
        /// [`Policy::Current`] everything is in level 0.
        pub levels: Vec<Vec<Seg>>,
        /// Stored bytes per level, kept as segments come and go.
        pub bytes: Vec<u64>,
        pub cursors: Vec<u64>,
        next_id: u64,
        pub seq: u64,
        memtable: BTreeMap<u64, Rec>,
        /// The newest version of every key written, and the stored size of
        /// one copy of the live ones.
        reference: HashMap<u64, Rec>,
        /// Ids of the published segments, and of retired ones snapshots hold.
        snapshots: Vec<(HashSet<u64>, u64)>,
        retired: HashMap<u64, u64>,
        boundaries: VecDeque<Vec<u64>>,
        pub stats: Stats,
        pub clock: u64,
        publications: u64,
        rng: Rng,
        /// Publish flushes without compacting after them or applying the
        /// level-0 stop, to build a state for a scenario.
        pub hold_maintenance: bool,
    }

    impl Tree {
        pub fn new(config: Config, sizes: Sizes) -> Self {
            Self {
                config,
                sizes,
                levels: vec![Vec::new(), Vec::new()],
                bytes: vec![0, 0],
                cursors: vec![0, 0],
                next_id: 1,
                seq: 0,
                memtable: BTreeMap::new(),
                reference: HashMap::new(),
                snapshots: Vec::new(),
                retired: HashMap::new(),
                boundaries: VecDeque::new(),
                stats: Stats {
                    smallest_piece: u64::MAX,
                    ..Stats::default()
                },
                clock: 0,
                publications: 0,
                rng: Rng(7),
                hold_maintenance: false,
            }
        }

        fn seg(&mut self, flush: u64, recs: Vec<Rec>) -> Seg {
            let logical = recs
                .iter()
                .map(|rec| {
                    if rec.dead {
                        self.sizes.dead
                    } else {
                        self.sizes.live
                    }
                })
                .sum::<u64>();
            let stored = (logical as f64 * self.sizes.stored_per_logical).ceil() as u64
                + self.sizes.run_overhead;
            self.next_id += 1;
            Seg {
                id: self.next_id,
                flush,
                recs,
                logical,
                stored,
                split: false,
                tail: false,
            }
        }

        fn rec_logical(&self, rec: &Rec) -> u64 {
            if rec.dead {
                self.sizes.dead
            } else {
                self.sizes.live
            }
        }

        /// Cuts records into segments at the target length, after the
        /// record that reaches it, as runs are cut.
        fn cut(&mut self, flush: u64, recs: Vec<Rec>, extra: &[u64]) -> Vec<Seg> {
            let mut out = Vec::new();
            let mut current = Vec::new();
            let mut length = 0;
            let mut extra = extra.iter().copied().peekable();
            for rec in recs {
                while extra.peek().is_some_and(|&cut| cut <= rec.key) {
                    extra.next();
                    if !current.is_empty() {
                        out.push(std::mem::take(&mut current));
                        length = 0;
                    }
                }
                length += self.rec_logical(&rec);
                current.push(rec);
                if length >= self.config.target {
                    out.push(std::mem::take(&mut current));
                    length = 0;
                }
            }
            if !current.is_empty() {
                out.push(current);
            }
            let mut segs: Vec<Seg> = out.into_iter().map(|recs| self.seg(flush, recs)).collect();
            if let Some(last) = segs.last_mut() {
                last.tail = true;
            }
            segs
        }

        pub fn published(&self) -> u64 {
            self.bytes.iter().sum()
        }

        pub fn segments(&self) -> usize {
            self.levels.iter().map(Vec::len).sum()
        }

        pub fn retained(&self) -> u64 {
            self.retired.values().sum()
        }

        fn l0_groups(&self) -> usize {
            let mut flushes: Vec<u64> = self.levels[0].iter().map(|seg| seg.flush).collect();
            flushes.dedup();
            flushes.len()
        }

        /// The read fan-in: the deepest overlap of all published segments,
        /// which is the number of groups the run directory forms.
        pub fn depth(&self) -> usize {
            let mut points: Vec<(u64, i32)> = self
                .levels
                .iter()
                .flatten()
                .flat_map(|seg| [(seg.min(), 1), (seg.max(), -1)])
                .collect();
            points.sort_by(|a, b| a.0.cmp(&b.0).then(b.1.cmp(&a.1)));
            let (mut depth, mut deepest) = (0, 0);
            for (_, step) in points {
                depth += step;
                deepest = deepest.max(depth);
            }
            deepest as usize
        }

        /// One copy of every live key, stored.
        pub fn live_stored(&self) -> u64 {
            let live = self.reference.values().filter(|rec| !rec.dead).count() as u64;
            (live as f64 * self.sizes.live as f64 * self.sizes.stored_per_logical) as u64
        }

        // Writes and flushes.

        /// Writes one version, flushing first if the memtable is full.
        /// Returns false if the flush was refused and maintenance could not
        /// make room: the write is not applied.
        pub fn write(&mut self, key: u64, dead: bool) -> bool {
            if self.memtable.len() >= self.config.memtable && !self.flush() {
                return false;
            }
            self.seq += 1;
            let rec = Rec {
                key,
                seq: self.seq,
                dead,
            };
            self.stats.writes += 1;
            self.stats.logical_in += self.rec_logical(&rec);
            self.memtable.insert(key, rec);
            self.reference.insert(key, rec);
            true
        }

        /// Flushes the memtable once admitted, compacting while it is not.
        pub fn flush(&mut self) -> bool {
            if self.memtable.is_empty() {
                return true;
            }
            let recs: Vec<Rec> = self.memtable.values().copied().collect();
            let flush = self.stats.flushes + 1;
            let cuts = self.flush_cuts(&recs);
            let segs = self.cut(flush, recs, &cuts);
            if self.config.split.is_some() {
                let pieces = segs.len();
                self.stats.flush_pieces += pieces as u64;
                self.stats.max_flush_pieces = self.stats.max_flush_pieces.max(pieces);
                for seg in &segs {
                    self.stats.smallest_piece = self.stats.smallest_piece.min(seg.logical);
                }
            }
            let stored: u64 = segs.iter().map(|seg| seg.stored).sum();
            let mut blocked_since = None;
            let mut stalls = 0;
            loop {
                let admitted = (self.hold_maintenance || self.l0_groups() < self.config.l0_stop)
                    && self.published() + stored
                        <= self
                            .config
                            .max_run_bytes
                            .saturating_sub(self.config.reserve);
                if admitted {
                    break;
                }
                if blocked_since.is_none() {
                    self.stats.blocked_flushes += 1;
                    blocked_since = Some(0u64);
                }
                if !self.maintain_once() {
                    // Nothing can run: time passes, snapshots may end. A
                    // flush still refused after that is refused for good,
                    // and the memtable keeps its records.
                    self.stats.stalls += 1;
                    stalls += 1;
                    self.tick();
                    if stalls > 1_000 {
                        return false;
                    }
                }
                *blocked_since.as_mut().unwrap() += 1;
            }
            if let Some(ops) = blocked_since {
                self.stats.recovery_ops.push(ops);
            }
            self.memtable.clear();
            self.stats.flushes += 1;
            self.stats.flushed += stored;
            for seg in segs {
                self.bytes[0] += seg.stored;
                self.levels[0].push(seg);
            }
            self.published_now();
            if !self.hold_maintenance {
                self.maintain();
            }
            self.tick();
            true
        }

        /// Where a flush is cut besides the target: at level 1's boundaries,
        /// as read `stale` publications ago, keeping every piece at least the
        /// minimum and at most `max_pieces` per segment.
        fn flush_cuts(&self, recs: &[Rec]) -> Vec<u64> {
            let (Some(split), Policy::Leveled) = (self.config.split, self.config.policy) else {
                return Vec::new();
            };
            let boundaries = self
                .boundaries
                .get(self.boundaries.len().saturating_sub(1 + self.config.stale))
                .cloned()
                .unwrap_or_default();
            let min_piece = (split.min_piece * self.config.target as f64) as u64;
            let mut cuts = Vec::new();
            let (mut piece, mut length) = (0u64, 0u64);
            let mut pieces = 1;
            let mut next = boundaries.iter().copied().peekable();
            for (at, rec) in recs.iter().enumerate() {
                while next.peek().is_some_and(|&cut| cut <= rec.key) {
                    let cut = next.next().unwrap();
                    let rest: u64 = recs[at..].iter().map(|rec| self.rec_logical(rec)).sum();
                    if piece >= min_piece && rest >= min_piece && pieces < split.max_pieces {
                        cuts.push(cut.max(rec.key));
                        piece = 0;
                        pieces += 1;
                    }
                }
                let size = self.rec_logical(rec);
                piece += size;
                length += size;
                if length >= self.config.target {
                    // A target cut starts a new segment with its own budget.
                    length = 0;
                    piece = 0;
                    pieces = 1;
                }
            }
            cuts
        }

        fn published_now(&mut self) {
            self.publications += 1;
            let bounds: Vec<u64> = self
                .levels
                .get(1)
                .map(|level| level.iter().map(Seg::min).collect())
                .unwrap_or_default();
            self.boundaries.push_back(bounds);
            if self.boundaries.len() > 64 {
                self.boundaries.pop_front();
            }
            self.stats.max_l0_groups = self.stats.max_l0_groups.max(self.l0_groups());
            self.stats.max_l0_runs = self.stats.max_l0_runs.max(self.levels[0].len());
            self.stats.max_segments = self.stats.max_segments.max(self.segments());
            self.stats.max_retained = self.stats.max_retained.max(self.retained());
            if self.publications.is_multiple_of(8) {
                self.stats.max_depth = self.stats.max_depth.max(self.depth());
            }
            if self.config.check_every > 0 {
                self.check_structure();
                if self.publications.is_multiple_of(self.config.check_every) {
                    self.check_reads(64);
                }
            }
        }

        // Snapshots.

        /// Takes a snapshot that ends `hold` ticks from now.
        pub fn snapshot(&mut self, hold: u64) {
            let ids = self.levels.iter().flatten().map(|seg| seg.id).collect();
            self.snapshots.push((ids, self.clock + hold));
        }

        /// Ends every snapshot.
        pub fn release_snapshots(&mut self) {
            self.snapshots.clear();
            self.retired.clear();
        }

        fn tick(&mut self) {
            self.clock += 1;
            let clock = self.clock;
            let before = self.snapshots.len();
            self.snapshots.retain(|(_, end)| *end > clock);
            if self.snapshots.len() != before {
                let snapshots = &self.snapshots;
                self.retired
                    .retain(|id, _| snapshots.iter().any(|(ids, _)| ids.contains(id)));
            }
        }

        fn retire(&mut self, seg: &Seg) {
            if self.snapshots.iter().any(|(ids, _)| ids.contains(&seg.id)) {
                self.retired.insert(seg.id, seg.stored);
            }
        }

        // Maintenance.

        /// Runs maintenance until there is nothing to do or it is refused.
        pub fn maintain(&mut self) {
            let mut recent = VecDeque::new();
            for _ in 0..100_000 {
                let plan = self.plan();
                recent.push_back(format!("{plan:?} {}", self.levels_report()));
                if recent.len() > 12 {
                    recent.pop_front();
                }
                if !self.apply(plan) {
                    return;
                }
            }
            panic!("maintenance did not settle: {recent:#?}");
        }

        /// Plans and applies one step. Returns whether anything changed.
        pub fn maintain_once(&mut self) -> bool {
            let plan = self.plan();
            self.apply(plan)
        }

        fn target_of(&self, level: usize) -> u64 {
            let last = self.levels.len() - 1;
            let shrink = self.config.ratio.pow((last - level) as u32);
            (self.bytes[last] / shrink).max(self.config.base)
        }

        pub fn plan(&self) -> Plan {
            if self.retained() > self.config.max_retained {
                return Plan::Refuse(Refusal::Retained);
            }
            match self.config.policy {
                Policy::Current => self.plan_current(),
                Policy::Leveled => self.plan_leveled(),
            }
        }

        fn plan_current(&self) -> Plan {
            let options = CompactionOptions {
                max_inputs: self.config.max_inputs,
                max_input_bytes: self.config.max_input as usize,
                max_output_bytes: self.config.max_output as usize,
                ..CompactionOptions::default()
            };
            let order = self.flat_order();
            let bounds: Vec<([u8; 8], [u8; 8])> = order
                .iter()
                .map(|&at| {
                    let seg = &self.levels[0][at];
                    (seg.min().to_be_bytes(), seg.max().to_be_bytes())
                })
                .collect();
            let candidates: Vec<Candidate<'_>> = order
                .iter()
                .zip(&bounds)
                .map(|(&at, (min, max))| Candidate {
                    min,
                    max,
                    bytes: self.levels[0][at].stored as usize,
                    tail: self.levels[0][at].tail,
                })
                .collect();
            match select(&candidates, options).ok().flatten() {
                Some(picks) => Plan::Picked(picks.into_iter().map(|pick| order[pick]).collect()),
                None => Plan::Idle,
            }
        }

        /// Level 0 by smallest key, then flush, as the run list is ordered.
        fn flat_order(&self) -> Vec<usize> {
            let mut order: Vec<usize> = (0..self.levels[0].len()).collect();
            order.sort_by_key(|&at| (self.levels[0][at].min(), self.levels[0][at].flush));
            order
        }

        fn plan_leveled(&self) -> Plan {
            let last = self.levels.len() - 1;
            let ratio = self.config.ratio;
            if self.bytes[last] / ratio.pow(last as u32 - 1) > self.config.base * ratio {
                return Plan::Insert;
            }
            let mut best = (0, 0.0);
            let l0 = self.l0_groups() as f64 / self.config.l0_trigger as f64;
            if l0 > 1.0 {
                best = (0, l0);
            }
            for level in 1..last {
                let score = self.bytes[level] as f64 / self.target_of(level) as f64;
                if score > 1.0 && score > best.1 {
                    best = (level, score);
                }
            }
            if best.1 > 1.0 {
                return self.plan_level(best.0);
            }
            self.plan_neighbours()
        }

        fn fits(&self, inputs: usize, stored: u64) -> bool {
            inputs <= self.config.max_inputs
                && stored <= self.config.max_input
                && stored <= self.config.max_output
        }

        fn overlapping(level: &[Seg], lo: u64, hi: u64) -> Range<usize> {
            let start = level.partition_point(|seg| seg.max() < lo);
            let end = level.partition_point(|seg| seg.min() <= hi);
            start..end.max(start)
        }

        fn plan_level(&self, level: usize) -> Plan {
            let here = &self.levels[level];
            let at = if level == 0 {
                0
            } else {
                let at = here.partition_point(|seg| seg.max() < self.cursors[level]);
                if at == here.len() { 0 } else { at }
            };
            let source = &here[at];
            let below = &self.levels[level + 1];
            let dests = Self::overlapping(below, source.min(), source.max());
            if dests.is_empty() {
                return Plan::Move { level, at };
            }
            let dest_bytes: u64 = below[dests.clone()].iter().map(|seg| seg.stored).sum();
            let mut stored = source.stored + dest_bytes;
            let inputs = 1 + dests.len();
            if self.fits(inputs, stored) {
                let mut sources = vec![at];
                let mut dests = dests;
                if level == 0 {
                    // Other level-0 runs overlapping the job join it, oldest
                    // first, with the level-1 runs they add, while it fits:
                    // reads go by sequence, so any subset is correct, and
                    // each level-1 range is rewritten once for all the
                    // flushes over it rather than once per flush.
                    let (mut lo, mut hi) = (source.min(), source.max());
                    let mut changed = true;
                    while changed {
                        changed = false;
                        for (other, seg) in here.iter().enumerate() {
                            if sources.contains(&other) || seg.max() < lo || hi < seg.min() {
                                continue;
                            }
                            let (lo2, hi2) = (lo.min(seg.min()), hi.max(seg.max()));
                            let dests2 = Self::overlapping(below, lo2, hi2);
                            let stored2 = stored
                                + seg.stored
                                + below[dests2.clone()].iter().map(|d| d.stored).sum::<u64>()
                                - below[dests.clone()].iter().map(|d| d.stored).sum::<u64>();
                            if self.fits(sources.len() + 1 + dests2.len(), stored2) {
                                sources.push(other);
                                (lo, hi, dests, stored) = (lo2, hi2, dests2, stored2);
                                changed = true;
                            }
                        }
                    }
                }
                return Plan::Merge {
                    level,
                    sources,
                    dests,
                };
            }
            self.plan_split(level, at, dests)
        }

        /// Cuts the source at destination boundaries so that each piece and
        /// the destinations it overlaps fit. Valid only if it leaves at least
        /// two pieces and every piece's job fits now; otherwise the job is
        /// refused, which only a configuration change resolves.
        fn plan_split(&self, level: usize, at: usize, dests: Range<usize>) -> Plan {
            let source = &self.levels[level][at];
            let below = &self.levels[level + 1][dests.clone()];
            let refuse = Plan::Refuse(Refusal::Job {
                stored: source.stored + below.iter().map(|seg| seg.stored).sum::<u64>(),
                inputs: 1 + below.len(),
            });
            let per_logical = source.stored as f64 / source.logical.max(1) as f64;
            let piece_bytes = |lo: u64, hi: u64| {
                let logical: u64 = source
                    .recs
                    .iter()
                    .filter(|rec| lo <= rec.key && rec.key <= hi)
                    .map(|rec| self.rec_logical(rec))
                    .sum();
                (logical as f64 * per_logical) as u64 + self.sizes.run_overhead
            };
            let mut cuts = Vec::new();
            let mut start = 0;
            while start < below.len() {
                let lo = if start == 0 { 0 } else { below[start].min() };
                let mut end = start + 1;
                loop {
                    let hi = if end == below.len() {
                        u64::MAX
                    } else {
                        below[end].min() - 1
                    };
                    let chunk: u64 = below[start..end].iter().map(|seg| seg.stored).sum();
                    if !self.fits(1 + end - start, chunk + piece_bytes(lo, hi)) {
                        if end == start + 1 {
                            return refuse;
                        }
                        end -= 1;
                        break;
                    }
                    if end == below.len() {
                        break;
                    }
                    end += 1;
                }
                if end < below.len() {
                    cuts.push(below[end].min());
                }
                start = end;
            }
            // Only cuts with records of the source on both sides make pieces.
            cuts.retain(|&cut| source.min() < cut && cut <= source.max());
            cuts.dedup();
            if cuts.is_empty() {
                return refuse;
            }
            Plan::Split { level, at, cuts }
        }

        fn plan_neighbours(&self) -> Plan {
            let small = self.config.target / 2;
            for level in 1..self.levels.len() {
                let here = &self.levels[level];
                for at in 1..here.len() {
                    let (a, b) = (&here[at - 1], &here[at]);
                    // Within the grandparent bound, so that the merged
                    // segment's next job stays bounded too.
                    let below = self.levels.get(level + 1).map_or(0, |below| {
                        below[Self::overlapping(below, a.min(), b.max())]
                            .iter()
                            .map(|seg| seg.logical)
                            .sum()
                    });
                    if a.logical < small
                        && b.logical < small
                        && self.fits(2, a.stored + b.stored)
                        && (!self.config.grandparent
                            || level + 1 == self.levels.len()
                            || below <= self.config.ratio * self.config.target)
                    {
                        return Plan::Neighbours { level, at: at - 1 };
                    }
                }
            }
            Plan::Idle
        }

        pub fn apply(&mut self, plan: Plan) -> bool {
            match plan {
                Plan::Idle => false,
                Plan::Refuse(refusal) => {
                    match refusal {
                        Refusal::Retained => self.stats.refused_retained += 1,
                        Refusal::Job { .. } => self.stats.refused_job += 1,
                    }
                    false
                }
                Plan::Insert => {
                    self.levels.insert(1, Vec::new());
                    self.bytes.insert(1, 0);
                    self.cursors.insert(1, 0);
                    self.stats.inserts += 1;
                    self.published_now();
                    true
                }
                Plan::Move { level, at } => {
                    let seg = self.levels[level].remove(at);
                    self.bytes[level] -= seg.stored;
                    self.bytes[level + 1] += seg.stored;
                    let below = &mut self.levels[level + 1];
                    let to = below.partition_point(|other| other.max() < seg.min());
                    below.insert(to, seg);
                    self.stats.moves += 1;
                    if level > 0 {
                        self.cursors[level] = self.levels[level + 1][to].max().saturating_add(1);
                    }
                    self.published_now();
                    true
                }
                Plan::Merge {
                    level,
                    sources,
                    dests,
                } => {
                    let cursor = self.levels[level][sources[0]].max().saturating_add(1);
                    let mut inputs = Vec::new();
                    let mut sources = sources;
                    sources.sort_unstable();
                    for &at in sources.iter().rev() {
                        inputs.push(self.levels[level].remove(at));
                    }
                    let dest_inputs: Vec<Seg> =
                        self.levels[level + 1].drain(dests.clone()).collect();
                    inputs.extend(dest_inputs);
                    let outputs = self.merge(&inputs, level + 1, true);
                    if !self.publish(&inputs, &outputs) {
                        // Put everything back: the state is unchanged.
                        let back: Vec<Seg> = inputs.split_off(sources.len());
                        self.levels[level + 1].splice(dests.start..dests.start, back);
                        for (seg, &at) in inputs.into_iter().rev().zip(&sources) {
                            self.levels[level].insert(at, seg);
                        }
                        return false;
                    }
                    let to = dests.start;
                    let input_stored: u64 = inputs.iter().map(|seg| seg.stored).sum();
                    for seg in &inputs[..sources.len()] {
                        self.bytes[level] -= seg.stored;
                    }
                    for seg in &inputs[sources.len()..] {
                        self.bytes[level + 1] -= seg.stored;
                    }
                    self.bytes[level + 1] += outputs.iter().map(|seg| seg.stored).sum::<u64>();
                    self.levels[level + 1].splice(to..to, outputs);
                    self.record_job(&inputs, input_stored);
                    if level > 0 {
                        self.cursors[level] = cursor;
                    }
                    for seg in &inputs {
                        self.retire(seg);
                    }
                    self.published_now();
                    true
                }
                Plan::Neighbours { level, at } => {
                    let inputs: Vec<Seg> = self.levels[level].drain(at..at + 2).collect();
                    let outputs = self.merge(&inputs, level, false);
                    // Progress: fewer segments, or the step is not taken.
                    assert!(
                        outputs.len() < 2,
                        "neighbours merged into {}",
                        outputs.len()
                    );
                    let input_stored: u64 = inputs.iter().map(|seg| seg.stored).sum();
                    self.bytes[level] -= input_stored;
                    self.bytes[level] += outputs.iter().map(|seg| seg.stored).sum::<u64>();
                    self.levels[level].splice(at..at, outputs);
                    self.record_job(&inputs, input_stored);
                    self.stats.neighbours += 1;
                    for seg in &inputs {
                        self.retire(seg);
                    }
                    self.published_now();
                    true
                }
                Plan::Split { level, at, cuts } => {
                    let source = self.levels[level].remove(at);
                    if source.split {
                        self.stats.resplits += 1;
                    }
                    let mut pieces = self.cut(source.flush, source.recs.clone(), &cuts);
                    for piece in &mut pieces {
                        piece.split = true;
                    }
                    let stored: u64 = pieces.iter().map(|seg| seg.stored).sum();
                    self.bytes[level] = self.bytes[level] - source.stored + stored;
                    self.stats.compacted += stored;
                    self.stats.splits += 1;
                    for (counts, amount) in [
                        (&mut self.stats.written_into, stored),
                        (&mut self.stats.splits_in, 1),
                    ] {
                        if counts.len() <= level {
                            counts.resize(level + 1, 0);
                        }
                        counts[level] += amount;
                    }
                    self.stats.split_pieces += pieces.len() as u64;
                    let ids: Vec<u64> = pieces.iter().map(|piece| piece.id).collect();
                    self.levels[level].splice(at..at, pieces);
                    self.retire(&source);
                    self.published_now();
                    // The split's measure of progress: the next job from this
                    // level takes its first piece and fits.
                    let next = self.plan_level(level);
                    let piece = |at: usize| ids.contains(&self.levels[level][at].id);
                    match next {
                        Plan::Merge { ref sources, .. } if piece(sources[0]) => {}
                        Plan::Move { at, .. } if piece(at) => {}
                        other => {
                            self.stats.unproductive_splits += 1;
                            if self.config.check_every > 0 {
                                panic!("a split left no legal job for its piece: {other:?}");
                            }
                        }
                    }
                    true
                }
                Plan::Picked(picks) => {
                    let mut picks = picks;
                    picks.sort_unstable();
                    let mut inputs = Vec::new();
                    for &at in picks.iter().rev() {
                        inputs.push(self.levels[0].remove(at));
                    }
                    let outputs = self.merge(&inputs, 0, false);
                    let input_stored: u64 = inputs.iter().map(|seg| seg.stored).sum();
                    self.bytes[0] -= input_stored;
                    self.bytes[0] += outputs.iter().map(|seg| seg.stored).sum::<u64>();
                    self.levels[0].extend(outputs);
                    self.record_job(&inputs, input_stored);
                    for seg in &inputs {
                        self.retire(seg);
                    }
                    self.published_now();
                    true
                }
            }
        }

        fn publish(&mut self, inputs: &[Seg], outputs: &[Seg]) -> bool {
            let before = self.published();
            let input: u64 = inputs.iter().map(|seg| seg.stored).sum();
            let output: u64 = outputs.iter().map(|seg| seg.stored).sum();
            let after = before - input + output;
            if after > self.config.max_run_bytes.max(before) {
                self.stats.refused_publish += 1;
                return false;
            }
            true
        }

        fn record_job(&mut self, inputs: &[Seg], input_stored: u64) {
            self.stats.jobs += 1;
            self.stats.largest_job = self.stats.largest_job.max(input_stored);
            self.stats.largest_inputs = self.stats.largest_inputs.max(inputs.len());
        }

        /// Merges whole segments: the newest version of each key, a
        /// tombstone dropped only if no other published segment covers its
        /// key; output cut at the target and, for a level with one below it,
        /// where it would overlap more than `ratio` segments there.
        fn merge(&mut self, inputs: &[Seg], level: usize, grandparent: bool) -> Vec<Seg> {
            let mut recs: Vec<Rec> = inputs
                .iter()
                .flat_map(|seg| seg.recs.iter().copied())
                .collect();
            recs.sort_unstable_by(|a, b| a.key.cmp(&b.key).then(b.seq.cmp(&a.seq)));
            recs.dedup_by_key(|rec| rec.key);
            let others: Vec<(u64, u64)> = self
                .levels
                .iter()
                .flatten()
                .map(|seg| (seg.min(), seg.max()))
                .collect();
            recs.retain(|rec| {
                !rec.dead
                    || others
                        .iter()
                        .any(|&(lo, hi)| lo <= rec.key && rec.key <= hi)
            });
            let cuts = if grandparent
                && self.config.grandparent
                && level + 1 < self.levels.len()
                && level > 0
            {
                self.grandparent_cuts(&recs, level + 1)
            } else {
                Vec::new()
            };
            let flush = inputs.iter().map(|seg| seg.flush).max().unwrap_or(0);
            let outputs = if recs.is_empty() {
                Vec::new()
            } else {
                self.cut(flush, recs, &cuts)
            };
            let stored_out: u64 = outputs.iter().map(|seg| seg.stored).sum();
            self.stats.compacted += stored_out;
            if self.stats.written_into.len() <= level {
                self.stats.written_into.resize(level + 1, 0);
            }
            self.stats.written_into[level] += stored_out;
            self.stats.largest_output = self.stats.largest_output.max(stored_out);
            outputs
        }

        /// Keys before which an output into the level above `grand` must
        /// be cut so that it overlaps at most `ratio` segments' bytes there.
        fn grandparent_cuts(&self, recs: &[Rec], grand: usize) -> Vec<u64> {
            let limit = self.config.ratio * self.config.target;
            let below = &self.levels[grand];
            let mut cuts = Vec::new();
            let mut overlap = 0;
            let mut next = below.partition_point(|seg| seg.max() < recs[0].key);
            for rec in recs {
                while next < below.len() && below[next].min() <= rec.key {
                    overlap += below[next].logical;
                    next += 1;
                    if overlap > limit {
                        cuts.push(rec.key);
                        overlap = below[next - 1].logical;
                    }
                }
            }
            cuts
        }

        // Invariants.

        /// After every publication: unique membership, ordered disjoint
        /// levels, byte accounting, level 0 in flush order.
        pub fn check_structure(&mut self) {
            let mut ids = HashSet::new();
            for (level, segs) in self.levels.iter().enumerate() {
                let mut total = 0;
                for seg in segs {
                    assert!(!seg.recs.is_empty(), "an empty segment in level {level}");
                    assert!(ids.insert(seg.id), "segment {} published twice", seg.id);
                    assert!(
                        seg.recs.windows(2).all(|pair| pair[0].key < pair[1].key),
                        "a segment holds one version per key, in order"
                    );
                    total += seg.stored;
                }
                assert_eq!(total, self.bytes[level], "bytes of level {level}");
                if level > 0 && self.config.policy == Policy::Leveled {
                    assert!(
                        segs.windows(2).all(|pair| pair[0].max() < pair[1].min()),
                        "level {level} is sorted and disjoint"
                    );
                }
            }
            if self.config.policy == Policy::Leveled {
                assert!(
                    self.levels[0]
                        .windows(2)
                        .all(|pair| pair[0].flush <= pair[1].flush),
                    "level 0 is in flush order"
                );
            }
            assert_eq!(self.cursors.len(), self.levels.len());
            assert_eq!(self.bytes.len(), self.levels.len());
        }

        /// Reads `samples` random written keys (all if 0) through every
        /// segment regardless of level, newest sequence first, and compares
        /// them with the reference.
        pub fn check_reads(&mut self, samples: usize) {
            let keys: Vec<u64> = if samples == 0 {
                self.reference.keys().copied().collect()
            } else {
                let all: Vec<u64> = self.reference.keys().copied().take(4096).collect();
                if all.is_empty() {
                    return;
                }
                (0..samples)
                    .map(|_| all[self.rng.below(all.len() as u64) as usize])
                    .collect()
            };
            for key in keys {
                let expected = self.reference[&key];
                let found = self.memtable.get(&key).copied().or_else(|| {
                    self.levels
                        .iter()
                        .flatten()
                        .filter(|seg| seg.covers(key))
                        .filter_map(|seg| seg.find(key))
                        .max_by_key(|rec| rec.seq)
                });
                match found {
                    Some(rec) => assert_eq!(rec, expected, "key {key}"),
                    None => assert!(expected.dead, "key {key} lost"),
                }
            }
        }

        /// Puts sorted live records into the last level, as recovery from a
        /// checkpoint would: one disjoint level.
        pub fn recover(&mut self, keys: u64) {
            let recs: Vec<Rec> = (0..keys)
                .map(|key| {
                    self.seq += 1;
                    let rec = Rec {
                        key,
                        seq: self.seq,
                        dead: false,
                    };
                    self.reference.insert(key, rec);
                    rec
                })
                .collect();
            let segs = self.cut(0, recs, &[]);
            let last = self.levels.len() - 1;
            for seg in segs {
                self.bytes[last] += seg.stored;
                self.levels[last].push(seg);
            }
            self.published_now();
        }

        pub fn levels_report(&self) -> String {
            let last = self.levels.len() - 1;
            (0..=last)
                .map(|level| {
                    let target = if level == 0 || level == last {
                        String::from("-")
                    } else {
                        format!(
                            "{:.2}",
                            self.bytes[level] as f64 / self.target_of(level) as f64
                        )
                    };
                    format!("L{level}:{}/{}", self.levels[level].len(), target)
                })
                .collect::<Vec<_>>()
                .join(" ")
        }
    }
}

use model::{Config, Plan, Policy, Refusal, Sizes, Split, Tree};

/// The engine's reduced scale (as in `capacity_tests`): 64 KiB segments,
/// 131-byte records (15-byte keys, 100-byte values, 16), 266 records a
/// flush, compaction limits of four segments of stored bytes.
const TARGET: u64 = 64 << 10;
const MEMTABLE: usize = 266;

/// Stored bytes per logical byte measured for the text-like values.
const TEXT: f64 = 0.214;

fn sizes(stored_per_logical: f64) -> Sizes {
    Sizes {
        live: 131,
        dead: 31,
        stored_per_logical,
        run_overhead: 64,
    }
}

fn current() -> Config {
    Config {
        policy: Policy::Current,
        target: TARGET,
        ratio: 4,
        base: 0,
        l0_trigger: 4,
        l0_stop: usize::MAX,
        max_inputs: 8,
        max_input: 4 * TARGET,
        max_output: 4 * TARGET,
        max_run_bytes: u64::MAX,
        reserve: 0,
        max_retained: u64::MAX,
        split: None,
        stale: 0,
        grandparent: false,
        check_every: 0,
        memtable: MEMTABLE,
    }
}

/// The proposed policy: ratio 4, level 1 at least four segments of stored
/// bytes, level 0 compacted at 4 flushes and stopping them at 12, flushes
/// split at level 1's boundaries into pieces of at least an eighth of a
/// segment and at most 8 per segment, outputs cut at grandparent overlap,
/// and 32 inputs a job (the default is 8; see `workloads_under_leveling`).
/// `LEVELING_MAX_INPUTS` and `LEVELING_SPLIT=none` override the last two.
fn leveled(stored_per_logical: f64) -> Config {
    Config {
        policy: Policy::Leveled,
        base: (4.0 * TARGET as f64 * stored_per_logical) as u64,
        l0_stop: 12,
        split: match std::env::var("LEVELING_SPLIT").as_deref() {
            Ok("none") => None,
            _ => Some(Split {
                min_piece: 1.0 / 8.0,
                max_pieces: 8,
            }),
        },
        grandparent: true,
        check_every: 50,
        max_inputs: std::env::var("LEVELING_MAX_INPUTS")
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(32),
        ..current()
    }
}

#[derive(Clone, Copy, Debug)]
enum History {
    Engine(Workload),
    /// From empty: half inserts of new keys, 30% overwrites and 20%
    /// deletes of live keys.
    Mixed,
    /// Uniform overwrites with a snapshot every 40 flushes held for 30
    /// ticks (flushes, or stalls while flushes are blocked), and at most 16
    /// segments retained.
    Snapshots,
}

struct Run {
    completed: bool,
    seconds: f64,
}

fn drive(tree: &mut Tree, history: History, writes: u64) -> Run {
    let start = Instant::now();
    let mut rng = Rng(11);
    let keys = match history {
        History::Engine(Workload::UniformUpdate | Workload::Hot) | History::Snapshots => writes / 4,
        _ => writes,
    };
    let mut live: Vec<u64> = Vec::new();
    let mut completed = true;
    for write in 0..writes {
        let (key, dead) = match history {
            History::Engine(workload) => (index(workload, write, keys, &mut rng), false),
            History::Snapshots => {
                if tree.stats.flushes > 0 && write % (40 * MEMTABLE as u64) == 0 {
                    tree.snapshot(30);
                }
                (index(Workload::UniformUpdate, write, keys, &mut rng), false)
            }
            History::Mixed => {
                let roll = rng.below(10);
                if roll < 5 || live.is_empty() {
                    let key = rng.below(u64::MAX >> 4);
                    live.push(key);
                    (key, false)
                } else if roll < 8 {
                    (live[rng.below(live.len() as u64) as usize], false)
                } else {
                    let at = rng.below(live.len() as u64) as usize;
                    (live.swap_remove(at), true)
                }
            }
        };
        if !tree.write(key, dead) {
            completed = false;
            break;
        }
    }
    if completed {
        completed = tree.flush();
    }
    if tree.config.check_every > 0 {
        tree.check_structure();
        tree.check_reads(0);
    }
    Run {
        completed,
        seconds: start.elapsed().as_secs_f64(),
    }
}

const HEADER: &str = "LEVELING | history | policy | completed | WA (flushed+compacted)/flushed | stored written / logical | live stored MiB | space amp. | segments (max) | L0 runs max / groups max | depth max | jobs / moves / splits (pieces) / resplits / neighbours / inserts | largest job KiB (inputs) | largest output KiB | refused retained / job / publish | blocked flushes, recovery ops max | levels (runs/score) | s |";

fn report(history: &str, policy: &str, tree: &Tree, run: &Run) {
    let stats = &tree.stats;
    let flushed = stats.flushed.max(1) as f64;
    println!(
        "LEVELING | {history} | {policy} | {} | {:.2} | {:.3} | {:.2} | {:.2} | {} ({}) | {} / {} | {} | {} / {} / {} ({}) / {} / {} / {} | {:.0} ({}) | {:.0} | {} / {} / {} | {}, {} | {} | {:.1} |",
        if run.completed { "yes" } else { "REFUSED" },
        (stats.flushed + stats.compacted) as f64 / flushed,
        (stats.flushed + stats.compacted) as f64 / stats.logical_in.max(1) as f64,
        tree.published() as f64 / (1 << 20) as f64,
        tree.published() as f64 / tree.live_stored().max(1) as f64,
        tree.segments(),
        stats.max_segments,
        stats.max_l0_runs,
        stats.max_l0_groups,
        stats.max_depth.max(tree.depth()),
        stats.jobs,
        stats.moves,
        stats.splits,
        stats.split_pieces,
        stats.resplits,
        stats.neighbours,
        stats.inserts,
        stats.largest_job as f64 / 1024.0,
        stats.largest_inputs,
        stats.largest_output as f64 / 1024.0,
        stats.refused_retained,
        stats.refused_job,
        stats.refused_publish,
        stats.blocked_flushes,
        stats.recovery_ops.iter().max().copied().unwrap_or(0),
        tree.levels_report(),
        run.seconds,
    );
    let flushed = stats.flushed.max(1) as f64;
    let into: Vec<String> = stats
        .written_into
        .iter()
        .map(|bytes| format!("{:.2}", *bytes as f64 / flushed))
        .collect();
    println!(
        "LEVELING detail | {history} | {policy} | written into each level per flushed byte: {} | splits per level: {:?} |",
        into.join(" "),
        stats.splits_in
    );
}

fn writes_from_env(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
        * 1_000
}

/// The discrete model under the production selector, against the engine's
/// measured histories (`capacity_tests::write_amplification_without_a_run_limit`
/// at `CAPACITY_WRITES=200`: WA 2.91 ascending, 42.65 uniform inserts,
/// 26.66 uniform updates, 2.88 hot).
#[test]
#[ignore = "research: run with --release --ignored --nocapture"]
fn the_discrete_model_reproduces_the_measured_policy() {
    let writes = writes_from_env("LEVELING_BASELINE_WRITES", 200);
    println!("{HEADER}");
    for (workload, keys) in [
        (Workload::Ascending, writes),
        (Workload::UniformInsert, writes),
        (Workload::UniformUpdate, writes / 2),
        (Workload::Hot, writes / 2),
    ] {
        let mut tree = Tree::new(current(), sizes(TEXT));
        let start = Instant::now();
        let mut rng = Rng(11);
        for write in 0..writes {
            assert!(tree.write(index(workload, write, keys, &mut rng), false));
        }
        assert!(tree.flush());
        let run = Run {
            completed: true,
            seconds: start.elapsed().as_secs_f64(),
        };
        report(&format!("{workload:?}"), "current", &tree, &run);
    }
}

/// Section 6: the workloads under the current policy and the proposed one,
/// with the invariants checked after every publication and reads checked
/// throughout and in full at the end.
#[test]
#[ignore = "research: run with --release --ignored --nocapture"]
fn workloads_under_leveling() {
    let writes = writes_from_env("LEVELING_WRITES", 400);
    let current_up_to = writes_from_env("LEVELING_CURRENT_WRITES", 200);
    println!("{HEADER}");
    let histories = [
        ("uniform insert", History::Engine(Workload::UniformInsert)),
        (
            "uniform overwrite",
            History::Engine(Workload::UniformUpdate),
        ),
        ("ascending insert", History::Engine(Workload::Ascending)),
        ("hot-range overwrite", History::Engine(Workload::Hot)),
        ("mixed with deletes", History::Mixed),
        ("snapshots", History::Snapshots),
    ];
    for (label, history) in histories {
        let snapshots = matches!(history, History::Snapshots);
        let retained = |mut config: Config| {
            if snapshots {
                config.max_retained = (16.0 * TARGET as f64 * TEXT) as u64;
            }
            config
        };
        let mut policies = vec![
            ("leveled 4", retained(leveled(TEXT))),
            (
                "leveled 8",
                retained(Config {
                    ratio: 8,
                    ..leveled(TEXT)
                }),
            ),
            (
                "leveled 4, no flush split",
                retained(Config {
                    split: None,
                    ..leveled(TEXT)
                }),
            ),
            (
                "leveled 4, 8 inputs",
                retained(Config {
                    max_inputs: 8,
                    ..leveled(TEXT)
                }),
            ),
            (
                "leveled 8, 8 inputs",
                retained(Config {
                    ratio: 8,
                    max_inputs: 8,
                    ..leveled(TEXT)
                }),
            ),
        ];
        if writes <= current_up_to {
            policies.insert(0, ("current", retained(current())));
        }
        for (policy, config) in policies {
            let mut tree = Tree::new(config, sizes(TEXT));
            let run = drive(&mut tree, history, writes);
            report(label, policy, &tree, &run);
            assert_eq!(tree.stats.resplits, 0, "{label}, {policy}");
            assert_eq!(tree.stats.refused_job, 0, "{label}, {policy}");
        }
    }
}

/// Section 3: each scenario either settles (no work left) within a bounded
/// number of steps or ends in an explicit refusal, with the invariants
/// checked after every publication.
#[test]
#[ignore = "research: run with --release --ignored --nocapture"]
fn every_admitted_state_progresses_or_refuses() {
    fn settle(tree: &mut Tree) -> (u64, Plan) {
        for step in 0..200_000 {
            let plan = tree.plan();
            if matches!(plan, Plan::Idle | Plan::Refuse(_)) {
                return (step, plan);
            }
            assert!(tree.apply(plan));
        }
        panic!("no settling within the bound");
    }
    fn line(name: &str, tree: &Tree, steps: u64, end: &Plan) {
        let stats = &tree.stats;
        println!(
            "PROGRESS | {name} | {steps} steps | end {end:?} | jobs {} moves {} splits {} ({} pieces) resplits {} neighbours {} inserts {} | largest job {:.0} KiB, {} inputs | {} |",
            stats.jobs,
            stats.moves,
            stats.splits,
            stats.split_pieces,
            stats.resplits,
            stats.neighbours,
            stats.inserts,
            stats.largest_job as f64 / 1024.0,
            stats.largest_inputs,
            tree.levels_report(),
        );
    }
    let mut rng = Rng(5);
    let mut uniform = |tree: &mut Tree, writes: u64, keys: u64| {
        for _ in 0..writes {
            assert!(tree.write(rng.below(keys), false));
        }
        assert!(tree.flush());
    };

    // A full level 0: 12 flushes published without compaction.
    let mut tree = Tree::new(leveled(TEXT), sizes(TEXT));
    tree.recover(40_000);
    tree.hold_maintenance = true;
    uniform(&mut tree, 12 * MEMTABLE as u64, 40_000);
    tree.hold_maintenance = false;
    let (steps, end) = settle(&mut tree);
    line("full level 0 over recovered data", &tree, steps, &end);
    assert_eq!(end, Plan::Idle);

    // Checkpoint recovery into one large level, then levels inserted and
    // legal work after recovery.
    let mut tree = Tree::new(leveled(TEXT), sizes(TEXT));
    tree.recover(400_000);
    let (steps, end) = settle(&mut tree);
    line("recovered into one level", &tree, steps, &end);
    assert!(tree.stats.inserts >= 2 && end == Plan::Idle);
    uniform(&mut tree, 100_000, 400_000);
    let (steps, end) = settle(&mut tree);
    line("writes after recovery", &tree, steps, &end);
    assert!(tree.stats.jobs > 0 && end == Plan::Idle);

    // A source overlapping many destinations: the last level recovered in
    // pieces of a sixteenth of a segment, so a level-0 run spans many of
    // them, and 8 inputs a job.
    let mut tree = Tree::new(
        Config {
            target: TARGET / 16,
            max_inputs: 8,
            ..leveled(TEXT)
        },
        sizes(TEXT),
    );
    tree.recover(100_000);
    tree.config.target = TARGET;
    uniform(&mut tree, 50_000, 100_000);
    let (steps, end) = settle(&mut tree);
    line("source over many small destinations", &tree, steps, &end);
    assert!(tree.stats.splits > 0 && end == Plan::Idle);
    assert_eq!(tree.stats.resplits, 0);

    // The same with the fewest inputs a job can have: every split piece
    // takes one destination.
    let mut tree = Tree::new(
        Config {
            target: TARGET / 16,
            max_inputs: 2,
            ..leveled(TEXT)
        },
        sizes(TEXT),
    );
    tree.recover(100_000);
    tree.config.target = TARGET;
    uniform(&mut tree, 50_000, 100_000);
    let (steps, end) = settle(&mut tree);
    line("two inputs a job", &tree, steps, &end);
    assert!(end == Plan::Idle && tree.stats.resplits == 0);

    // Input and output budgets below two segments: once two full segments
    // must meet, the job is refused explicitly (and keeps being refused:
    // only a configuration change resolves it). Writes continue while level
    // 0 can still drain into small pieces; deeper levels grow past their
    // targets.
    let mut tree = Tree::new(
        Config {
            max_input: (1.5 * TARGET as f64 * TEXT) as u64,
            max_output: (1.5 * TARGET as f64 * TEXT) as u64,
            check_every: 0,
            ..leveled(TEXT)
        },
        sizes(TEXT),
    );
    tree.recover(40_000);
    let mut accepted = 0;
    while accepted < 200 * MEMTABLE && tree.write(rng.below(40_000), false) {
        accepted += 1;
    }
    let (steps, end) = settle(&mut tree);
    line(
        &format!(
            "budget below two segments, {} refusals, {accepted} writes accepted",
            tree.stats.refused_job
        ),
        &tree,
        steps,
        &end,
    );
    assert!(tree.stats.refused_job > 0);
    tree.check_structure();
    tree.check_reads(0);

    // Snapshot retention: refused while the snapshot lives, then recovers.
    let mut tree = Tree::new(
        Config {
            max_retained: (4.0 * TARGET as f64 * TEXT) as u64,
            ..leveled(TEXT)
        },
        sizes(TEXT),
    );
    tree.recover(40_000);
    tree.snapshot(u64::MAX / 2);
    let mut refused = false;
    for _ in 0..200 * MEMTABLE {
        if !tree.write(rng.below(40_000), false) {
            refused = true;
            break;
        }
    }
    let (steps, end) = settle(&mut tree);
    line("snapshot retaining too much", &tree, steps, &end);
    assert!(refused && end == Plan::Refuse(Refusal::Retained));
    tree.release_snapshots();
    let (steps, end) = settle(&mut tree);
    line("after the snapshot ends", &tree, steps, &end);
    assert!(end == Plan::Idle && tree.flush());

    // The published-bytes limit: inserts fill it and flushes are refused
    // for good (user action); overwrites at the same size keep going.
    for (name, overwrite) in [
        ("byte limit, inserts", false),
        ("byte limit, overwrites", true),
    ] {
        let mut tree = Tree::new(
            Config {
                max_run_bytes: (200.0 * TARGET as f64 * TEXT) as u64,
                reserve: (4.0 * TARGET as f64 * TEXT) as u64,
                ..leveled(TEXT)
            },
            sizes(TEXT),
        );
        let mut accepted = 0;
        for write in 0..200_000 {
            let key = if overwrite {
                write % 60_000
            } else {
                write * 7_919 % 200_000
            };
            if !tree.write(key, false) {
                break;
            }
            accepted += 1;
        }
        let (steps, end) = settle(&mut tree);
        line(
            &format!("{name}: {accepted} writes accepted"),
            &tree,
            steps,
            &end,
        );
        assert_eq!(accepted == 200_000, overwrite);
        assert!(tree.published() <= tree.config.max_run_bytes);
    }

    // Small uneven segments in one level: neighbours merge.
    let mut tree = Tree::new(
        Config {
            target: TARGET / 8,
            ..leveled(TEXT)
        },
        sizes(TEXT),
    );
    tree.recover(20_000);
    tree.config.target = TARGET;
    let before = tree.segments();
    let (steps, end) = settle(&mut tree);
    line(
        &format!("small segments, {before} before"),
        &tree,
        steps,
        &end,
    );
    assert!(tree.segments() < before && end == Plan::Idle);
}

/// Section 4: job sizes under byte limits with the measured stored sizes of
/// compressible and incompressible values, at ratio 4 and 8: the default
/// limits (four segments' logical length of stored bytes: many compressed
/// segments, four incompressible ones), four segments of stored bytes
/// whatever the compression, and the smallest valid limits (a little over
/// two stored segments).
#[test]
#[ignore = "research: run with --release --ignored --nocapture"]
fn job_geometry_under_byte_limits() {
    let writes = writes_from_env("LEVELING_GEOMETRY_WRITES", 200);
    println!("{HEADER}");
    for (data, stored_per_logical) in [("text", TEXT), ("JSON", 0.18), ("incompressible", 1.02)] {
        for ratio in [4, 8] {
            for (limits, budget) in [
                ("default limits", 4 * TARGET),
                (
                    "4 stored segments",
                    (4.0 * TARGET as f64 * stored_per_logical) as u64,
                ),
                (
                    "2.2 stored segments",
                    (2.2 * TARGET as f64 * stored_per_logical) as u64,
                ),
            ] {
                let config = Config {
                    ratio,
                    max_input: budget,
                    max_output: budget,
                    ..leveled(stored_per_logical)
                };
                let mut tree = Tree::new(config, sizes(stored_per_logical));
                let run = drive(&mut tree, History::Engine(Workload::UniformUpdate), writes);
                report(
                    &format!("{data}, uniform overwrite"),
                    &format!("leveled {ratio}, {limits}"),
                    &tree,
                    &run,
                );
                assert!(run.completed && tree.stats.refused_job == 0);
                assert!(tree.stats.largest_job <= budget);
            }
        }
    }
}

/// Section 5: flush splitting's fragmentation bounds.
#[test]
#[ignore = "research: run with --release --ignored --nocapture"]
fn flush_splitting_fragmentation() {
    let writes = writes_from_env("LEVELING_SPLIT_WRITES", 200);
    println!(
        "SPLIT | history | split | stale | flush pieces (max per flush, smallest B) | {}",
        &HEADER[11..]
    );
    let splits = [
        ("none", None),
        (
            "unbounded",
            Some(Split {
                min_piece: 0.0,
                max_pieces: usize::MAX,
            }),
        ),
        (
            "1/16, 16",
            Some(Split {
                min_piece: 1.0 / 16.0,
                max_pieces: 16,
            }),
        ),
        (
            "1/8, 8",
            Some(Split {
                min_piece: 1.0 / 8.0,
                max_pieces: 8,
            }),
        ),
        (
            "1/4, 4",
            Some(Split {
                min_piece: 1.0 / 4.0,
                max_pieces: 4,
            }),
        ),
    ];
    let histories = [
        ("uniform insert", History::Engine(Workload::UniformInsert)),
        ("hot-range overwrite", History::Engine(Workload::Hot)),
        ("ascending insert", History::Engine(Workload::Ascending)),
        (
            "uniform overwrite",
            History::Engine(Workload::UniformUpdate),
        ),
    ];
    for (label, history) in histories {
        for (name, split) in splits {
            for stale in [0, 8] {
                if stale > 0 && split.is_none() {
                    continue;
                }
                let mut tree = Tree::new(
                    Config {
                        split,
                        stale,
                        ..leveled(TEXT)
                    },
                    sizes(TEXT),
                );
                let run = drive(&mut tree, history, writes);
                let stats = &tree.stats;
                print!(
                    "SPLIT | {label} | {name} | {stale} | {} ({}, {}) ",
                    stats.flush_pieces,
                    stats.max_flush_pieces,
                    if stats.smallest_piece == u64::MAX {
                        0
                    } else {
                        stats.smallest_piece
                    },
                );
                report(label, name, &tree, &run);
            }
        }
    }
    // Many nearby boundaries: level 1 recovered in pieces of a sixty-fourth
    // of a segment, then uniform overwrites.
    for (name, split) in splits {
        let mut tree = Tree::new(
            Config {
                target: TARGET / 64,
                split,
                ..leveled(TEXT)
            },
            sizes(TEXT),
        );
        tree.recover(50_000);
        tree.config.target = TARGET;
        let mut rng = Rng(3);
        let start = Instant::now();
        let mut completed = true;
        for _ in 0..50_000 {
            completed &= tree.write(rng.below(50_000), false);
        }
        completed &= tree.flush();
        let run = Run {
            completed,
            seconds: start.elapsed().as_secs_f64(),
        };
        let stats = &tree.stats;
        print!(
            "SPLIT | dense boundaries | {name} | 0 | {} ({}, {}) ",
            stats.flush_pieces,
            stats.max_flush_pieces,
            if stats.smallest_piece == u64::MAX {
                0
            } else {
                stats.smallest_piece
            },
        );
        report("dense boundaries", name, &tree, &run);
    }
}

// Engine-level checks: small real runs and the production compaction job.

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

fn random_bytes(rng: &mut Rng, len: usize) -> Vec<u8> {
    (0..len).map(|_| rng.below(256) as u8).collect()
}

/// Section 4 (measured): stored and held bytes of flushed segments against
/// their logical length for each codec and kind of value, and the largest
/// segment a compaction can write: the target plus one record.
#[test]
#[ignore = "research: run with --release --ignored --nocapture"]
fn segment_sizes_against_job_limits() {
    const RUN: usize = 1 << 20;
    println!(
        "SEGMENT | codec | values | records | logical KiB | runs | stored+held / logical | largest run / target |"
    );
    for compression in [
        Compression::Zstd,
        Compression::Lz4,
        Compression::ZstdDictionary,
        Compression::Uncompressed,
    ] {
        for kind in ["text", "JSON", "random 1 KiB", "tiny"] {
            let engine = Engine::new(EngineOptions {
                memtable_budget: 8 * RUN,
                target_run_len: RUN,
                max_runs: 1 << 16,
                background_compaction: None,
                compression,
                ..EngineOptions::default()
            })
            .unwrap();
            let mut rng = Rng(3);
            let (mut logical, mut records) = (0usize, 0u64);
            while logical < 6 * RUN {
                let index = rng.below(1 << 40);
                let (key, value) = match kind {
                    "text" => (engine_key(index), super::capacity_tests::value_for(index)),
                    "JSON" => (engine_key(index), json(index)),
                    "random 1 KiB" => (engine_key(index), random_bytes(&mut rng, 1024)),
                    _ => ((index as u32).to_be_bytes().to_vec(), Vec::new()),
                };
                logical += key.len() + value.len() + 16;
                records += 1;
                engine.put(&key, &value).unwrap();
            }
            engine.flush().unwrap();
            // A compaction of everything, so that dictionaries are trained
            // where the codec uses them.
            let picks: Vec<usize> = (0..engine.core.read_state().runs.len()).collect();
            if picks.len() > 1 {
                engine
                    .compact_picked(
                        CompactionOptions {
                            max_input_bytes: usize::MAX,
                            max_output_bytes: usize::MAX,
                            max_inputs: usize::MAX,
                            ..CompactionOptions::default()
                        },
                        picks,
                        &mut MergeWorkspace::new(),
                    )
                    .unwrap();
            }
            let stats = engine.stats();
            let largest = engine
                .core
                .read_state()
                .runs
                .iter()
                .map(|run| run.run.as_bytes().len() + run.run.held_bytes())
                .max()
                .unwrap();
            println!(
                "SEGMENT | {compression:?} | {kind} | {records} | {:.0} | {} | {:.3} | {:.3} |",
                logical as f64 / 1024.0,
                stats.runs,
                (stats.run_bytes + stats.held_bytes) as f64 / logical as f64,
                largest as f64 / RUN as f64,
            );
        }
    }

    // The largest segment: small records up to just under the target, and
    // one value as large as a memtable admits, merged.
    let engine = Engine::new(EngineOptions {
        memtable_budget: 2 * RUN,
        target_run_len: RUN,
        max_runs: 1 << 16,
        background_compaction: None,
        ..EngineOptions::default()
    })
    .unwrap();
    let mut rng = Rng(9);
    for index in 0..(RUN as u64 / 1_100) {
        engine
            .put(&engine_key(index * 4), &random_bytes(&mut rng, 1_000))
            .unwrap();
    }
    engine.flush().unwrap();
    let huge = random_bytes(&mut rng, 2 * RUN - 4_096);
    engine
        .put(&engine_key(RUN as u64 / 1_100 * 2 + 2), &huge)
        .unwrap();
    engine.flush().unwrap();
    let limits = CompactionOptions {
        max_input_bytes: 4 * RUN,
        max_output_bytes: 4 * RUN,
        ..CompactionOptions::default()
    };
    let runs = engine.core.read_state().runs.len();
    engine
        .compact_picked(limits, (0..runs).collect(), &mut MergeWorkspace::new())
        .unwrap();
    let sizes: Vec<f64> = engine
        .core
        .read_state()
        .runs
        .iter()
        .map(|run| (run.run.as_bytes().len() + run.run.held_bytes()) as f64 / RUN as f64)
        .collect();
    println!(
        "SEGMENT | largest | runs after merging under a one-record overshoot: {sizes:.3?} targets |"
    );

    // Two such segments meeting in one job exceed four targets of output.
    for index in 0..(RUN as u64 / 1_100) {
        engine
            .put(&engine_key(index * 4 + 1), &random_bytes(&mut rng, 1_000))
            .unwrap();
    }
    engine.flush().unwrap();
    engine
        .put(&engine_key(RUN as u64 / 1_100 * 2 + 3), &huge)
        .unwrap();
    engine.flush().unwrap();
    let runs = engine.core.read_state().runs.len();
    let before: Vec<f64> = engine
        .core
        .read_state()
        .runs
        .iter()
        .map(|run| (run.run.as_bytes().len() + run.run.held_bytes()) as f64 / RUN as f64)
        .collect();
    println!("SEGMENT | largest | inputs of the second job: {before:.3?} targets |");
    let result = engine.compact_picked(limits, (0..runs).collect(), &mut MergeWorkspace::new());
    if let Ok(report) = &result {
        println!(
            "SEGMENT | largest | second job output {:.3} targets |",
            report.output_bytes as f64 / RUN as f64
        );
    }
    println!(
        "SEGMENT | largest | all {runs} runs in one job under 4 targets: {:?} |",
        result
            .as_ref()
            .map(|report| report.outputs)
            .map_err(|error| error.to_string())
    );
    assert!(matches!(result, Err(CompactionError::OutputLimit { .. })));
    assert_eq!(
        engine.core.read_state().runs.len(),
        runs,
        "a refused job changes nothing"
    );
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
        max_runs: 1 << 16,
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
    // Recovery: every run in one sorted, disjoint level; one group.
    let mut recovered = engine.core.read_state().runs.clone();
    recovered.sort_by_key(|run| range(run).0);
    assert!(recovered.len() > 4 && disjoint(&recovered));
    assert_eq!(engine.core.read_state().directory.group_count(), 1);
    // Level 0, then levels 1 and 2: recovery's runs are the last level, and
    // a level is inserted above it when it holds more than four runs.
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
                CompactionOptions::default(),
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
            snapshot = Some((engine.snapshot().unwrap(), model.clone()));
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
