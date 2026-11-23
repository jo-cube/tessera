//! Compaction's view of the published runs: levels, kept beside the run
//! list and the directory and never read by lookups or scans.
//!
//! Level 0 holds flushed runs in publication order, which may overlap. Each
//! deeper level is sorted by smallest key and strictly disjoint. Every
//! published run is in exactly one level. Reads never consult levels: they
//! go through the [`RunDirectory`](super::directory::RunDirectory), which is
//! built from the run set alone and orders by sequence number, so a level
//! tag can only change what compaction does next, never what a read returns.
//! Checkpoints preserve level membership and level-0 flush tags. Scheduling
//! cursors and size estimates restart on recovery.

use std::ops::Range;
use std::sync::Arc;

use crate::memtable::FlushCuts;
use crate::run::workspace_estimate;

use super::{NOT_EMPTY, SharedRun, physical_runs};

/// Level 1's least target, in segments.
const BASE_SEGMENTS: usize = 4;

/// A flush is split at level 1's boundaries into pieces of at least this
/// fraction of a run length of input...
const FLUSH_MIN_PIECE: usize = 8;

/// ... and at most this many pieces per run length.
const FLUSH_MAX_PIECES: usize = 8;

/// What one compaction step does, chosen from metadata alone.
#[derive(Debug)]
pub(super) enum Plan {
    /// Nothing is due.
    Idle,
    /// Relabel `run` from `level` to the next, which it overlaps nowhere.
    Move { level: usize, run: SharedRun },
    /// Merge `sources` of `level` with the runs `dests` of the next level
    /// that they overlap, into the next level.
    Merge {
        level: usize,
        sources: Vec<SharedRun>,
        dests: Vec<SharedRun>,
        /// Smallest keys of the runs below the destination within the
        /// job's range, at which outputs are cut to bound their next job.
        below: Vec<Vec<u8>>,
    },
    /// Merge two adjacent small runs of `level` into one.
    Neighbours { level: usize, runs: [SharedRun; 2] },
    /// Rewrite an isolated run to discard its tombstones or old versions.
    Clean { level: usize, run: SharedRun },
}

/// Scheduling bounds on optional fan-in, from the internal policy.
#[derive(Clone, Copy, Debug)]
pub(super) struct Fit {
    pub(super) max_inputs: usize,
    /// Soft bytes of optional inputs.
    pub(super) max_bytes: usize,
    pub(super) max_workspace_bytes: usize,
    /// Level 0 is compacted once it holds runs of this many flushes.
    pub(super) trigger: usize,
    /// The flushes level 0 may hold before writers wait.
    pub(super) level0_stop: usize,
    /// The engine's run length, which bounds a segment.
    pub(super) target_run_len: usize,
}

impl Fit {
    fn fits(&self, inputs: usize, bytes: usize, workspace: usize) -> bool {
        inputs <= self.max_inputs
            && bytes <= self.max_bytes
            && workspace <= self.max_workspace_bytes
    }
}

/// Where a publication's outputs go.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Dest {
    /// Into this level, at least 1, in key order beside its other runs.
    Level(usize),
    /// Into `level` if they fit there, or else into level 0 as a new flush:
    /// for the tests' arbitrary merges.
    #[cfg(test)]
    Anywhere(usize),
}

/// How much a keyspace's levels need compaction, compared across keyspaces
/// to choose the one compaction serves: a level 0 at the writer stop first,
/// then the highest level score, then small neighbours to merge.
#[derive(Clone, Copy, Debug, Default)]
pub(super) struct Score {
    class: u8,
    /// The score as a fraction, compared exactly.
    numerator: usize,
    denominator: usize,
}

impl Score {
    fn new(class: u8, numerator: usize, denominator: usize) -> Self {
        Self {
            class,
            numerator,
            denominator,
        }
    }
}

impl Ord for Score {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        let left = self.numerator as u128 * other.denominator as u128;
        let right = other.numerator as u128 * self.denominator as u128;
        self.class.cmp(&other.class).then(left.cmp(&right))
    }
}

impl PartialOrd for Score {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for Score {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other).is_eq()
    }
}

impl Eq for Score {}

/// Why levels refused a change: what selection saw has since changed.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct Stale;

pub(super) struct Levels {
    ratio: usize,
    /// `levels[0]` in publication order; the rest sorted and disjoint.
    levels: Vec<Vec<SharedRun>>,
    /// The flush each level-0 run came from, beside `levels[0]`. A flush's
    /// runs stay next to each other, so counting changes counts flushes.
    flushes: Vec<u64>,
    next_flush: u64,
    /// Stored and held bytes per level.
    bytes: Vec<usize>,
    /// Per level, the key after the last source taken from it.
    cursors: Vec<Vec<u8>>,
    /// Logical bytes (keys, values and per-record overhead, as run lengths
    /// count them) and stored bytes of every flush, for the stored size of
    /// a full run.
    flushed_logical: u64,
    flushed_stored: u64,
}

fn bytes_of(run: &SharedRun) -> usize {
    run.run.stored_len() + run.run.held_bytes()
}

fn total_bytes(runs: &[SharedRun]) -> usize {
    physical_runs(runs).map(bytes_of).sum()
}

fn min_of(run: &SharedRun) -> &[u8] {
    run.run.key_range().expect(NOT_EMPTY).min
}

fn max_of(run: &SharedRun) -> &[u8] {
    run.run.key_range().expect(NOT_EMPTY).max
}

/// The smallest key above `key`.
fn after(key: &[u8]) -> Vec<u8> {
    let mut next = key.to_vec();
    next.push(0);
    next
}

/// The runs of a sorted, disjoint level whose ranges share a key with
/// `lo..=hi`, by binary search.
fn overlapping(level: &[SharedRun], lo: &[u8], hi: &[u8]) -> Range<usize> {
    let start = level.partition_point(|run| max_of(run) < lo);
    let end = level.partition_point(|run| min_of(run) <= hi);
    start..end.max(start)
}

fn workspace(runs: &[&SharedRun]) -> usize {
    workspace_estimate(runs.iter().map(|run| run.largest_block))
}

impl Levels {
    /// Level 0 and one empty level below it.
    pub(super) fn new(ratio: usize) -> Self {
        Self {
            ratio,
            levels: vec![Vec::new(), Vec::new()],
            flushes: Vec::new(),
            next_flush: 0,
            bytes: vec![0, 0],
            cursors: vec![Vec::new(), Vec::new()],
            flushed_logical: 0,
            flushed_stored: 0,
        }
    }

    /// Structural checkpoint order: level 0 in publication order, then each
    /// disjoint level in key order. Flush tags preserve level-0 backpressure.
    pub(super) fn checkpoint_runs(&self) -> Vec<(usize, u64, SharedRun)> {
        self.levels
            .iter()
            .enumerate()
            .flat_map(|(level, runs)| {
                runs.iter().enumerate().map(move |(at, run)| {
                    (
                        level,
                        if level == 0 { self.flushes[at] } else { 0 },
                        Arc::clone(run),
                    )
                })
            })
            .collect()
    }

    pub(super) fn next_flush(&self) -> u64 {
        self.next_flush
    }

    /// Restore the recorded shape without compacting or changing sequences.
    pub(super) fn recovered(runs: &[(usize, u64, SharedRun)], depth: usize, ratio: usize) -> Self {
        let mut levels = Self::new(ratio);
        levels.levels.resize_with(depth.max(1) + 1, Vec::new);
        levels.bytes.resize(depth.max(1) + 1, 0);
        levels.cursors.resize_with(depth.max(1) + 1, Vec::new);
        for &(level, flush, ref run) in runs {
            levels.levels[level].push(Arc::clone(run));
            levels.bytes[level] += bytes_of(run);
            if level == 0 {
                levels.flushes.push(flush);
                levels.next_flush = levels.next_flush.max(flush + 1);
            }
        }
        levels.recount();
        levels
    }

    pub(super) fn depth(&self) -> usize {
        self.levels.len() - 1
    }

    pub(super) fn has_level0(&self) -> bool {
        !self.flushes.is_empty()
    }

    /// Flushes level 0 holds runs of.
    pub(super) fn level0_flushes(&self) -> usize {
        let mut count = 0;
        let mut last = None;
        for &flush in &self.flushes {
            if last != Some(flush) {
                count += 1;
                last = Some(flush);
            }
        }
        count
    }

    /// Each level's runs and bytes, level 0 first.
    #[cfg_attr(not(any(test, feature = "diagnostics")), allow(dead_code))]
    pub(super) fn shape(&self) -> Vec<(usize, usize)> {
        self.levels
            .iter()
            .zip(&self.bytes)
            .map(|(runs, &bytes)| (runs.len(), bytes))
            .collect()
    }

    /// The runs of each level, level 0 first.
    #[cfg(any(test, feature = "diagnostics"))]
    pub(super) fn runs(&self) -> &[Vec<SharedRun>] {
        &self.levels
    }

    /// Adds a flush's runs, disjoint and in key order, to level 0, built
    /// from `logical` bytes of records.
    pub(super) fn push_flush(&mut self, runs: &[SharedRun], logical: usize) {
        let flush = self.next_flush;
        self.next_flush += 1;
        self.levels[0].extend(runs.iter().cloned());
        self.flushes.extend(runs.iter().map(|_| flush));
        let stored = runs.iter().map(bytes_of).sum::<usize>();
        self.bytes[0] += stored;
        self.flushed_logical = self.flushed_logical.saturating_add(logical as u64);
        self.flushed_stored = self.flushed_stored.saturating_add(stored as u64);
    }

    /// Where a flush is split: at the smallest keys of level 1's runs, so
    /// that each piece's level-0 job meets fewer of them.
    pub(super) fn flush_cuts(&self, target_run_len: usize) -> FlushCuts {
        FlushCuts {
            at: self.levels[1]
                .iter()
                .map(|run| min_of(run).to_vec())
                .collect(),
            min_piece: (target_run_len / FLUSH_MIN_PIECE).max(1),
            max_pieces: FLUSH_MAX_PIECES,
        }
    }

    /// Cuts for a quiet tail flush: when level 0 holds runs and level 1 exists,
    /// align to the level-1 boundaries of the oldest level-0 flush group so
    /// each tail slice merges in the same settle job as the overlapping slice
    /// instead of spanning all of level 1 and forcing a second rewrite.
    pub(super) fn quiet_flush_cuts(&self, target_run_len: usize) -> FlushCuts {
        let Some(oldest) = self.flushes.first().copied() else {
            return self.flush_cuts(target_run_len);
        };
        let l1 = &self.levels[1];
        if l1.is_empty() {
            return self.flush_cuts(target_run_len);
        }
        let mut at = Vec::new();
        for (run, &flush) in self.levels[0].iter().zip(&self.flushes) {
            if flush != oldest {
                break;
            }
            let min = min_of(run);
            let idx = l1.partition_point(|below| min_of(below) <= min);
            if let Some(pos) = idx.checked_sub(1)
                && pos > 0
            {
                let bound = min_of(&l1[pos]);
                if at
                    .last()
                    .is_none_or(|prev: &Vec<u8>| prev.as_slice() < bound)
                {
                    at.push(bound.to_vec());
                }
            }
        }
        if at.is_empty() {
            return self.flush_cuts(target_run_len);
        }
        let max_pieces = (at.len() + 1).min(FLUSH_MAX_PIECES);
        FlushCuts {
            at,
            min_piece: 1,
            max_pieces,
        }
    }

    /// The level holding `run`.
    #[cfg(test)]
    pub(super) fn level_of(&self, run: &SharedRun) -> Option<usize> {
        self.find(run).map(|(level, _)| level)
    }

    /// Where `run` is, by identity: level 0 by scan, the others by search.
    fn find(&self, run: &SharedRun) -> Option<(usize, usize)> {
        if let Some(at) = self.levels[0]
            .iter()
            .position(|other| Arc::ptr_eq(other, run))
        {
            return Some((0, at));
        }
        let min = min_of(run);
        self.levels
            .iter()
            .enumerate()
            .skip(1)
            .find_map(|(level, runs)| {
                let at = runs.partition_point(|other| min_of(other) < min);
                runs.get(at)
                    .filter(|other| Arc::ptr_eq(other, run))
                    .map(|_| (level, at))
            })
    }

    /// Replaces `inputs` with `outputs`, which are in key order and
    /// disjoint, placing them as `dest` says. Checks everything before
    /// changing anything: on [`Stale`] the levels are as they were.
    pub(super) fn replace(
        &mut self,
        inputs: &[SharedRun],
        outputs: &[SharedRun],
        dest: Dest,
    ) -> Result<(), Stale> {
        let mut found = Vec::with_capacity(inputs.len());
        for input in inputs {
            found.push(self.find(input).ok_or(Stale)?);
        }
        for pair in outputs.windows(2) {
            if max_of(&pair[0]) >= min_of(&pair[1]) {
                return Err(Stale);
            }
        }
        let is_input = |run: &SharedRun| inputs.iter().any(|input| Arc::ptr_eq(input, run));
        let span = outputs
            .first()
            .zip(outputs.last())
            .map(|(first, last)| (min_of(first).to_vec(), max_of(last).to_vec()));
        let fits_level = |level: usize| -> bool {
            let Some((lo, hi)) = &span else {
                return true;
            };
            let Some(runs) = self.levels.get(level) else {
                return false;
            };
            let range = overlapping(runs, lo, hi);
            runs[range].iter().all(is_input)
        };
        let dest = match dest {
            #[cfg(test)]
            Dest::Anywhere(level) if level >= 1 && fits_level(level) => Dest::Level(level),
            #[cfg(test)]
            Dest::Anywhere(_) => {
                self.remove(&found);
                self.push_flush(outputs, 0);
                return Ok(());
            }
            Dest::Level(level) if level >= 1 && fits_level(level) => dest,
            Dest::Level(_) => return Err(Stale),
        };
        match dest {
            Dest::Level(level) => {
                self.remove(&found);
                let runs = &mut self.levels[level];
                let at = match &span {
                    Some((lo, _)) => runs.partition_point(|run| max_of(run) < lo.as_slice()),
                    None => 0,
                };
                runs.splice(at..at, outputs.iter().cloned());
                self.bytes[level] += outputs.iter().map(bytes_of).sum::<usize>();
            }
            #[cfg(test)]
            Dest::Anywhere(_) => unreachable!("resolved above"),
        }
        self.recount();
        Ok(())
    }

    /// Removes the runs at `found`, which are valid positions.
    fn remove(&mut self, found: &[(usize, usize)]) {
        let mut found = found.to_vec();
        // Back to front, so that earlier positions stay valid.
        found.sort_unstable_by(|a, b| b.cmp(a));
        for (level, at) in found {
            let run = self.levels[level].remove(at);
            if level == 0 {
                self.flushes.remove(at);
            }
            let _ = run;
        }
        self.recount();
    }

    /// Publish one bottom piece and narrow its level-0 sources, preserving
    /// publication order and flush tags. Validate everything before changing it.
    pub(super) fn replace_piece(
        &mut self,
        sources: &[(SharedRun, Option<SharedRun>)],
        bottom: &[SharedRun],
        outputs: &[SharedRun],
        dest: Dest,
    ) -> Result<(), Stale> {
        let mut positions = Vec::with_capacity(sources.len());
        for (old, new) in sources {
            let (level, at) = self.find(old).ok_or(Stale)?;
            if level != 0 {
                return Err(Stale);
            }
            if let Some(new) = new {
                let (old_range, new_range) =
                    (old.run.key_range().unwrap(), new.run.key_range().unwrap());
                if new_range.min < old_range.min || new_range.max > old_range.max {
                    return Err(Stale);
                }
            }
            positions.push(at);
        }
        self.replace(bottom, outputs, dest)?;
        let mut replacements: Vec<_> = positions.into_iter().zip(sources).collect();
        replacements.sort_unstable_by_key(|(at, _)| std::cmp::Reverse(*at));
        for (at, (old, new)) in replacements {
            let _ = old;
            if let Some(new) = new {
                self.levels[0][at] = Arc::clone(new);
            } else {
                self.levels[0].remove(at);
                self.flushes.remove(at);
            }
        }
        self.recount();
        Ok(())
    }

    fn recount(&mut self) {
        for (bytes, runs) in self.bytes.iter_mut().zip(&self.levels) {
            *bytes = total_bytes(runs);
        }
    }

    /// Install disjoint views in place of L0 sources without changing their
    /// flush tags. Each view shares the original physical run and sequences.
    pub(super) fn split_sources(
        &mut self,
        sources: &[(SharedRun, Vec<SharedRun>)],
    ) -> Result<(), Stale> {
        let mut positions = Vec::new();
        for (old, views) in sources {
            let (level, at) = self.find(old).ok_or(Stale)?;
            if level != 0
                || views.is_empty()
                || views
                    .iter()
                    .any(|r| r.run.storage_id() != old.run.storage_id())
            {
                return Err(Stale);
            }
            for pair in views.windows(2) {
                if max_of(&pair[0]) >= min_of(&pair[1]) {
                    return Err(Stale);
                }
            }
            positions.push(at);
        }
        let mut replacements: Vec<_> = positions.into_iter().zip(sources).collect();
        replacements.sort_unstable_by_key(|(at, _)| std::cmp::Reverse(*at));
        for (at, (_, views)) in replacements {
            let tag = self.flushes[at];
            self.levels[0].splice(at..at + 1, views.iter().cloned());
            self.flushes.splice(at..at + 1, views.iter().map(|_| tag));
        }
        self.recount();
        Ok(())
    }

    /// Relabels `run` from `level` to the next level, which must have no
    /// run overlapping it.
    pub(super) fn move_down(&mut self, level: usize, run: &SharedRun) -> Result<(), Stale> {
        match self.find(run) {
            Some((found, _)) if found == level && level + 1 < self.levels.len() => {}
            _ => return Err(Stale),
        }
        self.replace(
            std::slice::from_ref(run),
            std::slice::from_ref(run),
            Dest::Level(level + 1),
        )?;
        if level > 0 {
            self.cursors[level] = after(max_of(run));
        }
        Ok(())
    }

    /// Notes that a job took `source` from `level`, so that the next starts
    /// after it.
    pub(super) fn advance(&mut self, level: usize, source: &[u8]) {
        if level > 0 {
            self.cursors[level] = after(source);
        }
    }

    /// The published runs outside `inputs` whose ranges share a key with
    /// `lo..=hi`: every level 0 run by scan, every deeper level by search.
    pub(super) fn others(&self, inputs: &[SharedRun], lo: &[u8], hi: &[u8]) -> Vec<SharedRun> {
        let is_input = |run: &SharedRun| inputs.iter().any(|input| Arc::ptr_eq(input, run));
        let level0 = self.levels[0]
            .iter()
            .filter(|run| min_of(run) <= hi && lo <= max_of(run));
        let deeper = self.levels[1..]
            .iter()
            .flat_map(|runs| runs[overlapping(runs, lo, hi)].iter());
        level0
            .chain(deeper)
            .filter(|run| !is_input(run))
            .cloned()
            .collect()
    }

    /// The stored and held bytes of a full run: the run length at the
    /// stored-to-logical ratio flushes have had, once they have written a
    /// run length; before that (after recovery), the largest run below
    /// level 0, or level 0's. At most the run length. It sizes level 1 and
    /// decides which runs are small.
    pub(super) fn segment(&self, fit: &Fit) -> usize {
        let target = fit.target_run_len.max(1);
        if self.flushed_logical >= target as u64 {
            let scaled =
                target as u128 * self.flushed_stored as u128 / self.flushed_logical as u128;
            return (scaled as usize).clamp(1, target);
        }
        let largest = |runs: &[SharedRun]| runs.iter().map(bytes_of).max();
        self.levels[1..]
            .iter()
            .filter_map(|runs| largest(runs))
            .max()
            .or_else(|| largest(&self.levels[0]))
            .unwrap_or(target)
            .clamp(1, target)
    }

    fn target(&self, level: usize, base: usize) -> usize {
        let last = self.levels.len() - 1;
        let shrink = self.ratio.saturating_pow((last - level) as u32);
        (self.bytes[last] / shrink).max(base)
    }

    /// Adds empty levels under level 0 while the last level is too large
    /// for the levels above it: dynamic level sizing. Moves nothing.
    /// Returns how many were added.
    pub(super) fn insert_levels(&mut self, fit: &Fit) -> usize {
        let base = BASE_SEGMENTS.saturating_mul(self.segment(fit));
        let mut added = 0;
        loop {
            let last = self.levels.len() - 1;
            let shrink = self.ratio.saturating_pow(last as u32 - 1);
            if self.bytes[last] / shrink <= base.saturating_mul(self.ratio) {
                return added;
            }
            self.levels.insert(1, Vec::new());
            self.bytes.insert(1, 0);
            self.cursors.insert(1, Vec::new());
            added += 1;
        }
    }

    /// The next step: the highest-scoring level, then small neighbours, or idle.
    pub(super) fn plan(&self, fit: &Fit) -> Plan {
        let segment = self.segment(fit);
        if let Some((level, _, _)) = self.due(fit, segment) {
            return self.plan_level(level, fit, false);
        }
        self.plan_neighbours(fit, segment).unwrap_or(Plan::Idle)
    }

    /// How much [`Self::plan`] has to do, or `None` if it would be idle.
    pub(super) fn score(&self, fit: &Fit) -> Option<Score> {
        if self.level0_flushes() >= fit.level0_stop {
            return Some(Score::new(2, 1, 1));
        }
        let segment = self.segment(fit);
        if let Some((_, numerator, denominator)) = self.due(fit, segment) {
            return Some(Score::new(1, numerator, denominator));
        }
        self.plan_neighbours(fit, segment)
            .map(|_| Score::new(0, 1, 1))
    }

    /// Bytes a reclaiming compaction could free at most: every level above
    /// the last, whose versions merging may discard, and the runs of the
    /// last level holding versions or tombstones to discard.
    pub(super) fn reclaimable_bytes(&self) -> usize {
        let last = self.levels.len() - 1;
        let above: usize = self.bytes[..last].iter().sum();
        let below: usize = self.levels[last]
            .iter()
            .filter(|run| run.run.reclaimable())
            .map(bytes_of)
            .sum();
        above + below
    }

    /// The highest-scoring level over its target, with its score as a
    /// fraction.
    fn due(&self, fit: &Fit, segment: usize) -> Option<(usize, usize, usize)> {
        let base = BASE_SEGMENTS.saturating_mul(segment);
        let last = self.levels.len() - 1;
        // Scores as fractions: (numerator, denominator), compared exactly.
        let mut due: Vec<(usize, usize, usize)> = Vec::new();
        // At most the stop, so that level 0 is due whenever flushes wait
        // for it: a pass of the worker never ends idle with level 0 at its
        // stop.
        let trigger = fit.trigger.min(fit.level0_stop).max(1);
        let flushes = self.level0_flushes();
        // Level 0's score is 1 however many flushes it holds, so a level
        // below over its target goes first. Scoring level 0 by its flushes
        // would let a backlog (the worker behind the writers) grow level 1
        // to the backlog's multiple of its target, and every level-0 merge
        // would then rewrite that much more of it.
        if flushes >= trigger {
            due.push((0, 1, 1));
        }
        for level in 1..last {
            let target = self.target(level, base).max(1);
            if self.bytes[level] > target {
                due.push((level, self.bytes[level], target));
            }
        }
        // Highest score first; equal scores, the shallower level.
        due.sort_by(|a, b| {
            let left = a.1 as u128 * b.2 as u128;
            let right = b.1 as u128 * a.2 as u128;
            right.cmp(&left).then(a.0.cmp(&b.0))
        });
        due.first().copied()
    }

    /// Pressure may reclaim versions below ordinary level targets. Every job
    /// still follows the same source, overlap and output-cut policy.
    pub(super) fn plan_reclaim(&self, fit: &Fit) -> Plan {
        let eager = Fit { trigger: 1, ..*fit };
        let plan = self.plan(&eager);
        if !matches!(plan, Plan::Idle) {
            return plan;
        }
        let last = self.levels.len() - 1;
        for level in 1..last {
            for run in &self.levels[level] {
                if self.levels[level + 1..]
                    .iter()
                    .any(|below| !overlapping(below, min_of(run), max_of(run)).is_empty())
                {
                    // Use the existing cursor choice; moving other sources out
                    // of its way also makes finite progress towards this run.
                    return self.plan_level(level, &eager, false);
                }
            }
        }
        for level in 1..=last {
            for run in &self.levels[level] {
                if run.run.reclaimable()
                    && self
                        .others(std::slice::from_ref(run), min_of(run), max_of(run))
                        .is_empty()
                {
                    return Plan::Clean {
                        level,
                        run: Arc::clone(run),
                    };
                }
            }
        }
        Plan::Idle
    }

    /// Promotes a level-0 slice below the usual flush threshold.
    pub(super) fn plan_idle(&self, fit: &Fit) -> Plan {
        self.plan_level(0, fit, true)
    }

    fn plan_level(&self, level: usize, fit: &Fit, idle: bool) -> Plan {
        let here = &self.levels[level];
        let at = if level == 0 {
            0
        } else {
            let at = here.partition_point(|run| max_of(run) < self.cursors[level].as_slice());
            if at == here.len() { 0 } else { at }
        };
        let source = &here[at];
        let below = &self.levels[level + 1];
        let dests = overlapping(below, min_of(source), max_of(source));
        // An isolated promotion into the bottom can also discard its filter
        // and any tombstones that unselected runs cannot contest.
        let clean = idle
            && self.depth() == 1
            && (source.run.reclaimable() || source.run.filter_bytes() != 0);
        if dests.is_empty() && !clean {
            return Plan::Move {
                level,
                run: Arc::clone(source),
            };
        }
        let mut sources = vec![source];
        let size = |sources: &[&SharedRun], dests: &Range<usize>| {
            let runs: Vec<&SharedRun> = sources
                .iter()
                .copied()
                .chain(&below[dests.clone()])
                .collect();
            // The destination overlap is rewritten once for all selected sources.
            let bytes: usize = sources.iter().map(|run| bytes_of(run)).sum();
            (runs.len(), bytes, workspace(&runs))
        };
        let (mut lo, mut hi) = (min_of(source), max_of(source));
        let mut dests = dests;
        if level == 0 {
            // Other level-0 runs overlapping the job join it, oldest first,
            // with the level-1 runs they add, while it fits: reads go by
            // sequence, so any subset of level 0 is correct, and each
            // level-1 range is rewritten once for all the flushes over it.
            let mut changed = true;
            while changed {
                changed = false;
                for run in &here[1..] {
                    if sources.iter().any(|taken| Arc::ptr_eq(taken, run))
                        || max_of(run) < lo
                        || hi < min_of(run)
                    {
                        continue;
                    }
                    let (lo2, hi2) = (lo.min(min_of(run)), hi.max(max_of(run)));
                    let dests2 = overlapping(below, lo2, hi2);
                    sources.push(run);
                    let (inputs, bytes, needed) = size(&sources, &dests2);
                    if fit.fits(inputs, bytes, needed) {
                        (lo, hi, dests) = (lo2, hi2, dests2);
                        changed = true;
                    } else {
                        sources.pop();
                    }
                }
            }
        }
        let below_dest = match self.levels.get(level + 2) {
            Some(runs) => runs[overlapping(runs, lo, hi)]
                .iter()
                .map(|run| min_of(run).to_vec())
                .collect(),
            None => Vec::new(),
        };
        Plan::Merge {
            level,
            sources: sources.into_iter().cloned().collect(),
            dests: below[dests].to_vec(),
            below: below_dest,
        }
    }

    /// Two adjacent runs of a level below 0, both under half a segment,
    /// whose merged range overlaps at most `ratio` segments' bytes below.
    fn plan_neighbours(&self, _fit: &Fit, segment: usize) -> Option<Plan> {
        let small = segment / 2;
        let last = self.levels.len() - 1;
        for level in 1..=last {
            let here = &self.levels[level];
            for pair in here.windows(2) {
                let (a, b) = (&pair[0], &pair[1]);
                if bytes_of(a) >= small || bytes_of(b) >= small {
                    continue;
                }
                let bounded = level == last || {
                    let below = &self.levels[level + 1];
                    let range = overlapping(below, min_of(a), max_of(b));
                    below[range].iter().map(bytes_of).sum::<usize>()
                        <= self.ratio.saturating_mul(segment)
                };
                if bounded {
                    return Some(Plan::Neighbours {
                        level,
                        runs: [Arc::clone(a), Arc::clone(b)],
                    });
                }
            }
        }
        None
    }

    /// Checks the invariants against `runs`, the published set: every run
    /// in exactly one level, deeper levels sorted and strictly disjoint,
    /// level 0's flush tags beside it, and byte counts exact.
    pub(super) fn check(&self, runs: &[SharedRun]) {
        let members: usize = self.levels.iter().map(Vec::len).sum();
        assert_eq!(members, runs.len(), "levels hold every published run once");
        let mut seen: Vec<*const super::Published> =
            self.levels.iter().flatten().map(Arc::as_ptr).collect();
        seen.sort_unstable();
        seen.dedup();
        assert_eq!(seen.len(), members, "a run in two levels");
        for run in runs {
            assert!(
                seen.binary_search(&Arc::as_ptr(run)).is_ok(),
                "a published run in no level"
            );
        }
        assert_eq!(self.flushes.len(), self.levels[0].len());
        assert!(self.levels.len() >= 2);
        assert_eq!(self.bytes.len(), self.levels.len());
        assert_eq!(self.cursors.len(), self.levels.len());
        for (level, runs) in self.levels.iter().enumerate() {
            assert_eq!(self.bytes[level], total_bytes(runs), "level {level} bytes");
            if level > 0 {
                for pair in runs.windows(2) {
                    assert!(
                        max_of(&pair[0]) < min_of(&pair[1]),
                        "level {level} is not sorted and disjoint"
                    );
                }
            }
        }
        for pair in self.flushes.windows(2) {
            assert!(pair[0] <= pair[1], "level 0 is in publication order");
        }
    }
}
