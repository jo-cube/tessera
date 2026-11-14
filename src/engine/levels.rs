//! Compaction's view of the published runs: levels, kept beside the run
//! list and the directory and never read by lookups or scans.
//!
//! Level 0 holds flushed runs in publication order, which may overlap. Each
//! deeper level is sorted by smallest key and strictly disjoint. Every
//! published run is in exactly one level. Reads never consult levels: they
//! go through the [`RunDirectory`](super::directory::RunDirectory), which is
//! built from the run set alone and orders by sequence number, so a level
//! tag can only change what compaction does next, never what a read returns.
//! Nothing here is persisted: recovery puts a checkpoint's runs, which are
//! disjoint, into the last level.
//!
//! See `docs/capacity-design.md`, sections 4 and 5, for the policy and its
//! invariants.

use std::ops::Range;
use std::sync::{Arc, Weak};

use crate::memtable::FlushCuts;
use crate::run::workspace_estimate;

use super::{NOT_EMPTY, SharedRun};

/// Each level's size target is this many times the one above it.
pub(super) const RATIO: usize = 4;

/// Level 1's least target, in segments.
const BASE_SEGMENTS: usize = 4;

/// Flushes level 0 may hold. A flush that would add one more is refused
/// until compaction drains level 0.
pub(super) const LEVEL0_STOP: usize = 12;

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
    /// Rewrite `source` alone, in its level, cut at `cuts`, because its job
    /// does not fit and each piece's does.
    Split {
        level: usize,
        source: SharedRun,
        cuts: Vec<Vec<u8>>,
        /// The whole job, reported if the split leaves one piece.
        refused: Refusal,
    },
    /// Merge two adjacent small runs of `level` into one.
    Neighbours { level: usize, runs: [SharedRun; 2] },
    /// A level needs compacting, but not even its smallest job fits.
    Refuse(Refusal),
}

/// The smallest job a level needed, which the limits refused.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct Refusal {
    pub(super) level: usize,
    pub(super) inputs: usize,
    pub(super) bytes: usize,
    pub(super) workspace_bytes: usize,
    /// Set when the job was refused under a source's allowance after its
    /// output outgrew the limit: the output bytes that crossed it.
    pub(super) outgrown: Option<usize>,
}

/// A run whose job failed at the output limit, and the most input bytes a
/// job taking it as its source may have since. Kept while the run is
/// published.
struct Outgrown {
    source: Weak<super::Published>,
    max_bytes: usize,
    output_bytes: usize,
}

/// The limits a job must fit, from the compaction options.
#[derive(Clone, Copy, Debug)]
pub(super) struct Fit {
    pub(super) max_inputs: usize,
    /// The lesser of the input limit and the output limit less a
    /// [`OUTPUT_HEADROOM`](super::OUTPUT_HEADROOM)th: a merge's output is
    /// estimated by its input, and can exceed it by run overhead (up to
    /// about a kilobyte a job measured), so a job admitted at the output
    /// limit itself would fail there, and be chosen again, every time.
    pub(super) max_bytes: usize,
    pub(super) max_workspace_bytes: usize,
    /// Level 0 is compacted once it holds runs of this many flushes.
    pub(super) trigger: usize,
    /// The flushes level 0 may hold before flushes wait or are refused.
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
    /// Into level 0 in place of the one input there, as part of its flush.
    InPlace,
    /// Into `level` if they fit there, or else into level 0 as a new flush:
    /// for the tests' arbitrary merges.
    #[cfg(test)]
    Anywhere(usize),
}

/// Why levels refused a change: what selection saw no longer holds.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct Stale;

pub(super) struct Levels {
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
    /// Sources whose jobs outgrew the output limit; see [`Levels::outgrown`].
    outgrown: Vec<Outgrown>,
}

fn bytes_of(run: &SharedRun) -> usize {
    run.run.as_bytes().len() + run.run.held_bytes()
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
    pub(super) fn new() -> Self {
        Self {
            levels: vec![Vec::new(), Vec::new()],
            flushes: Vec::new(),
            next_flush: 0,
            bytes: vec![0, 0],
            cursors: vec![Vec::new(), Vec::new()],
            flushed_logical: 0,
            flushed_stored: 0,
            outgrown: Vec::new(),
        }
    }

    /// Recovered runs, sorted and disjoint, as the last level.
    pub(super) fn recovered(runs: &[SharedRun]) -> Self {
        let mut levels = Self::new();
        levels.bytes[1] = runs.iter().map(bytes_of).sum();
        levels.levels[1] = runs.to_vec();
        levels
    }

    /// Notes that the job of `inputs`, `inputs[0]` its source, failed when
    /// its output reached `output_bytes`, over the output limit. Its input
    /// was admitted within the limit, so its output outgrew it (as a change
    /// of codec or dictionary can make it). Selection reads the same levels
    /// again, so without this it would choose the same job and fail the
    /// same way every time. From now on a job taking that source may have
    /// at most half this one's input bytes: it takes fewer runs, splits its
    /// source, or is refused before reading anything. Each failure halves
    /// the allowance again, so a source fails at most about
    /// log2(limit / its size) times before its jobs fit or are refused.
    pub(super) fn outgrown(&mut self, inputs: &[SharedRun], output_bytes: usize) {
        let Some(source) = inputs.first() else {
            return;
        };
        let half = inputs.iter().map(bytes_of).sum::<usize>() / 2;
        let source = Arc::downgrade(source);
        self.outgrown
            .retain(|outgrown| outgrown.source.strong_count() > 0);
        match self
            .outgrown
            .iter_mut()
            .find(|outgrown| outgrown.source.ptr_eq(&source))
        {
            Some(outgrown) => {
                outgrown.max_bytes = outgrown.max_bytes.min(half);
                outgrown.output_bytes = output_bytes;
            }
            None => self.outgrown.push(Outgrown {
                source,
                max_bytes: half,
                output_bytes,
            }),
        }
    }

    /// The limits for a job taking `source` as its source: `fit`, or less
    /// if its jobs have outgrown the output limit, with the output that did.
    fn fit_for(&self, source: &SharedRun, fit: &Fit) -> (Fit, Option<usize>) {
        let found = self
            .outgrown
            .iter()
            .find(|outgrown| std::ptr::eq(outgrown.source.as_ptr(), Arc::as_ptr(source)));
        match found {
            Some(outgrown) if outgrown.max_bytes < fit.max_bytes => (
                Fit {
                    max_bytes: outgrown.max_bytes,
                    ..*fit
                },
                Some(outgrown.output_bytes),
            ),
            _ => (*fit, None),
        }
    }

    /// Levels below 0, including empty ones.
    pub(super) fn depth(&self) -> usize {
        self.levels.len() - 1
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
    pub(super) fn shape(&self) -> Vec<(usize, usize)> {
        self.levels
            .iter()
            .zip(&self.bytes)
            .map(|(runs, &bytes)| (runs.len(), bytes))
            .collect()
    }

    /// The runs of each level, level 0 first.
    #[cfg(test)]
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
    /// that each piece's level-0 job meets fewer of them. See
    /// `docs/capacity-design.md`, section 8.
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
            Dest::InPlace => {
                let mut level0 = found.iter().filter(|(level, _)| *level == 0);
                if found.len() != 1 || level0.next().is_none() {
                    return Err(Stale);
                }
                dest
            }
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
            Dest::InPlace => {
                let at = found[0].1;
                let flush = self.flushes[at];
                self.bytes[0] -= bytes_of(&self.levels[0][at]);
                self.levels[0].splice(at..=at, outputs.iter().cloned());
                self.flushes.splice(at..=at, outputs.iter().map(|_| flush));
                self.bytes[0] += outputs.iter().map(bytes_of).sum::<usize>();
            }
            #[cfg(test)]
            Dest::Anywhere(_) => unreachable!("resolved above"),
        }
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
            self.bytes[level] -= bytes_of(&run);
        }
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

    /// Starts the next job of `level` at `key`.
    pub(super) fn rewind(&mut self, level: usize, key: &[u8]) {
        if level > 0 {
            self.cursors[level] = key.to_vec();
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
    fn segment(&self, fit: &Fit) -> usize {
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
        let shrink = RATIO.saturating_pow((last - level) as u32);
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
            let shrink = RATIO.saturating_pow(last as u32 - 1);
            if self.bytes[last] / shrink <= base.saturating_mul(RATIO) {
                return added;
            }
            self.levels.insert(1, Vec::new());
            self.bytes.insert(1, 0);
            self.cursors.insert(1, Vec::new());
            added += 1;
        }
    }

    /// The next step: the level with the highest score above 1 that has a
    /// job that fits, then any small neighbours, then the refusal of the
    /// highest-scoring level, or idle.
    pub(super) fn plan(&self, fit: &Fit) -> Plan {
        let segment = self.segment(fit);
        let base = BASE_SEGMENTS.saturating_mul(segment);
        let last = self.levels.len() - 1;
        // Scores as fractions: (numerator, denominator), compared exactly.
        let mut due: Vec<(usize, usize, usize)> = Vec::new();
        // At most the stop, so that level 0 is due whenever flushes wait
        // or are refused for it: a pass of the worker never ends idle with
        // level 0 at its stop.
        let trigger = fit.trigger.min(fit.level0_stop).max(1);
        let flushes = self.level0_flushes();
        // Level 0's score is 1 however many flushes it holds, so a level
        // below over its target goes first. Scored by its flushes, a level
        // 0 backlog (the worker behind the writers) let level 1 grow to the
        // backlog's multiple of its target, and every level-0 merge then
        // rewrote that much more of it (`docs/capacity-design.md`, section
        // 17).
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
        let mut refused = None;
        for &(level, _, _) in &due {
            match self.plan_level(level, fit) {
                Plan::Refuse(refusal) => {
                    refused.get_or_insert(refusal);
                }
                plan => return plan,
            }
        }
        if let Some(plan) = self.plan_neighbours(fit, segment) {
            return plan;
        }
        refused.map_or(Plan::Idle, Plan::Refuse)
    }

    fn plan_level(&self, level: usize, fit: &Fit) -> Plan {
        let here = &self.levels[level];
        let at = if level == 0 {
            0
        } else {
            let at = here.partition_point(|run| max_of(run) < self.cursors[level].as_slice());
            if at == here.len() { 0 } else { at }
        };
        let source = &here[at];
        let (reduced, outgrown) = self.fit_for(source, fit);
        let fit = &reduced;
        let below = &self.levels[level + 1];
        let dests = overlapping(below, min_of(source), max_of(source));
        if dests.is_empty() {
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
            let bytes: usize = runs.iter().map(|run| bytes_of(run)).sum();
            (runs.len(), bytes, workspace(&runs))
        };
        let (inputs, bytes, needed) = size(&sources, &dests);
        if !fit.fits(inputs, bytes, needed) {
            return self.plan_split(level, source, dests, fit, outgrown);
        }
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

    /// Cuts `source` at the boundaries of the runs below it so that each
    /// piece's job fits now, estimating each piece by the whole source.
    /// Taken only if that leaves at least two pieces; otherwise refused.
    fn plan_split(
        &self,
        level: usize,
        source: &SharedRun,
        dests: Range<usize>,
        fit: &Fit,
        outgrown: Option<usize>,
    ) -> Plan {
        let below = &self.levels[level + 1][dests];
        let all: Vec<&SharedRun> = std::iter::once(source).chain(below).collect();
        let refused = Refusal {
            level,
            inputs: all.len(),
            bytes: all.iter().map(|run| bytes_of(run)).sum(),
            workspace_bytes: workspace(&all),
            outgrown,
        };
        let mut cuts = Vec::new();
        let mut start = 0;
        while start < below.len() {
            let mut end = start;
            while end < below.len() {
                let runs: Vec<&SharedRun> =
                    std::iter::once(source).chain(&below[start..=end]).collect();
                let bytes = runs.iter().map(|run| bytes_of(run)).sum();
                if !fit.fits(runs.len(), bytes, workspace(&runs)) {
                    break;
                }
                end += 1;
            }
            if end == start {
                // The source with one run below does not fit.
                let runs = [source, &below[start]];
                return Plan::Refuse(Refusal {
                    level,
                    inputs: 2,
                    bytes: runs.iter().map(|run| bytes_of(run)).sum(),
                    workspace_bytes: workspace(&runs),
                    outgrown,
                });
            }
            if end < below.len() {
                cuts.push(min_of(&below[end]).to_vec());
            }
            start = end;
        }
        // Only cuts with the source's records on both sides make pieces.
        let (lo, hi) = (min_of(source), max_of(source));
        cuts.retain(|cut| lo < cut.as_slice() && cut.as_slice() <= hi);
        if cuts.is_empty() {
            return Plan::Refuse(refused);
        }
        Plan::Split {
            level,
            source: Arc::clone(source),
            cuts,
            refused,
        }
    }

    /// Two adjacent runs of a level below 0, both under half a segment,
    /// whose merged range overlaps at most [`RATIO`] segments' bytes below.
    fn plan_neighbours(&self, fit: &Fit, segment: usize) -> Option<Plan> {
        let small = segment / 2;
        let last = self.levels.len() - 1;
        for level in 1..=last {
            let here = &self.levels[level];
            for pair in here.windows(2) {
                let (a, b) = (&pair[0], &pair[1]);
                if bytes_of(a) >= small || bytes_of(b) >= small {
                    continue;
                }
                let runs = [a, b];
                let (fit, _) = self.fit_for(a, fit);
                if !fit.fits(2, bytes_of(a) + bytes_of(b), workspace(&runs)) {
                    continue;
                }
                let bounded = level == last || {
                    let below = &self.levels[level + 1];
                    let range = overlapping(below, min_of(a), max_of(b));
                    below[range].iter().map(bytes_of).sum::<usize>()
                        <= RATIO.saturating_mul(segment)
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
            assert_eq!(
                self.bytes[level],
                runs.iter().map(bytes_of).sum::<usize>(),
                "level {level} bytes"
            );
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
