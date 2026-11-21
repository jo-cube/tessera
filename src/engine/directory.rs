//! The published runs, indexed for reading: one immutable directory that
//! lookups, scans and snapshots share.
//!
//! The runs are partitioned into groups whose key ranges are strictly
//! disjoint, so that within a group at most one run can hold a given key and
//! a binary search finds it. A lookup visits the groups newest first and stops
//! once no group left can hold a version as new as the one found. A scan reads
//! each group as one key-ordered input, so a merge has one input and one
//! decoded block per group rather than per run.
//!
//! Partitioning is the greedy interval partitioning: runs sorted by smallest
//! key are each placed in a group whose last run ends strictly below their
//! smallest key, opening a new group when none does. This uses the fewest
//! groups possible, the largest number of runs whose inclusive key ranges share
//! one key. Nothing about it depends on sequence numbers, so how much a group's
//! bound prunes depends on the history. When runs overlap heavily the groups
//! are single runs, read in descending sequence-bound order.
//!
//! The directory holds a reference to each run and no keys: bounds are the
//! run's own, found when it was built or validated. It is rebuilt whole, under
//! the state lock, whenever the set of runs changes, and is never modified
//! after, so a reader holding one sees exactly the runs published together.

use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::fmt;
use std::ops::Range;

use crate::run::{LookupSource, Run, RunList};

use super::{NOT_EMPTY, SharedRun};

#[cfg(test)]
mod tests;

/// The published runs, grouped. See the [module documentation](self).
pub(super) struct RunDirectory {
    /// Every run once, group by group in the order of `groups`, and in
    /// ascending key order within a group.
    runs: Box<[SharedRun]>,
    /// Largest bound first.
    groups: Box<[Group]>,
}

/// A slice of the directory's runs whose key ranges are strictly disjoint.
#[derive(Clone, Copy, Debug)]
struct Group {
    /// The largest sequence number held by any of its runs.
    max_seq: u64,
    start: usize,
    end: usize,
}

impl RunDirectory {
    pub(super) fn empty() -> Self {
        Self {
            runs: Box::new([]),
            groups: Box::new([]),
        }
    }

    /// Partitions `runs`, which must have records. The result depends only on
    /// their key ranges and bounds and, between runs alike in both, on their
    /// order in `runs`.
    pub(super) fn build(runs: &[SharedRun]) -> Self {
        let mut order: Vec<usize> = (0..runs.len()).collect();
        let range = |index: usize| runs[index].run.key_range().expect(NOT_EMPTY);
        order.sort_by(|&a, &b| {
            let (a_range, b_range) = (range(a), range(b));
            (a_range.min, a_range.max, a).cmp(&(b_range.min, b_range.max, b))
        });

        // Groups as lists of run indices, and a min-heap of each group's
        // largest key. Placing a run in the group that ends lowest keeps the
        // others free for runs that start lower; any group ending below the
        // run's smallest key would do as well.
        let mut members: Vec<Vec<usize>> = Vec::new();
        let mut ends: BinaryHeap<Reverse<(&[u8], usize)>> = BinaryHeap::new();
        for index in order {
            let run_range = range(index);
            let group = match ends.peek() {
                Some(&Reverse((end, group))) if end < run_range.min => {
                    ends.pop();
                    group
                }
                _ => {
                    members.push(Vec::new());
                    members.len() - 1
                }
            };
            members[group].push(index);
            ends.push(Reverse((run_range.max, group)));
        }
        drop(ends);

        let max_seq = |group: &[usize]| {
            group
                .iter()
                .map(|&index| runs[index].run.max_seq())
                .max()
                .unwrap_or(0)
        };
        // Newest bound first; ties by the group's first key, then by the
        // order groups were opened, which the same runs always repeat.
        let mut ranked: Vec<(u64, usize)> = members
            .iter()
            .enumerate()
            .map(|(group, indices)| (max_seq(indices), group))
            .collect();
        ranked.sort_by(|&(a_seq, a), &(b_seq, b)| {
            b_seq
                .cmp(&a_seq)
                .then_with(|| range(members[a][0]).min.cmp(range(members[b][0]).min))
                .then(a.cmp(&b))
        });

        let mut flat = Vec::with_capacity(runs.len());
        let mut groups = Vec::with_capacity(ranked.len());
        for (max_seq, group) in ranked {
            let start = flat.len();
            flat.extend(
                members[group]
                    .iter()
                    .map(|&index| SharedRun::clone(&runs[index])),
            );
            groups.push(Group {
                max_seq,
                start,
                end: flat.len(),
            });
        }
        Self {
            runs: flat.into(),
            groups: groups.into(),
        }
    }

    /// Every run once, in directory order: a run's index here is its position
    /// in a [`ConflictingCopies`](crate::run::ConflictingCopies) report.
    #[cfg(test)]
    pub(super) fn runs(&self) -> &[SharedRun] {
        &self.runs
    }

    pub(super) fn len(&self) -> usize {
        self.runs.len()
    }

    /// The number of groups: the largest number of runs whose key ranges
    /// share a key.
    #[cfg_attr(not(any(test, feature = "diagnostics")), allow(dead_code))]
    pub(super) fn group_count(&self) -> usize {
        self.groups.len()
    }

    /// Bytes the directory itself holds on the heap, apart from the runs.
    #[cfg(test)]
    pub(super) fn heap_bytes(&self) -> usize {
        size_of_val::<[SharedRun]>(&self.runs) + size_of_val::<[Group]>(&self.groups)
    }

    /// The runs a lookup of one key reads, as a lazy source: per group, the
    /// one run whose range could hold the key.
    pub(super) fn lookup(&self) -> Lookup<'_> {
        Lookup {
            directory: self,
            next: 0,
        }
    }

    /// Each group as one input of a merge: the positions of its runs.
    pub(super) fn groups(&self) -> impl ExactSizeIterator<Item = Range<usize>> + '_ {
        self.groups.iter().map(|group| group.start..group.end)
    }

    /// Each group's sequence bound, first run position, and runs in ascending
    /// key order, ordered by descending sequence bound.
    pub(super) fn group_slices(&self) -> impl Iterator<Item = (u64, usize, &[SharedRun])> {
        self.groups.iter().map(|group| {
            (
                group.max_seq,
                group.start,
                &self.runs[group.start..group.end],
            )
        })
    }
}

impl RunList for RunDirectory {
    fn run(&self, position: usize) -> &Run<Vec<u8>> {
        &self.runs[position].run
    }
}

impl fmt::Debug for RunDirectory {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RunDirectory")
            .field("runs", &self.runs.len())
            .field("groups", &self.groups.len())
            .finish()
    }
}

/// The lazy source [`RunDirectory::lookup`] returns.
#[derive(Debug)]
pub(super) struct Lookup<'d> {
    directory: &'d RunDirectory,
    /// The next group to visit.
    next: usize,
}

impl<'d> LookupSource<'d> for Lookup<'d> {
    type Run = &'d Run<Vec<u8>>;

    /// Visits groups by descending bound and stops at the first whose bound
    /// is below `floor`: every later group's is too. A group whose bound
    /// equals it is still visited, since it may hold a copy of the version
    /// found. In a group, the run to read is the first whose largest key is at
    /// or above `key`; whether its smallest key is at or below `key`, and its
    /// own bound and filter, the lookup checks before decoding anything.
    fn next_run(&mut self, key: &[u8], floor: Option<u64>) -> Option<(usize, Self::Run)> {
        let directory = self.directory;
        while let Some(group) = directory.groups.get(self.next) {
            if floor.is_some_and(|seq| group.max_seq < seq) {
                self.next = directory.groups.len();
                return None;
            }
            self.next += 1;
            let members = &directory.runs[group.start..group.end];
            let at = if members.len() == 1 {
                0
            } else {
                members.partition_point(|run| run.run.key_range().expect(NOT_EMPTY).max < key)
            };
            if let Some(run) = members.get(at) {
                return Some((group.start + at, &run.run));
            }
        }
        None
    }
}
