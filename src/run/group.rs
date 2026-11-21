//! A cursor over a group of runs whose key ranges are strictly disjoint, read
//! one run after another in key order through one workspace.

use std::fmt;
use std::ops::Range;

use crate::block::Record;

use super::CorruptRun;
use super::reader::{ReadWorkspace, Run, RunCursor};

/// Runs addressed by position. A group is a range of positions whose runs
/// have records and strictly disjoint key ranges, in ascending key order: each
/// run's smallest key sorts above the previous run's largest key.
pub(crate) trait RunList: fmt::Debug + Sync {
    fn run(&self, position: usize) -> &Run<Vec<u8>>;
}

/// A lending cursor over a group of key-disjoint runs: the records of each run
/// in turn, which is the group's internal order because no key appears in two
/// of its runs.
///
/// It holds one [`RunCursor`] and so one decoded block and one decompression
/// context, whatever the number of runs. A seek binary-searches the runs by
/// their largest key and positions in one of them; moving past the end of a
/// run opens the next one in place. Under a prefix, the next run is opened
/// only if its first key has the prefix: keys with a prefix are contiguous, so
/// a run that starts outside them has none and neither has any later run.
#[derive(Debug)]
pub(super) struct GroupCursor<'a> {
    cursor: RunCursor<'a>,
    /// The list holding a group of several runs; `None` for a single run.
    runs: Option<&'a dyn RunList>,
    /// The positions of the group's runs.
    members: Range<usize>,
    /// The position of the run being read.
    current: usize,
    /// Whether no later run can be in bounds, because the run a prefix seek
    /// entered starts above every key with the prefix, or the prefix is too
    /// long for any key.
    closed: bool,
}

impl<'a> GroupCursor<'a> {
    /// A group of the one run `cursor` reads, at `position`.
    pub(super) fn one(cursor: RunCursor<'a>, position: usize) -> Self {
        Self {
            cursor,
            runs: None,
            members: position..position + 1,
            current: position,
            closed: false,
        }
    }

    /// A group of the runs of `runs` at `members`, which must not be empty,
    /// before its first record.
    pub(super) fn many(
        runs: &'a dyn RunList,
        members: Range<usize>,
        workspace: &'a mut ReadWorkspace,
    ) -> Self {
        let cursor = runs.run(members.start).view().cursor(workspace);
        Self {
            cursor,
            runs: (members.len() > 1).then_some(runs),
            current: members.start,
            members,
            closed: false,
        }
    }

    /// The position of the run being read.
    pub(super) fn position(&self) -> usize {
        self.current
    }

    /// Positions at the first record at or above `target`, without a bound.
    pub(super) fn seek(&mut self, target: &[u8]) -> Result<(), CorruptRun> {
        self.enter(target);
        self.closed = false;
        self.cursor.seek(target)
    }

    /// Positions at the first record at or above `target` and reads only
    /// while keys are within `end`, as [`RunCursor::seek_range`] does.
    pub(super) fn seek_range(
        &mut self,
        target: &[u8],
        end: std::ops::Bound<&[u8]>,
    ) -> Result<(), CorruptRun> {
        self.enter(target);
        self.closed = false;
        self.cursor.seek_range(target, end)
    }

    /// Positions at the first record whose key starts with `prefix` and reads
    /// only while keys do, as [`RunCursor::seek_prefix`] does.
    pub(super) fn seek_prefix(&mut self, prefix: &[u8]) -> Result<(), CorruptRun> {
        self.enter(prefix);
        let result = self.cursor.seek_prefix(prefix);
        // A run cursor keeps a prefix only if some key could match it; it
        // never refuses the empty one. A refusal by the first run that could
        // match rules out every later run too.
        self.closed = self.cursor.prefix().len() != prefix.len();
        result
    }

    /// Makes the run that can hold the first key at or above `target` the
    /// one being read: the first whose largest key is at or above it, or the
    /// last if none is, which the seek then finishes.
    fn enter(&mut self, target: &[u8]) {
        let Some(runs) = self.runs else {
            return;
        };
        let (mut low, mut high) = (self.members.start, self.members.end - 1);
        while low < high {
            let middle = low + (high - low) / 2;
            if largest_key(runs.run(middle)) < target {
                low = middle + 1;
            } else {
                high = middle;
            }
        }
        if low != self.current {
            self.current = low;
            self.cursor.continue_into(runs.run(low).view());
        }
    }

    /// Opens the next run if it can hold a key in bounds. Returns whether it
    /// did.
    fn advance_run(&mut self) -> bool {
        let Some(runs) = self.runs.filter(|_| !self.closed) else {
            return false;
        };
        let next = self.current + 1;
        if next >= self.members.end {
            return false;
        }
        let run = runs.run(next);
        if !self.cursor.admits(smallest_key(run)) {
            return false;
        }
        self.current = next;
        self.cursor.continue_into(run.view());
        true
    }

    /// The first key of the run being read, if nothing has been decoded from
    /// it since the group was positioned before that key.
    pub(super) fn deferred_key(&self) -> Option<&'a [u8]> {
        self.cursor.deferred_key()
    }

    /// Decodes the next record into the head, unless one is held, moving on
    /// through the group's runs until one has a record in bounds.
    pub(super) fn fill_head(&mut self) -> Result<(), CorruptRun> {
        loop {
            self.cursor.fill_head()?;
            if self.cursor.head_key().is_some() || !self.advance_run() {
                return Ok(());
            }
        }
    }

    /// Consumes the head and decodes the record after it.
    pub(super) fn advance_head(&mut self) -> Result<(), CorruptRun> {
        self.cursor.advance_head()?;
        self.fill_head()
    }

    pub(super) fn head_key(&self) -> Option<(&[u8], u64)> {
        self.cursor.head_key()
    }

    /// The cursor of a group of one run, for copying its blocks forward.
    pub(super) fn single(&mut self) -> Option<&mut RunCursor<'a>> {
        self.runs.is_none().then_some(&mut self.cursor)
    }

    pub(super) fn single_ref(&self) -> Option<&RunCursor<'a>> {
        self.runs.is_none().then_some(&self.cursor)
    }

    pub(super) fn head_record(&self) -> Option<Record<'_>> {
        self.cursor.head_record()
    }
}

/// Members of a list of several have records.
fn largest_key<B: AsRef<[u8]>>(run: &Run<B>) -> &[u8] {
    run.key_range().expect(GROUPED).max
}

fn smallest_key<B: AsRef<[u8]>>(run: &Run<B>) -> &[u8] {
    run.key_range().expect(GROUPED).min
}

const GROUPED: &str = "a run grouped with others has records";
