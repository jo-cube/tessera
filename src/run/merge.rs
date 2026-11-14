use std::ops::Range;

use crate::block::{Record, cmp_internal};

use super::CorruptRun;
use super::group::{GroupCursor, RunList};
use super::reader::{ReadWorkspace, RunView};

/// Reusable storage for merging runs: a decoding buffer per input and the
/// merge order. It retains the largest fan-in it has served. A group of
/// key-disjoint runs is one input.
#[derive(Debug, Default)]
pub struct MergeWorkspace {
    runs: Vec<ReadWorkspace>,
    heap: Vec<usize>,
    pending: Vec<usize>,
}

impl MergeWorkspace {
    /// Creates an empty workspace, which allocates nothing until first used.
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns the decoded-block bytes retained across all inputs.
    pub fn capacity(&self) -> usize {
        self.runs.iter().map(ReadWorkspace::capacity).sum()
    }

    /// Returns how many blocks reads through this workspace have loaded.
    pub fn blocks_loaded(&self) -> u64 {
        self.runs.iter().map(ReadWorkspace::blocks_loaded).sum()
    }

    /// Returns the bytes held by decompression contexts, which Zstandard
    /// allocates outside the Rust heap.
    pub fn context_bytes(&self) -> usize {
        self.runs.iter().map(ReadWorkspace::context_bytes).sum()
    }

    /// Returns one read workspace per input, creating any that are missing.
    pub(super) fn reads(&mut self, inputs: usize) -> &mut [ReadWorkspace] {
        if self.runs.len() < inputs {
            self.runs.resize_with(inputs, ReadWorkspace::default);
        }
        &mut self.runs[..inputs]
    }
}

/// An ordered stream over several runs, which may overlap and may use
/// different codecs and block sizes.
///
/// Records are yielded in internal order: user key ascending, then sequence
/// number descending. Every physical record is yielded, including all versions
/// and tombstones; a merge applies no visibility rule.
///
/// Identical internal keys in different inputs are all yielded, ordered by
/// input position, earliest first. A run cannot hold duplicates, but nothing
/// prevents two runs from holding the same key and sequence number, even with
/// different values. A merge does not compare values or report conflicts. A
/// user key and sequence number must name one logical record; a caller that
/// needs one record per user key should read through a
/// [`RunSet`](super::RunSet), which collapses identical copies and rejects
/// conflicting ones. A caller writing a merged stream to a new run must
/// resolve duplicates first, because a run rejects them.
///
/// This is a lending cursor like [`RunCursor`](super::reader::RunCursor).
/// Each input holds its head, the next record it would yield, decoded in its
/// own workspace, and the merge compares heads in place. So state is one cursor and at most one decoded block
/// per input, and values are never copied. Records need no allocation; creating
/// a cursor makes one, and [`seek`](Self::seek) makes none on a warm workspace.
#[derive(Debug)]
pub struct MergeCursor<'a> {
    cursors: Vec<GroupCursor<'a>>,
    /// A binary min-heap of input positions holding a head. Heads live in the
    /// cursors, so the heap holds no keys of its own.
    heap: &'a mut Vec<usize>,
    /// Inputs not decoded yet, largest first key first. Each joins the heap
    /// when the merge reaches its first key.
    pending: &'a mut Vec<usize>,
    positioned: bool,
    /// Whether the heap's first entry has been yielded and not yet advanced.
    yielded: bool,
    failure: Option<CorruptRun>,
}

impl<'a> MergeCursor<'a> {
    /// Creates a cursor before the first record of the merged stream.
    ///
    /// The order of `runs` decides only how identical internal keys are
    /// ordered. Nothing is decoded until the cursor is read or positioned.
    pub fn new<I>(runs: I, workspace: &'a mut MergeWorkspace) -> Self
    where
        I: IntoIterator<Item = RunView<'a>>,
        I::IntoIter: ExactSizeIterator,
    {
        let runs = runs.into_iter();
        workspace.reads(runs.len());
        let MergeWorkspace {
            runs: workspaces,
            heap,
            pending,
        } = workspace;
        let cursors = runs
            .zip(workspaces.iter_mut())
            .enumerate()
            .map(|(input, (run, workspace))| GroupCursor::one(run.cursor(workspace), input))
            .collect();
        Self::from_cursors(cursors, heap, pending)
    }

    /// Creates a cursor over groups of runs, each a range of positions in
    /// `runs` read as one input with one decoding buffer.
    ///
    /// Each group's runs must have strictly disjoint key ranges, in key order,
    /// and no group may be empty. Ranges must ascend, because a conflict
    /// between copies reports the positions of the two runs, lower first.
    pub(crate) fn grouped<I>(
        runs: &'a dyn RunList,
        groups: I,
        workspace: &'a mut MergeWorkspace,
    ) -> Self
    where
        I: IntoIterator<Item = Range<usize>>,
        I::IntoIter: ExactSizeIterator,
    {
        let groups = groups.into_iter();
        workspace.reads(groups.len());
        let MergeWorkspace {
            runs: workspaces,
            heap,
            pending,
        } = workspace;
        let cursors = groups
            .zip(workspaces.iter_mut())
            .map(|(members, workspace)| GroupCursor::many(runs, members, workspace))
            .collect();
        Self::from_cursors(cursors, heap, pending)
    }

    fn from_cursors(
        cursors: Vec<GroupCursor<'a>>,
        heap: &'a mut Vec<usize>,
        pending: &'a mut Vec<usize>,
    ) -> Self {
        Self {
            cursors,
            heap,
            pending,
            positioned: false,
            yielded: false,
            failure: None,
        }
    }

    /// Moves every input before its first record at or above `target` and
    /// iterates without a bound.
    ///
    /// An input whose keys are all below `target` is skipped without decoding.
    /// An input whose keys are all at or above `target` waits, undecoded, until
    /// the merge reaches its first key. Any other input decodes one block. If
    /// this fails, [`next_record`](Self::next_record) reports the same error
    /// until a later seek succeeds.
    #[cfg_attr(
        not(test),
        allow(
            dead_code,
            reason = "tested component API the engine does not use; Rust 1.89 does not see it as dead"
        )
    )]
    pub fn seek(&mut self, target: &[u8]) -> Result<(), CorruptRun> {
        self.reposition(|cursor| cursor.seek(target))
    }

    /// Moves to the first record whose user key starts with `prefix` and
    /// iterates only while keys do, with the behavior of
    /// [`RunCursor::seek_prefix`](super::reader::RunCursor::seek_prefix)
    /// applied to every input.
    pub fn seek_prefix(&mut self, prefix: &[u8]) -> Result<(), CorruptRun> {
        self.reposition(|cursor| cursor.seek_prefix(prefix))
    }

    fn reposition(
        &mut self,
        mut seek: impl FnMut(&mut GroupCursor<'a>) -> Result<(), CorruptRun>,
    ) -> Result<(), CorruptRun> {
        self.failure = None;
        self.yielded = false;
        self.positioned = true;
        let result = self
            .cursors
            .iter_mut()
            .try_for_each(&mut seek)
            .and_then(|()| self.build_heap());
        if let Err(error) = result {
            self.failure = Some(error);
        }
        result
    }

    /// Advances to the next record of the merged stream, or returns `None`
    /// when no input has one left. After `None`, it keeps returning `None`
    /// until a seek.
    ///
    /// A failure in any input is returned here and repeated until a later seek
    /// succeeds, because the merge cannot order what it cannot read.
    pub fn next_record(&mut self) -> Result<Option<Record<'_>>, CorruptRun> {
        if let Some(error) = self.failure {
            return Err(error);
        }
        if let Err(error) = self.step() {
            self.failure = Some(error);
            return Err(error);
        }
        Ok(self.current())
    }

    /// Returns the record most recently returned by `next_record`.
    pub(super) fn current(&self) -> Option<Record<'_>> {
        let input = *self.heap.first()?;
        self.cursors[input].head_record()
    }

    /// Finds another copy of the current record: the same user key and
    /// sequence number, held by another input. Returns its heap position.
    ///
    /// Every entry equal to the root has only equal entries above it, so if
    /// any copy exists, one of the root's children is a copy. Sequence numbers
    /// are compared first, so distinct records cost no key comparison unless
    /// their sequence numbers happen to match.
    pub(super) fn next_copy(&self) -> Option<usize> {
        let (key, seq) = self.cursors[*self.heap.first()?].head_key()?;
        (1..self.heap.len().min(3)).find(|&at| {
            let copy = self.cursors[self.heap[at]].head_key();
            copy.is_some_and(|(copy_key, copy_seq)| copy_seq == seq && copy_key == key)
        })
    }

    /// Returns the current record and the copy at heap position `at`, each
    /// with the position of the run holding it.
    pub(super) fn copies(&self, at: usize) -> Option<[(usize, Record<'_>); 2]> {
        let [first, other] = [&self.cursors[self.heap[0]], &self.cursors[self.heap[at]]];
        Some([
            (first.position(), first.head_record()?),
            (other.position(), other.head_record()?),
        ])
    }

    /// Removes the copy at heap position `at` from the stream.
    pub(super) fn drop_copy(&mut self, at: usize) -> Result<(), CorruptRun> {
        self.advance_at(at)
    }

    /// Makes the heap's first entry the record to return.
    fn step(&mut self) -> Result<(), CorruptRun> {
        if !self.positioned {
            self.positioned = true;
            self.build_heap()?;
        } else if self.yielded {
            self.advance_at(0)?;
        }
        self.admit_pending()?;
        self.yielded = !self.heap.is_empty();
        Ok(())
    }

    fn build_heap(&mut self) -> Result<(), CorruptRun> {
        self.heap.clear();
        self.pending.clear();
        for (input, cursor) in self.cursors.iter_mut().enumerate() {
            if cursor.deferred_key().is_some() {
                self.pending.push(input);
                continue;
            }
            cursor.fill_head()?;
            if cursor.head_key().is_some() {
                self.heap.push(input);
            }
        }
        for at in (0..self.heap.len() / 2).rev() {
            sift_down(self.heap, &self.cursors, at);
        }
        let cursors = &self.cursors;
        self.pending
            .sort_unstable_by(|&a, &b| cursors[b].deferred_key().cmp(&cursors[a].deferred_key()));
        Ok(())
    }

    /// Decodes every waiting input whose first key could sort before or tie
    /// with the current first record. A tie matters: the waiting input may hold
    /// a newer version of that key.
    fn admit_pending(&mut self) -> Result<(), CorruptRun> {
        while let Some(&input) = self.pending.last() {
            let first = self.cursors[input].deferred_key();
            let root = self.heap.first();
            let waits = root.is_some_and(|&root| {
                let key = self.cursors[root].head_key().map(|(key, _)| key);
                first > key
            });
            if waits {
                break;
            }
            self.pending.pop();
            self.cursors[input].fill_head()?;
            if self.cursors[input].head_key().is_some() {
                let at = self.heap.len();
                self.heap.push(input);
                sift_up(self.heap, &self.cursors, at);
            }
        }
        Ok(())
    }

    /// Advances the input at heap position `at`, which must be the root or a
    /// child of it, so an entry moved here never needs to rise.
    fn advance_at(&mut self, at: usize) -> Result<(), CorruptRun> {
        let input = self.heap[at];
        self.cursors[input].advance_head()?;
        if self.cursors[input].head_key().is_none() {
            self.heap.swap_remove(at);
        }
        sift_down(self.heap, &self.cursors, at);
        Ok(())
    }
}

/// Restores the heap above `at`. Every entry must hold a head.
fn sift_up(heap: &mut [usize], cursors: &[GroupCursor<'_>], mut at: usize) {
    while at > 0 {
        let parent = (at - 1) / 2;
        if !precedes(cursors, heap[at], heap[parent]) {
            return;
        }
        heap.swap(at, parent);
        at = parent;
    }
}

/// Restores the heap below `at`. Every entry must hold a head.
fn sift_down(heap: &mut [usize], cursors: &[GroupCursor<'_>], mut at: usize) {
    loop {
        let mut first = at;
        for child in [2 * at + 1, 2 * at + 2] {
            if child < heap.len() && precedes(cursors, heap[child], heap[first]) {
                first = child;
            }
        }
        if first == at {
            return;
        }
        heap.swap(at, first);
        at = first;
    }
}

/// Internal order, then input position, so identical keys have one order.
fn precedes(cursors: &[GroupCursor<'_>], a: usize, b: usize) -> bool {
    let (a_key, a_seq) = cursors[a].head_key().expect("heap entries hold a head");
    let (b_key, b_seq) = cursors[b].head_key().expect("heap entries hold a head");
    cmp_internal(a_key, a_seq, b_key, b_seq)
        .then(a.cmp(&b))
        .is_lt()
}
