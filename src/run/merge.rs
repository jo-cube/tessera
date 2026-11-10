use crate::block::{Record, cmp_internal};

use super::CorruptRun;
use super::reader::{ReadWorkspace, RunCursor, RunView};

/// Reusable storage for merging runs: one [`ReadWorkspace`] per input and the
/// merge order. It retains the largest fan-in it has served.
#[derive(Debug, Default)]
pub struct MergeWorkspace {
    runs: Vec<ReadWorkspace>,
    heap: Vec<usize>,
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
/// needs to enforce this can compare neighboring records, and a caller writing
/// a merged stream to a new run must resolve duplicates first, because a run
/// rejects them.
///
/// This is a lending cursor like [`RunCursor`]. Each input holds its head, the
/// next record it would yield, decoded in its own workspace, and the merge
/// compares heads in place. So state is one cursor and at most one decoded block
/// per input, and values are never copied. Records need no allocation; creating
/// a cursor makes one, and [`seek`](Self::seek) makes none on a warm workspace.
///
/// ```
/// use tessera::block::Value;
/// use tessera::run::{Codec, MergeCursor, MergeWorkspace, RunBuilder};
///
/// let mut older = RunBuilder::new(Codec::Lz4);
/// older.add(b"a", 1, Value::Bytes(b"old"))?;
/// let mut newer = RunBuilder::new(Codec::Zstd);
/// newer.add(b"a", 2, Value::Tombstone)?;
/// let (older, newer) = (older.finish(), newer.finish());
///
/// let mut workspace = MergeWorkspace::new();
/// let mut merged = MergeCursor::new([newer.view(), older.view()], &mut workspace);
/// assert_eq!(merged.next_record()?.unwrap().seq, 2);
/// assert_eq!(merged.next_record()?.unwrap().seq, 1);
/// assert_eq!(merged.next_record()?, None);
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
#[derive(Debug)]
pub struct MergeCursor<'a> {
    cursors: Vec<RunCursor<'a>>,
    /// A binary min-heap of input positions holding a head. Heads live in the
    /// cursors, so the heap holds no keys of its own.
    heap: &'a mut Vec<usize>,
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
        let MergeWorkspace {
            runs: workspaces,
            heap,
        } = workspace;
        let runs = runs.into_iter();
        if workspaces.len() < runs.len() {
            workspaces.resize_with(runs.len(), ReadWorkspace::default);
        }
        Self {
            cursors: runs
                .zip(workspaces.iter_mut())
                .map(|(run, workspace)| run.cursor(workspace))
                .collect(),
            heap,
            positioned: false,
            yielded: false,
            failure: None,
        }
    }

    /// Moves every input before its first record at or above `target` and
    /// iterates without a bound.
    ///
    /// An input whose keys are all below `target` is skipped without decoding,
    /// and every other input decodes one block to read its head. If this
    /// fails, [`next_record`](Self::next_record) reports the same error until a
    /// later seek succeeds.
    pub fn seek(&mut self, target: &[u8]) -> Result<(), CorruptRun> {
        self.reposition(|cursor| cursor.seek(target))
    }

    /// Moves to the first record whose user key starts with `prefix` and
    /// iterates only while keys do, with the behavior of
    /// [`RunCursor::seek_prefix`] applied to every input.
    pub fn seek_prefix(&mut self, prefix: &[u8]) -> Result<(), CorruptRun> {
        self.reposition(|cursor| cursor.seek_prefix(prefix))
    }

    fn reposition(
        &mut self,
        mut seek: impl FnMut(&mut RunCursor<'a>) -> Result<(), CorruptRun>,
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
        let head = self.heap.first();
        Ok(head.and_then(|&input| self.cursors[input].head_record()))
    }

    /// Makes the heap's first entry the record to return.
    fn step(&mut self) -> Result<(), CorruptRun> {
        if !self.positioned {
            self.positioned = true;
            self.build_heap()?;
        } else if self.yielded {
            self.advance_first()?;
        }
        self.yielded = !self.heap.is_empty();
        Ok(())
    }

    fn build_heap(&mut self) -> Result<(), CorruptRun> {
        self.heap.clear();
        for (input, cursor) in self.cursors.iter_mut().enumerate() {
            cursor.fill_head()?;
            if cursor.head_key().is_some() {
                self.heap.push(input);
            }
        }
        for at in (0..self.heap.len() / 2).rev() {
            sift_down(self.heap, &self.cursors, at);
        }
        Ok(())
    }

    fn advance_first(&mut self) -> Result<(), CorruptRun> {
        let input = self.heap[0];
        self.cursors[input].advance_head()?;
        if self.cursors[input].head_key().is_none() {
            self.heap.swap_remove(0);
        }
        sift_down(self.heap, &self.cursors, 0);
        Ok(())
    }
}

/// Restores the heap below `at`. Every entry must hold a head.
fn sift_down(heap: &mut [usize], cursors: &[RunCursor<'_>], mut at: usize) {
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
fn precedes(cursors: &[RunCursor<'_>], a: usize, b: usize) -> bool {
    let (a_key, a_seq) = cursors[a].head_key().expect("heap entries hold a head");
    let (b_key, b_seq) = cursors[b].head_key().expect("heap entries hold a head");
    cmp_internal(a_key, a_seq, b_key, b_seq)
        .then(a.cmp(&b))
        .is_lt()
}
