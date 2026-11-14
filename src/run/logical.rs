use std::fmt;
use std::ops::Range;

use crate::block::Value;

use super::CorruptRun;
use super::group::RunList;
use super::merge::{MergeCursor, MergeWorkspace};
use super::reader::{Found, LookupWorkspace, Run, RunView};

/// A fixed collection of immutable runs, read as the latest state of each key.
///
/// The newest version of a user key, by sequence number, decides its state: a
/// value makes the key live and a tombstone hides it, whichever run holds the
/// older versions and whatever order the runs are given in. Nothing can change
/// under a reader, because the runs are immutable and borrowed for its life.
///
/// A user key and sequence number name one logical update. Runs may hold
/// identical copies of a record, which count once. Copies that differ in value
/// or in kind have no correct winner, so the read returns
/// [`ReadError::ConflictingCopies`] rather than letting run order choose. A
/// tombstone and an empty value differ. Only the newest version of a key is
/// checked, because no older copy can affect a result.
#[derive(Clone, Copy, Debug)]
#[cfg_attr(
    not(test),
    allow(
        dead_code,
        reason = "tested component API the engine does not use; Rust 1.89 does not see it as dead"
    )
)]
pub struct RunSet<'a> {
    runs: &'a [RunView<'a>],
}

/// Reusable storage for logical reads: a [`MergeWorkspace`] for scans, which
/// keeps a buffer per run, storage for point lookups, which keeps two
/// whatever the number of runs, and one key.
#[derive(Debug, Default)]
pub struct LogicalWorkspace {
    merge: MergeWorkspace,
    lookup: LookupWorkspace,
    key: Vec<u8>,
}

/// The latest live value of a key.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct LiveValue<'a> {
    /// The sequence number of the version that decided the key's state.
    pub seq: u64,
    /// The value, borrowed from a decoded block or the run itself.
    pub value: &'a [u8],
}

/// A live key and its latest value.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct LiveRecord<'a> {
    /// The user key.
    pub key: &'a [u8],
    /// The sequence number of the version that decided the key's state.
    pub seq: u64,
    /// The value, borrowed from a decoded block or the run itself.
    pub value: &'a [u8],
}

/// Why a logical read could not produce a correct result.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ReadError {
    /// An input run is damaged.
    Corrupt(CorruptRun),
    /// Two runs disagree about one logical update.
    ConflictingCopies(ConflictingCopies),
}

/// Two copies of one internal key that differ in value or kind.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ConflictingCopies {
    /// The user key.
    pub key: Vec<u8>,
    /// The sequence number both copies carry.
    pub seq: u64,
    /// Positions of two disagreeing runs in the run set, lower first.
    pub inputs: (usize, usize),
}

impl From<CorruptRun> for ReadError {
    fn from(error: CorruptRun) -> Self {
        Self::Corrupt(error)
    }
}

impl fmt::Display for ReadError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Corrupt(error) => error.fmt(f),
            Self::ConflictingCopies(conflict) => write!(
                f,
                "runs {} and {} hold different records for sequence {} of a key",
                conflict.inputs.0, conflict.inputs.1, conflict.seq
            ),
        }
    }
}

impl std::error::Error for ReadError {}

/// Both callers find the lower input first: a lookup walks inputs in order,
/// and the merge orders identical internal keys by input.
pub(super) fn conflict(key: &[u8], seq: u64, lower: usize, higher: usize) -> ReadError {
    ReadError::ConflictingCopies(ConflictingCopies {
        key: key.to_vec(),
        seq,
        inputs: (lower, higher),
    })
}

impl LogicalWorkspace {
    /// Creates an empty workspace, which allocates nothing until first used.
    #[cfg_attr(
        not(test),
        expect(dead_code, reason = "tested component API the engine does not use")
    )]
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns the decoded-block bytes retained across all inputs.
    pub fn capacity(&self) -> usize {
        self.merge.capacity() + self.lookup.capacity()
    }

    /// Returns the bytes held by decompression contexts, which Zstandard
    /// allocates outside the Rust heap.
    pub fn context_bytes(&self) -> usize {
        self.merge.context_bytes() + self.lookup.context_bytes()
    }

    /// Returns how many blocks reads through this workspace have loaded.
    pub fn blocks_loaded(&self) -> u64 {
        self.merge.blocks_loaded() + self.lookup.blocks_loaded()
    }
}

#[cfg_attr(
    not(test),
    expect(dead_code, reason = "tested component API the engine does not use")
)]
impl<'a> RunSet<'a> {
    /// Creates a set over `runs`. Their order does not affect any result.
    pub fn new(runs: &'a [RunView<'a>]) -> Self {
        Self { runs }
    }

    /// Returns the latest value of `key`, or `None` if it is absent or its
    /// newest version is a tombstone.
    ///
    /// Every run whose key range holds `key` decodes one block, because any
    /// of them may hold the newest version. A run outside that range decodes
    /// nothing, so damage in it cannot fail this lookup. Once the workspace has
    /// grown to the largest block read, this performs no heap allocation.
    pub fn get<'r>(
        self,
        key: &[u8],
        workspace: &'r mut LogicalWorkspace,
    ) -> Result<Option<LiveValue<'r>>, ReadError>
    where
        'a: 'r,
    {
        latest_in(self.runs.iter().copied(), key, workspace)
    }

    /// Returns a cursor before the first live key. Nothing is decoded until
    /// it is read or positioned. Creating one makes a single allocation, and a
    /// cursor can be repositioned without any.
    pub fn cursor<'r>(self, workspace: &'r mut LogicalWorkspace) -> LogicalCursor<'r>
    where
        'a: 'r,
    {
        LogicalCursor::over(self.runs.iter().copied(), workspace)
    }
}

/// Finds the latest value of `key` across `runs`, as [`RunSet::get`] does,
/// skipping a run only where its key range or filter proves the key absent,
/// or where its bound, the largest sequence number it holds, is below that
/// of a version already found. Neither check opens the run.
///
/// Such a run cannot change the result. Every version in it is older than
/// the one found, so it cannot be newest; and a copy of the newest version
/// shares its sequence number, so it can only be in a run whose bound is at
/// least that one, which is still read and checked for a conflict. This
/// holds for runs given in any order and with interleaved sequence ranges;
/// visiting them in descending order of their bounds makes it prune most.
/// A bound of `u64::MAX` never prunes.
///
/// A lookup checks the integrity only of the blocks it decodes, so damage in
/// a skipped run, like damage outside the key's range, cannot fail it. Every
/// record of a run is validated when the run is loaded, and compaction reads
/// every record of its inputs.
///
/// The workspace keeps two block buffers whatever the number of runs. The
/// newest version found so far stays in one, and the next run read decodes
/// into the other, so a copy with the same sequence number can be compared
/// with it. Once the workspace is warm this performs no heap allocation, and
/// the value returned borrows whichever buffer holds the winner.
pub(crate) fn latest_in<'a, 'r, I>(
    runs: I,
    key: &[u8],
    workspace: &'r mut LogicalWorkspace,
) -> Result<Option<LiveValue<'r>>, ReadError>
where
    I: IntoIterator<Item: LookupInput<'a>>,
    'a: 'r,
{
    latest_from(InOrder(runs.into_iter().enumerate()), key, workspace)
}

/// [`latest_in`] over the runs `source` offers, each with its position, which
/// a conflict reports. The source is told the sequence number of the newest
/// version found so far, and may stop offering runs once none it has left can
/// hold a version at or above it; one that does can still be checked for a
/// conflicting copy.
pub(crate) fn latest_from<'a, 'r, S>(
    mut source: S,
    key: &[u8],
    workspace: &'r mut LogicalWorkspace,
) -> Result<Option<LiveValue<'r>>, ReadError>
where
    S: LookupSource<'a>,
    'a: 'r,
{
    let lookup = &mut workspace.lookup;
    let mut newest = None;
    let mut hash = None;
    // Held until every run is read: a later run may supersede both copies.
    let mut disagreement = None;
    while let Some((input, run)) = source.next_run(key, newest.map(|(_, seq, _)| seq)) {
        if newest.is_some_and(|(_, seq, _)| run.max_seq() < seq) {
            continue;
        }
        if run.excludes(key, &mut hash) {
            continue;
        }
        // Decodes into the buffer the newest version is not in.
        let buffer = newest.map_or(0, |(_, _, found): (_, _, Found<'_>)| 1 - found.buffer);
        let Some((seq, found)) = lookup.get(run.view(), key, buffer)? else {
            continue;
        };
        match newest {
            Some((_, newest_seq, _)) if newest_seq > seq => {}
            Some((other, newest_seq, held)) if newest_seq == seq => {
                if lookup.value(held) != lookup.value(found) && disagreement.is_none() {
                    disagreement = Some((seq, other, input));
                }
            }
            _ => {
                newest = Some((input, seq, found));
                disagreement = None;
            }
        }
    }
    if let Some((seq, lower, higher)) = disagreement {
        return Err(conflict(key, seq, lower, higher));
    }
    let lookup: &'r LookupWorkspace = lookup;
    Ok(
        newest.and_then(|(_, seq, found)| match lookup.value(found) {
            Value::Bytes(value) => Some(LiveValue { seq, value }),
            Value::Tombstone => None,
        }),
    )
}

/// The runs a lookup reads, offered one at a time.
pub(crate) trait LookupSource<'a> {
    type Run: LookupInput<'a>;

    /// Returns the next run that may hold a version of `key`, with its
    /// position, or `None` when none is left. With `floor`, the sequence
    /// number of the newest version found, a source may end early once every
    /// run it has left holds only older versions; it must still offer a run
    /// that may hold a version with exactly that sequence number.
    fn next_run(&mut self, key: &[u8], floor: Option<u64>) -> Option<(usize, Self::Run)>;
}

/// Every run, in the order given; positions count from 0.
struct InOrder<I>(I);

impl<'a, R, I> LookupSource<'a> for InOrder<I>
where
    R: LookupInput<'a>,
    I: Iterator<Item = (usize, R)>,
{
    type Run = R;

    fn next_run(&mut self, _: &[u8], _: Option<u64>) -> Option<(usize, R)> {
        self.0.next()
    }
}

/// A run as [`latest_in`] reads it: what rules it out without opening it,
/// and the view it reads otherwise.
pub(crate) trait LookupInput<'a>: Copy {
    /// The largest sequence number of any record, `u64::MAX` if unknown.
    fn max_seq(self) -> u64;

    /// Whether the run provably holds no version of `key`. `hash` caches the
    /// key's hash across runs.
    fn excludes(self, key: &[u8], hash: &mut Option<u64>) -> bool;

    fn view(self) -> RunView<'a>;
}

/// A bare view has no filter and no sequence bound, so it is always read; a
/// key outside its range still decodes nothing.
impl<'a> LookupInput<'a> for RunView<'a> {
    fn max_seq(self) -> u64 {
        u64::MAX
    }

    fn excludes(self, _: &[u8], _: &mut Option<u64>) -> bool {
        false
    }

    fn view(self) -> RunView<'a> {
        self
    }
}

impl<'a, B: AsRef<[u8]>> LookupInput<'a> for &'a Run<B> {
    fn max_seq(self) -> u64 {
        Run::max_seq(self)
    }

    fn excludes(self, key: &[u8], hash: &mut Option<u64>) -> bool {
        Run::excludes(self, key, hash)
    }

    fn view(self) -> RunView<'a> {
        Run::view(self)
    }
}

/// Iterates live keys in ascending order, each once with its latest value.
///
/// This is a filter over a [`MergeCursor`]. Each physical record costs one key
/// comparison against the key just decided, and values are neither compared
/// nor copied unless two runs hold the same internal key. One key is copied per
/// distinct user key, into a buffer in the workspace, because moving past a
/// record overwrites the buffer holding its key.
///
/// A failure is returned and repeated until a later seek succeeds.
#[derive(Debug)]
pub struct LogicalCursor<'a> {
    merge: MergeCursor<'a>,
    key: &'a mut Vec<u8>,
    has_key: bool,
    failure: Option<ReadError>,
}

impl<'r> LogicalCursor<'r> {
    /// Creates a cursor over `runs`, as [`RunSet::cursor`] does.
    pub(crate) fn over<I>(runs: I, workspace: &'r mut LogicalWorkspace) -> Self
    where
        I: IntoIterator<Item = RunView<'r>>,
        I::IntoIter: ExactSizeIterator,
    {
        let LogicalWorkspace { merge, key, .. } = workspace;
        Self::merging(MergeCursor::new(runs, merge), key)
    }

    /// Creates a cursor over groups of key-disjoint runs, as
    /// [`MergeCursor::grouped`] reads them: one decoding buffer per group.
    pub(crate) fn grouped<I>(
        runs: &'r dyn RunList,
        groups: I,
        workspace: &'r mut LogicalWorkspace,
    ) -> Self
    where
        I: IntoIterator<Item = Range<usize>>,
        I::IntoIter: ExactSizeIterator,
    {
        let LogicalWorkspace { merge, key, .. } = workspace;
        Self::merging(MergeCursor::grouped(runs, groups, merge), key)
    }

    fn merging(merge: MergeCursor<'r>, key: &'r mut Vec<u8>) -> Self {
        Self {
            merge,
            key,
            has_key: false,
            failure: None,
        }
    }

    /// Moves before the first live key at or above `target`, with no bound.
    #[cfg_attr(
        not(test),
        expect(dead_code, reason = "tested component API the engine does not use")
    )]
    pub fn seek(&mut self, target: &[u8]) -> Result<(), ReadError> {
        self.reposition(|merge| merge.seek(target))
    }

    /// Moves before the first live key starting with `prefix` and iterates
    /// only while keys do.
    pub fn seek_prefix(&mut self, prefix: &[u8]) -> Result<(), ReadError> {
        self.reposition(|merge| merge.seek_prefix(prefix))
    }

    fn reposition(
        &mut self,
        seek: impl FnOnce(&mut MergeCursor<'_>) -> Result<(), CorruptRun>,
    ) -> Result<(), ReadError> {
        self.failure = None;
        self.has_key = false;
        // A failed seek stays in the merge cursor, which repeats it on read.
        seek(&mut self.merge).map_err(ReadError::Corrupt)
    }

    /// Advances to the next live key, or returns `None` when none is left.
    /// After `None`, it keeps returning `None` until a seek.
    pub fn next_record(&mut self) -> Result<Option<LiveRecord<'_>>, ReadError> {
        if let Some(error) = &self.failure {
            return Err(error.clone());
        }
        match self.find_live() {
            Ok(found) => Ok(found.then(|| self.live()).flatten()),
            Err(error) => {
                self.failure = Some(error.clone());
                Err(error)
            }
        }
    }

    /// Moves the merge to the next record that decides a live key.
    fn find_live(&mut self) -> Result<bool, ReadError> {
        while let Some(record) = self.merge.next_record()? {
            if self.has_key && record.key == self.key.as_slice() {
                continue;
            }
            self.key.clear();
            self.key.extend_from_slice(record.key);
            self.has_key = true;
            let tombstone = record.value == Value::Tombstone;
            self.collapse_copies()?;
            if !tombstone {
                return Ok(true);
            }
        }
        Ok(false)
    }

    /// Removes identical copies of the current record and rejects any that
    /// differ. This compares values only when an internal key is duplicated.
    fn collapse_copies(&mut self) -> Result<(), ReadError> {
        while let Some(at) = self.merge.next_copy() {
            if let Some([(first, record), (other, copy)]) = self.merge.copies(at)
                && record.value != copy.value
            {
                return Err(conflict(record.key, record.seq, first, other));
            }
            self.merge.drop_copy(at)?;
        }
        Ok(())
    }

    /// Returns the record the last read stopped on, if it found one.
    pub(crate) fn live(&self) -> Option<LiveRecord<'_>> {
        let record = self.merge.current()?;
        match record.value {
            Value::Bytes(value) => Some(LiveRecord {
                key: record.key,
                seq: record.seq,
                value,
            }),
            Value::Tombstone => None,
        }
    }
}
