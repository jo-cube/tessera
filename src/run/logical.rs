use std::fmt;

use crate::block::Value;

use super::CorruptRun;
use super::merge::{MergeCursor, MergeWorkspace};
use super::reader::RunView;

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
///
/// ```
/// use tessera::block::Value;
/// use tessera::run::{Codec, LogicalWorkspace, RunBuilder, RunSet};
///
/// let mut older = RunBuilder::new(Codec::Lz4);
/// older.add(b"a", 1, Value::Bytes(b"old"))?;
/// older.add(b"b", 1, Value::Bytes(b"kept"))?;
/// let mut newer = RunBuilder::new(Codec::Zstd);
/// newer.add(b"a", 2, Value::Tombstone)?;
/// let (older, newer) = (older.finish(), newer.finish());
/// let views = [older.view(), newer.view()];
/// let set = RunSet::new(&views);
///
/// let mut workspace = LogicalWorkspace::new();
/// assert_eq!(set.get(b"a", &mut workspace)?, None);
/// let mut cursor = set.cursor(&mut workspace);
/// assert_eq!(cursor.next_record()?.unwrap().key, b"b");
/// assert_eq!(cursor.next_record()?, None);
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
#[derive(Clone, Copy, Debug)]
pub struct RunSet<'a> {
    runs: &'a [RunView<'a>],
}

/// Reusable storage for logical reads: a [`MergeWorkspace`] and one key.
#[derive(Debug, Default)]
pub struct LogicalWorkspace {
    merge: MergeWorkspace,
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
fn conflict(key: &[u8], seq: u64, lower: usize, higher: usize) -> ReadError {
    ReadError::ConflictingCopies(ConflictingCopies {
        key: key.to_vec(),
        seq,
        inputs: (lower, higher),
    })
}

impl LogicalWorkspace {
    /// Creates an empty workspace, which allocates nothing until first used.
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns the decoded-block bytes retained across all inputs.
    pub fn capacity(&self) -> usize {
        self.merge.capacity()
    }

    /// Returns how many blocks reads through this workspace have loaded.
    pub fn blocks_loaded(&self) -> u64 {
        self.merge.blocks_loaded()
    }
}

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

/// Finds the latest value of `key` across `runs`, as [`RunSet::get`] does.
pub(crate) fn latest_in<'a, 'r, I>(
    runs: I,
    key: &[u8],
    workspace: &'r mut LogicalWorkspace,
) -> Result<Option<LiveValue<'r>>, ReadError>
where
    I: ExactSizeIterator<Item = RunView<'a>>,
    'a: 'r,
{
    let reads = workspace.merge.reads(runs.len());
    let mut newest = None;
    // Held until every run is read: a later run may supersede both copies.
    let mut disagreement = None;
    for (input, (run, read)) in runs.zip(reads).enumerate() {
        let Some(version) = run.get(key, read)? else {
            continue;
        };
        match newest {
            Some((_, seq, _)) if seq > version.seq => {}
            Some((other, seq, value)) if seq == version.seq => {
                if value != version.value && disagreement.is_none() {
                    disagreement = Some((seq, other, input));
                }
            }
            _ => {
                newest = Some((input, version.seq, version.value));
                disagreement = None;
            }
        }
    }
    if let Some((seq, lower, higher)) = disagreement {
        return Err(conflict(key, seq, lower, higher));
    }
    Ok(newest.and_then(|(_, seq, value)| match value {
        Value::Bytes(value) => Some(LiveValue { seq, value }),
        Value::Tombstone => None,
    }))
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
        let LogicalWorkspace { merge, key } = workspace;
        Self {
            merge: MergeCursor::new(runs, merge),
            key,
            has_key: false,
            failure: None,
        }
    }

    /// Moves before the first live key at or above `target`, with no bound.
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
