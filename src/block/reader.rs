use std::cmp::Ordering;
use std::fmt;

use super::{
    MAX_BLOCK_LEN, MAX_KEY_LEN, MAX_VALUE_LEN, U32_LEN, Value, cmp_internal, common_prefix_len,
    varint,
};

/// A validated block over any byte storage, such as `Vec<u8>`, `Arc<[u8]>` or
/// `&[u8]`.
///
/// Structure is checked once by [`new`](Self::new), which walks every record.
/// Holding a `Block` is proof of validity, so [`get`](Self::get) and
/// [`Cursor`] neither repeat those checks nor return errors. The storage must
/// keep returning the same bytes from `as_ref`; if it does not, reads may
/// panic or return wrong data.
///
/// Use this for bytes of unknown origin, and for long-lived blocks. To read
/// bytes already known to be valid without another walk, see [`BlockView`].
#[derive(Clone, Debug)]
#[cfg(test)]
pub struct Block<B> {
    data: B,
    entries_len: usize,
    record_count: usize,
}

#[cfg(test)]
impl<B: AsRef<[u8]>> Block<B> {
    /// Validates `data` in full and wraps it. This costs O(block length) and
    /// reports the first structural problem found.
    pub fn new(data: B) -> Result<Self, CorruptBlock> {
        let mut walk = RecordWalk::default();
        let entries_len = walk.validate_block(data.as_ref())?;
        Ok(Self {
            data,
            entries_len,
            record_count: walk.record_count,
        })
    }

    /// Returns the number of stored records, counting every version.
    pub fn len(&self) -> usize {
        self.record_count
    }

    /// Returns whether the block stores no records.
    pub fn is_empty(&self) -> bool {
        self.record_count == 0
    }

    /// Returns the newest version of `user_key`, which may be a tombstone, or
    /// `None` if the block has no record for that key.
    ///
    /// Performs no heap allocation.
    pub fn get(&self, user_key: &[u8]) -> Option<Version<'_>> {
        self.view().get(user_key).expect(VALIDATED)
    }

    /// Returns a cursor positioned before the first record.
    pub fn cursor(&self) -> Cursor<'_> {
        Cursor {
            view: self.view(),
            position: BlockPosition::new(),
        }
    }

    /// Returns a view of this block's bytes for use with a [`BlockPosition`].
    pub fn view(&self) -> BlockView<'_> {
        let (entries, trailer) = self.data.as_ref().split_at(self.entries_len);
        let directory = &trailer[..trailer.len() - U32_LEN];
        BlockView {
            entries,
            restarts: directory.as_chunks().0,
        }
    }
}

#[cfg(test)]
const VALIDATED: &str = "block bytes were validated by Block::new";

/// One stored version of a user key, yielded by [`Cursor::next_record`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Record<'a> {
    /// The reconstructed user key.
    pub key: &'a [u8],
    /// The record's sequence number.
    pub seq: u64,
    /// The record's value or tombstone.
    pub value: Value<'a>,
}

/// The sequence number and payload found by [`Block::get`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Version<'a> {
    /// The record's sequence number.
    pub seq: u64,
    /// The record's value or tombstone.
    pub value: Value<'a>,
}

/// Ordered traversal of every record in a [`Block`].
///
/// This is a lending cursor rather than an [`Iterator`]: each [`Record`]
/// borrows the key rebuilt in the cursor's single reusable buffer, so records
/// need no individual allocation.
#[derive(Clone, Debug)]
#[cfg(test)]
pub struct Cursor<'a> {
    view: BlockView<'a>,
    position: BlockPosition,
}

#[cfg(test)]
impl Cursor<'_> {
    /// Advances to the next record, or returns `None` at the end of the block.
    pub fn next_record(&mut self) -> Option<Record<'_>> {
        self.position.next_record(self.view).expect(VALIDATED)
    }
}

/// Resumable traversal state that does not borrow the block it walks.
///
/// Because a position holds only an offset and the rebuilt key, a run iterator
/// can own it beside the buffer or cached block being read, which a borrowing
/// [`Cursor`] cannot do without a self-referential struct. The block is passed
/// to each step, and the key buffer is reused across blocks.
///
/// Between calls to [`reset`](Self::reset) every step must be given the same
/// block bytes. Passing different bytes is safe but yields an error or
/// meaningless records.
#[derive(Clone, Debug, Default)]
pub struct BlockPosition {
    offset: usize,
    key: Vec<u8>,
}

impl BlockPosition {
    /// Creates a position before the first record of a block.
    pub fn new() -> Self {
        Self::default()
    }

    /// Moves before the first record, keeping the key buffer's capacity.
    pub fn reset(&mut self) {
        self.offset = 0;
        self.key.clear();
    }

    /// Returns whether no records remain in `block`.
    ///
    /// A multi-block iterator should call this to decide whether to load the
    /// next block, then take its record. Loading the next block after a
    /// `next_record` call returns `None`, within a function returning that
    /// call's record, is rejected by the borrow checker.
    #[cfg(test)]
    pub fn is_finished(&self, block: BlockView<'_>) -> bool {
        self.offset >= block.entries.len()
    }

    /// Returns the key of the record most recently read.
    pub(crate) fn key(&self) -> &[u8] {
        &self.key
    }

    /// Returns the offset of the next record in the record region.
    pub(crate) fn offset(&self) -> usize {
        self.offset
    }

    /// Moves before the first record of `block` whose user key is at or above
    /// `target`, or to the end of the block if there is none.
    ///
    /// Versions of one key are stored newest first, so this stops at the newest
    /// version. It searches the restart directory, then walks at most one
    /// restart interval, reusing the key buffer. On error the position is reset.
    pub fn seek(&mut self, block: BlockView<'_>, target: &[u8]) -> Result<(), CorruptBlock> {
        self.reset();
        let result = self.walk_to(block, target);
        if result.is_err() {
            self.reset();
        }
        result
    }

    /// Walks from the search start to the first record at or above `target`.
    /// The buffer holds the previous key, which that record is encoded against.
    fn walk_to(&mut self, block: BlockView<'_>, target: &[u8]) -> Result<(), CorruptBlock> {
        let Some(start) = block.search_start(target)? else {
            return Ok(());
        };
        self.offset = start;
        let mut rest = &block.entries[start..];
        while !rest.is_empty() {
            let record = block.decode(&mut rest)?;
            record
                .check_key_len(self.key.len())
                .map_err(|kind| CorruptBlock {
                    offset: self.offset,
                    kind,
                })?;
            if cmp_joined(&self.key[..record.shared], record.suffix, target).is_ge() {
                return Ok(());
            }
            self.offset = block.entries.len() - rest.len();
            self.key.truncate(record.shared);
            self.key.extend_from_slice(record.suffix);
        }
        Ok(())
    }

    /// Advances to the next record of `block`, or returns `None` at its end.
    ///
    /// On error the position is unchanged.
    pub fn next_record<'p>(
        &'p mut self,
        block: BlockView<'p>,
    ) -> Result<Option<Record<'p>>, CorruptBlock> {
        let Some(mut rest) = block.entries.get(self.offset..) else {
            return Err(CorruptBlock {
                offset: block.entries.len(),
                kind: Corruption::Truncated,
            });
        };
        if rest.is_empty() {
            return Ok(None);
        }
        let record = block.decode(&mut rest)?;
        record
            .check_key_len(self.key.len())
            .map_err(|kind| CorruptBlock {
                offset: self.offset,
                kind,
            })?;

        self.offset = block.entries.len() - rest.len();
        self.key.truncate(record.shared);
        self.key.extend_from_slice(record.suffix);
        Ok(Some(Record {
            key: &self.key,
            seq: record.seq,
            value: record.value,
        }))
    }
}

/// Constant-time access to block bytes whose validity is already established.
///
/// [`open`](Self::open) inspects only the trailer, so a reader can decompress
/// into a reusable buffer and look up one key without walking the block.
///
/// # Trust contract
///
/// Reads are memory safe, panic free and bounded by the block length on every
/// input, and report any malformed record, offset or prefix they touch as
/// [`CorruptBlock`]. They cannot see what they do not touch: on invalid bytes,
/// such as a block with disordered keys, a read may return a wrong answer
/// instead of an error.
///
/// A correct answer therefore requires bytes known to be a valid block: built
/// by [`BlockBuilder`](super::BlockBuilder), or accepted once by
/// [`Block::new`], and unchanged since. A checksum can show bytes are
/// unchanged. It cannot show that whoever wrote them produced a valid block.
#[derive(Clone, Copy, Debug)]
pub struct BlockView<'a> {
    entries: &'a [u8],
    restarts: &'a [[u8; U32_LEN]],
}

impl<'a> BlockView<'a> {
    /// Checks the trailer in constant time and returns a view of `bytes`.
    pub fn open(bytes: &'a [u8]) -> Result<Self, CorruptBlock> {
        let corrupt = |offset, kind| CorruptBlock { offset, kind };

        if bytes.len() > MAX_BLOCK_LEN {
            return Err(corrupt(0, Corruption::BlockTooLong));
        }
        let Some((body, count)) = bytes.split_last_chunk::<U32_LEN>() else {
            return Err(corrupt(0, Corruption::Truncated));
        };
        let entries_len = (u32::from_le_bytes(*count) as usize)
            .checked_mul(U32_LEN)
            .and_then(|directory_len| body.len().checked_sub(directory_len))
            .ok_or(corrupt(body.len(), Corruption::InvalidRestarts))?;
        let (entries, directory) = body.split_at(entries_len);
        let restarts = directory.as_chunks().0;

        // Records require a first restart at offset 0, and no records none.
        let directory_matches = match restarts.first() {
            Some(&first) => !entries.is_empty() && restart_offset(first) == 0,
            None => entries.is_empty(),
        };
        if !directory_matches {
            return Err(corrupt(entries_len, Corruption::InvalidRestarts));
        }
        Ok(Self { entries, restarts })
    }

    /// Returns the length of the record region.
    pub(crate) fn entries_len(self) -> usize {
        self.entries.len()
    }

    /// Returns the newest version of `user_key`, which may be a tombstone, or
    /// `None` if the block has no record for that key.
    ///
    /// Performs no heap allocation.
    pub fn get(self, target: &[u8]) -> Result<Option<Version<'a>>, CorruptBlock> {
        let Some(start) = self.search_start(target)? else {
            return Ok(None);
        };
        let mut rest = &self.entries[start..];

        // Keys are compared without rebuilding them. Scanning continues only
        // while the current key is below the target, and `matched` is the
        // length of their common prefix. `matched` never exceeds the target's
        // length, so the slicing below holds on any input.
        let mut matched = 0;
        while !rest.is_empty() {
            let record = self.decode(&mut rest)?;
            // This key keeps a byte at position `matched` where the previous
            // key was already below the target, so it is below it as well.
            if record.shared > matched {
                continue;
            }
            // The first `shared` bytes are known to equal the target's.
            let target_rest = &target[record.shared..];
            let common = common_prefix_len(record.suffix, target_rest);
            matched = record.shared + common;
            match record.suffix[common..].cmp(&target_rest[common..]) {
                Ordering::Less => {}
                Ordering::Equal => {
                    return Ok(Some(Version {
                        seq: record.seq,
                        value: record.value,
                    }));
                }
                Ordering::Greater => return Ok(None),
            }
        }
        Ok(None)
    }

    /// Returns the offset of the record to start a search for `target` from,
    /// within the record region, or `None` if the block has no records.
    fn search_start(self, target: &[u8]) -> Result<Option<usize>, CorruptBlock> {
        // Search for the last restart strictly below the target, rather than
        // at or below it. Versions of one user key can span restarts, and the
        // newest is the first of them.
        let mut failure = None;
        let below = self
            .restarts
            .partition_point(|&restart| match self.restart_key(restart) {
                Ok(key) => key < target,
                Err(error) => {
                    failure = Some(error);
                    false
                }
            });
        if let Some(error) = failure {
            return Err(error);
        }
        let Some(&start) = self.restarts.get(below.saturating_sub(1)) else {
            return Ok(None);
        };
        let offset = restart_offset(start);
        if offset > self.entries.len() {
            return Err(CorruptBlock {
                offset,
                kind: Corruption::InvalidRestarts,
            });
        }
        Ok(Some(offset))
    }

    /// Returns the key of a restart record, which is stored whole.
    fn restart_key(self, restart: [u8; U32_LEN]) -> Result<&'a [u8], CorruptBlock> {
        let offset = restart_offset(restart);
        let invalid = CorruptBlock {
            offset,
            kind: Corruption::InvalidRestarts,
        };
        let mut rest = self.entries.get(offset..).ok_or(invalid)?;
        let record = self.decode(&mut rest)?;
        if record.shared != 0 {
            return Err(invalid);
        }
        Ok(record.suffix)
    }

    /// Decodes the record at the front of `rest`, a suffix of the entries.
    fn decode(self, rest: &mut &'a [u8]) -> Result<RawRecord<'a>, CorruptBlock> {
        decode_record(rest).map_err(|kind| CorruptBlock {
            offset: self.entries.len() - rest.len(),
            kind,
        })
    }
}

/// Orders the key `prefix` + `suffix` against `target` without joining them.
fn cmp_joined(prefix: &[u8], suffix: &[u8], target: &[u8]) -> Ordering {
    let (head, tail) = target.split_at(prefix.len().min(target.len()));
    prefix.cmp(head).then_with(|| suffix.cmp(tail))
}

fn restart_offset(restart: [u8; U32_LEN]) -> usize {
    u32::from_le_bytes(restart) as usize
}

struct RawRecord<'a> {
    shared: usize,
    suffix: &'a [u8],
    seq: u64,
    value: Value<'a>,
}

impl RawRecord<'_> {
    /// Checks the key this record forms from a previous key of the given
    /// length. Passing this also bounds any key buffer by [`MAX_KEY_LEN`].
    fn check_key_len(&self, previous_len: usize) -> Result<(), Corruption> {
        if self.shared > previous_len {
            return Err(Corruption::InvalidPrefix);
        }
        if self.shared + self.suffix.len() > MAX_KEY_LEN {
            return Err(Corruption::KeyTooLong);
        }
        Ok(())
    }
}

/// Decodes the record at the front of `input`, advancing past it on success.
fn decode_record<'a>(input: &mut &'a [u8]) -> Result<RawRecord<'a>, Corruption> {
    let mut cursor = *input;
    let shared = varint::read(&mut cursor)?;
    let suffix_len = varint::read(&mut cursor)?;
    let value_tag = varint::read(&mut cursor)?;
    let seq = varint::read(&mut cursor)?;

    if shared > MAX_KEY_LEN as u64 || suffix_len > MAX_KEY_LEN as u64 {
        return Err(Corruption::KeyTooLong);
    }
    if value_tag > MAX_VALUE_LEN as u64 + 1 {
        return Err(Corruption::ValueTooLong);
    }
    // The limit checks above make these conversions lossless.
    let (suffix, cursor) = cursor
        .split_at_checked(suffix_len as usize)
        .ok_or(Corruption::Truncated)?;
    let (value, cursor) = match value_tag.checked_sub(1) {
        None => (Value::Tombstone, cursor),
        Some(value_len) => {
            let (bytes, cursor) = cursor
                .split_at_checked(value_len as usize)
                .ok_or(Corruption::Truncated)?;
            (Value::Bytes(bytes), cursor)
        }
    };

    *input = cursor;
    Ok(RawRecord {
        shared: shared as usize,
        suffix,
        seq,
        value,
    })
}

/// Full validation state, which can continue across adjacent blocks.
///
/// The first record of a block has `shared == 0`, so walking a second block
/// with the same state also orders it against the previous block's last key.
/// The one key buffer is bounded by [`MAX_KEY_LEN`], whatever the input claims.
#[derive(Debug, Default)]
pub(crate) struct RecordWalk {
    /// The most recently walked user key.
    pub(crate) key: Vec<u8>,
    seq: u64,
    /// Records walked so far, across every block.
    pub(crate) record_count: usize,
    /// The largest sequence number walked so far.
    pub(crate) max_seq: u64,
    /// Tombstones or older versions that a complete merge could discard.
    pub(crate) reclaimable: bool,
    /// Logical output estimate for a run rebuilt with different block cuts.
    pub(crate) build_estimate: usize,
}

impl RecordWalk {
    /// Checks every format invariant of one block and returns the length of
    /// its record region.
    pub(crate) fn validate_block(&mut self, bytes: &[u8]) -> Result<usize, CorruptBlock> {
        let corrupt = |offset, kind| CorruptBlock { offset, kind };

        let BlockView { entries, restarts } = BlockView::open(bytes)?;
        let mut pending_restarts = restarts
            .iter()
            .map(|&restart| restart_offset(restart))
            .peekable();

        let mut rest = entries;
        while !rest.is_empty() {
            let offset = entries.len() - rest.len();
            let record = decode_record(&mut rest).map_err(|kind| corrupt(offset, kind))?;

            let is_restart = pending_restarts.next_if_eq(&offset).is_some();
            if is_restart && record.shared != 0 {
                return Err(corrupt(offset, Corruption::InvalidRestarts));
            }
            record
                .check_key_len(self.key.len())
                .map_err(|kind| corrupt(offset, kind))?;
            if self.record_count > 0 {
                // Both keys share their first `shared` bytes, so comparing
                // what follows orders the whole keys.
                match cmp_internal(
                    &self.key[record.shared..],
                    self.seq,
                    record.suffix,
                    record.seq,
                ) {
                    Ordering::Less => {}
                    Ordering::Equal => return Err(corrupt(offset, Corruption::DuplicateKey)),
                    Ordering::Greater => return Err(corrupt(offset, Corruption::OutOfOrder)),
                }
            }

            self.reclaimable |= record.value == Value::Tombstone
                || (self.record_count > 0 && self.key[record.shared..] == *record.suffix);
            self.key.truncate(record.shared);
            self.key.extend_from_slice(record.suffix);
            let value_bytes = match record.value {
                Value::Bytes(bytes) => bytes.len(),
                Value::Tombstone => 0,
            };
            self.build_estimate = self.build_estimate.saturating_add(
                (self.key.len() + value_bytes)
                    .saturating_mul(4)
                    .saturating_add(256),
            );
            self.seq = record.seq;
            self.max_seq = self.max_seq.max(record.seq);
            self.record_count += 1;
        }

        // Offsets that are unsorted, repeated, past the end or off a record
        // boundary are never consumed by the walk above.
        if pending_restarts.next().is_some() {
            return Err(corrupt(entries.len(), Corruption::InvalidRestarts));
        }
        Ok(entries.len())
    }
}

/// Why bytes were rejected by [`Block::new`] or a [`BlockView`] read.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CorruptBlock {
    /// Byte offset in the block of the offending record or directory.
    pub offset: usize,
    /// The invariant that was violated.
    pub kind: Corruption,
}

/// The kinds of structural corruption detected in a block.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Corruption {
    /// The bytes end before a required field does.
    Truncated,
    /// A varint overflows `u64` or is not in canonical form.
    InvalidVarint,
    /// A key exceeds [`MAX_KEY_LEN`](super::MAX_KEY_LEN).
    KeyTooLong,
    /// A value exceeds [`MAX_VALUE_LEN`](super::MAX_VALUE_LEN).
    ValueTooLong,
    /// The block exceeds the 128 MiB format limit.
    BlockTooLong,
    /// A record reuses more key bytes than the previous key has.
    InvalidPrefix,
    /// The restart directory does not match the records.
    InvalidRestarts,
    /// A record sorts before its predecessor.
    OutOfOrder,
    /// A record repeats its predecessor's user key and sequence number.
    DuplicateKey,
}

impl fmt::Display for Corruption {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Truncated => "truncated field",
            Self::InvalidVarint => "invalid varint",
            Self::KeyTooLong => "key exceeds limit",
            Self::ValueTooLong => "value exceeds limit",
            Self::BlockTooLong => "block exceeds limit",
            Self::InvalidPrefix => "shared prefix exceeds previous key",
            Self::InvalidRestarts => "restart directory does not match records",
            Self::OutOfOrder => "record out of order",
            Self::DuplicateKey => "duplicate user key and sequence number",
        })
    }
}

impl fmt::Display for CorruptBlock {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "corrupt block at offset {}: {}", self.offset, self.kind)
    }
}

impl std::error::Error for CorruptBlock {}
