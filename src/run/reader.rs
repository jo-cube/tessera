use crate::block::{BlockPosition, BlockView, CorruptBlock, Record, RecordWalk, Version};

use super::{
    CODEC_RAW, CorruptRun, FOOTER_LEN, FORMAT_VERSION, FRAME_HEADER_LEN, MAGIC, RunCorruption,
    TABLE_ENTRY_LEN, read_u32, read_u64,
};

/// A valid run over any byte storage, such as `Vec<u8>`, `Arc<[u8]>` or
/// `&[u8]`.
///
/// Holding a `Run` is proof of validity: it comes from
/// [`RunBuilder::finish`](super::RunBuilder::finish) or from a full check by
/// [`new`](Self::new). Reads neither repeat those checks nor return errors. The
/// storage must keep returning the same bytes from `as_ref`; if it does not,
/// reads may panic or return wrong data.
///
/// A run is immutable and has no interior mutability, so it can be shared
/// between threads whenever its storage can.
#[derive(Clone, Debug)]
pub struct Run<B> {
    data: B,
}

const VALIDATED: &str = "run bytes were validated or built by this crate";

impl<B: AsRef<[u8]>> Run<B> {
    /// Validates `data` in full and wraps it. This costs O(run length) and
    /// reports the first structural problem found.
    pub fn new(data: B) -> Result<Self, CorruptRun> {
        validate(data.as_ref())?;
        Ok(Self { data })
    }

    pub(super) fn from_builder(data: B) -> Self {
        Self { data }
    }

    /// Returns the number of stored records, counting every version.
    pub fn len(&self) -> usize {
        self.view().len()
    }

    /// Returns whether the run stores no records.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Returns the number of blocks.
    pub fn block_count(&self) -> usize {
        self.view().block_count()
    }

    /// Returns the newest version of `user_key`, which may be a tombstone, or
    /// `None` if the run has no record for that key.
    ///
    /// Performs no heap allocation.
    pub fn get(&self, user_key: &[u8]) -> Option<Version<'_>> {
        self.view().get(user_key).expect(VALIDATED)
    }

    /// Returns a cursor positioned before the first record.
    pub fn cursor(&self) -> RunCursor<'_> {
        RunCursor {
            view: self.view(),
            position: RunPosition::new(),
        }
    }

    /// Returns a view of this run's bytes for use with a [`RunPosition`].
    pub fn view(&self) -> RunView<'_> {
        RunView::open(self.data.as_ref()).expect(VALIDATED)
    }

    /// Returns the run's serialized bytes.
    pub fn as_bytes(&self) -> &[u8] {
        self.data.as_ref()
    }
}

/// Ordered traversal of every record in a [`Run`].
///
/// This is a lending cursor rather than an [`Iterator`]: each [`Record`]
/// borrows the key rebuilt in the cursor's single reusable buffer.
#[derive(Clone, Debug)]
pub struct RunCursor<'a> {
    view: RunView<'a>,
    position: RunPosition,
}

impl RunCursor<'_> {
    /// Advances to the next record, or returns `None` at the end of the run.
    pub fn next_record(&mut self) -> Option<Record<'_>> {
        self.position.next_record(self.view).expect(VALIDATED)
    }
}

/// Resumable traversal state that does not borrow the run it walks.
///
/// A position holds a block index and a [`BlockPosition`], so it can be owned
/// beside the storage being read. Every step must be given the same run bytes.
/// Passing different bytes is safe but yields an error or meaningless records.
#[derive(Clone, Debug, Default)]
pub struct RunPosition {
    block: usize,
    inner: BlockPosition,
}

impl RunPosition {
    /// Creates a position before the first record of a run.
    pub fn new() -> Self {
        Self::default()
    }

    /// Moves before the first record, keeping the key buffer's capacity.
    pub fn reset(&mut self) {
        self.block = 0;
        self.inner.reset();
    }

    /// Advances to the next record of `run`, moving across block boundaries,
    /// or returns `None` at its end.
    ///
    /// Each step costs one bounds-checked frame lookup. On error the position
    /// does not advance past the record that failed.
    pub fn next_record<'p>(
        &'p mut self,
        run: RunView<'p>,
    ) -> Result<Option<Record<'p>>, CorruptRun> {
        while self.block < run.table.len() {
            let (payload_start, view) = run.block_view(self.block)?;
            if self.inner.is_finished(view) {
                self.block += 1;
                self.inner.reset();
                continue;
            }
            return self
                .inner
                .next_record(view)
                .map_err(|error| in_block(payload_start, error));
        }
        Ok(None)
    }
}

/// Constant-time access to run bytes whose validity is already established.
///
/// [`open`](Self::open) checks only the footer and the ends of the index.
///
/// # Trust contract
///
/// Reads are memory safe, panic free and bounded by the run length on every
/// input, and report any malformed footer, table entry, frame or record they
/// touch as [`CorruptRun`]. They cannot see what they do not touch: on invalid
/// bytes, such as an index key that disagrees with its block or blocks in the
/// wrong order, a read may return a wrong answer instead of an error.
///
/// A correct answer therefore requires bytes known to be a valid run: built by
/// [`RunBuilder`](super::RunBuilder), or accepted once by [`Run::new`], and
/// unchanged since. A checksum can show bytes are unchanged. It cannot show
/// that whoever wrote them produced a valid run.
#[derive(Clone, Copy, Debug)]
pub struct RunView<'a> {
    frames: &'a [u8],
    table: &'a [[u8; TABLE_ENTRY_LEN]],
    keys: &'a [u8],
    record_count: usize,
}

impl<'a> RunView<'a> {
    /// Checks the footer and the ends of the index in constant time and
    /// returns a view of `bytes`.
    pub fn open(bytes: &'a [u8]) -> Result<Self, CorruptRun> {
        let Some((body, footer)) = bytes.split_last_chunk::<FOOTER_LEN>() else {
            return Err(corrupt(0, RunCorruption::Truncated));
        };
        let footer_offset = body.len();
        if read_u32(footer, 24) != MAGIC {
            return Err(corrupt(footer_offset, RunCorruption::InvalidMagic));
        }
        if read_u32(footer, 20) != FORMAT_VERSION {
            return Err(corrupt(footer_offset, RunCorruption::UnsupportedVersion));
        }

        let invalid_footer = corrupt(footer_offset, RunCorruption::InvalidFooter);
        let record_count = usize::try_from(read_u64(footer, 8)).map_err(|_| invalid_footer)?;
        let (frames, index) = usize::try_from(read_u64(footer, 0))
            .ok()
            .and_then(|index_offset| body.split_at_checked(index_offset))
            .ok_or(invalid_footer)?;
        let (table, keys) = (read_u32(footer, 16) as usize)
            .checked_mul(TABLE_ENTRY_LEN)
            .and_then(|table_len| index.split_at_checked(table_len))
            .ok_or(invalid_footer)?;
        let table: &[[u8; TABLE_ENTRY_LEN]] = table.as_chunks().0;

        let view = Self {
            frames,
            table,
            keys,
            record_count,
        };
        match (table.first(), table.last()) {
            (Some(&first), Some(&last)) => {
                if frame_offset(first) != 0 {
                    return Err(view.invalid_index(0));
                }
                if key_end(last) != keys.len() {
                    return Err(view.invalid_index(table.len() - 1));
                }
            }
            _ if frames.is_empty() && keys.is_empty() && record_count == 0 => {}
            _ => return Err(invalid_footer),
        }
        Ok(view)
    }

    /// Returns the number of stored records, as declared by the footer.
    pub fn len(self) -> usize {
        self.record_count
    }

    /// Returns whether the footer declares no records.
    pub fn is_empty(self) -> bool {
        self.record_count == 0
    }

    /// Returns the number of blocks.
    pub fn block_count(self) -> usize {
        self.table.len()
    }

    /// Returns the bytes used by the table and key area.
    pub fn index_len(self) -> usize {
        size_of_val(self.table) + self.keys.len()
    }

    /// Returns the index of the only block that can contain `target`, or
    /// `None` if `target` sorts after every key in the run.
    ///
    /// This is the first block whose last user key is at or above `target`.
    /// An earlier block ends below `target`, so it cannot hold the key, and
    /// any later block holds only older versions.
    pub fn locate(self, target: &[u8]) -> Result<Option<usize>, CorruptRun> {
        let (mut low, mut high) = (0, self.table.len());
        while low < high {
            let middle = low + (high - low) / 2;
            if self.last_key(middle)? < target {
                low = middle + 1;
            } else {
                high = middle;
            }
        }
        Ok((low < self.table.len()).then_some(low))
    }

    /// Returns the newest version of `user_key`, which may be a tombstone, or
    /// `None` if the run has no record for that key.
    ///
    /// Reads one block without walking it, and performs no heap allocation.
    pub fn get(self, target: &[u8]) -> Result<Option<Version<'a>>, CorruptRun> {
        let Some(block) = self.locate(target)? else {
            return Ok(None);
        };
        let (payload_start, view) = self.block_view(block)?;
        view.get(target)
            .map_err(|error| in_block(payload_start, error))
    }

    /// Returns a block's index key. `block` must be below the block count.
    fn last_key(self, block: usize) -> Result<&'a [u8], CorruptRun> {
        let start = match block.checked_sub(1) {
            Some(previous) => key_end(self.table[previous]),
            None => 0,
        };
        self.keys
            .get(start..key_end(self.table[block]))
            .ok_or(self.invalid_index(block))
    }

    /// Returns a block's bytes and their offset in the run, after checking
    /// its frame header. `block` must be below the block count.
    fn payload(self, block: usize) -> Result<(usize, &'a [u8]), CorruptRun> {
        let start = frame_offset(self.table[block]);
        let end = self
            .table
            .get(block + 1)
            .map_or(self.frames.len(), |&next| frame_offset(next));
        let (header, payload) = self
            .frames
            .get(start..end)
            .and_then(<[u8]>::split_first_chunk::<FRAME_HEADER_LEN>)
            .ok_or(self.invalid_index(block))?;

        if header[0] != CODEC_RAW {
            return Err(corrupt(start, RunCorruption::UnsupportedCodec));
        }
        let declared_len = read_u32(header, 4) as usize;
        if header[1..4] != [0; 3] || declared_len != payload.len() || read_u32(header, 8) != 0 {
            return Err(corrupt(start, RunCorruption::InvalidFrame));
        }
        Ok((start + FRAME_HEADER_LEN, payload))
    }

    /// Opens a block's view in constant time. `block` must be below the block
    /// count.
    fn block_view(self, block: usize) -> Result<(usize, BlockView<'a>), CorruptRun> {
        let (payload_start, payload) = self.payload(block)?;
        let view = BlockView::open(payload).map_err(|error| in_block(payload_start, error))?;
        Ok((payload_start, view))
    }

    fn invalid_index(self, block: usize) -> CorruptRun {
        corrupt(
            self.frames.len() + block * TABLE_ENTRY_LEN,
            RunCorruption::InvalidIndex,
        )
    }
}

fn corrupt(offset: usize, kind: RunCorruption) -> CorruptRun {
    CorruptRun { offset, kind }
}

fn in_block(payload_start: usize, error: CorruptBlock) -> CorruptRun {
    corrupt(
        payload_start + error.offset,
        RunCorruption::Block(error.kind),
    )
}

/// An offset too large for `usize` becomes one that no slice can satisfy.
fn frame_offset(entry: [u8; TABLE_ENTRY_LEN]) -> usize {
    usize::try_from(read_u64(&entry, 0)).unwrap_or(usize::MAX)
}

fn key_end(entry: [u8; TABLE_ENTRY_LEN]) -> usize {
    read_u32(&entry, 8) as usize
}

/// Checks every invariant in the module documentation.
///
/// Frames tile the frame region because the first starts at 0, each ends where
/// the next starts and each must hold its header. The key area is tiled the
/// same way, and matching every index key against its block completes the
/// index checks.
fn validate(bytes: &[u8]) -> Result<(), CorruptRun> {
    let view = RunView::open(bytes)?;
    let mut walk = RecordWalk::default();
    for block in 0..view.table.len() {
        let (payload_start, payload) = view.payload(block)?;
        let records_before = walk.record_count;
        walk.validate_block(payload)
            .map_err(|error| in_block(payload_start, error))?;
        if walk.record_count == records_before {
            return Err(corrupt(payload_start, RunCorruption::EmptyBlock));
        }
        if view.last_key(block)? != walk.key {
            return Err(view.invalid_index(block));
        }
    }
    if walk.record_count != view.record_count {
        return Err(corrupt(
            bytes.len() - FOOTER_LEN,
            RunCorruption::RecordCountMismatch,
        ));
    }
    Ok(())
}
