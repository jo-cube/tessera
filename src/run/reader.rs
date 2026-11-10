use std::fmt;

use crate::block::{BlockPosition, BlockView, MAX_KEY_LEN, Record, RecordWalk, Version};

use super::codec::{Codec, Decompressor, plausible_decoded_len};
use super::frame::{self, Frame};
use super::{
    CorruptRun, FOOTER_LEN, FORMAT_VERSION, MAGIC, RunCorruption, TABLE_ENTRY_LEN, read_u32,
    read_u64,
};

/// A valid run over any byte storage, such as `Vec<u8>`, `Arc<[u8]>` or
/// `&[u8]`.
///
/// Holding a `Run` is proof of validity: it comes from
/// [`RunBuilder::finish`](super::RunBuilder::finish) or from a full check by
/// [`new`](Self::new). Reads never repeat that check. They still return
/// errors, because a checksum mismatch in memory is a detectable event and not
/// a programming error. The storage must keep returning the same bytes from
/// `as_ref`.
///
/// A run is immutable and has no interior mutability, so it can be shared
/// between threads whenever its storage can. Each thread needs its own
/// [`ReadWorkspace`].
#[derive(Clone, Debug)]
pub struct Run<B> {
    data: B,
}

const VALIDATED: &str = "run bytes were validated or built by this crate";

impl<B: AsRef<[u8]>> Run<B> {
    /// Validates `data` in full and wraps it.
    ///
    /// This costs O(run length) and reports the first problem found. Temporary
    /// memory is one decoded block, at most
    /// [`MAX_BLOCK_LEN`](crate::block::MAX_BLOCK_LEN).
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
    /// Decodes at most one block. Once the workspace has grown to the largest
    /// block read, this performs no heap allocation.
    pub fn get<'r>(
        &'r self,
        user_key: &[u8],
        workspace: &'r mut ReadWorkspace,
    ) -> Result<Option<Version<'r>>, CorruptRun> {
        self.view().get(user_key, workspace)
    }

    /// Returns a cursor positioned before the first record, which holds the
    /// workspace for as long as it lives. Use [`RunCursor::seek`] or
    /// [`RunCursor::seek_prefix`] to start elsewhere.
    pub fn cursor<'r>(&'r self, workspace: &'r mut ReadWorkspace) -> RunCursor<'r> {
        self.view().cursor(workspace)
    }

    /// Returns a view of this run's bytes.
    pub fn view(&self) -> RunView<'_> {
        RunView::open(self.data.as_ref()).expect(VALIDATED)
    }

    /// Returns the run's serialized bytes.
    pub fn as_bytes(&self) -> &[u8] {
        self.data.as_ref()
    }
}

/// Reusable storage for reading runs, created once per reader.
///
/// One workspace serves any number of runs and codecs, and its results are
/// borrowed from it, so the borrow checker prevents overwriting a value that
/// is still in use. It retains the capacity of the largest block it has
/// decoded, the longest key and prefix a cursor has held, and a Zstandard
/// context once it has read a Zstandard frame. Keys and prefixes are limited
/// to [`MAX_KEY_LEN`], so a warm workspace serves a new cursor per query with
/// no heap allocation.
#[derive(Default)]
pub struct ReadWorkspace {
    scratch: Vec<u8>,
    decompressor: Decompressor,
    position: BlockPosition,
    prefix: Vec<u8>,
    blocks_loaded: u64,
}

impl ReadWorkspace {
    /// Creates an empty workspace, which allocates nothing until it decodes a
    /// compressed block.
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns the bytes retained for decoded blocks.
    pub fn capacity(&self) -> usize {
        self.scratch.capacity()
    }

    /// Returns how many blocks reads through this workspace have loaded, each
    /// of which is a decompression for a compressed run.
    ///
    /// A lookup or a seek that needs no block does not count.
    pub fn blocks_loaded(&self) -> u64 {
        self.blocks_loaded
    }

    /// Returns a frame's block, decoding and verifying it if it is compressed.
    fn load<'r>(&'r mut self, frame: &Frame<'r>) -> Result<&'r [u8], CorruptRun> {
        self.blocks_loaded += 1;
        if frame.codec == Codec::None {
            return Ok(frame.stored);
        }
        frame.verify_checksum()?;

        // Both checks precede sizing the buffer from a declared length.
        let len = frame.decoded_len;
        if !plausible_decoded_len(frame.codec, frame.stored, len) {
            return Err(frame.error(RunCorruption::InvalidFrame));
        }
        // Only growth needs initializing, and stale bytes past `len` are never
        // exposed.
        if self.scratch.len() < len {
            self.scratch.reserve_exact(len - self.scratch.len());
            self.scratch.resize(len, 0);
        }
        let written =
            self.decompressor
                .decompress(frame.codec, frame.stored, &mut self.scratch[..len]);
        if written != Some(len) {
            return Err(frame.error(RunCorruption::CorruptPayload));
        }
        Ok(&self.scratch[..len])
    }
}

/// Returns a block already decoded into `scratch` by [`ReadWorkspace::load`].
fn loaded_bytes<'r>(scratch: &'r [u8], frame: &Frame<'r>) -> &'r [u8] {
    if frame.codec == Codec::None {
        frame.stored
    } else {
        &scratch[..frame.decoded_len]
    }
}

impl fmt::Debug for ReadWorkspace {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ReadWorkspace")
            .field("capacity", &self.capacity())
            .finish_non_exhaustive()
    }
}

/// Ordered traversal of every record in a run, across block boundaries.
///
/// This is a lending cursor rather than an [`Iterator`]: each [`Record`]
/// borrows the key rebuilt in the cursor's single reusable buffer, and its
/// value from the run or the workspace. Each compressed block is decoded once.
/// Because the cursor holds the workspace exclusively, its decoded block
/// cannot be replaced between steps; a concurrent lookup uses a second
/// workspace.
///
/// [`seek`](Self::seek) and [`seek_prefix`](Self::seek_prefix) reposition a
/// cursor any number of times, forward or backward. All stored versions and
/// tombstones are yielded in internal order; a cursor applies no visibility.
///
/// ```
/// use tessera::block::Value;
/// use tessera::run::{Codec, ReadWorkspace, RunBuilder};
///
/// let mut builder = RunBuilder::new(Codec::Zstd);
/// builder.add(b"user:1", 7, Value::Bytes(b"a"))?;
/// builder.add(b"user:2", 8, Value::Tombstone)?;
/// builder.add(b"zone:1", 9, Value::Bytes(b"b"))?;
/// let run = builder.finish();
///
/// let mut workspace = ReadWorkspace::new();
/// let mut cursor = run.cursor(&mut workspace);
/// cursor.seek_prefix(b"user:")?;
/// let mut seen = 0;
/// while let Some(record) = cursor.next_record()? {
///     assert!(record.key.starts_with(b"user:"));
///     seen += 1;
/// }
/// assert_eq!(seen, 2);
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
#[derive(Debug)]
pub struct RunCursor<'a> {
    view: RunView<'a>,
    workspace: &'a mut ReadWorkspace,
    next_block: usize,
    loaded: Option<LoadedBlock<'a>>,
    failure: Option<CorruptRun>,
}

/// The block a cursor is walking.
#[derive(Clone, Copy, Debug)]
struct LoadedBlock<'a> {
    frame: Frame<'a>,
    entries_len: usize,
}

impl<'a> RunCursor<'a> {
    /// Moves before the first record whose user key is at or above `target`,
    /// which is the newest version of that key, and iterates without a bound.
    ///
    /// Uses the sparse index and decodes at most one block, or none if the
    /// target is already in the block being walked or above every key. If this
    /// fails, [`next_record`](Self::next_record) reports the same error until a
    /// later seek succeeds.
    pub fn seek(&mut self, target: &[u8]) -> Result<(), CorruptRun> {
        self.workspace.prefix.clear();
        self.reposition(target)
    }

    /// Moves to the first record whose user key starts with `prefix` and
    /// iterates only while keys do.
    ///
    /// An empty prefix selects the whole run. Keys are treated as raw bytes.
    /// The range ends at the first key without the prefix, so a prefix of
    /// `0xFF` bytes needs no special handling. That key must be read to be
    /// seen, so a prefix whose last match ends a block loads the next block.
    /// A seek clears any bound set earlier.
    pub fn seek_prefix(&mut self, prefix: &[u8]) -> Result<(), CorruptRun> {
        self.workspace.prefix.clear();
        if prefix.len() > MAX_KEY_LEN {
            // No stored key is that long, and the buffer stays bounded.
            self.failure = None;
            self.finish();
            return Ok(());
        }
        self.workspace.prefix.extend_from_slice(prefix);
        self.reposition(prefix)
    }

    fn reposition(&mut self, target: &[u8]) -> Result<(), CorruptRun> {
        self.failure = None;
        let result = self.position_at(target);
        if let Err(error) = result {
            self.failure = Some(error);
        }
        result
    }

    fn position_at(&mut self, target: &[u8]) -> Result<(), CorruptRun> {
        let Some(index) = self.view.locate(target)? else {
            self.finish();
            return Ok(());
        };
        let block = match self.loaded {
            Some(block) if self.next_block == index + 1 => block,
            _ => self.load_block(index)?,
        };
        let workspace = &mut *self.workspace;
        let bytes = loaded_bytes(&workspace.scratch, &block.frame);
        let view = BlockView::open(bytes).map_err(|error| block.frame.block_error(error))?;
        workspace
            .position
            .seek(view, target)
            .map_err(|error| block.frame.block_error(error))
    }

    /// Moves past the last record of the run.
    fn finish(&mut self) {
        self.loaded = None;
        self.next_block = self.view.table.len();
    }

    /// Advances to the next record, or returns `None` at the end of the run or
    /// of the prefix. After `None`, it keeps returning `None` until a seek.
    pub fn next_record(&mut self) -> Result<Option<Record<'_>>, CorruptRun> {
        if let Some(error) = self.failure {
            return Err(error);
        }
        let Some(block) = self.block_with_records()? else {
            return Ok(None);
        };
        let workspace = &mut *self.workspace;
        let bytes = loaded_bytes(&workspace.scratch, &block.frame);
        let view = BlockView::open(bytes).map_err(|error| block.frame.block_error(error))?;
        let record = workspace
            .position
            .next_record(view)
            .map_err(|error| block.frame.block_error(error))?;
        match record {
            // Spelled out rather than `finish`, because the record borrows the
            // workspace in the other arm.
            Some(record) if !record.key.starts_with(&workspace.prefix) => {
                self.loaded = None;
                self.next_block = self.view.table.len();
                Ok(None)
            }
            record => Ok(record),
        }
    }

    /// Returns the block holding the next record, decoding blocks as needed.
    fn block_with_records(&mut self) -> Result<Option<LoadedBlock<'a>>, CorruptRun> {
        loop {
            if let Some(block) = self.loaded {
                if self.workspace.position.offset() < block.entries_len {
                    return Ok(Some(block));
                }
                self.loaded = None;
                self.workspace.position.reset();
            }
            if self.next_block >= self.view.table.len() {
                return Ok(None);
            }
            self.load_block(self.next_block)?;
        }
    }

    /// Decodes block `index` and makes it the one being walked. The previous
    /// block is forgotten first, because decoding overwrites it.
    fn load_block(&mut self, index: usize) -> Result<LoadedBlock<'a>, CorruptRun> {
        self.loaded = None;
        let frame = self.view.frame(index)?;
        let bytes = self.workspace.load(&frame)?;
        let entries_len = BlockView::open(bytes)
            .map_err(|error| frame.block_error(error))?
            .entries_len();
        let block = LoadedBlock { frame, entries_len };
        self.next_block = index + 1;
        self.loaded = Some(block);
        Ok(block)
    }
}

/// Constant-time access to run bytes whose validity is already established.
///
/// [`open`](Self::open) checks only the footer and the ends of the index.
///
/// # Trust contract
///
/// Reads are memory safe, panic free and bounded by the run length and
/// [`MAX_BLOCK_LEN`](crate::block::MAX_BLOCK_LEN) on every input, and report
/// any malformed footer, table entry, frame or record they touch as
/// [`CorruptRun`]. Compressed frames are checksummed before they are decoded.
///
/// Reads cannot see what they do not touch: on invalid bytes, such as an index
/// key that disagrees with its block or blocks in the wrong order, a read may
/// return a wrong answer instead of an error.
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

    /// Returns the newest version of `target`, which may be a tombstone, or
    /// `None` if the run has no record for that key.
    ///
    /// Decodes at most one block and never walks it. Once the workspace has
    /// grown to the largest block read, this performs no heap allocation.
    pub fn get<'r>(
        self,
        target: &[u8],
        workspace: &'r mut ReadWorkspace,
    ) -> Result<Option<Version<'r>>, CorruptRun>
    where
        'a: 'r,
    {
        let Some(block) = self.locate(target)? else {
            return Ok(None);
        };
        let frame = self.frame(block)?;
        let bytes = workspace.load(&frame)?;
        BlockView::open(bytes)
            .and_then(|view| view.get(target))
            .map_err(|error| frame.block_error(error))
    }

    /// Returns a cursor positioned before the first record.
    pub fn cursor<'r>(self, workspace: &'r mut ReadWorkspace) -> RunCursor<'r>
    where
        'a: 'r,
    {
        workspace.position.reset();
        workspace.prefix.clear();
        RunCursor {
            view: self,
            workspace,
            next_block: 0,
            loaded: None,
            failure: None,
        }
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

    /// Finds a frame and checks its header. `block` must be below the block
    /// count.
    fn frame(self, block: usize) -> Result<Frame<'a>, CorruptRun> {
        let start = frame_offset(self.table[block]);
        let end = self
            .table
            .get(block + 1)
            .map_or(self.frames.len(), |&next| frame_offset(next));
        let (header, stored) = self
            .frames
            .get(start..end)
            .and_then(<[u8]>::split_first_chunk::<{ frame::HEADER_LEN }>)
            .ok_or(self.invalid_index(block))?;
        Frame::parse(start, header, stored)
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
    let mut workspace = ReadWorkspace::new();
    let mut walk = RecordWalk::default();
    for block in 0..view.table.len() {
        let frame = view.frame(block)?;
        // Decoding verifies compressed frames.
        if frame.codec == Codec::None {
            frame.verify_checksum()?;
        }
        let decoded = workspace.load(&frame)?;

        let records_before = walk.record_count;
        walk.validate_block(decoded)
            .map_err(|error| frame.block_error(error))?;
        if walk.record_count == records_before {
            return Err(frame.error(RunCorruption::EmptyBlock));
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
