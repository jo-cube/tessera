use std::fmt;
use std::sync::Arc;

use crate::block::{BlockPosition, BlockView, MAX_KEY_LEN, Record, RecordWalk, Value, Version};

use super::codec::{Codec, Decompressor, plausible_decoded_len};
use super::dictionary::{MAX_DICTIONARY_LEN, RunDictionary};
use super::filter::{self as key_filter, KeyFilter, KeyHashes};
use super::frame::{self, Frame};
use super::{
    CorruptRun, DICTIONARY_FOOTER_LEN, DICTIONARY_VERSION, FOOTER_LEN, FORMAT_VERSION, MAGIC,
    RunCorruption, TABLE_ENTRY_LEN, read_u32, read_u64,
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
///
/// A run its builder made also carries a Bloom filter over its user keys,
/// kept in memory beside the bytes and not part of them, so that a lookup of
/// a key the run does not hold usually decodes nothing. A run made with
/// [`new`](Self::new) has none and reads as before.
#[derive(Clone, Debug)]
pub struct Run<B> {
    data: B,
    /// Boxed so that a run without one costs no more than a pointer.
    filter: Option<Box<KeyFilter>>,
    /// The largest sequence number of any record, 0 if there are none. Not
    /// stored: every record carries its sequence number, so the builder and
    /// validation derive it.
    max_seq: u64,
    /// The prepared form of the dictionary the run stores, if it stores one,
    /// shared by every reader. Built once, after the dictionary is validated.
    dictionary: Option<Arc<RunDictionary>>,
}

const VALIDATED: &str = "run bytes were validated or built by this crate";

impl<B: AsRef<[u8]>> Run<B> {
    /// Validates `data` in full and wraps it.
    ///
    /// This costs O(run length) and reports the first problem found. Temporary
    /// memory is one decoded block, at most
    /// [`MAX_BLOCK_LEN`](crate::block::MAX_BLOCK_LEN).
    #[cfg_attr(
        not(test),
        expect(dead_code, reason = "tested component API the engine does not use")
    )]
    pub fn new(data: B) -> Result<Self, CorruptRun> {
        let (max_seq, dictionary) = validate(data.as_ref())?;
        Ok(Self {
            data,
            filter: None,
            max_seq,
            dictionary: dictionary.map(Arc::new),
        })
    }

    pub(super) fn from_builder(
        data: B,
        filter: Option<KeyFilter>,
        max_seq: u64,
        dictionary: Option<Arc<RunDictionary>>,
    ) -> Self {
        Self {
            data,
            filter: filter.map(Box::new),
            max_seq,
            dictionary,
        }
    }

    /// Returns the largest sequence number of any record in the run, or 0
    /// if it has none. No record in the run is newer.
    pub fn max_seq(&self) -> u64 {
        self.max_seq
    }

    /// Returns the bytes of the run's key filter, held beside the stored
    /// bytes: about 1.25 per distinct user key, or 0 without a filter.
    pub fn filter_bytes(&self) -> usize {
        self.filter.as_deref().map_or(0, KeyFilter::bytes)
    }

    /// The bytes the builder held for key hashes while building the filter.
    pub(crate) fn filter_build_bytes(&self) -> usize {
        self.filter.as_deref().map_or(0, KeyFilter::build_bytes)
    }

    pub(crate) fn filter(&self) -> Option<&KeyFilter> {
        self.filter.as_deref()
    }

    /// Returns the bytes of the run's prepared dictionary, held beside the
    /// stored bytes (which include the dictionary itself), or 0 without one.
    /// The runs one compaction builds share a single prepared form, and each
    /// counts it in full: an overstatement, never an understatement.
    pub fn dictionary_bytes(&self) -> usize {
        self.dictionary.as_deref().map_or(0, RunDictionary::bytes)
    }

    /// The run's stored dictionary and its prepared form, for the tests'
    /// dictionary experiments.
    #[cfg(test)]
    pub(crate) fn dictionary_parts(&self) -> Option<(&[u8], &Arc<RunDictionary>)> {
        let stored = self.view().stored_dictionary?;
        Some((stored.bytes, self.dictionary.as_ref()?))
    }

    /// Returns whether the run stores a dictionary.
    #[cfg_attr(
        not(test),
        expect(dead_code, reason = "tested component API the engine does not use")
    )]
    pub fn has_dictionary(&self) -> bool {
        self.dictionary.is_some()
    }

    /// Returns everything the run holds beside its stored bytes: its key
    /// filter and its prepared dictionary. What memory limits charge besides
    /// [`as_bytes`](Self::as_bytes).
    pub fn held_bytes(&self) -> usize {
        self.filter_bytes() + self.dictionary_bytes()
    }

    /// Returns the number of stored records, counting every version.
    pub fn len(&self) -> usize {
        self.view().len()
    }

    /// Returns whether the run stores no records.
    #[cfg_attr(
        not(test),
        expect(dead_code, reason = "tested component API the engine does not use")
    )]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Returns the number of blocks.
    #[cfg_attr(
        not(test),
        expect(dead_code, reason = "tested component API the engine does not use")
    )]
    pub fn block_count(&self) -> usize {
        self.view().block_count()
    }

    /// Returns the smallest and largest user keys, or `None` for an empty run.
    /// Decodes nothing.
    pub fn key_range(&self) -> Result<Option<KeyRange<'_>>, CorruptRun> {
        self.view().key_range()
    }

    /// Returns the newest version of `user_key`, which may be a tombstone, or
    /// `None` if the run has no record for that key.
    ///
    /// Decodes at most one block, and none for a key outside the run's range
    /// or one its filter excludes. Once the workspace has grown to the largest
    /// block read, this performs no heap allocation.
    #[cfg_attr(
        not(test),
        expect(dead_code, reason = "tested component API the engine does not use")
    )]
    pub fn get<'r>(
        &'r self,
        user_key: &[u8],
        workspace: &'r mut ReadWorkspace,
    ) -> Result<Option<Version<'r>>, CorruptRun> {
        let view = self.view();
        if excluded(view, self.filter(), user_key, &mut None)? {
            return Ok(None);
        }
        view.get(user_key, workspace)
    }

    /// Returns a cursor positioned before the first record, which holds the
    /// workspace for as long as it lives. Use [`RunCursor::seek`] or
    /// [`RunCursor::seek_prefix`] to start elsewhere.
    #[cfg_attr(
        not(test),
        expect(dead_code, reason = "tested component API the engine does not use")
    )]
    pub fn cursor<'r>(&'r self, workspace: &'r mut ReadWorkspace) -> RunCursor<'r> {
        self.view().cursor(workspace)
    }

    /// Returns a view of this run's bytes.
    pub fn view(&self) -> RunView<'_> {
        RunView::open(self.data.as_ref())
            .expect(VALIDATED)
            .with_dictionary(self.dictionary.as_deref())
    }

    /// Returns the run's serialized bytes.
    pub fn as_bytes(&self) -> &[u8] {
        self.data.as_ref()
    }
}

impl Run<Vec<u8>> {
    /// Checks `data` as [`new`](Self::new) does, then walks its records once
    /// more to rebuild the key filter a builder would have made, returning
    /// the run and its largest sequence number (0 if empty). For runs read
    /// back from storage.
    pub(crate) fn recover(data: Vec<u8>) -> Result<(Self, u64), CorruptRun> {
        let (max_seq, dictionary) = validate(&data)?;
        let dictionary = dictionary.map(Arc::new);
        let mut hashes = KeyHashes::new();
        {
            let mut workspace = ReadWorkspace::new();
            let view = RunView::open(&data)?.with_dictionary(dictionary.as_deref());
            let mut cursor = view.cursor(&mut workspace);
            while let Some(record) = cursor.next_record()? {
                hashes.add(record.key);
            }
        }
        let run = Self::from_builder(data, hashes.finish(), max_seq, dictionary);
        Ok((run, max_seq))
    }
}

/// Whether `filter` proves `key` absent from the run `view`, checked only for
/// a key inside the run's range. `hash` caches the key's hash across runs.
pub(crate) fn excluded(
    view: RunView<'_>,
    filter: Option<&KeyFilter>,
    key: &[u8],
    hash: &mut Option<u64>,
) -> Result<bool, CorruptRun> {
    let Some(filter) = filter else {
        return Ok(false);
    };
    let Some(range) = view.key_range()? else {
        return Ok(false);
    };
    if key < range.min || key > range.max {
        return Ok(false);
    }
    Ok(!filter.may_contain(*hash.get_or_insert_with(|| key_filter::hash(key))))
}

/// A run's smallest and largest user keys, bytewise.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct KeyRange<'a> {
    /// The user key of the run's first record.
    pub min: &'a [u8],
    /// The user key of the run's last record.
    pub max: &'a [u8],
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

    /// Returns the bytes held by a decompression context, which Zstandard
    /// allocates outside the Rust heap. It is zero until a Zstandard frame has
    /// been read.
    pub fn context_bytes(&self) -> usize {
        self.decompressor.context_bytes()
    }

    /// Returns how many blocks reads through this workspace have loaded, each
    /// of which is a decompression for a compressed run.
    ///
    /// A lookup or a seek that needs no block does not count.
    pub fn blocks_loaded(&self) -> u64 {
        self.blocks_loaded
    }

    /// Returns a frame's block, decoding and verifying it if it is compressed.
    /// A frame compressed with a dictionary must name `dictionary`, its run's.
    pub(super) fn load<'r>(
        &'r mut self,
        frame: &Frame<'r>,
        dictionary: Option<&RunDictionary>,
    ) -> Result<&'r [u8], CorruptRun> {
        self.blocks_loaded += 1;
        if frame.codec == Codec::None {
            return Ok(frame.stored);
        }
        frame.verify_checksum()?;
        if frame.codec == Codec::ZstdDictionary {
            let dictionary = dictionary.ok_or(frame.error(RunCorruption::MissingDictionary))?;
            if zstd_safe::get_dict_id_from_frame(frame.stored) != Some(dictionary.id()) {
                return Err(frame.error(RunCorruption::DictionaryMismatch));
            }
        }

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
        let written = self.decompressor.decompress(
            frame.codec,
            frame.stored,
            &mut self.scratch[..len],
            dictionary,
        );
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
#[derive(Debug)]
pub struct RunCursor<'a> {
    view: RunView<'a>,
    workspace: &'a mut ReadWorkspace,
    next_block: usize,
    loaded: Option<LoadedBlock<'a>>,
    failure: Option<CorruptRun>,
    /// The next record, already decoded by a peek. Its key is in the position's
    /// buffer and its value ends at the position's offset, so holding it
    /// borrows and copies nothing.
    head: Option<Head>,
}

#[derive(Clone, Copy, Debug)]
struct Head {
    seq: u64,
    value_len: Option<usize>,
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
    #[cfg_attr(
        not(test),
        allow(
            dead_code,
            reason = "tested component API the engine does not use; Rust 1.89 does not see it as dead"
        )
    )]
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
    ///
    /// A run whose keys all sort below or above the prefix's interval decodes
    /// nothing, so damage in that run is not noticed by this scan.
    pub fn seek_prefix(&mut self, prefix: &[u8]) -> Result<(), CorruptRun> {
        self.workspace.prefix.clear();
        if prefix.len() > MAX_KEY_LEN || self.view.min_above_prefix(prefix) {
            // No key in this run can match. An oversized prefix is refused
            // here so the buffer stays bounded.
            self.failure = None;
            self.finish();
            return Ok(());
        }
        self.workspace.prefix.extend_from_slice(prefix);
        self.reposition(prefix)
    }

    fn reposition(&mut self, target: &[u8]) -> Result<(), CorruptRun> {
        self.failure = None;
        self.head = None;
        let result = self.position_at(target);
        if let Err(error) = result {
            self.failure = Some(error);
        }
        result
    }

    fn position_at(&mut self, target: &[u8]) -> Result<(), CorruptRun> {
        let first_block_held = self.loaded.is_some() && self.next_block == 1;
        if !self.view.table.is_empty() && target <= self.view.min_key && !first_block_held {
            // The whole run is at or above the target, so its first record is
            // the answer and no block has to be decoded to find that out.
            self.head = None;
            self.loaded = None;
            self.next_block = 0;
            self.workspace.position.reset();
            return Ok(());
        }
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
        self.head = None;
        self.loaded = None;
        self.next_block = self.view.table.len();
    }

    /// Advances to the next record, or returns `None` at the end of the run or
    /// of the prefix. After `None`, it keeps returning `None` until a seek.
    pub fn next_record(&mut self) -> Result<Option<Record<'_>>, CorruptRun> {
        self.fill_head()?;
        let head = self.head.take();
        Ok(head.map(|head| self.record_of(head)))
    }

    /// Returns the record [`next_record`](Self::next_record) would return,
    /// without advancing. Repeated peeks decode nothing further.
    #[cfg_attr(
        not(test),
        expect(dead_code, reason = "tested component API the engine does not use")
    )]
    pub fn peek(&mut self) -> Result<Option<Record<'_>>, CorruptRun> {
        self.fill_head()?;
        Ok(self.head.map(|head| self.record_of(head)))
    }

    /// Decodes the next record into the head, unless one is already held.
    pub(super) fn fill_head(&mut self) -> Result<(), CorruptRun> {
        if self.head.is_some() {
            return Ok(());
        }
        if let Some(error) = self.failure {
            return Err(error);
        }
        let Some(block) = self.block_with_records()? else {
            return Ok(());
        };
        let workspace = &mut *self.workspace;
        let bytes = loaded_bytes(&workspace.scratch, &block.frame);
        let view = BlockView::open(bytes).map_err(|error| block.frame.block_error(error))?;
        let Some(record) = workspace
            .position
            .next_record(view)
            .map_err(|error| block.frame.block_error(error))?
        else {
            return Ok(());
        };
        let head = Head {
            seq: record.seq,
            value_len: match record.value {
                Value::Bytes(bytes) => Some(bytes.len()),
                Value::Tombstone => None,
            },
        };
        if record.key.starts_with(&workspace.prefix) {
            self.head = Some(head);
        } else {
            self.finish();
        }
        Ok(())
    }

    /// Consumes the head and decodes the record after it.
    pub(super) fn advance_head(&mut self) -> Result<(), CorruptRun> {
        self.head = None;
        self.fill_head()
    }

    /// Returns the head's user key and sequence number, if a head is held.
    pub(super) fn head_key(&self) -> Option<(&[u8], u64)> {
        self.head
            .map(|head| (self.workspace.position.key(), head.seq))
    }

    /// Returns the run's first key if nothing has been decoded since the
    /// cursor stood before its first record, so that decoding can wait until
    /// the merge reaches that key.
    pub(super) fn deferred_key(&self) -> Option<&'a [u8]> {
        // Loading any block advances `next_block`, so zero means none has been.
        (self.next_block == 0).then_some(self.view.min_key)
    }

    /// Returns the held head, if any.
    pub(super) fn head_record(&self) -> Option<Record<'_>> {
        self.head.map(|head| self.record_of(head))
    }

    /// Rebuilds a head as a record. A head belongs to the loaded block.
    fn record_of(&self, head: Head) -> Record<'_> {
        let block = self
            .loaded
            .expect("a head is decoded from the loaded block");
        let workspace = &*self.workspace;
        let bytes = loaded_bytes(&workspace.scratch, &block.frame);
        let end = workspace.position.offset();
        let value = head
            .value_len
            .map_or(Value::Tombstone, |len| Value::Bytes(&bytes[end - len..end]));
        Record {
            key: workspace.position.key(),
            seq: head.seq,
            value,
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
        let bytes = self.workspace.load(&frame, self.view.dictionary)?;
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
///
/// A view of a run that stores a dictionary reads its dictionary frames only
/// once the prepared dictionary is attached, as [`Run::view`] does; until
/// then they fail with [`RunCorruption::MissingDictionary`].
#[derive(Clone, Copy, Debug)]
pub struct RunView<'a> {
    frames: &'a [u8],
    table: &'a [[u8; TABLE_ENTRY_LEN]],
    keys: &'a [u8],
    min_key: &'a [u8],
    record_count: usize,
    /// The dictionary region of a version 4 run, unchecked.
    stored_dictionary: Option<StoredDictionary<'a>>,
    /// Its prepared form, attached by the run that owns it.
    dictionary: Option<&'a RunDictionary>,
}

/// A version 4 run's dictionary as its bytes and footer declare it.
#[derive(Clone, Copy, Debug)]
struct StoredDictionary<'a> {
    offset: usize,
    bytes: &'a [u8],
    checksum: u32,
}

impl<'a> RunView<'a> {
    /// Checks the footer and the ends of the index and returns a view of
    /// `bytes`. The cost does not depend on the run's length.
    ///
    /// The last 8 bytes, the version and magic, decide the footer's length,
    /// and nothing before them is read until both are known.
    pub fn open(bytes: &'a [u8]) -> Result<Self, CorruptRun> {
        let Some((_, short_footer)) = bytes.split_last_chunk::<FOOTER_LEN>() else {
            return Err(corrupt(0, RunCorruption::Truncated));
        };
        let short_offset = bytes.len() - FOOTER_LEN;
        if read_u32(short_footer, 28) != MAGIC {
            return Err(corrupt(short_offset, RunCorruption::InvalidMagic));
        }
        // The fields both footers share, then version 4's dictionary fields.
        let (body, footer, dictionary): (_, &[u8; FOOTER_LEN], _) = match read_u32(short_footer, 24)
        {
            FORMAT_VERSION => (&bytes[..short_offset], short_footer, None),
            DICTIONARY_VERSION => {
                let Some((body, footer)) = bytes.split_last_chunk::<DICTIONARY_FOOTER_LEN>() else {
                    return Err(corrupt(0, RunCorruption::Truncated));
                };
                let (shared, _) = footer.split_first_chunk::<FOOTER_LEN>().expect("longer");
                let fields = (read_u32(footer, 24) as usize, read_u32(footer, 28));
                (body, shared, Some(fields))
            }
            _ => return Err(corrupt(short_offset, RunCorruption::UnsupportedVersion)),
        };
        let footer_offset = body.len();

        let invalid_footer = corrupt(footer_offset, RunCorruption::InvalidFooter);
        let record_count = usize::try_from(read_u64(footer, 8)).map_err(|_| invalid_footer)?;
        let (frames, index) = usize::try_from(read_u64(footer, 0))
            .ok()
            .and_then(|index_offset| body.split_at_checked(index_offset))
            .ok_or(invalid_footer)?;
        let (frames, stored_dictionary) = match dictionary {
            None => (frames, None),
            Some((len, checksum)) => {
                if len == 0 || len > MAX_DICTIONARY_LEN {
                    return Err(corrupt(footer_offset, RunCorruption::InvalidDictionary));
                }
                let offset = frames.len().checked_sub(len).ok_or(invalid_footer)?;
                let (frames, bytes) = frames.split_at(offset);
                let stored = StoredDictionary {
                    offset,
                    bytes,
                    checksum,
                };
                (frames, Some(stored))
            }
        };
        let (table, keys) = (read_u32(footer, 16) as usize)
            .checked_mul(TABLE_ENTRY_LEN)
            .and_then(|table_len| index.split_at_checked(table_len))
            .ok_or(invalid_footer)?;
        let table: &[[u8; TABLE_ENTRY_LEN]] = table.as_chunks().0;
        let min_key = Some(read_u32(footer, 20) as usize)
            .filter(|&len| len <= MAX_KEY_LEN)
            .and_then(|len| keys.get(..len))
            .ok_or(invalid_footer)?;

        let view = Self {
            frames,
            table,
            keys,
            min_key,
            record_count,
            stored_dictionary,
            dictionary: None,
        };
        match (table.first(), table.last()) {
            (Some(&first), Some(&last)) => {
                if frame_offset(first) != 0 {
                    return Err(view.invalid_index(0));
                }
                if key_end(last) != keys.len() {
                    return Err(view.invalid_index(table.len() - 1));
                }
                if view.last_key(0)? < min_key {
                    return Err(view.invalid_key_range());
                }
            }
            _ if frames.is_empty() && keys.is_empty() && record_count == 0 => {}
            _ => return Err(invalid_footer),
        }
        Ok(view)
    }

    /// Attaches the prepared form of this run's dictionary, which reads of its
    /// dictionary frames need.
    pub(super) fn with_dictionary(self, dictionary: Option<&'a RunDictionary>) -> Self {
        Self { dictionary, ..self }
    }

    pub(super) fn dictionary(self) -> Option<&'a RunDictionary> {
        self.dictionary
    }

    /// Returns the number of stored records, as declared by the footer.
    pub fn len(self) -> usize {
        self.record_count
    }

    /// Returns the number of blocks.
    pub fn block_count(self) -> usize {
        self.table.len()
    }

    /// Returns the decoded length of the largest block, which is the most a
    /// reader of this run must hold for one block. Reads each frame header and
    /// decodes nothing.
    pub fn largest_block_len(self) -> Result<usize, CorruptRun> {
        (0..self.table.len()).try_fold(0, |largest, block| {
            Ok(largest.max(self.frame(block)?.decoded_len))
        })
    }

    /// Returns the bytes used by the table and key area.
    #[cfg_attr(
        not(test),
        expect(dead_code, reason = "tested component API the engine does not use")
    )]
    pub fn index_len(self) -> usize {
        size_of_val(self.table) + self.keys.len()
    }

    /// Returns the smallest and largest user keys, or `None` for an empty run.
    /// Decodes nothing.
    pub fn key_range(self) -> Result<Option<KeyRange<'a>>, CorruptRun> {
        let Some(last) = self.table.len().checked_sub(1) else {
            return Ok(None);
        };
        Ok(Some(KeyRange {
            min: self.min_key,
            max: self.last_key(last)?,
        }))
    }

    /// Returns whether every key sorts above all keys with `prefix`.
    ///
    /// Keys with a prefix are contiguous. A key above the prefix that does not
    /// start with it is above that whole interval, which needs no successor
    /// of the prefix, so a prefix of `0xFF` bytes needs no special case.
    fn min_above_prefix(self, prefix: &[u8]) -> bool {
        !self.table.is_empty() && self.min_key > prefix && !self.min_key.starts_with(prefix)
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
    /// Decodes at most one block and never walks it, and none for a key
    /// outside the run's range, so damage in such a run is not noticed by this
    /// lookup. Once the workspace has grown to the largest block read, this
    /// performs no heap allocation.
    pub fn get<'r>(
        self,
        target: &[u8],
        workspace: &'r mut ReadWorkspace,
    ) -> Result<Option<Version<'r>>, CorruptRun>
    where
        'a: 'r,
    {
        if target < self.min_key {
            return Ok(None);
        }
        let Some(block) = self.locate(target)? else {
            return Ok(None);
        };
        let frame = self.frame(block)?;
        let bytes = workspace.load(&frame, self.dictionary)?;
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
            head: None,
        }
    }

    /// Returns a block's index key. `block` must be below the block count.
    fn last_key(self, block: usize) -> Result<&'a [u8], CorruptRun> {
        let start = match block.checked_sub(1) {
            Some(previous) => key_end(self.table[previous]),
            None => self.min_key.len(),
        };
        self.keys
            .get(start..key_end(self.table[block]))
            .ok_or(self.invalid_index(block))
    }

    /// Finds a frame and checks its header. `block` must be below the block
    /// count.
    pub(super) fn frame(self, block: usize) -> Result<Frame<'a>, CorruptRun> {
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

    /// The offset of the table, after the frames and any dictionary.
    fn table_offset(self) -> usize {
        self.frames.len()
            + self
                .stored_dictionary
                .map_or(0, |stored| stored.bytes.len())
    }

    fn invalid_key_range(self) -> CorruptRun {
        corrupt(
            self.table_offset() + size_of_val(self.table),
            RunCorruption::InvalidKeyRange,
        )
    }

    fn invalid_index(self, block: usize) -> CorruptRun {
        corrupt(
            self.table_offset() + block * TABLE_ENTRY_LEN,
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
///
/// A stored dictionary is checked first, against its checksum and then by
/// preparing it, within [`MAX_DICTIONARY_LEN`]; every frame is then decoded
/// with it. Returns the largest sequence number of the run's records and the
/// prepared dictionary.
fn validate(bytes: &[u8]) -> Result<(u64, Option<RunDictionary>), CorruptRun> {
    let view = RunView::open(bytes)?;
    let dictionary = match view.stored_dictionary {
        None => None,
        Some(stored) => {
            if crc32fast::hash(stored.bytes) != stored.checksum {
                return Err(corrupt(stored.offset, RunCorruption::ChecksumMismatch));
            }
            let prepared = RunDictionary::prepare(stored.bytes);
            Some(prepared.ok_or(corrupt(stored.offset, RunCorruption::InvalidDictionary))?)
        }
    };
    let view = view.with_dictionary(dictionary.as_ref());
    let mut workspace = ReadWorkspace::new();
    let mut walk = RecordWalk::default();
    for block in 0..view.table.len() {
        let frame = view.frame(block)?;
        // Decoding verifies compressed frames.
        if frame.codec == Codec::None {
            frame.verify_checksum()?;
        }
        let decoded = workspace.load(&frame, view.dictionary)?;

        let records_before = walk.record_count;
        walk.validate_block(decoded)
            .map_err(|error| frame.block_error(error))?;
        if walk.record_count == records_before {
            return Err(frame.error(RunCorruption::EmptyBlock));
        }
        if block == 0 {
            let opened = BlockView::open(decoded).map_err(|error| frame.block_error(error))?;
            let mut position = BlockPosition::new();
            let first = position
                .next_record(opened)
                .map_err(|error| frame.block_error(error))?;
            if first.map(|record| record.key) != Some(view.min_key) {
                return Err(view.invalid_key_range());
            }
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
    Ok((walk.max_seq, dictionary))
}
