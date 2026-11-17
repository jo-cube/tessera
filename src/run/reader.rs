//! Immutable run ownership, validation and sparse-index reads. Recovery
//! establishes format invariants once; checked reads decode only touched
//! frames and lend values through caller-owned reusable workspaces.

use std::fmt;
use std::ops::Range;

use crate::block::{
    BlockPosition, BlockView, MAX_KEY_LEN, RESTART_INTERVAL, Record, RecordWalk, Value, Version,
};

use super::codec::{Codec, Decompressor, plausible_decoded_len};
use super::dictionary::DICTIONARY_LEN;
#[cfg(test)]
use super::filter::KeyHashes;
use super::filter::{self as key_filter, KeyFilter};
use super::frame::{self, Frame};
use super::index::{IndexKey, head};
use super::storage::{Packing, Region, RunData, Segmented};
use super::{
    CorruptRun, FOOTER_LEN, FORMAT_VERSION, MAGIC, RunCorruption, SEGMENT_ENTRY_LEN,
    TABLE_ENTRY_LEN, read_u32, read_u64,
};
use crate::chunk::ChunkPool;

/// A valid run over any byte storage, such as `Vec<u8>`, `Arc<[u8]>` or
/// `&[u8]`. Built and recovered runs own pooled chunks plus an exact
/// index/footer buffer; the serialized format is the same.
///
/// Holding a `Run` is proof of validity: it comes from
/// [`RunBuilder::finish`](super::RunBuilder::finish) or from a full check by
/// [`recover_checkpoint_in`](Self::recover_checkpoint_in). Reads never repeat that check. They still return
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
/// a key the run does not hold usually decodes nothing. Runs without a filter
/// use their key bounds and sparse index directly.
#[derive(Clone, Debug)]
pub struct Run<B> {
    data: RunData<B>,
    /// One contiguous search head per index restart, rebuilt from stored keys.
    restart_heads: Box<[u64]>,
    /// Boxed so that a run without one costs no more than a pointer.
    filter: Option<Box<KeyFilter>>,
    /// The smallest and largest stored sequence numbers, both 0 if there
    /// are none. The builder and validation derive them; `seq_override`
    /// supplies the effective bounds.
    seqs: Seqs,
    /// Publication sequence of a unique-key ingestion built with placeholders.
    /// Set only while the run is private; materialized when it is rewritten.
    seq_override: Option<u64>,
    /// Where the smallest and largest user keys lie in `data`, or `None` for
    /// an empty run, found once so that a lookup can rule the run out
    /// without opening it.
    bounds: Option<KeyBounds>,
    reclaimable: bool,
    build_estimate: usize,
}

/// The smallest and largest sequence numbers of a run's records.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct Seqs {
    pub(crate) min: u64,
    pub(crate) max: u64,
}

#[derive(Clone, Debug)]
struct KeyBounds {
    min: Range<usize>,
    max: Range<usize>,
}

impl KeyBounds {
    /// The bounds of the valid run `data`.
    fn of(data: &RunData<impl AsRef<[u8]>>) -> Option<Self> {
        let range = data
            .view()
            .expect(VALIDATED)
            .key_range()
            .expect(VALIDATED)?;
        let within = |key| subslice(data.key_data(), key).expect("a run's keys lie in its bytes");
        Some(Self {
            min: within(range.min),
            max: within(range.max),
        })
    }
}

/// Where `inner` lies within `outer`, if it is a subslice of it, found from
/// their addresses: the offset that `<[T]>::subslice_range` (unstable)
/// computes. An empty `inner` at either end of `outer` lies within it.
fn subslice(outer: &[u8], inner: &[u8]) -> Option<Range<usize>> {
    let start = inner.as_ptr().addr().checked_sub(outer.as_ptr().addr())?;
    let end = start.checked_add(inner.len())?;
    (end <= outer.len()).then_some(start..end)
}

const VALIDATED: &str = "run bytes were validated or built by this crate";

impl<B: AsRef<[u8]>> Run<B> {
    /// Validates `data` in full and wraps it.
    ///
    /// This costs O(run length) and reports the first problem found. Temporary
    /// memory is one decoded block, at most
    /// [`MAX_BLOCK_LEN`](crate::block::MAX_BLOCK_LEN).
    #[cfg(test)]
    pub fn new(data: B) -> Result<Self, CorruptRun> {
        let (seqs, reclaimable, build_estimate) = validate(data.as_ref(), None)?;
        let data = RunData::Contiguous(data);
        let bounds = KeyBounds::of(&data);
        let restart_heads = Self::restart_heads(&data);
        Ok(Self {
            data,
            restart_heads,
            filter: None,
            seqs,
            seq_override: None,
            bounds,
            reclaimable,
            build_estimate,
        })
    }

    pub(super) fn from_builder(
        data: RunData<B>,
        filter: Option<KeyFilter>,
        seqs: Seqs,
        reclaimable: bool,
        build_estimate: usize,
    ) -> Self {
        let bounds = KeyBounds::of(&data);
        let restart_heads = Self::restart_heads(&data);
        Self {
            data,
            restart_heads,
            filter: filter.map(Box::new),
            seqs,
            seq_override: None,
            bounds,
            reclaimable,
            build_estimate,
        }
    }

    fn restart_heads(data: &RunData<B>) -> Box<[u64]> {
        let view = data.view().expect(VALIDATED);
        (0..view.block_count())
            .step_by(RESTART_INTERVAL)
            .map(|block| head(view.unshared_key(block).expect(VALIDATED)))
            .collect()
    }

    pub(crate) fn build_estimate(&self) -> usize {
        self.build_estimate
    }

    pub(crate) fn reclaimable(&self) -> bool {
        self.reclaimable
    }

    /// Returns the largest sequence number of any record in the run, or 0
    /// if it has none. No record in the run is newer.
    pub fn max_seq(&self) -> u64 {
        self.seq_override.unwrap_or(self.seqs.max)
    }

    /// Returns the smallest sequence number of any record in the run, or 0
    /// if it has none. No record in the run is older.
    pub(crate) fn min_seq(&self) -> u64 {
        self.seq_override.unwrap_or(self.seqs.min)
    }

    /// Assigns the effective sequence once, before a placeholder run is shared.
    /// A valid run whose stored sequences are all zero has unique user keys.
    pub(crate) fn set_seq_override(&mut self, seq: u64) {
        assert!(self.seq_override.is_none() && self.seqs.max == 0);
        self.seq_override = Some(seq);
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

    /// The run with one stored byte inverted and everything found at
    /// validation kept, as memory damaged after a run was published would
    /// read.
    #[cfg(test)]
    pub(crate) fn damaged_at(&self, offset: usize) -> Run<Vec<u8>> {
        let mut data: RunData<Vec<u8>> = match &self.data {
            RunData::Contiguous(bytes) => RunData::Contiguous(bytes.as_ref().to_vec()),
            RunData::Segmented(parts) => RunData::Segmented(parts.clone()),
        };
        match &mut data {
            RunData::Contiguous(bytes) => bytes[offset] ^= 0xFF,
            RunData::Segmented(parts) => {
                let mut remaining = offset;
                let mut changed = false;
                for piece in parts.frames.iter().chain(parts.dictionaries.iter()) {
                    let bytes =
                        &mut parts.allocations[piece.allocation].bytes_mut()[piece.range.clone()];
                    if remaining < bytes.len() {
                        bytes[remaining] ^= 0xFF;
                        changed = true;
                        break;
                    }
                    remaining -= bytes.len();
                }
                if !changed {
                    parts.tail[remaining] ^= 0xFF;
                }
            }
        }
        Run {
            data,
            restart_heads: self.restart_heads.clone(),
            filter: self.filter.clone(),
            seqs: self.seqs,
            seq_override: self.seq_override,
            bounds: self.bounds.clone(),
            reclaimable: self.reclaimable,
            build_estimate: self.build_estimate,
        }
    }

    /// Key filter, restart heads and physical segment metadata, charged beside stored bytes.
    pub fn held_bytes(&self) -> usize {
        self.filter_bytes() + size_of_val(&*self.restart_heads) + self.data.held_bytes()
    }

    /// Returns the number of stored records, counting every version.
    pub fn len(&self) -> usize {
        self.view().len()
    }

    /// Returns whether the run stores no records.
    #[cfg(test)]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Returns the number of blocks.
    #[cfg(test)]
    pub fn block_count(&self) -> usize {
        self.view().block_count()
    }

    /// Returns the smallest and largest user keys, or `None` for an empty run.
    /// They were found when the run was built or validated, so this reads
    /// only the two keys.
    pub fn key_range(&self) -> Option<KeyRange<'_>> {
        let bounds = self.bounds.as_ref()?;
        let data = self.data.key_data();
        Some(KeyRange {
            min: &data[bounds.min.clone()],
            max: &data[bounds.max.clone()],
        })
    }

    /// Whether the run provably holds no version of `key`: the key is outside
    /// its range, or its filter rules the key out. This opens nothing, and
    /// `hash` caches the key's hash across runs.
    pub(crate) fn excludes(&self, key: &[u8], hash: &mut Option<u64>) -> bool {
        let Some(range) = self.key_range() else {
            return true;
        };
        if key < range.min || key > range.max {
            return true;
        }
        self.filter().is_some_and(|filter| {
            !filter.may_contain(*hash.get_or_insert_with(|| key_filter::hash(key)))
        })
    }

    /// Returns the newest version of `user_key`, which may be a tombstone, or
    /// `None` if the run has no record for that key.
    ///
    /// Decodes at most one block, and none for a key outside the run's range
    /// or one its filter excludes. Once the workspace has grown to the largest
    /// block read, this performs no heap allocation.
    #[cfg(test)]
    pub fn get<'r>(
        &'r self,
        user_key: &[u8],
        workspace: &'r mut ReadWorkspace,
    ) -> Result<Option<Version<'r>>, CorruptRun> {
        if self.excludes(user_key, &mut None) {
            return Ok(None);
        }
        self.view().get(user_key, workspace)
    }

    /// Returns a cursor positioned before the first record, which holds the
    /// workspace for as long as it lives. Use [`RunCursor::seek`] or
    /// [`RunCursor::seek_prefix`] to start elsewhere.
    #[cfg(test)]
    pub fn cursor<'r>(&'r self, workspace: &'r mut ReadWorkspace) -> RunCursor<'r> {
        self.view().cursor(workspace)
    }

    /// Returns a view with this run's effective sequences.
    pub fn view(&self) -> RunView<'_> {
        RunView {
            seq_override: self.seq_override,
            restart_heads: Some(&self.restart_heads),
            ..self.data.view().expect(VALIDATED)
        }
    }

    pub(crate) fn chunk_capacity(&self) -> usize {
        self.data.chunk_capacity()
    }
    pub(crate) fn frame_bytes(&self) -> usize {
        self.view().frame_bytes()
    }
    pub(crate) fn index_bytes(&self) -> usize {
        let view = self.view();
        view.index_len() + size_of_val(&*self.restart_heads)
    }
    /// Serialized byte count, excluding filters and physical segment metadata.
    pub fn stored_len(&self) -> usize {
        self.data.stored_len()
    }

    /// Regions in checkpoint file order, without materializing a whole run.
    pub(crate) fn chunks(&self) -> impl Iterator<Item = &[u8]> {
        self.data.chunks()
    }

    pub(crate) fn seq_override(&self) -> Option<u64> {
        self.seq_override
    }

    pub(crate) fn filter_words(&self) -> impl Iterator<Item = [u8; 8]> + '_ {
        self.filter.iter().flat_map(|filter| filter.encoded())
    }

    /// Contiguous serialization only for tests of the on-disk format.
    #[cfg(test)]
    pub fn as_bytes(&self) -> std::borrow::Cow<'_, [u8]> {
        match &self.data {
            RunData::Contiguous(bytes) => std::borrow::Cow::Borrowed(bytes.as_ref()),
            RunData::Segmented(_) => {
                std::borrow::Cow::Owned(self.chunks().flatten().copied().collect())
            }
        }
    }
}

impl Run<Vec<u8>> {
    /// Checks `data` as [`new`](Self::new) does, then walks its records once
    /// more to rebuild the key filter a builder would have made, returning
    /// the run and its largest sequence number (0 if empty). For runs read
    /// back from storage.
    #[cfg(test)]
    pub(crate) fn recover(data: Vec<u8>) -> Result<(Self, u64), CorruptRun> {
        Self::recover_in(data, &ChunkPool::new(0))
    }

    #[cfg(test)]
    pub(crate) fn recover_in(data: Vec<u8>, pool: &ChunkPool) -> Result<(Self, u64), CorruptRun> {
        let (seqs, reclaimable, build_estimate) = validate(&data, None)?;
        let mut hashes = KeyHashes::new();
        {
            let mut workspace = ReadWorkspace::new();
            let view = RunView::open(&data)?;
            let mut cursor = view.cursor(&mut workspace);
            while let Some(record) = cursor.next_record()? {
                hashes.add(record.key);
            }
        }
        Self::pack_recovered(
            data,
            pool,
            hashes.finish(),
            seqs,
            reclaimable,
            build_estimate,
        )
    }

    pub(crate) fn recover_checkpoint_in(
        data: Vec<u8>,
        filter: &[u8],
        pool: &ChunkPool,
    ) -> Result<(Self, u64), CorruptRun> {
        let filter = if filter.is_empty() {
            None
        } else {
            Some(
                KeyFilter::decode(filter)
                    .ok_or(corrupt(data.len(), RunCorruption::InvalidFilter))?,
            )
        };
        let (seqs, reclaimable, build_estimate) = validate(&data, filter.as_ref())?;
        Self::pack_recovered(data, pool, filter, seqs, reclaimable, build_estimate)
    }

    fn pack_recovered(
        data: Vec<u8>,
        pool: &ChunkPool,
        filter: Option<KeyFilter>,
        seqs: Seqs,
        reclaimable: bool,
        build_estimate: usize,
    ) -> Result<(Self, u64), CorruptRun> {
        let view = RunView::open(&data)?;
        let mut packing = Packing::new(pool);
        for block in 0..view.block_count() {
            let start = frame_offset(view.table[block]);
            let end = view
                .table
                .get(block + 1)
                .map_or(view.frames.len(), |&e| frame_offset(e));
            packing.frame(&data[start..end]);
        }
        for index in 0..view.segments.len() {
            packing.dictionary(view.dictionary((index + 1) as u32)?);
        }
        let tail_start = view.frames.len() + view.dictionaries.len();
        let parts = packing.finish(data[tail_start..].into());
        let run = Self::from_builder(
            RunData::Segmented(parts),
            filter,
            seqs,
            reclaimable,
            build_estimate,
        );
        Ok((run, seqs.max))
    }
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
    decoded_bytes: u64,
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

    /// Raw bytes in blocks loaded through this workspace.
    pub fn decoded_bytes(&self) -> u64 {
        self.decoded_bytes
    }

    /// Returns a frame's block, decoding and verifying it if it is compressed.
    /// The frame borrows its raw segment dictionary from the stored run.
    pub(super) fn load<'r>(&'r mut self, frame: &Frame<'r>) -> Result<&'r [u8], CorruptRun> {
        self.blocks_loaded += 1;
        self.decoded_bytes += frame.decoded_len as u64;
        decode(frame, &mut self.scratch, &mut self.decompressor)
    }
}

/// [`ReadWorkspace::load`], with the buffer and the decompression context
/// given separately so that a [`LookupWorkspace`] can share one context
/// between its buffers.
fn decode<'r>(
    frame: &Frame<'r>,
    scratch: &'r mut Vec<u8>,
    decompressor: &mut Decompressor,
) -> Result<&'r [u8], CorruptRun> {
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
    if scratch.len() < len {
        scratch.reserve_exact(len - scratch.len());
        scratch.resize(len, 0);
    }
    let written = decompressor.decompress(
        frame.codec,
        frame.stored,
        &mut scratch[..len],
        frame.dictionary,
    );
    if written != Some(len) {
        return Err(frame.error(RunCorruption::CorruptPayload));
    }
    Ok(&scratch[..len])
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

/// Reusable storage for point lookups across any number of runs, created
/// once per reader: two decoded-block buffers and one decompression context.
///
/// A lookup keeps the newest version found so far in one buffer and decodes
/// the next run it reads into the other, so two suffice however many runs
/// there are. The context serves both buffers and every codec and
/// dictionary, since each call names its dictionary.
#[derive(Default)]
pub(crate) struct LookupWorkspace {
    buffers: [Vec<u8>; 2],
    decompressor: Decompressor,
    blocks_loaded: u64,
    decoded_bytes: u64,
}

/// A version [`LookupWorkspace::get`] found, located rather than borrowed so
/// that the workspace can decode into its other buffer while this is held.
#[derive(Clone, Copy, Debug)]
pub(crate) struct Found<'a> {
    /// The buffer the block was read into, whether or not it was decoded.
    pub(crate) buffer: usize,
    value: Located<'a>,
}

#[derive(Clone, Copy, Debug)]
enum Located<'a> {
    Tombstone,
    /// In a stored block, so in the run itself.
    Stored(&'a [u8]),
    /// At these offsets in the buffer.
    Decoded(usize, usize),
}

impl LookupWorkspace {
    /// Returns the bytes retained for decoded blocks.
    pub(crate) fn capacity(&self) -> usize {
        self.buffers.iter().map(Vec::capacity).sum()
    }

    /// Returns the bytes held by the decompression context, outside the Rust
    /// heap.
    pub(crate) fn context_bytes(&self) -> usize {
        self.decompressor.context_bytes()
    }

    /// Returns how many blocks lookups through this workspace have loaded.
    pub(crate) fn blocks_loaded(&self) -> u64 {
        self.blocks_loaded
    }

    pub(crate) fn decoded_bytes(&self) -> u64 {
        self.decoded_bytes
    }

    /// Returns the sequence number and location of the newest version of
    /// `target` in `view`, decoding into `buffer`
    /// (0 or 1) and leaving the other buffer as it was.
    pub(crate) fn get<'a>(
        &mut self,
        view: RunView<'a>,
        target: &[u8],
        buffer: usize,
    ) -> Result<Option<(u64, Found<'a>)>, CorruptRun> {
        let scratch = &mut self.buffers[buffer];
        let found = view.find(
            target,
            scratch,
            &mut self.decompressor,
            &mut self.blocks_loaded,
            &mut self.decoded_bytes,
        )?;
        let Some((block, version)) = found else {
            return Ok(None);
        };
        let value = match version.value {
            Value::Tombstone => Located::Tombstone,
            Value::Bytes(value) => match view.frames.stored_subslice(value) {
                Some(value) => Located::Stored(value),
                None => {
                    let range = subslice(block, value).expect("a value lies in its block");
                    Located::Decoded(range.start, range.end)
                }
            },
        };
        Ok(Some((version.seq, Found { buffer, value })))
    }

    /// Returns the value of a version [`get`](Self::get) found. Its buffer
    /// must not have been decoded into since.
    pub(crate) fn value<'s, 'a: 's>(&'s self, found: Found<'a>) -> Value<'s> {
        match found.value {
            Located::Tombstone => Value::Tombstone,
            Located::Stored(value) => Value::Bytes(value),
            Located::Decoded(start, end) => Value::Bytes(&self.buffers[found.buffer][start..end]),
        }
    }
}

impl fmt::Debug for LookupWorkspace {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LookupWorkspace")
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
    /// fails, subsequent reads report the same error until a
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

    /// Moves to before the first record of `view`, reusing the workspace and
    /// keeping the prefix bound, for a group cursor that has finished one run
    /// and continues into the next in key order. Decodes nothing.
    pub(super) fn continue_into(&mut self, view: RunView<'a>) {
        self.view = view;
        self.next_block = 0;
        self.loaded = None;
        self.failure = None;
        self.head = None;
        self.workspace.position.reset();
    }

    /// Returns the prefix the cursor iterates within, empty for none.
    pub(super) fn prefix(&self) -> &[u8] {
        &self.workspace.prefix
    }

    /// Moves past the last record of the run.
    fn finish(&mut self) {
        self.head = None;
        self.loaded = None;
        self.next_block = self.view.table.len();
    }

    /// Advances to the next record, or returns `None` at the end of the run or
    /// of the prefix. After `None`, it keeps returning `None` until a seek.
    #[cfg(test)]
    pub fn next_record(&mut self) -> Result<Option<Record<'_>>, CorruptRun> {
        self.fill_head()?;
        let head = self.head.take();
        Ok(head.map(|head| self.record_of(head)))
    }

    /// Returns the record [`next_record`](Self::next_record) would return,
    /// without advancing. Repeated peeks decode nothing further.
    #[cfg(test)]
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
            seq: self.view.seq_override.unwrap_or(record.seq),
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
/// [`RunBuilder`](super::RunBuilder), or accepted once by recovery, and
/// unchanged since. A checksum can show bytes are unchanged. It cannot show
/// that whoever wrote them produced a valid run.
///
/// Raw segment dictionaries are borrowed directly from the run on each decode.
#[derive(Clone, Copy, Debug)]
pub struct RunView<'a> {
    frames: Region<'a>,
    dictionaries: Region<'a>,
    segments: &'a [[u8; SEGMENT_ENTRY_LEN]],
    table: &'a [[u8; TABLE_ENTRY_LEN]],
    keys: &'a [u8],
    min_key: &'a [u8],
    record_count: usize,
    seq_override: Option<u64>,
    // Raw borrowed views used during validation allocate nothing. Owned runs
    // attach their once-built heads to every view used for reads.
    restart_heads: Option<&'a [u64]>,
}

impl<'a> RunView<'a> {
    /// Checks the footer and the index ends without allocating or decoding.
    pub fn open(bytes: &'a [u8]) -> Result<Self, CorruptRun> {
        Self::open_parts(bytes, None)
    }

    pub(super) fn open_segmented(data: &'a Segmented) -> Result<Self, CorruptRun> {
        Self::open_parts(&data.tail, Some(data))
    }

    fn open_parts(bytes: &'a [u8], physical: Option<&'a Segmented>) -> Result<Self, CorruptRun> {
        let Some((body, footer)) = bytes.split_last_chunk::<FOOTER_LEN>() else {
            return Err(corrupt(0, RunCorruption::Truncated));
        };
        let offset = body.len() + physical.map_or(0, |p| p.frames_len + p.dictionaries_len);
        if read_u32(footer, 44) != MAGIC {
            return Err(corrupt(offset, RunCorruption::InvalidMagic));
        }
        if read_u32(footer, 40) != FORMAT_VERSION {
            return Err(corrupt(offset, RunCorruption::UnsupportedVersion));
        }
        let invalid = corrupt(offset, RunCorruption::InvalidFooter);
        if offset.saturating_add(FOOTER_LEN) > u32::MAX as usize {
            return Err(invalid);
        }
        let record_count = usize::try_from(read_u64(footer, 8)).map_err(|_| invalid)?;
        let index_offset = usize::try_from(read_u64(footer, 0)).map_err(|_| invalid)?;
        let dictionary_offset = usize::try_from(read_u64(footer, 24)).map_err(|_| invalid)?;
        let segment_len = (read_u32(footer, 32) as usize)
            .checked_mul(SEGMENT_ENTRY_LEN)
            .ok_or(invalid)?;
        let segment_offset = index_offset.checked_sub(segment_len).ok_or(invalid)?;
        let (frames, dictionaries, segments, index) = if let Some(data) = physical {
            if dictionary_offset != data.frames_len
                || segment_offset != data.frames_len + data.dictionaries_len
            {
                return Err(invalid);
            }
            let (segments, index) = body.split_at_checked(segment_len).ok_or(invalid)?;
            (
                Region::Frames(data),
                Region::Dictionaries(data),
                segments,
                index,
            )
        } else {
            let (before, index) = body.split_at_checked(index_offset).ok_or(invalid)?;
            let (contents, segments) = before.split_at_checked(segment_offset).ok_or(invalid)?;
            let (frames, dictionaries) = contents
                .split_at_checked(dictionary_offset)
                .ok_or(invalid)?;
            (
                Region::Contiguous(frames),
                Region::Contiguous(dictionaries),
                segments,
                index,
            )
        };
        let table_len = (read_u32(footer, 16) as usize)
            .checked_mul(TABLE_ENTRY_LEN)
            .ok_or(invalid)?;
        let (table, keys) = index.split_at_checked(table_len).ok_or(invalid)?;
        let min_key = Some(read_u32(footer, 20) as usize)
            .filter(|&n| n <= MAX_KEY_LEN)
            .and_then(|n| keys.get(..n))
            .ok_or(invalid)?;
        let view = Self {
            frames,
            dictionaries,
            segments: segments.as_chunks().0,
            table: table.as_chunks().0,
            keys,
            min_key,
            record_count,
            seq_override: None,
            restart_heads: None,
        };
        match (view.table.first(), view.table.last()) {
            (Some(&first), Some(&last)) => {
                if frame_offset(first) != 0 {
                    return Err(view.invalid_index(0));
                }
                if key_end(last) != keys.len() {
                    return Err(view.invalid_index(view.table.len() - 1));
                }
                if view.last_key(0)?.cmp(min_key).is_lt() {
                    return Err(view.invalid_key_range());
                }
            }
            _ if frames.is_empty()
                && dictionaries.is_empty()
                && segments.is_empty()
                && keys.is_empty()
                && record_count == 0 => {}
            _ => return Err(invalid),
        }
        Ok(view)
    }

    fn dictionary(self, reference: u32) -> Result<&'a [u8], CorruptRun> {
        let invalid = corrupt(self.frames.len(), RunCorruption::InvalidDictionary);
        let entry = reference
            .checked_sub(1)
            .and_then(|i| self.segments.get(i as usize))
            .ok_or(corrupt(self.frames.len(), RunCorruption::MissingDictionary))?;
        let start = usize::try_from(read_u64(entry, 0))
            .ok()
            .and_then(|n| n.checked_sub(self.frames.len()))
            .ok_or(invalid)?;
        let len = read_u32(entry, 8) as usize;
        if len == 0 || len > DICTIONARY_LEN {
            return Err(invalid);
        }
        let bytes = start
            .checked_add(len)
            .and_then(|end| self.dictionaries.get(start..end))
            .ok_or(invalid)?;
        if bytes.starts_with(&[0x37, 0xa4, 0x30, 0xec]) {
            return Err(invalid);
        }
        if crc32fast::hash(bytes) != read_u32(entry, 12) {
            return Err(corrupt(
                self.frames.len() + start,
                RunCorruption::ChecksumMismatch,
            ));
        }
        Ok(bytes)
    }

    /// Returns the number of stored records, as declared by the footer.
    pub fn len(self) -> usize {
        self.record_count
    }

    /// Compressed frame bytes, excluding dictionaries and the index.
    pub(super) fn frame_bytes(self) -> usize {
        self.frames.len()
    }

    pub(super) fn block_bytes(self, block: usize) -> usize {
        let end = self
            .table
            .get(block + 1)
            .map_or(self.frames.len(), |entry| frame_offset(*entry));
        end.saturating_sub(frame_offset(self.table[block]))
    }

    /// Returns the number of blocks.
    pub fn block_count(self) -> usize {
        self.table.len()
    }

    /// Returns the decoded length of the largest block, which is the most a
    /// reader of this run must hold for one block. Checks frame metadata and
    /// referenced dictionaries without decoding.
    pub fn largest_block_len(self) -> Result<usize, CorruptRun> {
        (0..self.table.len()).try_fold(0, |largest, block| {
            Ok(largest.max(self.frame(block)?.decoded_len))
        })
    }

    /// Returns the bytes used by the table and key area.
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
            max: self.unshared_key(last)?,
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
        // Find the first restart key >= target. Its preceding interval may
        // already hold the key, including versions spanning restart points.
        let restarts = self.table.len().div_ceil(RESTART_INTERVAL);
        let target_head = head(target);
        let (mut low, mut high) = (0, restarts);
        while low < high {
            let middle = low + (high - low) / 2;
            let before = match self.restart_heads {
                Some(heads) if heads[middle] != target_head => heads[middle] < target_head,
                _ => self.unshared_key(middle * RESTART_INTERVAL)? < target,
            };
            if before {
                low = middle + 1;
            } else {
                high = middle;
            }
        }
        let start = low.saturating_sub(1) * RESTART_INTERVAL;
        let end = (low * RESTART_INTERVAL).min(self.table.len());
        let mut key = IndexKey::new();
        for block in start..end {
            self.append_index_key(block, &mut key)?;
            if !key.cmp(target).is_lt() {
                return Ok(Some(block));
            }
        }
        Ok((low < restarts).then_some(low * RESTART_INTERVAL))
    }

    /// Returns the newest version of `target`, which may be a tombstone, or
    /// `None` if the run has no record for that key.
    ///
    /// Decodes at most one block and never walks it, and none for a key
    /// outside the run's range, so damage in such a run is not noticed by this
    /// lookup. Once the workspace has grown to the largest block read, this
    /// performs no heap allocation.
    #[cfg(test)]
    pub fn get<'r>(
        self,
        target: &[u8],
        workspace: &'r mut ReadWorkspace,
    ) -> Result<Option<Version<'r>>, CorruptRun>
    where
        'a: 'r,
    {
        let ReadWorkspace {
            scratch,
            decompressor,
            blocks_loaded,
            decoded_bytes,
            ..
        } = workspace;
        let found = self.find(target, scratch, decompressor, blocks_loaded, decoded_bytes)?;
        Ok(found.map(|(_, version)| version))
    }

    /// Probes sorted indices, loading each candidate block once. Callbacks
    /// borrow a block only until the next callback or decode.
    pub(crate) fn get_many<K: AsRef<[u8]>>(
        self,
        keys: &[K],
        indices: &[usize],
        workspace: &mut ReadWorkspace,
        mut found: impl FnMut(usize, Option<Version<'_>>),
    ) -> Result<(), CorruptRun> {
        let mut next = 0;
        while next < indices.len() {
            let key = keys[indices[next]].as_ref();
            if key < self.min_key {
                found(indices[next], None);
                next += 1;
                continue;
            }
            let Some(block) = self.locate(key)? else {
                break;
            };
            let last = self.last_key(block)?;
            let start = next;
            while next < indices.len() && !last.cmp(keys[indices[next]].as_ref()).is_lt() {
                next += 1;
            }
            let frame = self.frame(block)?;
            let bytes = workspace.load(&frame)?;
            let view = BlockView::open(bytes).map_err(|error| frame.block_error(error))?;
            for &index in &indices[start..next] {
                let version = view
                    .get(keys[index].as_ref())
                    .map_err(|error| frame.block_error(error))?;
                found(index, version.map(|version| self.effective(version)));
            }
        }
        Ok(())
    }

    /// Point lookup, with the buffer, context and block count given
    /// separately. Also returns the block the version is in.
    fn find<'r>(
        self,
        target: &[u8],
        scratch: &'r mut Vec<u8>,
        decompressor: &mut Decompressor,
        blocks_loaded: &mut u64,
        decoded_bytes: &mut u64,
    ) -> Result<Option<(&'r [u8], Version<'r>)>, CorruptRun>
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
        *blocks_loaded += 1;
        *decoded_bytes += frame.decoded_len as u64;
        let bytes = decode(&frame, scratch, decompressor)?;
        let version = BlockView::open(bytes)
            .and_then(|view| view.get(target))
            .map_err(|error| frame.block_error(error))?;
        Ok(version.map(|version| (bytes, self.effective(version))))
    }

    fn effective(self, version: Version<'_>) -> Version<'_> {
        Version {
            seq: self.seq_override.unwrap_or(version.seq),
            ..version
        }
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
    pub(super) fn last_key(self, block: usize) -> Result<IndexKey<'a>, CorruptRun> {
        let mut key = IndexKey::new();
        for at in block / RESTART_INTERVAL * RESTART_INTERVAL..=block {
            self.append_index_key(at, &mut key)?;
        }
        Ok(key)
    }

    fn index_entry(self, block: usize) -> Result<(usize, &'a [u8]), CorruptRun> {
        let entry = self.table.get(block).ok_or(self.invalid_index(block))?;
        let start = block
            .checked_sub(1)
            .map_or(self.min_key.len(), |previous| key_end(self.table[previous]));
        let bytes = self
            .keys
            .get(start..key_end(*entry))
            .ok_or(self.invalid_index(block))?;
        let (shared, suffix) = bytes
            .split_first_chunk::<2>()
            .ok_or(self.invalid_index(block))?;
        let shared = u16::from_le_bytes(*shared) as usize;
        if ((block.is_multiple_of(RESTART_INTERVAL) || block + 1 == self.table.len())
            && shared != 0)
            || suffix.len() > MAX_KEY_LEN
        {
            return Err(self.invalid_index(block));
        }
        Ok((shared, suffix))
    }
    fn unshared_key(self, block: usize) -> Result<&'a [u8], CorruptRun> {
        let (shared, suffix) = self.index_entry(block)?;
        if shared != 0 {
            return Err(self.invalid_index(block));
        }
        Ok(suffix)
    }
    fn append_index_key(self, block: usize, key: &mut IndexKey<'a>) -> Result<(), CorruptRun> {
        let (shared, suffix) = self.index_entry(block)?;
        key.append(shared, suffix).ok_or(self.invalid_index(block))
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
        let mut frame = Frame::parse(start, header, stored)?;
        if frame.codec == Codec::Zstd {
            frame.dictionary = self.dictionary(frame.dictionary_ref)?;
        }
        Ok(frame)
    }

    fn table_offset(self) -> usize {
        self.frames.len() + self.dictionaries.len() + size_of_val(self.segments)
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
    read_u32(&entry, 0) as usize
}

fn key_end(entry: [u8; TABLE_ENTRY_LEN]) -> usize {
    read_u32(&entry, 4) as usize
}

/// Checks every invariant in the module documentation.
///
/// Frames tile the frame region because the first starts at 0, each ends where
/// the next starts and each must hold its header. The key area is tiled the
/// same way, and matching every index key against its block completes the
/// index checks.
///
/// Checks all raw dictionaries before decoding any frame. No prepared state.
fn validate(bytes: &[u8], filter: Option<&KeyFilter>) -> Result<(Seqs, bool, usize), CorruptRun> {
    let view = RunView::open(bytes)?;
    let footer_at = bytes.len() - FOOTER_LEN;
    let expected = u32::from_le_bytes(bytes[footer_at + 36..footer_at + 40].try_into().unwrap());
    if crc32fast::hash(&bytes[view.table_offset()..footer_at]) != expected {
        return Err(corrupt(
            view.table_offset(),
            RunCorruption::ChecksumMismatch,
        ));
    }
    let mut next = view.frames.len();
    for (i, entry) in view.segments.iter().enumerate() {
        if read_u64(entry, 0) != next as u64 {
            return Err(corrupt(next, RunCorruption::InvalidDictionary));
        }
        next += view.dictionary((i + 1) as u32)?.len();
    }
    if next != view.frames.len() + view.dictionaries.len() {
        return Err(corrupt(next, RunCorruption::InvalidDictionary));
    }
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
        if block == 0 || filter.is_some() {
            let opened = BlockView::open(decoded).map_err(|error| frame.block_error(error))?;
            let mut position = BlockPosition::new();
            let first = position
                .next_record(opened)
                .map_err(|error| frame.block_error(error))?;
            if block == 0 && first.map(|record| record.key) != Some(view.min_key) {
                return Err(view.invalid_key_range());
            }
            // A checksummed filter must also agree with the records: a false
            // negative would make point reads disagree with scans. Reuse the
            // decoded block rather than decompressing it a second time.
            if let Some(filter) = filter {
                let mut record = first;
                while let Some(current) = record {
                    if !filter.may_contain(super::filter::hash(current.key)) {
                        return Err(corrupt(bytes.len(), RunCorruption::InvalidFilter));
                    }
                    record = position
                        .next_record(opened)
                        .map_err(|error| frame.block_error(error))?;
                }
            }
        }
        if !view.last_key(block)?.cmp(&walk.key).is_eq() {
            return Err(view.invalid_index(block));
        }
    }
    if walk.record_count != view.record_count {
        return Err(corrupt(
            bytes.len() - FOOTER_LEN,
            RunCorruption::RecordCountMismatch,
        ));
    }
    let seqs = Seqs {
        min: walk.min_seq,
        max: walk.max_seq,
    };
    Ok((seqs, walk.reclaimable, walk.build_estimate))
}

#[cfg(test)]
#[path = "tests/storage.rs"]
mod storage_tests;

/// Stored bytes of one run by component, and its raw block bytes.
#[cfg(feature = "diagnostics")]
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct RunMemory {
    pub records: usize,
    pub raw: usize,
    pub frames: usize,
    pub dictionaries: usize,
    pub segment_table: usize,
    pub index: usize,
    pub filters: usize,
    pub footer: usize,
    pub physical: usize,
}

#[cfg(feature = "diagnostics")]
impl<B: AsRef<[u8]>> Run<B> {
    /// Raw bytes of every block, from frame headers.
    pub(crate) fn raw_len(&self) -> usize {
        let view = self.view();
        (0..view.block_count())
            .map(|block| view.frame(block).expect(VALIDATED).decoded_len)
            .sum()
    }
    pub(crate) fn memory(&self) -> RunMemory {
        let view = self.view();
        RunMemory {
            records: view.len(),
            raw: self.raw_len(),
            frames: view.frame_bytes(),
            dictionaries: view.dictionaries.len(),
            segment_table: size_of_val(view.segments),
            index: self.index_bytes(),
            filters: self.filter_bytes(),
            footer: FOOTER_LEN,
            physical: self.data.held_bytes(),
        }
    }
}
