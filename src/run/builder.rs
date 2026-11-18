//! Sorted-run encoding with bounded segment buffers and adaptive block cuts.
//! Records arrive in internal-key order and are cut into raw blocks as they
//! are added; each segment's blocks are compressed with its dictionary once
//! the segment is complete. Frames and dictionaries are packed atomically so
//! reads can borrow contiguous bytes from shared chunks.

use super::OutputBudget;
use super::codec::{Codec, Compressor};
use super::dictionary::{self, SAMPLE_LEN, SEGMENT_RAW_LEN, Segment};
use super::filter::KeyHashes;
use super::storage::{Packing, RunData};
use super::{
    FOOTER_LEN, FORMAT_VERSION, MAGIC, RAW_BLOCK_CAP, SEGMENT_ENTRY_LEN, TABLE_ENTRY_LEN,
    TARGET_BLOCK_COMPRESSED, TARGET_BLOCK_LEN, frame,
    reader::{BlockSizes, Run, Seqs},
};
use crate::block::{
    BlockBuilder, BlockPosition, BlockView, BuildError, MAX_KEY_LEN, RESTART_INTERVAL, Value,
    cmp_internal,
};
use crate::chunk::ChunkPool;
use std::fmt;
use std::ops::Range;
use std::sync::Arc;

/// Encodes sorted records, buffering at most one segment of raw blocks before
/// compressing. Rejected records leave the builder unchanged. A single
/// oversized record is stored in its own block and, if necessary, its own
/// segment.
///
/// Blocks are cut at a raw length estimated to compress to the target, from
/// the ratio of each segment's first 64 KiB, encoded as one block and
/// compressed at level 1 without a dictionary. The records of that sample are
/// then cut at the estimate; later records are cut as they are added.
pub struct RunBuilder {
    target: usize,
    segment_len: usize,
    raw_target: Option<usize>,
    /// Raw block length for the open segment, or `None` until its probe.
    block_target: Option<usize>,
    compressor: Compressor,
    block: BlockBuilder,
    last_key: Vec<u8>,
    last_seq: u64,
    /// Key bytes of the records in the open block.
    pending_key_bytes: usize,
    /// The open segment's finished raw blocks, concatenated.
    pending: Vec<u8>,
    /// Each pending block's end and the segment's record count through it.
    pending_blocks: Vec<(u32, u32)>,
    frames: Vec<u8>,
    storage: Packing,
    scratch: Vec<u8>,
    segments: Vec<u8>,
    table: Vec<u8>,
    keys: Vec<u8>,
    index_last_key: Vec<u8>,
    min_key_len: usize,
    record_count: usize,
    reclaimable: bool,
    build_estimate: usize,
    largest_block: usize,
    min_seq: u64,
    max_seq: u64,
    key_hashes: KeyHashes,
    output_budget: Option<Arc<OutputBudget>>,
    output_refused: bool,
}
impl Default for RunBuilder {
    fn default() -> Self {
        Self::new(Codec::default())
    }
}
impl RunBuilder {
    pub fn new(codec: Codec) -> Self {
        Self::with_target_block_compressed(codec, TARGET_BLOCK_COMPRESSED)
    }
    pub fn with_target_block_compressed(codec: Codec, target: usize) -> Self {
        Self {
            target: target.clamp(1, RAW_BLOCK_CAP),
            segment_len: SEGMENT_RAW_LEN,
            raw_target: None,
            block_target: (codec == Codec::None).then_some(TARGET_BLOCK_LEN),
            compressor: Compressor::new(codec),
            block: BlockBuilder::new(),
            last_key: Vec::new(),
            last_seq: 0,
            pending_key_bytes: 0,
            pending: Vec::new(),
            pending_blocks: Vec::new(),
            frames: Vec::new(),
            storage: Packing::new(&ChunkPool::new(0)),
            scratch: Vec::new(),
            segments: Vec::new(),
            table: Vec::new(),
            keys: Vec::new(),
            index_last_key: Vec::new(),
            min_key_len: 0,
            record_count: 0,
            reclaimable: false,
            build_estimate: 0,
            largest_block: 0,
            min_seq: 0,
            max_seq: 0,
            key_hashes: KeyHashes::new(),
            output_budget: None,
            output_refused: false,
        }
    }
    pub(crate) fn in_pool(codec: Codec, target: usize, pool: &ChunkPool) -> Self {
        let mut builder = Self::with_target_block_compressed(codec, target);
        builder.storage = Packing::new(pool);
        builder
    }
    /// Fixed targets used by physical reader tests, including tiny blocks.
    #[cfg(test)]
    pub fn with_raw_block_target(codec: Codec, target: usize) -> Self {
        let mut builder = Self::new(codec);
        builder.raw_target = Some(target.clamp(1, RAW_BLOCK_CAP));
        builder.block_target = builder.raw_target;
        builder
    }
    /// The raw block length expected to compress to the target.
    fn target_for(&self, raw: usize, stored: usize) -> usize {
        self.target
            .saturating_mul(raw)
            .checked_div(stored)
            .unwrap_or(1)
            .clamp(1, RAW_BLOCK_CAP)
    }
    pub fn add(&mut self, key: &[u8], seq: u64, value: Value<'_>) -> Result<(), BuildError> {
        self.block.check(key, seq, value)?;
        if self.record_count > 0 {
            match cmp_internal(&self.last_key, self.last_seq, key, seq) {
                std::cmp::Ordering::Less => {}
                std::cmp::Ordering::Equal => return Err(BuildError::DuplicateKey),
                std::cmp::Ordering::Greater => return Err(BuildError::OutOfOrder),
            }
        }
        // Reserve index room even for an open block cut into singletons.
        if !index_has_room(
            self.table.len() / TABLE_ENTRY_LEN + self.block.len(),
            self.keys.len().saturating_add(self.pending_key_bytes),
        ) {
            return Err(BuildError::RunFull);
        }
        // Bound the complete serialized run, including the buffered segment,
        // before accepting a record. No truncating offset casts can follow.
        // The record adds at most a block of its own to the raw bytes.
        let single = BlockBuilder::single_len(key, seq, value);
        let stored = self
            .storage
            .frames_len
            .saturating_add(self.storage.dictionaries_len)
            .saturating_add(self.segments.len())
            .saturating_add(self.table.len())
            .saturating_add(self.keys.len());
        if !run_has_room(
            stored,
            self.pending
                .len()
                .saturating_add(self.block.encoded_len())
                .saturating_add(single),
            self.pending_blocks.len() + self.block.len() + 1,
            self.pending_key_bytes.saturating_add(key.len()),
        ) {
            return Err(BuildError::RunFull);
        }
        if !self.block.is_empty() {
            let projected = self.block.encoded_len_after(key, seq, value);
            if self.block_target.is_none() && projected > SAMPLE_LEN {
                self.probe();
            }
            if self.cuts_before(key, seq, value) {
                self.finish_block();
            }
        }
        let projected = if self.block.is_empty() {
            single
        } else {
            self.block.encoded_len_after(key, seq, value)
        };
        if !(self.block.is_empty() && self.pending_blocks.is_empty())
            && self.pending.len() + projected > self.segment_len
        {
            self.cut_segment();
        }
        if self.output_refused {
            return Ok(());
        }
        if self.record_count == 0 && !self.reserve_output(key.len()) {
            return Ok(());
        }
        self.block.append(key, seq, value)?;
        if self.record_count == 0 {
            self.keys.extend_from_slice(key);
            self.min_key_len = key.len();
        }
        let value_bytes = match value {
            Value::Bytes(bytes) => bytes.len(),
            Value::Tombstone => 0,
        };
        self.build_estimate = self.build_estimate.saturating_add(
            (key.len() + value_bytes)
                .saturating_mul(4)
                .saturating_add(256),
        );
        self.reclaimable |=
            value == Value::Tombstone || (self.record_count > 0 && self.last_key == key);
        self.last_key.clear();
        self.last_key.extend_from_slice(key);
        self.last_seq = seq;
        self.pending_key_bytes += key.len();
        self.min_seq = if self.record_count == 0 {
            seq
        } else {
            self.min_seq.min(seq)
        };
        self.record_count += 1;
        self.max_seq = self.max_seq.max(seq);
        self.key_hashes.add(key);
        Ok(())
    }
    /// Whether the open block ends before this record. Without a target yet,
    /// the block grows to the probe length.
    fn cuts_before(&self, key: &[u8], seq: u64, value: Value<'_>) -> bool {
        let projected = self.block.encoded_len_after(key, seq, value);
        let Some(target) = self.block_target else {
            return projected > SAMPLE_LEN;
        };
        if self.raw_target.is_some() {
            let record_len = key.len()
                + match value {
                    Value::Bytes(v) => v.len(),
                    Value::Tombstone => 0,
                };
            let limit = if self.block.last_key() == key {
                2 * target
            } else {
                target
            };
            record_len >= target
                || self.block.encoded_len() + record_len / 2 >= limit
                || projected > RAW_BLOCK_CAP
        } else {
            projected > target
        }
    }
    #[cfg(test)]
    pub fn len(&self) -> usize {
        self.record_count
    }
    #[cfg(test)]
    pub fn is_empty(&self) -> bool {
        self.record_count == 0
    }
    pub(super) fn limit_segment_len(&mut self, bytes: usize) {
        self.segment_len = bytes.clamp(RAW_BLOCK_CAP, SEGMENT_RAW_LEN);
    }
    pub(crate) fn limit_filter_build_bytes(&mut self, bytes: usize) {
        self.key_hashes.limit_to(bytes / size_of::<u64>());
    }
    pub(super) fn limit_output(&mut self, budget: Arc<OutputBudget>) {
        self.output_budget = Some(budget);
        self.reserve_output(FOOTER_LEN + 192);
    }
    pub(super) fn output_refused(&self) -> bool {
        self.output_refused
    }
    fn reserve_output(&mut self, bytes: usize) -> bool {
        if self.output_refused {
            return false;
        }
        if self
            .output_budget
            .as_ref()
            .is_some_and(|budget| !budget.reserve(bytes))
        {
            self.output_refused = true;
            return false;
        }
        true
    }

    #[cfg(test)]
    pub fn context_bytes(&self) -> usize {
        self.compressor.context_bytes()
    }
    pub fn finish(self) -> Run<Vec<u8>> {
        self.finish_checked()
            .expect("unlimited builders cannot exhaust output")
    }

    pub(super) fn finish_checked(mut self) -> Result<Run<Vec<u8>>, ()> {
        self.cut_segment();
        if self.output_refused {
            return Err(());
        }
        // Store the upper bound whole as well, so range pruning can borrow it.
        // All other restart keys occur exactly every sixteen entries.
        if !self.table.is_empty() {
            let last = self.table.len() / TABLE_ENTRY_LEN - 1;
            let start = if last == 0 {
                self.min_key_len
            } else {
                u32::from_le_bytes(
                    self.table[(last - 1) * TABLE_ENTRY_LEN + 4..last * TABLE_ENTRY_LEN]
                        .try_into()
                        .unwrap(),
                ) as usize
            };
            self.keys.truncate(start);
            self.keys.extend_from_slice(&0u16.to_le_bytes());
            self.keys.extend_from_slice(&self.index_last_key);
            let end = u32::try_from(self.keys.len()).expect("bounded key area");
            let at = self.table.len() - 4;
            self.table[at..].copy_from_slice(&end.to_le_bytes());
        }
        let dictionaries_offset = self.storage.frames_len;
        let index_offset =
            dictionaries_offset + self.storage.dictionaries_len + self.segments.len();
        // Segment offsets were relative to the dictionary region while building.
        for entry in self.segments.as_chunks_mut::<SEGMENT_ENTRY_LEN>().0 {
            let offset = u64::from_le_bytes(entry[..8].try_into().expect("offset"))
                + dictionaries_offset as u64;
            entry[..8].copy_from_slice(&offset.to_le_bytes());
        }
        let segment_count = self.segments.len() / SEGMENT_ENTRY_LEN;
        let total = index_offset + self.table.len() + self.keys.len() + FOOTER_LEN;
        assert!(u32::try_from(total).is_ok(), "bounded run size");
        let mut checksum = crc32fast::Hasher::new();
        checksum.update(&self.table);
        checksum.update(&self.keys);
        let index_crc = checksum.finalize();
        let mut data = self.segments;
        data.extend_from_slice(&self.table);
        data.extend_from_slice(&self.keys);
        data.extend_from_slice(&(index_offset as u64).to_le_bytes());
        data.extend_from_slice(&(self.record_count as u64).to_le_bytes());
        data.extend_from_slice(&((self.table.len() / TABLE_ENTRY_LEN) as u32).to_le_bytes());
        data.extend_from_slice(&(self.min_key_len as u32).to_le_bytes());
        data.extend_from_slice(&(dictionaries_offset as u64).to_le_bytes());
        data.extend_from_slice(&(segment_count as u32).to_le_bytes());
        data.extend_from_slice(&index_crc.to_le_bytes());
        data.extend_from_slice(&FORMAT_VERSION.to_le_bytes());
        data.extend_from_slice(&MAGIC.to_le_bytes());
        Ok(Run::from_builder(
            RunData::Segmented(self.storage.finish(data.into_boxed_slice())),
            self.key_hashes.finish(),
            Seqs {
                min: self.min_seq,
                max: self.max_seq,
            },
            self.reclaimable,
            BlockSizes {
                build_estimate: self.build_estimate,
                largest_block: self.largest_block,
            },
        ))
    }
    /// Sets the segment's target from the open block's first 64 KiB,
    /// compressed at level 1 without a dictionary, then cuts the block's
    /// records again at that target.
    fn probe(&mut self) {
        let mut raw = std::mem::take(&mut self.scratch);
        raw.clear();
        self.block.flush_into(&mut raw);
        self.pending_key_bytes = 0;
        let sample = &raw[..raw.len().min(SAMPLE_LEN)];
        let mut compressed = std::mem::take(&mut self.frames);
        let stored = self.compressor.probe(sample, &mut compressed);
        self.block_target = Some(self.target_for(sample.len(), stored));
        self.frames = compressed;
        let view = BlockView::open(&raw).expect("builder's block");
        let mut cursor = BlockPosition::new();
        while let Some(record) = cursor.next_record(view).expect("builder's record") {
            if !self.block.is_empty() && self.cuts_before(record.key, record.seq, record.value) {
                self.finish_block();
            }
            self.pending_key_bytes += record.key.len();
            self.block
                .append(record.key, record.seq, record.value)
                .expect("one record fits a block");
        }
        retain_scratch(&mut raw, 2 * RAW_BLOCK_CAP);
        self.scratch = raw;
    }
    /// Moves the open block into the segment buffer and writes its index
    /// entry, whose frame offset is filled in when the segment is compressed.
    fn finish_block(&mut self) {
        if self.block.is_empty() {
            return;
        }
        if self.block_target.is_none() {
            self.probe();
        }
        let last = self.block.last_key();
        // Full keys twice bound both index suffixes and the final unshared key;
        // 64 bounds restart heads and physical frame/dictionary ownership.
        if !self.reserve_output(TABLE_ENTRY_LEN + 2 * last.len() + 64) {
            return;
        }
        let last = self.block.last_key();
        let shared = if (self.table.len() / TABLE_ENTRY_LEN).is_multiple_of(RESTART_INTERVAL) {
            0
        } else {
            self.index_last_key
                .iter()
                .zip(last)
                .take_while(|(a, b)| a == b)
                .count()
                .min(u16::MAX as usize)
        };
        self.keys.extend_from_slice(&(shared as u16).to_le_bytes());
        self.keys.extend_from_slice(&last[shared..]);
        self.index_last_key.clear();
        self.index_last_key.extend_from_slice(last);
        self.table.extend_from_slice(&0u32.to_le_bytes());
        self.table.extend_from_slice(
            &u32::try_from(self.keys.len())
                .expect("bounded key area")
                .to_le_bytes(),
        );
        let records = self.pending_blocks.last().map_or(0, |&(_, n)| n as usize) + self.block.len();
        self.block.flush_into(&mut self.pending);
        self.pending_blocks.push((
            u32::try_from(self.pending.len()).expect("bounded segment"),
            u32::try_from(records).expect("bounded segment"),
        ));
        self.pending_key_bytes = 0;
    }
    fn cut_segment(&mut self) {
        self.finish_block();
        if self.output_refused || self.pending_blocks.is_empty() {
            return;
        }
        let codec = self.compressor.codec();
        let dictionary = if codec == Codec::Zstd {
            dictionary::sample(&Segment {
                raw: &self.pending,
                blocks: &self.pending_blocks,
            })
        } else {
            Vec::new()
        };
        if !self.reserve_output(
            dictionary.len()
                + if codec == Codec::Zstd {
                    SEGMENT_ENTRY_LEN + 32
                } else {
                    0
                },
        ) {
            return;
        }
        let reference = if codec == Codec::Zstd {
            let id = self.segments.len() / SEGMENT_ENTRY_LEN + 1;
            assert!(id < 1 << 24, "segment reference fits in 24 bits");
            self.segments
                .extend_from_slice(&(self.storage.dictionaries_len as u64).to_le_bytes());
            self.segments
                .extend_from_slice(&(dictionary.len() as u32).to_le_bytes());
            self.segments
                .extend_from_slice(&crc32fast::hash(&dictionary).to_le_bytes());
            id as u32
        } else {
            0
        };
        let target = self.block_target.expect("a finished block has a target");
        // The context prepares the dictionary once for every block.
        self.compressor.begin_segment(&dictionary, target);
        let mut start = 0;
        let first = self.table.len() / TABLE_ENTRY_LEN - self.pending_blocks.len();
        for i in 0..self.pending_blocks.len() {
            let end = self.pending_blocks[i].0 as usize;
            self.compress_block(first + i, start..end, reference);
            if self.output_refused {
                return;
            }
            start = end;
        }
        // The next segment measures its own ratio.
        if codec == Codec::Zstd && self.raw_target.is_none() {
            self.block_target = None;
        }
        if !self.output_refused {
            self.storage.dictionary(&dictionary);
        }
        retain_scratch(&mut self.pending, 2 * self.segment_len);
        self.pending_blocks.clear();
        self.block.trim_empty(2 * RAW_BLOCK_CAP);
    }
    /// Compresses the pending bytes of block `number` into its frame and
    /// records the frame's offset in the index.
    fn compress_block(&mut self, number: usize, bytes: Range<usize>, reference: u32) {
        let block = &self.pending[bytes];
        self.frames.clear();
        let offset = self.storage.frames_len;
        let len = block.len();
        let codec = self.compressor.codec();
        self.frames.resize(frame::HEADER_LEN, 0);
        if codec == Codec::None {
            self.frames.extend_from_slice(block);
        } else {
            self.compressor.compress(block, &mut self.frames);
        }
        frame::seal(&mut self.frames, codec, len, reference);
        if !self.reserve_output(self.frames.len()) {
            return;
        }
        self.largest_block = self.largest_block.max(len);
        self.storage
            .compressed(len, self.frames.len(), codec == Codec::Zstd);
        self.storage.frame(&self.frames);
        retain_scratch(&mut self.frames, 2 * RAW_BLOCK_CAP);
        let at = number * TABLE_ENTRY_LEN;
        self.table[at..at + 4].copy_from_slice(
            &u32::try_from(offset)
                .expect("bounded run offset")
                .to_le_bytes(),
        );
    }
}

/// Keep ordinary scratch, but do not retain a singleton oversized record.
fn retain_scratch(bytes: &mut Vec<u8>, limit: usize) {
    if bytes.capacity() > limit {
        *bytes = Vec::new();
    } else {
        bytes.clear();
    }
}

/// Returns whether another block can be started and later indexed, given the
/// blocks and key bytes already recorded. It reserves a table entry and maximum
/// key space for both the open block and the new one, so that cutting those
/// blocks later cannot fail.
fn index_has_room(blocks: usize, key_bytes: usize) -> bool {
    let fits = |total: Option<usize>| total.is_some_and(|n| n <= u32::MAX as usize);
    fits(blocks.checked_add(2)) && fits(key_bytes.checked_add(2 * (MAX_KEY_LEN + 2)))
}

/// Conservative upper bound for pending frames: even singleton Zstd frames
/// fit in twice the raw bytes plus 128 bytes each. Reserve both a segment
/// dictionary and a possible new oversized segment, and a whole final key.
fn run_has_room(stored: usize, raw: usize, records: usize, keys: usize) -> bool {
    let bound = stored
        .saturating_add(raw.saturating_mul(2))
        .saturating_add(records.saturating_mul(frame::HEADER_LEN + TABLE_ENTRY_LEN + 2 + 128))
        .saturating_add(keys)
        .saturating_add(2 * (dictionary::DICTIONARY_LEN + SEGMENT_ENTRY_LEN + MAX_KEY_LEN))
        .saturating_add(FOOTER_LEN);
    bound <= u32::MAX as usize
}

impl fmt::Debug for RunBuilder {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RunBuilder")
            .field("codec", &self.compressor.codec())
            .field("target", &self.target)
            .field("record_count", &self.record_count)
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const LIMIT: usize = u32::MAX as usize;

    #[test]
    fn block_count_stops_with_room_for_two_more_blocks() {
        assert!(index_has_room(0, 0));
        assert!(index_has_room(LIMIT - 2, 0));
        assert!(!index_has_room(LIMIT - 1, 0));
    }

    #[test]
    fn key_area_stops_with_room_for_two_maximum_keys() {
        assert!(index_has_room(0, LIMIT - 2 * (MAX_KEY_LEN + 2)));
        assert!(!index_has_room(0, LIMIT - 2 * (MAX_KEY_LEN + 2) + 1));
    }

    #[test]
    fn oversize_runs_are_rejected_before_a_record_or_offset_is_written() {
        for codec in [Codec::None, Codec::Zstd] {
            let mut builder = RunBuilder::new(codec);
            builder.storage.frames_len = LIMIT - 1;
            assert_eq!(
                builder.add(b"key", 1, Value::Bytes(b"v")),
                Err(BuildError::RunFull)
            );
            assert_eq!(builder.len(), 0);
            assert!(builder.table.is_empty() && builder.block.is_empty());
        }
        assert!(!run_has_room(usize::MAX, 0, 0, 0));
        assert!(!run_has_room(0, usize::MAX, 1, 0));
        assert!(run_has_room(0, 1 << 30, 1000, 16000));
    }

    #[test]
    fn extreme_counts_are_rejected_without_overflow() {
        assert!(!index_has_room(usize::MAX, 0));
        assert!(!index_has_room(0, usize::MAX));
    }
}
