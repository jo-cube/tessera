use super::codec::{Codec, Compressor};
use super::dictionary::{self, SAMPLE_LEN, SEGMENT_RAW_LEN};
use super::filter::KeyHashes;
use super::storage::{Packing, RunData};
use super::{
    FOOTER_LEN, FORMAT_VERSION, MAGIC, RAW_BLOCK_CAP, SEGMENT_ENTRY_LEN, TABLE_ENTRY_LEN,
    TARGET_BLOCK_COMPRESSED, TARGET_BLOCK_LEN, frame, reader::Run,
};
use crate::block::{
    BlockBuilder, BlockPosition, BlockView, BuildError, MAX_KEY_LEN, RESTART_INTERVAL, Value,
    cmp_internal,
};
use crate::chunk::ChunkPool;
use std::fmt;

/// Encodes sorted records, buffering at most one raw segment before compressing.
/// Rejected records leave the builder unchanged. A single oversized record is
/// stored in its own block and, if necessary, its own segment.
pub struct RunBuilder {
    target: usize,
    segment_len: usize,
    raw_target: Option<usize>,
    compressor: Compressor,
    segment: BlockBuilder,
    last_key: Vec<u8>,
    last_seq: u64,
    pending_key_bytes: usize,
    frames: Vec<u8>,
    storage: Packing,
    raw: Vec<u8>,
    uncompressed: Vec<u8>,
    block: BlockBuilder,
    segments: Vec<u8>,
    table: Vec<u8>,
    keys: Vec<u8>,
    index_last_key: Vec<u8>,
    min_key_len: usize,
    record_count: usize,
    reclaimable: bool,
    build_estimate: usize,
    max_seq: u64,
    key_hashes: KeyHashes,
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
            compressor: Compressor::new(codec),
            segment: BlockBuilder::new(),
            last_key: Vec::new(),
            last_seq: 0,
            pending_key_bytes: 0,
            frames: Vec::new(),
            storage: Packing::new(&ChunkPool::new(0)),
            raw: Vec::new(),
            uncompressed: Vec::new(),
            block: BlockBuilder::new(),
            segments: Vec::new(),
            table: Vec::new(),
            keys: Vec::new(),
            index_last_key: Vec::new(),
            min_key_len: 0,
            record_count: 0,
            reclaimable: false,
            build_estimate: 0,
            max_seq: 0,
            key_hashes: KeyHashes::new(),
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
        builder
    }
    pub fn add(&mut self, key: &[u8], seq: u64, value: Value<'_>) -> Result<(), BuildError> {
        self.segment.check(key, seq, value)?;
        if self.record_count > 0 {
            match cmp_internal(&self.last_key, self.last_seq, key, seq) {
                std::cmp::Ordering::Less => {}
                std::cmp::Ordering::Equal => return Err(BuildError::DuplicateKey),
                std::cmp::Ordering::Greater => return Err(BuildError::OutOfOrder),
            }
        }
        // Reserve index room even for a segment made entirely of singleton blocks.
        if !index_has_room(
            self.table.len() / TABLE_ENTRY_LEN + self.segment.len(),
            self.keys.len().saturating_add(self.pending_key_bytes),
        ) {
            return Err(BuildError::RunFull);
        }
        // Bound the complete serialized run, including the buffered segment,
        // before accepting a record. No truncating offset casts can follow.
        let stored = self
            .storage
            .frames_len
            .saturating_add(self.storage.dictionaries_len)
            .saturating_add(self.segments.len())
            .saturating_add(self.table.len())
            .saturating_add(self.keys.len());
        if !run_has_room(
            stored,
            self.segment.encoded_len_after(key, seq, value),
            self.segment.len().saturating_add(1),
            self.pending_key_bytes.saturating_add(key.len()),
        ) {
            return Err(BuildError::RunFull);
        }
        if !self.segment.is_empty()
            && self.segment.encoded_len_after(key, seq, value) > self.segment_len
        {
            self.cut_segment();
        }
        self.segment.append(key, seq, value)?;
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
        self.record_count += 1;
        self.max_seq = self.max_seq.max(seq);
        self.key_hashes.add(key);
        Ok(())
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
    #[cfg(test)]
    pub fn context_bytes(&self) -> usize {
        self.compressor.context_bytes()
    }
    pub fn finish(mut self) -> Run<Vec<u8>> {
        self.cut_segment();
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
        Run::from_builder(
            RunData::Segmented(self.storage.finish(data.into_boxed_slice())),
            self.key_hashes.finish(),
            self.max_seq,
            self.reclaimable,
            self.build_estimate,
        )
    }
    fn cut_segment(&mut self) {
        if self.segment.is_empty() {
            return;
        }
        let count = self.segment.len();
        self.pending_key_bytes = 0;
        let mut raw = std::mem::take(&mut self.raw);
        raw.clear();
        self.segment.flush_into(&mut raw);
        self.segment.trim_empty(2 * self.segment_len);
        let codec = self.compressor.codec();
        let dictionary = if codec == Codec::Zstd {
            dictionary::sample(&raw, count)
        } else {
            Vec::new()
        };
        // Prepared once for the sample and every block, then dropped before
        // the next segment. Only the raw dictionary is stored in the run.
        let prepared = self.compressor.prepare_dictionary(&dictionary);
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
        let target = self.raw_target.unwrap_or_else(|| {
            if codec == Codec::None {
                return TARGET_BLOCK_LEN;
            }
            let start = if raw.len() <= SAMPLE_LEN {
                0
            } else {
                raw.len() / 2
            };
            let sample = &raw[start..(start + SAMPLE_LEN).min(raw.len())];
            let mut compressed = Vec::new();
            self.compressor
                .compress(sample, prepared.as_ref(), &mut compressed);
            self.target
                .saturating_mul(sample.len())
                .checked_div(compressed.len())
                .unwrap_or(1)
                .clamp(1, RAW_BLOCK_CAP)
        });
        self.compressor.begin_segment(&dictionary, target);
        let view = BlockView::open(&raw).expect("builder's segment");
        let mut cursor = BlockPosition::new();
        let mut block = std::mem::take(&mut self.block);
        let mut uncompressed = std::mem::take(&mut self.uncompressed);
        while let Some(record) = cursor.next_record(view).expect("builder's record") {
            let projected = block.encoded_len_after(record.key, record.seq, record.value);
            let cut = if self.raw_target.is_some() {
                let record_len = record.key.len()
                    + match record.value {
                        Value::Bytes(v) => v.len(),
                        Value::Tombstone => 0,
                    };
                let limit = if block.last_key() == record.key {
                    2 * target
                } else {
                    target
                };
                record_len >= target
                    || block.encoded_len() + record_len / 2 >= limit
                    || projected > RAW_BLOCK_CAP
            } else {
                projected > target
            };
            if !block.is_empty() && cut {
                self.cut_block(&mut block, &mut uncompressed, reference);
            }
            block
                .append(record.key, record.seq, record.value)
                .expect("one record fits a block");
        }
        if !block.is_empty() {
            self.cut_block(&mut block, &mut uncompressed, reference);
        }
        self.storage.dictionary(&dictionary);
        retain_scratch(&mut raw, 2 * self.segment_len);
        retain_scratch(&mut uncompressed, 2 * RAW_BLOCK_CAP);
        block.trim_empty(2 * RAW_BLOCK_CAP);
        self.raw = raw;
        self.uncompressed = uncompressed;
        self.block = block;
    }
    fn cut_block(&mut self, block: &mut BlockBuilder, raw: &mut Vec<u8>, reference: u32) {
        self.frames.clear();
        let offset = self.storage.frames_len;
        let len = block.encoded_len();
        let codec = self.compressor.codec();
        self.frames.resize(frame::HEADER_LEN, 0);
        if codec == Codec::None {
            block.flush_into(&mut self.frames);
        } else {
            raw.clear();
            block.flush_into(raw);
            self.compressor.compress(raw, None, &mut self.frames);
        }
        frame::seal(&mut self.frames, codec, len, reference);
        self.storage
            .compressed(len, self.frames.len(), codec == Codec::Zstd);
        self.storage.frame(&self.frames);
        retain_scratch(&mut self.frames, 2 * RAW_BLOCK_CAP);
        let last = block.last_key();
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
        self.table.extend_from_slice(
            &u32::try_from(offset)
                .expect("bounded run offset")
                .to_le_bytes(),
        );
        self.table.extend_from_slice(
            &u32::try_from(self.keys.len())
                .expect("bounded key area")
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
            assert!(builder.table.is_empty() && builder.segment.is_empty());
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
