use std::fmt;
use std::sync::Arc;

use crate::block::{BlockBuilder, BuildError, MAX_KEY_LEN, Value};

use super::codec::{Codec, Compressor};
use super::dictionary::Trained;
use super::filter::KeyHashes;
use super::frame;
use super::reader::Run;
use super::{
    DICTIONARY_FOOTER_LEN, DICTIONARY_VERSION, FOOTER_LEN, FORMAT_VERSION, MAGIC,
    MAX_TARGET_BLOCK_LEN, TABLE_ENTRY_LEN, TARGET_BLOCK_LEN,
};

/// Encodes records, supplied in internal-key order, into an immutable [`Run`].
///
/// Records must arrive by user key ascending, then sequence number descending,
/// across the whole run. A rejected [`add`](Self::add) changes nothing, and
/// the result does not depend on rejected records.
///
/// Each block is compressed independently as it is cut, so the builder holds
/// at most one uncompressed block beside the frames already written. Block
/// boundaries depend only on uncompressed sizes, so they are the same for
/// every codec.
///
/// The builder also hashes each distinct user key, 8 bytes per key until
/// [`finish`](Self::finish), for the run's in-memory key filter; see
/// [`Run::filter_bytes`]. The hash buffer doubles as needed and stays below
/// 16 bytes per distinct key; a run of more than 2^24 distinct keys gets no
/// filter.
///
/// # Block boundaries
///
/// A block is cut before a record, which keeps the builder streaming:
///
/// - A new user key starts a new block once the block has reached the target,
///   with the record's midpoint deciding which side of the target is closer.
/// - A further version of the current key cuts only at twice the target, so
///   ordinary version chains stay whole and exceptionally long ones span
///   blocks of bounded size.
/// - A record at least as large as the target always starts a new block.
pub struct RunBuilder {
    target: usize,
    compressor: Compressor,
    block: BlockBuilder,
    /// One finished block awaiting compression. Unused without a codec.
    uncompressed: Vec<u8>,
    frames: Vec<u8>,
    table: Vec<u8>,
    keys: Vec<u8>,
    min_key_len: usize,
    record_count: usize,
    max_seq: u64,
    key_hashes: KeyHashes,
}

impl Default for RunBuilder {
    /// A builder with the default codec and [`TARGET_BLOCK_LEN`].
    fn default() -> Self {
        Self::new(Codec::default())
    }
}

impl RunBuilder {
    /// Creates a builder aiming for blocks of [`TARGET_BLOCK_LEN`] bytes
    /// before compression.
    pub fn new(codec: Codec) -> Self {
        Self::with_target_block_len(codec, TARGET_BLOCK_LEN)
    }

    /// Creates a builder aiming for blocks of `target` bytes before
    /// compression, reduced to [`MAX_TARGET_BLOCK_LEN`] if larger.
    ///
    /// # Panics
    ///
    /// If `codec` is [`Codec::ZstdDictionary`], which needs
    /// [`with_dictionary`](Self::with_dictionary).
    pub fn with_target_block_len(codec: Codec, target: usize) -> Self {
        Self::with_compressor(Compressor::new(codec), target)
    }

    /// Creates a builder like [`with_target_block_len`](Self::with_target_block_len)
    /// whose blocks are Zstandard frames compressed with `dictionary`. The run
    /// stores a copy of the dictionary and is written in version 4.
    pub(crate) fn with_dictionary(dictionary: Arc<Trained>, target: usize) -> Self {
        Self::with_compressor(Compressor::with_dictionary(dictionary), target)
    }

    fn with_compressor(compressor: Compressor, target: usize) -> Self {
        Self {
            target: target.min(MAX_TARGET_BLOCK_LEN),
            compressor,
            block: BlockBuilder::new(),
            uncompressed: Vec::new(),
            frames: Vec::new(),
            table: Vec::new(),
            keys: Vec::new(),
            min_key_len: 0,
            record_count: 0,
            max_seq: 0,
            key_hashes: KeyHashes::new(),
        }
    }

    /// Appends one record.
    pub fn add(&mut self, user_key: &[u8], seq: u64, value: Value<'_>) -> Result<(), BuildError> {
        self.block.check(user_key, seq, value)?;

        let cut = self.should_cut(user_key, value);
        if (cut || self.block.is_empty()) && !self.has_room_for_block() {
            return Err(BuildError::RunFull);
        }
        if cut {
            self.cut_block();
        }

        // A record joins a non-empty block only if the block is below twice
        // the target and the record is below the target. The target is at most
        // a quarter of the block limit, so BlockFull cannot occur here.
        self.block.append(user_key, seq, value)?;
        if self.record_count == 0 {
            // Records arrive in key order, so the first is the smallest.
            self.keys.extend_from_slice(user_key);
            self.min_key_len = user_key.len();
        }
        self.record_count += 1;
        self.max_seq = self.max_seq.max(seq);
        self.key_hashes.add(user_key);
        Ok(())
    }

    /// Returns the number of records added so far.
    #[cfg_attr(
        not(test),
        expect(dead_code, reason = "tested component API the engine does not use")
    )]
    pub fn len(&self) -> usize {
        self.record_count
    }

    /// Returns whether no records have been added.
    #[cfg_attr(
        not(test),
        expect(dead_code, reason = "tested component API the engine does not use")
    )]
    pub fn is_empty(&self) -> bool {
        self.record_count == 0
    }

    /// Returns the length the run would have if finished now, not counting
    /// the open block.
    pub fn stored_len(&self) -> usize {
        let (dictionary, footer) = match self.compressor.dictionary() {
            Some(dictionary) => (dictionary.bytes().len(), DICTIONARY_FOOTER_LEN),
            None => (0, FOOTER_LEN),
        };
        self.frames.len() + dictionary + self.table.len() + self.keys.len() + footer
    }

    /// Builds the run without a filter if its distinct keys would need more
    /// than `bytes` of hash buffer.
    pub(crate) fn limit_filter_build_bytes(&mut self, bytes: usize) {
        self.key_hashes.limit_to(bytes / size_of::<u64>());
    }

    /// Returns the bytes held by a compression context, which Zstandard
    /// allocates outside the Rust heap.
    #[cfg_attr(
        not(test),
        expect(dead_code, reason = "tested component API the engine does not use")
    )]
    pub fn context_bytes(&self) -> usize {
        self.compressor.context_bytes()
    }

    /// Writes the index and footer and returns the finished run, which is
    /// valid by construction and is not walked again. A dictionary run also
    /// stores its dictionary and prepares its own decoding form of it.
    pub fn finish(mut self) -> Run<Vec<u8>> {
        if !self.block.is_empty() {
            self.cut_block();
        }

        let block_count = self.table.len() / TABLE_ENTRY_LEN;
        let dictionary = self.compressor.dictionary().map(Trained::bytes);
        let dictionary_len = dictionary.map_or(0, <[u8]>::len);
        let index_offset = self.frames.len() + dictionary_len;
        let mut data = self.frames;
        data.reserve_exact(
            dictionary_len + self.table.len() + self.keys.len() + DICTIONARY_FOOTER_LEN,
        );
        if let Some(dictionary) = dictionary {
            data.extend_from_slice(dictionary);
        }
        data.extend_from_slice(&self.table);
        data.extend_from_slice(&self.keys);
        data.extend_from_slice(&(index_offset as u64).to_le_bytes());
        data.extend_from_slice(&(self.record_count as u64).to_le_bytes());
        // has_room_for_block keeps the block count within u32.
        data.extend_from_slice(&(block_count as u32).to_le_bytes());
        // A key is at most MAX_KEY_LEN.
        data.extend_from_slice(&(self.min_key_len as u32).to_le_bytes());
        let prepared = match dictionary {
            None => {
                data.extend_from_slice(&FORMAT_VERSION.to_le_bytes());
                None
            }
            Some(dictionary) => {
                // A trained dictionary is at most DICTIONARY_LEN.
                data.extend_from_slice(&(dictionary.len() as u32).to_le_bytes());
                data.extend_from_slice(&crc32fast::hash(dictionary).to_le_bytes());
                data.extend_from_slice(&DICTIONARY_VERSION.to_le_bytes());
                let trained = self.compressor.dictionary().expect("a dictionary run");
                Some(Arc::clone(trained.prepared()))
            }
        };
        data.extend_from_slice(&MAGIC.to_le_bytes());
        data.shrink_to_fit();
        Run::from_builder(data, self.key_hashes.finish(), self.max_seq, prepared)
    }

    fn should_cut(&self, user_key: &[u8], value: Value<'_>) -> bool {
        if self.block.is_empty() {
            return false;
        }
        let value_len = match value {
            Value::Bytes(bytes) => bytes.len(),
            Value::Tombstone => 0,
        };
        let record_len = user_key.len() + value_len;
        if record_len >= self.target {
            return true;
        }
        let limit = if self.block.last_key() == user_key {
            2 * self.target
        } else {
            self.target
        };
        self.block.encoded_len() + record_len / 2 >= limit
    }

    fn has_room_for_block(&self) -> bool {
        index_has_room(self.table.len() / TABLE_ENTRY_LEN, self.keys.len())
    }

    fn cut_block(&mut self) {
        let frame_offset = self.frames.len();
        let block_len = self.block.encoded_len();
        let codec = self.compressor.codec();

        self.frames.resize(frame_offset + frame::HEADER_LEN, 0);
        if codec == Codec::None {
            self.block.flush_into(&mut self.frames);
        } else {
            self.uncompressed.clear();
            self.block.flush_into(&mut self.uncompressed);
            self.compressor
                .compress(&self.uncompressed, &mut self.frames);
        }
        frame::seal(&mut self.frames[frame_offset..], codec, block_len);

        self.keys.extend_from_slice(self.block.last_key());
        self.table
            .extend_from_slice(&(frame_offset as u64).to_le_bytes());
        // has_room_for_block admitted this block with its key space reserved.
        self.table
            .extend_from_slice(&(self.keys.len() as u32).to_le_bytes());
    }
}

/// Returns whether another block can be started and later indexed, given the
/// blocks and key bytes already recorded. It reserves a table entry and maximum
/// key space for both the open block and the new one, so that cutting those
/// blocks later cannot fail.
fn index_has_room(blocks: usize, key_bytes: usize) -> bool {
    let fits = |total: Option<usize>| total.is_some_and(|n| n <= u32::MAX as usize);
    fits(blocks.checked_add(2)) && fits(key_bytes.checked_add(2 * MAX_KEY_LEN))
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
        assert!(index_has_room(0, LIMIT - 2 * MAX_KEY_LEN));
        assert!(!index_has_room(0, LIMIT - 2 * MAX_KEY_LEN + 1));
    }

    #[test]
    fn extreme_counts_are_rejected_without_overflow() {
        assert!(!index_has_room(usize::MAX, 0));
        assert!(!index_has_room(0, usize::MAX));
    }
}
