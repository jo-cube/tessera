use std::fmt;

use crate::block::{BlockBuilder, BuildError, MAX_KEY_LEN, Value};

use super::codec::{Codec, Compressor};
use super::frame;
use super::reader::Run;
use super::{
    FOOTER_LEN, FORMAT_VERSION, MAGIC, MAX_TARGET_BLOCK_LEN, TABLE_ENTRY_LEN, TARGET_BLOCK_LEN,
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
    record_count: usize,
}

impl RunBuilder {
    /// Creates a builder aiming for blocks of [`TARGET_BLOCK_LEN`] bytes
    /// before compression.
    pub fn new(codec: Codec) -> Self {
        Self::with_target_block_len(codec, TARGET_BLOCK_LEN)
    }

    /// Creates a builder aiming for blocks of `target` bytes before
    /// compression, reduced to [`MAX_TARGET_BLOCK_LEN`] if larger.
    pub fn with_target_block_len(codec: Codec, target: usize) -> Self {
        Self {
            target: target.min(MAX_TARGET_BLOCK_LEN),
            compressor: Compressor::new(codec),
            block: BlockBuilder::new(),
            uncompressed: Vec::new(),
            frames: Vec::new(),
            table: Vec::new(),
            keys: Vec::new(),
            record_count: 0,
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
        self.record_count += 1;
        Ok(())
    }

    /// Returns the number of records added so far.
    pub fn len(&self) -> usize {
        self.record_count
    }

    /// Returns whether no records have been added.
    pub fn is_empty(&self) -> bool {
        self.record_count == 0
    }

    /// Writes the index and footer and returns the finished run, which is
    /// valid by construction and is not walked again.
    pub fn finish(mut self) -> Run<Vec<u8>> {
        if !self.block.is_empty() {
            self.cut_block();
        }

        let block_count = self.table.len() / TABLE_ENTRY_LEN;
        let index_offset = self.frames.len();
        let mut data = self.frames;
        data.reserve_exact(self.table.len() + self.keys.len() + FOOTER_LEN);
        data.extend_from_slice(&self.table);
        data.extend_from_slice(&self.keys);
        data.extend_from_slice(&(index_offset as u64).to_le_bytes());
        data.extend_from_slice(&(self.record_count as u64).to_le_bytes());
        // has_room_for_block keeps the block count within u32.
        data.extend_from_slice(&(block_count as u32).to_le_bytes());
        data.extend_from_slice(&FORMAT_VERSION.to_le_bytes());
        data.extend_from_slice(&MAGIC.to_le_bytes());
        data.shrink_to_fit();
        Run::from_builder(data)
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
