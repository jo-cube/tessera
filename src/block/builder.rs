//! Deterministic block encoding with prefix sharing and restart keys.
//! Records must arrive in internal-key order; preflight checks let callers
//! finish a full block and retry without losing the rejected record.

use std::fmt;

use super::{
    MAX_BLOCK_LEN, MAX_KEY_LEN, MAX_VALUE_LEN, RESTART_INTERVAL, U32_LEN, Value, common_prefix_len,
    varint,
};

/// Encodes records, supplied in block order, into an immutable block.
///
/// Records must arrive by user key ascending, then sequence number descending.
/// A rejected [`check`](Self::check) leaves the builder unchanged, so a caller can
/// respond to [`BuildError::BlockFull`] by finishing this block and retrying
/// the record in a new one.
#[derive(Debug, Default)]
pub struct BlockBuilder {
    /// Sealed blocks, then the open block's records from `start`.
    entries: Vec<u8>,
    start: usize,
    restarts: Vec<u32>,
    last_key: Vec<u8>,
    last_seq: u64,
    record_count: usize,
}

impl BlockBuilder {
    /// Creates an empty builder.
    pub fn new() -> Self {
        Self::default()
    }

    /// Appends one record.
    #[cfg(test)]
    pub fn add(&mut self, user_key: &[u8], seq: u64, value: Value<'_>) -> Result<(), BuildError> {
        self.check(user_key, seq, value)?;
        self.append(user_key, seq, value)
    }

    /// Checks limits and order against the previous record.
    #[cfg(test)]
    pub(crate) fn check(
        &self,
        user_key: &[u8],
        seq: u64,
        value: Value<'_>,
    ) -> Result<(), BuildError> {
        Self::check_limits(user_key, value)?;
        if self.record_count > 0 {
            use std::cmp::Ordering;
            match super::cmp_internal(&self.last_key, self.last_seq, user_key, seq) {
                Ordering::Less => {}
                Ordering::Equal => return Err(BuildError::DuplicateKey),
                Ordering::Greater => return Err(BuildError::OutOfOrder),
            }
        }
        Ok(())
    }

    /// Checks the key and value length limits only, for callers that order
    /// records themselves.
    pub(crate) fn check_limits(user_key: &[u8], value: Value<'_>) -> Result<(), BuildError> {
        if user_key.len() > MAX_KEY_LEN {
            return Err(BuildError::KeyTooLong {
                len: user_key.len(),
            });
        }
        if let Value::Bytes(bytes) = value
            && bytes.len() > MAX_VALUE_LEN
        {
            return Err(BuildError::ValueTooLong { len: bytes.len() });
        }
        Ok(())
    }

    /// Exact encoded length after an already checked record, including restarts.
    #[cfg(feature = "diagnostics")]
    pub(crate) fn encoded_len_after(&self, key: &[u8], seq: u64, value: Value<'_>) -> usize {
        self.project(key, seq, value).after
    }

    /// The encoded lengths of an already checked record, computed once: in
    /// this block, and in a block of its own, which bounds the bytes it adds
    /// to any block.
    pub(crate) fn project(&self, key: &[u8], seq: u64, value: Value<'_>) -> Projection {
        let restart = self.record_count.is_multiple_of(RESTART_INTERVAL);
        let shared = if restart {
            0
        } else {
            common_prefix_len(&self.last_key, key)
        };
        let (tag, len) = match value {
            Value::Bytes(v) => (v.len() as u64 + 1, v.len()),
            Value::Tombstone => (0, 0),
        };
        let fixed = varint::encoded_len(tag) + varint::encoded_len(seq) + len;
        let suffix = key.len() - shared;
        let record = varint::encoded_len(shared as u64)
            + varint::encoded_len(suffix as u64)
            + fixed
            + suffix;
        Projection {
            shared,
            record,
            after: self.encoded_len() + record + if restart { U32_LEN } else { 0 },
            single: 1 + varint::encoded_len(key.len() as u64) + fixed + key.len() + 2 * U32_LEN,
        }
    }

    /// Encodes a record that has passed [`check`](Self::check).
    #[cfg(any(test, feature = "diagnostics"))]
    pub(crate) fn append(
        &mut self,
        user_key: &[u8],
        seq: u64,
        value: Value<'_>,
    ) -> Result<(), BuildError> {
        let projection = self.project(user_key, seq, value);
        self.append_projected(user_key, seq, value, projection)
    }

    /// [`append`](Self::append) with the record's [`project`](Self::project)
    /// result, taken since the last change to this builder.
    pub(crate) fn append_projected(
        &mut self,
        user_key: &[u8],
        seq: u64,
        value: Value<'_>,
        projection: Projection,
    ) -> Result<(), BuildError> {
        let (value_tag, value_bytes): (u64, &[u8]) = match value {
            Value::Tombstone => (0, &[]),
            Value::Bytes(bytes) => (bytes.len() as u64 + 1, bytes),
        };
        if projection.after > MAX_BLOCK_LEN {
            return Err(BuildError::BlockFull);
        }
        let shared = projection.shared;
        let suffix = &user_key[shared..];
        self.entries.reserve(projection.record);
        if self.record_count.is_multiple_of(RESTART_INTERVAL) {
            // Offsets are below MAX_BLOCK_LEN, which fits in u32.
            self.restarts.push((self.entries.len() - self.start) as u32);
        }
        varint::write(&mut self.entries, shared as u64);
        varint::write(&mut self.entries, suffix.len() as u64);
        varint::write(&mut self.entries, value_tag);
        varint::write(&mut self.entries, seq);
        self.entries.extend_from_slice(suffix);
        self.entries.extend_from_slice(value_bytes);

        self.last_key.truncate(shared);
        self.last_key.extend_from_slice(suffix);
        self.last_seq = seq;
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

    /// Returns the exact encoded length, including the restart directory.
    pub fn encoded_len(&self) -> usize {
        self.entries.len() - self.start + self.restarts.len() * U32_LEN + U32_LEN
    }

    /// Writes the restart directory and returns the finished block bytes.
    ///
    /// The result is never longer than [`MAX_BLOCK_LEN`] because
    /// [`add`](Self::add) accounts for the directory before accepting a record.
    #[cfg(test)]
    pub fn finish(self) -> Vec<u8> {
        let mut block = self.entries;
        block.reserve_exact(self.restarts.len() * U32_LEN + U32_LEN);
        write_directory(&self.restarts, &mut block);
        block
    }

    /// Appends the finished block to `out` and empties this builder, keeping
    /// its buffers. The last key stays readable until the next record.
    pub(crate) fn flush_into(&mut self, out: &mut Vec<u8>) {
        out.reserve(self.encoded_len());
        out.extend_from_slice(&self.entries[self.start..]);
        write_directory(&self.restarts, out);
        self.entries.truncate(self.start);
        self.restarts.clear();
        self.record_count = 0;
    }

    /// Finishes the open block in place, after the blocks sealed before it,
    /// and empties the open block. The last key stays readable until the
    /// next record.
    pub(crate) fn seal(&mut self) {
        write_directory(&self.restarts, &mut self.entries);
        self.start = self.entries.len();
        self.restarts.clear();
        self.record_count = 0;
    }

    /// The finished blocks sealed since the last [`clear_sealed`](Self::clear_sealed).
    pub(crate) fn sealed(&self) -> &[u8] {
        &self.entries[..self.start]
    }

    /// Discards the sealed blocks; the open block must be empty.
    pub(crate) fn clear_sealed(&mut self) {
        debug_assert!(self.is_empty());
        self.entries.clear();
        self.start = 0;
    }

    /// Release unusually large empty build buffers after an oversized record.
    pub(crate) fn trim_empty(&mut self, limit: usize) {
        debug_assert!(self.is_empty() && self.start == 0);
        if self.entries.capacity() > limit {
            self.entries = Vec::new();
        }
        if self.restarts.capacity() * U32_LEN > limit {
            self.restarts = Vec::new();
        }
    }

    /// Returns the user key of the most recently added record.
    pub(crate) fn last_key(&self) -> &[u8] {
        &self.last_key
    }
}

/// A record's encoded lengths from [`BlockBuilder::project`].
#[derive(Clone, Copy, Debug)]
pub(crate) struct Projection {
    shared: usize,
    /// The record's bytes, excluding any restart entry.
    record: usize,
    /// The open block's encoded length with the record.
    pub after: usize,
    /// The encoded length of a block holding only the record.
    pub single: usize,
}

impl Projection {
    /// The projection after the open block was finished, which makes the
    /// record a new block's first.
    pub(crate) fn in_new_block(self) -> Self {
        Self {
            shared: 0,
            record: self.single - 2 * U32_LEN,
            after: self.single,
            single: self.single,
        }
    }
}

fn write_directory(restarts: &[u32], out: &mut Vec<u8>) {
    for offset in restarts {
        out.extend_from_slice(&offset.to_le_bytes());
    }
    out.extend_from_slice(&(restarts.len() as u32).to_le_bytes());
}

/// Why a record was rejected by [`BlockBuilder::check`] or [`BlockBuilder::append`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BuildError {
    /// The user key exceeds [`MAX_KEY_LEN`].
    KeyTooLong {
        /// Length of the rejected key.
        len: usize,
    },
    /// The value exceeds [`MAX_VALUE_LEN`].
    ValueTooLong {
        /// Length of the rejected value.
        len: usize,
    },
    /// The record sorts before the preceding record.
    OutOfOrder,
    /// The record has the same user key and sequence number as the previous
    /// record. An internal key may appear at most once in a block.
    DuplicateKey,
    /// Accepting the record would exceed [`MAX_BLOCK_LEN`].
    BlockFull,
    /// The run's block table or key area would exceed its 32-bit limits.
    RunFull,
}

impl fmt::Display for BuildError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::KeyTooLong { len } => {
                write!(f, "key of {len} bytes exceeds limit of {MAX_KEY_LEN}")
            }
            Self::ValueTooLong { len } => {
                write!(f, "value of {len} bytes exceeds limit of {MAX_VALUE_LEN}")
            }
            Self::OutOfOrder => f.write_str("record sorts before the previous record"),
            Self::DuplicateKey => f.write_str("duplicate user key and sequence number"),
            Self::BlockFull => write!(f, "record would exceed block limit of {MAX_BLOCK_LEN}"),
            Self::RunFull => f.write_str("run would exceed its 32-bit size or index limits"),
        }
    }
}

impl std::error::Error for BuildError {}
