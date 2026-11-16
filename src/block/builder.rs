//! Deterministic block encoding with prefix sharing and restart keys.
//! Records must arrive in internal-key order; preflight checks let callers
//! finish a full block and retry without losing the rejected record.

use std::cmp::Ordering;
use std::fmt;

use super::{
    MAX_BLOCK_LEN, MAX_KEY_LEN, MAX_VALUE_LEN, RESTART_INTERVAL, U32_LEN, Value, cmp_internal,
    common_prefix_len, varint,
};

/// Encodes records, supplied in block order, into an immutable block.
///
/// Records must arrive by user key ascending, then sequence number descending.
/// A rejected [`check`](Self::check) leaves the builder unchanged, so a caller can
/// respond to [`BuildError::BlockFull`] by finishing this block and retrying
/// the record in a new one.
#[derive(Debug, Default)]
pub struct BlockBuilder {
    entries: Vec<u8>,
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
    pub(crate) fn check(
        &self,
        user_key: &[u8],
        seq: u64,
        value: Value<'_>,
    ) -> Result<(), BuildError> {
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
        if self.record_count > 0 {
            match cmp_internal(&self.last_key, self.last_seq, user_key, seq) {
                Ordering::Less => {}
                Ordering::Equal => return Err(BuildError::DuplicateKey),
                Ordering::Greater => return Err(BuildError::OutOfOrder),
            }
        }
        Ok(())
    }

    /// Exact encoded length after an already checked record, including restarts.
    /// Run builders use this to cut at raw byte limits without over-counting
    /// the shared part of a prefix-compressed key.
    pub(crate) fn encoded_len_after(&self, key: &[u8], seq: u64, value: Value<'_>) -> usize {
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
        let suffix = key.len() - shared;
        self.encoded_len()
            + varint::encoded_len(shared as u64)
            + varint::encoded_len(suffix as u64)
            + varint::encoded_len(tag)
            + varint::encoded_len(seq)
            + suffix
            + len
            + if restart { U32_LEN } else { 0 }
    }

    /// Encodes a record that has passed [`check`](Self::check).
    pub(crate) fn append(
        &mut self,
        user_key: &[u8],
        seq: u64,
        value: Value<'_>,
    ) -> Result<(), BuildError> {
        let (value_tag, value_bytes): (u64, &[u8]) = match value {
            Value::Tombstone => (0, &[]),
            Value::Bytes(bytes) => (bytes.len() as u64 + 1, bytes),
        };

        let is_restart = self.record_count.is_multiple_of(RESTART_INTERVAL);
        let shared = if is_restart {
            0
        } else {
            common_prefix_len(&self.last_key, user_key)
        };
        let suffix = &user_key[shared..];

        // Every term is bounded by the limits above and the current length by
        // MAX_BLOCK_LEN, so this sum cannot overflow.
        let record_len = varint::encoded_len(shared as u64)
            + varint::encoded_len(suffix.len() as u64)
            + varint::encoded_len(value_tag)
            + varint::encoded_len(seq)
            + suffix.len()
            + value_bytes.len();
        let directory_growth = if is_restart { U32_LEN } else { 0 };
        if self.encoded_len() + record_len + directory_growth > MAX_BLOCK_LEN {
            return Err(BuildError::BlockFull);
        }

        if is_restart {
            // Offsets are below MAX_BLOCK_LEN, which fits in u32.
            self.restarts.push(self.entries.len() as u32);
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
        self.entries.len() + self.restarts.len() * U32_LEN + U32_LEN
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
        out.extend_from_slice(&self.entries);
        write_directory(&self.restarts, out);
        self.entries.clear();
        self.restarts.clear();
        self.record_count = 0;
    }

    /// Release unusually large empty build buffers after an oversized record.
    pub(crate) fn trim_empty(&mut self, limit: usize) {
        debug_assert!(self.is_empty());
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
