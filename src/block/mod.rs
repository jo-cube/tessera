//! Compact, immutable, sorted data blocks.
//!
//! A block stores versioned records ordered by user key ascending, then
//! sequence number descending. [`BlockBuilder`] encodes records supplied in
//! that order. [`RecordWalk`] validates every record during run recovery.
//!
//! # Reading
//!
//! Full validation costs O(block length), so it is separated from access:
//!
//! - [`RecordWalk::validate_block_with`] validates bytes of unknown origin.
//! - [`BlockView::open`] costs O(1) and suits bytes already known to be valid,
//!   such as a validated block decompressed again into a scratch buffer. Its
//!   reads stay panic free and report what they touch; its documentation
//!   states the trust contract.
//! - [`BlockPosition`] is a cursor that does not borrow the block, so it can
//!   live beside the buffer it walks.
//!
//! # Format
//!
//! All integers are either canonical unsigned LEB128 (`varint`) or
//! little-endian `u32`. A given record sequence always encodes to the same
//! bytes.
//!
//! ```text
//! block   := record* restart* restart_count
//! restart := u32        byte offset of a record from the start of the block
//! restart_count := u32  number of restart entries
//!
//! record  := shared suffix_len value_tag seq suffix value
//! shared     := varint  key bytes reused from the previous record's key
//! suffix_len := varint  length of suffix
//! value_tag  := varint  0 for a tombstone, otherwise value length + 1
//! seq        := varint  full 64-bit sequence number
//! suffix     := suffix_len bytes
//! value      := value_tag - 1 bytes, absent for a tombstone
//! ```
//!
//! Invariants of a valid block, all checked by [`RecordWalk`]:
//!
//! - Internal keys `(user key, seq)` strictly increase in block order, where
//!   user keys compare ascending and sequence numbers descending. Strictness
//!   means an internal key appears at most once.
//! - `shared` never exceeds the previous key's length and is 0 for the first
//!   record. A record's key is `previous_key[..shared] + suffix`.
//! - Restart offsets strictly increase, each lands on a record boundary whose
//!   `shared` is 0, and the first is 0 whenever the block has records. Restart
//!   keys are therefore stored whole and can be binary searched in place.
//! - Keys, values and the block respect [`MAX_KEY_LEN`], [`MAX_VALUE_LEN`] and
//!   [`MAX_BLOCK_LEN`]. Records exactly fill the bytes before the directory.
//!
//! The restart spacing is a builder policy and is not part of the format, and
//! neither is maximal prefix sharing: readers accept any valid placement.

mod builder;
mod reader;
mod varint;

#[cfg(test)]
mod tests;

use std::cmp::Ordering;

pub use builder::{BlockBuilder, BuildError};
#[cfg(test)]
pub use reader::Block;
pub use reader::{BlockPosition, BlockView, CorruptBlock, Record, Version};
pub(crate) use reader::{Corruption, RecordWalk};

/// Number of records between restart points written by [`BlockBuilder`].
pub const RESTART_INTERVAL: usize = 16;

/// Maximum user key length in bytes.
pub const MAX_KEY_LEN: usize = 1 << 16;

/// Maximum value length in bytes.
pub const MAX_VALUE_LEN: usize = 1 << 26;

/// Maximum encoded block length in bytes.
///
/// This is a hard format ceiling, not a target block size. Keeping all limits
/// well below `u32::MAX` means offsets fit their encoding and sums of bounded
/// lengths cannot overflow, even with a 32-bit `usize`. Raising a limit later
/// does not invalidate existing blocks.
pub const MAX_BLOCK_LEN: usize = 1 << 27;

/// Width of one restart entry, and of the trailing restart count.
const U32_LEN: usize = size_of::<u32>();

/// Any record within the key and value limits fits in an empty block, so
/// [`BuildError::BlockFull`] always means "start a new block".
const _: () =
    assert!(MAX_KEY_LEN + MAX_VALUE_LEN + 4 * varint::MAX_LEN + 2 * U32_LEN <= MAX_BLOCK_LEN);

/// The payload of a record: either value bytes or a deletion marker.
///
/// An empty value is `Bytes(b"")`, which is distinct from `Tombstone`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Value<'a> {
    /// The record holds these value bytes, which may be empty.
    Bytes(&'a [u8]),
    /// The record marks its user key as deleted.
    Tombstone,
}

/// Compares internal keys: user key ascending, then sequence number
/// descending. This is the single definition of block order.
pub(crate) fn cmp_internal(a_key: &[u8], a_seq: u64, b_key: &[u8], b_seq: u64) -> Ordering {
    a_key.cmp(b_key).then(b_seq.cmp(&a_seq))
}

pub(crate) fn common_prefix_len(a: &[u8], b: &[u8]) -> usize {
    let len = a.len().min(b.len());
    let mut offset = 0;
    let (a_chunks, _) = a[..len].as_chunks::<8>();
    let (b_chunks, _) = b[..len].as_chunks::<8>();
    for (&ac, &bc) in a_chunks.iter().zip(b_chunks) {
        let diff = u64::from_le_bytes(ac) ^ u64::from_le_bytes(bc);
        if diff != 0 {
            return offset + (diff.trailing_zeros() / 8) as usize;
        }
        offset += 8;
    }
    while offset < len && a[offset] == b[offset] {
        offset += 1;
    }
    offset
}

#[cfg(test)]
mod prefix_tests {
    use super::common_prefix_len;

    #[test]
    fn common_prefix_len_matches_bytewise_across_word_boundaries() {
        let base: Vec<u8> = (0..33u8).collect();
        for len_a in 0..=base.len() {
            for len_b in 0..=base.len() {
                assert_eq!(
                    common_prefix_len(&base[..len_a], &base[..len_b]),
                    len_a.min(len_b)
                );
            }
            for diff_at in 0..len_a {
                let mut other = base[..len_a].to_vec();
                other[diff_at] ^= 0x80;
                assert_eq!(common_prefix_len(&base[..len_a], &other), diff_at);
            }
        }
    }
}
