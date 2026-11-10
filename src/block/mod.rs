//! Compact, immutable, sorted data blocks.
//!
//! A block stores versioned records ordered by user key ascending, then
//! sequence number descending. [`BlockBuilder`] encodes records supplied in
//! that order and [`Block`] validates the bytes once, after which lookups and
//! iteration cannot fail.
//!
//! ```
//! use tessera::block::{Block, BlockBuilder, Value};
//!
//! let mut builder = BlockBuilder::new();
//! builder.add(b"apple", 9, Value::Bytes(b"red"))?;
//! builder.add(b"apple", 7, Value::Tombstone)?;
//! builder.add(b"pear", 3, Value::Bytes(b""))?;
//! let block = Block::new(builder.finish())?;
//!
//! // Lookup returns the newest version, including tombstones.
//! let newest = block.get(b"apple").unwrap();
//! assert_eq!((newest.seq, newest.value), (9, Value::Bytes(b"red")));
//!
//! // A cursor visits every stored version.
//! let mut cursor = block.cursor();
//! while let Some(record) = cursor.next_record() {
//!     println!("{:?}@{} = {:?}", record.key, record.seq, record.value);
//! }
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! ```
//!
//! # Reading
//!
//! Full validation costs O(block length), so it is separated from access:
//!
//! - [`Block::new`] validates bytes of unknown origin once. Reads on the
//!   resulting [`Block`] cannot fail.
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
//! Invariants of a valid block, all checked by [`Block::new`]:
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

use std::cmp::Ordering;

pub use builder::{BlockBuilder, BuildError};
pub(crate) use reader::RecordWalk;
pub use reader::{
    Block, BlockPosition, BlockView, CorruptBlock, Corruption, Cursor, Record, Version,
};

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

fn common_prefix_len(a: &[u8], b: &[u8]) -> usize {
    a.iter().zip(b).take_while(|(x, y)| x == y).count()
}
