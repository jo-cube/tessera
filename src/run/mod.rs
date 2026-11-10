//! Self-contained, immutable sorted runs of data blocks.
//!
//! A run stores versioned records across many independently decodable
//! [`block`](crate::block)s, with a sparse index holding each block's last user
//! key. [`RunBuilder`] consumes records in internal-key order and returns a
//! [`Run`], whose lookups and iteration cannot fail.
//!
//! ```
//! use tessera::block::Value;
//! use tessera::run::{Run, RunBuilder};
//!
//! let mut builder = RunBuilder::new();
//! builder.add(b"apple", 9, Value::Bytes(b"red"))?;
//! builder.add(b"apple", 7, Value::Tombstone)?;
//! builder.add(b"pear", 3, Value::Bytes(b"green"))?;
//! let run = builder.finish();
//!
//! // Lookup returns the newest version, including tombstones.
//! assert_eq!(run.get(b"apple").unwrap().seq, 9);
//!
//! // A cursor visits every stored version, across block boundaries.
//! let mut cursor = run.cursor();
//! while let Some(record) = cursor.next_record() {
//!     println!("{:?}@{} = {:?}", record.key, record.seq, record.value);
//! }
//!
//! // Bytes of unknown origin are validated in full before they become a Run.
//! let reloaded = Run::new(run.as_bytes())?;
//! assert_eq!(reloaded.len(), 3);
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! ```
//!
//! # Reading
//!
//! Three levels of trust are kept apart:
//!
//! - [`RunBuilder::finish`] returns a [`Run`] that is valid by construction.
//! - [`Run::new`] validates bytes of unknown origin in O(run length). Reads on
//!   the resulting [`Run`] cannot fail.
//! - [`RunView::open`] checks only the footer, in O(1), and suits bytes already
//!   known to be valid. Its reads stay panic free and report what they touch,
//!   and its documentation states the trust contract.
//!
//! A point lookup binary searches the index and then reads one block, without
//! validating that block. [`RunPosition`] walks a run without borrowing it.
//!
//! # Format
//!
//! All integers are little-endian. A given record sequence and target block
//! length always encode to the same bytes. A run contains no process pointers.
//!
//! ```text
//! run      := frame* table key_area footer
//!
//! frame    := header block
//! header   := codec:u8 reserved:[u8; 3] decoded_len:u32 checksum:u32
//!
//! table    := entry*                 one per frame, in key order
//! entry    := frame_offset:u64 key_end:u32
//! key_area := the concatenated last user key of each block
//!
//! footer   := index_offset:u64 record_count:u64 block_count:u32
//!             version:u32 magic:u32
//! ```
//!
//! `block` uses the [block format](crate::block#format). A frame's stored
//! length is implied by the next frame's offset, or by `index_offset` for the
//! last frame. Block `i`'s last user key is `key_area[key_end[i - 1]..key_end[i]]`,
//! with `key_end[-1]` taken as 0. The footer is last, so a run can be written
//! as a stream.
//!
//! In version 1, `codec` is 0 (uncompressed), `reserved` and `checksum` are
//! zero and `decoded_len` equals the block length. The fields exist so that
//! compressed frames and integrity checks need no change to the layout. A
//! checksum could show bytes are unchanged, but not that they were valid when
//! written, so it would never replace [`Run::new`].
//!
//! Invariants of a valid run, all checked by [`Run::new`]:
//!
//! - The magic and version match, and the frames, table, key area and footer
//!   exactly cover the bytes. An empty run is a footer alone.
//! - Frames are contiguous and start at offset 0. Every frame holds a valid
//!   block with at least one record.
//! - Internal keys strictly increase across the entire run, including from the
//!   last record of one block to the first record of the next.
//! - Each block's index key equals the user key of its last record.
//! - `record_count` equals the total number of records.
//!
//! Where blocks are cut is a builder policy and is not part of the format.
//! Versions of one user key may span blocks. The first block whose last key is
//! at or above a target is therefore the only candidate for that target, and
//! contains its newest version.

mod builder;
mod reader;

use std::fmt;

use crate::block::Corruption;

pub use builder::RunBuilder;
pub use reader::{Run, RunCursor, RunPosition, RunView};

/// Encoded block length that [`RunBuilder::new`] aims for.
pub const TARGET_BLOCK_LEN: usize = 64 << 10;

/// Largest target accepted by [`RunBuilder::with_target_block_len`]. Staying
/// at a quarter of the block limit means a soft cut always happens before a
/// block can become full.
pub const MAX_TARGET_BLOCK_LEN: usize = crate::block::MAX_BLOCK_LEN / 4;

const MAGIC: u32 = u32::from_le_bytes(*b"TSRA");
const FORMAT_VERSION: u32 = 1;
const CODEC_RAW: u8 = 0;
const FRAME_HEADER_LEN: usize = 12;
const TABLE_ENTRY_LEN: usize = 12;
const FOOTER_LEN: usize = 28;

/// Reads a little-endian `u32` from a fixed-size structure.
fn read_u32<const N: usize>(bytes: &[u8; N], at: usize) -> u32 {
    u32::from_le_bytes([bytes[at], bytes[at + 1], bytes[at + 2], bytes[at + 3]])
}

/// Reads a little-endian `u64` from a fixed-size structure.
fn read_u64<const N: usize>(bytes: &[u8; N], at: usize) -> u64 {
    let low = u64::from(read_u32(bytes, at));
    let high = u64::from(read_u32(bytes, at + 4));
    low | (high << 32)
}

/// Why bytes were rejected by [`Run::new`] or a [`RunView`] read.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CorruptRun {
    /// Byte offset in the run of the offending structure.
    pub offset: usize,
    /// The invariant that was violated.
    pub kind: RunCorruption,
}

/// The kinds of structural corruption detected in a run.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RunCorruption {
    /// The bytes are too short to hold a footer.
    Truncated,
    /// The footer does not end with the run magic number.
    InvalidMagic,
    /// The footer declares a format version this build cannot read.
    UnsupportedVersion,
    /// The footer's regions do not exactly cover the bytes.
    InvalidFooter,
    /// A table entry, or the key it delimits, does not match the frames.
    InvalidIndex,
    /// A frame header is inconsistent with its frame.
    InvalidFrame,
    /// A frame uses a compression codec this build cannot read.
    UnsupportedCodec,
    /// A frame holds a block with no records.
    EmptyBlock,
    /// The footer's record count differs from the records stored.
    RecordCountMismatch,
    /// A block is malformed, or is out of order against the previous block.
    Block(Corruption),
}

impl fmt::Display for CorruptRun {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "corrupt run at offset {}: ", self.offset)?;
        match self.kind {
            RunCorruption::Truncated => f.write_str("too short for a footer"),
            RunCorruption::InvalidMagic => f.write_str("invalid magic number"),
            RunCorruption::UnsupportedVersion => f.write_str("unsupported format version"),
            RunCorruption::InvalidFooter => f.write_str("footer does not match the regions"),
            RunCorruption::InvalidIndex => f.write_str("index does not match the frames"),
            RunCorruption::InvalidFrame => f.write_str("inconsistent frame header"),
            RunCorruption::UnsupportedCodec => f.write_str("unsupported codec"),
            RunCorruption::EmptyBlock => f.write_str("block has no records"),
            RunCorruption::RecordCountMismatch => f.write_str("record count mismatch"),
            RunCorruption::Block(kind) => write!(f, "invalid block: {kind}"),
        }
    }
}

impl std::error::Error for CorruptRun {}
