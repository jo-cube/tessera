//! Self-contained, immutable sorted runs of independently compressed blocks.
//!
//! A run stores versioned records across many [`block`](crate::block)s, with a
//! sparse index holding each block's last user key. [`RunBuilder`] consumes
//! records in internal-key order and compresses each block as it is cut.
//!
//! # Reading and ownership
//!
//! Every read takes a [`ReadWorkspace`], which owns the decompression buffer.
//! Returned values borrow from the run for uncompressed blocks and from the
//! workspace otherwise, under one signature. The borrow checker therefore
//! rejects a second read through the workspace while a result is alive.
//!
//! # Trust
//!
//! Four concerns are kept apart:
//!
//! - **Structural validation** is [`Run::new`], for bytes of unknown origin.
//!   It verifies every checksum, decompresses every block and walks every
//!   record, using memory for one decoded block at a time.
//! - **Trusted runs** come from [`RunBuilder::finish`], which is not walked
//!   again. Reads of any [`Run`] never scan it.
//! - **Integrity** is a CRC-32 over each frame's header fields and stored
//!   bytes. Decompressing a frame verifies it first, at a small cost relative
//!   to decompression. An uncompressed frame is read in place, where a checksum
//!   would turn a lookup into a read of the whole block, so only [`Run::new`]
//!   verifies it. A checksum shows bytes are unchanged. It cannot show that
//!   whoever wrote them produced a valid run.
//! - **Checked decoding** is what [`RunView`] reads do on every path: they are
//!   panic free and report whatever malformed structure they touch, through
//!   [`CorruptRun`], whether or not the run was validated. Memory corruption
//!   of a validated compressed run is a reportable event, so reads on [`Run`]
//!   are fallible too.
//!
//! A point lookup binary searches the index and then decodes one frame.
//!
//! # Format
//!
//! All integers are little-endian. A given record sequence, codec and target
//! block length always encode to the same bytes. A run contains no process
//! pointers.
//!
//! ```text
//! run      := frame* table key_area footer
//!
//! frame    := header stored
//! header   := codec:u8 reserved:[u8; 3] decoded_len:u32 checksum:u32
//!
//! table    := entry*                 one per frame, in key order
//! entry    := frame_offset:u64 key_end:u32
//! key_area := min_key last_key*      min_key_len bytes, then one per block
//!
//! footer   := index_offset:u64 record_count:u64 block_count:u32
//!             min_key_len:u32 version:u32 magic:u32
//! ```
//!
//! `stored` decodes to one block in the [block format](crate::block#format)
//! with no reference to any other frame. `codec` is 0 for uncompressed, 1 for
//! an LZ4 block and 2 for a Zstandard frame that declares its content size.
//! `decoded_len` is the block's exact length and at most
//! [`MAX_BLOCK_LEN`](crate::block::MAX_BLOCK_LEN). `reserved` is zero.
//! `checksum` is the CRC-32 (IEEE) of the header's first 8 bytes followed by
//! `stored`.
//!
//! A frame's stored length is implied by the next frame's offset, or by
//! `index_offset` for the last frame. Block `i`'s last user key is
//! `key_area[key_end[i - 1]..key_end[i]]`, with `key_end[-1]` taken as
//! `min_key_len`. The run's smallest user key is `key_area[..min_key_len]` and
//! its largest is the last block's key, so a run's key range is read without
//! decoding anything. The footer is last, so a run can be written as a stream.
//!
//! Invariants of a valid run, all checked by [`Run::new`]:
//!
//! - The magic and version match, and the frames, table, key area and footer
//!   exactly cover the bytes. An empty run is a footer alone.
//! - Frames are contiguous and start at offset 0. Every frame has a correct
//!   checksum and decodes to exactly `decoded_len` bytes, which form a valid
//!   block with at least one record.
//! - Internal keys strictly increase across the entire run, including from the
//!   last record of one block to the first record of the next.
//! - Each block's index key equals the user key of its last record.
//! - `min_key` equals the user key of the first record, and an empty run has
//!   `min_key_len` zero.
//! - `record_count` equals the total number of records.
//!
//! Where blocks are cut is a builder policy and is not part of the format.
//! Versions of one user key may span blocks. The first block whose last key is
//! at or above a target is therefore the only candidate for that target, and
//! contains its newest version. The codec is per frame in the format, and
//! [`RunBuilder`] uses one codec for a whole run.

mod builder;
mod codec;
mod compact;
mod filter;
mod frame;
mod logical;
mod merge;
mod reader;

#[cfg(test)]
mod tests;

use std::fmt;

use crate::block::Corruption;

pub use builder::RunBuilder;
pub use codec::Codec;
pub use compact::{
    CompactError, CompactLimits, CompactStats, Compacted, MAX_ENGINE_BLOCK_LEN, compact,
};
pub(crate) use logical::latest_in;
pub use logical::{
    ConflictingCopies, LiveRecord, LiveValue, LogicalCursor, LogicalWorkspace, ReadError,
};
pub use merge::{MergeCursor, MergeWorkspace};
pub use reader::{Run, RunView};

// Used beyond this module only by its tests.
#[cfg(test)]
pub use compact::CONTEXT_ALLOWANCE;
#[cfg(test)]
pub use logical::RunSet;
#[cfg(test)]
pub use reader::{KeyRange, ReadWorkspace, RunCursor};

/// Encoded block length that [`RunBuilder::new`] aims for. Provisional: see
/// `docs/benchmarks.md` for the measurements behind it.
pub const TARGET_BLOCK_LEN: usize = 16 << 10;

/// Largest target accepted by [`RunBuilder::with_target_block_len`]. Staying
/// at a quarter of the block limit means a soft cut always happens before a
/// block can become full.
pub const MAX_TARGET_BLOCK_LEN: usize = crate::block::MAX_BLOCK_LEN / 4;

/// The largest input per output run accepted when flushing or compacting. It
/// keeps every run far enough inside the format's 32-bit index limits that
/// building one cannot fail.
pub const MAX_TARGET_RUN_LEN: usize = 1 << 30;

/// Input counted per record beyond its key and value when splitting output
/// into runs, so that the number of records in a run is bounded as well.
pub(crate) const RECORD_INPUT_OVERHEAD: usize = 16;

const MAGIC: u32 = u32::from_le_bytes(*b"TSRA");
const FORMAT_VERSION: u32 = 3;
const TABLE_ENTRY_LEN: usize = 12;
const FOOTER_LEN: usize = 32;

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

/// Why a run's bytes were rejected as damaged: on loading a checkpoint,
/// which validates every run in full, or while reading.
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
    /// The stored minimum key does not match the run's records.
    InvalidKeyRange,
    /// A frame header is inconsistent with its frame.
    InvalidFrame,
    /// A frame uses a compression codec this build cannot read.
    UnsupportedCodec,
    /// A frame's stored bytes or header fields do not match its checksum.
    ChecksumMismatch,
    /// A frame's stored bytes do not decompress to its declared length.
    CorruptPayload,
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
            RunCorruption::InvalidKeyRange => f.write_str("minimum key does not match the records"),
            RunCorruption::InvalidFrame => f.write_str("inconsistent frame header"),
            RunCorruption::UnsupportedCodec => f.write_str("unsupported codec"),
            RunCorruption::ChecksumMismatch => f.write_str("frame checksum mismatch"),
            RunCorruption::CorruptPayload => f.write_str("corrupt compressed payload"),
            RunCorruption::EmptyBlock => f.write_str("block has no records"),
            RunCorruption::RecordCountMismatch => f.write_str("record count mismatch"),
            RunCorruption::Block(kind) => write!(f, "invalid block: {kind}"),
        }
    }
}

impl std::error::Error for CorruptRun {}
