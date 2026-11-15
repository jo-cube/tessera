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
//! All integers are little-endian. A given record sequence, codec, target
//! block length and dictionary always encode to the same bytes. A run
//! contains no process pointers.
//!
//! Version 5 stores independently decodable blocks followed by raw-content
//! segment dictionaries, a segment table, the sparse block index, keys and footer.
//!
//! ```text
//! frame := codec:u8 dictionary_ref:u24 decoded_len:u32 checksum:u32 stored
//! segment := dictionary_offset:u64 dictionary_len:u32 dictionary_crc:u32
//! entry := frame_offset:u64 key_end:u32
//! footer := index_offset:u64 record_count:u64 block_count:u32 min_key_len:u32
//!           dictionaries_offset:u64 segment_count:u32 reserved:u32
//!           version:u32 magic:u32
//! ```
//!
//! Codec 0 is uncompressed and has reference 0. Codec 1 is Zstandard and
//! references segment `dictionary_ref - 1`. The segment table immediately
//! precedes the block index; dictionary regions tile the bytes between the
//! frames and that table. Dictionaries contain at most 16 KiB of raw content.
//! The frame CRC covers its first eight header bytes and stored payload.
//! Dictionary CRCs are checked before decoding. No prepared dictionaries are held.
//!
//! Validation checks all regions, checksums, record ordering, index keys,
//! the minimum key and record count. Empty runs contain only the footer.
//! Internal keys increase across blocks; versions of one key may span blocks.
//! A lookup chooses the first block whose last key is at least the target.

mod builder;
mod codec;
mod compact;
mod dictionary;
mod filter;
mod frame;
mod group;
mod logical;
mod merge;
mod reader;
mod storage;

#[cfg(test)]
mod tests;

use std::fmt;

use crate::block::Corruption;

pub use builder::RunBuilder;
pub use codec::Codec;
#[cfg(test)]
pub(crate) use compact::compact_parallel;
pub(crate) use compact::compact_parallel_in;
#[cfg(test)]
pub(crate) use compact::compact_with;
pub use compact::{CompactLimits, CompactStats, Compacted, OutputCuts, workspace_estimate};
pub(crate) use group::RunList;
#[cfg(test)]
pub(crate) use logical::latest_in;
pub use logical::{
    ConflictingCopies, LiveRecord, LiveValue, LogicalCursor, LogicalWorkspace, ReadError,
};
pub(crate) use logical::{LookupSource, latest_from};
pub use merge::{MergeCursor, MergeWorkspace};
pub use reader::{Run, RunView};

// Used beyond this module only by its tests.
#[cfg(test)]
pub use compact::{CONTEXT_ALLOWANCE, compact};
#[cfg(test)]
pub use logical::RunSet;
pub(crate) use reader::ReadWorkspace;
#[cfg(test)]
pub use reader::{KeyRange, RunCursor};

/// Default estimated compressed block target.
pub const TARGET_BLOCK_COMPRESSED: usize = 1024;
/// Fixed raw target for uncompressed runs.
pub const TARGET_BLOCK_LEN: usize = 16 << 10;
/// Raw block cap, except for a single oversized record.
pub const RAW_BLOCK_CAP: usize = 256 << 10;

/// The largest input per output run accepted when flushing or compacting. It
/// keeps every run far enough inside the format's 32-bit index limits that
/// building one cannot fail.
pub const MAX_TARGET_RUN_LEN: usize = 1 << 30;

/// Input counted per record beyond its key and value when splitting output
/// into runs, so that the number of records in a run is bounded as well.
pub(crate) const RECORD_INPUT_OVERHEAD: usize = 16;

const MAGIC: u32 = u32::from_le_bytes(*b"TSRA");
const FORMAT_VERSION: u32 = 5;
const TABLE_ENTRY_LEN: usize = 12;
const SEGMENT_ENTRY_LEN: usize = 16;
const FOOTER_LEN: usize = 48;

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
    /// A frame's stored bytes or header fields, or the dictionary's bytes, do
    /// not match their checksum.
    ChecksumMismatch,
    /// A raw dictionary region or reference is invalid.
    InvalidDictionary,
    /// A frame needs a dictionary and the run stores none.
    MissingDictionary,
    /// A frame's stored bytes do not decompress to its declared length.
    CorruptPayload,
    /// A persisted Bloom filter has an invalid length.
    InvalidFilter,
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
            RunCorruption::ChecksumMismatch => f.write_str("checksum mismatch"),
            RunCorruption::InvalidDictionary => f.write_str("invalid dictionary"),
            RunCorruption::MissingDictionary => f.write_str("frame needs a missing dictionary"),
            RunCorruption::CorruptPayload => f.write_str("corrupt compressed payload"),
            RunCorruption::InvalidFilter => f.write_str("invalid persisted filter"),
            RunCorruption::EmptyBlock => f.write_str("block has no records"),
            RunCorruption::RecordCountMismatch => f.write_str("record count mismatch"),
            RunCorruption::Block(kind) => write!(f, "invalid block: {kind}"),
        }
    }
}

impl std::error::Error for CorruptRun {}
