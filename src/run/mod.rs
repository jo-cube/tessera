//! Self-contained, immutable sorted runs of independently compressed blocks.
//!
//! A run stores versioned records across many [`block`](crate::block)s, with a
//! sparse index holding each block's last user key. [`RunBuilder`] consumes
//! records in internal-key order and compresses each block as it is cut.
//!
//! ```
//! use tessera::block::Value;
//! use tessera::run::{Codec, ReadWorkspace, Run, RunBuilder};
//!
//! let mut builder = RunBuilder::new(Codec::Lz4);
//! builder.add(b"apple", 9, Value::Bytes(b"red"))?;
//! builder.add(b"apple", 7, Value::Tombstone)?;
//! builder.add(b"pear", 3, Value::Bytes(b"green"))?;
//! let run = builder.finish();
//!
//! // One workspace per reader, reused for every read.
//! let mut workspace = ReadWorkspace::new();
//!
//! // Lookup returns the newest version, including tombstones.
//! let newest = run.get(b"apple", &mut workspace)?.unwrap();
//! assert_eq!((newest.seq, newest.value), (9, Value::Bytes(b"red")));
//!
//! // A cursor visits every stored version, across block boundaries.
//! let mut cursor = run.cursor(&mut workspace);
//! while let Some(record) = cursor.next_record()? {
//!     println!("{:?}@{} = {:?}", record.key, record.seq, record.value);
//! }
//!
//! // Bytes of unknown origin are validated in full before they become a Run.
//! let reloaded = Run::new(run.as_bytes())?;
//! assert_eq!(reloaded.len(), 3);
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! ```
//!
//! # Reading and ownership
//!
//! Every read takes a [`ReadWorkspace`], which owns the decompression buffer.
//! Returned values borrow from the run for uncompressed blocks and from the
//! workspace otherwise, under one signature. The borrow checker therefore
//! rejects a second read through the workspace while a result is alive:
//!
//! ```compile_fail
//! # use tessera::block::Value;
//! # use tessera::run::{Codec, ReadWorkspace, RunBuilder};
//! # let run = RunBuilder::new(Codec::Lz4).finish();
//! let mut workspace = ReadWorkspace::new();
//! let first = run.get(b"a", &mut workspace).unwrap();
//! let second = run.get(b"b", &mut workspace).unwrap();
//! println!("{first:?} {second:?}");
//! ```
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
//! key_area := the concatenated last user key of each block
//!
//! footer   := index_offset:u64 record_count:u64 block_count:u32
//!             version:u32 magic:u32
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
//! `key_area[key_end[i - 1]..key_end[i]]`, with `key_end[-1]` taken as 0. The
//! footer is last, so a run can be written as a stream.
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
//! - `record_count` equals the total number of records.
//!
//! Where blocks are cut is a builder policy and is not part of the format.
//! Versions of one user key may span blocks. The first block whose last key is
//! at or above a target is therefore the only candidate for that target, and
//! contains its newest version. The codec is per frame in the format, and
//! [`RunBuilder`] uses one codec for a whole run.

mod builder;
mod codec;
mod frame;
mod reader;

use std::fmt;

use crate::block::Corruption;

pub use builder::RunBuilder;
pub use codec::Codec;
pub use reader::{ReadWorkspace, Run, RunCursor, RunView};

/// Encoded block length that [`RunBuilder::new`] aims for.
pub const TARGET_BLOCK_LEN: usize = 64 << 10;

/// Largest target accepted by [`RunBuilder::with_target_block_len`]. Staying
/// at a quarter of the block limit means a soft cut always happens before a
/// block can become full.
pub const MAX_TARGET_BLOCK_LEN: usize = crate::block::MAX_BLOCK_LEN / 4;

const MAGIC: u32 = u32::from_le_bytes(*b"TSRA");
const FORMAT_VERSION: u32 = 2;
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
