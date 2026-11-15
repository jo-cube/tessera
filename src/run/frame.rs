use crc32fast::Hasher;

use crate::block::{CorruptBlock, MAX_BLOCK_LEN};

use super::codec::Codec;
use super::{CorruptRun, RunCorruption, read_u32};

/// Length of a frame header.
pub(super) const HEADER_LEN: usize = 12;

/// The header bytes covered by the checksum.
const CHECKSUMMED_LEN: usize = 8;

/// Writes the header of `frame`, whose stored bytes follow a zeroed header.
pub(super) fn seal(frame: &mut [u8], codec: Codec, decoded_len: usize, dictionary_ref: u32) {
    let (header, stored) = frame.split_at_mut(HEADER_LEN);
    header[0] = codec.to_byte();
    header[1..4].copy_from_slice(&dictionary_ref.to_le_bytes()[..3]);
    // Blocks are at most MAX_BLOCK_LEN, which fits in u32.
    header[4..CHECKSUMMED_LEN].copy_from_slice(&(decoded_len as u32).to_le_bytes());
    let checksum = checksum(&header[..CHECKSUMMED_LEN], stored);
    header[CHECKSUMMED_LEN..].copy_from_slice(&checksum.to_le_bytes());
}

fn checksum(header: &[u8], stored: &[u8]) -> u32 {
    let mut hasher = Hasher::new();
    hasher.update(header);
    hasher.update(stored);
    hasher.finalize()
}

/// A frame whose header fields have passed their constant-time checks.
#[derive(Clone, Copy, Debug)]
pub(super) struct Frame<'a> {
    offset: usize,
    header: &'a [u8; HEADER_LEN],
    pub(super) codec: Codec,
    pub(super) decoded_len: usize,
    pub(super) dictionary_ref: u32,
    pub(super) dictionary: &'a [u8],
    pub(super) stored: &'a [u8],
}

impl<'a> Frame<'a> {
    /// Checks the header of the frame at `offset` in the run.
    pub(super) fn parse(
        offset: usize,
        header: &'a [u8; HEADER_LEN],
        stored: &'a [u8],
    ) -> Result<Self, CorruptRun> {
        let error = |kind| CorruptRun { offset, kind };

        let codec =
            Codec::from_byte(header[0]).ok_or_else(|| error(RunCorruption::UnsupportedCodec))?;
        let decoded_len = read_u32(header, 4) as usize;
        let raw_len_differs = codec == Codec::None && decoded_len != stored.len();
        let dictionary_ref = u32::from_le_bytes([header[1], header[2], header[3], 0]);
        if decoded_len > MAX_BLOCK_LEN
            || raw_len_differs
            || (codec == Codec::None && dictionary_ref != 0)
        {
            return Err(error(RunCorruption::InvalidFrame));
        }
        Ok(Self {
            offset,
            header,
            codec,
            decoded_len,
            stored,
            dictionary_ref,
            dictionary: &[],
        })
    }

    pub(super) fn verify_checksum(&self) -> Result<(), CorruptRun> {
        if checksum(&self.header[..CHECKSUMMED_LEN], self.stored) == read_u32(self.header, 8) {
            Ok(())
        } else {
            Err(self.error(RunCorruption::ChecksumMismatch))
        }
    }

    pub(super) fn error(&self, kind: RunCorruption) -> CorruptRun {
        CorruptRun {
            offset: self.offset,
            kind,
        }
    }

    /// Places a block error in the run. A position inside a compressed block
    /// has no meaning in the run's bytes, so it is reported at the frame.
    pub(super) fn block_error(&self, error: CorruptBlock) -> CorruptRun {
        let offset = if self.codec == Codec::None {
            self.offset + HEADER_LEN + error.offset
        } else {
            self.offset
        };
        CorruptRun {
            offset,
            kind: RunCorruption::Block(error.kind),
        }
    }
}
