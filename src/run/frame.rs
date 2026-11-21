//! Frame headers, checksums and bounded structural checks. Headers bind the
//! codec, dictionary reference and decoded length to the stored payload;
//! checksums are verified before compressed data reaches the decoder.

use crc32fast::Hasher;

use crate::block::{CorruptBlock, MAX_BLOCK_LEN};

use super::codec::Codec;
use super::{CorruptRun, RunCorruption, read_u32};

/// Length of a frame header.
pub(super) const HEADER_LEN: usize = 12;

/// The header bytes covered by the checksum.
const CHECKSUMMED_LEN: usize = 8;

/// Builds the sealed header for `stored` with `codec`, `decoded_len` and `dictionary_ref`.
pub(super) fn header(
    codec: Codec,
    decoded_len: usize,
    dictionary_ref: u32,
    stored: &[u8],
) -> [u8; HEADER_LEN] {
    let mut header = [0u8; HEADER_LEN];
    header[0] = codec.to_byte();
    header[1..4].copy_from_slice(&dictionary_ref.to_le_bytes()[..3]);
    // Blocks are at most MAX_BLOCK_LEN, which fits in u32.
    header[4..CHECKSUMMED_LEN].copy_from_slice(&(decoded_len as u32).to_le_bytes());
    let checksum = checksum(&header[..CHECKSUMMED_LEN], stored);
    header[CHECKSUMMED_LEN..].copy_from_slice(&checksum.to_le_bytes());
    header
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

    /// Verifies the stored bytes against this frame's checksum and returns the
    /// header re-sealed under `dictionary_ref`, hashing `stored` only once.
    pub(super) fn verify_and_reseal(
        &self,
        dictionary_ref: u32,
    ) -> Result<[u8; HEADER_LEN], CorruptRun> {
        let expected = read_u32(self.header, 8);
        if self.dictionary_ref == dictionary_ref {
            if checksum(&self.header[..CHECKSUMMED_LEN], self.stored) != expected {
                return Err(self.error(RunCorruption::ChecksumMismatch));
            }
            return Ok(*self.header);
        }
        let mut payload = Hasher::new();
        payload.update(self.stored);
        let mut old = Hasher::new();
        old.update(&self.header[..CHECKSUMMED_LEN]);
        old.combine(&payload);
        if old.finalize() != expected {
            return Err(self.error(RunCorruption::ChecksumMismatch));
        }
        let mut header = [0u8; HEADER_LEN];
        header[0] = self.codec.to_byte();
        header[1..4].copy_from_slice(&dictionary_ref.to_le_bytes()[..3]);
        header[4..CHECKSUMMED_LEN].copy_from_slice(&(self.decoded_len as u32).to_le_bytes());
        let mut sealed = Hasher::new();
        sealed.update(&header[..CHECKSUMMED_LEN]);
        sealed.combine(&payload);
        header[CHECKSUMMED_LEN..].copy_from_slice(&sealed.finalize().to_le_bytes());
        Ok(header)
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn verify_and_reseal_matches_fresh_header_and_rejects_damage() {
        let payload = b"compressed block payload spanning multiple words for crc combine";
        let raw_len = 256usize;
        let orig = header(Codec::Zstd, raw_len, 3, payload);

        let frame = Frame::parse(0, &orig, payload).expect("valid frame");
        assert_eq!(
            frame.verify_and_reseal(3).expect("same dictionary ref"),
            orig
        );
        assert_eq!(
            frame.verify_and_reseal(17).expect("new dictionary ref"),
            header(Codec::Zstd, raw_len, 17, payload)
        );
        assert_eq!(
            frame.verify_and_reseal(0).expect("cleared dictionary ref"),
            header(Codec::Zstd, raw_len, 0, payload)
        );

        let mut damaged = payload.to_vec();
        damaged[5] ^= 1;
        let bad = Frame::parse(0, &orig, &damaged).expect("header still parses");
        assert_eq!(
            bad.verify_and_reseal(17).unwrap_err().kind,
            RunCorruption::ChecksumMismatch
        );
    }
}
