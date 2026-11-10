use lz4_flex::block as lz4;
use zstd_safe::{CCtx, DCtx};

/// How a run's blocks are stored.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Codec {
    /// Blocks are stored as they are and read in place.
    None,
    /// LZ4 block compression.
    Lz4,
    /// Zstandard compression at level 3, the library's own default.
    Zstd,
}

const ZSTD_LEVEL: i32 = 3;

/// No valid LZ4 block decodes to more than this many bytes per stored byte.
///
/// This is a property of the format, not a policy, so it cannot reject a valid
/// block. A sequence stores `3 + L + e` bytes at least (token, offset, `L`
/// literals, `e` match length extensions) and produces at most
/// `L + 19 + 255 * e` bytes, which is less than 255 times what it stores. A
/// final literal-only sequence produces less than it stores.
const LZ4_MAX_RATIO: usize = 255;

impl Codec {
    pub(super) fn to_byte(self) -> u8 {
        match self {
            Self::None => 0,
            Self::Lz4 => 1,
            Self::Zstd => 2,
        }
    }

    pub(super) fn from_byte(byte: u8) -> Option<Self> {
        match byte {
            0 => Some(Self::None),
            1 => Some(Self::Lz4),
            2 => Some(Self::Zstd),
            _ => None,
        }
    }
}

/// Reusable compression state for one builder.
pub(super) struct Compressor {
    codec: Codec,
    zstd: Option<CCtx<'static>>,
}

impl Compressor {
    pub(super) fn new(codec: Codec) -> Self {
        Self {
            codec,
            zstd: (codec == Codec::Zstd).then(CCtx::create),
        }
    }

    pub(super) fn codec(&self) -> Codec {
        self.codec
    }

    /// Appends the compressed form of `block` to `out`, with no intermediate
    /// buffer. `out` briefly holds the worst-case compressed length.
    ///
    /// Compression cannot fail here: the output space is the codec's own
    /// worst-case bound and the input length is within the block limit.
    pub(super) fn compress(&mut self, block: &[u8], out: &mut Vec<u8>) {
        const ROOM: &str = "output has the codec's worst-case length";

        let start = out.len();
        let bound = match self.codec {
            Codec::None => block.len(),
            Codec::Lz4 => lz4::get_maximum_output_size(block.len()),
            Codec::Zstd => zstd_safe::compress_bound(block.len()),
        };
        out.resize(start + bound, 0);
        let output = &mut out[start..];
        let written = match (self.codec, &mut self.zstd) {
            (Codec::Lz4, _) => lz4::compress_into(block, output).expect(ROOM),
            (Codec::Zstd, Some(zstd)) => zstd.compress(output, block, ZSTD_LEVEL).expect(ROOM),
            _ => {
                output.copy_from_slice(block);
                block.len()
            }
        };
        out.truncate(start + written);
    }
}

/// Returns whether `stored` can decode to `decoded_len` bytes. This costs
/// O(1) and runs before any buffer is sized from `decoded_len`, and it accepts
/// every length the codec can actually produce.
pub(super) fn plausible_decoded_len(codec: Codec, stored: &[u8], decoded_len: usize) -> bool {
    match codec {
        Codec::None => stored.len() == decoded_len,
        Codec::Lz4 => decoded_len <= stored.len().saturating_mul(LZ4_MAX_RATIO),
        Codec::Zstd => matches!(
            zstd_safe::get_frame_content_size(stored),
            Ok(Some(size)) if size == decoded_len as u64
        ),
    }
}

/// Reusable decompression state for one reader. The Zstandard context is
/// created on first use, so readers of other codecs do not pay for it.
#[derive(Default)]
pub(super) struct Decompressor {
    zstd: Option<DCtx<'static>>,
}

impl Decompressor {
    /// Decompresses `stored` into `output`, which bounds what can be written,
    /// and returns the decoded length. `None` means the bytes are corrupt.
    pub(super) fn decompress(
        &mut self,
        codec: Codec,
        stored: &[u8],
        output: &mut [u8],
    ) -> Option<usize> {
        match codec {
            Codec::None => None,
            Codec::Lz4 => lz4::decompress_into(stored, output).ok(),
            Codec::Zstd => self
                .zstd
                .get_or_insert_with(DCtx::create)
                .decompress(output, stored)
                .ok(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lz4_length_bound_is_the_format_limit() {
        let stored = [0; 4];
        assert!(plausible_decoded_len(Codec::Lz4, &stored, 4 * 255));
        assert!(!plausible_decoded_len(Codec::Lz4, &stored, 4 * 255 + 1));
    }
}
