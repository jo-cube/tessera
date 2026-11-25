//! Zstandard contexts and raw block encoding. Stored Zstandard frames omit
//! fields supplied by the run header; size checks bound decode allocations.
//! Contexts are reused, and raw dictionaries are borrowed for each decode.

use zstd_safe::{CCtx, CParameter, DCtx, DParameter, FrameFormat, ParamSwitch};

/// Block compression used by a run.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum Codec {
    /// Read blocks directly from the run.
    None,
    /// Zstandard level 1 with a raw-content segment dictionary.
    #[default]
    Zstd,
}

/// How a builder encodes new blocks. Frames record only the codec: blocks
/// with uncompressed literals are ordinary Zstandard frames.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Encoding {
    pub codec: Codec,
    /// Store Zstandard literals uncompressed, so that decoding builds no
    /// Huffman table, at the cost of larger frames.
    pub raw_literals: bool,
}

impl From<Codec> for Encoding {
    fn from(codec: Codec) -> Self {
        Self {
            codec,
            raw_literals: false,
        }
    }
}

pub(super) const ZSTD_LEVEL: i32 = 1;

/// Conservative allowance for a segment's prepared tables and their
/// dictionary copy, held by the compression context.
pub(super) const CDICT_ALLOWANCE: usize = 2 << 20;

impl Codec {
    pub(super) fn to_byte(self) -> u8 {
        match self {
            Self::None => 0,
            Self::Zstd => 1,
        }
    }
    pub(super) fn from_byte(byte: u8) -> Option<Self> {
        match byte {
            0 => Some(Self::None),
            1 => Some(Self::Zstd),
            _ => None,
        }
    }
}

pub(super) struct Compressor {
    codec: Codec,
    literals: ParamSwitch,
    zstd: Option<CCtx<'static>>,
}

impl Compressor {
    pub(super) fn new(encoding: impl Into<Encoding>) -> Self {
        let Encoding {
            codec,
            raw_literals,
        } = encoding.into();
        Self {
            codec,
            literals: if raw_literals {
                ParamSwitch::Disable
            } else {
                ParamSwitch::Auto
            },
            zstd: (codec == Codec::Zstd).then(|| {
                let mut ctx = CCtx::create();
                for parameter in [
                    CParameter::Format(FrameFormat::Magicless),
                    CParameter::ContentSizeFlag(false),
                    CParameter::ChecksumFlag(false),
                    CParameter::DictIdFlag(false),
                ] {
                    ctx.set_parameter(parameter)
                        .expect("supported frame parameter");
                }
                ctx
            }),
        }
    }
    pub(super) fn codec(&self) -> Codec {
        self.codec
    }
    #[cfg(test)]
    pub(super) fn context_bytes(&self) -> usize {
        self.zstd.as_ref().map_or(0, CCtx::sizeof)
    }
    /// Cache a dictionary whose tables are sized for the estimated raw block.
    /// load_dictionary lazily constructs one CDict from these parameters on
    /// the first compress2 call; subsequent frames reuse it and the CCtx.
    pub(super) fn begin_segment(&mut self, dictionary: &[u8], block_len: usize) {
        if let Some(ctx) = &mut self.zstd {
            set_level(ctx, self.literals, block_len);
            ctx.load_dictionary(dictionary)
                .expect("raw segment dictionary");
        }
    }

    /// Compressed length of `sample` at level 1 without a dictionary, with
    /// the builder's literal setting, for a ratio estimate before a segment's
    /// dictionary exists. The next [`begin_segment`](Self::begin_segment)
    /// replaces these settings.
    pub(super) fn probe(&mut self, sample: &[u8], out: &mut Vec<u8>) -> usize {
        if let Some(ctx) = &mut self.zstd {
            set_level(ctx, self.literals, sample.len());
            ctx.load_dictionary(&[]).expect("no dictionary");
        }
        out.clear();
        self.compress(sample, out);
        out.len()
    }

    pub(super) fn compress(&mut self, block: &[u8], out: &mut Vec<u8>) {
        out.clear();
        match &mut self.zstd {
            Some(ctx) => {
                out.reserve(zstd_safe::compress_bound(block.len()));
                ctx.compress2(out, block)
                    .expect("codec worst-case output bound");
            }
            None => out.extend_from_slice(block),
        }
    }
}

/// Level 1 with `literals`, its tables sized for `len` input bytes.
fn set_level(ctx: &mut CCtx<'static>, literals: ParamSwitch, len: usize) {
    for parameter in [
        CParameter::CompressionLevel(ZSTD_LEVEL),
        CParameter::LiteralCompressionMode(literals),
        CParameter::SrcSizeHint(len as u32),
    ] {
        ctx.set_parameter(parameter)
            .expect("supported level parameter");
    }
}

pub(super) fn plausible_decoded_len(codec: Codec, stored: &[u8], decoded_len: usize) -> bool {
    match codec {
        Codec::None => stored.len() == decoded_len,
        Codec::Zstd => magicless_decoded_bounds(stored)
            .is_some_and(|(min, max)| (min..=max).contains(&decoded_len)),
    }
}

/// Without a content-size field, bound allocation from the block headers.
/// Raw/RLE blocks declare their exact decoded size; compressed blocks decode
/// to at most 128 KiB. This inspects no compressed data. The Zstd decoder still
/// validates it and must produce exactly the outer frame's decoded length.
fn magicless_decoded_bounds(stored: &[u8]) -> Option<(usize, usize)> {
    // No content size, single-segment flag, checksum, dictionary ID or reserved
    // bits. The second byte is the window descriptor, checked by Zstd itself.
    if stored.first() != Some(&0) {
        return None;
    }
    let mut rest = stored.get(2..)?;
    let (mut min, mut max) = (0usize, 0usize);
    loop {
        let (header, tail) = rest.split_first_chunk::<3>()?;
        let header = u32::from_le_bytes([header[0], header[1], header[2], 0]);
        let size = (header >> 3) as usize;
        if size > 128 << 10 {
            return None;
        }
        let (stored_len, lower, upper) = match (header >> 1) & 3 {
            0 => (size, size, size),
            1 => (1, size, size),
            2 => (size, 0, 128 << 10),
            _ => return None,
        };
        rest = tail.get(stored_len..)?;
        min = min.checked_add(lower)?;
        max = max.checked_add(upper)?;
        if header & 1 != 0 {
            return rest.is_empty().then_some((min, max));
        }
    }
}

/// One reusable context per reader, with no prepared dictionary or block cache.
#[derive(Default)]
pub(super) struct Decompressor {
    zstd: Option<DCtx<'static>>,
}
impl Decompressor {
    #[cfg_attr(not(any(test, feature = "diagnostics")), allow(dead_code))]
    pub(super) fn context_bytes(&self) -> usize {
        self.zstd.as_ref().map_or(0, DCtx::sizeof)
    }
    pub(super) fn decompress(
        &mut self,
        codec: Codec,
        stored: &[u8],
        output: &mut [u8],
        dictionary: &[u8],
    ) -> Option<usize> {
        match codec {
            // ZSTD_decompress_usingDict inserts raw dictionary bytes by reference
            // for this call; it neither copies them nor constructs a DDict.
            Codec::Zstd => self
                .zstd
                .get_or_insert_with(|| {
                    let mut ctx = DCtx::create();
                    ctx.set_parameter(DParameter::Format(FrameFormat::Magicless))
                        .expect("supported magicless format");
                    ctx
                })
                .decompress_using_dict(output, stored, dictionary)
                .ok(),
            Codec::None => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn segment_contexts_fit_the_build_allowance_and_round_trip() {
        let dictionary: Vec<_> = (0..super::super::dictionary::DICTIONARY_LEN)
            .map(|i| (i.wrapping_mul(37) ^ (i >> 8)) as u8)
            .collect();
        let block: Vec<_> = dictionary.iter().copied().cycle().take(64 << 10).collect();
        let mut compressor = Compressor::new(Codec::Zstd);
        let mut decompressor = Decompressor::default();
        for len in [0, 1, 256, dictionary.len()] {
            let raw = &dictionary[..len];
            for input in [&block[..1024], block.as_slice()] {
                // A probe before the segment leaves no dictionary behind.
                let mut probe = Vec::new();
                assert!(compressor.probe(input, &mut probe) > 0);
                let mut decoded = vec![0; input.len()];
                assert_eq!(
                    decompressor.decompress(Codec::Zstd, &probe, &mut decoded, &[]),
                    Some(input.len())
                );
                // Reuse the same segment dictionary for several blocks.
                compressor.begin_segment(raw, input.len());
                for _ in 0..2 {
                    let mut stored = Vec::new();
                    compressor.compress(input, &mut stored);
                    assert!(compressor.context_bytes() <= CDICT_ALLOWANCE + (2 << 20));
                    assert_eq!(stored[0], 0, "no size, checksum or dictionary-ID fields");
                    assert!(plausible_decoded_len(Codec::Zstd, &stored, input.len()));
                    assert_eq!(
                        decompressor.decompress(Codec::Zstd, &stored, &mut decoded, raw),
                        Some(input.len())
                    );
                    assert_eq!(decoded, input);
                }
            }
        }
    }
}
