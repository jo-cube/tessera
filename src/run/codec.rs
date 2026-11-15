use zstd_safe::{CCtx, CDict, CParameter, DCtx};

/// Block compression used by a run.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum Codec {
    /// Read blocks directly from the run.
    None,
    /// Zstandard level 1 with a raw-content segment dictionary.
    #[default]
    Zstd,
}

pub(super) const ZSTD_LEVEL: i32 = 1;

/// Conservative allowance for prepared tables and their dictionary copy.
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
    #[cfg(test)]
    pub(super) fn context_bytes(&self) -> usize {
        self.zstd.as_ref().map_or(0, CCtx::sizeof)
    }
    pub(super) fn prepare_dictionary(&self, dictionary: &[u8]) -> Option<CDict<'static>> {
        self.zstd.as_ref().map(|_| CDict::create(dictionary, 3))
    }
    /// Cache a dictionary whose tables are sized for the estimated raw block.
    /// load_dictionary lazily constructs one CDict from these parameters on
    /// the first compress2 call; subsequent frames reuse it and the CCtx.
    pub(super) fn begin_segment(&mut self, dictionary: &[u8], block_len: usize) {
        if let Some(ctx) = &mut self.zstd {
            ctx.set_parameter(CParameter::CompressionLevel(ZSTD_LEVEL))
                .expect("supported level");
            ctx.set_parameter(CParameter::SrcSizeHint(block_len as u32))
                .expect("bounded block size hint");
            ctx.load_dictionary(dictionary)
                .expect("raw segment dictionary");
        }
    }

    pub(super) fn compress(
        &mut self,
        block: &[u8],
        dictionary: Option<&CDict<'_>>,
        out: &mut Vec<u8>,
    ) {
        let start = out.len();
        let bound = if self.codec == Codec::None {
            block.len()
        } else {
            zstd_safe::compress_bound(block.len())
        };
        out.resize(start + bound, 0);
        let output = &mut out[start..];
        let written = match &mut self.zstd {
            Some(ctx) => match dictionary {
                // Keep the level-3 ratio probe and therefore block cuts stable.
                Some(dictionary) => ctx.compress_using_cdict(output, block, dictionary),
                None => ctx.compress2(output, block),
            }
            .expect("codec worst-case output bound"),
            None => {
                output.copy_from_slice(block);
                block.len()
            }
        };
        out.truncate(start + written);
    }
}

pub(super) fn plausible_decoded_len(codec: Codec, stored: &[u8], decoded_len: usize) -> bool {
    match codec {
        Codec::None => stored.len() == decoded_len,
        Codec::Zstd => {
            matches!(zstd_safe::get_frame_content_size(stored), Ok(Some(size)) if size == decoded_len as u64)
        }
    }
}

/// One reusable context per reader, with no prepared dictionary or block cache.
#[derive(Default)]
pub(super) struct Decompressor {
    zstd: Option<DCtx<'static>>,
}
impl Decompressor {
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
                .get_or_insert_with(DCtx::create)
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
    fn prepared_dictionaries_fit_the_build_allowance_and_round_trip() {
        let dictionary: Vec<_> = (0..super::super::dictionary::DICTIONARY_LEN)
            .map(|i| (i.wrapping_mul(37) ^ (i >> 8)) as u8)
            .collect();
        let block: Vec<_> = dictionary.iter().copied().cycle().take(64 << 10).collect();
        let mut compressor = Compressor::new(Codec::Zstd);
        let mut decompressor = Decompressor::default();
        for len in [0, 1, 256, dictionary.len()] {
            let raw = &dictionary[..len];
            let prepared = compressor.prepare_dictionary(raw).unwrap();
            assert!(
                prepared.sizeof() <= CDICT_ALLOWANCE,
                "{} at {len}",
                prepared.sizeof()
            );
            // Reuse exactly the same prepared dictionary for multiple blocks.
            for input in [&block[..1024], block.as_slice()] {
                let mut stored = Vec::new();
                compressor.begin_segment(raw, input.len());
                compressor.compress(input, None, &mut stored);
                assert!(compressor.context_bytes() <= CDICT_ALLOWANCE + (2 << 20));
                let mut decoded = vec![0; input.len()];
                assert_eq!(
                    decompressor.decompress(Codec::Zstd, &stored, &mut decoded, raw),
                    Some(input.len())
                );
                assert_eq!(decoded, input);
            }
        }
        assert!(
            Compressor::new(Codec::None)
                .prepare_dictionary(&dictionary)
                .is_none()
        );
    }
}
