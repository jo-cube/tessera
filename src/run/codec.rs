use zstd_safe::{CCtx, CDict, CParameter, DCtx};

/// Block compression used by a run.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum Codec {
    /// Read blocks directly from the run.
    None,
    /// Adaptive Zstandard level 1 or 3 with a raw-content segment dictionary.
    #[default]
    Zstd,
}

#[cfg(feature = "compression-stats")]
static SEGMENTS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
#[cfg(feature = "compression-stats")]
static LEVEL3: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

#[cfg(feature = "compression-stats")]
pub(crate) fn compression_build_counts() -> (u64, u64) {
    use std::sync::atomic::Ordering;
    (
        SEGMENTS.load(Ordering::Relaxed),
        LEVEL3.load(Ordering::Relaxed),
    )
}

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
    #[cfg(test)]
    pub(super) selected_level: i32,
}

impl Compressor {
    pub(super) fn new(codec: Codec) -> Self {
        Self {
            codec,
            zstd: (codec == Codec::Zstd).then(CCtx::create),
            #[cfg(test)]
            selected_level: 1,
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
    pub(super) fn begin_segment(&mut self, dictionary: &[u8], block_len: usize, level: i32) {
        if let Some(ctx) = &mut self.zstd {
            ctx.set_parameter(CParameter::CompressionLevel(level))
                .expect("supported level");
            ctx.set_parameter(CParameter::SrcSizeHint(block_len as u32))
                .expect("bounded block size hint");
            ctx.load_dictionary(dictionary)
                .expect("raw segment dictionary");
        }
    }

    /// Compare the same sample using the dictionary parameters of real blocks.
    /// Leave the context configured for the chosen level; no level is persisted.
    pub(super) fn select_level(&mut self, sample: &[u8], dictionary: &[u8], prime: &[u8]) -> i32 {
        // The first frame prepares the cached dictionary. Give it the real
        // block length: SrcSizeHint alone loses to a known 64 KiB sample size.
        let block_len = prime.len();
        let mut compressed = Vec::new();
        self.begin_segment(dictionary, block_len, 1);
        self.compress(prime, None, &mut compressed);
        compressed.clear();
        self.compress(sample, None, &mut compressed);
        let level1 = compressed.len();
        self.begin_segment(dictionary, block_len, 3);
        compressed.clear();
        self.compress(prime, None, &mut compressed);
        compressed.clear();
        self.compress(sample, None, &mut compressed);
        let level = if compressed.len() * 100 <= level1 * 95 {
            3
        } else {
            1
        };
        if level == 1 {
            self.begin_segment(dictionary, block_len, 1);
        }
        #[cfg(test)]
        {
            self.selected_level = level;
        }
        #[cfg(feature = "compression-stats")]
        {
            use std::sync::atomic::Ordering;
            SEGMENTS.fetch_add(1, Ordering::Relaxed);
            LEVEL3.fetch_add(u64::from(level == 3), Ordering::Relaxed);
        }
        level
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
    fn redundant_segments_choose_three_and_random_segments_choose_one() {
        let mut rng = 12345u64;
        let mut random = || {
            rng ^= rng << 13;
            rng ^= rng >> 7;
            rng ^= rng << 17;
            rng as u8
        };
        let incompressible: Vec<u8> = (0..64 << 10).map(|_| random()).collect();
        let mut compressor = Compressor::new(Codec::Zstd);
        let mut redundant = Vec::new();
        for i in 0..1024 {
            let base: String = (0..180).map(|_| (b'a' + random() % 2) as char).collect();
            redundant.extend_from_slice(format!("{i}: {base} {base} {base}\n").as_bytes());
        }
        let sample = &redundant[redundant.len() / 2..redundant.len() / 2 + (64 << 10)];
        assert_eq!(
            compressor.select_level(sample, &redundant[..16 << 10], &redundant[..4096]),
            3
        );
        assert_eq!(
            compressor.select_level(
                &incompressible,
                &incompressible[..16 << 10],
                &incompressible[..4096]
            ),
            1
        );
        let mut stored = Vec::new();
        compressor.compress(&incompressible, None, &mut stored);
        let mut decoded = vec![0; incompressible.len()];
        assert_eq!(
            Decompressor::default().decompress(
                Codec::Zstd,
                &stored,
                &mut decoded,
                &incompressible[..16 << 10]
            ),
            Some(incompressible.len())
        );
        assert_eq!(decoded, incompressible);
    }

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
                compressor.begin_segment(raw, input.len(), 1);
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
