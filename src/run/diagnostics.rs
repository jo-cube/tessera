//! Relative Zstandard cost by level over blocks cut as the builder cuts them.

use std::time::{Duration, Instant};

use super::codec::{Codec, Compressor, Decompressor};

/// Compression and decompression time, and stored bytes, at one level.
#[derive(Clone, Copy, Debug)]
pub struct LevelCost {
    /// The Zstandard level.
    pub level: i32,
    /// Raw bytes compressed.
    pub raw: usize,
    /// Compressed bytes produced.
    pub stored: usize,
    /// Fastest of the rounds' compression time.
    pub compress: Duration,
    /// Fastest of the rounds' decompression time.
    pub decompress: Duration,
}

/// Compresses `blocks`, each with its segment dictionary, at each level,
/// beginning a segment when the dictionary changes, then decompresses them.
pub(crate) fn level_costs(
    blocks: &[(usize, Vec<u8>)],
    dictionaries: &[Vec<u8>],
    levels: &[i32],
    rounds: usize,
) -> Vec<LevelCost> {
    let mut costs: Vec<_> = levels
        .iter()
        .map(|&level| LevelCost {
            level,
            raw: blocks.iter().map(|(_, raw)| raw.len()).sum(),
            stored: 0,
            compress: Duration::MAX,
            decompress: Duration::MAX,
        })
        .collect();
    let mut frames = vec![Vec::new(); blocks.len()];
    let mut out = vec![0; super::RAW_BLOCK_CAP.max(1 << 17)];
    for _ in 0..rounds {
        for cost in &mut costs {
            let mut compressor = Compressor::new(Codec::Zstd);
            let started = Instant::now();
            let mut current = usize::MAX;
            for ((dictionary, raw), frame) in blocks.iter().zip(&mut frames) {
                if *dictionary != current {
                    compressor.begin_segment(&dictionaries[*dictionary], raw.len(), cost.level);
                    current = *dictionary;
                }
                frame.clear();
                compressor.compress(raw, None, frame);
            }
            cost.compress = cost.compress.min(started.elapsed());
            cost.stored = frames.iter().map(Vec::len).sum();
            let mut decompressor = Decompressor::default();
            let started = Instant::now();
            for ((dictionary, raw), frame) in blocks.iter().zip(&frames) {
                let len = decompressor
                    .decompress(
                        Codec::Zstd,
                        frame,
                        &mut out[..raw.len()],
                        &dictionaries[*dictionary],
                    )
                    .expect("own frame");
                assert_eq!(len, raw.len());
            }
            cost.decompress = cost.decompress.min(started.elapsed());
        }
    }
    costs
}
