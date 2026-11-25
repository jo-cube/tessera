//! The run format and its readers: physical, merged, logical, seeking and
//! ranged reads.

mod compact;
mod dictionary;
mod format;
mod forward;
mod logical;
mod merge;
mod ranges;
mod seek;
mod sequences;

use crate::run::{Codec, Run};

/// The literal section types of a magicless Zstandard frame's compressed
/// blocks: 0 raw, 1 RLE, 2 Huffman-coded, 3 with the previous table.
fn literal_types(stored: &[u8]) -> Vec<u8> {
    let mut rest = &stored[2..];
    let mut types = Vec::new();
    loop {
        let header = u32::from_le_bytes([rest[0], rest[1], rest[2], 0]);
        let size = (header >> 3) as usize;
        let stored_len = match (header >> 1) & 3 {
            1 => 1,
            2 => {
                types.push(rest[3] & 3);
                size
            }
            _ => size,
        };
        rest = &rest[3 + stored_len..];
        if header & 1 != 0 {
            assert!(rest.is_empty());
            return types;
        }
    }
}
fn literal_counts(run: &Run<Vec<u8>>) -> [usize; 4] {
    let view = run.view();
    let mut counts = [0; 4];
    for block in 0..view.block_count() {
        let frame = view.frame(block).unwrap();
        assert_eq!(frame.codec, Codec::Zstd);
        for kind in literal_types(frame.stored) {
            counts[kind as usize] += 1;
        }
    }
    counts
}
