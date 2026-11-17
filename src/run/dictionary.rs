//! Raw-content dictionaries sampled from a bounded segment without training.
//! Samples stay within the dictionary limit so every segment can be decoded
//! independently using dictionary bytes stored beside its frames.

use crate::block::{BlockPosition, BlockView, Value};

pub(super) const SEGMENT_RAW_LEN: usize = 4 << 20;
/// The length builders sample. Even highly compressible segments store
/// several times this in frames, and longer samples gain nothing on data
/// with weak locality.
pub(super) const DICTIONARY_LEN: usize = 4 << 10;
/// The longest dictionary a run may store, which readers accept.
#[cfg(not(feature = "diagnostics"))]
pub(super) const MAX_DICTIONARY_LEN: usize = 16 << 10;
#[cfg(feature = "diagnostics")]
pub(super) const MAX_DICTIONARY_LEN: usize = 64 << 10;
pub(super) const SAMPLE_LEN: usize = 64 << 10;

/// Concatenate independently encoded records at evenly spaced positions,
/// retaining the final `len` bytes, at most [`MAX_DICTIONARY_LEN`].
pub(super) fn sample(bytes: &[u8], records: usize, len: usize) -> Vec<u8> {
    let len = len.clamp(1, MAX_DICTIONARY_LEN);
    let block = BlockView::open(bytes).expect("builder's segment");
    let mut cursor = BlockPosition::new();
    let mut out = Vec::with_capacity(len);
    let mut count = 1;
    let mut sample = 0;
    let mut at = 0;
    while let Some(record) = cursor.next_record(block).expect("builder's record") {
        let value = match record.value {
            Value::Bytes(v) => v,
            Value::Tombstone => &[],
        };
        if at == 0 {
            count = (len / (record.key.len() + value.len() + 4))
                .max(1)
                .min(records.saturating_sub(1).max(1));
        }
        while sample <= count && at == sample * (records - 1) / count {
            let tag = match record.value {
                Value::Bytes(v) => v.len() as u64 + 1,
                Value::Tombstone => 0,
            };
            for mut n in [0, record.key.len() as u64, tag, record.seq] {
                while n >= 128 {
                    tail(&mut out, &[(n as u8) | 128], len);
                    n >>= 7;
                }
                tail(&mut out, &[n as u8], len);
            }
            tail(&mut out, record.key, len);
            tail(&mut out, value, len);
            sample += 1;
            if out.len() == len || records == 1 {
                return raw_content(out);
            }
        }
        at += 1;
    }
    raw_content(out)
}

fn tail(out: &mut Vec<u8>, bytes: &[u8], len: usize) {
    if bytes.len() >= len {
        out.clear();
        out.extend_from_slice(&bytes[bytes.len() - len..]);
    } else {
        let discard = (out.len() + bytes.len()).saturating_sub(len);
        out.drain(..discard);
        out.extend_from_slice(bytes);
    }
}

// The stable safe zstd API auto-detects trained-dictionary magic. Preserve
// raw-content interpretation for arbitrary sampled values by retaining a
// one-byte-shorter suffix in this exceptional case.
fn raw_content(mut bytes: Vec<u8>) -> Vec<u8> {
    if bytes.starts_with(&[0x37, 0xa4, 0x30, 0xec]) {
        bytes.remove(0);
    }
    bytes
}

/// How long a segment's dictionary is.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Length {
    Fixed(usize),
    /// About 1/32 of the segment's estimated compressed size, from 2 to 64 KiB.
    #[cfg_attr(not(feature = "diagnostics"), allow(dead_code))]
    Adaptive,
}

/// The adaptive length for a segment of `raw` bytes whose ratio probe
/// compressed `probe_raw` bytes into `probe_stored`.
#[cfg_attr(not(feature = "diagnostics"), allow(dead_code))]
pub(super) fn adaptive_len(raw: usize, probe_raw: usize, probe_stored: usize) -> usize {
    let estimate = (raw as u128 * probe_stored as u128 / probe_raw.max(1) as u128) as usize;
    (estimate / 32)
        .next_power_of_two()
        .clamp(2 << 10, MAX_DICTIONARY_LEN.max(2 << 10))
}
