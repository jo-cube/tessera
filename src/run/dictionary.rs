//! Raw-content dictionaries sampled from a buffered segment; no training.
use crate::block::{BlockPosition, BlockView, Value};

pub(super) const SEGMENT_RAW_LEN: usize = 4 << 20;
pub(super) const DICTIONARY_LEN: usize = 16 << 10;
pub(super) const SAMPLE_LEN: usize = 64 << 10;

/// Concatenate independently encoded records at evenly spaced positions,
/// retaining the final 16 KiB, as in the compression spike.
pub(super) fn sample(bytes: &[u8], records: usize) -> Vec<u8> {
    let block = BlockView::open(bytes).expect("builder's segment");
    let mut cursor = BlockPosition::new();
    let mut out = Vec::with_capacity(DICTIONARY_LEN);
    let mut count = 1;
    let mut sample = 0;
    let mut at = 0;
    while let Some(record) = cursor.next_record(block).expect("builder's record") {
        let value = match record.value {
            Value::Bytes(v) => v,
            Value::Tombstone => &[],
        };
        if at == 0 {
            count = (DICTIONARY_LEN / (record.key.len() + value.len() + 4))
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
                    tail(&mut out, &[(n as u8) | 128]);
                    n >>= 7;
                }
                tail(&mut out, &[n as u8]);
            }
            tail(&mut out, record.key);
            tail(&mut out, value);
            sample += 1;
            if out.len() == DICTIONARY_LEN || records == 1 {
                return raw_content(out);
            }
        }
        at += 1;
    }
    raw_content(out)
}

fn tail(out: &mut Vec<u8>, bytes: &[u8]) {
    if bytes.len() >= DICTIONARY_LEN {
        out.clear();
        out.extend_from_slice(&bytes[bytes.len() - DICTIONARY_LEN..]);
    } else {
        let discard = (out.len() + bytes.len()).saturating_sub(DICTIONARY_LEN);
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
