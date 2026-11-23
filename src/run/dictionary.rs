//! Raw-content dictionaries sampled from a bounded segment without training.
//! Samples stay within the dictionary limit so every segment can be decoded
//! independently using dictionary bytes stored beside its frames.

use crate::block::{BlockPosition, BlockView, RESTART_INTERVAL, Record, Value};

pub(super) const SEGMENT_RAW_LEN: usize = 4 << 20;
pub(super) const DICTIONARY_LEN: usize = 16 << 10;
pub(super) const SAMPLE_LEN: usize = 64 << 10;
/// Stored frame bytes per sampled segment before closing its dictionary scope,
/// as a multiple of the dictionary length. Bounds dictionary overhead on
/// compressible data while leaving weakly compressible segments at one sampled
/// prefix.
pub(super) const SCOPE_STORED_MULTIPLE: usize = 16;
/// Maximum raw bytes a sampled segment and its streaming continuation may span,
/// as a multiple of the builder's segment length.
pub(super) const SCOPE_MAX_RAW_MULTIPLE: usize = 8;

/// Finished raw blocks of one segment, concatenated, with each block's end
/// and the number of records up to and including it.
pub(super) struct Segment<'a> {
    pub raw: &'a [u8],
    pub blocks: &'a [(u32, u32)],
}

impl Segment<'_> {
    fn records(&self) -> usize {
        self.blocks
            .last()
            .map_or(0, |&(_, records)| records as usize)
    }

    /// Calls `f` with the record at ordinal `at`, found from its block's
    /// restart directory, walking at most one restart interval.
    fn with_record<R>(
        &self,
        at: usize,
        position: &mut BlockPosition,
        f: impl FnOnce(Record<'_>) -> R,
    ) -> R {
        let block = self
            .blocks
            .partition_point(|&(_, records)| records as usize <= at);
        let (start, first) = match block.checked_sub(1) {
            Some(previous) => {
                let (end, records) = self.blocks[previous];
                (end as usize, records as usize)
            }
            None => (0, 0),
        };
        let end = self.blocks[block].0 as usize;
        let view = BlockView::open(&self.raw[start..end]).expect("builder's block");
        let local = at - first;
        position.seek_restart(view, local / RESTART_INTERVAL);
        for _ in 0..local % RESTART_INTERVAL {
            position.next_record(view).expect("builder's record");
        }
        f(position
            .next_record(view)
            .expect("builder's record")
            .expect("record within its block"))
    }
}

/// Concatenate independently encoded records at evenly spaced positions,
/// retaining the final 16 KiB. Only the sampled records are decoded.
pub(super) fn sample(segment: &Segment<'_>) -> Vec<u8> {
    let records = segment.records();
    let mut out = Vec::with_capacity(DICTIONARY_LEN);
    if records == 0 {
        return out;
    }
    let mut position = BlockPosition::new();
    let count = segment.with_record(0, &mut position, |record| {
        let value = match record.value {
            Value::Bytes(v) => v.len(),
            Value::Tombstone => 0,
        };
        (DICTIONARY_LEN / (record.key.len() + value + 4))
            .max(1)
            .min(records.saturating_sub(1).max(1))
    });
    for sample in 0..=count {
        segment.with_record(sample * (records - 1) / count, &mut position, |record| {
            append(&mut out, record);
        });
        if out.len() == DICTIONARY_LEN || records == 1 {
            break;
        }
    }
    raw_content(out)
}

fn append(out: &mut Vec<u8>, record: Record<'_>) {
    let (tag, value) = match record.value {
        Value::Bytes(v) => (v.len() as u64 + 1, v),
        Value::Tombstone => (0, &[][..]),
    };
    for mut n in [0, record.key.len() as u64, tag, record.seq] {
        while n >= 128 {
            tail(out, &[(n as u8) | 128]);
            n >>= 7;
        }
        tail(out, &[n as u8]);
    }
    tail(out, record.key);
    tail(out, value);
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::block::BlockBuilder;
    use crate::run::{Codec, ReadWorkspace, RunBuilder};

    /// Samples a segment encoded as one block, walking every record.
    fn whole_segment_sample(bytes: &[u8], records: usize) -> Vec<u8> {
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

    /// Each stored dictionary equals the sample of its segment's records
    /// encoded as one block and walked in full.
    #[test]
    fn dictionaries_from_buffered_blocks_match_whole_segment_samples() {
        type Records = Vec<(Vec<u8>, u64, Option<Vec<u8>>)>;
        let mut sets: Vec<Records> = Vec::new();
        // Shared prefixes, tombstones and several versions per key.
        sets.push(
            (0..100_000u32)
                .flat_map(|i| {
                    let key = format!("user:{:08}", i / 3).into_bytes();
                    let value = (i % 7 != 0).then(|| vec![(i % 251) as u8; (i % 97) as usize]);
                    std::iter::once((key, u64::from(u32::MAX - i), value))
                })
                .collect(),
        );
        // Records larger than a block, and fewer records than samples.
        sets.push(
            (0..200u32)
                .map(|i| (i.to_be_bytes().to_vec(), 1, Some(vec![i as u8; 40_000])))
                .collect(),
        );
        sets.push(vec![(b"only".to_vec(), 5, Some(b"value".to_vec()))]);
        sets.push(
            (0..3u32)
                .map(|i| (i.to_be_bytes().to_vec(), 0, None))
                .collect(),
        );
        for (n, records) in sets.iter().enumerate() {
            let mut builder = RunBuilder::new(Codec::Zstd);
            for (key, seq, value) in records {
                let value = value.as_deref().map_or(Value::Tombstone, Value::Bytes);
                builder.add(key, *seq, value).unwrap();
            }
            let run = builder.finish();
            let view = run.view();
            let mut workspace = ReadWorkspace::new();
            let mut segments = 0;
            let mut block = 0;
            while block < view.block_count() {
                let first = view.frame(block).unwrap();
                let mut whole = BlockBuilder::new();
                while block < view.block_count()
                    && view.frame(block).unwrap().dictionary_ref == first.dictionary_ref
                {
                    let frame = view.frame(block).unwrap();
                    let decoded = workspace.load(&frame).unwrap();
                    let opened = BlockView::open(decoded).unwrap();
                    let mut cursor = BlockPosition::new();
                    while let Some(r) = cursor.next_record(opened).unwrap() {
                        whole.append(r.key, r.seq, r.value).unwrap();
                    }
                    block += 1;
                }
                let count = whole.len();
                let mut bytes = Vec::new();
                whole.flush_into(&mut bytes);
                assert_eq!(
                    first.dictionary,
                    whole_segment_sample(&bytes, count).as_slice(),
                    "set {n}, segment {segments}"
                );
                segments += 1;
            }
            assert!(segments >= 1);
            if n == 0 {
                assert!(segments > 1, "the first set spans segments");
            }
        }
    }
}
