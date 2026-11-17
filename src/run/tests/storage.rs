//! Packed and contiguous runs must agree on restart searches and decoding.
//! Round trips check byte identity, shared chunk retention and exact tails.

use super::*;
use crate::block::BlockBuilder;
use crate::run::RunBuilder;
use crate::run::dictionary::SEGMENT_RAW_LEN;

#[test]
fn restart_heads_search_equal_heads_and_short_zero_padded_keys() {
    let mut keys: Vec<Vec<u8>> = (0..=12).map(|len| vec![0; len]).collect();
    keys.extend((0..=255u8).map(|byte| vec![byte]));
    keys.extend((0..512u16).map(|i| [b"samehead".as_slice(), &i.to_be_bytes()].concat()));
    keys.sort();
    keys.dedup();
    for codec in [Codec::None, Codec::Zstd] {
        let mut builder = RunBuilder::with_raw_block_target(codec, 1);
        for key in &keys {
            builder.add(key, 1, Value::Bytes(b"v")).unwrap();
        }
        let built = builder.finish();
        assert_eq!(built.block_count(), keys.len());
        let bytes = built.as_bytes().into_owned();
        let (recovered, _) = Run::recover(bytes.clone()).unwrap();
        let wrapped = Run::new(bytes.as_slice()).unwrap();
        assert_eq!(built.backing.restart_heads, recovered.backing.restart_heads);
        assert_eq!(built.backing.restart_heads, wrapped.backing.restart_heads);
        assert_eq!(recovered.as_bytes(), bytes);
        assert_eq!(
            built.backing.restart_heads.len(),
            keys.len().div_ceil(RESTART_INTERVAL)
        );
        assert_eq!(
            built.index_bytes(),
            built.view().index_len() + built.backing.restart_heads.len() * 8
        );
        let raw = RunView::open(&bytes).unwrap();
        for target in keys.iter().cloned().chain(keys.iter().map(|key| {
            let mut absent = key.clone();
            absent.push(0);
            absent
        })) {
            let at = keys.partition_point(|key| key < &target);
            let expected = (at < keys.len()).then_some(at);
            assert_eq!(raw.locate(&target).unwrap(), expected);
            for run in [&built, &recovered] {
                assert_eq!(run.view().locate(&target).unwrap(), expected);
                assert_eq!(
                    run.get(&target, &mut ReadWorkspace::new())
                        .unwrap()
                        .is_some(),
                    keys.binary_search(&target).is_ok()
                );
            }
            assert_eq!(wrapped.view().locate(&target).unwrap(), expected);
        }
    }
}

#[test]
fn persisted_filters_cannot_hide_present_keys() {
    for codec in [Codec::None, Codec::Zstd] {
        let mut builder = RunBuilder::new(codec);
        builder.add(b"key", 1, Value::Bytes(b"value")).unwrap();
        let bytes = builder.finish().as_bytes().into_owned();
        let result = Run::recover_checkpoint_in(bytes.clone(), &[0; 64], &ChunkPool::new(0));
        assert_eq!(result.unwrap_err().kind, RunCorruption::InvalidFilter);
        // Extra Bloom bits and absent filters are safe.
        for filter in [&[][..], &[255; 64][..]] {
            let (run, _) =
                Run::recover_checkpoint_in(bytes.clone(), filter, &ChunkPool::new(0)).unwrap();
            assert!(
                run.get(b"key", &mut ReadWorkspace::new())
                    .unwrap()
                    .is_some()
            );
        }
    }
}

fn build(codec: Codec) -> Run<Vec<u8>> {
    let mut builder = RunBuilder::new(codec);
    for i in 0..9_000u32 {
        builder
            .add(
                &i.to_be_bytes(),
                u64::from(i) + 1,
                Value::Bytes(&vec![(i % 19) as u8; 1024]),
            )
            .unwrap();
    }
    builder.finish()
}
fn parts(run: &Run<Vec<u8>>) -> &Segmented {
    let RunData::Segmented(parts) = &run.backing.data else {
        panic!("expected segmented storage");
    };
    parts
}
#[test]
fn multiple_allocations_preserve_serialization_and_all_read_paths() {
    for codec in [Codec::None, Codec::Zstd] {
        let run = build(codec);
        assert_eq!(
            run.view().segments.len(),
            if codec == Codec::Zstd { 3 } else { 0 }
        );
        if codec == Codec::Zstd {
            assert_eq!(parts(&run).allocations.len(), 1);
        } else {
            assert!(parts(&run).allocations.len() >= 9);
        }
        let bytes = run.as_bytes().into_owned();
        assert_eq!(bytes.len(), run.stored_len());
        let (recovered, max) = Run::recover(bytes.clone()).unwrap();
        assert_eq!(max, 9_000);
        assert_eq!(recovered.view().block_count(), run.view().block_count());
        assert_eq!(recovered.as_bytes(), bytes);
        let mut workspace = ReadWorkspace::new();
        for candidate in [&run, &recovered] {
            let mut cursor = candidate.cursor(&mut workspace);
            for i in 0..9_000u32 {
                let record = cursor.next_record().unwrap().unwrap();
                assert_eq!(record.key, i.to_be_bytes());
                assert_eq!(record.value, Value::Bytes(&vec![(i % 19) as u8; 1024]));
            }
            assert!(cursor.next_record().unwrap().is_none());
            for i in [0u32, 4_000, 4_100, 8_000, 8_999] {
                let found = candidate
                    .get(&i.to_be_bytes(), &mut workspace)
                    .unwrap()
                    .unwrap();
                assert_eq!(found.seq, u64::from(i) + 1);
                assert_eq!(found.value, Value::Bytes(&vec![(i % 19) as u8; 1024]));
            }
        }
    }
}
#[test]
fn a_segment_exactly_at_the_raw_boundary_ends_before_the_next_record() {
    let value = vec![7; SEGMENT_RAW_LEN - 16];
    let mut block = BlockBuilder::new();
    block.add(b"a", 1, Value::Bytes(&value)).unwrap();
    assert_eq!(block.encoded_len(), SEGMENT_RAW_LEN);
    for codec in [Codec::None, Codec::Zstd] {
        let mut builder = RunBuilder::new(codec);
        builder.add(b"a", 1, Value::Bytes(&value)).unwrap();
        builder.add(b"b", 2, Value::Bytes(b"next")).unwrap();
        let run = builder.finish();
        assert_eq!(run.view().block_count(), 2);
        assert_eq!(run.view().frame(0).unwrap().decoded_len, SEGMENT_RAW_LEN);
        let mut workspace = ReadWorkspace::new();
        assert_eq!(
            run.get(b"a", &mut workspace).unwrap().unwrap().value,
            Value::Bytes(&value)
        );
        assert_eq!(
            run.get(b"b", &mut workspace).unwrap().unwrap().value,
            Value::Bytes(b"next")
        );
    }
}
#[test]
fn damage_in_one_segment_leaves_other_segments_readable() {
    let run = build(Codec::Zstd);
    let view = run.view();
    let block = view.block_count() / 2;
    let middle_start = frame_offset(view.table[block]);
    let frame = view.frame(block).unwrap();
    let mut workspace = ReadWorkspace::new();
    let decoded = workspace.load(&frame).unwrap();
    let mut position = BlockPosition::new();
    let key = position
        .next_record(BlockView::open(decoded).unwrap())
        .unwrap()
        .unwrap()
        .key
        .to_vec();
    let damaged = run.damaged_at(middle_start + frame::HEADER_LEN);
    assert_eq!(
        damaged.get(&key, &mut workspace).unwrap_err().kind,
        RunCorruption::ChecksumMismatch
    );
    for i in [0u32, 8_999] {
        assert!(
            damaged
                .get(&i.to_be_bytes(), &mut workspace)
                .unwrap()
                .is_some()
        );
    }
    let mut cursor = damaged.cursor(&mut workspace);
    assert!(cursor.seek(&key).is_err());
    cursor.seek(&8_999u32.to_be_bytes()).unwrap();
    assert_eq!(
        cursor.next_record().unwrap().unwrap().key,
        8_999u32.to_be_bytes()
    );
    assert_eq!(
        Run::new(damaged.as_bytes()).unwrap_err().kind,
        RunCorruption::ChecksumMismatch
    );
}

#[test]
fn every_frame_and_dictionary_stays_in_one_allocation_including_oversized_frames() {
    use crate::chunk::{CHUNK_BYTES, ChunkPool};
    for codec in [Codec::None, Codec::Zstd] {
        let pool = ChunkPool::new(4 * CHUNK_BYTES);
        let mut builder = RunBuilder::in_pool(codec, 1024, &pool);
        for i in 0..6000u32 {
            builder
                .add(&i.to_be_bytes(), i as u64 + 1, Value::Bytes(&[3; 1024]))
                .unwrap();
        }
        let big = vec![7; CHUNK_BYTES + 211];
        builder
            .add(&6000u32.to_be_bytes(), 6001, Value::Bytes(&big))
            .unwrap();
        let run = builder.finish();
        let storage = parts(&run);
        let contains = |bytes: &[u8]| {
            storage.allocations.iter().any(|chunk| {
                let offset = bytes
                    .as_ptr()
                    .addr()
                    .checked_sub(chunk.bytes().as_ptr().addr());
                offset.is_some_and(|offset| offset + bytes.len() <= chunk.bytes().len())
            })
        };
        let view = run.view();
        for block in 0..view.block_count() {
            let start = frame_offset(view.table[block]);
            let end = view
                .table
                .get(block + 1)
                .map_or(view.frames.len(), |&e| frame_offset(e));
            assert!(contains(view.frames.get(start..end).unwrap()));
        }
        for segment in 0..view.segments.len() {
            assert!(contains(view.dictionary((segment + 1) as u32).unwrap()));
        }
        if codec == Codec::None {
            assert!(
                storage
                    .allocations
                    .iter()
                    .any(|chunk| chunk.bytes().len() > CHUNK_BYTES)
            );
        }
        let mut workspace = ReadWorkspace::new();
        assert_eq!(
            run.get(&6000u32.to_be_bytes(), &mut workspace)
                .unwrap()
                .unwrap()
                .value,
            Value::Bytes(&big)
        );
        let bytes = run.as_bytes().into_owned();
        assert_eq!(
            storage.frames_len + storage.dictionaries_len + storage.tail.len(),
            bytes.len()
        );
        let (restored, _) = Run::recover_in(bytes.clone(), &pool).unwrap();
        assert_eq!(restored.as_bytes(), bytes);
    }
}

#[test]
fn bounded_views_cover_empty_single_binary_and_maximum_key_edges() {
    let max = vec![255; crate::block::MAX_KEY_LEN];
    let keys = [
        vec![],
        vec![0],
        vec![0, 0],
        vec![0, 255],
        vec![1],
        vec![255],
        max.clone(),
    ];
    for codec in [Codec::None, Codec::Zstd] {
        let mut builder = RunBuilder::with_raw_block_target(codec, 32);
        for key in &keys {
            for seq in (1..=3).rev() {
                builder.add(key, seq, Value::Bytes(b"v")).unwrap();
            }
        }
        let run = builder.finish();
        let mut ws = ReadWorkspace::new();
        assert!(
            run.bounded(Some(b"x"), Some(b"x"), &mut ws)
                .unwrap()
                .is_none()
        );
        assert!(
            run.bounded(Some(&max), Some(&max), &mut ws)
                .unwrap()
                .is_none()
        );
        assert!(
            run.bounded(Some(&[255, 255]), Some(&[255, 255, 0]), &mut ws)
                .unwrap()
                .is_none()
        );
        for (lo, hi) in [
            (None, None),
            (Some(&[][..]), Some(&[0][..])),
            (Some(&[0][..]), Some(&[0, 0][..])),
            (Some(&[0, 1][..]), Some(&[1][..])),
            (Some(max.as_slice()), None),
        ] {
            let bounded = run.bounded(lo, hi, &mut ws).unwrap().unwrap();
            assert!(Arc::ptr_eq(&run.backing, &bounded.backing));
            assert_eq!(run.as_bytes(), bounded.as_bytes());
            let expected: Vec<_> = keys
                .iter()
                .filter(|k| {
                    lo.is_none_or(|lo| k.as_slice() >= lo) && hi.is_none_or(|hi| k.as_slice() < hi)
                })
                .collect();
            assert_eq!(bounded.key_range().unwrap().min, expected[0]);
            assert_eq!(bounded.key_range().unwrap().max, *expected.last().unwrap());
            {
                let mut cursor = bounded.cursor(&mut ws);
                for key in &expected {
                    for seq in (1..=3).rev() {
                        let got = cursor.next_record().unwrap().unwrap();
                        assert_eq!(got.key, key.as_slice());
                        assert_eq!(got.seq, seq);
                    }
                }
                assert!(cursor.next_record().unwrap().is_none());
                cursor.seek(&[]).unwrap();
                assert_eq!(
                    cursor.next_record().unwrap().unwrap().key,
                    expected[0].as_slice()
                );
                cursor.seek_prefix(&[0]).unwrap();
                let mut prefixed = expected
                    .iter()
                    .filter(|k| k.starts_with(&[0]))
                    .flat_map(|k| std::iter::repeat_n(k.as_slice(), 3));
                while let Some(record) = cursor.next_record().unwrap() {
                    assert_eq!(record.key, prefixed.next().unwrap());
                }
                assert!(prefixed.next().is_none());
            }
            for key in &keys {
                let want = expected.contains(&key);
                assert_eq!(bounded.get(key, &mut ws).unwrap().is_some(), want);
                assert_eq!(bounded.view().get(key, &mut ws).unwrap().is_some(), want);
                assert_eq!(bounded.excludes(key, &mut None), !want);
            }
            let mut seen = 0;
            bounded
                .view()
                .get_many(
                    &keys,
                    &(0..keys.len()).collect::<Vec<_>>(),
                    &mut ws,
                    |index, version| {
                        assert_eq!(
                            version.is_some(),
                            expected.iter().any(|k| **k == keys[index])
                        );
                        seen += 1;
                    },
                )
                .unwrap();
            assert_eq!(seen, keys.len());
        }
        let mut invalid = run.clone();
        assert!(invalid.restore_slice(vec![0, 1], vec![1]).is_err());
        assert!(invalid.restore_slice(vec![1], vec![0]).is_err());
    }
}
