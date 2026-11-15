//! Segment dictionaries, adaptive cuts and checked direct-load decoding.
use crate::block::Value;
use crate::run::{Codec, RAW_BLOCK_CAP, ReadWorkspace, Run, RunBuilder, RunCorruption};

fn u32_at(bytes: &[u8], at: usize) -> usize {
    u32::from_le_bytes(bytes[at..at + 4].try_into().unwrap()) as usize
}
fn u64_at(bytes: &[u8], at: usize) -> usize {
    u64::from_le_bytes(bytes[at..at + 8].try_into().unwrap()) as usize
}
fn segments(bytes: &[u8]) -> (usize, usize, usize) {
    let footer = bytes.len() - 48;
    let count = u32_at(bytes, footer + 32);
    (
        u64_at(bytes, footer + 24),
        u64_at(bytes, footer) - count * 16,
        count,
    )
}
fn build(codec: Codec, target: usize, count: u32, size: usize) -> Run<Vec<u8>> {
    let mut b = RunBuilder::with_target_block_compressed(codec, target);
    for i in 0..count {
        b.add(
            &i.to_be_bytes(),
            u64::from(i) + 1,
            Value::Bytes(&vec![(i % 19) as u8; size]),
        )
        .unwrap();
    }
    b.finish()
}
fn check<B: AsRef<[u8]>>(run: &Run<B>, count: u32, size: usize) {
    let mut w = ReadWorkspace::new();
    for i in 0..count {
        let v = run.get(&i.to_be_bytes(), &mut w).unwrap().unwrap();
        assert_eq!(v.seq, u64::from(i) + 1);
        assert_eq!(v.value, Value::Bytes(&vec![(i % 19) as u8; size]));
    }
    let view = run.view();
    let mut c = view.cursor(&mut w);
    let mut n = 0u32;
    while let Some(r) = c.next_record().unwrap() {
        assert_eq!(r.key, n.to_be_bytes());
        n += 1u32;
    }
    assert_eq!(n, count);
}
#[test]
fn segment_boundaries_dictionaries_and_references_round_trip() {
    let run = build(Codec::Zstd, 1024, 9_000, 1024);
    let bytes = run.as_bytes();
    let (mut next, table, count) = segments(&bytes);
    assert_eq!(count, 3);
    for i in 0..count {
        let at = table + i * 16;
        assert_eq!(u64_at(&bytes, at), next);
        let len = u32_at(&bytes, at + 8);
        assert_eq!(len, 16 << 10);
        assert_eq!(
            crc32fast::hash(&bytes[next..next + len]) as usize,
            u32_at(&bytes, at + 12)
        );
        next += len;
    }
    assert_eq!(next, table);
    let view = run.view();
    let mut previous = 1;
    for i in 0..view.block_count() {
        let f = view.frame(i).unwrap();
        assert!(f.dictionary_ref == previous || f.dictionary_ref == previous + 1);
        previous = f.dictionary_ref;
        assert_eq!(f.dictionary.len(), 16 << 10);
    }
    assert_eq!(previous, count as u32);
    assert!(run.held_bytes() > run.filter_bytes());
    check(&run, 9_000, 1024);
    check(&Run::new(bytes).unwrap(), 9_000, 1024);
}
#[test]
fn adaptive_blocks_reach_but_do_not_exceed_the_raw_cap() {
    let run = build(Codec::Zstd, RAW_BLOCK_CAP, 300, 4096);
    let lengths: Vec<_> = (0..run.block_count())
        .map(|i| run.view().frame(i).unwrap().decoded_len)
        .collect();
    assert!(lengths.iter().all(|&n| n <= RAW_BLOCK_CAP));
    assert!(lengths.iter().any(|&n| n > RAW_BLOCK_CAP - 8192));
    check(&run, 300, 4096);
    // Four-byte keys, four varints (six bytes), and an eight-byte trailer.
    let exact = build(Codec::Zstd, RAW_BLOCK_CAP, 2, RAW_BLOCK_CAP - 18);
    assert_eq!(exact.block_count(), 2);
    for i in 0..2 {
        assert_eq!(exact.view().frame(i).unwrap().decoded_len, RAW_BLOCK_CAP);
    }
    Run::new(exact.as_bytes()).unwrap();
}
#[test]
fn an_oversized_record_has_its_own_block() {
    let mut b = RunBuilder::default();
    b.add(b"a", 3, Value::Bytes(b"small")).unwrap();
    let large = vec![7; RAW_BLOCK_CAP + 1000];
    b.add(b"b", 2, Value::Bytes(&large)).unwrap();
    b.add(b"c", 1, Value::Tombstone).unwrap();
    let run = b.finish();
    assert_eq!(run.block_count(), 3);
    let mut w = ReadWorkspace::new();
    let f = run.view().frame(1).unwrap();
    assert!(f.decoded_len > RAW_BLOCK_CAP);
    let block = crate::block::Block::new(w.load(&f).unwrap()).unwrap();
    assert_eq!(block.len(), 1);
    assert_eq!(
        run.get(b"b", &mut w).unwrap().unwrap().value,
        Value::Bytes(&large)
    );
    Run::new(run.as_bytes()).unwrap();
}
#[test]
fn corrupt_dictionary_regions_are_rejected_before_decode() {
    let run = build(Codec::Zstd, 1024, 4_200, 1024);
    let (start, table, count) = segments(&run.as_bytes());
    assert_eq!(count, 2);
    for at in [start, table - 1] {
        let mut bytes = run.as_bytes().to_vec();
        bytes[at] ^= 1;
        assert_eq!(
            Run::new(&bytes).unwrap_err().kind,
            RunCorruption::ChecksumMismatch
        );
        let damaged = run.damaged_at(at);
        let mut w = ReadWorkspace::new();
        let key = if at == start { 0u32 } else { 4199u32 };
        assert_eq!(
            damaged.get(&key.to_be_bytes(), &mut w).unwrap_err().kind,
            RunCorruption::ChecksumMismatch
        );
        assert_eq!(w.capacity(), 0);
    }
    for (at, value) in [(table + 8, 0), (table + 8, (16 << 10) + 1), (table, 0)] {
        let mut bytes = run.as_bytes().to_vec();
        bytes[at..at + 4].copy_from_slice(&(value as u32).to_le_bytes());
        assert_eq!(
            Run::new(&bytes).unwrap_err().kind,
            RunCorruption::InvalidDictionary
        );
    }
}
#[test]
fn missing_or_out_of_range_dictionary_references_are_rejected() {
    let run = build(Codec::Zstd, 1024, 100, 500);
    for reference in [0, 2, 0xff_ffff] {
        let mut bytes = run.as_bytes().to_vec();
        bytes[1..4].copy_from_slice(&(reference as u32).to_le_bytes()[..3]);
        assert_eq!(
            Run::new(&bytes).unwrap_err().kind,
            RunCorruption::MissingDictionary
        );
    }
}
#[test]
fn uncompressed_runs_have_fixed_raw_blocks_and_no_dictionaries() {
    let a = build(Codec::None, 1, 1000, 500);
    let b = build(Codec::None, RAW_BLOCK_CAP, 1000, 500);
    assert_eq!(a.as_bytes(), b.as_bytes());
    assert_eq!(segments(&a.as_bytes()).2, 0);
    for i in 0..a.block_count() {
        let f = a.view().frame(i).unwrap();
        assert_eq!(f.dictionary_ref, 0);
        assert!(f.decoded_len <= 16 << 10);
    }
    check(&a, 1000, 500);
    check(&Run::new(a.as_bytes()).unwrap(), 1000, 500);
}

#[test]
fn arbitrary_content_cannot_be_mistaken_for_a_trained_dictionary() {
    let mut value = vec![7; (16 << 10) + 4];
    value[4..8].copy_from_slice(&[0x37, 0xa4, 0x30, 0xec]);
    let mut b = RunBuilder::default();
    b.add(b"key", 1, Value::Bytes(&value)).unwrap();
    let run = b.finish();
    let frame = run.view().frame(0).unwrap();
    assert_eq!(frame.dictionary.len(), (16 << 10) - 1);
    Run::new(run.as_bytes()).unwrap();
    assert_eq!(
        run.get(b"key", &mut ReadWorkspace::new())
            .unwrap()
            .unwrap()
            .value,
        Value::Bytes(&value)
    );
}

#[test]
fn raw_cuts_count_encoded_prefixes_and_restart_keys() {
    let mut b = RunBuilder::with_target_block_compressed(Codec::Zstd, RAW_BLOCK_CAP);
    let mut key = vec![b'x'; 60_000];
    for i in 0..128u32 {
        key[59_996..].copy_from_slice(&i.to_be_bytes());
        b.add(&key, u64::from(i) + 1, Value::Bytes(&[7; 1024]))
            .unwrap();
    }
    let run = b.finish();
    assert!(run.block_count() <= 4);
    for i in 0..run.block_count() {
        assert!(run.view().frame(i).unwrap().decoded_len <= RAW_BLOCK_CAP);
    }
    Run::new(run.as_bytes()).unwrap();
}

#[test]
fn raw_floor_controls_cuts_with_short_final_blocks_and_the_existing_cap() {
    let mut previous_blocks = usize::MAX;
    for floor in [0, 4 << 10, 8 << 10, 16 << 10, RAW_BLOCK_CAP, usize::MAX] {
        let mut builder =
            RunBuilder::with_target_block_compressed(Codec::Zstd, 1).with_min_block_raw(floor);
        for i in 0..2000u32 {
            builder
                .add(
                    &i.to_be_bytes(),
                    u64::from(i) + 1,
                    Value::Bytes(&vec![(i % 19) as u8; 512]),
                )
                .unwrap();
        }
        let run = builder.finish();
        assert!(run.block_count() <= previous_blocks);
        previous_blocks = run.block_count();
        if floor > 0 {
            let target = floor.min(RAW_BLOCK_CAP);
            for i in 0..run.block_count() {
                let len = run.view().frame(i).unwrap().decoded_len;
                assert!(len <= target, "{floor}: {len}");
                if i + 1 < run.block_count() {
                    assert!(len >= target - 1024);
                }
            }
        }
        check(&run, 2000, 512);
        check(&Run::new(run.as_bytes()).unwrap(), 2000, 512);
    }
}
