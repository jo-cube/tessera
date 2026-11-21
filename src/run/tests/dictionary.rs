//! Segment dictionaries, adaptive cuts and checked direct-load decoding.
use crate::block::Value;
use crate::run::{Codec, RAW_BLOCK_CAP, ReadWorkspace, Run, RunBuilder, RunCorruption, RunView};

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
fn noisy_value(i: u32, size: usize) -> Vec<u8> {
    let mut state = (u64::from(i) + 1).wrapping_mul(0x9e37_79b9_7f4a_7c15);
    (0..size)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            (state >> 24) as u8
        })
        .collect()
}
fn build_noisy(codec: Codec, target: usize, count: u32, size: usize) -> Run<Vec<u8>> {
    let mut b = RunBuilder::with_target_block_compressed(codec, target);
    for i in 0..count {
        b.add(
            &i.to_be_bytes(),
            u64::from(i) + 1,
            Value::Bytes(&noisy_value(i, size)),
        )
        .unwrap();
    }
    b.finish()
}
fn check_noisy<B: AsRef<[u8]>>(run: &Run<B>, count: u32, size: usize) {
    let mut w = ReadWorkspace::new();
    for i in (0..count).step_by(37) {
        let v = run.get(&i.to_be_bytes(), &mut w).unwrap().unwrap();
        assert_eq!(v.seq, u64::from(i) + 1);
        assert_eq!(v.value, Value::Bytes(&noisy_value(i, size)));
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
    let run = build_noisy(Codec::Zstd, 1024, 9_000, 1024);
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
    check_noisy(&run, 9_000, 1024);
    check_noisy(&Run::new(bytes).unwrap(), 9_000, 1024);
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
    // Built and validated runs record the same largest block, which a view
    // of the borrowed bytes recomputes from the frame headers.
    let bytes = run.as_bytes();
    assert_eq!(run.largest_block(), f.decoded_len);
    assert_eq!(Run::new(&bytes).unwrap().largest_block(), f.decoded_len);
    assert_eq!(
        RunView::open(&bytes).unwrap().largest_block_len(),
        Ok(f.decoded_len)
    );
}
/// Validation checks each dictionary CRC once. Reads trust the dictionaries
/// of built runs: damage after validation is a decode failure where
/// Zstandard detects it, and never a panic.
#[test]
fn corrupt_dictionary_regions_are_rejected_by_validation() {
    let run = build_noisy(Codec::Zstd, 1024, 4_200, 1024);
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
        for key in 0..4_200u32 {
            if let Err(error) = damaged.get(&key.to_be_bytes(), &mut w) {
                assert_eq!(error.kind, RunCorruption::CorruptPayload);
            }
        }
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
    // A 60 KB restart key leaves room for at least four records per block.
    assert!(run.block_count() <= 32);
    for i in 0..run.block_count() {
        assert!(run.view().frame(i).unwrap().decoded_len <= RAW_BLOCK_CAP);
    }
    Run::new(run.as_bytes()).unwrap();
}

/// Per-segment `(raw_bytes, stored_frame_bytes)` totals across a run.
fn segment_totals(run: &Run<Vec<u8>>) -> Vec<(usize, usize)> {
    let view = run.view();
    let mut out: Vec<(usize, usize)> = Vec::new();
    let mut current = 0u32;
    for block in 0..view.block_count() {
        let f = view.frame(block).unwrap();
        if f.dictionary_ref != current {
            current = f.dictionary_ref;
            out.push((0, 0));
        }
        let last = out.last_mut().unwrap();
        last.0 += f.decoded_len;
        last.1 += view.block_bytes(block);
    }
    out
}

#[test]
fn widened_scope_spans_several_sampled_segments() {
    // 13 MiB raw of repetitive records spans more than three 4 MiB
    // sampled-segment lengths under one shared dictionary.
    let run = build(Codec::Zstd, 1024, 13_000, 1024);
    let totals = segment_totals(&run);
    assert_eq!(totals.len(), 1);
    assert!(totals[0].0 > 12 << 20, "raw bytes: {}", totals[0].0);
    assert_eq!(run.view().segment_count(), 1);
    check(&run, 13_000, 1024);
    check(&Run::new(run.as_bytes()).unwrap(), 13_000, 1024);
}

#[test]
fn scope_closes_at_stored_target_raw_cap_and_drift_guard() {
    // 1. Stored-bytes target: semi-compressible values (~25% stored/raw) with
    // a 256 KiB segment length hit 16 * 16 KiB = 256 KiB stored well before
    // the 8 * 256 KiB = 2 MiB raw cap.
    let seg_len = 256 << 10;
    let mut stored_builder = RunBuilder::new(Codec::Zstd);
    stored_builder.limit_segment_len(seg_len);
    for i in 0..1_600u32 {
        let mut value = noisy_value(i, 200);
        value.extend_from_slice(&[b'a'; 800]);
        stored_builder
            .add(&i.to_be_bytes(), 1, Value::Bytes(&value))
            .unwrap();
    }
    let stored_run = stored_builder.finish();
    let stored_segs = segment_totals(&stored_run);
    assert!(stored_segs.len() >= 2, "{stored_segs:?}");
    let dict_len = stored_run.view().frame(0).unwrap().dictionary.len();
    assert!(
        stored_segs[0].1 >= 16 * dict_len,
        "first segment must reach stored target: {:?}",
        stored_segs[0]
    );
    assert!(
        stored_segs[0].0 < 8 * seg_len,
        "first segment must close before raw cap: {:?}",
        stored_segs[0]
    );
    Run::new(stored_run.as_bytes()).unwrap();

    // 2. Raw cap: ultra-compressible values hit 8 * 64 KiB = 512 KiB raw well
    // below the 256 KiB stored target.
    let mut raw_builder = RunBuilder::new(Codec::Zstd);
    raw_builder.limit_segment_len(RAW_BLOCK_CAP);
    for i in 0..1_200u32 {
        raw_builder
            .add(&i.to_be_bytes(), 1, Value::Bytes(&[7u8; 700]))
            .unwrap();
    }
    let raw_run = raw_builder.finish();
    let raw_segs = segment_totals(&raw_run);
    assert!(raw_segs.len() >= 2, "{raw_segs:?}");
    let raw_dict_len = raw_run.view().frame(0).unwrap().dictionary.len();
    assert!(
        raw_segs[0].0 > 7 * RAW_BLOCK_CAP && raw_segs[0].0 <= 8 * RAW_BLOCK_CAP,
        "first segment must close at raw cap: {:?}",
        raw_segs[0]
    );
    assert!(
        raw_segs[0].1 < 16 * raw_dict_len,
        "first segment must stay below stored target: {:?}",
        raw_segs[0]
    );
    Run::new(raw_run.as_bytes()).unwrap();

    // 3. Drift guard: a compressible 64 KiB prefix followed by incompressible
    // records triggers a ratio jump within the first 64 KiB drift window,
    // closing well below both the stored target and the raw cap.
    let mut drift_builder = RunBuilder::new(Codec::Zstd);
    drift_builder.limit_segment_len(RAW_BLOCK_CAP);
    for i in 0..80u32 {
        drift_builder
            .add(&i.to_be_bytes(), 1, Value::Bytes(&[b'z'; 1000]))
            .unwrap();
    }
    for i in 80..300u32 {
        drift_builder
            .add(&i.to_be_bytes(), 1, Value::Bytes(&noisy_value(i, 1000)))
            .unwrap();
    }
    let drift_run = drift_builder.finish();
    let drift_segs = segment_totals(&drift_run);
    assert!(drift_segs.len() >= 2, "{drift_segs:?}");
    let drift_dict_len = drift_run.view().frame(0).unwrap().dictionary.len();
    assert!(
        drift_segs[0].0 > RAW_BLOCK_CAP && drift_segs[0].0 < 4 * RAW_BLOCK_CAP,
        "drift guard must close early: {:?}",
        drift_segs[0]
    );
    assert!(
        drift_segs[0].1 < 16 * drift_dict_len,
        "drift guard must close below stored target: {:?}",
        drift_segs[0]
    );
    Run::new(drift_run.as_bytes()).unwrap();
}

#[test]
fn run_ending_mid_scope_recovers_and_reads_all_records() {
    let mut b = RunBuilder::new(Codec::Zstd);
    b.limit_segment_len(RAW_BLOCK_CAP);
    // ~2.5 sampled-segment lengths leaves an open continuation with a partial
    // final block when finish() runs.
    let count = 330u32;
    for i in 0..count {
        b.add(
            &i.to_be_bytes(),
            u64::from(i) + 1,
            Value::Bytes(&vec![(i % 7) as u8; 500]),
        )
        .unwrap();
    }
    let run = b.finish();
    let totals = segment_totals(&run);
    assert_eq!(totals.len(), 1);
    assert!(totals[0].0 > 2 * RAW_BLOCK_CAP && totals[0].0 < 4 * RAW_BLOCK_CAP);
    let (recovered, max_seq) = Run::recover(run.as_bytes().to_vec()).unwrap();
    assert_eq!(max_seq, u64::from(count));
    let mut w = ReadWorkspace::new();
    for i in 0..count {
        let got = recovered.get(&i.to_be_bytes(), &mut w).unwrap().unwrap();
        assert_eq!(got.value, Value::Bytes(&vec![(i % 7) as u8; 500]));
    }
}

#[test]
fn corruption_in_continuation_frame_is_rejected() {
    let mut b = RunBuilder::new(Codec::Zstd);
    b.limit_segment_len(RAW_BLOCK_CAP);
    let count = 400u32;
    for i in 0..count {
        b.add(&i.to_be_bytes(), 1, Value::Bytes(&[b'k'; 600]))
            .unwrap();
    }
    let run = b.finish();
    let view = run.view();
    assert_eq!(view.segment_count(), 1);
    // Find a block strictly past the sampled prefix (after RAW_BLOCK_CAP raw).
    let mut raw_seen = 0usize;
    let mut offset = 0usize;
    let mut target_block = None;
    for block in 0..view.block_count() {
        let f = view.frame(block).unwrap();
        if raw_seen >= RAW_BLOCK_CAP {
            target_block = Some((block, offset, view.block_bytes(block)));
            break;
        }
        raw_seen += f.decoded_len;
        offset += view.block_bytes(block);
    }
    let (block, frame_offset, frame_len) = target_block.expect("continuation block exists");
    let target_key = view.last_key(block).unwrap().to_vec();
    for (flip, expected) in [
        (frame_offset + 2, RunCorruption::MissingDictionary),
        (frame_offset + 8, RunCorruption::ChecksumMismatch),
        (
            frame_offset + frame_len - 1,
            RunCorruption::ChecksumMismatch,
        ),
    ] {
        let mut bytes = run.as_bytes().to_vec();
        bytes[flip] ^= 1;
        assert_eq!(Run::new(&bytes).unwrap_err().kind, expected);
        let damaged = run.damaged_at(flip);
        let mut w = ReadWorkspace::new();
        assert_eq!(damaged.get(&target_key, &mut w).unwrap_err().kind, expected);
        // Prefix blocks before the corrupted continuation frame still decode.
        assert!(damaged.get(&0u32.to_be_bytes(), &mut w).unwrap().is_some());
    }
}
