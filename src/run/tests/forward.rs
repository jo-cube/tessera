//! Copying settled blocks forward: which blocks a merge into the bottom level
//! copies, the inherited-segment policy, integrity, and settled counts.
use crate::block::Value;
use crate::run::{
    Codec, CompactLimits, Compacted, CorruptRun, MergeWorkspace, OutputCuts, RAW_BLOCK_CAP,
    ReadError, ReadWorkspace, Run, RunBuilder, RunCorruption, compact_erasing,
};

type Physical = (Vec<u8>, u64, Option<Vec<u8>>);

fn key(i: usize) -> Vec<u8> {
    format!("k{i:06}").into_bytes()
}

/// A 900-byte value that compresses a little.
fn value(i: usize) -> Vec<u8> {
    let mut state = (i as u64 + 1).wrapping_mul(0x9e37_79b9_7f4a_7c15);
    (0..900)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            b"abcdefghijklmnop"[(state % 16) as usize]
        })
        .collect()
}

/// `count` records, one per block, in segments of at most 64 KiB raw.
/// Record `i` has sequence `seq(i)` and is a tombstone if `tombstone(i)`.
fn bottom(
    count: usize,
    seq: impl Fn(usize) -> u64,
    tombstone: impl Fn(usize) -> bool,
) -> Run<Vec<u8>> {
    let mut builder = RunBuilder::with_raw_block_target(Codec::Zstd, 1);
    builder.limit_segment_len(RAW_BLOCK_CAP);
    for i in 0..count {
        let value = value(i);
        let value = if tombstone(i) {
            Value::Tombstone
        } else {
            Value::Bytes(&value)
        };
        builder.add(&key(i), seq(i), value).unwrap();
    }
    builder.finish()
}

fn newer(keys: &[Vec<u8>]) -> Run<Vec<u8>> {
    let mut keys = keys.to_vec();
    keys.sort();
    let mut builder = RunBuilder::new(Codec::Zstd);
    for key in &keys {
        builder.add(key, 9, Value::Bytes(b"newer")).unwrap();
    }
    builder.finish()
}

fn limits(copy_forward: bool) -> CompactLimits {
    CompactLimits {
        target_run_len: usize::MAX,
        max_workspace_bytes: usize::MAX,
        codec: Codec::Zstd,
        target_block_compressed: 1024,
        zero_seq: true,
        copy_forward,
    }
}

fn merge(inputs: &[&Run<Vec<u8>>], copy: bool) -> Result<Compacted, ReadError> {
    let views: Vec<_> = inputs.iter().map(|run| run.view()).collect();
    let cuts = OutputCuts {
        omit_filter: true,
        ..OutputCuts::default()
    };
    compact_erasing(
        &views,
        |_| true,
        limits(copy),
        &cuts,
        &[],
        &mut MergeWorkspace::new(),
    )
}

fn records(runs: &[Run<Vec<u8>>]) -> Vec<Physical> {
    let mut out = Vec::new();
    let mut ws = ReadWorkspace::new();
    for run in runs {
        let mut cursor = run.cursor(&mut ws);
        while let Some(record) = cursor.next_record().unwrap() {
            let value = match record.value {
                Value::Bytes(bytes) => Some(bytes.to_vec()),
                Value::Tombstone => None,
            };
            out.push((record.key.to_vec(), record.seq, value));
        }
    }
    out
}

fn settled(run: &Run<Vec<u8>>) -> Vec<usize> {
    let view = run.view();
    (0..view.block_count())
        .map(|block| view.settled_records(block))
        .collect()
}

/// Merges with copying on and off, checks that both outputs hold the same
/// records and recover to the same settled counts, and returns the blocks
/// copied.
fn copied(inputs: &[&Run<Vec<u8>>]) -> u64 {
    let on = merge(inputs, true).unwrap();
    let off = merge(inputs, false).unwrap();
    assert_eq!(off.copied.blocks, 0);
    assert_eq!(records(&on.runs), records(&off.runs));
    assert_eq!(on.stats, off.stats);
    for run in on.runs.iter().chain(&off.runs) {
        let (recovered, _) = Run::recover(run.as_bytes().to_vec()).unwrap();
        assert_eq!(settled(&recovered), settled(run));
    }
    on.copied.blocks
}

#[test]
fn blocks_touched_by_another_input_are_decoded() {
    let run = bottom(400, |_| 0, |_| false);
    // Before every key; equal to block 0's only key; equal to block 10's
    // last key; just above block 20's last key, inside block 21; and equal
    // to the last block's key.
    let mut just_above = key(20);
    just_above.push(0);
    let touching = newer(&[b"a".to_vec(), key(0), key(10), just_above, key(399)]);
    assert_eq!(copied(&[&touching, &run]), 400 - 4);
    // The newer input listed after the bottom one: order changes nothing.
    assert_eq!(copied(&[&run, &touching]), 400 - 4);
}

#[test]
fn a_first_block_needs_the_run_minimum_and_an_untouched_start() {
    let run = bottom(200, |_| 0, |_| false);
    // Nothing newer below or in the run's range: every block is copied,
    // the first with the bottom run's minimum key as the output's.
    let after = newer(&[b"z".to_vec()]);
    assert_eq!(copied(&[&after, &run]), 200);
    // A newer version of the minimum key holds the first block back.
    let first = newer(&[key(0)]);
    assert_eq!(copied(&[&first, &run]), 199);
}

#[test]
fn a_bounded_view_counts_only_the_keys_it_shows() {
    let run = bottom(200, |_| 0, |_| false);
    let physical = newer(&[key(30), key(120)]);
    let mut ws = ReadWorkspace::new();
    // The view starts at key 120, so key 30 does not touch block 30.
    let view = physical
        .bounded(Some(&key(100)), None, &mut ws)
        .unwrap()
        .unwrap();
    assert_eq!(copied(&[&view, &run]), 199);
    // A view ending before key 120 touches block 30 only.
    let view = physical
        .bounded(None, Some(&key(100)), &mut ws)
        .unwrap()
        .unwrap();
    assert_eq!(copied(&[&view, &run]), 199);
    // A view of the other input is never copied from.
    let settled_view = run.bounded(Some(&key(50)), None, &mut ws).unwrap().unwrap();
    let after = newer(&[b"z".to_vec()]);
    assert_eq!(
        merge(&[&after, &settled_view], true).unwrap().copied.blocks,
        0
    );
}

#[test]
fn blocks_with_sequences_or_tombstones_are_never_copied() {
    let run = bottom(400, |i| u64::from(i == 50) * 3, |i| i == 60);
    let counts = settled(&run);
    assert_eq!((counts[50], counts[60], counts[61]), (0, 0, 1));
    let after = newer(&[b"z".to_vec()]);
    // Blocks 50 and 60 are decoded. Block 51 is too: block 50 ends with a
    // nonzero sequence, so block 51 could begin with an older version.
    assert_eq!(copied(&[&after, &run]), 400 - 3);
}

#[test]
fn an_input_segment_is_inherited_from_half_its_blocks_copyable() {
    let layout = bottom(400, |_| 0, |_| false);
    let view = layout.view();
    let segments: Vec<usize> = {
        let mut sizes = vec![0usize];
        let mut reference = view.frame(0).unwrap().dictionary_ref;
        for block in 0..view.block_count() {
            let next = view.frame(block).unwrap().dictionary_ref;
            if next != reference {
                sizes.push(0);
                reference = next;
            }
            *sizes.last_mut().unwrap() += 1;
        }
        sizes
    };
    assert!(segments.len() >= 3 && segments[0] >= 8, "{segments:?}");
    let first = segments[0];
    let after = newer(&[b"z".to_vec()]);
    // Exactly half of the first segment is settled, at its start; the rest
    // of the run is not.
    let half = bottom(400, |i| u64::from(i >= first.div_ceil(2)), |_| false);
    assert_eq!(copied(&[&after, &half]), first.div_ceil(2) as u64);
    // One block fewer than half: the segment is rewritten.
    let below = bottom(400, |i| u64::from(i + 1 >= first.div_ceil(2)), |_| false);
    assert_eq!(copied(&[&after, &below]), 0);
}

#[test]
fn an_inherited_segment_ends_at_its_raw_cap() {
    // One input segment of about 700 KiB raw, exceeding the limited builder's
    // inherited cap of (SCOPE_MAX_RAW_MULTIPLE + 1) * RAW_BLOCK_CAP (576 KiB).
    let mut source = RunBuilder::with_raw_block_target(Codec::Zstd, 1);
    for i in 0..750 {
        source.add(&key(i), 0, Value::Bytes(&value(i))).unwrap();
    }
    let source = source.finish();
    let view = source.view();
    let frame = view.frame(0).unwrap();
    assert!((1..750).all(|b| view.frame(b).unwrap().dictionary_ref == frame.dictionary_ref));
    let dictionary = view.inherited_dictionary(frame.dictionary_ref).unwrap();
    let (raw, stored): (usize, usize) = (0..750)
        .map(|b| view.frame(b).unwrap())
        .fold((0, 0), |(r, s), f| (r + f.decoded_len, s + f.stored.len()));
    let expected_cap = dictionary
        .len()
        .saturating_mul(16)
        .saturating_mul(raw)
        .checked_div(stored)
        .unwrap_or(RAW_BLOCK_CAP)
        .saturating_add(RAW_BLOCK_CAP)
        .clamp(2 * RAW_BLOCK_CAP, 9 * RAW_BLOCK_CAP);

    let mut builder = RunBuilder::new(Codec::Zstd);
    builder.limit_segment_len(RAW_BLOCK_CAP);
    builder.limit_filter_build_bytes(0);
    builder.begin_inherited(dictionary, raw, stored);
    assert!(builder.inheriting());
    let mut block = 0;
    let mut copied_raw = 0;
    loop {
        let frame = view.frame(block).unwrap();
        let last = view.last_key(block).unwrap().to_vec();
        let first = (block == 0).then(|| view.min_key());
        if !builder.copy_frame(&frame, &last, 1, first).unwrap() {
            break;
        }
        copied_raw += frame.decoded_len;
        block += 1;
    }
    assert!(!builder.inheriting());
    assert!(copied_raw <= expected_cap);
    assert!(copied_raw + view.frame(block).unwrap().decoded_len > expected_cap);
    // The rest of the input segment goes into sampled segments.
    for i in block..750 {
        builder.add(&key(i), 0, Value::Bytes(&value(i))).unwrap();
    }
    let run = builder.finish();
    assert!(run.view().segment_count() >= 2);
    assert_eq!(records(std::slice::from_ref(&run)), records(&[source]));
    let (recovered, _) = Run::recover(run.as_bytes().to_vec()).unwrap();
    let counts = settled(&run);
    assert!(counts[..block].iter().all(|&n| n == 1) && counts.iter().all(|&n| n > 0));
    assert_eq!(settled(&recovered), counts);
}

#[test]
fn copy_forward_into_and_out_of_a_widened_segment() {
    // Build a settled bottom run with two segments:
    // - Segment 1: incompressible 4.2 MiB (stays at one sampled segment)
    // - Segment 2: compressible 10 MiB (widens past 2 * 4 MiB = 8 MiB)
    let mut b = RunBuilder::new(Codec::Zstd);
    let mut state = 0x1234_5678_9abc_def0u64;
    for i in 0..4_200usize {
        let mut v = vec![0u8; 1024];
        for byte in &mut v {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            *byte = (state >> 24) as u8;
        }
        b.add(&key(i), 0, Value::Bytes(&v)).unwrap();
    }
    let compressible = vec![b'a'; 1024];
    for i in 4_200..18_000usize {
        b.add(&key(i), 0, Value::Bytes(&compressible)).unwrap();
    }
    let run = b.finish();
    let view = run.view();
    assert!(view.segment_count() >= 2);
    let widened_ref = view.frame(view.block_count() - 1).unwrap().dictionary_ref;
    let (widened_blocks, widened_raw): (usize, usize) = (0..view.block_count())
        .map(|blk| view.frame(blk).unwrap())
        .filter(|f| f.dictionary_ref == widened_ref)
        .fold((0, 0), |(n, r), f| (n + 1, r + f.decoded_len));
    assert!(widened_raw > 9 << 20, "widened segment raw: {widened_raw}");

    // Case 1 (copy out of a widened segment): touch every block before the
    // widened segment so earlier segments rewrite while the widened segment
    // (> 9 MiB) is inherited and copied verbatim past twice the sampled
    // segment length (8 MiB).
    let first_widened_block = (0..view.block_count())
        .find(|&blk| view.frame(blk).unwrap().dictionary_ref == widened_ref)
        .unwrap();
    let before_widened = view.last_key(first_widened_block - 1).unwrap().to_vec();
    let touch_first: Vec<Vec<u8>> = (0..18_000)
        .step_by(2)
        .map(key)
        .filter(|k| k <= &before_widened)
        .collect();
    let updates = newer(&touch_first);
    let tail_update = newer(&[key(17_999)]);
    let out = merge(&[&updates, &tail_update, &run], true).unwrap();
    let off = merge(&[&updates, &tail_update, &run], false).unwrap();
    assert_eq!(records(&out.runs), records(&off.runs));
    assert_eq!(out.copied.inherited, 1);
    assert_eq!(out.copied.blocks as usize, widened_blocks - 1);
    assert!(out.copied.raw > 9 << 20);
    for r in &out.runs {
        let (recovered, _) = Run::recover(r.as_bytes().to_vec()).unwrap();
        assert_eq!(settled(&recovered), settled(r));
    }

    // Case 2 (copy into a run that first writes a widened sampled segment):
    // rewrite a 10 MiB compressible prefix (which opens a continued sampled
    // segment) followed by an untouched widened segment (begin_inherited closes
    // the continued segment cleanly and inherits the second widened segment).
    let mut b2 = RunBuilder::new(Codec::Zstd);
    for i in 0..10_000usize {
        b2.add(&key(i), 0, Value::Bytes(&compressible)).unwrap();
    }
    // High-entropy bridge forces a segment close before the second widened segment.
    for i in 10_000..14_500usize {
        let mut v = vec![0u8; 1024];
        for byte in &mut v {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            *byte = (state >> 24) as u8;
        }
        b2.add(&key(i), 0, Value::Bytes(&v)).unwrap();
    }
    for i in 14_500..24_500usize {
        b2.add(&key(i), 0, Value::Bytes(&compressible)).unwrap();
    }
    let run2 = b2.finish();
    let touch_prefix: Vec<Vec<u8>> = (0..10_000).step_by(20).map(key).collect();
    let updates2 = newer(&touch_prefix);
    let out2 = merge(&[&updates2, &run2], true).unwrap();
    let off2 = merge(&[&updates2, &run2], false).unwrap();
    assert_eq!(records(&out2.runs), records(&off2.runs));
    assert!(out2.copied.inherited >= 1);
    assert!(out2.copied.raw > 9 << 20);
    for r in &out2.runs {
        let (recovered, _) = Run::recover(r.as_bytes().to_vec()).unwrap();
        assert_eq!(settled(&recovered), settled(r));
    }
}

#[test]
fn a_damaged_frame_or_dictionary_refuses_the_merge() {
    let run = bottom(200, |_| 0, |_| false);
    let view = run.view();
    let after = newer(&[b"z".to_vec()]);
    let frame_at: usize = (0..30).map(|b| view.block_bytes(b)).sum();
    // A stored byte of block 30, which would be copied.
    let damaged = run.damaged_at(frame_at + 20);
    let error = merge(&[&after, &damaged], true).unwrap_err();
    assert!(
        matches!(
            error,
            ReadError::Corrupt(CorruptRun {
                offset,
                kind: RunCorruption::ChecksumMismatch,
            }) if offset == frame_at
        ),
        "{error:?}"
    );
    // A byte of the first segment's dictionary, which would be inherited.
    let damaged = run.damaged_at(view.frame_bytes() + 5);
    let error = merge(&[&after, &damaged], true).unwrap_err();
    assert!(
        matches!(
            error,
            ReadError::Corrupt(CorruptRun {
                kind: RunCorruption::ChecksumMismatch,
                ..
            })
        ),
        "{error:?}"
    );
}

#[test]
fn recovery_recomputes_settled_counts() {
    // Several records per block, with versions, tombstones and sequences.
    for target in [1, 300, 4 << 10] {
        let mut builder = RunBuilder::with_raw_block_target(Codec::Zstd, target);
        for i in 0..2_000usize {
            let seq = if i % 97 == 0 { 7 } else { 0 };
            let value = value(i);
            let value = if i % 131 == 0 {
                Value::Tombstone
            } else {
                Value::Bytes(&value[..i % 200])
            };
            builder.add(&key(i), seq, value).unwrap();
        }
        let run = builder.finish();
        let counts = settled(&run);
        assert!(counts.iter().any(|&n| n > 0) && counts.contains(&0));
        let (recovered, _) = Run::recover(run.as_bytes().to_vec()).unwrap();
        assert_eq!(settled(&recovered), counts, "target {target}");
    }
    // A run without a settled block holds no counts.
    let flushed = newer(&[key(1), key(2)]);
    assert_eq!(settled(&flushed), vec![0]);
}
