//! In-memory publication sequences must survive every read and rewrite path.
use crate::block::Value;
use crate::run::{
    Codec, CompactError, CompactLimits, LogicalWorkspace, MergeCursor, MergeWorkspace, ReadError,
    ReadWorkspace, Run, RunBuilder, RunSet, RunView, compact, latest_in,
};

fn build(codec: Codec, records: &[(&[u8], u64, Value<'_>)]) -> Run<Vec<u8>> {
    let mut builder = RunBuilder::with_raw_block_target(codec, 16);
    for &(key, seq, value) in records {
        builder.add(key, seq, value).unwrap();
    }
    builder.finish()
}

fn limits(zero_seq: bool) -> CompactLimits {
    CompactLimits {
        target_run_len: usize::MAX,
        max_output_bytes: usize::MAX,
        max_workspace_bytes: usize::MAX,
        codec: Codec::Zstd,
        target_block_compressed: 1024,
        zero_seq,
    }
}

#[test]
fn effective_sequences_reach_point_batch_and_seek_readers_without_changing_bytes() {
    for codec in [Codec::None, Codec::Zstd] {
        let mut run = build(
            codec,
            &[
                (b"a", 0, Value::Bytes(b"one")),
                (b"b", 0, Value::Tombstone),
                (b"c", 0, Value::Bytes(b"three")),
            ],
        );
        let bytes = run.as_bytes().to_vec();
        let seq = u64::MAX;
        run.set_seq_override(seq);
        assert_eq!(run.as_bytes(), bytes);
        assert_eq!(run.max_seq(), seq);
        assert_eq!(run.damaged_at(0).max_seq(), seq);
        let mut ws = ReadWorkspace::new();
        for key in [b"a", b"b", b"c"] {
            assert_eq!(run.get(key, &mut ws).unwrap().unwrap().seq, seq);
            assert_eq!(run.view().get(key, &mut ws).unwrap().unwrap().seq, seq);
        }
        let keys: &[&[u8]] = &[b"a", b"b", b"b", b"c", b"missing"];
        let mut hits = Vec::new();
        run.view()
            .get_many(keys, &[0, 1, 2, 3, 4], &mut ws, |i, found| {
                if let Some(found) = found {
                    hits.push((i, found.seq));
                }
            })
            .unwrap();
        assert_eq!(hits, [(0, seq), (1, seq), (2, seq), (3, seq)]);
        let mut cursor = run.cursor(&mut ws);
        cursor.seek(b"b").unwrap();
        assert_eq!(cursor.peek().unwrap().unwrap().seq, seq);
        assert_eq!(
            cursor.next_record().unwrap().unwrap().value,
            Value::Tombstone
        );
        assert_eq!(cursor.next_record().unwrap().unwrap().seq, seq);
        assert!(cursor.next_record().unwrap().is_none());
        cursor.seek_prefix(b"a").unwrap();
        assert_eq!(cursor.next_record().unwrap().unwrap().seq, seq);
        assert!(cursor.next_record().unwrap().is_none());
        let raw = RunView::open(&bytes).unwrap();
        assert_eq!(
            raw.get(b"a", &mut ReadWorkspace::new())
                .unwrap()
                .unwrap()
                .seq,
            0
        );
        assert_eq!(Run::recover(bytes).unwrap().1, 0);
    }
}

#[test]
fn effective_sequences_order_merges_pruning_and_materialize_in_compaction() {
    for codec in [Codec::None, Codec::Zstd] {
        let older = build(
            codec,
            &[
                (b"a", 5, Value::Bytes(b"old")),
                (b"b", 5, Value::Bytes(b"live")),
            ],
        );
        let mut ingested = build(
            codec,
            &[(b"a", 0, Value::Bytes(b"new")), (b"b", 0, Value::Tombstone)],
        );
        ingested.set_seq_override(9);
        let mut logical = LogicalWorkspace::new();
        let found = latest_in([&older, &ingested], b"a", &mut logical)
            .unwrap()
            .unwrap();
        assert_eq!((found.seq, found.value), (9, &b"new"[..]));
        assert!(
            latest_in([&ingested, &older], b"b", &mut logical)
                .unwrap()
                .is_none()
        );
        let views = [older.view(), ingested.view()];
        let set = RunSet::new(&views);
        let mut cursor = set.cursor(&mut logical);
        let found = cursor.next_record().unwrap().unwrap();
        assert_eq!(
            (found.key, found.seq, found.value),
            (&b"a"[..], 9, &b"new"[..])
        );
        assert!(cursor.next_record().unwrap().is_none());
        let mut merge_ws = MergeWorkspace::new();
        let mut merge = MergeCursor::new(views, &mut merge_ws);
        let mut sequences = Vec::new();
        while let Some(record) = merge.next_record().unwrap() {
            sequences.push(record.seq);
        }
        assert_eq!(sequences, [9, 5, 9, 5]);
        for zero_seq in [false, true] {
            let output = compact(
                &views,
                |_| true,
                limits(zero_seq),
                &mut MergeWorkspace::new(),
            )
            .unwrap();
            let run = &output.runs[0];
            let expected = if zero_seq { 0 } else { 9 };
            let (recovered, max) = Run::recover(run.as_bytes().to_vec()).unwrap();
            assert_eq!((run.max_seq(), max), (expected, expected));
            let mut ws = ReadWorkspace::new();
            for key in [b"a", b"b"] {
                assert_eq!(recovered.get(key, &mut ws).unwrap().unwrap().seq, expected);
            }
        }
    }
}

#[test]
fn effective_sequences_detect_conflicting_copies_and_collapse_identical_ones() {
    let mut first = build(Codec::None, &[(b"key", 0, Value::Bytes(b"one"))]);
    first.set_seq_override(17);
    let other = build(Codec::Zstd, &[(b"key", 17, Value::Bytes(b"two"))]);
    let views = [first.view(), other.view()];
    let mut logical = LogicalWorkspace::new();
    let Err(ReadError::ConflictingCopies(error)) = RunSet::new(&views).get(b"key", &mut logical)
    else {
        panic!("expected effective-sequence conflict")
    };
    assert_eq!(error.seq, 17);
    let mut scan = RunSet::new(&views).cursor(&mut logical);
    assert!(
        matches!(scan.next_record(), Err(ReadError::ConflictingCopies(error)) if error.seq == 17)
    );
    assert!(
        matches!(compact(&views, |_| true, limits(false), &mut MergeWorkspace::new()), Err(CompactError::Read(ReadError::ConflictingCopies(error))) if error.seq == 17)
    );
    let copy = build(Codec::None, &[(b"key", 17, Value::Bytes(b"one"))]);
    let output = compact(
        &[first.view(), copy.view()],
        |_| true,
        limits(false),
        &mut MergeWorkspace::new(),
    )
    .unwrap();
    assert_eq!(output.stats.copies_collapsed, 1);
    assert_eq!(output.runs[0].max_seq(), 17);
}
