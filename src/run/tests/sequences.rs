//! In-memory publication sequences must survive every read and rewrite path.
use crate::block::Value;
use crate::run::{
    Codec, CompactLimits, LogicalWorkspace, MergeCursor, MergeWorkspace, OutputCuts, ReadError,
    ReadWorkspace, Run, RunBuilder, RunSet, RunView, SeqFloor, compact, compact_erasing, latest_in,
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
        max_workspace_bytes: usize::MAX,
        codec: Codec::Zstd,
        target_block_compressed: 1024,
        zero_seq,
        copy_forward: true,
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
        matches!(compact(&views, |_| true, limits(false), &mut MergeWorkspace::new()), Err(ReadError::ConflictingCopies(error)) if error.seq == 17)
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

#[test]
fn min_seq_follows_the_builder_overrides_erasure_and_recovery() {
    let run = build(
        Codec::Zstd,
        &[
            (b"a", 7, Value::Bytes(b"one")),
            (b"b", 3, Value::Tombstone),
            (b"c", 9, Value::Bytes(b"three")),
        ],
    );
    assert_eq!((run.min_seq(), run.max_seq()), (3, 9));
    let (recovered, max) = Run::recover(run.as_bytes().to_vec()).unwrap();
    assert_eq!((recovered.min_seq(), recovered.max_seq(), max), (3, 9, 9));
    let mut ingested = build(
        Codec::Zstd,
        &[
            (b"b", 0, Value::Bytes(b"new")),
            (b"d", 0, Value::Bytes(b"four")),
        ],
    );
    assert_eq!((ingested.min_seq(), ingested.max_seq()), (0, 0));
    ingested.set_seq_override(12);
    assert_eq!((ingested.min_seq(), ingested.max_seq()), (12, 12));
    for zero_seq in [false, true] {
        let output = compact(
            &[run.view(), ingested.view()],
            |_| true,
            limits(zero_seq),
            &mut MergeWorkspace::new(),
        )
        .unwrap();
        let out = &output.runs[0];
        let expected = if zero_seq { (0, 0) } else { (7, 12) };
        assert_eq!((out.min_seq(), out.max_seq()), expected);
        let (recovered, _) = Run::recover(out.as_bytes().to_vec()).unwrap();
        assert_eq!((recovered.min_seq(), recovered.max_seq()), expected);
    }
}

fn key(index: usize) -> Vec<u8> {
    format!("key/{index:06}").into_bytes()
}

/// A run holding `keys`, each at sequence `seq`.
fn run_of(keys: impl IntoIterator<Item = usize>, seq: u64) -> Run<Vec<u8>> {
    let mut builder = RunBuilder::with_raw_block_target(Codec::Zstd, 1024);
    for index in keys {
        builder.add(&key(index), seq, Value::Bytes(b"v")).unwrap();
    }
    builder.finish()
}

/// Erases `input`'s sequences into the bottom level against `floors`, and
/// returns which keys kept theirs.
fn kept(input: &Run<Vec<u8>>, floors: &[SeqFloor<'_>]) -> Vec<Vec<u8>> {
    let output = compact_erasing(
        &[input.view()],
        |_| true,
        limits(true),
        &OutputCuts::default(),
        floors,
        &mut MergeWorkspace::new(),
    )
    .unwrap();
    let mut out = Vec::new();
    for run in &output.runs {
        let mut workspace = ReadWorkspace::new();
        let mut cursor = run.cursor(&mut workspace);
        while let Some(record) = cursor.next_record().unwrap() {
            if record.seq != 0 {
                assert_eq!(record.seq, input.max_seq());
                out.push(record.key.to_vec());
            }
        }
    }
    out
}

#[test]
fn a_covering_run_contests_only_the_keys_its_filter_may_hold() {
    // Every key at sequence 20; the skipped run holds the even keys of
    // 1000..3000 at sequence 10.
    let input = run_of(0..4000, 20);
    let skipped = run_of((1000..3000).step_by(2), 10);
    let filter = skipped.filter().expect("built runs carry a filter");
    let floors = [SeqFloor::of(&skipped).unwrap()];
    let held = kept(&input, &floors);
    let positive = |index: usize| filter.may_contain(crate::run::filter::hash(&key(index)));
    let expected: Vec<_> = (1000..=2998).filter(|&i| positive(i)).map(key).collect();
    assert_eq!(held, expected);
    // Some odd key is a false positive: the run does not hold it, but it
    // still keeps its sequence.
    assert!(
        (1001..2998)
            .step_by(2)
            .any(|i| positive(i) && held.contains(&key(i)))
    );
    // Without a filter, every key in the range keeps its sequence.
    let unfiltered = [SeqFloor {
        filter: None,
        ..floors[0]
    }];
    assert_eq!(
        kept(&input, &unfiltered),
        (1000..=2998).map(key).collect::<Vec<_>>()
    );
}

#[test]
fn erasure_refuses_only_where_an_unselected_run_may_hold_an_older_or_equal_version() {
    let input = run_of(0..100, 20);
    for (seq, refused) in [(5, true), (20, true), (21, false)] {
        let other = run_of([10, 50], seq);
        let floors = [SeqFloor::of(&other).unwrap()];
        let expected = if refused {
            vec![key(10), key(50)]
        } else {
            Vec::new()
        };
        assert_eq!(kept(&input, &floors), expected, "{seq}");
    }
    // Several covering runs, entered and left in key order: each contests
    // only its own keys, and runs that do not overlap cost nothing.
    let low = run_of([5, 30], 10);
    let high = run_of([20, 80], 10);
    let newer = run_of([25, 40], 30);
    let mut floors: Vec<_> = [&low, &high, &newer]
        .into_iter()
        .map(|run| SeqFloor::of(run).unwrap())
        .collect();
    floors.sort_by_key(|floor| floor.min_key);
    assert_eq!(kept(&input, &floors), [key(5), key(20), key(30), key(80)]);
    assert!(kept(&input, &[]).is_empty());
}
