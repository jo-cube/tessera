//! Compaction's merge: what it keeps and the limits that stop it. How the
//! engine selects, replaces and retains runs is tested in `tests/compaction.rs`.

use crate::block::Value;
use crate::run::{
    CONTEXT_ALLOWANCE, CompactError, CompactLimits, Compacted, ConflictingCopies, MergeWorkspace,
    ReadError, ReadWorkspace, Run, RunBuilder, compact,
};

type Physical = (Vec<u8>, u64, Option<Vec<u8>>);

fn run(records: &[(&str, u64, Option<&str>)]) -> Run<Vec<u8>> {
    let mut builder = RunBuilder::default();
    for &(key, seq, value) in records {
        let value = value.map_or(Value::Tombstone, |v| Value::Bytes(v.as_bytes()));
        builder.add(key.as_bytes(), seq, value).unwrap();
    }
    builder.finish()
}

fn records(run: &Run<Vec<u8>>) -> Vec<Physical> {
    let mut workspace = ReadWorkspace::new();
    let mut cursor = run.cursor(&mut workspace);
    let mut out = Vec::new();
    while let Some(record) = cursor.next_record().unwrap() {
        let value = match record.value {
            Value::Bytes(bytes) => Some(bytes.to_vec()),
            Value::Tombstone => None,
        };
        out.push((record.key.to_vec(), record.seq, value));
    }
    out
}

fn physical(key: &str, seq: u64, value: Option<&str>) -> Physical {
    (key.into(), seq, value.map(Into::into))
}

const UNLIMITED: CompactLimits = CompactLimits {
    target_run_len: usize::MAX,
    max_output_bytes: usize::MAX,
    max_workspace_bytes: usize::MAX,
};

fn merge(
    inputs: &[&Run<Vec<u8>>],
    keep_tombstone: impl FnMut(&[u8]) -> bool,
    limits: CompactLimits,
) -> Result<Compacted, CompactError> {
    let views: Vec<_> = inputs.iter().map(|run| run.view()).collect();
    compact(&views, keep_tombstone, limits, &mut MergeWorkspace::new())
}

fn merged(compacted: &Compacted) -> Vec<Physical> {
    compacted.runs.iter().flat_map(records).collect()
}

/// The non-MVCC rule: only the newest version of a key survives, and an
/// identical copy counts once wherever it sits in the version chain.
#[test]
fn only_the_newest_version_survives_and_copies_count_once() {
    let older = run(&[
        ("a", 1, Some("a1")),
        ("b", 2, Some("b2")),
        ("c", 3, Some("c3")),
    ]);
    let newer = run(&[("a", 5, Some("a5")), ("b", 2, Some("b2")), ("d", 6, None)]);
    let out = merge(&[&older, &newer], |_| true, UNLIMITED).unwrap();
    assert_eq!(
        merged(&out),
        [
            physical("a", 5, Some("a5")),
            physical("b", 2, Some("b2")),
            physical("c", 3, Some("c3")),
            physical("d", 6, None),
        ]
    );
    let stats = out.stats;
    assert_eq!((stats.records_read, stats.records_written), (6, 4));
    assert_eq!((stats.versions_dropped, stats.copies_collapsed), (1, 1));
    assert_eq!(stats.tombstones_dropped, 0);
    assert_eq!(
        out.output_bytes,
        out.runs.iter().map(|r| r.as_bytes().len()).sum::<usize>()
    );
}

/// Dropping an obsolete version must not hide that the inputs disagree about
/// it: no later read could tell which was meant.
#[test]
fn differing_copies_fail_the_merge_even_when_obsolete() {
    let left = run(&[("k", 9, Some("new")), ("k", 4, Some("x"))]);
    let right = run(&[("k", 4, Some("y"))]);
    let error = merge(&[&left, &right], |_| true, UNLIMITED).unwrap_err();
    assert_eq!(
        error,
        CompactError::Read(ReadError::ConflictingCopies(ConflictingCopies {
            key: b"k".to_vec(),
            seq: 4,
            inputs: (0, 1),
        }))
    );
}

#[test]
fn a_newest_tombstone_is_written_only_when_the_caller_keeps_it() {
    let older = run(&[("drop", 2, Some("v")), ("keep", 1, Some("v"))]);
    let newer = run(&[("drop", 4, None), ("keep", 3, None)]);
    let out = merge(&[&older, &newer], |key| key == b"keep", UNLIMITED).unwrap();
    assert_eq!(merged(&out), [physical("keep", 3, None)]);
    assert_eq!(out.stats.tombstones_dropped, 1);
    assert_eq!(out.stats.versions_dropped, 2);

    // Everything deleted: nothing is built at all.
    let out = merge(&[&older, &newer], |_| false, UNLIMITED).unwrap();
    assert!(out.runs.is_empty());
    assert_eq!(out.output_bytes, 0);
}

#[test]
fn output_is_split_into_ascending_disjoint_runs() {
    let records: Vec<(String, u64, Option<String>)> = (0..400)
        .map(|i| (format!("key{i:04}"), i + 1, Some(format!("value{i}"))))
        .collect();
    let borrowed: Vec<_> = records
        .iter()
        .map(|(k, s, v)| (k.as_str(), *s, v.as_deref()))
        .collect();
    let (left, right) = borrowed.split_at(200);
    let (left, right) = (run(left), run(right));
    let limits = CompactLimits {
        target_run_len: 1_000,
        ..UNLIMITED
    };
    let out = merge(&[&right, &left], |_| true, limits).unwrap();
    assert!(out.runs.len() > 10, "{}", out.runs.len());
    let ranges: Vec<_> = out
        .runs
        .iter()
        .map(|r| r.key_range().unwrap().unwrap())
        .collect();
    assert!(ranges.windows(2).all(|pair| pair[0].max < pair[1].min));
    assert_eq!(merged(&out).len(), 400);
}

/// Each limit fails with a precise error and nothing is returned. The
/// workspace limit fails before any block is decoded.
#[test]
fn limits_fail_the_merge_with_the_amount_needed() {
    let older = run(&[("a", 1, Some("one")), ("b", 2, Some("two"))]);
    let newer = run(&[("a", 3, Some("three"))]);
    let full = merge(&[&older, &newer], |_| true, UNLIMITED).unwrap();

    // Output counts the stored bytes and the filters together.
    let output = full.output_bytes + full.filter_bytes;
    assert!(full.filter_bytes > 0);
    let error = merge(
        &[&older, &newer],
        |_| true,
        CompactLimits {
            max_output_bytes: output - 1,
            ..UNLIMITED
        },
    )
    .unwrap_err();
    assert_eq!(
        error,
        CompactError::OutputLimit {
            output_bytes: output,
            max_output_bytes: output - 1
        }
    );
    let at_limit = CompactLimits {
        max_output_bytes: output,
        ..UNLIMITED
    };
    merge(&[&older, &newer], |_| true, at_limit).unwrap();

    let views = [older.view(), newer.view()];
    let mut workspace = MergeWorkspace::new();
    let limits = CompactLimits {
        max_workspace_bytes: full.workspace_bytes - 1,
        ..UNLIMITED
    };
    let error = compact(&views, |_| true, limits, &mut workspace).unwrap_err();
    assert_eq!(
        error,
        CompactError::WorkspaceLimit {
            workspace_bytes: full.workspace_bytes,
            max_workspace_bytes: full.workspace_bytes - 1,
        }
    );
    assert_eq!(workspace.blocks_loaded(), 0);

    // What the estimate leaves of the workspace limit holds the key hashes:
    // none left means output without a filter, which reads the same.
    for (spare, filtered) in [(0, false), (15, false), (16, true)] {
        let limits = CompactLimits {
            max_workspace_bytes: full.workspace_bytes + spare,
            ..UNLIMITED
        };
        let out = merge(&[&older, &newer], |_| true, limits).unwrap();
        assert_eq!(out.filter_bytes > 0, filtered, "{spare}");
        assert_eq!(merged(&out), merged(&full));
        let key = b"a".as_slice();
        assert_eq!(
            out.runs[0].get(key, &mut ReadWorkspace::new()).unwrap(),
            full.runs[0].get(key, &mut ReadWorkspace::new()).unwrap()
        );
    }
}

/// The workspace estimate allows a fixed amount per codec context; the real
/// contexts, measured by Zstandard itself, must fit in it.
#[test]
fn measured_codec_contexts_fit_their_allowance() {
    let input: Vec<(String, u64, Option<String>)> = (0..3_000)
        .map(|i| (format!("key{i:05}"), i + 1, Some(format!("{:064}", i * 7))))
        .collect();
    let mut builder = RunBuilder::default();
    for (key, seq, value) in &input {
        let value = Value::Bytes(value.as_deref().unwrap().as_bytes());
        builder.add(key.as_bytes(), *seq, value).unwrap();
    }
    assert!(builder.context_bytes() > 0);
    assert!(
        builder.context_bytes() <= CONTEXT_ALLOWANCE,
        "{}",
        builder.context_bytes()
    );
    let run = builder.finish();

    let views = [run.view()];
    let mut workspace = MergeWorkspace::new();
    compact(&views, |_| true, UNLIMITED, &mut workspace).unwrap();
    assert!(workspace.context_bytes() > 0);
    assert!(workspace.context_bytes() <= CONTEXT_ALLOWANCE);
}
