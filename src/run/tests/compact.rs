//! Compaction's merge: what it keeps and the limits that stop it. How the
//! engine selects, replaces and retains runs is tested in `tests/compaction.rs`.

use crate::block::Value;
use crate::run::{
    CONTEXT_ALLOWANCE, Codec, CompactLimits, Compacted, ConflictingCopies, MergeWorkspace,
    OutputCuts, RAW_BLOCK_CAP, RECORD_INPUT_OVERHEAD, ReadError, ReadWorkspace, Run, RunBuilder,
    TARGET_BLOCK_LEN, compact, compact_with,
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
    max_workspace_bytes: usize::MAX,
    codec: Codec::Zstd,
    target_block_compressed: crate::run::TARGET_BLOCK_LEN,
    zero_seq: false,
};

fn merge(
    inputs: &[&Run<Vec<u8>>],
    keep_tombstone: impl FnMut(&[u8]) -> bool,
    limits: CompactLimits,
) -> Result<Compacted, ReadError> {
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
        ReadError::ConflictingCopies(ConflictingCopies {
            key: b"k".to_vec(),
            seq: 4,
            inputs: (0, 1),
        })
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
    let ranges: Vec<_> = out.runs.iter().map(|r| r.key_range().unwrap()).collect();
    assert!(ranges.windows(2).all(|pair| pair[0].max < pair[1].min));
    assert_eq!(merged(&out).len(), 400);
}

/// Workspace allowances reduce filter construction without refusing a merge.
#[test]
fn workspace_allowance_controls_filters_without_refusing_the_merge() {
    let older = run(&[("a", 1, Some("one")), ("b", 2, Some("two"))]);
    let newer = run(&[("a", 3, Some("three"))]);
    let full = merge(&[&older, &newer], |_| true, UNLIMITED).unwrap();

    // A tight allowance still admits the serial job rather than refusing it.
    let tight = merge(
        &[&older, &newer],
        |_| true,
        CompactLimits {
            max_workspace_bytes: 0,
            ..UNLIMITED
        },
    )
    .unwrap();
    assert_eq!(merged(&tight), merged(&full));

    // What the estimate leaves of the workspace limit holds the key hashes:
    // none left means output without a filter, which reads the same.
    for (spare, filtered) in [(0, false), (15, false), (16, true)] {
        let limits = CompactLimits {
            max_workspace_bytes: full.workspace_bytes + spare,
            ..UNLIMITED
        };
        let out = merge(&[&older, &newer], |_| true, limits).unwrap();
        assert_eq!(
            out.runs.iter().any(|run| run.filter_bytes() > 0),
            filtered,
            "{spare}"
        );
        assert_eq!(merged(&out), merged(&full));
        let key = b"a".as_slice();
        assert_eq!(
            out.runs[0].get(key, &mut ReadWorkspace::new()).unwrap(),
            full.runs[0].get(key, &mut ReadWorkspace::new()).unwrap()
        );
    }
}

/// The workspace estimate allows a fixed amount per codec context; the real
/// contexts, measured by Zstandard itself, must fit in it at every block
/// length the engine builds. A compression context grows with the block.
#[test]
fn measured_codec_contexts_fit_their_allowance() {
    for target in [256, TARGET_BLOCK_LEN, RAW_BLOCK_CAP] {
        // Fill a segment so compression happens before the context is measured.
        let input: Vec<(String, String)> = (0..(4 << 20) / 72 + 64)
            .map(|i| (format!("key{i:07}"), format!("{:064}", i * 7)))
            .collect();
        let mut builder = RunBuilder::with_raw_block_target(Codec::Zstd, target);
        for (seq, (key, value)) in input.iter().enumerate() {
            let value = Value::Bytes(value.as_bytes());
            builder.add(key.as_bytes(), seq as u64 + 1, value).unwrap();
        }
        assert!(builder.context_bytes() > 0);
        assert!(
            builder.context_bytes() <= CONTEXT_ALLOWANCE,
            "{} at {target}",
            builder.context_bytes()
        );
        let run = builder.finish();
        assert!(run.view().largest_block_len().unwrap() >= target - 128);

        let views = [run.view()];
        let mut workspace = MergeWorkspace::new();
        let limits = CompactLimits {
            target_block_compressed: target,
            ..UNLIMITED
        };
        compact(&views, |_| true, limits, &mut workspace).unwrap();
        assert!(workspace.context_bytes() > 0);
        assert!(workspace.context_bytes() <= CONTEXT_ALLOWANCE);
    }
}

/// Output runs end before the record that would take them past the target
/// (5AT), so a run is at most the target unless it holds one record longer
/// than the target, which is a run of its own. No run is empty, and every
/// record survives in order.
#[test]
fn outputs_end_before_a_record_that_overshoots() {
    let charge = |key: &str, value: usize| key.len() + value + RECORD_INPUT_OVERHEAD;
    let target = 1_000;
    // Values sized so that the third record of each run would overshoot,
    // one record alone over the target, and incompressible bytes.
    let mut noise = 0x2545_f491_4f6c_dd1d_u64;
    let mut bytes = |len: usize| -> String {
        (0..len)
            .map(|_| {
                noise ^= noise << 13;
                noise ^= noise >> 7;
                noise ^= noise << 17;
                char::from(b'!' + (noise % 90) as u8)
            })
            .collect()
    };
    let lens = [400, 400, 400, 2_500, 10, 990, 400, 400, 1];
    let values: Vec<String> = lens.iter().map(|&len| bytes(len)).collect();
    let keys: Vec<String> = (0..lens.len()).map(|i| format!("k{i}")).collect();
    let written: Vec<(&str, u64, Option<&str>)> = keys
        .iter()
        .zip(&values)
        .enumerate()
        .map(|(i, (key, value))| (key.as_str(), i as u64 + 1, Some(value.as_str())))
        .collect();
    let input = run(&written);
    let limits = CompactLimits {
        target_run_len: target,
        codec: Codec::None,
        ..UNLIMITED
    };
    let out = merge(&[&input], |_| true, limits).unwrap();
    let sizes: Vec<usize> = out
        .runs
        .iter()
        .map(|run| {
            records(run)
                .iter()
                .map(|(key, _, value)| {
                    charge(
                        std::str::from_utf8(key).unwrap(),
                        value.as_ref().unwrap().len(),
                    )
                })
                .sum()
        })
        .collect();
    let counts: Vec<usize> = out.runs.iter().map(|run| records(run).len()).collect();
    // Charges 418 each for 400 bytes: [418, 418] then 418 would reach 1254;
    // [418] then 2518; [2518] alone; [28] then 1008; [1008] alone, at the
    // target; [418, 418, 19].
    assert_eq!(counts, [2, 1, 1, 1, 1, 3], "{sizes:?}");
    for (size, count) in sizes.iter().zip(&counts) {
        assert!(*size <= target || *count == 1, "{sizes:?}");
    }
    assert_eq!(merged(&out), records(&input));
}

/// Cuts at given keys fall between keys, including the empty key and keys
/// ending in 0xFF, and never make an empty run: a cut with no record
/// before it, or several cuts between two records, start one run.
#[test]
fn outputs_end_at_the_given_cuts_between_keys() {
    let keys: [&[u8]; 6] = [b"", b"a", b"a\xff", b"a\xff\xff", b"b", b"\xff"];
    let mut builder = RunBuilder::default();
    for (seq, key) in keys.iter().enumerate() {
        builder
            .add(key, seq as u64 + 1, Value::Bytes(b"v"))
            .unwrap();
    }
    let input = builder.finish();
    let cuts = OutputCuts {
        // Before everything (no run yet), then two cuts between "a\xff" and
        // "a\xff\xff", then one at "b" exactly, then one past the end.
        at: vec![
            Vec::new(),
            b"a\xff\x00".to_vec(),
            b"a\xff\xff".to_vec(),
            b"b".to_vec(),
            b"\xff\xff".to_vec(),
        ],
        ..OutputCuts::default()
    };
    let views = [input.view()];
    let out = compact_with(
        &views,
        |_| true,
        UNLIMITED,
        &cuts,
        &mut MergeWorkspace::new(),
    )
    .unwrap();
    let firsts: Vec<Vec<u8>> = out
        .runs
        .iter()
        .map(|run| records(run)[0].0.clone())
        .collect();
    assert_eq!(firsts, [b"".to_vec(), b"a\xff\xff".to_vec(), b"b".to_vec()]);
    assert_eq!(merged(&out), records(&input));

    // One run whatever the target, for merging small neighbours.
    let single = OutputCuts {
        single: true,
        ..OutputCuts::default()
    };
    let limits = CompactLimits {
        target_run_len: 1,
        ..UNLIMITED
    };
    let out = compact_with(
        &views,
        |_| true,
        limits,
        &single,
        &mut MergeWorkspace::new(),
    )
    .unwrap();
    assert_eq!(out.runs.len(), 1);
}

/// A run being built ends once it would reach more than `max_overlapped`
/// runs of the level below, counted by their smallest keys.
#[test]
fn outputs_end_where_they_would_overlap_too_many_runs_below() {
    let keys: Vec<String> = (0..20).map(|i| format!("k{i:02}")).collect();
    let written: Vec<(&str, u64, Option<&str>)> = keys
        .iter()
        .enumerate()
        .map(|(i, key)| (key.as_str(), i as u64 + 1, Some("v")))
        .collect();
    let input = run(&written);
    // Runs below start at k00, k03, k06, ... k18.
    let overlapped: Vec<Vec<u8>> = (0..20)
        .step_by(3)
        .map(|i| format!("k{i:02}").into_bytes())
        .collect();
    let cuts = OutputCuts {
        overlapped,
        max_overlapped: 1,
        ..OutputCuts::default()
    };
    let views = [input.view()];
    let out = compact_with(
        &views,
        |_| true,
        UNLIMITED,
        &cuts,
        &mut MergeWorkspace::new(),
    )
    .unwrap();
    let firsts: Vec<String> = out
        .runs
        .iter()
        .map(|run| String::from_utf8(records(run)[0].0.clone()).unwrap())
        .collect();
    // Each run reaches its first run below and one more start.
    assert_eq!(firsts, ["k00", "k06", "k12", "k18"]);
    assert_eq!(merged(&out), records(&input));
}

fn parallel_inputs() -> Vec<Run<Vec<u8>>> {
    (0..2)
        .map(|round| {
            let mut builder = RunBuilder::with_raw_block_target(Codec::None, 4096);
            for at in 0..12_000u64 {
                let key = format!("k{at:05}");
                let bytes = [at as u8; 128];
                let value = if round == 1 && at % 7 == 0 {
                    Value::Tombstone
                } else {
                    Value::Bytes(&bytes)
                };
                builder
                    .add(key.as_bytes(), at + 1 + round * 12_000, value)
                    .unwrap();
                // Versions of the same key can straddle input block boundaries.
                builder
                    .add(key.as_bytes(), at + round * 12_000, Value::Bytes(&bytes))
                    .unwrap();
            }
            builder.finish()
        })
        .collect()
}

#[test]
fn subcompactions_match_serial_versions_tombstones_sequences_and_cuts() {
    let runs = parallel_inputs();
    let mut views: Vec<_> = runs.iter().map(Run::view).collect();
    views.push(runs[0].view());
    let cuts = OutputCuts {
        at: vec![b"k03000".to_vec(), b"k09000".to_vec()],
        overlapped: (0..12)
            .map(|i| format!("k{:05}", i * 1000).into_bytes())
            .collect(),
        max_overlapped: 1,
        single: false,
    };
    for zero_seq in [false, true] {
        let limits = CompactLimits {
            target_run_len: 256 << 10,
            max_workspace_bytes: 64 << 20,
            zero_seq,
            ..UNLIMITED
        };
        let keep = |key: &[u8]| key < b"k06000".as_slice();
        let serial = compact_with(&views, keep, limits, &cuts, &mut MergeWorkspace::new()).unwrap();
        for threads in [1, 4] {
            let parallel = super::super::compact_parallel(
                &views,
                keep,
                limits,
                &cuts,
                threads,
                limits.target_run_len,
                &mut MergeWorkspace::new(),
            )
            .unwrap();
            assert_eq!(merged(&serial), merged(&parallel));
            assert_eq!(serial.stats, parallel.stats);
            assert!(parallel.stats.copies_collapsed > 0);
            assert!(parallel.workspace_bytes <= limits.max_workspace_bytes);
            if threads == 4 {
                assert!(parallel.workspace_bytes > serial.workspace_bytes);
            }
            for pair in parallel.runs.windows(2) {
                assert!(pair[0].key_range().unwrap().max < pair[1].key_range().unwrap().min);
            }
            for run in &parallel.runs {
                let range = run.key_range().unwrap();
                assert!(
                    !cuts
                        .at
                        .iter()
                        .any(|cut| range.min < cut.as_slice() && cut.as_slice() <= range.max)
                );
                assert!(
                    cuts.overlapped
                        .iter()
                        .filter(|key| range.min < key.as_slice() && key.as_slice() <= range.max)
                        .count()
                        <= cuts.max_overlapped
                );
            }
        }
    }
}

#[test]
fn subcompactions_share_limits_and_fall_back_with_tight_workspace() {
    let runs = parallel_inputs();
    let views: Vec<_> = runs.iter().map(Run::view).collect();
    let needed = super::super::compact::workspace_bytes(&views).unwrap();
    let limits = CompactLimits {
        max_workspace_bytes: needed,
        ..UNLIMITED
    };
    let out = super::super::compact_parallel(
        &views,
        |_| true,
        limits,
        &OutputCuts::default(),
        4,
        limits.target_run_len,
        &mut MergeWorkspace::new(),
    )
    .unwrap();
    assert_eq!(out.workspace_bytes, needed);
    let single = OutputCuts {
        single: true,
        ..OutputCuts::default()
    };
    assert_eq!(
        super::super::compact_parallel(
            &views,
            |_| true,
            UNLIMITED,
            &single,
            4,
            limits.target_run_len,
            &mut MergeWorkspace::new()
        )
        .unwrap()
        .runs
        .len(),
        1
    );
}

#[test]
fn subcompaction_splits_balance_whole_runs_and_leave_one_tail() {
    use super::super::compact::partition_starts;
    let keys: Vec<_> = (0..111u64).map(u64::to_be_bytes).collect();
    let boundaries: Vec<_> = keys.iter().map(|key| (key.as_slice(), 10)).collect();
    // Eleven full 100-byte runs and a 10-byte tail: 2, 3, 3, 3+tail.
    assert_eq!(
        partition_starts(&boundaries, 100, 4),
        [
            keys[19].as_slice(),
            keys[49].as_slice(),
            keys[79].as_slice()
        ]
    );
    let repeated = [
        (b"a".as_slice(), 80),
        (b"b".as_slice(), 60),
        (b"b".as_slice(), 60),
        (b"c".as_slice(), 20),
    ];
    assert_eq!(partition_starts(&repeated, 100, 2), [b"b".as_slice()]);
}

#[test]
fn fewer_than_two_estimated_full_runs_use_the_serial_builder() {
    let runs = parallel_inputs();
    let views: Vec<_> = runs.iter().map(Run::view).collect();
    let stored: usize = views.iter().map(|v| v.frame_bytes()).sum();
    for target in [stored, stored / 2 + 1] {
        let limits = CompactLimits {
            max_workspace_bytes: 64 << 20,
            ..UNLIMITED
        };
        let serial = compact_with(
            &views,
            |_| true,
            limits,
            &OutputCuts::default(),
            &mut MergeWorkspace::new(),
        )
        .unwrap();
        let parallel = super::super::compact_parallel(
            &views,
            |_| true,
            limits,
            &OutputCuts::default(),
            4,
            target,
            &mut MergeWorkspace::new(),
        )
        .unwrap();
        assert_eq!(parallel.workspace_bytes, serial.workspace_bytes);
        assert_eq!(parallel.runs.len(), serial.runs.len());
        assert_eq!(merged(&parallel), merged(&serial));
    }
}
