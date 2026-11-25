//! Latest-state reads over several runs.
//!
//! Expected results come from a map built only from the input records, which
//! keeps the highest sequence number per key, so they do not depend on merging.

use std::collections::BTreeMap;

use crate::block::{MAX_KEY_LEN, Value};
use crate::run::{
    Codec, CompactLimits, ConflictingCopies, CorruptRun, Encoding, LiveRecord, LogicalCursor,
    LogicalWorkspace, MergeWorkspace, ReadError, Run, RunBuilder, RunCorruption, RunSet, RunView,
    compact,
};

type Input = (Vec<u8>, u64, Option<Vec<u8>>);
type Live = (Vec<u8>, u64, Vec<u8>);

const CODECS: [Codec; 2] = [Codec::None, Codec::Zstd];
const RECORD_BOUND: usize = 200_000;
const RETAINED_BOUND: usize = 64 << 20;
const SMALL_BLOCK: usize = 1024;

fn rec(key: &[u8], seq: u64, value: Option<&[u8]>) -> Input {
    (key.to_vec(), seq, value.map(<[u8]>::to_vec))
}

fn put(key: &[u8], seq: u64, value: &[u8]) -> Input {
    rec(key, seq, Some(value))
}

fn del(key: &[u8], seq: u64) -> Input {
    rec(key, seq, None)
}

fn live(key: &[u8], seq: u64, value: &[u8]) -> Live {
    (key.to_vec(), seq, value.to_vec())
}

fn owned(record: LiveRecord<'_>) -> Live {
    (record.key.to_vec(), record.seq, record.value.to_vec())
}

/// Sorts into internal order, which a builder requires.
fn build(encoding: impl Into<Encoding>, records: &[Input], target: usize) -> Run<Vec<u8>> {
    let mut sorted: Vec<&Input> = records.iter().collect();
    sorted.sort_by(|a, b| a.0.cmp(&b.0).then(b.1.cmp(&a.1)));
    let mut builder = RunBuilder::with_raw_block_target(encoding, target);
    for (key, seq, value) in sorted {
        let value = value.as_deref().map_or(Value::Tombstone, Value::Bytes);
        builder.add(key, *seq, value).unwrap();
    }
    builder.finish()
}

fn build_all(inputs: &[Vec<Input>], target: usize) -> Vec<Run<Vec<u8>>> {
    inputs
        .iter()
        .enumerate()
        .map(|(i, records)| build(CODECS[i % CODECS.len()], records, target))
        .collect()
}

fn views(runs: &[Run<Vec<u8>>]) -> Vec<RunView<'_>> {
    runs.iter().map(Run::view).collect()
}

fn rest(cursor: &mut LogicalCursor<'_>) -> Result<Vec<Live>, ReadError> {
    let mut found = Vec::new();
    let mut retained = 0;
    while let Some(record) = cursor.next_record()? {
        retained += record.key.len() + record.value.len();
        found.push(owned(record));
        assert!(
            found.len() <= RECORD_BOUND && retained <= RETAINED_BOUND,
            "scan did not terminate"
        );
    }
    Ok(found)
}

fn scan(inputs: &[Vec<Input>]) -> Vec<Live> {
    let runs = build_all(inputs, SMALL_BLOCK);
    let views = views(&runs);
    let mut workspace = LogicalWorkspace::new();
    rest(&mut RunSet::new(&views).cursor(&mut workspace)).unwrap()
}

fn get(inputs: &[Vec<Input>], key: &[u8]) -> Result<Option<(u64, Vec<u8>)>, ReadError> {
    let runs = build_all(inputs, SMALL_BLOCK);
    let views = views(&runs);
    let mut workspace = LogicalWorkspace::new();
    let found = RunSet::new(&views).get(key, &mut workspace)?;
    Ok(found.map(|v| (v.seq, v.value.to_vec())))
}

/// The expected latest state, from the inputs alone: for each key, the record
/// with the highest sequence number. Inputs here hold no conflicts.
fn model(inputs: &[Vec<Input>]) -> BTreeMap<Vec<u8>, (u64, Option<Vec<u8>>)> {
    let mut latest: BTreeMap<Vec<u8>, (u64, Option<Vec<u8>>)> = BTreeMap::new();
    for (key, seq, value) in inputs.iter().flatten() {
        match latest.get(key) {
            Some((newest, _)) if newest >= seq => {}
            _ => {
                latest.insert(key.clone(), (*seq, value.clone()));
            }
        }
    }
    latest
}

fn model_live(latest: &BTreeMap<Vec<u8>, (u64, Option<Vec<u8>>)>) -> Vec<Live> {
    latest
        .iter()
        .filter_map(|(key, (seq, value))| Some((key.clone(), *seq, value.clone()?)))
        .collect()
}

fn frame_start(bytes: &[u8], frame: usize) -> usize {
    let footer = bytes.len() - 48;
    let table = u64::from_le_bytes(bytes[footer..footer + 8].try_into().unwrap()) as usize;
    let at = table + frame * 8;
    u32::from_le_bytes(bytes[at..at + 4].try_into().unwrap()) as usize
}

fn conflict_of(error: ReadError) -> ConflictingCopies {
    match error {
        ReadError::ConflictingCopies(conflict) => conflict,
        other => panic!("expected a conflict, got {other:?}"),
    }
}

fn numbered(count: usize) -> Vec<Input> {
    (0..count)
        .map(|i| put(format!("key:{i:05}").as_bytes(), i as u64 + 1, b"value"))
        .collect()
}

#[test]
fn an_empty_set_and_empty_runs_hold_nothing() {
    let mut workspace = LogicalWorkspace::new();
    let set = RunSet::new(&[]);
    assert_eq!(set.get(b"a", &mut workspace).unwrap(), None);
    let mut cursor = set.cursor(&mut workspace);
    assert_eq!(cursor.next_record().unwrap(), None);
    cursor.seek(b"a").unwrap();
    assert_eq!(cursor.next_record().unwrap(), None);
    cursor.seek_prefix(b"").unwrap();
    assert_eq!(cursor.next_record().unwrap(), None);

    let inputs = vec![Vec::new(), Vec::new()];
    assert_eq!(scan(&inputs), Vec::new());
    assert_eq!(get(&inputs, b"a").unwrap(), None);
}

#[test]
fn a_single_run_shows_only_its_latest_state() {
    let inputs = vec![vec![
        put(b"a", 3, b"newest"),
        put(b"a", 2, b"middle"),
        put(b"a", 1, b"oldest"),
        del(b"b", 2),
        put(b"b", 1, b"gone"),
        put(b"c", 1, b""),
    ]];
    let expected = vec![live(b"a", 3, b"newest"), live(b"c", 1, b"")];
    assert_eq!(scan(&inputs), expected);
    assert_eq!(get(&inputs, b"a").unwrap(), Some((3, b"newest".to_vec())));
    assert_eq!(get(&inputs, b"b").unwrap(), None);
    assert_eq!(
        get(&inputs, b"c").unwrap(),
        Some((1, Vec::new())),
        "empty value"
    );
    assert_eq!(get(&inputs, b"d").unwrap(), None);
}

#[test]
fn newer_versions_win_in_any_run_and_any_input_order() {
    let a = vec![put(b"k", 9, b"newest"), put(b"x", 1, b"x1")];
    let b = vec![put(b"k", 5, b"middle"), put(b"x", 7, b"x7")];
    let c = vec![put(b"k", 2, b"oldest"), put(b"y", 3, b"y3")];
    let expected = vec![
        live(b"k", 9, b"newest"),
        live(b"x", 7, b"x7"),
        live(b"y", 3, b"y3"),
    ];
    for order in [
        [&a, &b, &c],
        [&a, &c, &b],
        [&b, &a, &c],
        [&b, &c, &a],
        [&c, &a, &b],
        [&c, &b, &a],
    ] {
        let inputs: Vec<Vec<Input>> = order.into_iter().cloned().collect();
        assert_eq!(scan(&inputs), expected);
        assert_eq!(get(&inputs, b"k").unwrap(), Some((9, b"newest".to_vec())));
    }
}

#[test]
fn a_newest_tombstone_hides_older_values_in_other_runs() {
    let inputs = vec![
        vec![put(b"a", 1, b"old"), put(b"b", 1, b"kept")],
        vec![del(b"a", 2)],
    ];
    assert_eq!(scan(&inputs), vec![live(b"b", 1, b"kept")]);
    assert_eq!(get(&inputs, b"a").unwrap(), None);
    assert_eq!(get(&inputs, b"b").unwrap(), Some((1, b"kept".to_vec())));
}

#[test]
fn an_older_tombstone_does_not_hide_a_newer_value() {
    let inputs = vec![vec![del(b"a", 1)], vec![put(b"a", 2, b"new")]];
    assert_eq!(scan(&inputs), vec![live(b"a", 2, b"new")]);
    assert_eq!(get(&inputs, b"a").unwrap(), Some((2, b"new".to_vec())));
}

#[test]
fn a_key_can_be_reinserted_after_deletion() {
    let inputs = vec![
        vec![put(b"a", 1, b"first")],
        vec![del(b"a", 2)],
        vec![put(b"a", 3, b"again")],
        vec![
            del(b"b", 4),
            put(b"b", 3, b"x"),
            del(b"b", 2),
            put(b"b", 1, b"y"),
        ],
    ];
    assert_eq!(scan(&inputs), vec![live(b"a", 3, b"again")]);
    assert_eq!(get(&inputs, b"a").unwrap(), Some((3, b"again".to_vec())));
    assert_eq!(get(&inputs, b"b").unwrap(), None);
}

#[test]
fn every_version_of_a_deleted_key_is_skipped_before_the_next_key() {
    let inputs = vec![
        vec![del(b"a", 9), put(b"a", 7, b"x"), put(b"c", 1, b"c")],
        vec![put(b"a", 8, b"y"), put(b"a", 5, b"z"), put(b"b", 2, b"b")],
    ];
    assert_eq!(
        scan(&inputs),
        vec![live(b"b", 2, b"b"), live(b"c", 1, b"c")]
    );
}

#[test]
fn disjoint_and_overlapping_runs_read_alike() {
    let records = numbered(900);
    let disjoint = vec![
        records[600..].to_vec(),
        records[..300].to_vec(),
        records[300..600].to_vec(),
    ];
    let mut overlapping = vec![Vec::new(), Vec::new(), Vec::new()];
    for (index, record) in records.iter().enumerate() {
        overlapping[index % 3].push(record.clone());
    }
    let expected = model_live(&model(std::slice::from_ref(&records)));
    assert_eq!(scan(&disjoint), expected);
    assert_eq!(scan(&overlapping), expected);
}

#[test]
fn identical_copies_count_once() {
    for value in [Some(&b"same"[..]), Some(&b""[..]), None] {
        let copy = rec(b"k", 5, value);
        let older = put(b"k", 1, b"old");
        let inputs = vec![
            vec![copy.clone(), older.clone()],
            vec![copy.clone()],
            vec![copy.clone(), put(b"z", 1, b"z")],
        ];
        let mut expected = Vec::new();
        if let Some(value) = value {
            expected.push(live(b"k", 5, value));
        }
        expected.push(live(b"z", 1, b"z"));
        assert_eq!(scan(&inputs), expected, "value {value:?}");
        assert_eq!(get(&inputs, b"k").unwrap(), value.map(|v| (5, v.to_vec())));
    }
}

#[test]
fn identical_copies_of_many_keys_in_many_runs_collapse() {
    let records = numbered(200);
    let inputs = vec![records; 7];
    let expected = model_live(&model(&inputs));
    assert_eq!(scan(&inputs), expected);
}

fn check_conflict(inputs: &[Vec<Input>], key: &[u8], seq: u64) -> ConflictingCopies {
    let runs = build_all(inputs, SMALL_BLOCK);
    let views = views(&runs);
    let set = RunSet::new(&views);
    let mut workspace = LogicalWorkspace::new();
    let from_get = conflict_of(set.get(key, &mut workspace).unwrap_err());
    let mut cursor = set.cursor(&mut workspace);
    let error = loop {
        match cursor.next_record() {
            Ok(Some(_)) => {}
            Ok(None) => panic!("the conflict must be reported"),
            Err(error) => break error,
        }
    };
    let from_scan = conflict_of(error.clone());
    assert_eq!(
        cursor.next_record().unwrap_err(),
        error,
        "the failure persists"
    );
    for conflict in [&from_get, &from_scan] {
        assert_eq!((conflict.key.as_slice(), conflict.seq), (key, seq));
        assert!(conflict.inputs.0 < conflict.inputs.1);
    }
    from_scan
}

#[test]
fn copies_with_different_values_are_rejected_whatever_the_input_order() {
    let a = vec![put(b"k", 5, b"one"), put(b"k", 1, b"older")];
    let b = vec![put(b"k", 5, b"two")];
    let c = vec![put(b"a", 1, b"before"), put(b"z", 1, b"after")];
    for inputs in [
        vec![a.clone(), b.clone(), c.clone()],
        vec![b.clone(), a.clone(), c.clone()],
        vec![c.clone(), b.clone(), a.clone()],
    ] {
        check_conflict(&inputs, b"k", 5);
    }
}

#[test]
fn a_tombstone_conflicts_with_a_value_and_with_an_empty_value() {
    let tomb = vec![del(b"k", 5)];
    let value = vec![put(b"k", 5, b"v")];
    let empty = vec![put(b"k", 5, b"")];
    check_conflict(&[tomb.clone(), value.clone()], b"k", 5);
    check_conflict(&[value, tomb.clone()], b"k", 5);
    check_conflict(&[empty.clone(), tomb.clone()], b"k", 5);
    check_conflict(&[tomb, empty], b"k", 5);
}

#[test]
fn a_conflict_among_identical_copies_is_still_found() {
    let same = vec![put(b"k", 5, b"same")];
    let other = vec![put(b"k", 5, b"other")];
    for inputs in [
        vec![same.clone(), same.clone(), other.clone()],
        vec![same.clone(), other.clone(), same.clone()],
        vec![other.clone(), same.clone(), same.clone()],
    ] {
        check_conflict(&inputs, b"k", 5);
    }
    let many: Vec<Vec<Input>> = (0..9)
        .map(|i| if i == 8 { other.clone() } else { same.clone() })
        .collect();
    assert_eq!(check_conflict(&many, b"k", 5).inputs.1, 8);
}

#[test]
fn a_conflict_reports_a_seekable_failure() {
    let inputs = vec![
        vec![put(b"a", 1, b"a"), put(b"k", 5, b"one"), put(b"z", 1, b"z")],
        vec![put(b"k", 5, b"two")],
    ];
    let runs = build_all(&inputs, SMALL_BLOCK);
    let views = views(&runs);
    let mut workspace = LogicalWorkspace::new();
    let mut cursor = RunSet::new(&views).cursor(&mut workspace);
    assert_eq!(
        owned(cursor.next_record().unwrap().unwrap()),
        live(b"a", 1, b"a")
    );
    assert!(matches!(
        cursor.next_record(),
        Err(ReadError::ConflictingCopies(_))
    ));
    assert!(cursor.next_record().is_err());
    cursor.seek(b"m").unwrap();
    assert_eq!(rest(&mut cursor).unwrap(), vec![live(b"z", 1, b"z")]);
    cursor.seek_prefix(b"k").unwrap();
    assert!(cursor.next_record().is_err());
}

#[test]
fn a_conflict_below_the_newest_version_cannot_affect_a_result() {
    let inputs = vec![
        vec![put(b"k", 9, b"newest"), put(b"k", 5, b"one")],
        vec![put(b"k", 5, b"two")],
    ];
    assert_eq!(scan(&inputs), vec![live(b"k", 9, b"newest")]);
    assert_eq!(get(&inputs, b"k").unwrap(), Some((9, b"newest".to_vec())));
}

#[test]
fn a_conflict_superseded_in_a_later_run_does_not_fail_any_input_order() {
    let one = vec![put(b"k", 5, b"one")];
    let two = vec![put(b"k", 5, b"two")];
    let newest = vec![put(b"k", 7, b"newest")];
    for order in [
        [&one, &two, &newest],
        [&one, &newest, &two],
        [&two, &one, &newest],
        [&two, &newest, &one],
        [&newest, &one, &two],
        [&newest, &two, &one],
    ] {
        let inputs: Vec<Vec<Input>> = order.into_iter().cloned().collect();
        assert_eq!(scan(&inputs), vec![live(b"k", 7, b"newest")]);
        assert_eq!(get(&inputs, b"k").unwrap(), Some((7, b"newest".to_vec())));
    }
}

#[test]
fn seeks_and_prefixes_return_each_live_key_once() {
    let inputs = vec![
        vec![
            put(b"app/1", 3, b"a3"),
            put(b"app/1", 1, b"a1"),
            del(b"app/2", 4),
            put(b"bee", 1, b"b"),
        ],
        vec![
            put(b"app/2", 2, b"gone"),
            put(b"app/3", 1, b"c"),
            put(b"apple", 2, b"d"),
        ],
    ];
    let runs = build_all(&inputs, SMALL_BLOCK);
    let views = views(&runs);
    let mut workspace = LogicalWorkspace::new();
    let mut cursor = RunSet::new(&views).cursor(&mut workspace);

    cursor.seek_prefix(b"app/").unwrap();
    let found: Vec<Vec<u8>> = rest(&mut cursor)
        .unwrap()
        .into_iter()
        .map(|r| r.0)
        .collect();
    assert_eq!(found, [b"app/1".to_vec(), b"app/3".to_vec()]);

    cursor.seek(b"app/2").unwrap();
    let found: Vec<Vec<u8>> = rest(&mut cursor)
        .unwrap()
        .into_iter()
        .map(|r| r.0)
        .collect();
    assert_eq!(
        found,
        [b"app/3".to_vec(), b"apple".to_vec(), b"bee".to_vec()]
    );

    cursor.seek_prefix(b"nothing").unwrap();
    assert_eq!(cursor.next_record().unwrap(), None);
    cursor.seek_prefix(&vec![b'a'; MAX_KEY_LEN + 1]).unwrap();
    assert_eq!(cursor.next_record().unwrap(), None);
}

#[test]
fn binary_keys_and_0xff_prefixes_work() {
    let keys: [&[u8]; 8] = [
        &[],
        &[0x00],
        &[0x00, 0xFF],
        &[0xFE, 0xFF],
        &[0xFF],
        &[0xFF, 0x00],
        &[0xFF, 0xFF],
        &[0xFF, 0xFF, 0xFF],
    ];
    let inputs: Vec<Vec<Input>> = vec![
        keys.iter().map(|k| put(k, 1, b"old")).collect(),
        keys.iter().step_by(2).map(|k| put(k, 2, k)).collect(),
        keys.iter().skip(1).step_by(4).map(|k| del(k, 3)).collect(),
    ];
    let latest = model(&inputs);
    let expected = model_live(&latest);
    let runs = build_all(&inputs, SMALL_BLOCK);
    let views = views(&runs);
    let set = RunSet::new(&views);
    let mut workspace = LogicalWorkspace::new();
    for prefix in [
        &[][..],
        &[0x00],
        &[0xFE],
        &[0xFF],
        &[0xFF, 0xFF],
        &[0xFF, 0xFF, 0xFF, 0xFF],
    ] {
        let mut cursor = set.cursor(&mut workspace);
        cursor.seek_prefix(prefix).unwrap();
        let wanted: Vec<Live> = expected
            .iter()
            .filter(|r| r.0.starts_with(prefix))
            .cloned()
            .collect();
        assert_eq!(rest(&mut cursor).unwrap(), wanted, "prefix {prefix:02x?}");
    }
    for key in keys {
        let want = latest[key].1.clone();
        let found = set.get(key, &mut workspace).unwrap();
        assert_eq!(found.map(|v| v.value.to_vec()), want, "key {key:02x?}");
    }
}

#[test]
fn long_version_chains_across_blocks_and_runs_resolve_to_one_record() {
    let mut inputs = vec![Vec::new(), Vec::new(), Vec::new()];
    for seq in 1..=3000u64 {
        let value = format!("version-{seq:06}");
        inputs[seq as usize % 3].push(put(b"chain", seq, value.as_bytes()));
    }
    for input in &mut inputs {
        input.push(put(b"next", 1, b"after"));
    }
    let runs = build_all(&inputs, 256);
    assert!(runs.iter().all(|run| run.block_count() > 2));
    let views = views(&runs);
    let set = RunSet::new(&views);
    let mut workspace = LogicalWorkspace::new();
    let found = set.get(b"chain", &mut workspace).unwrap().unwrap();
    assert_eq!((found.seq, found.value), (3000, &b"version-003000"[..]));
    let mut cursor = set.cursor(&mut workspace);
    assert_eq!(
        rest(&mut cursor).unwrap(),
        vec![
            live(b"chain", 3000, b"version-003000"),
            live(b"next", 1, b"after"),
        ]
    );
    cursor.seek_prefix(b"chain").unwrap();
    assert_eq!(rest(&mut cursor).unwrap().len(), 1);
}

#[test]
fn mixed_codecs_and_block_sizes_agree_with_the_model() {
    let encodings = [
        Codec::None.into(),
        Codec::Zstd.into(),
        Encoding {
            codec: Codec::Zstd,
            raw_literals: true,
        },
    ];
    let mut rng = Rng(11);
    let inputs: Vec<Vec<Input>> = (0..6).map(|_| random_input(&mut rng, 300)).collect();
    let expected = model_live(&model(&inputs));
    let runs: Vec<_> = inputs
        .iter()
        .enumerate()
        .map(|(i, input)| build(encodings[i % 3], input, [128, 1024, 8192][(i + i / 3) % 3]))
        .collect();
    let mixed = views(&runs);
    let mut workspace = LogicalWorkspace::new();
    let mut cursor = RunSet::new(&mixed).cursor(&mut workspace);
    assert_eq!(rest(&mut cursor).unwrap(), expected);
    // Compaction of the mix into each encoding, and of that output into
    // every other, reads the same.
    for encoding in encodings {
        let limits = |encoding| CompactLimits {
            target_run_len: usize::MAX,
            max_workspace_bytes: usize::MAX,
            encoding,
            target_block_compressed: 256,
            zero_seq: false,
            copy_forward: false,
        };
        let mut merge = MergeWorkspace::new();
        let once = compact(&mixed, |_| false, limits(encoding), &mut merge).unwrap();
        let once = views(&once.runs);
        for again in encodings {
            let twice = compact(&once, |_| false, limits(again), &mut merge).unwrap();
            for out in [once.clone(), views(&twice.runs)] {
                let mut cursor = RunSet::new(&out).cursor(&mut workspace);
                assert_eq!(
                    rest(&mut cursor).unwrap(),
                    expected,
                    "{encoding:?}, {again:?}"
                );
            }
        }
    }
}

#[test]
fn cursors_reposition_and_stay_exhausted_until_moved() {
    let records = numbered(120);
    let inputs = vec![records[..80].to_vec(), records[40..].to_vec()];
    let runs = build_all(&inputs, SMALL_BLOCK);
    let views = views(&runs);
    let expected = model_live(&model(&inputs));
    let mut workspace = LogicalWorkspace::new();
    let mut cursor = RunSet::new(&views).cursor(&mut workspace);

    assert_eq!(rest(&mut cursor).unwrap(), expected);
    assert_eq!(cursor.next_record().unwrap(), None);
    assert_eq!(cursor.next_record().unwrap(), None);

    // A key decided before a seek must not hide the same key after it.
    cursor.seek(b"key:00050").unwrap();
    assert_eq!(owned(cursor.next_record().unwrap().unwrap()), expected[50]);
    cursor.seek(b"key:00050").unwrap();
    assert_eq!(owned(cursor.next_record().unwrap().unwrap()), expected[50]);
    cursor.seek_prefix(b"key:00050").unwrap();
    assert_eq!(owned(cursor.next_record().unwrap().unwrap()), expected[50]);
    assert_eq!(cursor.next_record().unwrap(), None);
}

#[test]
fn the_empty_key_is_a_key() {
    let inputs = vec![vec![put(b"", 1, b"old")], vec![put(b"", 2, b"new")]];
    assert_eq!(scan(&inputs), vec![live(b"", 2, b"new")]);
    assert_eq!(get(&inputs, b"").unwrap(), Some((2, b"new".to_vec())));
    let deleted = vec![
        vec![put(b"", 1, b"old"), put(b"a", 1, b"a")],
        vec![del(b"", 2)],
    ];
    assert_eq!(scan(&deleted), vec![live(b"a", 1, b"a")]);
}

fn damaged(records: &[Input], frame: usize) -> (Vec<u8>, CorruptRun) {
    let mut bytes = build(Codec::Zstd, records, SMALL_BLOCK).as_bytes().to_vec();
    let offset = frame_start(&bytes, frame);
    bytes[offset + 12] ^= 0xFF;
    let error = CorruptRun {
        offset,
        kind: RunCorruption::ChecksumMismatch,
    };
    (bytes, error)
}

#[test]
fn a_damaged_run_fails_only_lookups_that_read_it() {
    let records = numbered(600);
    let (bytes, error) = damaged(&records, 1);
    let bad = RunView::open(&bytes).unwrap();
    let good = build(Codec::Zstd, &[put(b"zz", 1, b"elsewhere")], SMALL_BLOCK);
    let views = [good.view(), bad];
    let set = RunSet::new(&views);
    let inside = records
        .iter()
        .find(|r| bad.locate(&r.0).unwrap() == Some(1))
        .unwrap();
    let mut workspace = LogicalWorkspace::new();
    assert_eq!(
        set.get(&inside.0, &mut workspace),
        Err(ReadError::Corrupt(error))
    );
    assert_eq!(
        set.get(b"zz", &mut workspace).unwrap().unwrap().value,
        b"elsewhere",
        "above every key of the damaged run"
    );
    assert!(set.get(&records[0].0, &mut workspace).unwrap().is_some());
}

#[test]
fn a_scan_stops_at_damage_until_a_good_seek() {
    let records = numbered(600);
    let (bytes, error) = damaged(&records, 1);
    let views = [RunView::open(&bytes).unwrap()];
    let mut workspace = LogicalWorkspace::new();
    let mut cursor = RunSet::new(&views).cursor(&mut workspace);
    let mut count = 0;
    let found = loop {
        match cursor.next_record() {
            Ok(Some(_)) => count += 1,
            Ok(None) => panic!("the scan must reach the damage"),
            Err(found) => break found,
        }
        assert!(count <= records.len());
    };
    assert_eq!(found, ReadError::Corrupt(error));
    assert_eq!(cursor.next_record(), Err(ReadError::Corrupt(error)));
    cursor.seek(&records[0].0).unwrap();
    assert_eq!(cursor.next_record().unwrap().map(|r| r.seq), Some(1));
}

#[test]
fn a_failed_seek_is_reported_until_a_later_one_succeeds() {
    let records = numbered(600);
    let (bytes, error) = damaged(&records, 1);
    let bad = RunView::open(&bytes).unwrap();
    let inside = records
        .iter()
        .find(|r| bad.locate(&r.0).unwrap() == Some(1))
        .unwrap()
        .0
        .clone();
    let views = [bad];
    let mut workspace = LogicalWorkspace::new();
    let mut cursor = RunSet::new(&views).cursor(&mut workspace);
    assert_eq!(cursor.seek(&inside), Err(ReadError::Corrupt(error)));
    assert_eq!(cursor.next_record(), Err(ReadError::Corrupt(error)));
    cursor.seek(&records[0].0).unwrap();
    assert!(cursor.next_record().unwrap().is_some());
}

#[test]
fn errors_describe_themselves() {
    let error = ReadError::ConflictingCopies(ConflictingCopies {
        key: b"k".to_vec(),
        seq: 7,
        inputs: (1, 3),
    });
    let text = error.to_string();
    assert!(
        text.contains("runs 1 and 3") && text.contains("sequence 7"),
        "{text}"
    );
}

#[test]
fn the_workspace_reports_and_reuses_what_it_retains() {
    let records = numbered(400);
    let runs = build_all(std::slice::from_ref(&records), SMALL_BLOCK);
    let views = [runs[0].view()];
    let mut workspace = LogicalWorkspace::new();
    assert_eq!((workspace.capacity(), workspace.blocks_loaded()), (0, 0));
    let set = RunSet::new(&views);
    assert_eq!(rest(&mut set.cursor(&mut workspace)).unwrap().len(), 400);
    assert_eq!(workspace.blocks_loaded(), runs[0].block_count() as u64);
    // Codec None reads in place, so build a compressed one to see the buffer.
    let zstd = build(Codec::Zstd, &records, SMALL_BLOCK);
    let views = [zstd.view()];
    let set = RunSet::new(&views);
    let mut workspace = LogicalWorkspace::new();
    assert_eq!(rest(&mut set.cursor(&mut workspace)).unwrap().len(), 400);
    assert!(workspace.capacity() >= SMALL_BLOCK);
}

/// Deterministic SplitMix64, so failures reproduce from the seed alone.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^ (z >> 31)
    }

    fn below(&mut self, bound: usize) -> usize {
        (self.next() % bound as u64) as usize
    }
}

const ALPHABET: [u8; 5] = [0x00, 0x01, b'a', 0xFE, 0xFF];

fn random_bytes(rng: &mut Rng, max_len: usize) -> Vec<u8> {
    (0..rng.below(max_len))
        .map(|_| ALPHABET[rng.below(ALPHABET.len())])
        .collect()
}

/// The record for an update is a function of its key and sequence number, so
/// independent inputs produce identical copies and never conflict.
fn update(key: &[u8], seq: u64) -> Input {
    let hash = key
        .iter()
        .fold(seq, |h, &b| h.wrapping_mul(31) ^ u64::from(b));
    let value = (hash % 4 != 0).then(|| vec![hash as u8; (hash % 24) as usize]);
    (key.to_vec(), seq, value)
}

fn random_input(rng: &mut Rng, target: usize) -> Vec<Input> {
    let mut records = BTreeMap::new();
    while records.len() < target {
        let key = random_bytes(rng, 6);
        let versions = if rng.below(16) == 0 { 40 } else { 3 };
        for _ in 0..rng.below(versions) + 1 {
            let seq = rng.below(64) as u64;
            records.insert((key.clone(), seq), update(&key, seq));
        }
    }
    records.into_values().collect()
}

#[test]
fn logical_reads_match_a_reference_model() {
    for seed in 0..48 {
        let mut rng = Rng(7000 + seed);
        let count = rng.below(7);
        let inputs: Vec<Vec<Input>> = (0..count)
            .map(|_| {
                let size = rng.below(400);
                if size < 20 {
                    Vec::new()
                } else {
                    random_input(&mut rng, size)
                }
            })
            .collect();
        let latest = model(&inputs);
        let expected = model_live(&latest);
        let runs: Vec<_> = inputs
            .iter()
            .map(|input| {
                build(
                    CODECS[rng.below(CODECS.len())],
                    input,
                    [64, 256, 4096][rng.below(3)],
                )
            })
            .collect();
        let views = views(&runs);
        let set = RunSet::new(&views);
        let mut workspace = LogicalWorkspace::new();

        assert_eq!(
            rest(&mut set.cursor(&mut workspace)).unwrap(),
            expected,
            "seed {seed}"
        );

        for (key, (seq, value)) in &latest {
            let found = set.get(key, &mut workspace).unwrap();
            let want = value.as_deref().map(|v| (*seq, v));
            assert_eq!(
                found.map(|v| (v.seq, v.value)),
                want,
                "seed {seed} key {key:02x?}"
            );
        }
        for _ in 0..40 {
            let probe = random_bytes(&mut rng, 8);
            let found = set
                .get(&probe, &mut workspace)
                .unwrap()
                .map(|v| v.value.to_vec());
            let want = latest.get(&probe).and_then(|(_, v)| v.clone());
            assert_eq!(found, want, "seed {seed} probe {probe:02x?}");
        }

        let mut cursor = set.cursor(&mut workspace);
        for _ in 0..80 {
            let probe = if rng.below(3) == 0 && !expected.is_empty() {
                let key = &expected[rng.below(expected.len())].0;
                key[..rng.below(key.len() + 1)].to_vec()
            } else {
                random_bytes(&mut rng, 7)
            };
            let use_prefix = rng.below(2) == 0;
            let wanted: Vec<&Live> = if use_prefix {
                cursor.seek_prefix(&probe).unwrap();
                expected
                    .iter()
                    .filter(|r| r.0.starts_with(&probe))
                    .collect()
            } else {
                cursor.seek(&probe).unwrap();
                expected.iter().filter(|r| r.0 >= probe).collect()
            };
            let limit = match rng.below(3) {
                0 => wanted.len(),
                1 => rng.below(wanted.len() + 1),
                _ => wanted.len().min(rng.below(8)),
            };
            for want in &wanted[..limit] {
                let found = cursor.next_record().unwrap().map(owned);
                assert_eq!(
                    found.as_ref(),
                    Some(*want),
                    "seed {seed} probe {probe:02x?}"
                );
            }
            if limit == wanted.len() {
                assert_eq!(cursor.next_record().unwrap(), None, "seed {seed}");
            }
        }
    }
}

// Sequence-bound pruning: lookups that skip runs whose largest sequence
// number is below a version already found.

/// Reads `key` from `runs` in the order given, each bounded by its largest
/// sequence number, and returns the result and the blocks decoded.
type Found = Result<Option<(u64, Vec<u8>)>, ReadError>;

fn get_bounded(runs: &[Run<Vec<u8>>], key: &[u8]) -> (Found, u64) {
    let mut workspace = LogicalWorkspace::new();
    let found = crate::run::latest_in(runs, key, &mut workspace)
        .map(|found| found.map(|found| (found.seq, found.value.to_vec())));
    (found, workspace.blocks_loaded())
}

#[test]
fn runs_know_their_largest_sequence_number_however_they_were_made() {
    let records = [put(b"a", 4, b"x"), del(b"b", 9), put(b"c", 2, b"y")];
    let built = build(Codec::Zstd, &records, SMALL_BLOCK);
    assert_eq!(built.max_seq(), 9);
    assert_eq!(Run::new(built.as_bytes().to_vec()).unwrap().max_seq(), 9);
    let (recovered, max_seq) = Run::recover(built.as_bytes().to_vec()).unwrap();
    assert_eq!((recovered.max_seq(), max_seq), (9, 9));
    // Tombstones count; a run of nothing has 0.
    assert_eq!(
        build(Codec::None, &[del(b"a", 5)], SMALL_BLOCK).max_seq(),
        5
    );
    assert_eq!(RunBuilder::new(Codec::None).finish().max_seq(), 0);
}

#[test]
fn a_found_version_skips_runs_holding_only_older_ones() {
    let runs: Vec<_> = [30, 20, 10]
        .map(|seq| build(Codec::Zstd, &[put(b"k", seq, b"v")], SMALL_BLOCK))
        .into_iter()
        .collect();
    // Newest first, the older runs are not decoded; in any other order the
    // result is the same and only the saving is lost.
    assert_eq!(get_bounded(&runs, b"k"), (Ok(Some((30, b"v".to_vec()))), 1));
    let reversed: Vec<_> = runs
        .iter()
        .rev()
        .map(|run| Run::new(run.as_bytes().to_vec()).unwrap())
        .collect();
    assert_eq!(
        get_bounded(&reversed, b"k").0,
        Ok(Some((30, b"v".to_vec())))
    );

    // A newest tombstone ends the search the same way.
    let deleted = [
        build(Codec::Zstd, &[del(b"k", 20)], SMALL_BLOCK),
        build(Codec::None, &[put(b"k", 10, b"old")], SMALL_BLOCK),
    ];
    assert_eq!(get_bounded(&deleted, b"k"), (Ok(None), 1));
}

#[test]
fn interleaved_sequence_ranges_are_read_until_no_run_can_be_newer() {
    // The first run holds the oldest version of "k" but the largest sequence
    // number overall; the second holds the newest version. A lookup that
    // stopped at its first version would return seq 1.
    let runs = [
        build(
            Codec::Zstd,
            &[put(b"k", 1, b"old"), put(b"z", 10, b"z")],
            SMALL_BLOCK,
        ),
        build(Codec::Zstd, &[put(b"k", 5, b"new")], SMALL_BLOCK),
        build(Codec::None, &[put(b"k", 3, b"mid")], SMALL_BLOCK),
    ];
    // The third run's bound, 3, is below 5, so it is not decoded.
    assert_eq!(
        get_bounded(&runs, b"k"),
        (Ok(Some((5, b"new".to_vec()))), 2)
    );
}

#[test]
fn a_copy_of_the_newest_version_is_still_checked() {
    // The second run's bound equals the version found, so it is read.
    let conflicting = [
        build(
            Codec::Zstd,
            &[put(b"k", 7, b"x"), put(b"z", 9, b"z")],
            SMALL_BLOCK,
        ),
        build(Codec::Zstd, &[put(b"k", 7, b"y")], SMALL_BLOCK),
    ];
    let error = get_bounded(&conflicting, b"k").0.unwrap_err();
    assert_eq!(conflict_of(error).seq, 7);
    let identical = [
        build(
            Codec::Zstd,
            &[put(b"k", 7, b"x"), put(b"z", 9, b"z")],
            SMALL_BLOCK,
        ),
        build(Codec::Zstd, &[put(b"k", 7, b"x")], SMALL_BLOCK),
    ];
    assert_eq!(
        get_bounded(&identical, b"k"),
        (Ok(Some((7, b"x".to_vec()))), 2)
    );
    // An older version cannot decide the lookup, so runs holding only
    // older versions are skipped.
    let older = [
        build(Codec::Zstd, &[put(b"k", 8, b"new")], SMALL_BLOCK),
        build(Codec::Zstd, &[put(b"k", 3, b"p")], SMALL_BLOCK),
        build(Codec::Zstd, &[put(b"k", 3, b"q")], SMALL_BLOCK),
    ];
    assert_eq!(
        get_bounded(&older, b"k"),
        (Ok(Some((8, b"new".to_vec()))), 1)
    );
}
