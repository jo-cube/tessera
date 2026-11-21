//! Run key ranges and the reads they let skip decoding.
//!
//! Expected results come from a map built only from the input records, and
//! expected decode counts from the input records' key ranges.

use std::collections::BTreeMap;

use crate::block::{MAX_KEY_LEN, Value};
use crate::run::{
    Codec, KeyRange, LogicalCursor, LogicalWorkspace, MergeCursor, MergeWorkspace, ReadError,
    ReadWorkspace, Run, RunBuilder, RunCorruption, RunSet, RunView,
};

type Input = (Vec<u8>, u64, Option<Vec<u8>>);

const CODECS: [Codec; 2] = [Codec::None, Codec::Zstd];
const FOOTER: usize = 48;
const SMALL_BLOCK: usize = 512;
const RECORD_BOUND: usize = 200_000;

fn put(key: &[u8], seq: u64, value: &[u8]) -> Input {
    (key.to_vec(), seq, Some(value.to_vec()))
}

fn del(key: &[u8], seq: u64) -> Input {
    (key.to_vec(), seq, None)
}

fn build(codec: Codec, records: &[Input], target: usize) -> Run<Vec<u8>> {
    let mut sorted: Vec<&Input> = records.iter().collect();
    sorted.sort_by(|a, b| a.0.cmp(&b.0).then(b.1.cmp(&a.1)));
    let mut builder = RunBuilder::with_raw_block_target(codec, target);
    for (key, seq, value) in sorted {
        let value = value.as_deref().map_or(Value::Tombstone, Value::Bytes);
        builder.add(key, *seq, value).unwrap();
    }
    builder.finish()
}

fn numbered(range: std::ops::Range<usize>) -> Vec<Input> {
    range
        .map(|i| put(format!("key:{i:05}").as_bytes(), i as u64 + 1, b"value"))
        .collect()
}

fn range_of(run: &Run<Vec<u8>>) -> Option<(Vec<u8>, Vec<u8>)> {
    run.key_range()
        .map(|KeyRange { min, max }| (min.to_vec(), max.to_vec()))
}

fn rest(cursor: &mut LogicalCursor<'_>) -> Result<Vec<Vec<u8>>, ReadError> {
    let mut keys = Vec::new();
    while let Some(record) = cursor.next_record()? {
        keys.push(record.key.to_vec());
        assert!(keys.len() <= RECORD_BOUND, "scan did not terminate");
    }
    Ok(keys)
}

/// The byte offsets of a finished run's parts.
struct Layout {
    table: usize,
    keys: usize,
    footer: usize,
    min_len: usize,
    first_frame_payload: usize,
}

fn layout(bytes: &[u8]) -> Layout {
    let footer = bytes.len() - FOOTER;
    let u64_at = |at: usize| u32::from_le_bytes(bytes[at..at + 4].try_into().unwrap()) as usize;
    let u32_at = |at: usize| u32::from_le_bytes(bytes[at..at + 4].try_into().unwrap()) as usize;
    let table = u64_at(footer);
    Layout {
        table,
        keys: table + u32_at(footer + 16) * 8,
        footer,
        min_len: u32_at(footer + 20),
        first_frame_payload: 12,
    }
}

fn put_u32(bytes: &mut [u8], at: usize, value: u32) {
    bytes[at..at + 4].copy_from_slice(&value.to_le_bytes());
}

fn kind_of(error: crate::run::CorruptRun) -> RunCorruption {
    error.kind
}

#[test]
fn an_empty_run_has_no_range_and_no_stored_key() {
    let run = build(Codec::Zstd, &[], SMALL_BLOCK);
    assert_eq!(run.key_range(), None);
    assert_eq!(run.as_bytes().len(), FOOTER);
    assert_eq!(layout(&run.as_bytes()).min_len, 0);
    assert_eq!(Run::new(run.as_bytes()).unwrap().key_range(), None);
}

#[test]
fn a_run_reports_its_smallest_and_largest_user_keys() {
    for codec in CODECS {
        let run = build(codec, &numbered(0..500), SMALL_BLOCK);
        assert!(run.block_count() > 2);
        let expected = (b"key:00000".to_vec(), b"key:00499".to_vec());
        assert_eq!(range_of(&run), Some(expected.clone()), "builder");
        assert_eq!(
            range_of(&Run::new(run.as_bytes().to_vec()).unwrap()),
            Some(expected),
            "reloaded"
        );
    }
}

#[test]
fn versions_and_tombstones_bound_the_range_like_any_record() {
    let run = build(
        Codec::Zstd,
        &[
            del(b"a", 9),
            put(b"a", 1, b"x"),
            put(b"z", 3, b"y"),
            put(b"z", 2, b"w"),
        ],
        SMALL_BLOCK,
    );
    assert_eq!(range_of(&run), Some((b"a".to_vec(), b"z".to_vec())));
}

#[test]
fn the_empty_key_and_binary_keys_are_ordinary_bounds() {
    let run = build(
        Codec::None,
        &[put(b"", 1, b"e"), put(&[0xFF, 0xFF], 1, b"f")],
        SMALL_BLOCK,
    );
    assert_eq!(range_of(&run), Some((Vec::new(), vec![0xFF, 0xFF])));
    assert_eq!(layout(&run.as_bytes()).min_len, 0);
    let reloaded = Run::new(run.as_bytes().to_vec()).unwrap();
    assert_eq!(range_of(&reloaded), Some((Vec::new(), vec![0xFF, 0xFF])));

    let single = build(Codec::None, &[put(&[0x00, 0xFF], 1, b"v")], SMALL_BLOCK);
    assert_eq!(
        range_of(&single),
        Some((vec![0x00, 0xFF], vec![0x00, 0xFF]))
    );
}

#[test]
fn the_minimum_key_is_stored_once_ahead_of_the_block_keys() {
    let run = build(Codec::None, &numbered(0..3), 1);
    let bytes = run.as_bytes();
    let l = layout(&bytes);
    assert_eq!(l.min_len, 9);
    assert_eq!(&bytes[l.keys..l.keys + 9], b"key:00000");
    // One minimum key, then one last key per block.
    assert_eq!(l.footer - l.keys, 9 + (2 + 9) * 2 + (2 + 1));
    assert_eq!(
        run.view().index_len(),
        8 * run.block_count() + 9 + (2 + 9) * 2 + (2 + 1)
    );
}

#[test]
fn reading_a_range_touches_no_frame() {
    let mut bytes = build(Codec::Zstd, &numbered(0..500), SMALL_BLOCK)
        .as_bytes()
        .to_vec();
    let l = layout(&bytes);
    for byte in &mut bytes[..l.table] {
        *byte ^= 0xFF;
    }
    let view = RunView::open(&bytes).unwrap();
    let range = view.key_range().unwrap().unwrap();
    assert_eq!(range.min, b"key:00000");
    assert_eq!(range.max, b"key:00499");
}

#[test]
fn a_minimum_key_differing_from_the_first_record_is_rejected() {
    for (name, damage) in [
        (
            "lowered",
            (|b: &mut [u8], at: usize| b[at] -= 1) as fn(&mut [u8], usize),
        ),
        ("raised within the first block", |b: &mut [u8], at| {
            b[at + 8] += 1
        }),
    ] {
        let mut bytes = build(Codec::Zstd, &numbered(0..500), SMALL_BLOCK)
            .as_bytes()
            .to_vec();
        let at = layout(&bytes).keys;
        damage(&mut bytes, at);
        let l = layout(&bytes);
        let sum = crc32fast::hash(&bytes[l.table..l.footer]);
        put_u32(&mut bytes, l.footer + 36, sum);
        assert_eq!(
            kind_of(Run::new(bytes).unwrap_err()),
            RunCorruption::InvalidKeyRange,
            "{name}"
        );
    }
}

#[test]
fn a_minimum_above_the_first_blocks_last_key_is_rejected_on_open() {
    let mut bytes = build(Codec::Zstd, &numbered(0..500), SMALL_BLOCK)
        .as_bytes()
        .to_vec();
    let at = layout(&bytes).keys;
    bytes[at] = 0xFF;
    let error = RunView::open(&bytes).unwrap_err();
    assert_eq!(kind_of(error), RunCorruption::InvalidKeyRange);
    assert_eq!(error.offset, at);
}

#[test]
fn malformed_minimum_key_lengths_are_rejected_on_open() {
    let original = build(Codec::Zstd, &numbered(0..500), SMALL_BLOCK)
        .as_bytes()
        .to_vec();
    let l = layout(&original);
    let key_area = (l.footer - l.keys) as u32;
    for (name, value, kind) in [
        (
            "past the key area",
            key_area + 1,
            RunCorruption::InvalidFooter,
        ),
        (
            "over the key limit",
            MAX_KEY_LEN as u32 + 1,
            RunCorruption::InvalidFooter,
        ),
        ("overflow", u32::MAX, RunCorruption::InvalidFooter),
        ("past the first block key", 19, RunCorruption::InvalidIndex),
    ] {
        let mut bytes = original.clone();
        put_u32(&mut bytes, l.footer + 20, value);
        let error = RunView::open(&bytes).unwrap_err();
        assert_eq!(kind_of(error), kind, "{name}");
        assert_eq!(kind_of(Run::new(bytes).unwrap_err()), kind, "{name}");
    }
}

#[test]
fn a_minimum_key_over_the_limit_is_rejected_even_inside_a_large_key_area() {
    // One block per record, so the key area outgrows the key length limit.
    let mut bytes = build(Codec::None, &numbered(0..24000), 1)
        .as_bytes()
        .to_vec();
    let l = layout(&bytes);
    assert!(l.footer - l.keys > MAX_KEY_LEN);
    put_u32(&mut bytes, l.footer + 20, MAX_KEY_LEN as u32 + 1);
    assert_eq!(
        kind_of(RunView::open(&bytes).unwrap_err()),
        RunCorruption::InvalidFooter
    );
}

#[test]
fn a_shortened_minimum_key_is_rejected() {
    let original = build(Codec::Zstd, &numbered(0..500), SMALL_BLOCK)
        .as_bytes()
        .to_vec();
    let l = layout(&original);
    let mut bytes = original.clone();
    put_u32(&mut bytes, l.footer + 20, 4);
    assert!(Run::new(bytes).is_err());
}

#[test]
fn an_empty_run_with_a_stored_minimum_is_rejected() {
    let mut bytes = build(Codec::Zstd, &[], SMALL_BLOCK).as_bytes().to_vec();
    put_u32(&mut bytes, 20, 1);
    let error = RunView::open(&bytes).unwrap_err();
    assert_eq!(kind_of(error), RunCorruption::InvalidFooter);
}

#[test]
fn a_lookup_outside_a_range_decodes_nothing() {
    let run = build(Codec::Zstd, &numbered(100..200), SMALL_BLOCK);
    let mut workspace = ReadWorkspace::new();
    assert_eq!(run.get(b"a", &mut workspace).unwrap(), None);
    assert_eq!(run.get(b"key:00099", &mut workspace).unwrap(), None);
    assert_eq!(run.get(b"key:00200", &mut workspace).unwrap(), None);
    assert_eq!(run.get(b"z", &mut workspace).unwrap(), None);
    assert_eq!(workspace.blocks_loaded(), 0);
}

#[test]
fn a_lookup_at_either_bound_still_finds_the_key() {
    let run = build(Codec::Zstd, &numbered(100..200), SMALL_BLOCK);
    let mut workspace = ReadWorkspace::new();
    assert_eq!(
        run.get(b"key:00100", &mut workspace).unwrap().unwrap().seq,
        101
    );
    assert_eq!(
        run.get(b"key:00199", &mut workspace).unwrap().unwrap().seq,
        200
    );
    assert_eq!(workspace.blocks_loaded(), 2);
}

fn three_disjoint() -> Vec<Run<Vec<u8>>> {
    vec![
        build(Codec::None, &numbered(0..300), SMALL_BLOCK),
        build(Codec::Zstd, &numbered(300..600), SMALL_BLOCK),
        build(Codec::Zstd, &numbered(600..900), SMALL_BLOCK),
    ]
}

fn views(runs: &[Run<Vec<u8>>]) -> Vec<RunView<'_>> {
    runs.iter().map(Run::view).collect()
}

#[test]
fn a_logical_lookup_decodes_only_the_run_holding_the_key() {
    let runs = three_disjoint();
    let views = views(&runs);
    let set = RunSet::new(&views);
    let mut workspace = LogicalWorkspace::new();
    for (key, seq) in [
        (&b"key:00000"[..], 1),
        (b"key:00299", 300),
        (b"key:00300", 301),
        (b"key:00899", 900),
    ] {
        let before = workspace.blocks_loaded();
        assert_eq!(set.get(key, &mut workspace).unwrap().unwrap().seq, seq);
        assert_eq!(workspace.blocks_loaded() - before, 1, "{key:?}");
    }
    // Between two runs and outside all of them, nothing is decoded.
    let before = workspace.blocks_loaded();
    for key in [&b"key:002995"[..], b"a", b"z"] {
        assert_eq!(set.get(key, &mut workspace).unwrap(), None);
    }
    assert_eq!(workspace.blocks_loaded(), before);
}

#[test]
fn a_seek_waits_to_decode_runs_above_the_target() {
    let runs = three_disjoint();
    let mut workspace = MergeWorkspace::new();
    let mut cursor = MergeCursor::new(runs.iter().map(Run::view), &mut workspace);
    cursor.seek(b"key:00450").unwrap();
    assert_eq!(cursor.next_record().unwrap().unwrap().seq, 451);
    assert_eq!(
        workspace.blocks_loaded(),
        1,
        "only the run holding the target"
    );
}

#[test]
fn a_merge_decodes_each_disjoint_run_only_when_it_reaches_it() {
    let runs = three_disjoint();
    let mut workspace = MergeWorkspace::new();
    let mut cursor = MergeCursor::new(runs.iter().map(Run::view), &mut workspace);
    assert_eq!(cursor.next_record().unwrap().unwrap().seq, 1);
    for seq in 2..=300 {
        assert_eq!(cursor.next_record().unwrap().unwrap().seq, seq);
    }
    let loaded_first_run = runs[0].block_count() as u64;
    assert_eq!(workspace.blocks_loaded(), loaded_first_run);
}

#[test]
fn a_seek_below_every_run_decodes_nothing_until_read() {
    let runs = three_disjoint();
    let mut workspace = MergeWorkspace::new();
    let mut cursor = MergeCursor::new(runs.iter().map(Run::view), &mut workspace);
    cursor.seek(b"a").unwrap();
    assert_eq!(workspace.blocks_loaded(), 0);
}

#[test]
fn a_waiting_run_tying_the_current_key_is_merged_before_it_is_returned() {
    let older = build(
        Codec::Zstd,
        &[put(b"a", 1, b"a"), put(b"m", 1, b"old")],
        SMALL_BLOCK,
    );
    let newer = build(Codec::Zstd, &[put(b"m", 5, b"new")], SMALL_BLOCK);
    let mut workspace = MergeWorkspace::new();
    let mut cursor = MergeCursor::new([older.view(), newer.view()], &mut workspace);
    let mut found = Vec::new();
    while let Some(record) = cursor.next_record().unwrap() {
        found.push((record.key.to_vec(), record.seq));
    }
    assert_eq!(
        found,
        [(b"a".to_vec(), 1), (b"m".to_vec(), 5), (b"m".to_vec(), 1)]
    );
}

#[test]
fn a_tombstone_in_a_waiting_run_still_hides_an_older_value() {
    let older = build(Codec::Zstd, &numbered(0..200), SMALL_BLOCK);
    let newer = build(Codec::Zstd, &[del(b"key:00150", 900)], SMALL_BLOCK);
    let views = [older.view(), newer.view()];
    let set = RunSet::new(&views);
    let mut workspace = LogicalWorkspace::new();
    assert_eq!(set.get(b"key:00150", &mut workspace).unwrap(), None);
    let mut cursor = set.cursor(&mut workspace);
    let found = rest(&mut cursor).unwrap();
    assert_eq!(found.len(), 199);
    assert!(!found.contains(&b"key:00150".to_vec()));
    cursor.seek(b"key:00150").unwrap();
    assert_eq!(
        cursor.next_record().unwrap().unwrap().key,
        b"key:00151",
        "seeking onto a deleted key"
    );
}

#[test]
fn a_conflict_with_a_waiting_run_is_still_detected() {
    let a = build(
        Codec::Zstd,
        &[put(b"a", 1, b"a"), put(b"m", 5, b"one")],
        SMALL_BLOCK,
    );
    let b = build(Codec::Zstd, &[put(b"m", 5, b"two")], SMALL_BLOCK);
    let views = [a.view(), b.view()];
    let set = RunSet::new(&views);
    let mut workspace = LogicalWorkspace::new();
    let mut cursor = set.cursor(&mut workspace);
    assert_eq!(cursor.next_record().unwrap().unwrap().key, b"a");
    assert!(matches!(
        cursor.next_record(),
        Err(ReadError::ConflictingCopies(_))
    ));
}

fn prefix_scan(runs: &[Run<Vec<u8>>], prefix: &[u8]) -> (Vec<Vec<u8>>, u64) {
    let views = views(runs);
    let mut workspace = LogicalWorkspace::new();
    let mut cursor = RunSet::new(&views).cursor(&mut workspace);
    cursor.seek_prefix(prefix).unwrap();
    let found = rest(&mut cursor).unwrap();
    (found, workspace.blocks_loaded())
}

#[test]
fn a_prefix_scan_decodes_only_runs_its_interval_can_touch() {
    let runs = three_disjoint();
    let (found, blocks) = prefix_scan(&runs, b"key:0035");
    assert_eq!(found.len(), 10);
    assert_eq!(blocks, 1, "the runs below and above are not decoded");
    let (found, blocks) = prefix_scan(&runs, b"nothing");
    assert_eq!((found.len(), blocks), (0, 0), "above every run");
    let (found, blocks) = prefix_scan(&runs, b"a");
    assert_eq!((found.len(), blocks), (0, 0), "below every run");
}

#[test]
fn a_prefix_ending_at_a_run_boundary_covers_both_runs() {
    let runs = three_disjoint();
    let (found, _) = prefix_scan(&runs, b"key:0029");
    assert_eq!(found.len(), 10);
    let (found, _) = prefix_scan(&runs, b"key:0030");
    assert_eq!(found.len(), 10);
    let (found, blocks) = prefix_scan(&runs, b"key:00299");
    assert_eq!((found, blocks), (vec![b"key:00299".to_vec()], 1));
    let (found, blocks) = prefix_scan(&runs, b"key:00300");
    assert_eq!((found, blocks), (vec![b"key:00300".to_vec()], 1));
    let (found, _) = prefix_scan(&runs, b"key:");
    assert_eq!(found.len(), 900, "a prefix spanning every run");
}

#[test]
fn prefixes_of_0xff_bytes_and_binary_ranges_prune_correctly() {
    let low = build(
        Codec::Zstd,
        &[put(&[0x00], 1, b"a"), put(&[0x00, 0xFF], 1, b"b")],
        SMALL_BLOCK,
    );
    let high = build(
        Codec::Zstd,
        &[
            put(&[0xFF], 1, b"c"),
            put(&[0xFF, 0x00], 1, b"d"),
            put(&[0xFF, 0xFF], 1, b"e"),
        ],
        SMALL_BLOCK,
    );
    let runs = vec![low, high];
    assert_eq!(prefix_scan(&runs, &[]).0.len(), 5);
    assert_eq!(prefix_scan(&runs, &[0xFF]).0.len(), 3);
    assert_eq!(prefix_scan(&runs, &[0xFF, 0xFF]).0.len(), 1);
    assert_eq!(prefix_scan(&runs, &[0xFF, 0xFF, 0xFF]), (Vec::new(), 0));
    assert_eq!(prefix_scan(&runs, &[0x00]).0.len(), 2);
    let (found, blocks) = prefix_scan(&runs, &[0xFE]);
    assert_eq!((found.len(), blocks), (0, 0), "between the two runs");
    assert_eq!(
        prefix_scan(&runs, &vec![0xFF; MAX_KEY_LEN + 1]),
        (Vec::new(), 0)
    );
}

#[test]
fn an_empty_prefix_and_the_empty_key_read_a_run_whose_minimum_is_empty() {
    let runs = vec![build(
        Codec::Zstd,
        &[put(b"", 1, b"e"), put(b"a", 1, b"a")],
        SMALL_BLOCK,
    )];
    assert_eq!(prefix_scan(&runs, b"").0, [Vec::new(), b"a".to_vec()]);
    let views = views(&runs);
    let mut workspace = LogicalWorkspace::new();
    let set = RunSet::new(&views);
    assert_eq!(set.get(b"", &mut workspace).unwrap().unwrap().value, b"e");
}

fn damaged(run: &Run<Vec<u8>>, frame: usize) -> Vec<u8> {
    let mut bytes = run.as_bytes().to_vec();
    let at = layout(&bytes).table + frame * 8;
    let start = u32::from_le_bytes(bytes[at..at + 4].try_into().unwrap()) as usize;
    let _ = layout(&bytes).first_frame_payload;
    bytes[start + 12] ^= 0xFF;
    bytes
}

/// A range check trusts validated metadata, so a read the range rules out
/// never reaches the damaged frame and cannot report it. A read that needs
/// the run still fails.
#[test]
fn a_damaged_run_is_reported_only_by_reads_its_range_admits() {
    let run = build(Codec::Zstd, &numbered(100..700), SMALL_BLOCK);
    let bytes = damaged(&run, 0);
    let bad = RunView::open(&bytes).unwrap();
    let views = [bad];
    let set = RunSet::new(&views);
    let mut workspace = LogicalWorkspace::new();

    assert_eq!(
        set.get(b"key:00099", &mut workspace).unwrap(),
        None,
        "below"
    );
    assert_eq!(set.get(b"z", &mut workspace).unwrap(), None, "above");
    assert!(matches!(
        set.get(b"key:00100", &mut workspace),
        Err(ReadError::Corrupt(_))
    ));

    let mut cursor = set.cursor(&mut workspace);
    cursor.seek_prefix(b"a").unwrap();
    assert_eq!(
        cursor.next_record().unwrap(),
        None,
        "a prefix below the range"
    );
    cursor.seek_prefix(b"z").unwrap();
    assert_eq!(
        cursor.next_record().unwrap(),
        None,
        "a prefix above the range"
    );
    cursor.seek_prefix(b"key:001").unwrap();
    assert!(matches!(cursor.next_record(), Err(ReadError::Corrupt(_))));
}

#[test]
fn a_waiting_damaged_run_fails_when_the_merge_reaches_it() {
    let good = build(Codec::Zstd, &numbered(0..100), SMALL_BLOCK);
    let bad_run = build(Codec::Zstd, &numbered(100..200), SMALL_BLOCK);
    let bytes = damaged(&bad_run, 0);
    let views = [good.view(), RunView::open(&bytes).unwrap()];
    let set = RunSet::new(&views);
    let mut workspace = LogicalWorkspace::new();
    let mut cursor = set.cursor(&mut workspace);
    for _ in 0..100 {
        cursor.next_record().unwrap().unwrap();
    }
    assert!(matches!(cursor.next_record(), Err(ReadError::Corrupt(_))));
    assert!(cursor.next_record().is_err(), "the failure persists");
    cursor.seek(b"key:00000").unwrap();
    assert_eq!(cursor.next_record().unwrap().unwrap().seq, 1);
}

#[test]
fn a_seek_after_a_read_can_rewind_to_a_waiting_position() {
    let runs = three_disjoint();
    let views = views(&runs);
    let mut workspace = LogicalWorkspace::new();
    let mut cursor = RunSet::new(&views).cursor(&mut workspace);
    assert_eq!(rest(&mut cursor).unwrap().len(), 900);
    cursor.seek(b"a").unwrap();
    assert_eq!(cursor.next_record().unwrap().unwrap().seq, 1);
    cursor.seek(b"key:00150").unwrap();
    assert_eq!(cursor.next_record().unwrap().unwrap().seq, 151);
    cursor.seek(b"key:00000").unwrap();
    assert_eq!(cursor.next_record().unwrap().unwrap().seq, 1);
}

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

const ALPHABET: [u8; 4] = [0x00, b'a', b'b', 0xFF];

fn random_key(rng: &mut Rng) -> Vec<u8> {
    (0..rng.below(5))
        .map(|_| ALPHABET[rng.below(ALPHABET.len())])
        .collect()
}

/// A value decided by the key and sequence alone, so copies never conflict.
fn update(key: &[u8], seq: u64) -> Input {
    let hash = key
        .iter()
        .fold(seq, |h, &b| h.wrapping_mul(31) ^ u64::from(b));
    let value = (hash % 4 != 0).then(|| vec![hash as u8; (hash % 16) as usize]);
    (key.to_vec(), seq, value)
}

/// Inputs drawn from narrow key slices, so ranges are often disjoint, with
/// sequence numbers shared across runs.
fn random_inputs(rng: &mut Rng) -> Vec<Vec<Input>> {
    (0..rng.below(9))
        .map(|_| {
            let slice = if rng.below(3) == 0 {
                Vec::new()
            } else {
                vec![ALPHABET[rng.below(ALPHABET.len())]]
            };
            let mut records = BTreeMap::new();
            for _ in 0..rng.below(120) {
                let key = [slice.as_slice(), random_key(rng).as_slice()].concat();
                let seq = rng.below(32) as u64;
                records.insert((key.clone(), seq), update(&key, seq));
            }
            records.into_values().collect()
        })
        .collect()
}

fn model(inputs: &[Vec<Input>]) -> BTreeMap<Vec<u8>, (u64, Option<Vec<u8>>)> {
    let mut latest: BTreeMap<Vec<u8>, (u64, Option<Vec<u8>>)> = BTreeMap::new();
    for (key, seq, value) in inputs.iter().flatten() {
        if latest.get(key).is_none_or(|(newest, _)| newest < seq) {
            latest.insert(key.clone(), (*seq, value.clone()));
        }
    }
    latest
}

fn bounds(records: &[Input]) -> Option<(&[u8], &[u8])> {
    let min = records.iter().map(|r| r.0.as_slice()).min()?;
    let max = records.iter().map(|r| r.0.as_slice()).max()?;
    Some((min, max))
}

#[test]
fn pruned_reads_match_a_reference_model_and_decode_no_excluded_run() {
    for seed in 0..64 {
        let mut rng = Rng(4000 + seed);
        let inputs = random_inputs(&mut rng);
        let latest = model(&inputs);
        let live: Vec<(&Vec<u8>, u64, &Vec<u8>)> = latest
            .iter()
            .filter_map(|(key, (seq, value))| Some((key, *seq, value.as_ref()?)))
            .collect();
        let runs: Vec<_> = inputs
            .iter()
            .map(|input| {
                build(
                    CODECS[rng.below(CODECS.len())],
                    input,
                    [64, 512][rng.below(2)],
                )
            })
            .collect();
        let views = views(&runs);
        let set = RunSet::new(&views);
        let mut workspace = LogicalWorkspace::new();

        for input in &inputs {
            if let Some((min, max)) = bounds(input) {
                let run_index = inputs.iter().position(|i| std::ptr::eq(i, input)).unwrap();
                let range = runs[run_index].key_range().unwrap();
                assert_eq!((range.min, range.max), (min, max), "seed {seed}");
            }
        }

        for _ in 0..60 {
            let key = random_key(&mut rng);
            let holding = inputs
                .iter()
                .filter_map(|input| bounds(input))
                .filter(|(min, max)| *min <= key.as_slice() && key.as_slice() <= *max)
                .count() as u64;
            let before = workspace.blocks_loaded();
            let found = set
                .get(&key, &mut workspace)
                .unwrap()
                .map(|v| (v.seq, v.value.to_vec()));
            assert!(
                workspace.blocks_loaded() - before <= holding,
                "seed {seed} {key:02x?}"
            );
            let want = latest
                .get(&key)
                .and_then(|(seq, value)| Some((*seq, value.clone()?)));
            assert_eq!(found, want, "seed {seed} key {key:02x?}");
        }

        let mut cursor = set.cursor(&mut workspace);
        for _ in 0..40 {
            let probe = random_key(&mut rng);
            let (wanted, prefix): (Vec<_>, bool) = if rng.below(2) == 0 {
                cursor.seek_prefix(&probe).unwrap();
                (
                    live.iter().filter(|r| r.0.starts_with(&probe)).collect(),
                    true,
                )
            } else {
                cursor.seek(&probe).unwrap();
                (live.iter().filter(|r| r.0 >= &probe).collect(), false)
            };
            for want in wanted {
                let record = cursor.next_record().unwrap().unwrap();
                assert_eq!(
                    (record.key, record.seq, record.value),
                    (want.0.as_slice(), want.1, want.2.as_slice()),
                    "seed {seed} probe {probe:02x?} prefix {prefix}"
                );
            }
            assert_eq!(
                cursor.next_record().unwrap(),
                None,
                "seed {seed} {probe:02x?}"
            );
        }
    }
}
