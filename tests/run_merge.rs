//! Merged iteration over several runs.
//!
//! The expected stream is a sorted reference model built only from the input
//! records: internal order, then input position for identical internal keys.
//! Every collecting loop bounds records and bytes, so a stuck cursor fails an
//! assertion instead of exhausting memory.

use tessera::block::{Record, Value};
use tessera::run::{
    Codec, CorruptRun, MergeCursor, MergeWorkspace, Run, RunBuilder, RunCorruption, RunView,
};

type Owned = (Vec<u8>, u64, Option<Vec<u8>>);

const CODECS: [Codec; 3] = [Codec::None, Codec::Lz4, Codec::Zstd];
const RECORD_BOUND: usize = 200_000;
const RETAINED_BOUND: usize = 64 << 20;
const SMALL_BLOCK: usize = 1024;

fn rec(key: &[u8], seq: u64, value: Option<&[u8]>) -> Owned {
    (key.to_vec(), seq, value.map(<[u8]>::to_vec))
}

fn owned(record: Record<'_>) -> Owned {
    let value = match record.value {
        Value::Bytes(bytes) => Some(bytes.to_vec()),
        Value::Tombstone => None,
    };
    (record.key.to_vec(), record.seq, value)
}

fn build(codec: Codec, records: &[Owned], target: usize) -> Run<Vec<u8>> {
    let mut builder = RunBuilder::with_target_block_len(codec, target);
    for (key, seq, value) in records {
        let value = value.as_deref().map_or(Value::Tombstone, Value::Bytes);
        builder.add(key, *seq, value).unwrap();
    }
    builder.finish()
}

fn views(runs: &[Run<Vec<u8>>]) -> Vec<RunView<'_>> {
    runs.iter().map(Run::view).collect()
}

fn rest(cursor: &mut MergeCursor<'_>) -> Result<Vec<Owned>, CorruptRun> {
    let mut records = Vec::new();
    let mut retained = 0;
    while let Some(record) = cursor.next_record()? {
        retained += record.key.len();
        records.push(owned(record));
        assert!(
            records.len() <= RECORD_BOUND && retained <= RETAINED_BOUND,
            "merge did not terminate"
        );
    }
    Ok(records)
}

/// The expected merged stream, from the inputs alone.
fn model(inputs: &[Vec<Owned>]) -> Vec<Owned> {
    let mut all: Vec<(usize, &Owned)> = inputs
        .iter()
        .enumerate()
        .flat_map(|(input, records)| records.iter().map(move |record| (input, record)))
        .collect();
    all.sort_by(|(a_input, a), (b_input, b)| {
        a.0.cmp(&b.0).then(b.1.cmp(&a.1)).then(a_input.cmp(b_input))
    });
    all.into_iter().map(|(_, record)| record.clone()).collect()
}

fn build_all(inputs: &[Vec<Owned>], codecs: &[Codec], target: usize) -> Vec<Run<Vec<u8>>> {
    inputs
        .iter()
        .enumerate()
        .map(|(i, records)| build(codecs[i % codecs.len()], records, target))
        .collect()
}

fn scan(runs: &[Run<Vec<u8>>]) -> Vec<Owned> {
    let mut workspace = MergeWorkspace::new();
    let mut cursor = MergeCursor::new(views(runs), &mut workspace);
    rest(&mut cursor).unwrap()
}

fn seek_all(runs: &[Run<Vec<u8>>], target: &[u8]) -> Vec<Owned> {
    let mut workspace = MergeWorkspace::new();
    let mut cursor = MergeCursor::new(views(runs), &mut workspace);
    cursor.seek(target).unwrap();
    rest(&mut cursor).unwrap()
}

fn scan_prefix(runs: &[Run<Vec<u8>>], prefix: &[u8]) -> Vec<Owned> {
    let mut workspace = MergeWorkspace::new();
    let mut cursor = MergeCursor::new(views(runs), &mut workspace);
    cursor.seek_prefix(prefix).unwrap();
    rest(&mut cursor).unwrap()
}

fn numbered(range: std::ops::Range<usize>) -> Vec<Owned> {
    range
        .map(|i| {
            rec(
                format!("key:{i:05}").as_bytes(),
                i as u64 + 1,
                Some(b"value"),
            )
        })
        .collect()
}

/// Every record to the input `(index % count)`, so runs interleave closely.
fn interleave(records: &[Owned], count: usize) -> Vec<Vec<Owned>> {
    let mut inputs = vec![Vec::new(); count];
    for (index, record) in records.iter().enumerate() {
        inputs[index % count].push(record.clone());
    }
    inputs
}

fn frame_start(bytes: &[u8], frame: usize) -> usize {
    let footer = bytes.len() - 28;
    let table = u64::from_le_bytes(bytes[footer..footer + 8].try_into().unwrap()) as usize;
    let at = table + frame * 12;
    u64::from_le_bytes(bytes[at..at + 8].try_into().unwrap()) as usize
}

fn prefix_of(records: &[Owned], prefix: &[u8]) -> Vec<Owned> {
    records
        .iter()
        .filter(|r| r.0.starts_with(prefix))
        .cloned()
        .collect()
}

fn from_target(records: &[Owned], target: &[u8]) -> Vec<Owned> {
    records
        .iter()
        .filter(|r| r.0.as_slice() >= target)
        .cloned()
        .collect()
}

#[test]
fn no_inputs_and_empty_inputs_yield_nothing() {
    let mut workspace = MergeWorkspace::new();
    let mut cursor = MergeCursor::new(Vec::<RunView<'_>>::new(), &mut workspace);
    assert_eq!(cursor.next_record().unwrap(), None);
    cursor.seek(b"a").unwrap();
    cursor.seek_prefix(b"").unwrap();
    assert_eq!(cursor.next_record().unwrap(), None);

    let empty = vec![build(Codec::Zstd, &[], SMALL_BLOCK); 3];
    assert_eq!(scan(&empty), Vec::new());
    assert_eq!(seek_all(&empty, b"a"), Vec::new());
    assert_eq!(scan_prefix(&empty, b""), Vec::new());
}

#[test]
fn empty_inputs_do_not_disturb_the_others() {
    let records = numbered(0..50);
    let inputs = vec![Vec::new(), records.clone(), Vec::new()];
    let runs = build_all(&inputs, &CODECS, SMALL_BLOCK);
    assert_eq!(scan(&runs), records);
}

#[test]
fn a_single_input_reads_like_its_cursor() {
    let records = numbered(0..400);
    for codec in CODECS {
        let runs = vec![build(codec, &records, SMALL_BLOCK)];
        assert_eq!(scan(&runs), records);
    }
}

#[test]
fn disjoint_and_interleaved_inputs_merge_in_order() {
    let records = numbered(0..900);
    let disjoint = vec![
        records[600..].to_vec(),
        records[..300].to_vec(),
        records[300..600].to_vec(),
    ];
    let interleaved = interleave(&records, 3);
    for inputs in [disjoint, interleaved] {
        for codec in CODECS {
            let runs = build_all(&inputs, &[codec], SMALL_BLOCK);
            assert_eq!(scan(&runs), records);
        }
    }
}

#[test]
fn versions_of_one_key_across_inputs_are_newest_first() {
    let inputs = vec![
        vec![rec(b"a", 5, Some(b"5")), rec(b"a", 2, None)],
        vec![rec(b"a", 7, None), rec(b"b", 1, Some(b"b1"))],
        vec![rec(b"a", 3, Some(b"3")), rec(b"c", 9, None)],
    ];
    let runs = build_all(&inputs, &CODECS, SMALL_BLOCK);
    let found = scan(&runs);
    assert_eq!(
        found.iter().map(|r| r.1).collect::<Vec<_>>(),
        [7, 5, 3, 2, 1, 9]
    );
    assert_eq!(found, model(&inputs));
}

#[test]
fn version_chains_spanning_blocks_interleave_across_inputs() {
    let mut inputs = vec![Vec::new(), Vec::new(), Vec::new()];
    for seq in (1..=900u64).rev() {
        inputs[seq as usize % 3].push(rec(b"chain", seq, Some(b"version")));
    }
    let runs = build_all(&inputs, &CODECS, 256);
    assert!(runs.iter().all(|run| run.block_count() > 2));
    let found = scan(&runs);
    assert_eq!(found.first().map(|r| r.1), Some(900));
    assert_eq!(found, model(&inputs));
    assert_eq!(scan_prefix(&runs, b"chain"), model(&inputs));
    assert_eq!(seek_all(&runs, b"chain"), model(&inputs));
}

#[test]
fn identical_internal_keys_keep_every_copy_in_input_order() {
    let inputs = vec![
        vec![rec(b"a", 2, Some(b"from-0")), rec(b"b", 1, Some(b"same"))],
        vec![rec(b"a", 2, Some(b"from-1")), rec(b"b", 1, Some(b"same"))],
        vec![rec(b"a", 2, None), rec(b"c", 1, None)],
    ];
    let runs = build_all(&inputs, &CODECS, SMALL_BLOCK);
    let found = scan(&runs);
    assert_eq!(
        found[..3],
        [
            rec(b"a", 2, Some(b"from-0")),
            rec(b"a", 2, Some(b"from-1")),
            rec(b"a", 2, None),
        ]
    );
    assert_eq!(found, model(&inputs));

    // Reordering inputs reorders only the copies.
    let reversed: Vec<Vec<Owned>> = inputs.iter().rev().cloned().collect();
    let runs = build_all(&reversed, &CODECS, SMALL_BLOCK);
    assert_eq!(scan(&runs), model(&reversed));
    assert_eq!(scan(&runs)[0], rec(b"a", 2, None));
}

#[test]
fn runs_with_different_codecs_and_block_sizes_merge() {
    let records = numbered(0..1200);
    let inputs = interleave(&records, 6);
    let runs: Vec<_> = inputs
        .iter()
        .enumerate()
        .map(|(i, input)| build(CODECS[i % 3], input, [256, 1024, 8192][i % 3]))
        .collect();
    assert_eq!(scan(&runs), records);
    assert_eq!(scan_prefix(&runs, b"key:0"), records);
    assert_eq!(seek_all(&runs, b"key:00600"), records[600..]);
}

#[test]
fn seeks_land_before_within_and_after_every_input() {
    let records = numbered(0..300);
    let inputs = interleave(&records, 4);
    let runs = build_all(&inputs, &CODECS, SMALL_BLOCK);
    assert_eq!(seek_all(&runs, b""), records);
    assert_eq!(seek_all(&runs, b"key:00150"), records[150..]);
    assert_eq!(seek_all(&runs, b"key:00150\0"), records[151..]);
    assert_eq!(seek_all(&runs, b"key:00299"), records[299..]);
    assert_eq!(seek_all(&runs, b"z"), Vec::new());
}

#[test]
fn prefixes_over_disjoint_and_overlapping_inputs() {
    let records = numbered(0..1000);
    let disjoint = vec![
        records[..250].to_vec(),
        records[250..500].to_vec(),
        records[500..1000].to_vec(),
    ];
    for inputs in [disjoint, interleave(&records, 3)] {
        let runs = build_all(&inputs, &CODECS, SMALL_BLOCK);
        for prefix in [
            &b""[..],
            b"key:",
            b"key:0",
            b"key:00",
            b"key:002",
            b"key:0049",
            b"key:00499",
            b"key:9",
            b"key:002x",
            b"zzz",
        ] {
            assert_eq!(
                scan_prefix(&runs, prefix),
                prefix_of(&records, prefix),
                "prefix {prefix:02x?}"
            );
        }
    }
}

#[test]
fn prefixes_are_binary_and_may_end_in_0xff() {
    let keys: [&[u8]; 8] = [
        &[0x00],
        &[0x00, 0xFF],
        &[0x01],
        &[0xFE, 0xFF],
        &[0xFF],
        &[0xFF, 0x00],
        &[0xFF, 0xFF],
        &[0xFF, 0xFF, 0xFF],
    ];
    let inputs: Vec<Vec<Owned>> = vec![
        keys.iter()
            .step_by(2)
            .map(|k| rec(k, 1, Some(b"v")))
            .collect(),
        keys.iter()
            .skip(1)
            .step_by(2)
            .map(|k| rec(k, 2, None))
            .collect(),
        keys.iter().map(|k| rec(k, 3, Some(b"w"))).collect(),
    ];
    let expected = model(&inputs);
    for target in [1, SMALL_BLOCK] {
        let runs = build_all(&inputs, &CODECS, target);
        for prefix in [
            &[][..],
            &[0x00],
            &[0xFE, 0xFF],
            &[0xFF],
            &[0xFF, 0xFF],
            &[0xFF, 0xFF, 0xFF],
            &[0xFF, 0xFF, 0xFF, 0xFF],
        ] {
            assert_eq!(
                scan_prefix(&runs, prefix),
                prefix_of(&expected, prefix),
                "prefix {prefix:02x?} target {target}"
            );
        }
    }
}

#[test]
fn cursors_reposition_and_stay_exhausted_until_moved() {
    let records = numbered(0..300);
    let inputs = interleave(&records, 3);
    let runs = build_all(&inputs, &CODECS, SMALL_BLOCK);
    let mut workspace = MergeWorkspace::new();
    let mut cursor = MergeCursor::new(views(&runs), &mut workspace);

    cursor.seek(b"key:00200").unwrap();
    assert_eq!(owned(cursor.next_record().unwrap().unwrap()), records[200]);
    cursor.seek(b"key:00010").unwrap();
    assert_eq!(owned(cursor.next_record().unwrap().unwrap()), records[10]);

    cursor.seek_prefix(b"key:0029").unwrap();
    assert_eq!(rest(&mut cursor).unwrap(), records[290..]);
    assert_eq!(cursor.next_record().unwrap(), None);
    assert_eq!(cursor.next_record().unwrap(), None);

    cursor.seek(b"key:00298").unwrap();
    assert_eq!(rest(&mut cursor).unwrap(), records[298..]);
    assert_eq!(cursor.next_record().unwrap(), None);
    cursor.seek(b"").unwrap();
    assert_eq!(owned(cursor.next_record().unwrap().unwrap()), records[0]);
}

#[test]
fn a_new_cursor_on_the_same_workspace_starts_clean() {
    let records = numbered(0..60);
    let inputs = interleave(&records, 3);
    let runs = build_all(&inputs, &CODECS, SMALL_BLOCK);
    let mut workspace = MergeWorkspace::new();
    let mut cursor = MergeCursor::new(views(&runs), &mut workspace);
    cursor.seek_prefix(b"key:0001").unwrap();
    assert_eq!(rest(&mut cursor).unwrap().len(), 10);
    let mut cursor = MergeCursor::new(views(&runs), &mut workspace);
    assert_eq!(rest(&mut cursor).unwrap(), records);
}

#[test]
fn a_full_merge_loads_each_block_once() {
    let records = numbered(0..900);
    let inputs = interleave(&records, 3);
    let runs = build_all(&inputs, &CODECS, SMALL_BLOCK);
    let blocks: u64 = runs.iter().map(|run| run.block_count() as u64).sum();
    let mut workspace = MergeWorkspace::new();
    let mut cursor = MergeCursor::new(views(&runs), &mut workspace);
    assert_eq!(rest(&mut cursor).unwrap().len(), records.len());
    assert_eq!(workspace.blocks_loaded(), blocks);
}

#[test]
fn seeks_skip_inputs_entirely_below_the_target() {
    let records = numbered(0..900);
    let inputs = vec![
        records[..300].to_vec(),
        records[300..600].to_vec(),
        records[600..].to_vec(),
    ];
    let runs = build_all(&inputs, &CODECS, SMALL_BLOCK);
    let mut workspace = MergeWorkspace::new();
    let mut cursor = MergeCursor::new(views(&runs), &mut workspace);
    cursor.seek(b"key:00700").unwrap();
    assert_eq!(
        cursor.next_record().unwrap().map(owned).as_ref(),
        Some(&records[700])
    );
    assert_eq!(
        workspace.blocks_loaded(),
        1,
        "only the last input has a head"
    );
}

#[test]
fn a_seek_decodes_one_block_in_each_input_that_may_hold_the_target() {
    let records = numbered(0..900);
    let runs = build_all(&interleave(&records, 3), &CODECS, SMALL_BLOCK);
    let mut workspace = MergeWorkspace::new();
    let mut cursor = MergeCursor::new(views(&runs), &mut workspace);
    cursor.seek(b"key:00450").unwrap();
    assert_eq!(workspace.blocks_loaded(), 3);
}

fn corrupt_run(codec: Codec, records: &[Owned], frame: usize) -> Vec<u8> {
    let mut bytes = build(codec, records, SMALL_BLOCK).as_bytes().to_vec();
    let start = frame_start(&bytes, frame);
    bytes[start + 12] ^= 0xFF;
    bytes
}

#[test]
fn a_damaged_input_fails_the_seek_until_a_good_seek() {
    let records = numbered(0..600);
    let inputs = interleave(&records, 2);
    let good = build(Codec::Lz4, &inputs[0], SMALL_BLOCK);
    let bytes = corrupt_run(Codec::Zstd, &inputs[1], 1);
    let damaged = RunView::open(&bytes).unwrap();
    let in_second_block = inputs[1]
        .iter()
        .find(|r| damaged.locate(&r.0).unwrap() == Some(1))
        .unwrap()
        .0
        .clone();
    let expected = CorruptRun {
        offset: frame_start(&bytes, 1),
        kind: RunCorruption::ChecksumMismatch,
    };

    let mut workspace = MergeWorkspace::new();
    let mut cursor = MergeCursor::new([good.view(), damaged], &mut workspace);
    assert_eq!(cursor.seek(&in_second_block), Err(expected));
    assert_eq!(cursor.next_record(), Err(expected));
    assert_eq!(cursor.next_record(), Err(expected), "the failure persists");

    cursor.seek(b"").unwrap();
    assert_eq!(
        cursor.next_record().unwrap().map(owned).as_ref(),
        Some(&records[0])
    );
}

#[test]
fn iteration_stops_at_a_damaged_input_without_misordering() {
    let records = numbered(0..600);
    let inputs = interleave(&records, 2);
    let good = build(Codec::Lz4, &inputs[0], SMALL_BLOCK);
    let bytes = corrupt_run(Codec::Zstd, &inputs[1], 2);
    let damaged = RunView::open(&bytes).unwrap();
    let expected = CorruptRun {
        offset: frame_start(&bytes, 2),
        kind: RunCorruption::ChecksumMismatch,
    };

    let mut workspace = MergeWorkspace::new();
    let mut cursor = MergeCursor::new([good.view(), damaged], &mut workspace);
    let mut seen = Vec::new();
    let error = loop {
        match cursor.next_record() {
            Ok(Some(record)) => seen.push(owned(record)),
            Ok(None) => panic!("the merge must reach the damaged frame"),
            Err(error) => break error,
        }
        assert!(seen.len() <= RECORD_BOUND);
    };
    assert_eq!(error, expected);
    assert_eq!(seen, records[..seen.len()]);
    assert_eq!(cursor.next_record(), Err(expected));
}

#[test]
fn a_first_read_of_a_damaged_input_fails_and_keeps_failing() {
    let records = numbered(0..200);
    let inputs = interleave(&records, 2);
    let good = build(Codec::Lz4, &inputs[0], SMALL_BLOCK);
    let bytes = corrupt_run(Codec::Zstd, &inputs[1], 0);
    let damaged = RunView::open(&bytes).unwrap();
    let expected = CorruptRun {
        offset: frame_start(&bytes, 0),
        kind: RunCorruption::ChecksumMismatch,
    };

    let mut workspace = MergeWorkspace::new();
    let mut cursor = MergeCursor::new([good.view(), damaged], &mut workspace);
    assert_eq!(cursor.next_record(), Err(expected));
    assert_eq!(cursor.next_record(), Err(expected), "not a partial stream");
}

#[test]
fn the_workspace_reports_and_reuses_what_it_retains() {
    let records = numbered(0..900);
    let runs = build_all(&interleave(&records, 3), &CODECS, SMALL_BLOCK);
    let mut workspace = MergeWorkspace::new();
    assert_eq!(workspace.capacity(), 0);
    let mut cursor = MergeCursor::new(views(&runs), &mut workspace);
    assert_eq!(rest(&mut cursor).unwrap().len(), records.len());
    let retained = workspace.capacity();
    assert!(
        retained >= SMALL_BLOCK,
        "compressed inputs decode into scratch"
    );
    let mut cursor = MergeCursor::new(views(&runs), &mut workspace);
    assert_eq!(rest(&mut cursor).unwrap().len(), records.len());
    assert_eq!(
        workspace.capacity(),
        retained,
        "a second pass needs no growth"
    );
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

/// One sorted input with collisions, tombstones and long version chains.
/// Sequence numbers come from a small range, so inputs share internal keys.
fn random_input(rng: &mut Rng, target: usize) -> Vec<Owned> {
    let mut records = std::collections::BTreeMap::new();
    while records.len() < target {
        let key = random_bytes(rng, 6);
        let versions = if rng.below(16) == 0 { 40 } else { 3 };
        for _ in 0..rng.below(versions) + 1 {
            let seq = rng.below(64) as u64;
            let value = (rng.below(4) != 0).then(|| vec![rng.next() as u8; rng.below(24)]);
            records.insert((key.clone(), std::cmp::Reverse(seq)), value);
        }
    }
    records
        .into_iter()
        .map(|((key, std::cmp::Reverse(seq)), value)| (key, seq, value))
        .collect()
}

#[test]
fn merges_match_a_reference_model() {
    for seed in 0..48 {
        let mut rng = Rng(9000 + seed);
        let count = rng.below(7);
        let inputs: Vec<Vec<Owned>> = (0..count)
            .map(|_| {
                let size = rng.below(400);
                if size < 20 {
                    Vec::new()
                } else {
                    random_input(&mut rng, size)
                }
            })
            .collect();
        let expected = model(&inputs);
        let runs: Vec<_> = inputs
            .iter()
            .map(|input| build(CODECS[rng.below(3)], input, [64, 256, 4096][rng.below(3)]))
            .collect();

        assert_eq!(scan(&runs), expected, "seed {seed}");

        let mut workspace = MergeWorkspace::new();
        let mut cursor = MergeCursor::new(views(&runs), &mut workspace);
        for _ in 0..80 {
            let probe = if rng.below(3) == 0 && !expected.is_empty() {
                let key = &expected[rng.below(expected.len())].0;
                key[..rng.below(key.len() + 1)].to_vec()
            } else {
                random_bytes(&mut rng, 7)
            };
            let use_prefix = rng.below(2) == 0;
            let wanted = if use_prefix {
                cursor.seek_prefix(&probe).unwrap();
                prefix_of(&expected, &probe)
            } else {
                cursor.seek(&probe).unwrap();
                from_target(&expected, &probe)
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
                    Some(want),
                    "seed {seed} probe {probe:02x?} prefix {use_prefix}"
                );
            }
            if limit == wanted.len() {
                assert_eq!(
                    cursor.next_record().unwrap(),
                    None,
                    "seed {seed} probe {probe:02x?} prefix {use_prefix}"
                );
            }
        }
    }
}
