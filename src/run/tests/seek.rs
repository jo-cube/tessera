//! Seeking and prefix-bounded iteration over runs.
//!
//! Each behavior is checked once per codec. Every collecting loop has an
//! explicit bound, so a cursor that stops advancing fails an assertion instead
//! of exhausting memory. Blocks are kept small so most operations cross them.

use crate::block::{MAX_KEY_LEN, Record, Value};
use crate::run::{
    Codec, CorruptRun, ReadWorkspace, Run, RunBuilder, RunCorruption, RunCursor, RunView,
};

type Owned = (Vec<u8>, u64, Option<Vec<u8>>);

const CODECS: [Codec; 2] = [Codec::None, Codec::Zstd];
const COMPRESSED: [Codec; 1] = [Codec::Zstd];
const RECORD_BOUND: usize = 200_000;
/// Bounds retained bytes too, since keys can be 64 KiB long.
const RETAINED_BOUND: usize = 64 << 20;
/// About 30 small records, which is more than one restart interval.
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
    let mut builder = RunBuilder::with_raw_block_target(codec, target);
    for (key, seq, value) in records {
        let value = value.as_deref().map_or(Value::Tombstone, Value::Bytes);
        builder.add(key, *seq, value).unwrap();
    }
    builder.finish()
}

fn rest(cursor: &mut RunCursor<'_>) -> Result<Vec<Owned>, CorruptRun> {
    let mut records = Vec::new();
    let mut retained = 0;
    while let Some(record) = cursor.next_record()? {
        retained += record.key.len();
        records.push(owned(record));
        assert!(
            records.len() <= RECORD_BOUND && retained <= RETAINED_BOUND,
            "cursor did not terminate"
        );
    }
    Ok(records)
}

fn expected_from(records: &[Owned], target: &[u8]) -> Vec<Owned> {
    records
        .iter()
        .filter(|(key, ..)| key.as_slice() >= target)
        .cloned()
        .collect()
}

fn expected_prefix(records: &[Owned], prefix: &[u8]) -> Vec<Owned> {
    records
        .iter()
        .filter(|(key, ..)| key.starts_with(prefix))
        .cloned()
        .collect()
}

fn seek_all<B: AsRef<[u8]>>(run: &Run<B>, target: &[u8]) -> Vec<Owned> {
    let mut workspace = ReadWorkspace::new();
    let mut cursor = run.cursor(&mut workspace);
    cursor.seek(target).unwrap();
    rest(&mut cursor).unwrap()
}

fn scan_prefix<B: AsRef<[u8]>>(run: &Run<B>, prefix: &[u8]) -> Vec<Owned> {
    let mut workspace = ReadWorkspace::new();
    let mut cursor = run.cursor(&mut workspace);
    cursor.seek_prefix(prefix).unwrap();
    rest(&mut cursor).unwrap()
}

/// Keys `key:0000` upward, enough for several small blocks.
fn numbered(count: usize) -> Vec<Owned> {
    (0..count)
        .map(|i| {
            rec(
                format!("key:{i:04}").as_bytes(),
                i as u64 + 1,
                Some(b"value"),
            )
        })
        .collect()
}

#[test]
fn seek_lands_before_within_and_after_the_run() {
    let records = numbered(300);
    for codec in CODECS {
        let run = build(codec, &records, SMALL_BLOCK);
        assert!(run.block_count() > 3);
        assert_eq!(seek_all(&run, b""), records);
        assert_eq!(seek_all(&run, b"a"), records);
        assert_eq!(seek_all(&run, b"key:0150"), records[150..]);
        assert_eq!(seek_all(&run, b"key:0150\0"), records[151..]);
        assert_eq!(seek_all(&run, b"key:0299"), records[299..]);
        assert_eq!(seek_all(&run, b"key:9"), Vec::new());
        assert_eq!(seek_all(&run, b"z"), Vec::new());
    }
}

#[test]
fn seek_reaches_every_record_across_blocks_and_restarts() {
    let records = numbered(300);
    for codec in CODECS {
        let run = build(codec, &records, SMALL_BLOCK);
        for i in (0..records.len()).step_by(3) {
            assert_eq!(seek_all(&run, &records[i].0), records[i..], "exact {i}");
            let gap = [records[i].0.as_slice(), &[0]].concat();
            assert_eq!(seek_all(&run, &gap), records[i + 1..], "gap {i}");
        }
    }
}

#[test]
fn seek_stops_at_the_newest_version_of_a_chain_spanning_blocks() {
    let mut records = vec![rec(b"a", 1, Some(b"before"))];
    records.extend((1..=400).rev().map(|seq| rec(b"chain", seq, Some(b"v"))));
    records.push(rec(b"z", 1, Some(b"after")));
    for codec in CODECS {
        let run = build(codec, &records, 256);
        assert!(run.block_count() > 3, "the chain must span blocks");
        let found = seek_all(&run, b"chain");
        assert_eq!(found[0].1, 400);
        assert_eq!(found, records[1..]);
        assert_eq!(scan_prefix(&run, b"chain"), records[1..401]);
        assert_eq!(seek_all(&run, b"chain\0"), records[401..]);
    }
}

#[test]
fn tombstones_and_repeated_keys_are_all_yielded() {
    let records = vec![
        rec(b"a", 3, None),
        rec(b"a", 2, Some(b"old")),
        rec(b"a", 1, None),
        rec(b"b", 9, None),
    ];
    for codec in CODECS {
        let run = build(codec, &records, SMALL_BLOCK);
        assert_eq!(scan_prefix(&run, b"a"), records[..3]);
        assert_eq!(seek_all(&run, b"b"), records[3..]);
    }
}

#[test]
fn prefixes_with_no_one_and_many_matches() {
    let records = numbered(300);
    for codec in CODECS {
        let run = build(codec, &records, SMALL_BLOCK);
        assert_eq!(scan_prefix(&run, b""), records, "empty prefix");
        assert_eq!(scan_prefix(&run, b"key:01"), records[100..200], "many");
        assert_eq!(scan_prefix(&run, b"key:0042"), records[42..43], "one");
        assert_eq!(scan_prefix(&run, b"key:9"), Vec::new(), "above every key");
        assert_eq!(scan_prefix(&run, b"key:01x"), Vec::new(), "gap");
        assert_eq!(scan_prefix(&run, b"a"), Vec::new(), "below every key");
        assert_eq!(
            scan_prefix(&run, b"key:00420"),
            Vec::new(),
            "longer than keys"
        );
    }
}

#[test]
fn prefixes_are_binary_and_may_end_in_0xff() {
    let keys: [&[u8]; 11] = [
        &[0x00],
        &[0x00, 0x00],
        &[0x00, 0x01],
        &[0x01],
        &[0xFE, 0xFF],
        &[0xFF],
        &[0xFF, 0x00],
        &[0xFF, 0xFF],
        &[0xFF, 0xFF, 0x01],
        &[0xFF, 0xFF, 0xFF],
        &[0xFF, 0xFF, 0xFF, 0xFF],
    ];
    let records: Vec<Owned> = keys.iter().map(|key| rec(key, 1, Some(b"v"))).collect();
    let prefixes: [&[u8]; 9] = [
        &[],
        &[0x00],
        &[0x01],
        &[0xFE],
        &[0xFE, 0xFF],
        &[0xFF],
        &[0xFF, 0xFF],
        &[0xFF, 0xFF, 0xFF],
        &[0xFF, 0xFF, 0xFF, 0xFF, 0xFF],
    ];
    for codec in CODECS {
        // Single-record blocks put every prefix boundary at a block boundary.
        for target in [1, SMALL_BLOCK] {
            let run = build(codec, &records, target);
            for prefix in prefixes {
                assert_eq!(
                    scan_prefix(&run, prefix),
                    expected_prefix(&records, prefix),
                    "prefix {prefix:02x?} target {target}"
                );
            }
        }
    }
}

#[test]
fn a_full_length_prefix_matches_and_a_longer_one_cannot() {
    let key = vec![b'k'; MAX_KEY_LEN];
    let records = vec![rec(&key, 1, Some(b"v"))];
    let run = build(Codec::Zstd, &records, SMALL_BLOCK);
    assert_eq!(scan_prefix(&run, &key), records);
    assert_eq!(scan_prefix(&run, &vec![b'k'; MAX_KEY_LEN + 1]), Vec::new());
}

#[test]
fn an_empty_run_has_nothing_to_seek() {
    for codec in CODECS {
        let run = build(codec, &[], SMALL_BLOCK);
        assert_eq!(seek_all(&run, b"a"), Vec::new());
        assert_eq!(scan_prefix(&run, b""), Vec::new());
    }
}

#[test]
fn cursors_reposition_in_any_direction_and_after_ending() {
    let records = numbered(300);
    for codec in CODECS {
        let run = build(codec, &records, SMALL_BLOCK);
        let mut workspace = ReadWorkspace::new();
        let mut cursor = run.cursor(&mut workspace);

        cursor.seek(b"key:0250").unwrap();
        assert_eq!(owned(cursor.next_record().unwrap().unwrap()), records[250]);
        cursor.seek(b"key:0010").unwrap();
        assert_eq!(owned(cursor.next_record().unwrap().unwrap()), records[10]);
        cursor.seek(b"key:0299").unwrap();
        assert_eq!(rest(&mut cursor).unwrap(), records[299..]);
        assert_eq!(cursor.next_record().unwrap(), None, "the end is sticky");
        assert_eq!(cursor.next_record().unwrap(), None);

        cursor.seek(b"key:0000").unwrap();
        assert_eq!(owned(cursor.next_record().unwrap().unwrap()), records[0]);

        cursor.seek_prefix(b"key:02").unwrap();
        assert_eq!(rest(&mut cursor).unwrap(), records[200..300]);
        assert_eq!(cursor.next_record().unwrap(), None, "the bound is sticky");

        cursor.seek_prefix(b"key:00").unwrap();
        assert_eq!(rest(&mut cursor).unwrap(), records[..100]);

        // A plain seek removes the bound set by the previous prefix.
        cursor.seek(b"key:0298").unwrap();
        assert_eq!(rest(&mut cursor).unwrap(), records[298..]);
    }
}

#[test]
fn a_new_cursor_forgets_a_previous_prefix() {
    let records = numbered(40);
    let run = build(Codec::Zstd, &records, SMALL_BLOCK);
    let mut workspace = ReadWorkspace::new();
    let mut first = run.cursor(&mut workspace);
    first.seek_prefix(b"key:001").unwrap();
    assert_eq!(rest(&mut first).unwrap().len(), 10);
    let mut second = run.cursor(&mut workspace);
    assert_eq!(rest(&mut second).unwrap(), records);
}

#[test]
fn traversal_loads_each_block_once() {
    let records = numbered(300);
    for codec in CODECS {
        let run = build(codec, &records, SMALL_BLOCK);
        let blocks = run.block_count() as u64;
        let mut workspace = ReadWorkspace::new();

        let mut cursor = run.cursor(&mut workspace);
        assert_eq!(rest(&mut cursor).unwrap().len(), records.len());
        assert_eq!(workspace.blocks_loaded(), blocks);

        let before = workspace.blocks_loaded();
        let mut cursor = run.cursor(&mut workspace);
        cursor.seek(&records[0].0).unwrap();
        assert_eq!(rest(&mut cursor).unwrap().len(), records.len());
        assert_eq!(workspace.blocks_loaded() - before, blocks, "seek shares it");
    }
}

#[test]
fn seeks_load_only_blocks_they_need() {
    let records = numbered(300);
    for codec in CODECS {
        let run = build(codec, &records, SMALL_BLOCK);
        let view = run.view();
        let index = |key: &[u8]| view.locate(key).unwrap().unwrap();
        // The first key of the first block with an interior record to spare.
        let block = index(&records[100].0);
        let first = records.iter().position(|r| index(&r.0) == block).unwrap();

        let mut workspace = ReadWorkspace::new();
        let mut cursor = run.cursor(&mut workspace);
        cursor.seek(b"z").unwrap();
        assert_eq!(cursor.next_record().unwrap(), None);
        assert_eq!(workspace.blocks_loaded(), 0, "beyond the last key");

        let mut cursor = run.cursor(&mut workspace);
        cursor.seek(&records[first].0).unwrap();
        cursor.seek(&records[first + 2].0).unwrap();
        let found = owned(cursor.next_record().unwrap().unwrap());
        assert_eq!(found, records[first + 2]);
        cursor.seek(&records[first + 1].0).unwrap();
        let found = owned(cursor.next_record().unwrap().unwrap());
        assert_eq!(found, records[first + 1]);
        assert_eq!(workspace.blocks_loaded(), 1, "reseeks within the block");

        // A fresh cursor shares no decoded state with the previous query.
        let mut cursor = run.cursor(&mut workspace);
        cursor.seek(&records[first].0).unwrap();
        assert_eq!(workspace.blocks_loaded(), 2);
    }
}

#[test]
fn prefix_scans_stop_without_loading_later_blocks() {
    let records = numbered(300);
    for codec in CODECS {
        let run = build(codec, &records, SMALL_BLOCK);
        let view = run.view();
        let index = |key: &[u8]| view.locate(key).unwrap().unwrap();
        let block = index(&records[100].0);
        let interior = records.iter().position(|r| index(&r.0) == block).unwrap() + 1;

        let mut workspace = ReadWorkspace::new();
        let mut cursor = run.cursor(&mut workspace);
        cursor.seek_prefix(&records[interior].0).unwrap();
        assert_eq!(rest(&mut cursor).unwrap(), records[interior..=interior]);
        assert_eq!(workspace.blocks_loaded(), 1, "ends inside its block");

        // The last key of a block ends its prefix there, and finding that out
        // takes the next block's first record.
        let last = records.iter().rposition(|r| index(&r.0) == block).unwrap();
        let before = workspace.blocks_loaded();
        let mut cursor = run.cursor(&mut workspace);
        cursor.seek_prefix(&records[last].0).unwrap();
        assert_eq!(rest(&mut cursor).unwrap(), records[last..=last]);
        assert_eq!(workspace.blocks_loaded() - before, 2, "one more to end");

        // Once ended, further calls neither return records nor load blocks.
        let mut cursor = run.cursor(&mut workspace);
        cursor.seek_prefix(&records[interior].0).unwrap();
        assert_eq!(rest(&mut cursor).unwrap(), records[interior..=interior]);
        for _ in 0..4 {
            assert_eq!(cursor.next_record().unwrap(), None);
        }
        let loaded = workspace.blocks_loaded();
        let mut cursor = run.cursor(&mut workspace);
        cursor.seek_prefix(&records[interior].0).unwrap();
        assert_eq!(rest(&mut cursor).unwrap(), records[interior..=interior]);
        for _ in 0..4 {
            assert_eq!(cursor.next_record().unwrap(), None);
        }
        assert_eq!(workspace.blocks_loaded() - loaded, 1, "the end is free");
    }
}

fn frame_starts(bytes: &[u8]) -> Vec<usize> {
    let footer = bytes.len() - 48;
    let table = u64::from_le_bytes(bytes[footer..footer + 8].try_into().unwrap()) as usize;
    let count = u32::from_le_bytes(bytes[footer + 16..footer + 20].try_into().unwrap()) as usize;
    (0..count)
        .map(|i| {
            let at = table + i * 12;
            u64::from_le_bytes(bytes[at..at + 8].try_into().unwrap()) as usize
        })
        .collect()
}

#[test]
fn a_corrupt_frame_fails_a_seek_until_a_later_seek_succeeds() {
    let records = numbered(300);
    for codec in COMPRESSED {
        let run = build(codec, &records, SMALL_BLOCK);
        let mut bytes = run.as_bytes().to_vec();
        let starts = frame_starts(&bytes);
        bytes[starts[1] + 12] ^= 0xFF;
        let view = RunView::open(&bytes).unwrap();
        let in_bad_block = records
            .iter()
            .find(|r| view.locate(&r.0).unwrap() == Some(1))
            .unwrap()
            .0
            .clone();
        let expected = CorruptRun {
            offset: starts[1],
            kind: RunCorruption::ChecksumMismatch,
        };

        let mut workspace = ReadWorkspace::new();
        let mut cursor = view.cursor(&mut workspace);
        assert_eq!(cursor.seek(&in_bad_block), Err(expected));
        assert_eq!(cursor.next_record(), Err(expected));
        assert_eq!(cursor.next_record(), Err(expected), "the failure persists");
        assert_eq!(cursor.seek_prefix(&in_bad_block), Err(expected));
        assert_eq!(cursor.next_record(), Err(expected));

        cursor.seek(&records[0].0).unwrap();
        assert_eq!(owned(cursor.next_record().unwrap().unwrap()), records[0]);
    }
}

/// A checksum can pass while the payload still fails to decode, and a failed
/// decode may have overwritten the block the workspace was holding.
#[test]
fn a_failed_decode_forgets_the_block_it_overwrote() {
    let records = numbered(300);
    for codec in COMPRESSED {
        let run = build(codec, &records, SMALL_BLOCK);
        let mut bytes = run.as_bytes().to_vec();
        let starts = frame_starts(&bytes);
        let (start, end) = (starts[1], starts[2]);
        let stored = &mut bytes[start + 12..end];
        let middle = stored.len() / 2;
        stored[middle..].fill(0xFF);
        let mut hasher = crc32fast::Hasher::new();
        hasher.update(&bytes[start..start + 8]);
        hasher.update(&bytes[start + 12..end]);
        let checksum = hasher.finalize().to_le_bytes();
        bytes[start + 8..start + 12].copy_from_slice(&checksum);

        let view = RunView::open(&bytes).unwrap();
        let in_block = |block| {
            records
                .iter()
                .position(|r| view.locate(&r.0).unwrap() == Some(block))
                .unwrap()
        };
        let good = in_block(0);
        let bad = in_block(1);

        let mut workspace = ReadWorkspace::new();
        let mut cursor = view.cursor(&mut workspace);
        cursor.seek(&records[good].0).unwrap();
        assert!(cursor.seek(&records[bad].0).is_err());
        cursor.seek(&records[good].0).unwrap();
        assert_eq!(owned(cursor.next_record().unwrap().unwrap()), records[good]);
    }
}

#[test]
fn iteration_stops_at_a_corrupt_frame_it_crosses_into() {
    let records = numbered(300);
    for codec in COMPRESSED {
        let run = build(codec, &records, SMALL_BLOCK);
        let mut bytes = run.as_bytes().to_vec();
        let starts = frame_starts(&bytes);
        bytes[starts[2] + 12] ^= 0xFF;
        let view = RunView::open(&bytes).unwrap();
        let expected = CorruptRun {
            offset: starts[2],
            kind: RunCorruption::ChecksumMismatch,
        };
        let mut workspace = ReadWorkspace::new();
        let mut cursor = view.cursor(&mut workspace);
        cursor.seek_prefix(b"key:").unwrap();
        let mut seen = Vec::new();
        let error = loop {
            match cursor.next_record() {
                Ok(Some(record)) => seen.push(owned(record)),
                Ok(None) => panic!("the scan must reach the corrupt frame"),
                Err(error) => break error,
            }
            assert!(seen.len() <= RECORD_BOUND);
        };
        assert_eq!(error, expected);
        assert_eq!(seen, records[..seen.len()]);
        assert_eq!(cursor.next_record(), Err(expected));
    }
}

#[test]
fn an_unknown_codec_fails_a_seek_in_any_run() {
    let records = numbered(100);
    for codec in CODECS {
        let run = build(codec, &records, SMALL_BLOCK);
        let mut bytes = run.as_bytes().to_vec();
        let starts = frame_starts(&bytes);
        bytes[starts[0]] = 9;
        let view = RunView::open(&bytes).unwrap();
        let mut workspace = ReadWorkspace::new();
        let mut cursor = view.cursor(&mut workspace);
        let expected = CorruptRun {
            offset: starts[0],
            kind: RunCorruption::UnsupportedCodec,
        };
        // A seek at the first key decodes nothing, so the read reports it.
        cursor.seek(&records[0].0).unwrap();
        assert_eq!(cursor.next_record(), Err(expected));
        // A target inside the block has to read it during the seek.
        assert_eq!(cursor.seek(&records[1].0), Err(expected));
    }
}

#[test]
fn seeks_over_damaged_bytes_never_panic_or_run_away() {
    let records = numbered(120);
    let probes: [&[u8]; 4] = [b"", b"key:0050", b"key:0119\0", b"z"];
    for codec in CODECS {
        let run = build(codec, &records, 512);
        let clean = run.as_bytes();
        for at in (0..clean.len()).step_by(11) {
            let mut bytes = clean.to_vec();
            bytes[at] ^= 0x41;
            let Ok(view) = RunView::open(&bytes) else {
                continue;
            };
            let mut workspace = ReadWorkspace::new();
            let mut cursor = view.cursor(&mut workspace);
            for probe in probes {
                if cursor.seek(probe).is_ok() {
                    let mut steps = 0;
                    while let Ok(Some(_)) = cursor.next_record() {
                        steps += 1;
                        assert!(steps <= RECORD_BOUND);
                    }
                }
                if cursor.seek_prefix(probe).is_ok() {
                    let mut steps = 0;
                    while let Ok(Some(_)) = cursor.next_record() {
                        steps += 1;
                        assert!(steps <= RECORD_BOUND);
                    }
                }
            }
        }
    }
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

/// Sorted records with collisions, shared prefixes and long version chains.
fn random_records(rng: &mut Rng, target: usize) -> Vec<Owned> {
    let mut records = std::collections::BTreeMap::new();
    while records.len() < target {
        let key = random_bytes(rng, 7);
        // Some keys get a long chain, so that chains span small blocks.
        let versions = if rng.below(16) == 0 { 40 } else { 3 };
        for _ in 0..rng.below(versions) + 1 {
            let seq = rng.below(1 << 20) as u64;
            let value = (rng.below(4) != 0).then(|| vec![rng.next() as u8; rng.below(24)]);
            records.insert((key.clone(), std::cmp::Reverse(seq)), value);
        }
    }
    records
        .into_iter()
        .map(|((key, std::cmp::Reverse(seq)), value)| (key, seq, value))
        .collect()
}

/// One long-lived cursor is repositioned at random and read partway, which is
/// where stale decoded state or a stale bound would show.
#[test]
fn cursors_match_a_reference_model() {
    for seed in 0..48 {
        let mut rng = Rng(7000 + seed);
        let count = 1 + rng.below(1500);
        let records = random_records(&mut rng, count);
        for codec in CODECS {
            let target = [64, 256, 4096][rng.below(3)];
            let run = build(codec, &records, target);
            let mut workspace = ReadWorkspace::new();
            let mut cursor = run.cursor(&mut workspace);
            for _ in 0..120 {
                let probe = if rng.below(3) == 0 && !records.is_empty() {
                    let key = &records[rng.below(records.len())].0;
                    key[..rng.below(key.len() + 1)].to_vec()
                } else {
                    random_bytes(&mut rng, 8)
                };
                let use_prefix = rng.below(2) == 0;
                let expected = if use_prefix {
                    cursor.seek_prefix(&probe).unwrap();
                    expected_prefix(&records, &probe)
                } else {
                    cursor.seek(&probe).unwrap();
                    expected_from(&records, &probe)
                };
                let limit = match rng.below(3) {
                    0 => expected.len(),
                    1 => rng.below(expected.len() + 1),
                    _ => expected.len().min(rng.below(8)),
                };
                for wanted in &expected[..limit] {
                    let found = cursor.next_record().unwrap().map(owned);
                    assert_eq!(
                        found.as_ref(),
                        Some(wanted),
                        "seed {seed} {codec:?} probe {probe:02x?} prefix {use_prefix}"
                    );
                }
                if limit == expected.len() {
                    assert_eq!(
                        cursor.next_record().unwrap(),
                        None,
                        "seed {seed} {codec:?} probe {probe:02x?} prefix {use_prefix}"
                    );
                }
            }
        }
    }
}

#[test]
fn peek_shows_the_next_record_without_advancing() {
    let records = numbered(300);
    for codec in CODECS {
        let run = build(codec, &records, SMALL_BLOCK);
        let mut workspace = ReadWorkspace::new();
        let mut cursor = run.cursor(&mut workspace);
        for expected in &records {
            let first = owned(cursor.peek().unwrap().unwrap());
            assert_eq!(&first, expected);
            assert_eq!(owned(cursor.peek().unwrap().unwrap()), first, "repeatable");
            assert_eq!(owned(cursor.next_record().unwrap().unwrap()), first);
        }
        assert_eq!(cursor.peek().unwrap(), None);
        assert_eq!(cursor.next_record().unwrap(), None);
        assert_eq!(workspace.blocks_loaded(), run.block_count() as u64);
    }
}

#[test]
fn peek_observes_seeks_and_prefix_bounds() {
    let records = numbered(100);
    for codec in CODECS {
        let run = build(codec, &records, SMALL_BLOCK);
        let mut workspace = ReadWorkspace::new();
        let mut cursor = run.cursor(&mut workspace);
        cursor.seek(b"key:0050").unwrap();
        assert_eq!(owned(cursor.peek().unwrap().unwrap()), records[50]);

        // A seek discards a head that was peeked earlier.
        cursor.seek(b"key:0010").unwrap();
        assert_eq!(owned(cursor.peek().unwrap().unwrap()), records[10]);

        cursor.seek_prefix(b"key:0099").unwrap();
        assert_eq!(owned(cursor.next_record().unwrap().unwrap()), records[99]);
        assert_eq!(cursor.peek().unwrap(), None, "the bound ends a peek");
        assert_eq!(cursor.next_record().unwrap(), None);
    }
}

#[test]
fn peek_preserves_tombstones_and_empty_values() {
    let records = vec![
        rec(b"a", 2, None),
        rec(b"a", 1, Some(b"")),
        rec(b"b", 1, Some(b"v")),
    ];
    for codec in CODECS {
        let run = build(codec, &records, SMALL_BLOCK);
        let mut workspace = ReadWorkspace::new();
        let mut cursor = run.cursor(&mut workspace);
        for expected in &records {
            assert_eq!(&owned(cursor.peek().unwrap().unwrap()), expected);
            assert_eq!(&owned(cursor.next_record().unwrap().unwrap()), expected);
        }
    }
}

#[test]
fn peek_reports_a_corrupt_frame_and_leaves_a_seek_able_to_recover() {
    let records = numbered(300);
    for codec in COMPRESSED {
        let run = build(codec, &records, SMALL_BLOCK);
        let mut bytes = run.as_bytes().to_vec();
        let starts = frame_starts(&bytes);
        bytes[starts[1] + 12] ^= 0xFF;
        let view = RunView::open(&bytes).unwrap();
        let bad = records
            .iter()
            .position(|r| view.locate(&r.0).unwrap() == Some(1))
            .unwrap();
        let mut workspace = ReadWorkspace::new();
        let mut cursor = view.cursor(&mut workspace);
        for expected in &records[..bad] {
            assert_eq!(&owned(cursor.next_record().unwrap().unwrap()), expected);
        }
        assert!(cursor.peek().is_err());
        cursor.seek(&records[0].0).unwrap();
        assert_eq!(owned(cursor.peek().unwrap().unwrap()), records[0]);
    }
}

#[test]
fn a_finished_prefix_reads_nothing_further_however_often_it_is_polled() {
    let records = numbered(300);
    let run = build(Codec::Zstd, &records, SMALL_BLOCK);
    assert!(run.block_count() > 2);
    let mut workspace = ReadWorkspace::new();
    let mut cursor = run.cursor(&mut workspace);
    cursor.seek_prefix(&records[0].0).unwrap();
    assert_eq!(owned(cursor.next_record().unwrap().unwrap()), records[0]);
    for _ in 0..2 * records.len() {
        assert_eq!(cursor.peek().unwrap(), None);
        assert_eq!(cursor.next_record().unwrap(), None);
    }
    assert_eq!(workspace.blocks_loaded(), 1);
}

#[test]
fn an_oversized_prefix_discards_a_peeked_record() {
    let records = numbered(10);
    let run = build(Codec::Zstd, &records, SMALL_BLOCK);
    let mut workspace = ReadWorkspace::new();
    let mut cursor = run.cursor(&mut workspace);
    assert_eq!(owned(cursor.peek().unwrap().unwrap()), records[0]);
    cursor
        .seek_prefix(&vec![b'k'; crate::block::MAX_KEY_LEN + 1])
        .unwrap();
    assert_eq!(cursor.peek().unwrap(), None);
    assert_eq!(cursor.next_record().unwrap(), None);
}

#[test]
fn seeking_to_the_first_key_reuses_a_first_block_already_held() {
    let records = numbered(100);
    let run = build(Codec::Zstd, &records, SMALL_BLOCK);
    let mut workspace = ReadWorkspace::new();
    let mut cursor = run.cursor(&mut workspace);
    assert_eq!(owned(cursor.next_record().unwrap().unwrap()), records[0]);
    cursor.seek(&records[0].0).unwrap();
    assert_eq!(owned(cursor.next_record().unwrap().unwrap()), records[0]);
    assert_eq!(workspace.blocks_loaded(), 1);
}
