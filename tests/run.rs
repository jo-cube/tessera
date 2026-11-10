//! Executable specification of the run format and its readers.
//!
//! Every loop that retains data has an explicit bound, so a broken reader
//! fails an assertion instead of exhausting memory. Bytes are assembled by hand
//! where a test must not depend on the crate's own encoder.

use std::cmp::Reverse;
use std::collections::BTreeMap;
use std::sync::Arc;

use tessera::block::{Block, BuildError, Corruption, Value};
use tessera::run::{
    CorruptRun, Run, RunBuilder, RunCorruption, RunCursor, RunPosition, RunView, TARGET_BLOCK_LEN,
};

type Owned = (Vec<u8>, u64, Option<Vec<u8>>);
type Model = BTreeMap<(Vec<u8>, Reverse<u64>), Option<Vec<u8>>>;

const RECORD_BOUND: usize = 200_000;
const FRAME_HEADER: usize = 12;
const ENTRY: usize = 12;
const FOOTER: usize = 28;

fn rec(key: &[u8], seq: u64, value: Option<&[u8]>) -> Owned {
    (key.to_vec(), seq, value.map(<[u8]>::to_vec))
}

fn as_value(value: &Option<Vec<u8>>) -> Value<'_> {
    value.as_deref().map_or(Value::Tombstone, Value::Bytes)
}

fn build(records: &[Owned], target: usize) -> Run<Vec<u8>> {
    let mut builder = RunBuilder::with_target_block_len(target);
    for (key, seq, value) in records {
        builder.add(key, *seq, as_value(value)).unwrap();
    }
    assert_eq!(builder.len(), records.len());
    builder.finish()
}

fn owned(record: tessera::block::Record<'_>) -> Owned {
    let value = match record.value {
        Value::Bytes(bytes) => Some(bytes.to_vec()),
        Value::Tombstone => None,
    };
    (record.key.to_vec(), record.seq, value)
}

fn collect(cursor: &mut RunCursor<'_>) -> Vec<Owned> {
    let mut records = Vec::new();
    while let Some(record) = cursor.next_record() {
        records.push(owned(record));
        assert!(records.len() <= RECORD_BOUND, "cursor did not terminate");
    }
    records
}

fn walk(view: RunView<'_>) -> Result<Vec<Owned>, CorruptRun> {
    let mut position = RunPosition::new();
    let mut records = Vec::new();
    while let Some(record) = position.next_record(view)? {
        records.push(owned(record));
        assert!(records.len() <= RECORD_BOUND, "position did not terminate");
    }
    Ok(records)
}

/// Offsets of each region, decoded independently of the crate.
#[derive(Clone, Copy, Debug)]
struct Layout {
    table: usize,
    keys: usize,
    footer: usize,
    block_count: usize,
}

fn u32_at(bytes: &[u8], at: usize) -> u32 {
    u32::from_le_bytes(bytes[at..at + 4].try_into().unwrap())
}

fn u64_at(bytes: &[u8], at: usize) -> u64 {
    u64::from_le_bytes(bytes[at..at + 8].try_into().unwrap())
}

fn layout(bytes: &[u8]) -> Layout {
    let footer = bytes.len() - FOOTER;
    let table = u64_at(bytes, footer) as usize;
    let block_count = u32_at(bytes, footer + 16) as usize;
    Layout {
        table,
        keys: table + block_count * ENTRY,
        footer,
        block_count,
    }
}

/// Returns each block's bytes, from the table and the frame boundaries.
fn block_payloads(bytes: &[u8]) -> Vec<&[u8]> {
    let l = layout(bytes);
    (0..l.block_count)
        .map(|i| {
            let start = u64_at(bytes, l.table + i * ENTRY) as usize;
            let end = if i + 1 < l.block_count {
                u64_at(bytes, l.table + (i + 1) * ENTRY) as usize
            } else {
                l.table
            };
            &bytes[start + FRAME_HEADER..end]
        })
        .collect()
}

/// Returns every block's records.
fn blocks(run: &Run<Vec<u8>>) -> Vec<Vec<Owned>> {
    block_payloads(run.as_bytes())
        .into_iter()
        .map(|payload| {
            let block = Block::new(payload).unwrap();
            let mut cursor = block.cursor();
            let mut records = Vec::new();
            while let Some(record) = cursor.next_record() {
                records.push(owned(record));
                assert!(records.len() <= RECORD_BOUND);
            }
            records
        })
        .collect()
}

fn leb128(mut value: u64, out: &mut Vec<u8>) {
    while value >= 0x80 {
        out.push(value as u8 | 0x80);
        value >>= 7;
    }
    out.push(value as u8);
}

/// Encodes a block with whole keys and a single restart, by hand.
fn raw_block(records: &[Owned]) -> Vec<u8> {
    let mut block = Vec::new();
    for (key, seq, value) in records {
        leb128(0, &mut block);
        leb128(key.len() as u64, &mut block);
        leb128(value.as_ref().map_or(0, |v| v.len() as u64 + 1), &mut block);
        leb128(*seq, &mut block);
        block.extend_from_slice(key);
        block.extend_from_slice(value.as_deref().unwrap_or_default());
    }
    if records.is_empty() {
        block.extend_from_slice(&0u32.to_le_bytes());
    } else {
        block.extend_from_slice(&0u32.to_le_bytes());
        block.extend_from_slice(&1u32.to_le_bytes());
    }
    block
}

/// Assembles a version 1 run by hand from block bytes and index keys.
fn assemble(blocks: &[Vec<u8>], last_keys: &[&[u8]], record_count: u64) -> Vec<u8> {
    let mut run = Vec::new();
    let mut table = Vec::new();
    let mut keys = Vec::new();
    for (block, key) in blocks.iter().zip(last_keys) {
        table.extend_from_slice(&(run.len() as u64).to_le_bytes());
        run.extend_from_slice(&[0, 0, 0, 0]);
        run.extend_from_slice(&(block.len() as u32).to_le_bytes());
        run.extend_from_slice(&0u32.to_le_bytes());
        run.extend_from_slice(block);
        keys.extend_from_slice(key);
        table.extend_from_slice(&(keys.len() as u32).to_le_bytes());
    }
    let index_offset = run.len() as u64;
    run.extend_from_slice(&table);
    run.extend_from_slice(&keys);
    run.extend_from_slice(&index_offset.to_le_bytes());
    run.extend_from_slice(&record_count.to_le_bytes());
    run.extend_from_slice(&(blocks.len() as u32).to_le_bytes());
    run.extend_from_slice(&1u32.to_le_bytes());
    run.extend_from_slice(b"TSRA");
    run
}

fn three_block_bytes() -> Vec<u8> {
    let blocks = [
        raw_block(&[rec(b"apple", 5, Some(b"one")), rec(b"apple", 4, None)]),
        raw_block(&[rec(b"mango", 9, Some(b"two"))]),
        raw_block(&[rec(b"pear", 3, Some(b"")), rec(b"plum", 8, Some(b"three"))]),
    ];
    assemble(&blocks, &[b"apple", b"mango", b"plum"], 5)
}

fn sample_records(count: usize) -> Vec<Owned> {
    (0..count)
        .map(|i| {
            rec(
                format!("key{i:05}").as_bytes(),
                1_000_000 - i as u64,
                Some(format!("value-{i}").as_bytes()),
            )
        })
        .collect()
}

#[derive(Clone, Copy)]
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^ (z >> 31)
    }

    fn below(&mut self, limit: usize) -> usize {
        (self.next() % limit as u64) as usize
    }
}

#[test]
fn empty_run_is_a_footer_alone() {
    let run = RunBuilder::new().finish();
    assert_eq!(run.as_bytes().len(), FOOTER);
    assert!(run.is_empty());
    assert_eq!(run.block_count(), 0);
    assert_eq!(run.get(b"anything"), None);
    assert_eq!(run.view().locate(b"anything"), Ok(None));
    assert_eq!(run.cursor().next_record(), None);

    let reparsed = Run::new(run.as_bytes()).unwrap();
    assert!(reparsed.is_empty());
    assert_eq!(walk(reparsed.view()).unwrap(), vec![]);
}

#[test]
fn format_is_exactly_the_documented_bytes() {
    let run = build(
        &[rec(b"a", 2, Some(b"x")), rec(b"b", 1, None)],
        1, // Each record becomes its own block.
    );

    #[rustfmt::skip]
    let expected: Vec<u8> = [
        // frame 0: codec, reserved, decoded_len, checksum
        &[0, 0, 0, 0, 14, 0, 0, 0, 0, 0, 0, 0][..],
        // block 0: shared, suffix_len, value_tag, seq, suffix, value, restart, count
        &[0, 1, 2, 2, b'a', b'x', 0, 0, 0, 0, 1, 0, 0, 0],
        // frame 1
        &[0, 0, 0, 0, 13, 0, 0, 0, 0, 0, 0, 0],
        // block 1
        &[0, 1, 0, 1, b'b', 0, 0, 0, 0, 1, 0, 0, 0],
        // table: frame_offset, key_end
        &[0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0],
        &[26, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0],
        // key area
        b"ab",
        // footer: index_offset, record_count, block_count, version, magic
        &[51, 0, 0, 0, 0, 0, 0, 0],
        &[2, 0, 0, 0, 0, 0, 0, 0],
        &[2, 0, 0, 0],
        &[1, 0, 0, 0],
        b"TSRA",
    ]
    .concat();
    assert_eq!(run.as_bytes(), expected);
}

#[test]
fn hand_assembled_run_is_accepted_and_read() {
    let bytes = three_block_bytes();
    let run = Run::new(bytes.as_slice()).unwrap();
    assert_eq!(run.len(), 5);
    assert_eq!(run.block_count(), 3);
    assert_eq!(run.get(b"apple").unwrap().seq, 5);
    assert_eq!(run.get(b"mango").unwrap().value, Value::Bytes(b"two"));
    assert_eq!(run.get(b"pear").unwrap().value, Value::Bytes(b""));
    assert_eq!(run.get(b"plum").unwrap().seq, 8);
    assert_eq!(run.get(b"melon"), None);
    assert_eq!(
        collect(&mut run.cursor()),
        vec![
            rec(b"apple", 5, Some(b"one")),
            rec(b"apple", 4, None),
            rec(b"mango", 9, Some(b"two")),
            rec(b"pear", 3, Some(b"")),
            rec(b"plum", 8, Some(b"three")),
        ]
    );
}

#[test]
fn single_block_run_round_trips() {
    let records = sample_records(50);
    let run = build(&records, TARGET_BLOCK_LEN);
    assert_eq!(run.block_count(), 1);
    assert_eq!(run.len(), 50);
    assert_eq!(collect(&mut run.cursor()), records);
    for (key, seq, _) in &records {
        assert_eq!(run.get(key).unwrap().seq, *seq);
    }
}

#[test]
fn multiple_blocks_preserve_order_and_every_key() {
    let records = sample_records(2000);
    let run = build(&records, 512);
    assert!(run.block_count() > 10);

    assert_eq!(collect(&mut run.cursor()), records);
    assert_eq!(walk(run.view()).unwrap(), records);
    for (key, seq, value) in &records {
        let found = run.get(key).unwrap();
        assert_eq!((found.seq, found.value), (*seq, as_value(value)));
    }
    // The records either side of every block boundary.
    let by_block = blocks(&run);
    for pair in by_block.windows(2) {
        let last = pair[0].last().unwrap();
        let first = pair[1].first().unwrap();
        assert_eq!(run.get(&last.0).unwrap().seq, last.1);
        assert_eq!(run.get(&first.0).unwrap().seq, first.1);
    }
}

#[test]
fn locate_selects_the_only_candidate_block() {
    let records = sample_records(1000);
    let run = build(&records, 512);
    let view = run.view();
    for (index, block) in blocks(&run).iter().enumerate() {
        for (key, ..) in block {
            assert_eq!(view.locate(key), Ok(Some(index)));
        }
    }
    assert_eq!(view.locate(b"zzz"), Ok(None));
    assert_eq!(view.locate(b""), Ok(Some(0)));
}

#[test]
fn lookup_returns_the_newest_version_and_iteration_exposes_all() {
    let records = vec![
        rec(b"a", 9, Some(b"a9")),
        rec(b"a", 5, None),
        rec(b"a", 1, Some(b"a1")),
        rec(b"b", 7, None),
        rec(b"b", 6, Some(b"b6")),
    ];
    for target in [1, 8, 32, 4096] {
        let run = build(&records, target);
        let newest = run.get(b"a").unwrap();
        assert_eq!((newest.seq, newest.value), (9, Value::Bytes(b"a9")));
        // A newest version that is a tombstone is returned, not skipped.
        let deleted = run.get(b"b").unwrap();
        assert_eq!((deleted.seq, deleted.value), (7, Value::Tombstone));
        assert_eq!(collect(&mut run.cursor()), records, "target {target}");
    }
}

#[test]
fn tombstone_and_empty_value_are_distinct() {
    let records = vec![rec(b"empty", 2, Some(b"")), rec(b"gone", 1, None)];
    let run = build(&records, 1);
    assert_eq!(run.get(b"empty").unwrap().value, Value::Bytes(b""));
    assert_eq!(run.get(b"gone").unwrap().value, Value::Tombstone);
}

#[test]
fn misses_are_reported_everywhere_in_the_key_space() {
    let records = vec![
        rec(b"b", 1, Some(b"")),
        rec(b"d", 1, Some(b"")),
        rec(b"dd", 1, Some(b"")),
        rec(b"f", 1, Some(b"")),
    ];
    let run = build(&records, 1);
    assert_eq!(run.block_count(), 4);
    for absent in [&b""[..], b"a", b"c", b"d\0", b"de", b"e", b"f\0", b"g"] {
        assert_eq!(run.get(absent), None, "{absent:?}");
    }
    assert_eq!(run.view().get(b"g"), Ok(None));
}

#[test]
fn empty_user_key_is_supported() {
    let records = vec![
        rec(b"", 2, Some(b"v")),
        rec(b"", 1, None),
        rec(b"a", 1, None),
    ];
    let run = build(&records, 1);
    assert_eq!(run.get(b"").unwrap().seq, 2);
    Run::new(run.as_bytes()).unwrap();
    assert_eq!(collect(&mut run.cursor()), records);
}

#[test]
fn blocks_land_near_the_target() {
    let records: Vec<Owned> = (0..4000)
        .map(|i| rec(format!("key{i:06}").as_bytes(), 1, Some(&[7; 40])))
        .collect();
    let target = 1024;
    let run = build(&records, target);
    let payloads = block_payloads(run.as_bytes());
    let (last, whole) = payloads.split_last().unwrap();
    assert!(whole.len() > 10);
    for payload in whole {
        // One record is about 60 bytes, and cuts land within half of one.
        assert!(
            payload.len().abs_diff(target) <= 64,
            "block of {} bytes",
            payload.len()
        );
    }
    assert!(last.len() <= target + 64);
}

/// A cut lands on whichever side of the target is closer, so a record much
/// larger than the remaining space moves to the next block.
#[test]
fn large_ordinary_records_prefer_the_nearer_boundary() {
    for (value_len, per_block) in [(800, 1), (300, 3)] {
        let value = vec![7; value_len];
        let records: Vec<Owned> = (0..30)
            .map(|i| rec(format!("{i:02}").as_bytes(), 1, Some(&value)))
            .collect();
        let run = build(&records, 1024);
        let sizes: Vec<usize> = blocks(&run).iter().map(Vec::len).collect();
        assert!(sizes.iter().all(|&n| n == per_block), "{sizes:?}");
        assert_eq!(collect(&mut run.cursor()), records);
    }
}

#[test]
fn oversized_record_occupies_its_own_block() {
    let large = vec![9; 100 << 10];
    let records = vec![
        rec(b"a", 1, Some(b"small")),
        rec(b"b", 1, Some(&large)),
        rec(b"c", 1, Some(b"small")),
    ];
    let run = build(&records, TARGET_BLOCK_LEN);
    let by_block = blocks(&run);
    assert_eq!(by_block.len(), 3);
    assert_eq!(by_block[1], vec![records[1].clone()]);
    assert_eq!(run.get(b"b").unwrap().value, Value::Bytes(&large));
    Run::new(run.as_bytes()).unwrap();
    assert_eq!(collect(&mut run.cursor()), records);
}

#[test]
fn consecutive_oversized_records_each_get_a_block() {
    let large = vec![1; 3000];
    let records: Vec<Owned> = (0..5)
        .map(|i| rec(format!("k{i}").as_bytes(), 1, Some(&large)))
        .collect();
    let run = build(&records, 1024);
    assert_eq!(run.block_count(), 5);
    assert_eq!(collect(&mut run.cursor()), records);
}

#[test]
fn oversized_version_inside_a_chain_splits_it_safely() {
    let large = vec![2; 5000];
    let records = vec![
        rec(b"chain", 9, Some(b"new")),
        rec(b"chain", 8, Some(&large)),
        rec(b"chain", 7, Some(b"old")),
        rec(b"next", 1, None),
    ];
    let run = build(&records, 1024);
    let by_block = blocks(&run);
    assert_eq!(by_block[0], vec![records[0].clone()]);
    assert_eq!(by_block[1][0], records[1]);
    assert_eq!(run.get(b"chain").unwrap().seq, 9);
    Run::new(run.as_bytes()).unwrap();
    assert_eq!(collect(&mut run.cursor()), records);
}

#[test]
fn ordinary_version_chains_are_not_split() {
    let mut records = Vec::new();
    for i in 0..400 {
        let key = format!("chain{i:04}");
        for version in 0..8u64 {
            records.push(rec(key.as_bytes(), 100 - version, Some(&[3; 32])));
        }
    }
    let run = build(&records, 1024);
    let by_block = blocks(&run);
    assert!(by_block.len() > 5);
    for pair in by_block.windows(2) {
        assert_ne!(
            pair[0].last().unwrap().0,
            pair[1].first().unwrap().0,
            "a chain under the target was split"
        );
    }
}

#[test]
fn exceptionally_long_chain_spans_bounded_blocks() {
    let mut records = vec![rec(b"before", 1, Some(b"b"))];
    records.extend((0..5000u64).map(|i| rec(b"long", 10_000 - i, Some(&[4; 16]))));
    records.push(rec(b"tail", 1, None));

    let target = 1024;
    let run = build(&records, target);
    let by_block = blocks(&run);
    assert!(by_block.len() > 5, "chain must span blocks");
    for payload in block_payloads(run.as_bytes()) {
        assert!(payload.len() <= 2 * target + 64, "{} bytes", payload.len());
    }

    // The newest version is found even though older ones fill later blocks.
    assert_eq!(run.get(b"long").unwrap().seq, 10_000);
    assert_eq!(run.get(b"before").unwrap().seq, 1);
    assert_eq!(run.get(b"tail").unwrap().seq, 1);
    assert_eq!(run.view().locate(b"long"), Ok(Some(0)));
    Run::new(run.as_bytes()).unwrap();
    assert_eq!(collect(&mut run.cursor()), records);
}

#[test]
fn chain_boundary_blocks_keep_descending_sequence_numbers() {
    let records: Vec<Owned> = (0..300u64)
        .map(|i| rec(b"k", 1000 - i, Some(&[1; 8])))
        .collect();
    let run = build(&records, 1);
    assert_eq!(run.block_count(), 300);
    let mut cursor = run.cursor();
    let mut previous = u64::MAX;
    while let Some(record) = cursor.next_record() {
        assert!(record.seq < previous);
        previous = record.seq;
    }
    assert_eq!(previous, 701);
}

#[test]
fn builder_rejects_invalid_ordering() {
    let mut builder = RunBuilder::new();
    builder.add(b"m", 5, Value::Tombstone).unwrap();
    let rejected = [
        (&b"a"[..], 9, BuildError::OutOfOrder),
        (b"m", 6, BuildError::OutOfOrder),
        (b"m", 5, BuildError::DuplicateKey),
    ];
    for (key, seq, error) in rejected {
        assert_eq!(builder.add(key, seq, Value::Tombstone), Err(error));
    }
    builder.add(b"m", 4, Value::Tombstone).unwrap();
    builder.add(b"n", 9, Value::Tombstone).unwrap();
    assert_eq!(builder.len(), 3);
}

#[test]
fn ordering_is_enforced_across_block_boundaries() {
    // A target of 1 cuts on every record, so the previous record has left
    // the open block by the time the next one arrives.
    let mut builder = RunBuilder::with_target_block_len(1);
    builder.add(b"k", 5, Value::Bytes(b"xx")).unwrap();
    builder.add(b"l", 5, Value::Bytes(b"xx")).unwrap();
    assert_eq!(
        builder.add(b"k", 9, Value::Bytes(b"xx")),
        Err(BuildError::OutOfOrder)
    );
    assert_eq!(
        builder.add(b"l", 5, Value::Bytes(b"xx")),
        Err(BuildError::DuplicateKey)
    );
    assert_eq!(
        builder.add(b"l", 6, Value::Bytes(b"xx")),
        Err(BuildError::OutOfOrder)
    );
}

#[test]
fn limit_rejections_are_reported() {
    let mut builder = RunBuilder::new();
    let key = vec![b'k'; tessera::block::MAX_KEY_LEN + 1];
    assert_eq!(
        builder.add(&key, 1, Value::Tombstone),
        Err(BuildError::KeyTooLong { len: key.len() })
    );
    assert!(builder.is_empty());
}

#[test]
fn rejected_records_do_not_change_the_layout() {
    let records = sample_records(300);
    let clean = build(&records, 128);

    let mut builder = RunBuilder::with_target_block_len(128);
    for (key, seq, value) in &records {
        // A large out-of-order record would force a cut if it were not
        // rejected first.
        assert!(builder.is_empty() || builder.add(b"", 0, Value::Bytes(&[0; 4096])).is_err());
        builder.add(key, *seq, as_value(value)).unwrap();
    }
    assert_eq!(builder.finish().as_bytes(), clean.as_bytes());
}

#[test]
fn construction_is_deterministic() {
    let records = sample_records(500);
    assert_eq!(
        build(&records, 256).as_bytes(),
        build(&records, 256).as_bytes()
    );
}

#[test]
fn finished_run_is_accepted_by_full_validation() {
    for count in [0, 1, 2, 17, 500] {
        for target in [1, 64, 1024] {
            let run = build(&sample_records(count), target);
            let reparsed = Run::new(run.as_bytes()).unwrap();
            assert_eq!(reparsed.len(), count);
            assert_eq!(reparsed.block_count(), run.block_count());
        }
    }
}

#[test]
fn runs_work_over_shared_and_borrowed_storage() {
    let records = sample_records(100);
    let bytes = build(&records, 128).as_bytes().to_vec();
    let shared: Arc<[u8]> = bytes.as_slice().into();
    let run = Run::new(shared).unwrap();
    assert_eq!(collect(&mut run.cursor()), records);
    assert_eq!(Run::new(bytes.as_slice()).unwrap().len(), 100);
}

#[test]
fn runtime_descriptor_owns_only_its_storage() {
    assert_eq!(size_of::<Run<Vec<u8>>>(), size_of::<Vec<u8>>());
    assert_eq!(size_of::<Run<&[u8]>>(), size_of::<&[u8]>());
}

#[test]
fn run_supports_concurrent_readers() {
    fn assert_shareable<T: Send + Sync>(_: &T) {}

    let records = sample_records(2000);
    let run = build(&records, 256);
    assert_shareable(&run);
    assert_shareable(&run.view());

    std::thread::scope(|scope| {
        for thread in 0..2 {
            let (run, records) = (&run, &records);
            scope.spawn(move || {
                let mut rng = Rng(thread);
                for _ in 0..2000 {
                    let (key, seq, _) = &records[rng.below(records.len())];
                    assert_eq!(run.get(key).unwrap().seq, *seq);
                }
                assert_eq!(collect(&mut run.cursor()), *records);
            });
        }
    });
}

#[test]
fn position_lives_beside_the_storage_it_reads() {
    struct Owner {
        bytes: Vec<u8>,
        position: RunPosition,
    }

    impl Owner {
        fn step(&mut self) -> Option<Owned> {
            let view = RunView::open(&self.bytes).unwrap();
            self.position.next_record(view).unwrap().map(owned)
        }
    }

    let records = sample_records(100);
    let mut owner = Owner {
        bytes: build(&records, 64).as_bytes().to_vec(),
        position: RunPosition::new(),
    };
    for expected in &records {
        assert_eq!(owner.step().as_ref(), Some(expected));
    }
    assert_eq!(owner.step(), None);
    assert_eq!(owner.step(), None);

    owner.position.reset();
    assert_eq!(owner.step().as_ref(), Some(&records[0]));
}

/// Generates a sorted, deduplicated record list with some long chains.
fn random_model(rng: &mut Rng, keys: usize) -> Model {
    let mut model = BTreeMap::new();
    for _ in 0..keys {
        let len = 1 + rng.below(12);
        let key: Vec<u8> = (0..len).map(|_| b'a' + rng.below(3) as u8).collect();
        let versions = match rng.below(20) {
            0 => 40 + rng.below(40),
            1..=5 => 2 + rng.below(6),
            _ => 1,
        };
        for _ in 0..versions {
            let value = match rng.below(5) {
                0 => None,
                1 => Some(Vec::new()),
                2 if rng.below(10) == 0 => Some(vec![9; 300 + rng.below(3000)]),
                _ => Some(vec![1; rng.below(48)]),
            };
            model.insert((key.clone(), Reverse(rng.next() >> rng.below(64))), value);
        }
    }
    model
}

fn flatten(model: &Model) -> Vec<Owned> {
    model
        .iter()
        .map(|((key, seq), value)| (key.clone(), seq.0, value.clone()))
        .collect()
}

#[test]
fn iteration_and_lookup_match_a_reference_model() {
    for seed in 0..48u64 {
        let mut rng = Rng(seed);
        let keys = 20 + rng.below(300);
        let records = flatten(&random_model(&mut rng, keys));
        let target = [1, 16, 64, 256, 1024, 4096, TARGET_BLOCK_LEN][seed as usize % 7];
        let run = build(&records, target);

        assert_eq!(collect(&mut run.cursor()), records, "seed {seed}");
        let validated = Run::new(run.as_bytes().to_vec()).unwrap();
        assert_eq!(validated.len(), records.len());
        assert_eq!(walk(validated.view()).unwrap(), records, "seed {seed}");

        let mut newest = BTreeMap::new();
        for record in records.iter().rev() {
            newest.insert(record.0.clone(), record);
        }
        for (key, (_, seq, value)) in &newest {
            let found = run.get(key).expect("model key must be present");
            assert_eq!(
                (found.seq, found.value),
                (*seq, as_value(value)),
                "seed {seed}"
            );
            assert_eq!(run.view().get(key).unwrap(), Some(found));
        }
        for _ in 0..500 {
            let len = rng.below(14);
            let probe: Vec<u8> = (0..len).map(|_| b'a' + rng.below(4) as u8).collect();
            let expected = newest.get(&probe).map(|record| record.1);
            assert_eq!(run.get(&probe).map(|v| v.seq), expected, "seed {seed}");
        }
    }
}

fn assert_read_paths_are_safe(bytes: &[u8]) {
    if let Ok(view) = RunView::open(bytes) {
        let _ = walk(view);
        for probe in [&b""[..], b"apple", b"mango", b"pear", b"plum", b"zzz"] {
            let _ = view.get(probe);
            let _ = view.locate(probe);
        }
    }
    if let Ok(run) = Run::new(bytes) {
        // Accepted bytes must then read without any panic.
        let mut cursor = run.cursor();
        let mut steps = 0;
        while cursor.next_record().is_some() {
            steps += 1;
            assert!(steps <= RECORD_BOUND);
        }
        assert_eq!(steps, run.len());
        for probe in [&b""[..], b"apple", b"mango", b"pear", b"plum", b"zzz"] {
            let _ = run.get(probe);
        }
    }
}

type Mutation = fn(&mut Vec<u8>, Layout);

fn put_u32(bytes: &mut [u8], at: usize, value: u32) {
    bytes[at..at + 4].copy_from_slice(&value.to_le_bytes());
}

fn put_u64(bytes: &mut [u8], at: usize, value: u64) {
    bytes[at..at + 8].copy_from_slice(&value.to_le_bytes());
}

#[test]
fn malformed_structures_are_rejected() {
    use RunCorruption::*;

    let cases: &[(&str, Mutation, RunCorruption)] = &[
        ("magic", |b, l| b[l.footer + 27] ^= 1, InvalidMagic),
        (
            "version",
            |b, l| put_u32(b, l.footer + 20, 2),
            UnsupportedVersion,
        ),
        (
            "index offset past end",
            |b, l| put_u64(b, l.footer, 1 << 40),
            InvalidFooter,
        ),
        (
            "index offset overflow",
            |b, l| put_u64(b, l.footer, u64::MAX),
            InvalidFooter,
        ),
        (
            "block count too large",
            |b, l| put_u32(b, l.footer + 16, u32::MAX),
            InvalidFooter,
        ),
        (
            "no blocks but frames",
            |b, l| put_u32(b, l.footer + 16, 0),
            InvalidFooter,
        ),
        (
            "first frame offset",
            |b, l| put_u64(b, l.table, 1),
            InvalidIndex,
        ),
        (
            "offset past frames",
            |b, l| put_u64(b, l.table + ENTRY, 1 << 30),
            InvalidIndex,
        ),
        (
            "offset overflow",
            |b, l| put_u64(b, l.table + ENTRY, u64::MAX),
            InvalidIndex,
        ),
        (
            "frame too short for header",
            |b, l| put_u64(b, l.table + ENTRY, 5),
            InvalidIndex,
        ),
        (
            "frame overlap",
            |b, l| {
                let second = u64_at(b, l.table + ENTRY);
                put_u64(b, l.table + 2 * ENTRY, second - 1);
            },
            InvalidIndex,
        ),
        (
            "frame gap",
            |b, l| {
                let second = u64_at(b, l.table + ENTRY);
                put_u64(b, l.table + ENTRY, second + 1);
            },
            InvalidFrame,
        ),
        (
            "key end past area",
            |b, l| put_u32(b, l.table + 2 * ENTRY + 8, 1 << 20),
            InvalidIndex,
        ),
        (
            "key ends decrease",
            |b, l| put_u32(b, l.table + ENTRY + 8, 1),
            InvalidIndex,
        ),
        (
            "key area has trailing bytes",
            |b, l| b.insert(l.footer, 0),
            InvalidIndex,
        ),
        ("last key differs", |b, l| b[l.keys] ^= 1, InvalidIndex),
        (
            "final key differs",
            |b, l| b[l.footer - 1] ^= 1,
            InvalidIndex,
        ),
        ("codec", |b, _| b[0] = 1, UnsupportedCodec),
        ("reserved", |b, _| b[2] = 1, InvalidFrame),
        ("decoded length", |b, _| put_u32(b, 4, 1), InvalidFrame),
        ("checksum", |b, _| put_u32(b, 8, 7), InvalidFrame),
        (
            "record count low",
            |b, l| put_u64(b, l.footer + 8, 4),
            RecordCountMismatch,
        ),
        (
            "record count high",
            |b, l| put_u64(b, l.footer + 8, 6),
            RecordCountMismatch,
        ),
    ];
    let base = three_block_bytes();
    Run::new(base.as_slice()).unwrap();
    for (name, mutate, kind) in cases {
        let mut bytes = base.clone();
        mutate(&mut bytes, layout(&base));
        let error = Run::new(bytes.as_slice()).expect_err(name);
        assert_eq!(error.kind, *kind, "{name}");
        assert_read_paths_are_safe(&bytes);
    }
}

#[test]
fn too_short_input_is_truncated() {
    for len in 0..FOOTER {
        let error = Run::new(vec![0; len]).unwrap_err();
        assert_eq!(
            error,
            CorruptRun {
                offset: 0,
                kind: RunCorruption::Truncated
            }
        );
    }
}

#[test]
fn nonempty_regions_in_an_empty_run_are_rejected() {
    let mut bytes = assemble(&[], &[], 0);
    bytes[8] = 1; // record_count
    assert_eq!(
        Run::new(bytes.as_slice()).unwrap_err().kind,
        RunCorruption::InvalidFooter
    );

    let mut bytes = vec![7];
    bytes.extend_from_slice(&assemble(&[], &[], 0));
    assert_eq!(
        Run::new(bytes.as_slice()).unwrap_err().kind,
        RunCorruption::InvalidFooter
    );
}

#[test]
fn malformed_blocks_report_their_position_in_the_run() {
    let good = raw_block(&[rec(b"a", 1, None)]);

    // A truncated block whose frame header still matches.
    let mut short = good.clone();
    short.truncate(2);
    let bytes = assemble(&[good.clone(), short], &[b"a", b"b"], 2);
    let error = Run::new(bytes.as_slice()).unwrap_err();
    assert!(
        matches!(error.kind, RunCorruption::Block(Corruption::Truncated)),
        "{error:?}"
    );
    assert_eq!(error.offset, FRAME_HEADER * 2 + good.len());

    // A record field that runs past the block.
    let mut overrun = good.clone();
    overrun[1] = 100;
    let bytes = assemble(&[overrun], &[b"a"], 1);
    let error = Run::new(bytes.as_slice()).unwrap_err();
    assert_eq!(error.kind, RunCorruption::Block(Corruption::Truncated));
    assert_read_paths_are_safe(&bytes);

    // A block with no records.
    let bytes = assemble(&[raw_block(&[])], &[b""], 0);
    assert_eq!(
        Run::new(bytes.as_slice()).unwrap_err().kind,
        RunCorruption::EmptyBlock
    );
}

type Disorder<'a> = (&'a str, Vec<u8>, Vec<u8>, &'a [u8], &'a [u8], Corruption);

#[test]
fn disorder_between_blocks_is_rejected() {
    use Corruption::{DuplicateKey, OutOfOrder};

    let one = |key: &[u8], seq| raw_block(&[rec(key, seq, None)]);
    let cases: [Disorder; 3] = [
        (
            "user key",
            one(b"b", 1),
            one(b"a", 1),
            b"b",
            b"a",
            OutOfOrder,
        ),
        (
            "sequence",
            one(b"a", 1),
            one(b"a", 2),
            b"a",
            b"a",
            OutOfOrder,
        ),
        (
            "duplicate",
            one(b"a", 1),
            one(b"a", 1),
            b"a",
            b"a",
            DuplicateKey,
        ),
    ];
    for (name, first, second, first_key, second_key, kind) in cases {
        let first_len = first.len();
        let bytes = assemble(&[first, second], &[first_key, second_key], 2);
        let error = Run::new(bytes.as_slice()).unwrap_err();
        assert_eq!(error.kind, RunCorruption::Block(kind), "{name}");
        assert_eq!(error.offset, 2 * FRAME_HEADER + first_len, "{name}");
    }

    // The order of a chain that legitimately spans two blocks is accepted.
    let bytes = assemble(&[one(b"a", 2), one(b"a", 1)], &[b"a", b"a"], 2);
    let run = Run::new(bytes.as_slice()).unwrap();
    assert_eq!(run.get(b"a").unwrap().seq, 2);
}

#[test]
fn view_reads_report_corruption_they_touch() {
    let base = three_block_bytes();
    let mut codec = base.clone();
    let l = layout(&base);
    let second = u64_at(&base, l.table + ENTRY) as usize;
    codec[second] = 9;

    let view = RunView::open(&codec).unwrap();
    assert_eq!(view.get(b"apple").unwrap().unwrap().seq, 5);
    let error = view.get(b"mango").unwrap_err();
    assert_eq!(error.kind, RunCorruption::UnsupportedCodec);
    assert_eq!(error.offset, second);
    assert_eq!(
        walk(view).unwrap_err().kind,
        RunCorruption::UnsupportedCodec
    );
}

/// `RunView` is constant time to open and only checks what it reads. This
/// states that boundary: an index that lies is invisible to it and is caught
/// by full validation.
#[test]
fn view_trusts_the_index_and_full_validation_does_not() {
    let base = three_block_bytes();
    let l = layout(&base);
    let mut lying = base.clone();
    // "mango" becomes "aango", which is below its block's real last key.
    lying[l.keys + 5] = b'a';

    let view = RunView::open(&lying).unwrap();
    assert_eq!(view.locate(b"mango"), Ok(Some(2)));
    assert_eq!(view.get(b"mango"), Ok(None), "a wrong answer, not an error");
    assert_eq!(
        Run::new(lying.as_slice()).unwrap_err().kind,
        RunCorruption::InvalidIndex
    );
}

#[test]
fn every_truncation_is_rejected() {
    let bytes = build(&sample_records(40), 64).as_bytes().to_vec();
    for len in 0..bytes.len() {
        assert!(Run::new(&bytes[..len]).is_err(), "prefix {len}");
        assert_read_paths_are_safe(&bytes[..len]);
    }
}

#[test]
fn bit_flips_never_panic_or_hang() {
    let bytes = three_block_bytes();
    for at in 0..bytes.len() {
        for bit in 0..8 {
            let mut flipped = bytes.clone();
            flipped[at] ^= 1 << bit;
            assert_read_paths_are_safe(&flipped);
        }
    }
}

#[test]
fn random_corruption_never_panics_or_hangs() {
    let base = build(&sample_records(60), 128).as_bytes().to_vec();
    for seed in 0..3000 {
        let mut rng = Rng(seed);
        let mut bytes = base.clone();
        for _ in 0..1 + rng.below(6) {
            let at = rng.below(bytes.len());
            bytes[at] = rng.next() as u8;
        }
        if rng.below(4) == 0 {
            bytes.truncate(rng.below(bytes.len() + 1));
        }
        assert_read_paths_are_safe(&bytes);
    }
}

#[test]
fn errors_format_their_offset_and_reason() {
    let error = Run::new(vec![0; FOOTER]).unwrap_err();
    assert_eq!(
        error.to_string(),
        "corrupt run at offset 0: invalid magic number"
    );
}
