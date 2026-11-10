//! Executable specification of the block format and its public API.

use std::cmp::Reverse;
use std::collections::BTreeMap;
use std::sync::Arc;

use tessera::block::{
    Block, BlockBuilder, BlockPosition, BlockView, BuildError, CorruptBlock, Corruption,
    MAX_BLOCK_LEN, MAX_KEY_LEN, MAX_VALUE_LEN, RESTART_INTERVAL, Record, Value, Version,
};

type Owned = (Vec<u8>, u64, Option<Vec<u8>>);
type Model = BTreeMap<(Vec<u8>, Reverse<u64>), Option<Vec<u8>>>;

fn value_of(value: &Option<Vec<u8>>) -> Value<'_> {
    match value {
        Some(bytes) => Value::Bytes(bytes),
        None => Value::Tombstone,
    }
}

fn build(records: &[Owned]) -> Vec<u8> {
    let mut builder = BlockBuilder::new();
    for (key, seq, value) in records {
        builder.add(key, *seq, value_of(value)).unwrap();
    }
    assert_eq!(builder.len(), records.len());
    let expected_len = builder.encoded_len();
    let bytes = builder.finish();
    assert_eq!(bytes.len(), expected_len);
    bytes
}

fn owned(key: &[u8], seq: u64, value: Option<&[u8]>) -> Owned {
    (key.to_vec(), seq, value.map(<[u8]>::to_vec))
}

/// Larger than any test block, so a traversal that stops advancing fails
/// quickly instead of exhausting memory.
const RECORD_BOUND: usize = 100_000;

fn collect<B: AsRef<[u8]>>(block: &Block<B>) -> Vec<Owned> {
    let mut cursor = block.cursor();
    let mut records = Vec::new();
    while let Some(Record { key, seq, value }) = cursor.next_record() {
        let value = match value {
            Value::Bytes(bytes) => Some(bytes.to_vec()),
            Value::Tombstone => None,
        };
        records.push((key.to_vec(), seq, value));
        assert!(records.len() < RECORD_BOUND, "cursor did not terminate");
    }
    records
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

// Hand encoders, independent of the crate, that state the format directly and
// can produce blocks the builder never would.

fn leb128(out: &mut Vec<u8>, mut value: u64) {
    while value >= 0x80 {
        out.push(value as u8 | 0x80);
        value >>= 7;
    }
    out.push(value as u8);
}

fn raw_record(shared: u64, suffix: &[u8], value_tag: u64, seq: u64, value: &[u8]) -> Vec<u8> {
    let mut out = Vec::new();
    leb128(&mut out, shared);
    leb128(&mut out, suffix.len() as u64);
    leb128(&mut out, value_tag);
    leb128(&mut out, seq);
    out.extend_from_slice(suffix);
    out.extend_from_slice(value);
    out
}

fn raw_block(entries: &[u8], restarts: &[u32]) -> Vec<u8> {
    let mut out = entries.to_vec();
    for restart in restarts {
        out.extend_from_slice(&restart.to_le_bytes());
    }
    out.extend_from_slice(&(restarts.len() as u32).to_le_bytes());
    out
}

fn corruption(bytes: &[u8]) -> Corruption {
    Block::new(bytes).unwrap_err().kind
}

#[test]
fn encoding_matches_the_documented_layout() {
    let bytes = build(&[
        owned(b"apple", 9, Some(b"red")),
        owned(b"apple", 7, None),
        owned(b"apricot", 3, Some(b"")),
    ]);
    #[rustfmt::skip]
    let expected: &[u8] = &[
        0, 5, 4, 9, b'a', b'p', b'p', b'l', b'e', b'r', b'e', b'd',
        5, 0, 0, 7,
        2, 5, 1, 3, b'r', b'i', b'c', b'o', b't',
        0, 0, 0, 0, // restart at offset 0
        1, 0, 0, 0, // one restart
    ];
    assert_eq!(bytes, expected);
}

#[test]
fn output_is_deterministic() {
    let records: Vec<Owned> = (0..100u32)
        .map(|i| owned(format!("key{i:04}").as_bytes(), 1, Some(b"v")))
        .collect();
    assert_eq!(build(&records), build(&records));
}

#[test]
fn empty_block_is_valid_and_has_no_records() {
    let builder = BlockBuilder::new();
    assert!(builder.is_empty());
    let bytes = builder.finish();
    assert_eq!(bytes, [0, 0, 0, 0]);

    let block = Block::new(bytes).unwrap();
    assert!(block.is_empty());
    assert_eq!(block.len(), 0);
    assert_eq!(block.get(b""), None);
    assert_eq!(block.get(b"anything"), None);
    assert_eq!(block.cursor().next_record(), None);
}

#[test]
fn builder_rejects_out_of_order_records_and_stays_usable() {
    let mut builder = BlockBuilder::new();
    builder.add(b"b", 5, Value::Bytes(b"x")).unwrap();
    let len = builder.encoded_len();

    let lower_key = builder.add(b"a", 5, Value::Bytes(b"x"));
    let higher_seq = builder.add(b"b", 6, Value::Bytes(b"x"));
    let duplicate = builder.add(b"b", 5, Value::Tombstone);
    assert_eq!(lower_key, Err(BuildError::OutOfOrder));
    assert_eq!(higher_seq, Err(BuildError::OutOfOrder));
    assert_eq!(duplicate, Err(BuildError::DuplicateKey));

    assert_eq!(builder.len(), 1);
    assert_eq!(builder.encoded_len(), len);
    builder.add(b"b", 4, Value::Tombstone).unwrap();
    builder.add(b"c", 9, Value::Bytes(b"y")).unwrap();

    let block = Block::new(builder.finish()).unwrap();
    assert_eq!(
        collect(&block),
        [
            owned(b"b", 5, Some(b"x")),
            owned(b"b", 4, None),
            owned(b"c", 9, Some(b"y")),
        ]
    );
}

#[test]
fn lookup_returns_the_newest_version_and_cursor_returns_all() {
    let records = [
        owned(b"k", 30, None),
        owned(b"k", 20, Some(b"old")),
        owned(b"k", 10, Some(b"older")),
    ];
    let block = Block::new(build(&records)).unwrap();
    assert_eq!(
        block.get(b"k"),
        Some(Version {
            seq: 30,
            value: Value::Tombstone
        })
    );
    assert_eq!(collect(&block), records);
}

#[test]
fn newest_version_is_found_when_versions_span_restart_intervals() {
    // "b" begins near the end of the first interval and covers several
    // restarts, so later restart keys equal the target.
    let mut records = vec![owned(b"a", 1, Some(b"before"))];
    let versions = RESTART_INTERVAL as u64 * 3;
    records.extend((0..versions).map(|i| owned(b"b", 1000 - i, Some(&i.to_le_bytes()))));
    records.push(owned(b"c", 1, Some(b"after")));

    let block = Block::new(build(&records)).unwrap();
    let newest = block.get(b"b").unwrap();
    assert_eq!(newest.seq, 1000);
    assert_eq!(newest.value, Value::Bytes(&0u64.to_le_bytes()));
    assert_eq!(block.len(), records.len());
    assert_eq!(collect(&block), records);
}

#[test]
fn restart_points_occur_every_interval_and_hold_whole_keys() {
    let interval = RESTART_INTERVAL;
    for count in [
        1,
        interval - 1,
        interval,
        interval + 1,
        2 * interval,
        2 * interval + 1,
    ] {
        let records: Vec<Owned> = (0..count)
            .map(|i| owned(format!("shared-prefix-{i:05}").as_bytes(), 1, Some(b"v")))
            .collect();
        let bytes = build(&records);

        let count_at = bytes.len() - 4;
        let restarts = u32::from_le_bytes(bytes[count_at..].try_into().unwrap()) as usize;
        assert_eq!(restarts, count.div_ceil(interval), "{count} records");

        for restart in bytes[count_at - restarts * 4..count_at].as_chunks().0 {
            let offset = u32::from_le_bytes(*restart) as usize;
            assert_eq!(bytes[offset], 0, "restart at {offset} shares a prefix");
        }

        let block = Block::new(&bytes[..]).unwrap();
        assert_eq!(collect(&block), records);
        for (key, seq, _) in &records {
            assert_eq!(block.get(key).unwrap().seq, *seq);
        }
    }
}

#[test]
fn prefix_compression_reconstructs_keys_and_saves_space() {
    let keys: [&[u8]; 7] = [b"a", b"ab", b"abc", b"abd", b"abd\0", b"b", b"ba"];
    let records: Vec<Owned> = keys.iter().map(|k| owned(k, 1, Some(b""))).collect();
    let block = Block::new(build(&records)).unwrap();
    assert_eq!(collect(&block), records);

    let long: Vec<Owned> = (0..16u8)
        .map(|i| {
            let mut key = vec![b'x'; 1000];
            key.push(i);
            (key, 1, None)
        })
        .collect();
    assert!(build(&long).len() < 1000 + 16 * 8 + 8);
}

#[test]
fn tombstones_are_distinct_from_empty_values() {
    let records = [owned(b"deleted", 2, None), owned(b"empty", 1, Some(b""))];
    let block = Block::new(build(&records)).unwrap();
    assert_eq!(block.get(b"deleted").unwrap().value, Value::Tombstone);
    assert_eq!(block.get(b"empty").unwrap().value, Value::Bytes(b""));
    assert_eq!(collect(&block), records);
}

#[test]
fn binary_keys_order_bytewise_and_include_the_empty_key() {
    let records = [
        owned(b"", 2, Some(b"empty-new")),
        owned(b"", 1, Some(b"empty-old")),
        owned(b"\x00", 1, Some(b"nul")),
        owned(b"\x00\x00", 1, Some(b"nul2")),
        owned(b"\x7f\xff", 1, Some(b"mid")),
        owned(b"\xff", 1, Some(b"high")),
        owned(b"\xff\xff", 1, None),
    ];
    let block = Block::new(build(&records)).unwrap();
    assert_eq!(collect(&block), records);
    assert_eq!(block.get(b"").unwrap().seq, 2);
    assert_eq!(block.get(b"\xff\xff").unwrap().value, Value::Tombstone);
    assert_eq!(block.get(b"\x00\x01"), None);
}

#[test]
fn lookup_misses_at_every_relative_position() {
    let records: Vec<Owned> = (0..50u32)
        .map(|i| owned(format!("key{:04}", i * 2).as_bytes(), 1, Some(b"v")))
        .collect();
    let block = Block::new(build(&records)).unwrap();

    for absent in [
        &b""[..],     // before everything
        b"a",         // before the first key
        b"key",       // proper prefix of every key
        b"key0000\0", // extension of the first key
        b"key0001",   // between keys in one interval
        b"key0033",   // between intervals
        b"key0098\0", // extension of the last key
        b"z",         // after everything
    ] {
        assert_eq!(block.get(absent), None, "{absent:?}");
    }
}

#[test]
fn extreme_sequence_numbers_round_trip() {
    let records = [
        owned(b"k", u64::MAX, Some(b"max")),
        owned(b"k", 1 << 63, Some(b"top-bit")),
        owned(b"k", 0, Some(b"zero")),
    ];
    let bytes = build(&records);
    let block = Block::new(bytes).unwrap();
    assert_eq!(block.get(b"k").unwrap().seq, u64::MAX);
    assert_eq!(collect(&block), records);
}

#[test]
fn block_works_over_any_byte_storage() {
    let bytes = build(&[owned(b"k", 1, Some(b"v"))]);
    let expected = Some(Version {
        seq: 1,
        value: Value::Bytes(b"v"),
    });

    assert_eq!(Block::new(&bytes[..]).unwrap().get(b"k"), expected);
    let shared: Arc<[u8]> = Arc::from(bytes.as_slice());
    assert_eq!(Block::new(shared).unwrap().get(b"k"), expected);
    assert_eq!(Block::new(bytes).unwrap().get(b"k"), expected);
}

#[test]
fn builder_enforces_key_and_value_limits() {
    let mut builder = BlockBuilder::new();
    let long_key = vec![b'k'; MAX_KEY_LEN + 1];
    assert_eq!(
        builder.add(&long_key, 1, Value::Tombstone),
        Err(BuildError::KeyTooLong {
            len: MAX_KEY_LEN + 1
        })
    );
    let long_value = vec![0; MAX_VALUE_LEN + 1];
    assert_eq!(
        builder.add(b"k", 1, Value::Bytes(&long_value)),
        Err(BuildError::ValueTooLong {
            len: MAX_VALUE_LEN + 1
        })
    );
    assert!(builder.is_empty());
}

#[test]
fn malformed_trailers_are_rejected() {
    assert_eq!(corruption(&[]), Corruption::Truncated);
    assert_eq!(corruption(&[0, 0, 0]), Corruption::Truncated);
    assert_eq!(corruption(&[1, 0, 0, 0]), Corruption::InvalidRestarts);
    assert_eq!(
        corruption(&[0xff, 0xff, 0xff, 0xff]),
        Corruption::InvalidRestarts
    );
    assert_eq!(
        corruption(&vec![0; MAX_BLOCK_LEN + 1]),
        Corruption::BlockTooLong
    );
}

#[test]
fn malformed_restart_directories_are_rejected() {
    let first = raw_record(0, b"a", 1, 1, b"");
    let second = raw_record(0, b"b", 1, 1, b"");
    let third = raw_record(1, b"c", 1, 1, b"");
    let second_at = first.len() as u32;
    let two = [first.clone(), second.clone()].concat();
    let shared_second = [first.clone(), third].concat();

    let cases: [(&str, Vec<u8>); 7] = [
        ("records but no restarts", raw_block(&two, &[])),
        ("restarts but no records", raw_block(&[], &[0])),
        ("first restart not at zero", raw_block(&two, &[second_at])),
        ("offset inside a record", raw_block(&two, &[0, 1])),
        ("unsorted", raw_block(&two, &[second_at, 0])),
        ("repeated", raw_block(&two, &[0, 0])),
        ("past the records", raw_block(&two, &[0, two.len() as u32])),
    ];
    for (name, bytes) in cases {
        assert_eq!(corruption(&bytes), Corruption::InvalidRestarts, "{name}");
    }

    // A restart record must carry its whole key.
    let bytes = raw_block(&shared_second, &[0, second_at]);
    let error = Block::new(&bytes[..]).unwrap_err();
    assert_eq!(error.kind, Corruption::InvalidRestarts);
    assert_eq!(error.offset, second_at as usize);
}

#[test]
fn malformed_records_are_rejected() {
    let a = raw_record(0, b"a", 1, 5, b"");

    let cases = [
        (
            "suffix past the records",
            vec![0, 9, 1, 1, b'a'],
            Corruption::Truncated,
        ),
        (
            "value past the records",
            vec![0, 1, 9, 1, b'a', b'v'],
            Corruption::Truncated,
        ),
        (
            "field cut off mid-header",
            vec![0, 1, 1],
            Corruption::Truncated,
        ),
        (
            "overlong varint",
            vec![0x80, 0x00, 1, 1, 1],
            Corruption::InvalidVarint,
        ),
        (
            "varint overflow",
            [&[0xff; 9][..], &[0x7f, 1, 1, 1]].concat(),
            Corruption::InvalidVarint,
        ),
        (
            "first record claims a shared prefix",
            raw_record(1, b"b", 1, 1, b""),
            Corruption::InvalidRestarts,
        ),
        (
            // Only the six header bytes: the limit applies before any bounds check.
            "suffix length over the key limit",
            raw_record(0, &vec![0; MAX_KEY_LEN + 1], 1, 1, b"")[..6].to_vec(),
            Corruption::KeyTooLong,
        ),
        (
            "value length over the limit",
            raw_record(0, b"a", MAX_VALUE_LEN as u64 + 2, 1, b""),
            Corruption::ValueTooLong,
        ),
    ];
    for (name, entries, expected) in cases {
        assert_eq!(corruption(&raw_block(&entries, &[0])), expected, "{name}");
    }

    let sequences = [
        (
            "shared prefix longer than previous key",
            raw_record(2, b"", 1, 1, b""),
            Corruption::InvalidPrefix,
        ),
        (
            "user keys descend",
            raw_record(0, b"\x10", 1, 1, b""),
            Corruption::OutOfOrder,
        ),
        (
            "sequence numbers ascend",
            raw_record(1, b"", 1, 6, b""),
            Corruption::OutOfOrder,
        ),
        (
            "internal key repeated",
            raw_record(1, b"", 0, 5, b""),
            Corruption::DuplicateKey,
        ),
    ];
    for (name, second, expected) in sequences {
        let entries = [a.clone(), second].concat();
        let error = Block::new(&raw_block(&entries, &[0])[..]).unwrap_err();
        assert_eq!(error.kind, expected, "{name}");
        assert_eq!(error.offset, a.len(), "{name}");
    }
}

#[test]
fn reconstructed_key_over_the_limit_is_rejected() {
    let key = vec![b'k'; MAX_KEY_LEN];
    let entries = [
        raw_record(0, &key, 1, 1, b""),
        raw_record(MAX_KEY_LEN as u64, b"x", 1, 1, b""),
    ]
    .concat();
    assert_eq!(
        corruption(&raw_block(&entries, &[0])),
        Corruption::KeyTooLong
    );
}

fn owned_record(record: Record<'_>) -> Owned {
    let value = match record.value {
        Value::Bytes(bytes) => Some(bytes.to_vec()),
        Value::Tombstone => None,
    };
    (record.key.to_vec(), record.seq, value)
}

fn walk(view: BlockView<'_>) -> Result<Vec<Owned>, CorruptBlock> {
    let mut position = BlockPosition::new();
    let mut records = Vec::new();
    while let Some(record) = position.next_record(view)? {
        records.push(owned_record(record));
        assert!(records.len() < RECORD_BOUND, "position did not terminate");
    }
    assert!(position.is_finished(view));
    Ok(records)
}

/// Any bytes `Block::new` accepts must then be fully readable, and a view must
/// be panic free and terminate whether or not the bytes are valid.
fn assert_reads_are_safe(bytes: &[u8], probes: &[&[u8]]) {
    if let Ok(block) = Block::new(bytes) {
        assert_eq!(collect(&block).len(), block.len());
        for probe in probes {
            let _ = block.get(probe);
        }
    }
    if let Ok(view) = BlockView::open(bytes) {
        for probe in probes {
            let _ = view.get(probe);
        }
        let _ = walk(view);
    }
}

#[test]
fn truncation_and_bit_flips_never_panic() {
    let records: Vec<Owned> = (0..40u32)
        .map(|i| {
            let value = (i % 3 != 0).then(|| vec![i as u8; (i % 5) as usize]);
            (
                format!("key{:03}", i / 2).into_bytes(),
                100 - u64::from(i),
                value,
            )
        })
        .collect();
    let bytes = build(&records);
    let probes: [&[u8]; 4] = [b"", b"key005", b"key005\0", b"zzz"];

    for end in 0..bytes.len() {
        assert_reads_are_safe(&bytes[..end], &probes);
    }
    for position in 0..bytes.len() {
        for mask in [0x01, 0x10, 0x80, 0xff] {
            let mut mutated = bytes.clone();
            mutated[position] ^= mask;
            assert_reads_are_safe(&mutated, &probes);
        }
    }
}

#[test]
fn views_over_arbitrary_bytes_never_panic() {
    let mut rng = Rng(7);
    let probes: [&[u8]; 4] = [b"", b"\x01", b"\x02\x03", b"\xff\xff\xff"];
    for _ in 0..4000 {
        // Small byte values parse as plausible headers, reaching deeper paths.
        let entries: Vec<u8> = (0..rng.below(120))
            .map(|_| match rng.below(4) {
                0 => rng.next() as u8,
                _ => rng.below(5) as u8,
            })
            .collect();
        let mut restarts = vec![0];
        restarts.extend((0..rng.below(5)).map(|_| rng.below(entries.len() + 3) as u32));
        assert_reads_are_safe(&raw_block(&entries, &restarts), &probes);
    }
}

#[test]
fn opening_a_view_checks_only_the_trailer() {
    let record = raw_record(0, b"a", 1, 1, b"");
    let cases: [(&[u8], Corruption); 6] = [
        (&[], Corruption::Truncated),
        (&[0, 0, 0], Corruption::Truncated),
        (&[1, 0, 0, 0], Corruption::InvalidRestarts),
        (&raw_block(&[], &[0]), Corruption::InvalidRestarts),
        (&raw_block(&record, &[]), Corruption::InvalidRestarts),
        (&raw_block(&record, &[1]), Corruption::InvalidRestarts),
    ];
    for (bytes, expected) in cases {
        assert_eq!(
            BlockView::open(bytes).unwrap_err().kind,
            expected,
            "{bytes:?}"
        );
    }
    assert_eq!(
        BlockView::open(&vec![0; MAX_BLOCK_LEN + 1])
            .unwrap_err()
            .kind,
        Corruption::BlockTooLong
    );

    // Garbage behind a consistent trailer opens, but full validation rejects it.
    let garbage = raw_block(&[0xff; 12], &[0]);
    assert!(BlockView::open(&garbage).is_ok());
    assert_eq!(corruption(&garbage), Corruption::InvalidVarint);

    assert_eq!(
        walk(BlockView::open(&[0, 0, 0, 0]).unwrap()),
        Ok(Vec::new())
    );
    assert_eq!(BlockView::open(&[0, 0, 0, 0]).unwrap().get(b"k"), Ok(None));
}

#[test]
fn view_lookups_report_malformed_bytes_they_touch() {
    let a = raw_record(0, b"a", 1, 1, b"");
    let cut_off = vec![0, 1, 9, 1, b'b'];
    let shared_restart = raw_record(1, b"c", 1, 1, b"");
    let a_len = a.len();

    let cases = [
        (
            "record in the scanned interval",
            raw_block(&[a.clone(), cut_off.clone()].concat(), &[0]),
            a_len,
            Corruption::Truncated,
        ),
        (
            "record at a probed restart",
            raw_block(&[a.clone(), cut_off].concat(), &[0, a_len as u32]),
            a_len,
            Corruption::Truncated,
        ),
        (
            "restart past the records",
            raw_block(&a, &[0, 9999]),
            9999,
            Corruption::InvalidRestarts,
        ),
        (
            "restart with a shared prefix",
            raw_block(&[a.clone(), shared_restart].concat(), &[0, a_len as u32]),
            a_len,
            Corruption::InvalidRestarts,
        ),
    ];
    for (name, bytes, offset, kind) in cases {
        let error = BlockView::open(&bytes).unwrap().get(b"zzz");
        assert_eq!(error, Err(CorruptBlock { offset, kind }), "{name}");
    }
}

#[test]
fn positions_report_malformed_records_and_stay_put() {
    let a = raw_record(0, b"a", 1, 1, b"");
    let long_key = vec![b'k'; MAX_KEY_LEN];

    let cases = [
        (
            [a.clone(), raw_record(5, b"", 1, 1, b"")].concat(),
            1,
            a.len(),
            Corruption::InvalidPrefix,
        ),
        (
            [
                raw_record(0, &long_key, 1, 2, b""),
                raw_record(MAX_KEY_LEN as u64, b"x", 1, 1, b""),
            ]
            .concat(),
            1,
            MAX_KEY_LEN + 6,
            Corruption::KeyTooLong,
        ),
        (
            raw_record(1, b"a", 1, 1, b""),
            0,
            0,
            Corruption::InvalidPrefix,
        ),
    ];
    for (entries, good_records, offset, kind) in cases {
        let bytes = raw_block(&entries, &[0]);
        let view = BlockView::open(&bytes).unwrap();
        let mut position = BlockPosition::new();
        for _ in 0..good_records {
            position.next_record(view).unwrap().unwrap();
        }
        let expected = Err(CorruptBlock { offset, kind });
        assert_eq!(position.next_record(view), expected);
        assert_eq!(position.next_record(view), expected, "position moved");
    }
}

#[test]
fn position_from_a_longer_block_is_an_error_on_a_shorter_one() {
    let long = build(&[owned(b"a", 2, Some(b"long")), owned(b"b", 1, None)]);
    let short = build(&[owned(b"a", 2, None)]);
    let mut position = BlockPosition::new();
    let long_view = BlockView::open(&long).unwrap();
    position.next_record(long_view).unwrap().unwrap();
    position.next_record(long_view).unwrap().unwrap();

    let short_view = BlockView::open(&short).unwrap();
    assert!(position.is_finished(short_view));
    assert_eq!(
        position.next_record(short_view).unwrap_err().kind,
        Corruption::Truncated
    );

    position.reset();
    assert!(!position.is_finished(short_view));
    assert_eq!(position.next_record(short_view).unwrap().unwrap().seq, 2);

    // Reset forgets the previous key, so a first record cannot reuse it.
    position.reset();
    let shares_stale_key = raw_block(&raw_record(1, b"x", 1, 1, b""), &[0]);
    assert_eq!(
        position
            .next_record(BlockView::open(&shares_stale_key).unwrap())
            .unwrap_err()
            .kind,
        Corruption::InvalidPrefix
    );
}

/// Documents the boundary of the trust contract: a view does not establish
/// validity, which is why trust must come from `Block::new` or the builder.
#[test]
fn only_full_validation_detects_disordered_keys() {
    let entries = [
        raw_record(0, b"b", 1, 1, b""),
        raw_record(0, b"a", 1, 1, b""),
    ]
    .concat();
    let bytes = raw_block(&entries, &[0]);

    assert_eq!(corruption(&bytes), Corruption::OutOfOrder);
    let view = BlockView::open(&bytes).unwrap();
    assert_eq!(view.get(b"a"), Ok(None), "stops at the first key above");
    assert_eq!(walk(view).unwrap().len(), 2);
}

/// The shape of a future run iterator: one struct owns the block buffer and
/// its traversal state, which a borrowing cursor cannot share with its block.
struct RunIterator<'f> {
    frames: std::slice::Iter<'f, Vec<u8>>,
    scratch: Vec<u8>,
    position: BlockPosition,
}

impl RunIterator<'_> {
    fn next_record(&mut self) -> Result<Option<Record<'_>>, CorruptBlock> {
        while self.scratch.is_empty() || self.position.is_finished(BlockView::open(&self.scratch)?)
        {
            let Some(frame) = self.frames.next() else {
                return Ok(None);
            };
            self.scratch.clear();
            self.scratch.extend_from_slice(frame);
            self.position.reset();
        }
        self.position.next_record(BlockView::open(&self.scratch)?)
    }
}

#[test]
fn an_iterator_can_own_a_reused_buffer_and_position_across_blocks() {
    let groups: [Vec<Owned>; 3] = [
        (0..40u32)
            .map(|i| owned(format!("a{i:03}").as_bytes(), 9, Some(b"first")))
            .collect(),
        Vec::new(),
        (0..40u32)
            .map(|i| owned(format!("b{i:03}").as_bytes(), 1, None))
            .collect(),
    ];
    let frames: Vec<Vec<u8>> = groups.iter().map(|group| build(group)).collect();
    let largest = frames.iter().map(Vec::len).max().unwrap();

    let mut iterator = RunIterator {
        frames: frames.iter(),
        scratch: Vec::with_capacity(largest),
        position: BlockPosition::new(),
    };
    let buffer = iterator.scratch.as_ptr();

    let mut seen = Vec::new();
    while let Some(record) = iterator.next_record().unwrap() {
        seen.push(owned_record(record));
        assert!(seen.len() < RECORD_BOUND, "iterator did not terminate");
    }
    assert_eq!(seen, groups.concat());
    assert_eq!(iterator.scratch.as_ptr(), buffer, "buffer was reallocated");
}

#[test]
fn a_validated_block_and_its_view_agree() {
    let records = [owned(b"a", 2, Some(b"v")), owned(b"a", 1, None)];
    let bytes = build(&records);
    let block = Block::new(&bytes[..]).unwrap();
    assert_eq!(walk(block.view()), Ok(records.to_vec()));
    assert_eq!(block.view().get(b"a"), Ok(block.get(b"a")));
}

/// Sorted, versioned records with collisions and shared prefixes.
fn random_model(rng: &mut Rng, target: usize) -> Model {
    const ALPHABET: [u8; 4] = [0x00, 0x01, b'a', 0xff];
    let mut model = BTreeMap::new();
    for _ in 0..target {
        let key: Vec<u8> = (0..rng.below(7)).map(|_| ALPHABET[rng.below(4)]).collect();
        let seq = rng.below(20) as u64 * (1 << rng.below(58));
        let value = (rng.below(4) != 0).then(|| vec![rng.next() as u8; rng.below(12)]);
        model.insert((key, Reverse(seq)), value);
    }
    model
}

fn flatten(model: &Model) -> Vec<Owned> {
    model
        .iter()
        .map(|((key, Reverse(seq)), value)| (key.clone(), *seq, value.clone()))
        .collect()
}

fn assert_matches_model(bytes: &[u8], records: &[Owned], rng: &mut Rng) {
    let block = Block::new(bytes).unwrap();
    assert_eq!(block.len(), records.len());
    assert_eq!(collect(&block), records);
    let view = BlockView::open(bytes).unwrap();
    assert_eq!(walk(view).as_deref(), Ok(records));

    let mut newest = BTreeMap::new();
    for (key, seq, value) in records.iter().rev() {
        newest.insert(key.as_slice(), (*seq, value_of(value)));
    }
    for (key, (seq, value)) in &newest {
        let expected = Some(Version {
            seq: *seq,
            value: *value,
        });
        assert_eq!(block.get(key), expected);
        assert_eq!(view.get(key), Ok(expected));
    }
    for _ in 0..200 {
        let probe: Vec<u8> = (0..rng.below(8))
            .map(|_| [0x00, 0x01, b'a', b'b', 0xff][rng.below(5)])
            .collect();
        let expected = newest
            .get(probe.as_slice())
            .map(|&(seq, value)| Version { seq, value });
        assert_eq!(block.get(&probe), expected, "probe {probe:02x?}");
        assert_eq!(view.get(&probe), Ok(expected), "view probe {probe:02x?}");
    }
}

#[test]
fn builder_output_matches_a_reference_model() {
    for seed in 0..64 {
        let mut rng = Rng(seed);
        let target = 1 + rng.below(800);
        let records = flatten(&random_model(&mut rng, target));
        assert_matches_model(&build(&records), &records, &mut rng);
    }
}

/// The reader's contract is the format, not the builder's particular choices:
/// any restart placement and any legal shared prefix must read identically.
#[test]
fn readers_accept_any_restart_spacing_and_prefix_length() {
    for seed in 0..64 {
        let mut rng = Rng(1000 + seed);
        let target = 1 + rng.below(400);
        let records = flatten(&random_model(&mut rng, target));

        let mut entries = Vec::new();
        let mut restarts = Vec::new();
        let mut previous: &[u8] = &[];
        for (index, (key, seq, value)) in records.iter().enumerate() {
            let is_restart = index == 0 || rng.below(6) == 0;
            let common = previous.iter().zip(key).take_while(|(a, b)| a == b).count();
            let shared = if is_restart { 0 } else { rng.below(common + 1) };
            if is_restart {
                restarts.push(entries.len() as u32);
            }
            let (tag, bytes) = match value {
                Some(bytes) => (bytes.len() as u64 + 1, bytes.as_slice()),
                None => (0, &[][..]),
            };
            entries.extend(raw_record(shared as u64, &key[shared..], tag, *seq, bytes));
            previous = key;
        }
        assert_matches_model(&raw_block(&entries, &restarts), &records, &mut rng);
    }
}

// Seeking within one block.

fn from_seek(view: BlockView<'_>, target: &[u8]) -> Result<Vec<Owned>, CorruptBlock> {
    let mut position = BlockPosition::new();
    position.seek(view, target)?;
    let mut records = Vec::new();
    while let Some(record) = position.next_record(view)? {
        records.push(owned_record(record));
        assert!(records.len() < RECORD_BOUND, "seek did not terminate");
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

#[test]
fn seek_stops_at_the_first_record_at_or_above_every_target() {
    // Several restart intervals, with keys that share long prefixes.
    let records: Vec<Owned> = (0..RESTART_INTERVAL * 5)
        .flat_map(|i| {
            let key = format!("key:{i:05}").into_bytes();
            [owned(&key, 2, Some(b"new")), owned(&key, 1, None)]
        })
        .collect();
    let bytes = build(&records);
    let view = BlockView::open(&bytes).unwrap();

    let mut targets: Vec<Vec<u8>> = vec![Vec::new(), b"a".to_vec(), b"z".to_vec()];
    for (key, ..) in &records {
        targets.push(key.clone());
        // A gap directly after this key and a shorter key directly before it.
        targets.push([key.as_slice(), &[0]].concat());
        targets.push(key[..key.len() - 1].to_vec());
    }
    for target in &targets {
        assert_eq!(
            from_seek(view, target).as_ref(),
            Ok(&expected_from(&records, target)),
            "target {target:02x?}"
        );
    }
}

#[test]
fn seek_finds_the_newest_version_of_a_chain_spanning_restarts() {
    let mut records = vec![owned(b"a", 1, Some(b"before"))];
    let chain = (RESTART_INTERVAL * 3 + 5) as u64;
    records.extend((1..=chain).rev().map(|seq| owned(b"m", seq, Some(b"v"))));
    records.push(owned(b"z", 1, Some(b"after")));
    let bytes = build(&records);
    let view = BlockView::open(&bytes).unwrap();

    let found = from_seek(view, b"m").unwrap();
    assert_eq!(found.first().map(|(_, seq, _)| *seq), Some(chain));
    assert_eq!(found, expected_from(&records, b"m"));
}

#[test]
fn seek_on_an_empty_block_and_past_the_end_yields_nothing() {
    let empty = build(&[]);
    let view = BlockView::open(&empty).unwrap();
    assert_eq!(from_seek(view, b"anything"), Ok(Vec::new()));

    let bytes = build(&[owned(b"a", 1, None)]);
    let view = BlockView::open(&bytes).unwrap();
    let mut position = BlockPosition::new();
    position.seek(view, b"b").unwrap();
    assert!(position.is_finished(view));
    assert_eq!(position.next_record(view), Ok(None));

    // A position that has walked elsewhere finds an empty block empty too.
    let view = BlockView::open(&empty).unwrap();
    position.seek(view, b"a").unwrap();
    assert_eq!(position.next_record(view), Ok(None));
}

#[test]
fn seek_repositions_in_any_direction_and_reuses_the_position() {
    let records: Vec<Owned> = (0..50)
        .map(|i| owned(format!("{i:03}").as_bytes(), 1, Some(b"v")))
        .collect();
    let bytes = build(&records);
    let view = BlockView::open(&bytes).unwrap();
    let mut position = BlockPosition::new();
    for target in [&b"030"[..], b"040", b"010", b"010", b"", b"049"] {
        position.seek(view, target).unwrap();
        let record = position.next_record(view).unwrap().map(owned_record);
        assert_eq!(record.as_ref(), expected_from(&records, target).first());
    }
}

#[test]
fn seek_reports_malformed_bytes_it_touches_and_resets() {
    // A restart past the record region.
    let entries = raw_record(0, b"a", 1, 1, b"");
    let bytes = raw_block(&entries, &[0, 4096]);
    let view = BlockView::open(&bytes).unwrap();
    let mut position = BlockPosition::new();
    let error = position.seek(view, b"zzz").unwrap_err();
    assert_eq!(error.kind, Corruption::InvalidRestarts);
    let first = position.next_record(view).unwrap().unwrap();
    assert_eq!(first.key, b"a");

    // A record in the walked interval that shares more than its predecessor.
    let mut entries = raw_record(0, b"a", 1, 2, b"");
    entries.extend(raw_record(9, b"b", 1, 1, b""));
    let bytes = raw_block(&entries, &[0]);
    let view = BlockView::open(&bytes).unwrap();
    let error = position.seek(view, b"zzz").unwrap_err();
    assert_eq!(error.kind, Corruption::InvalidPrefix);
    assert_eq!(position.next_record(view).unwrap().unwrap().key, b"a");
}

#[test]
fn seek_matches_a_reference_model() {
    for seed in 0..64 {
        let mut rng = Rng(2000 + seed);
        let target = 1 + rng.below(600);
        let records = flatten(&random_model(&mut rng, target));
        let bytes = build(&records);
        let view = BlockView::open(&bytes).unwrap();
        for _ in 0..100 {
            let probe: Vec<u8> = (0..rng.below(8))
                .map(|_| [0x00, 0x01, b'a', b'b', 0xff][rng.below(5)])
                .collect();
            assert_eq!(
                from_seek(view, &probe).as_ref(),
                Ok(&expected_from(&records, &probe)),
                "seed {seed} probe {probe:02x?}"
            );
        }
    }
}
