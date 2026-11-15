//! Executable specification of the run format and its readers.
//!
//! Logical behavior is checked once per codec by looping over `CODECS`. Every
//! loop that retains data has an explicit bound, so a broken reader fails an
//! assertion instead of exhausting memory. Bytes are assembled by hand, using
//! the codec libraries directly, where a test must not depend on the crate's
//! own encoder or decoder.

use std::cmp::Reverse;
use std::collections::BTreeMap;
use std::sync::Arc;

use crate::block::{Block, BuildError, Corruption, MAX_BLOCK_LEN, Record, Value};
use crate::run::{
    Codec, CorruptRun, ReadWorkspace, Run, RunBuilder, RunCorruption, RunView, TARGET_BLOCK_LEN,
};

type Owned = (Vec<u8>, u64, Option<Vec<u8>>);
type Newest = (u64, Option<Vec<u8>>);
type Model = BTreeMap<(Vec<u8>, Reverse<u64>), Option<Vec<u8>>>;

const CODECS: [Codec; 2] = [Codec::None, Codec::Zstd];
const COMPRESSED: [Codec; 1] = [Codec::Zstd];
const RECORD_BOUND: usize = 200_000;
/// Bounds retained bytes too, since values can be megabytes long.
const RETAINED_BOUND: usize = 512 << 20;
const HEADER: usize = 12;
const ENTRY: usize = 12;
const FOOTER: usize = 48;

fn rec(key: &[u8], seq: u64, value: Option<&[u8]>) -> Owned {
    (key.to_vec(), seq, value.map(<[u8]>::to_vec))
}

fn as_value(value: &Option<Vec<u8>>) -> Value<'_> {
    value.as_deref().map_or(Value::Tombstone, Value::Bytes)
}

fn owned_value(value: Value<'_>) -> Option<Vec<u8>> {
    match value {
        Value::Bytes(bytes) => Some(bytes.to_vec()),
        Value::Tombstone => None,
    }
}

fn value_len(value: Value<'_>) -> usize {
    match value {
        Value::Bytes(bytes) => bytes.len(),
        Value::Tombstone => 0,
    }
}

fn owned(record: Record<'_>) -> Owned {
    (record.key.to_vec(), record.seq, owned_value(record.value))
}

fn build(codec: Codec, records: &[Owned], target: usize) -> Run<Vec<u8>> {
    let mut builder = RunBuilder::with_raw_block_target(codec, target);
    for (key, seq, value) in records {
        builder.add(key, *seq, as_value(value)).unwrap();
    }
    assert_eq!(builder.len(), records.len());
    builder.finish()
}

fn walk(view: RunView<'_>) -> Result<Vec<Owned>, CorruptRun> {
    let mut workspace = ReadWorkspace::new();
    let mut cursor = view.cursor(&mut workspace);
    let mut records = Vec::new();
    let mut retained = 0;
    while let Some(record) = cursor.next_record()? {
        retained += record.key.len() + value_len(record.value);
        records.push(owned(record));
        assert!(
            records.len() <= RECORD_BOUND && retained <= RETAINED_BOUND,
            "cursor did not terminate"
        );
    }
    Ok(records)
}

fn all<B: AsRef<[u8]>>(run: &Run<B>) -> Vec<Owned> {
    walk(run.view()).unwrap()
}

fn newest<B: AsRef<[u8]>>(run: &Run<B>, key: &[u8]) -> Option<Newest> {
    let mut workspace = ReadWorkspace::new();
    run.get(key, &mut workspace)
        .unwrap()
        .map(|found| (found.seq, owned_value(found.value)))
}

fn seq_of<B: AsRef<[u8]>>(run: &Run<B>, key: &[u8]) -> u64 {
    newest(run, key).expect("key must be present").0
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

fn put_u32(bytes: &mut [u8], at: usize, value: u32) {
    bytes[at..at + 4].copy_from_slice(&value.to_le_bytes());
}

fn put_u64(bytes: &mut [u8], at: usize, value: u64) {
    bytes[at..at + 8].copy_from_slice(&value.to_le_bytes());
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

fn frame_ranges(bytes: &[u8]) -> Vec<(usize, usize)> {
    let l = layout(bytes);
    (0..l.block_count)
        .map(|i| {
            let start = u64_at(bytes, l.table + i * ENTRY) as usize;
            let end = if i + 1 < l.block_count {
                u64_at(bytes, l.table + (i + 1) * ENTRY) as usize
            } else {
                u64_at(bytes, l.footer + 24) as usize
            };
            (start, end)
        })
        .collect()
}

fn checksum(header: &[u8], stored: &[u8]) -> u32 {
    let mut hasher = crc32fast::Hasher::new();
    hasher.update(&header[..8]);
    hasher.update(stored);
    hasher.finalize()
}

/// Recomputes every frame checksum, so that a mutation reaches the checks
/// behind the checksum instead of stopping at it.
fn reseal(bytes: &mut [u8]) {
    for (start, end) in frame_ranges(bytes) {
        let sum = checksum(&bytes[start..start + HEADER], &bytes[start + HEADER..end]);
        put_u32(bytes, start + 8, sum);
    }
}

fn codec_byte(codec: Codec) -> u8 {
    match codec {
        Codec::None => 0,
        Codec::Zstd => 1,
    }
}
fn compress(codec: Codec, block: &[u8]) -> Vec<u8> {
    if codec == Codec::None {
        return block.to_vec();
    }
    let mut out = Vec::with_capacity(zstd_safe::compress_bound(block.len()));
    zstd_safe::compress(&mut out, block, 3).unwrap();
    out
}
fn decode_frame(frame: &[u8], dictionary: &[u8]) -> Vec<u8> {
    let stored = &frame[HEADER..];
    let mut out = vec![0; u32_at(frame, 4) as usize];
    match frame[0] {
        0 => out.copy_from_slice(stored),
        1 => {
            zstd_safe::DCtx::create()
                .decompress_using_dict(&mut out[..], stored, dictionary)
                .unwrap();
        }
        other => panic!("codec {other}"),
    }
    out
}
fn dictionary_of<'a>(bytes: &'a [u8], frame: &[u8]) -> &'a [u8] {
    if frame[0] == 0 {
        return &[];
    }
    let footer = bytes.len() - FOOTER;
    let segments = u64_at(bytes, footer) as usize - u32_at(bytes, footer + 32) as usize * 16;
    let reference = u32::from_le_bytes([frame[1], frame[2], frame[3], 0]) as usize;
    let entry = segments + (reference - 1) * 16;
    let start = u64_at(bytes, entry) as usize;
    &bytes[start..start + u32_at(bytes, entry + 8) as usize]
}

/// Returns each decoded block's bytes.
fn decoded_blocks(bytes: &[u8]) -> Vec<Vec<u8>> {
    frame_ranges(bytes)
        .into_iter()
        .map(|(start, end)| {
            decode_frame(&bytes[start..end], dictionary_of(bytes, &bytes[start..end]))
        })
        .collect()
}

/// Returns every block's records.
fn blocks<B: AsRef<[u8]>>(run: &Run<B>) -> Vec<Vec<Owned>> {
    decoded_blocks(run.as_bytes())
        .iter()
        .map(|payload| {
            let block = Block::new(payload.as_slice()).unwrap();
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
    block.extend_from_slice(&0u32.to_le_bytes());
    if !records.is_empty() {
        block.extend_from_slice(&1u32.to_le_bytes());
    }
    block
}

/// Assembles a run by hand from `(codec byte, decoded length, stored bytes)`
/// frames and index keys, with correct checksums.
///
/// The minimum key is taken to be the first block's last key, which suits
/// frames whose first block has a single user key or that are not decodable.
fn assemble_frames(
    frames: &[(u8, u32, Vec<u8>)],
    last_keys: &[&[u8]],
    record_count: u64,
) -> Vec<u8> {
    let min_key = last_keys.first().copied().unwrap_or(b"");
    assemble_frames_with_min(frames, min_key, last_keys, record_count)
}

/// Reads the first user key of an encoded block. A first record shares no
/// prefix, so this needs only its leading varints.
fn first_key_of(block: &[u8]) -> Option<&[u8]> {
    fn varint(rest: &mut &[u8]) -> Option<usize> {
        let (mut value, mut shift) = (0, 0);
        loop {
            let (&byte, tail) = rest.split_first()?;
            *rest = tail;
            value |= usize::from(byte & 0x7F).checked_shl(shift)?;
            if byte & 0x80 == 0 {
                return Some(value);
            }
            shift += 7;
        }
    }
    let mut rest = block;
    varint(&mut rest)?;
    let suffix_len = varint(&mut rest)?;
    varint(&mut rest)?;
    varint(&mut rest)?;
    rest.get(..suffix_len)
}

fn assemble_frames_with_min(
    frames: &[(u8, u32, Vec<u8>)],
    min_key: &[u8],
    last_keys: &[&[u8]],
    record_count: u64,
) -> Vec<u8> {
    let mut run = Vec::new();
    let mut table = Vec::new();
    let mut keys = min_key.to_vec();
    for ((codec, decoded_len, stored), key) in frames.iter().zip(last_keys) {
        table.extend_from_slice(&(run.len() as u64).to_le_bytes());
        let mut header = vec![*codec, u8::from(*codec == 1), 0, 0];
        header.extend_from_slice(&decoded_len.to_le_bytes());
        let sum = checksum(&header, stored);
        run.extend_from_slice(&header);
        run.extend_from_slice(&sum.to_le_bytes());
        run.extend_from_slice(stored);
        keys.extend_from_slice(key);
        table.extend_from_slice(&(keys.len() as u32).to_le_bytes());
    }
    let dictionaries_offset = run.len() as u64;
    let has_dictionary = frames.iter().any(|f| f.0 == 1);
    if has_dictionary {
        run.push(0);
        run.extend_from_slice(&dictionaries_offset.to_le_bytes());
        run.extend_from_slice(&1u32.to_le_bytes());
        run.extend_from_slice(&crc32fast::hash(&[0]).to_le_bytes());
    }
    let index_offset = run.len() as u64;
    run.extend_from_slice(&table);
    run.extend_from_slice(&keys);
    run.extend_from_slice(&index_offset.to_le_bytes());
    run.extend_from_slice(&record_count.to_le_bytes());
    run.extend_from_slice(&(frames.len() as u32).to_le_bytes());
    run.extend_from_slice(&(min_key.len() as u32).to_le_bytes());
    run.extend_from_slice(&dictionaries_offset.to_le_bytes());
    run.extend_from_slice(&u32::from(has_dictionary).to_le_bytes());
    run.extend_from_slice(&0u32.to_le_bytes());
    run.extend_from_slice(&5u32.to_le_bytes());
    run.extend_from_slice(b"TSRA");
    run
}

/// Assembles a run whose blocks are all compressed with `codec`.
fn assemble(codec: Codec, blocks: &[Vec<u8>], last_keys: &[&[u8]], record_count: u64) -> Vec<u8> {
    let frames: Vec<_> = blocks
        .iter()
        .map(|block| {
            (
                codec_byte(codec),
                block.len() as u32,
                compress(codec, block),
            )
        })
        .collect();
    // A first key above the index key would be refused on open. Clamping it
    // lets tests of malformed blocks reach the block checks.
    let last = last_keys.first().copied();
    let first = blocks.first().and_then(|block| first_key_of(block));
    let min_key = first
        .filter(|&key| Some(key) <= last)
        .or(last)
        .unwrap_or(b"");
    assemble_frames_with_min(&frames, min_key, last_keys, record_count)
}

fn three_blocks() -> Vec<Vec<u8>> {
    vec![
        raw_block(&[rec(b"apple", 5, Some(b"one")), rec(b"apple", 4, None)]),
        raw_block(&[rec(b"mango", 9, Some(b"two"))]),
        raw_block(&[rec(b"pear", 3, Some(b"")), rec(b"plum", 8, Some(b"three"))]),
    ]
}

fn three_block_bytes(codec: Codec) -> Vec<u8> {
    assemble(codec, &three_blocks(), &[b"apple", b"mango", b"plum"], 5)
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

// Format.

#[test]
fn empty_run_is_a_footer_alone() {
    for codec in CODECS {
        let run = RunBuilder::new(codec).finish();
        assert_eq!(run.as_bytes().len(), FOOTER);
        assert!(run.is_empty());
        assert_eq!(run.block_count(), 0);
        assert_eq!(newest(&run, b"anything"), None);
        assert_eq!(run.view().locate(b"anything"), Ok(None));
        assert_eq!(all(&run), vec![]);
        assert!(Run::new(run.as_bytes()).unwrap().is_empty());
    }
}

#[test]
fn uncompressed_format_is_exactly_the_documented_bytes() {
    let run = build(
        Codec::None,
        &[rec(b"a", 2, Some(b"x")), rec(b"b", 1, None)],
        1, // Each record becomes its own block.
    );

    let block0: &[u8] = &[0, 1, 2, 2, b'a', b'x', 0, 0, 0, 0, 1, 0, 0, 0];
    let block1: &[u8] = &[0, 1, 0, 1, b'b', 0, 0, 0, 0, 1, 0, 0, 0];
    let header0 = [0, 0, 0, 0, 14, 0, 0, 0];
    let header1 = [0, 0, 0, 0, 13, 0, 0, 0];

    #[rustfmt::skip]
    let expected: Vec<u8> = [
        // frame: codec, reserved, decoded_len, checksum, stored
        &header0[..],
        &checksum(&header0, block0).to_le_bytes(),
        block0,
        &header1,
        &checksum(&header1, block1).to_le_bytes(),
        block1,
        // table: frame_offset, key_end
        &[0, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0],
        &[26, 0, 0, 0, 0, 0, 0, 0, 3, 0, 0, 0],
        // key area: the minimum key, then each block's last key
        b"aab",
        // footer: index_offset, record_count, block_count, min_key_len, version, magic
        &[51, 0, 0, 0, 0, 0, 0, 0],
        &[2, 0, 0, 0, 0, 0, 0, 0],
        &[2, 0, 0, 0],
        &[1, 0, 0, 0],
        &[51, 0, 0, 0, 0, 0, 0, 0],
        &[0; 8],
        &[5, 0, 0, 0],
        b"TSRA",
    ]
    .concat();
    assert_eq!(run.as_bytes(), expected);
}

#[test]
fn compressed_frames_match_independent_codec_output() {
    let records = sample_records(200);
    let run = build(Codec::Zstd, &records, 512);
    for (start, end) in frame_ranges(run.as_bytes()) {
        let frame = &run.as_bytes()[start..end];
        let dictionary = dictionary_of(run.as_bytes(), frame);
        let decoded = decode_frame(frame, dictionary);
        let mut expected = Vec::with_capacity(zstd_safe::compress_bound(decoded.len()));
        zstd_safe::CCtx::create()
            .compress_using_cdict(
                &mut expected,
                &decoded,
                &zstd_safe::CDict::create(dictionary, 3),
            )
            .unwrap();
        assert_eq!(&frame[HEADER..], expected);
    }
}

#[test]
fn hand_assembled_runs_are_accepted_and_read() {
    for codec in CODECS {
        let bytes = three_block_bytes(codec);
        let run = Run::new(bytes.as_slice()).unwrap();
        assert_eq!(run.len(), 5);
        assert_eq!(run.block_count(), 3);
        assert_eq!(newest(&run, b"apple"), Some((5, Some(b"one".to_vec()))));
        assert_eq!(newest(&run, b"mango"), Some((9, Some(b"two".to_vec()))));
        assert_eq!(newest(&run, b"pear"), Some((3, Some(Vec::new()))));
        assert_eq!(seq_of(&run, b"plum"), 8);
        assert_eq!(newest(&run, b"melon"), None);
        assert_eq!(
            all(&run),
            vec![
                rec(b"apple", 5, Some(b"one")),
                rec(b"apple", 4, None),
                rec(b"mango", 9, Some(b"two")),
                rec(b"pear", 3, Some(b"")),
                rec(b"plum", 8, Some(b"three")),
            ]
        );
    }
}

#[test]
fn block_boundaries_do_not_depend_on_the_codec() {
    let records = sample_records(1500);
    let expected = blocks(&build(Codec::None, &records, 512));
    for codec in COMPRESSED {
        assert_eq!(blocks(&build(codec, &records, 512)), expected);
    }
}

/// Very high compression ratios remain valid, including oversized records.
#[test]
fn highly_repetitive_data_round_trips_at_extreme_ratios() {
    let value_len = 8 << 20;
    let values = [vec![0; value_len], b"abcd".repeat(value_len / 4)];
    for value in &values {
        let records = vec![rec(b"a", 2, Some(value)), rec(b"b", 1, Some(value))];
        {
            let (codec, least_ratio) = (Codec::Zstd, 1000);
            let run = build(codec, &records, TARGET_BLOCK_LEN);
            let stored: usize = frame_ranges(run.as_bytes())
                .iter()
                .map(|(a, b)| b - a - HEADER)
                .sum();
            let ratio = 2 * value_len / stored;
            assert!(ratio >= least_ratio, "{codec:?} ratio {ratio}");
            let validated = Run::new(run.as_bytes()).unwrap();
            assert_eq!(all(&validated), records);
        }
    }
}

#[test]
fn compression_shrinks_compressible_blocks() {
    let records: Vec<Owned> = (0..2000)
        .map(|i| rec(format!("key{i:06}").as_bytes(), 1, Some(&[b'x'; 200])))
        .collect();
    let plain = build(Codec::None, &records, TARGET_BLOCK_LEN)
        .as_bytes()
        .len();
    for codec in COMPRESSED {
        let compressed = build(codec, &records, TARGET_BLOCK_LEN).as_bytes().len();
        assert!(compressed * 4 < plain, "{codec:?}: {compressed} of {plain}");
    }
}

#[test]
fn construction_is_deterministic() {
    let records = sample_records(500);
    for codec in CODECS {
        assert_eq!(
            build(codec, &records, 256).as_bytes(),
            build(codec, &records, 256).as_bytes()
        );
    }
}

// Logical behavior, once per codec.

#[test]
fn single_block_run_round_trips() {
    let records = sample_records(50);
    for codec in CODECS {
        let run = build(codec, &records, TARGET_BLOCK_LEN);
        assert_eq!(run.block_count(), 1);
        assert_eq!(run.len(), 50);
        assert_eq!(all(&run), records);
        for (key, seq, _) in &records {
            assert_eq!(seq_of(&run, key), *seq);
        }
    }
}

#[test]
fn multiple_blocks_preserve_order_and_every_key() {
    let records = sample_records(2000);
    for codec in CODECS {
        let run = build(codec, &records, 512);
        assert!(run.block_count() > 10);
        assert_eq!(all(&run), records);
        let mut workspace = ReadWorkspace::new();
        for (key, seq, value) in &records {
            let found = run.get(key, &mut workspace).unwrap().unwrap();
            assert_eq!((found.seq, found.value), (*seq, as_value(value)));
        }
        // The records either side of every block boundary.
        for pair in blocks(&run).windows(2) {
            let last = pair[0].last().unwrap();
            let first = pair[1].first().unwrap();
            assert_eq!(seq_of(&run, &last.0), last.1);
            assert_eq!(seq_of(&run, &first.0), first.1);
        }
    }
}

#[test]
fn locate_selects_the_only_candidate_block() {
    let records = sample_records(1000);
    let run = build(Codec::Zstd, &records, 512);
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
    for codec in CODECS {
        for target in [1, 8, 32, 4096] {
            let run = build(codec, &records, target);
            assert_eq!(newest(&run, b"a"), Some((9, Some(b"a9".to_vec()))));
            // A newest version that is a tombstone is returned, not skipped.
            assert_eq!(newest(&run, b"b"), Some((7, None)));
            assert_eq!(all(&run), records, "target {target}");
        }
    }
}

#[test]
fn tombstone_and_empty_value_are_distinct() {
    let records = vec![rec(b"empty", 2, Some(b"")), rec(b"gone", 1, None)];
    for codec in CODECS {
        let run = build(codec, &records, 1);
        assert_eq!(newest(&run, b"empty"), Some((2, Some(Vec::new()))));
        assert_eq!(newest(&run, b"gone"), Some((1, None)));
    }
}

#[test]
fn misses_are_reported_everywhere_in_the_key_space() {
    let records = vec![
        rec(b"b", 1, Some(b"")),
        rec(b"d", 1, Some(b"")),
        rec(b"dd", 1, Some(b"")),
        rec(b"f", 1, Some(b"")),
    ];
    for codec in CODECS {
        let run = build(codec, &records, 1);
        assert_eq!(run.block_count(), 4);
        for absent in [&b""[..], b"a", b"c", b"d\0", b"de", b"e", b"f\0", b"g"] {
            assert_eq!(newest(&run, absent), None, "{absent:?}");
        }
    }
}

#[test]
fn empty_user_key_is_supported() {
    let records = vec![
        rec(b"", 2, Some(b"v")),
        rec(b"", 1, None),
        rec(b"a", 1, None),
    ];
    for codec in CODECS {
        let run = build(codec, &records, 1);
        assert_eq!(seq_of(&run, b""), 2);
        Run::new(run.as_bytes()).unwrap();
        assert_eq!(all(&run), records);
    }
}

#[test]
fn blocks_land_near_the_target() {
    let records: Vec<Owned> = (0..4000)
        .map(|i| rec(format!("key{i:06}").as_bytes(), 1, Some(&[7; 40])))
        .collect();
    let target = 1024;
    for codec in CODECS {
        let run = build(codec, &records, target);
        let payloads = decoded_blocks(run.as_bytes());
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
        let run = build(Codec::None, &records, 1024);
        let sizes: Vec<usize> = blocks(&run).iter().map(Vec::len).collect();
        assert!(sizes.iter().all(|&n| n == per_block), "{sizes:?}");
        assert_eq!(all(&run), records);
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
    for codec in CODECS {
        let run = build(codec, &records, TARGET_BLOCK_LEN);
        let by_block = blocks(&run);
        assert_eq!(by_block.len(), 3);
        assert_eq!(by_block[1], vec![records[1].clone()]);
        assert_eq!(newest(&run, b"b"), Some((1, Some(large.clone()))));
        Run::new(run.as_bytes()).unwrap();
        assert_eq!(all(&run), records);
    }
}

#[test]
fn consecutive_oversized_records_each_get_a_block() {
    let large = vec![1; 3000];
    let records: Vec<Owned> = (0..5)
        .map(|i| rec(format!("k{i}").as_bytes(), 1, Some(&large)))
        .collect();
    for codec in CODECS {
        let run = build(codec, &records, 1024);
        assert_eq!(run.block_count(), 5);
        assert_eq!(all(&run), records);
    }
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
    for codec in CODECS {
        let run = build(codec, &records, 1024);
        let by_block = blocks(&run);
        assert_eq!(by_block[0], vec![records[0].clone()]);
        assert_eq!(by_block[1][0], records[1]);
        assert_eq!(seq_of(&run, b"chain"), 9);
        Run::new(run.as_bytes()).unwrap();
        assert_eq!(all(&run), records);
    }
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
    let run = build(Codec::Zstd, &records, 1024);
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
    for codec in CODECS {
        let run = build(codec, &records, target);
        let payloads = decoded_blocks(run.as_bytes());
        assert!(payloads.len() > 5, "chain must span blocks");
        for payload in &payloads {
            assert!(payload.len() <= 2 * target + 64, "{} bytes", payload.len());
        }

        // The newest version is found even though older ones fill later blocks.
        assert_eq!(seq_of(&run, b"long"), 10_000);
        assert_eq!(seq_of(&run, b"before"), 1);
        assert_eq!(seq_of(&run, b"tail"), 1);
        assert_eq!(run.view().locate(b"long"), Ok(Some(0)));
        Run::new(run.as_bytes()).unwrap();
        assert_eq!(all(&run), records);
    }
}

#[test]
fn chain_boundary_blocks_keep_descending_sequence_numbers() {
    let records: Vec<Owned> = (0..300u64)
        .map(|i| rec(b"k", 1000 - i, Some(&[1; 8])))
        .collect();
    for codec in CODECS {
        let run = build(codec, &records, 1);
        assert_eq!(run.block_count(), 300);
        assert_eq!(all(&run), records);
    }
}

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

/// One workspace serves every probe of every run, so stale data left by a
/// differently sized block would be observed here.
#[test]
fn iteration_and_lookup_match_a_reference_model() {
    let mut workspace = ReadWorkspace::new();
    for seed in 0..48u64 {
        let mut rng = Rng(seed);
        let keys = 20 + rng.below(300);
        let records = flatten(&random_model(&mut rng, keys));
        let target = [1, 16, 64, 256, 1024, 4096, TARGET_BLOCK_LEN][seed as usize % 7];
        let mut newest_model = BTreeMap::new();
        for record in records.iter().rev() {
            newest_model.insert(record.0.clone(), record);
        }

        for codec in CODECS {
            let run = build(codec, &records, target);
            assert_eq!(all(&run), records, "seed {seed}");
            let validated = Run::new(run.as_bytes().to_vec()).unwrap();
            assert_eq!(validated.len(), records.len());

            for (key, (_, seq, value)) in &newest_model {
                let found = validated.get(key, &mut workspace).unwrap().unwrap();
                assert_eq!(
                    (found.seq, found.value),
                    (*seq, as_value(value)),
                    "seed {seed}"
                );
            }
            let mut probes = rng;
            for _ in 0..200 {
                let len = probes.below(14);
                let probe: Vec<u8> = (0..len).map(|_| b'a' + probes.below(4) as u8).collect();
                let expected = newest_model.get(&probe).map(|record| record.1);
                let found = run.get(&probe, &mut workspace).unwrap();
                assert_eq!(found.map(|v| v.seq), expected, "seed {seed}");
            }
        }
    }
}

// Builder contract.

#[test]
fn builder_rejects_invalid_ordering() {
    let mut builder = RunBuilder::new(Codec::None);
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
    for codec in CODECS {
        // A target of 1 cuts on every record.
        let mut builder = RunBuilder::with_raw_block_target(codec, 1);
        builder.add(b"k", 5, Value::Bytes(b"xx")).unwrap();
        builder.add(b"l", 5, Value::Bytes(b"xx")).unwrap();
        for (key, seq, error) in [
            (&b"k"[..], 9, BuildError::OutOfOrder),
            (b"l", 5, BuildError::DuplicateKey),
            (b"l", 6, BuildError::OutOfOrder),
        ] {
            assert_eq!(builder.add(key, seq, Value::Bytes(b"xx")), Err(error));
        }
    }
}

#[test]
fn limit_rejections_are_reported() {
    let mut builder = RunBuilder::new(Codec::Zstd);
    let key = vec![b'k'; crate::block::MAX_KEY_LEN + 1];
    assert_eq!(
        builder.add(&key, 1, Value::Tombstone),
        Err(BuildError::KeyTooLong { len: key.len() })
    );
    assert!(builder.is_empty());
}

#[test]
fn rejected_records_do_not_change_the_layout() {
    let records = sample_records(300);
    for codec in CODECS {
        let clean = build(codec, &records, 128);
        let mut builder = RunBuilder::with_raw_block_target(codec, 128);
        for (key, seq, value) in &records {
            // A large out-of-order record would force a cut if it were not
            // rejected first.
            assert!(builder.is_empty() || builder.add(b"", 0, Value::Bytes(&[0; 4096])).is_err());
            builder.add(key, *seq, as_value(value)).unwrap();
        }
        assert_eq!(builder.finish().as_bytes(), clean.as_bytes());
    }
}

#[test]
fn finished_run_is_accepted_by_full_validation() {
    for codec in CODECS {
        for count in [0, 1, 2, 17, 500] {
            for target in [1, 64, 1024] {
                let run = build(codec, &sample_records(count), target);
                let reparsed = Run::new(run.as_bytes()).unwrap();
                assert_eq!(reparsed.len(), count);
                assert_eq!(reparsed.block_count(), run.block_count());
            }
        }
    }
}

// Workspace ownership and storage.

#[test]
fn runs_work_over_shared_and_borrowed_storage() {
    let records = sample_records(100);
    for codec in CODECS {
        let bytes = build(codec, &records, 128).as_bytes().to_vec();
        let shared: Arc<[u8]> = bytes.as_slice().into();
        let run = Run::new(shared).unwrap();
        assert_eq!(all(&run), records);
        assert_eq!(Run::new(bytes.as_slice()).unwrap().len(), 100);
    }
}

/// Beside its storage a run holds two pointers, to the key filter its
/// builder made, its largest
/// sequence number, and the four offsets of its smallest and largest keys
/// with a word saying whether it has any.
#[test]
fn runtime_descriptor_owns_its_storage_and_an_optional_filter() {
    let extra = size_of::<usize>() + size_of::<u64>() + 5 * size_of::<usize>();
    assert_eq!(size_of::<Run<Vec<u8>>>(), size_of::<Vec<u8>>() + extra);
    assert_eq!(size_of::<Run<&[u8]>>(), size_of::<&[u8]>() + extra);
}

#[test]
fn uncompressed_reads_borrow_the_run_and_never_use_the_workspace() {
    let run = build(Codec::None, &sample_records(200), 256);
    let mut workspace = ReadWorkspace::new();
    let range = run.as_bytes().as_ptr_range();
    let found = run.get(b"key00150", &mut workspace).unwrap().unwrap();
    let Value::Bytes(value) = found.value else {
        panic!("expected a value");
    };
    assert!(range.contains(&value.as_ptr()));
    assert_eq!(workspace.capacity(), 0);

    let mut cursor = run.cursor(&mut workspace);
    let mut steps = 0;
    while cursor.next_record().unwrap().is_some() {
        steps += 1;
        assert!(steps <= 200);
    }
    assert_eq!(workspace.capacity(), 0);
}

#[test]
fn compressed_reads_borrow_the_workspace_at_exactly_the_block_size() {
    for codec in COMPRESSED {
        let run = build(codec, &sample_records(200), 256);
        let largest = decoded_blocks(run.as_bytes())
            .iter()
            .map(Vec::len)
            .max()
            .unwrap();
        let mut workspace = ReadWorkspace::new();
        let range = run.as_bytes().as_ptr_range();
        {
            let found = run.get(b"key00150", &mut workspace).unwrap().unwrap();
            let Value::Bytes(value) = found.value else {
                panic!("expected a value");
            };
            assert!(!range.contains(&value.as_ptr()));
        }
        // Touch every block, then the capacity must stop changing.
        for (key, ..) in sample_records(200) {
            run.get(&key, &mut workspace).unwrap().unwrap();
        }
        let warm = workspace.capacity();
        assert_eq!(warm, largest, "{codec:?}");
        for (key, ..) in sample_records(200) {
            run.get(&key, &mut workspace).unwrap().unwrap();
        }
        assert_eq!(workspace.capacity(), warm);
    }
}

#[test]
fn one_workspace_serves_runs_of_every_codec() {
    let small = sample_records(50);
    let large: Vec<Owned> = (0..50)
        .map(|i| rec(format!("key{i:05}").as_bytes(), 7, Some(&[i as u8; 2000])))
        .collect();
    let runs = [
        build(Codec::Zstd, &large, 8192),
        build(Codec::Zstd, &small, 128),
        build(Codec::None, &small, 128),
        build(Codec::Zstd, &large, 8192),
        build(Codec::Zstd, &small, 128),
    ];
    let mut workspace = ReadWorkspace::new();
    for round in 0..3 {
        for run in &runs {
            for i in 0..50 {
                let key = format!("key{i:05}");
                let expected = newest(run, key.as_bytes());
                let found = run.get(key.as_bytes(), &mut workspace).unwrap().unwrap();
                let got = Some((found.seq, owned_value(found.value)));
                assert_eq!(got, expected, "round {round}");
            }
        }
    }
}

#[test]
fn a_cursor_and_a_lookup_use_separate_workspaces() {
    let records = sample_records(300);
    for codec in CODECS {
        let run = build(codec, &records, 128);
        let (mut scan, mut lookup) = (ReadWorkspace::new(), ReadWorkspace::new());
        let mut cursor = run.cursor(&mut scan);
        let last = &records[records.len() - 1];
        for (key, seq, value) in &records {
            let record = cursor.next_record().unwrap().unwrap();
            assert_eq!(owned(record), (key.clone(), *seq, value.clone()));
            let found = run.get(&last.0, &mut lookup).unwrap().unwrap();
            assert_eq!(found.seq, last.1);
        }
        assert_eq!(cursor.next_record().unwrap(), None);
        assert_eq!(cursor.next_record().unwrap(), None);
    }
}

#[test]
fn run_supports_concurrent_readers() {
    fn assert_shareable<T: Send + Sync>(_: &T) {}
    fn assert_movable<T: Send>(_: &T) {}

    let records = sample_records(2000);
    for codec in CODECS {
        let run = build(codec, &records, 256);
        assert_shareable(&run);
        assert_shareable(&run.view());
        assert_movable(&ReadWorkspace::new());

        std::thread::scope(|scope| {
            for thread in 0..2 {
                let (run, records) = (&run, &records);
                scope.spawn(move || {
                    let mut rng = Rng(thread);
                    let mut workspace = ReadWorkspace::new();
                    for _ in 0..500 {
                        let (key, seq, _) = &records[rng.below(records.len())];
                        let found = run.get(key, &mut workspace).unwrap().unwrap();
                        assert_eq!(found.seq, *seq);
                    }
                    assert_eq!(all(run), *records);
                });
            }
        });
    }
}

// Malformed input.

fn probe_all(view: RunView<'_>, workspace: &mut ReadWorkspace) {
    for probe in [&b""[..], b"apple", b"mango", b"pear", b"plum", b"zzz"] {
        let _ = view.get(probe, workspace);
        let _ = view.locate(probe);
    }
}

/// No input may panic, hang or grow a workspace past the block limit.
fn assert_read_paths_are_safe(bytes: &[u8]) {
    let mut workspace = ReadWorkspace::new();
    if let Ok(view) = RunView::open(bytes) {
        let _ = walk(view);
        probe_all(view, &mut workspace);
    }
    if let Ok(run) = Run::new(bytes) {
        // A bit flip may survive validation only inside a value, so accepted
        // bytes must still scan to the end.
        let scanned = walk(run.view()).expect("validated bytes must scan");
        assert_eq!(scanned.len(), run.len());
        probe_all(run.view(), &mut workspace);
    }
    assert!(workspace.capacity() <= MAX_BLOCK_LEN);
}

type Mutation = fn(&mut Vec<u8>, Layout);

/// Damage to the footer and index, which is the same for every codec.
#[test]
fn malformed_structures_are_rejected() {
    use RunCorruption::*;

    #[rustfmt::skip]
    let cases: &[(&str, Mutation, RunCorruption)] = &[
        ("magic", |b, l| b[l.footer + 47] ^= 1, InvalidMagic),
        ("version 2", |b, l| put_u32(b, l.footer + 40, 2), UnsupportedVersion),
        ("version 5", |b, l| put_u32(b, l.footer + 40, 6), UnsupportedVersion),
        ("index offset past end", |b, l| put_u64(b, l.footer, 1 << 40), InvalidFooter),
        ("index offset overflow", |b, l| put_u64(b, l.footer, u64::MAX), InvalidFooter),
        ("block count too large", |b, l| put_u32(b, l.footer + 16, u32::MAX), InvalidFooter),
        ("no blocks but frames", |b, l| put_u32(b, l.footer + 16, 0), InvalidFooter),
        ("first frame offset", |b, l| put_u64(b, l.table, 1), InvalidIndex),
        ("offset past frames", |b, l| put_u64(b, l.table + ENTRY, 1 << 30), InvalidIndex),
        ("offset overflow", |b, l| put_u64(b, l.table + ENTRY, u64::MAX), InvalidIndex),
        ("frame too short for header", |b, l| put_u64(b, l.table + ENTRY, 5), InvalidIndex),
        ("frame overlap", |b, l| {
            let second = u64_at(b, l.table + ENTRY);
            put_u64(b, l.table + 2 * ENTRY, second - 1);
        }, InvalidIndex),
        ("key end past area", |b, l| put_u32(b, l.table + 2 * ENTRY + 8, 1 << 20), InvalidIndex),
        ("key ends decrease", |b, l| put_u32(b, l.table + ENTRY + 8, 1), InvalidIndex),
        ("key area has trailing bytes", |b, l| b.insert(l.footer, 0), InvalidIndex),
        // The key area starts with the five bytes of the minimum key, "apple".
        ("minimum key differs", |b, l| b[l.keys] ^= 1, InvalidKeyRange),
        ("last key differs", |b, l| b[l.keys + 9] += 1, InvalidIndex),
        ("last key below minimum", |b, l| b[l.keys + 5] ^= 1, InvalidKeyRange),
        ("final key differs", |b, l| b[l.footer - 1] ^= 1, InvalidIndex),
        ("record count low", |b, l| put_u64(b, l.footer + 8, 4), RecordCountMismatch),
        ("record count high", |b, l| put_u64(b, l.footer + 8, 6), RecordCountMismatch),
    ];
    for codec in CODECS {
        let base = three_block_bytes(codec);
        Run::new(base.as_slice()).unwrap();
        for (name, mutate, kind) in cases {
            let mut bytes = base.clone();
            mutate(&mut bytes, layout(&base));
            let error = Run::new(bytes.as_slice()).expect_err(name);
            assert_eq!(error.kind, *kind, "{codec:?} {name}");
            assert_read_paths_are_safe(&bytes);
        }
    }
}

type HeaderMutation = fn(&mut Vec<u8>);

/// Header damage behind a correct checksum, so the checks behind it run. The
/// expected kinds are for uncompressed and Zstandard frames.
#[test]
fn invalid_frame_headers_are_rejected_behind_a_valid_checksum() {
    use RunCorruption::*;

    let cases: [(&str, HeaderMutation, [RunCorruption; 2]); 5] = [
        ("unknown codec", |b| b[0] = 4, [UnsupportedCodec; 2]),
        (
            "dictionary reference",
            |b| b[2] = 1,
            [InvalidFrame, MissingDictionary],
        ),
        (
            "decoded length over the limit",
            |b| put_u32(b, 4, MAX_BLOCK_LEN as u32 + 1),
            [InvalidFrame; 2],
        ),
        (
            "decoded length one too long",
            |b| {
                let n = u32_at(b, 4);
                put_u32(b, 4, n + 1);
            },
            [InvalidFrame; 2],
        ),
        (
            "decoded length one too short",
            |b| {
                let n = u32_at(b, 4);
                put_u32(b, 4, n - 1);
            },
            [InvalidFrame; 2],
        ),
    ];
    for (index, codec) in CODECS.into_iter().enumerate() {
        for (name, mutate, kinds) in &cases {
            let mut bytes = three_block_bytes(codec);
            mutate(&mut bytes);
            reseal(&mut bytes);
            let error = Run::new(bytes.as_slice()).expect_err(name);
            assert_eq!(error.kind, kinds[index], "{codec:?} {name}");
            if *name != "dictionary reference" {
                assert_eq!(error.offset, 0, "{codec:?} {name}");
            }
            // A lookup of a key in that frame reports the same problem.
            let mut workspace = ReadWorkspace::new();
            let view = RunView::open(&bytes).unwrap();
            assert_eq!(
                view.get(b"apple", &mut workspace).unwrap_err().kind,
                kinds[index],
                "{codec:?} {name}"
            );
        }
    }
}

#[test]
fn a_gap_between_frames_is_rejected() {
    for (codec, kind) in [
        (Codec::None, RunCorruption::InvalidFrame),
        (Codec::Zstd, RunCorruption::ChecksumMismatch),
    ] {
        let mut bytes = three_block_bytes(codec);
        let l = layout(&bytes);
        let second = u64_at(&bytes, l.table + ENTRY);
        put_u64(&mut bytes, l.table + ENTRY, second + 1);
        assert_eq!(Run::new(bytes.as_slice()).unwrap_err().kind, kind);
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
    let mut bytes = assemble(Codec::None, &[], &[], 0);
    bytes[8] = 1; // record_count
    assert_eq!(
        Run::new(bytes.as_slice()).unwrap_err().kind,
        RunCorruption::InvalidFooter
    );

    let mut bytes = vec![7];
    bytes.extend_from_slice(&assemble(Codec::None, &[], &[], 0));
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
    let bytes = assemble(Codec::None, &[good.clone(), short], &[b"a", b"b"], 2);
    let error = Run::new(bytes.as_slice()).unwrap_err();
    assert!(
        matches!(error.kind, RunCorruption::Block(Corruption::Truncated)),
        "{error:?}"
    );
    assert_eq!(error.offset, HEADER * 2 + good.len());

    // Inside a compressed block, the position is the frame's.
    let mut overrun = good.clone();
    overrun[1] = 100;
    for (codec, offset) in [(Codec::None, HEADER), (Codec::Zstd, 0)] {
        let bytes = assemble(codec, std::slice::from_ref(&overrun), &[b"a"], 1);
        let error = Run::new(bytes.as_slice()).unwrap_err();
        assert_eq!(error.kind, RunCorruption::Block(Corruption::Truncated));
        assert_eq!(error.offset, offset);
        assert_read_paths_are_safe(&bytes);
    }

    // A block with no records.
    for codec in CODECS {
        let bytes = assemble(codec, &[raw_block(&[])], &[b""], 0);
        assert_eq!(
            Run::new(bytes.as_slice()).unwrap_err().kind,
            RunCorruption::EmptyBlock
        );
    }
}

type Disorder<'a> = (&'a str, Vec<Vec<u8>>, &'a [&'a [u8]], Corruption);

/// A correct checksum over a structurally invalid block proves nothing.
#[test]
fn disorder_behind_valid_checksums_is_rejected() {
    use Corruption::{DuplicateKey, OutOfOrder};

    let one = |key: &[u8], seq| raw_block(&[rec(key, seq, None)]);
    let within = raw_block(&[rec(b"b", 1, None), rec(b"a", 1, None)]);
    for codec in CODECS {
        let cases: [Disorder; 4] = [
            (
                "user key",
                vec![one(b"b", 1), one(b"a", 1)],
                &[b"b", b"a"],
                OutOfOrder,
            ),
            (
                "sequence",
                vec![one(b"a", 1), one(b"a", 2)],
                &[b"a", b"a"],
                OutOfOrder,
            ),
            (
                "duplicate",
                vec![one(b"a", 1), one(b"a", 1)],
                &[b"a", b"a"],
                DuplicateKey,
            ),
            ("within a block", vec![within.clone()], &[b"a"], OutOfOrder),
        ];
        for (name, blocks, keys, kind) in cases {
            let bytes = assemble(codec, &blocks, keys, 2);
            let error = Run::new(bytes.as_slice()).unwrap_err();
            assert_eq!(error.kind, RunCorruption::Block(kind), "{codec:?} {name}");
        }

        // A chain that legitimately spans two blocks is accepted.
        let bytes = assemble(codec, &[one(b"a", 2), one(b"a", 1)], &[b"a", b"a"], 2);
        let run = Run::new(bytes.as_slice()).unwrap();
        assert_eq!(seq_of(&run, b"a"), 2);
    }
}

// Integrity.

#[test]
fn wrong_checksums_are_rejected_by_validation() {
    for codec in CODECS {
        let mut bytes = three_block_bytes(codec);
        bytes[8] ^= 1;
        let error = Run::new(bytes.as_slice()).unwrap_err();
        assert_eq!(error.kind, RunCorruption::ChecksumMismatch, "{codec:?}");
        assert_eq!(error.offset, 0);
    }
}

#[test]
fn every_bit_flip_in_a_frame_is_detected() {
    for codec in CODECS {
        let bytes = three_block_bytes(codec);
        for at in 0..layout(&bytes).table {
            for bit in 0..8 {
                let mut flipped = bytes.clone();
                flipped[at] ^= 1 << bit;
                assert!(
                    Run::new(flipped.as_slice()).is_err(),
                    "{codec:?} byte {at} bit {bit}"
                );
                assert_read_paths_are_safe(&flipped);
            }
        }
    }
}

/// Decoding a frame verifies it first; a raw frame is read in place and is
/// checked only by full validation.
#[test]
fn lookups_verify_only_frames_they_decode() {
    for codec in COMPRESSED {
        let mut bytes = three_block_bytes(codec);
        let last_stored_byte = frame_ranges(&bytes)[1].1 - 1;
        bytes[last_stored_byte] ^= 1;
        let view = RunView::open(&bytes).unwrap();
        let mut workspace = ReadWorkspace::new();
        assert!(view.get(b"apple", &mut workspace).is_ok());
        assert_eq!(
            view.get(b"mango", &mut workspace).unwrap_err().kind,
            RunCorruption::ChecksumMismatch
        );
        assert_eq!(
            walk(view).unwrap_err().kind,
            RunCorruption::ChecksumMismatch
        );
    }

    let mut bytes = three_block_bytes(Codec::None);
    bytes[8] ^= 1;
    let view = RunView::open(&bytes).unwrap();
    let mut workspace = ReadWorkspace::new();
    assert!(view.get(b"apple", &mut workspace).unwrap().is_some());
}

// Bounded decoding.

#[test]
fn implausible_decoded_lengths_allocate_nothing() {
    let block = raw_block(&[rec(b"a", 1, None)]);
    let claim = MAX_BLOCK_LEN as u32;
    for codec in COMPRESSED {
        let frames = [(codec_byte(codec), claim, compress(codec, &block))];
        let bytes = assemble_frames(&frames, &[b"a"], 1);
        let mut workspace = ReadWorkspace::new();
        let view = RunView::open(&bytes).unwrap();
        assert_eq!(
            view.get(b"a", &mut workspace).unwrap_err().kind,
            RunCorruption::InvalidFrame,
            "{codec:?}"
        );
        assert_eq!(workspace.capacity(), 0, "{codec:?}");
        assert_eq!(
            Run::new(bytes.as_slice()).unwrap_err().kind,
            RunCorruption::InvalidFrame
        );
    }
}

/// An excessive declared length is refused before inspecting the payload.
#[test]
fn decoded_lengths_over_the_block_limit_allocate_nothing() {
    let stored = vec![0; 32];
    let frames = [(1, MAX_BLOCK_LEN as u32 + 1, stored)];
    let bytes = assemble_frames(&frames, &[b"a"], 1);
    let mut workspace = ReadWorkspace::new();
    let view = RunView::open(&bytes).unwrap();
    assert_eq!(
        view.get(b"a", &mut workspace).unwrap_err().kind,
        RunCorruption::InvalidFrame
    );
    assert_eq!(workspace.capacity(), 0);
}

#[test]
fn output_larger_than_declared_is_rejected_within_the_declared_size() {
    let zeros = vec![0; 1 << 20];
    {
        let (codec, kind) = (Codec::Zstd, RunCorruption::InvalidFrame);
        let frames = [(codec_byte(codec), 1024, compress(codec, &zeros))];
        let bytes = assemble_frames(&frames, &[b"a"], 1);
        let mut workspace = ReadWorkspace::new();
        let view = RunView::open(&bytes).unwrap();
        assert_eq!(view.get(b"a", &mut workspace).unwrap_err().kind, kind);
        assert!(workspace.capacity() <= 1024);
    }
}

#[test]
fn zstd_frames_must_declare_their_content_size() {
    let block = raw_block(&[rec(b"a", 1, None)]);
    let mut context = zstd_safe::CCtx::create();
    context
        .set_parameter(zstd_safe::CParameter::ContentSizeFlag(false))
        .unwrap();
    let mut stored = Vec::with_capacity(zstd_safe::compress_bound(block.len()));
    context.compress2(&mut stored, &block).unwrap();

    let frames = [(1, block.len() as u32, stored)];
    let bytes = assemble_frames(&frames, &[b"a"], 1);
    assert_eq!(
        Run::new(bytes.as_slice()).unwrap_err().kind,
        RunCorruption::InvalidFrame
    );
}

#[test]
fn truncated_compressed_payloads_are_rejected() {
    let block = raw_block(&sample_records(40));
    for codec in COMPRESSED {
        let stored = compress(codec, &block);
        for cut in 1..stored.len() {
            let frames = [(
                codec_byte(codec),
                block.len() as u32,
                stored[..cut].to_vec(),
            )];
            let bytes = assemble_frames(&frames, &[b"key00039"], 40);
            let error = Run::new(bytes.as_slice()).unwrap_err();
            assert!(
                matches!(
                    error.kind,
                    RunCorruption::CorruptPayload | RunCorruption::InvalidFrame
                ),
                "{codec:?} cut {cut}: {error:?}"
            );
        }
    }
}

#[test]
fn codec_confusion_is_rejected() {
    let block = raw_block(&sample_records(40));
    for (written, claimed) in [(Codec::None, Codec::Zstd), (Codec::Zstd, Codec::None)] {
        let frames = [(
            codec_byte(claimed),
            block.len() as u32,
            compress(written, &block),
        )];
        let bytes = assemble_frames(&frames, &[b"key00039"], 40);
        assert!(
            Run::new(bytes.as_slice()).is_err(),
            "{written:?} as {claimed:?}"
        );
        assert_read_paths_are_safe(&bytes);
    }
}

/// `RunView` is constant time to open and only checks what it reads. This
/// states that boundary: an index that lies is invisible to it and is caught
/// by full validation.
#[test]
fn view_trusts_the_index_and_full_validation_does_not() {
    for codec in CODECS {
        let base = three_block_bytes(codec);
        let l = layout(&base);
        let mut lying = base.clone();
        // "mango" becomes "aango", which is below its block's real last key.
        // It follows the minimum key and the first block's key, both "apple".
        lying[l.keys + 10] = b'a';

        let view = RunView::open(&lying).unwrap();
        let mut workspace = ReadWorkspace::new();
        assert_eq!(view.locate(b"mango"), Ok(Some(2)));
        assert_eq!(view.get(b"mango", &mut workspace), Ok(None));
        assert_eq!(
            Run::new(lying.as_slice()).unwrap_err().kind,
            RunCorruption::InvalidIndex
        );
    }
}

#[test]
fn every_truncation_is_rejected() {
    for codec in CODECS {
        let bytes = build(codec, &sample_records(40), 64).as_bytes().to_vec();
        for len in 0..bytes.len() {
            assert!(Run::new(&bytes[..len]).is_err(), "{codec:?} prefix {len}");
            assert_read_paths_are_safe(&bytes[..len]);
        }
    }
}

#[test]
fn random_corruption_never_panics_or_hangs() {
    for codec in CODECS {
        let base = build(codec, &sample_records(60), 128).as_bytes().to_vec();
        for seed in 0..1500 {
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
}

/// Resealing lets random damage reach the decompressors and the block reader.
#[test]
fn corrupt_payloads_behind_valid_checksums_never_panic_or_hang() {
    for codec in COMPRESSED {
        let base = build(codec, &sample_records(60), 128).as_bytes().to_vec();
        let frames = frame_ranges(&base);
        for seed in 0..1500 {
            let mut rng = Rng(seed);
            let mut bytes = base.clone();
            for _ in 0..1 + rng.below(4) {
                let (start, end) = frames[rng.below(frames.len())];
                let at = start + HEADER + rng.below(end - start - HEADER);
                bytes[at] = rng.next() as u8;
            }
            reseal(&mut bytes);
            assert_read_paths_are_safe(&bytes);
        }
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

#[test]
fn the_default_builder_uses_adaptive_zstandard_blocks() {
    assert_eq!(TARGET_BLOCK_LEN, 16 << 10);
    let records: Vec<Owned> = (0..4000)
        .map(|i| rec(format!("key:{i:06}").as_bytes(), 1, Some(&[7; 40])))
        .collect();
    let mut builder = RunBuilder::default();
    for (key, seq, value) in &records {
        builder.add(key, *seq, as_value(value)).unwrap();
    }
    let run = builder.finish();
    assert_eq!(run.as_bytes()[0], codec_byte(Codec::Zstd));
    let mut explicit = RunBuilder::new(Codec::Zstd);
    for (key, seq, value) in &records {
        explicit.add(key, *seq, as_value(value)).unwrap();
    }
    let explicit = explicit.finish();
    assert_eq!(run.as_bytes(), explicit.as_bytes());
    let blocks = decoded_blocks(run.as_bytes());
    assert!(
        blocks
            .iter()
            .all(|block| block.len() <= crate::run::RAW_BLOCK_CAP)
    );
    assert_eq!(all(&run), records);
}

/// A built run's filter never hides a stored key, version or tombstone, and
/// skips most absent keys without decoding; the same bytes reloaded have no
/// filter and answer identically by decoding.
#[test]
fn a_built_runs_filter_never_hides_a_key_and_reloaded_bytes_fall_back() {
    let large = vec![0xFF; 1_000];
    let mut records: Vec<Owned> = vec![
        (Vec::new(), 9, Some(b"empty".to_vec())),
        (vec![0x00], 8, None),
        (vec![0x00, 0xFF], 7, Some(b"v".to_vec())),
    ];
    // A key with versions spanning blocks, newest a tombstone.
    records.extend((0..40).map(|i| {
        (
            b"chain".to_vec(),
            1_000 - i,
            (i > 0).then(|| vec![b'x'; 64]),
        )
    }));
    records.extend((0..2_000u32).map(|i| {
        (
            format!("k{i:05}").into_bytes(),
            5,
            Some(i.to_le_bytes().to_vec()),
        )
    }));
    records.push((vec![0xFF, 0xFF], 4, None));
    records.push((large, 3, Some(b"large".to_vec())));
    let run = build(Codec::Zstd, &records, 256);
    let reloaded = Run::new(run.as_bytes().to_vec()).unwrap();
    assert!(run.filter_bytes() > 0);
    assert_eq!(reloaded.filter_bytes(), 0);

    let mut workspace = ReadWorkspace::new();
    let mut newest: BTreeMap<&[u8], (u64, &Option<Vec<u8>>)> = BTreeMap::new();
    for (key, seq, value) in &records {
        newest.entry(key).or_insert((*seq, value));
    }
    for (key, (seq, value)) in &newest {
        for run in [&run, &reloaded] {
            let found = run.get(key, &mut workspace).unwrap().unwrap();
            assert_eq!((found.seq, found.value), (*seq, as_value(value)));
        }
    }

    // Absent keys inside the range: the reloaded run decodes a block for
    // each, the filtered one for about 1% (the hash is fixed, so exactly).
    let absent: Vec<Vec<u8>> = (0..2_000u32)
        .map(|i| format!("k{i:05}a").into_bytes())
        .collect();
    let mut blocks = Vec::new();
    for run in [&run, &reloaded] {
        let before = workspace.blocks_loaded();
        for key in &absent {
            assert!(run.get(key, &mut workspace).unwrap().is_none());
        }
        blocks.push(workspace.blocks_loaded() - before);
    }
    assert_eq!(blocks[1], 2_000);
    assert!(blocks[0] < 60, "{} of 2000 absent keys decoded", blocks[0]);
}
