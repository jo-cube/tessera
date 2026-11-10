//! Repeatable block and run measurements on deterministic datasets.
//!
//! With the argument `geometry`, instead compares run block sizes and codecs.
//! With the argument `prefix`, instead compares prefix scans over those sizes.
//! With the argument `merge`, instead measures merged iteration over many runs.
//! With the argument `logical`, instead measures latest-state reads over runs.
//!
//! Run with `scripts/validate.sh bench`. Times are the median nanoseconds per
//! record or lookup across a fixed number of rounds, and every loop has a
//! fixed bound, so a broken reader or builder cannot run away. Allocation counts come
//! from one pass with a counting global allocator, which is the only unsafe
//! code in the repository and exists solely in this example.

use std::alloc::{GlobalAlloc, Layout, System};
use std::hint::black_box;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use crc32fast::Hasher;
use lz4_flex::block as lz4;
use tessera::block::{Block, BlockBuilder, BlockPosition, BlockView, Record, Value};
use tessera::run::{
    Codec, LogicalCursor, LogicalWorkspace, MergeCursor, MergeWorkspace, ReadWorkspace, Run,
    RunBuilder, RunCursor, RunSet, RunView,
};

struct CountingAllocator;

static ALLOCATIONS: AtomicU64 = AtomicU64::new(0);
static LIVE: AtomicU64 = AtomicU64::new(0);
static PEAK: AtomicU64 = AtomicU64::new(0);

/// Records `added` more live bytes. `held` are live only during the call, as
/// the old buffer is while a reallocation copies it.
fn note_live(added: u64, held: u64) {
    let live = LIVE.fetch_add(added, Ordering::Relaxed) + added;
    PEAK.fetch_max(live + held, Ordering::Relaxed);
}

// SAFETY: every call forwards unchanged to the system allocator.
unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        ALLOCATIONS.fetch_add(1, Ordering::Relaxed);
        note_live(layout.size() as u64, 0);
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        LIVE.fetch_sub(layout.size() as u64, Ordering::Relaxed);
        unsafe { System.dealloc(ptr, layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        ALLOCATIONS.fetch_add(1, Ordering::Relaxed);
        LIVE.fetch_sub(layout.size() as u64, Ordering::Relaxed);
        note_live(new_size as u64, layout.size() as u64);
        unsafe { System.realloc(ptr, layout, new_size) }
    }
}

#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator;

fn allocations_during(operation: impl FnOnce()) -> u64 {
    let before = ALLOCATIONS.load(Ordering::Relaxed);
    operation();
    ALLOCATIONS.load(Ordering::Relaxed) - before
}

/// Returns the peak live Rust heap above its level at entry. A reallocation
/// is counted as if it copied, so this is an upper bound. Memory the Zstandard
/// C library allocates for its contexts is not included.
fn peak_during(operation: impl FnOnce()) -> u64 {
    let before = LIVE.load(Ordering::Relaxed);
    PEAK.store(before, Ordering::Relaxed);
    operation();
    PEAK.load(Ordering::Relaxed) - before
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

    fn bytes(&mut self, len: usize) -> Vec<u8> {
        (0..len).map(|_| self.next() as u8).collect()
    }
}

struct Owned {
    key: Vec<u8>,
    seq: u64,
    value: Option<Vec<u8>>,
}

impl Owned {
    fn value(&self) -> Value<'_> {
        self.value.as_deref().map_or(Value::Tombstone, Value::Bytes)
    }
}

/// Sequence numbers start high enough to take a realistic six-byte varint.
const BASE_SEQ: u64 = 1 << 40;

/// Yields sorted records until the block would reach `target` encoded bytes.
fn fill(target: usize, mut next: impl FnMut(u64) -> Vec<Owned>) -> Vec<Owned> {
    let mut builder = BlockBuilder::new();
    let mut records = Vec::new();
    // Every group adds at least one record of at least four bytes.
    for group in 0..target as u64 {
        for record in next(group) {
            builder
                .add(&record.key, record.seq, record.value())
                .unwrap();
            records.push(record);
        }
        if builder.encoded_len() >= target {
            return records;
        }
    }
    panic!("dataset did not reach {target} encoded bytes");
}

fn structured_keys(target: usize) -> Vec<Owned> {
    let mut rng = Rng(1);
    fill(target, |i| {
        vec![Owned {
            key: format!("user:{i:012}:profile").into_bytes(),
            seq: BASE_SEQ + rng.next() % (1 << 20),
            value: Some(rng.bytes(64)),
        }]
    })
}

fn random_keys(target: usize) -> Vec<Owned> {
    let mut rng = Rng(2);
    let mut keys = Vec::new();
    let estimated = target / 64 + 16;
    while keys.len() < estimated {
        keys.push(rng.bytes(16));
    }
    keys.sort();
    keys.dedup();
    let mut keys = keys.into_iter();
    fill(target, move |_| {
        vec![Owned {
            key: keys.next().expect("estimate covers the target size"),
            seq: BASE_SEQ + rng.next() % (1 << 20),
            value: Some(rng.bytes(64)),
        }]
    })
}

fn small_values(target: usize) -> Vec<Owned> {
    let mut rng = Rng(3);
    fill(target, |i| {
        vec![Owned {
            key: format!("metric:{i:010}").into_bytes(),
            seq: BASE_SEQ + i,
            value: Some(rng.bytes(8)),
        }]
    })
}

fn multi_version(target: usize) -> Vec<Owned> {
    let mut rng = Rng(4);
    fill(target, |i| {
        let key = format!("order:{i:012}").into_bytes();
        (0..8)
            .map(|version| Owned {
                key: key.clone(),
                seq: BASE_SEQ + 1000 - version,
                value: (version != 3).then(|| rng.bytes(32)),
            })
            .collect()
    })
}

/// Compressible values, in the style of a JSON document store.
fn json_like(target: usize) -> Vec<Owned> {
    const STATUS: [&str; 4] = ["active", "pending", "suspended", "archived"];
    const REGION: [&str; 4] = ["eu-west-1", "us-east-1", "ap-south-1", "sa-east-1"];

    let mut rng = Rng(5);
    fill(target, |i| {
        let value = format!(
            r#"{{"id":{i},"name":"user_{}","email":"user{}@example.com","status":"{}","region":"{}","score":{},"verified":{}}}"#,
            rng.next() % 100_000,
            rng.next() % 100_000,
            STATUS[(rng.next() % 4) as usize],
            REGION[(rng.next() % 4) as usize],
            rng.next() % 1000,
            rng.next().is_multiple_of(2),
        );
        vec![Owned {
            key: format!("user:{i:012}:profile").into_bytes(),
            seq: BASE_SEQ + rng.next() % (1 << 20),
            value: Some(value.into_bytes()),
        }]
    })
}

fn build(records: &[Owned]) -> Vec<u8> {
    let mut builder = BlockBuilder::new();
    for record in records {
        builder
            .add(&record.key, record.seq, record.value())
            .unwrap();
    }
    builder.finish()
}

const ROUNDS: usize = 201;

/// Returns the median time per operation over `ROUNDS` runs of `round`.
fn median_ns(operations: usize, round: impl FnMut()) -> f64 {
    median_ns_of(ROUNDS, operations, round)
}

/// Returns the median time per operation over `rounds` runs of `round`.
fn median_ns_of(rounds: usize, operations: usize, mut round: impl FnMut()) -> f64 {
    let mut samples: Vec<f64> = (0..rounds)
        .map(|_| {
            let start = Instant::now();
            round();
            start.elapsed().as_nanos() as f64 / operations as f64
        })
        .collect();
    samples.sort_by(f64::total_cmp);
    samples[rounds / 2]
}

fn measure(name: &str, records: &[Owned]) {
    let bytes = build(records);
    let block = Block::new(bytes.as_slice()).unwrap();
    assert_eq!(block.len(), records.len());

    let raw: usize = records
        .iter()
        .map(|r| r.key.len() + r.value.as_ref().map_or(0, Vec::len) + 8 + 1)
        .sum();

    let mut rng = Rng(99);
    let mut hits: Vec<&[u8]> = records.iter().map(|r| r.key.as_slice()).collect();
    hits.dedup();
    for i in (1..hits.len()).rev() {
        hits.swap(i, (rng.next() % (i as u64 + 1)) as usize);
    }
    // Appending NUL yields keys that sort between stored keys but never exist.
    let misses: Vec<Vec<u8>> = hits
        .iter()
        .map(|k| [*k, &[0][..]].concat())
        .filter(|k| block.get(k).is_none())
        .collect();

    let build_ns = median_ns(records.len(), || {
        black_box(build(black_box(records)));
    });
    let validate_ns = median_ns(records.len(), || {
        black_box(Block::new(black_box(bytes.as_slice())).unwrap());
    });
    let scan = || {
        let mut cursor = block.cursor();
        let mut total = 0usize;
        for _ in 0..records.len() {
            total += cursor.next_record().expect("record").key.len();
        }
        assert!(cursor.next_record().is_none());
        black_box(total);
    };
    let scan_ns = median_ns(records.len(), scan);
    let hit_ns = median_ns(hits.len(), || {
        for key in &hits {
            black_box(block.get(black_box(key)));
        }
    });
    let miss_ns = median_ns(misses.len(), || {
        for key in &misses {
            black_box(block.get(black_box(key)));
        }
    });

    let build_allocs = allocations_during(|| drop(build(records)));
    let validate_allocs = allocations_during(|| {
        black_box(Block::new(bytes.as_slice()).unwrap());
    });
    let scan_allocs = allocations_during(scan);
    let lookup_allocs = allocations_during(|| {
        for key in &hits {
            black_box(block.get(key));
        }
        for key in &misses {
            black_box(block.get(key));
        }
    });

    println!(
        "| {name} | {} | {} | {raw} | {:.3} | {:.1} | {build_ns:.0} | {validate_ns:.1} | {scan_ns:.1} | {hit_ns:.0} | {miss_ns:.0} | {build_allocs}/{validate_allocs}/{scan_allocs}/{lookup_allocs} |",
        records.len(),
        bytes.len(),
        bytes.len() as f64 / raw as f64,
        (bytes.len() as f64
            - records
                .iter()
                .map(|r| r.value.as_ref().map_or(0, Vec::len))
                .sum::<usize>() as f64)
            / records.len() as f64,
    );
}

/// Lookups per round on the reusable-buffer paths, which reload every time.
const SCRATCH_LOOKUPS: usize = 32;

/// Compares the ways a reader can reach a block, using a memory copy as a
/// stand-in for decompression. No codec cost is included.
fn measure_reader_paths(name: &str, records: &[Owned]) {
    let bytes = build(records);
    let block = Block::new(bytes.as_slice()).unwrap();

    let mut rng = Rng(99);
    let mut keys: Vec<&[u8]> = records.iter().map(|r| r.key.as_slice()).collect();
    keys.dedup();
    for i in (1..keys.len()).rev() {
        keys.swap(i, (rng.next() % (i as u64 + 1)) as usize);
    }
    let few = &keys[..SCRATCH_LOOKUPS.min(keys.len())];

    let mut scratch = Vec::with_capacity(bytes.len());
    let load = |scratch: &mut Vec<u8>| {
        scratch.clear();
        scratch.extend_from_slice(black_box(&bytes));
    };

    let validate_ns = median_ns(1, || {
        black_box(Block::new(black_box(bytes.as_slice())).unwrap());
    });
    let validated_get_ns = median_ns(keys.len(), || {
        for key in &keys {
            black_box(block.get(black_box(key)));
        }
    });
    let view_get_ns = median_ns(keys.len(), || {
        for key in &keys {
            let view = BlockView::open(black_box(bytes.as_slice())).unwrap();
            black_box(view.get(black_box(key)).unwrap());
        }
    });
    let load_ns = median_ns(1, || load(&mut scratch));
    let scratch_view_ns = median_ns(few.len(), || {
        for key in few {
            load(&mut scratch);
            let view = BlockView::open(&scratch).unwrap();
            black_box(view.get(black_box(key)).unwrap());
        }
    });
    let scratch_validate_ns = median_ns(few.len(), || {
        for key in few {
            load(&mut scratch);
            let block = Block::new(scratch.as_slice()).unwrap();
            black_box(block.get(black_box(key)));
        }
    });

    let cursor_scan = || {
        let mut cursor = block.cursor();
        let mut total = 0usize;
        for _ in 0..records.len() {
            total += cursor.next_record().expect("record").key.len();
        }
        assert!(cursor.next_record().is_none());
        black_box(total);
    };
    let cursor_ns = median_ns(records.len(), cursor_scan);

    // Reopens the view at every step, as an iterator owning its buffer would.
    let mut position = BlockPosition::new();
    let mut position_scan = |scratch: &[u8]| {
        position.reset();
        let mut total = 0usize;
        for _ in 0..records.len() {
            let view = BlockView::open(scratch).unwrap();
            total += position
                .next_record(view)
                .unwrap()
                .expect("record")
                .key
                .len();
        }
        black_box(total);
    };
    load(&mut scratch);
    let position_ns = median_ns(records.len(), || position_scan(&scratch));

    // Both paths are warm here, so these count only per-access allocations.
    let scratch_allocs = allocations_during(|| {
        for key in few {
            load(&mut scratch);
            let view = BlockView::open(&scratch).unwrap();
            black_box(view.get(key).unwrap());
        }
    });
    let position_allocs = allocations_during(|| position_scan(&scratch));
    let cursor_allocs = allocations_during(cursor_scan);

    println!(
        "| {name} | {:.2} | {:.2} | {validated_get_ns:.0} | {view_get_ns:.0} | {scratch_view_ns:.0} | {scratch_validate_ns:.0} | {cursor_ns:.1} | {position_ns:.1} | {scratch_allocs}/{position_allocs}/{cursor_allocs} |",
        validate_ns / 1000.0,
        load_ns / 1000.0,
    );
}

/// Rounds for run measurements, which cover far more bytes per round.
const RUN_ROUNDS: usize = 15;

/// Distinct keys probed per round, drawn from the shuffled key list.
const RUN_PROBES: usize = 1000;

const CODECS: [(Codec, &str); 3] = [
    (Codec::None, "none"),
    (Codec::Lz4, "lz4"),
    (Codec::Zstd, "zstd"),
];

fn build_run(codec: Codec, records: &[Owned]) -> Run<Vec<u8>> {
    let mut builder = RunBuilder::new(codec);
    for record in records {
        builder
            .add(&record.key, record.seq, record.value())
            .unwrap();
    }
    builder.finish()
}

/// One frame's codec, decoded length, stored bytes and checksummed header.
type StoredFrame<'a> = (u8, usize, &'a [u8], &'a [u8]);

/// Every frame of a run, from the documented layout, so codec costs can be
/// timed apart from the reader.
fn frames(bytes: &[u8]) -> Vec<StoredFrame<'_>> {
    let footer = bytes.len() - 28;
    let u32_at = |at: usize| u32::from_le_bytes(bytes[at..at + 4].try_into().unwrap()) as usize;
    let u64_at = |at: usize| u64::from_le_bytes(bytes[at..at + 8].try_into().unwrap()) as usize;
    let table = u64_at(footer);
    let count = u32_at(footer + 16);
    (0..count)
        .map(|i| {
            let start = u64_at(table + i * 12);
            let end = if i + 1 < count {
                u64_at(table + (i + 1) * 12)
            } else {
                table
            };
            (
                bytes[start],
                u32_at(start + 4),
                &bytes[start + 12..end],
                &bytes[start..start + 8],
            )
        })
        .collect()
}

fn measure_run(
    name: &str,
    label: &str,
    codec: Codec,
    records: &[Owned],
    uncompressed: usize,
) -> usize {
    let run = build_run(codec, records);
    assert_eq!(run.len(), records.len());
    let bytes = run.as_bytes();
    let view = run.view();

    let mut rng = Rng(77);
    let mut keys: Vec<&[u8]> = records.iter().map(|r| r.key.as_slice()).collect();
    keys.dedup();
    for i in (1..keys.len()).rev() {
        keys.swap(i, (rng.next() % (i as u64 + 1)) as usize);
    }
    keys.truncate(RUN_PROBES);
    let mut workspace = ReadWorkspace::new();
    let misses: Vec<Vec<u8>> = keys
        .iter()
        .map(|k| [*k, &[0][..]].concat())
        .filter(|k| run.get(k, &mut workspace).unwrap().is_none())
        .collect();

    let timed = |operations, round: &mut dyn FnMut()| median_ns_of(RUN_ROUNDS, operations, round);
    let build_ns = timed(records.len(), &mut || {
        black_box(build_run(codec, black_box(records)));
    });
    let validate_ns = timed(records.len(), &mut || {
        black_box(Run::new(black_box(bytes)).unwrap());
    });
    let mut scan_workspace = ReadWorkspace::new();
    let mut scan = || {
        let mut cursor = run.cursor(&mut scan_workspace);
        let mut total = 0usize;
        for _ in 0..records.len() {
            total += cursor.next_record().unwrap().expect("record").key.len();
        }
        assert!(cursor.next_record().unwrap().is_none());
        black_box(total);
    };
    let scan_ns = timed(records.len(), &mut || scan());
    let locate_ns = timed(keys.len(), &mut || {
        for key in &keys {
            black_box(view.locate(black_box(key)).unwrap());
        }
    });
    let hit_ns = timed(keys.len(), &mut || {
        for key in &keys {
            black_box(run.get(black_box(key), &mut workspace).unwrap());
        }
    });
    let miss_ns = timed(misses.len(), &mut || {
        for key in &misses {
            black_box(run.get(black_box(key), &mut workspace).unwrap());
        }
    });

    // Codec costs alone, measured on the same frames through the libraries.
    let stored = frames(bytes);
    let largest = stored.iter().map(|f| f.1).max().unwrap();
    let mut out = vec![0; largest];
    let mut zstd = zstd_safe::DCtx::create();
    let checksum_ns = timed(stored.len(), &mut || {
        for (_, _, payload, _) in &stored {
            black_box(crc32fast::hash(black_box(payload)));
        }
    });
    let decompress_ns = timed(stored.len(), &mut || {
        for (id, len, payload, _) in &stored {
            match id {
                1 => {
                    lz4_flex::block::decompress_into(payload, &mut out[..*len]).unwrap();
                }
                2 => {
                    zstd.decompress(&mut out[..*len], payload).unwrap();
                }
                _ => {}
            }
            black_box(&out);
        }
    });

    let warm_allocs = allocations_during(|| {
        for key in &keys {
            black_box(run.get(key, &mut workspace).unwrap());
        }
        for key in &misses {
            black_box(run.get(key, &mut workspace).unwrap());
        }
    });
    let scan_allocs = allocations_during(&mut scan);
    let build_allocs = allocations_during(|| drop(build_run(codec, records)));
    let build_peak = peak_during(|| drop(build_run(codec, records)));
    let validate_peak = peak_during(|| {
        black_box(Run::new(bytes).unwrap());
    });

    let total = bytes.len();
    let baseline = if uncompressed == 0 {
        total
    } else {
        uncompressed
    };
    let mib = |n: u64| n as f64 / (1 << 20) as f64;
    println!(
        "| {name} | {label} | {:.2} | {:.3} | {build_ns:.0} | {validate_ns:.1} | {scan_ns:.1} | {locate_ns:.0} | {hit_ns:.0} | {miss_ns:.0} | {:.1} | {:.1} | {} | {:.1} | {:.2} | {build_allocs}/{scan_allocs}/{warm_allocs} |",
        mib(total as u64),
        total as f64 / baseline as f64,
        checksum_ns / 1000.0,
        decompress_ns / 1000.0,
        stored.iter().map(|f| f.2.len()).sum::<usize>() / stored.len(),
        mib(build_peak),
        mib(validate_peak),
    );
    total
}

/// Versions of one key that differ in a few fields, as successive updates do.
fn version_heavy(target: usize) -> Vec<Owned> {
    const STATE: [&str; 4] = ["created", "paid", "shipped", "delivered"];

    let mut rng = Rng(6);
    fill(target, |i| {
        let key = format!("order:{i:012}").into_bytes();
        let customer = rng.next() % 100_000;
        let total = rng.next() % 100_000;
        let versions = 4 + rng.next() % 9;
        (0..versions)
            .map(|version| Owned {
                key: key.clone(),
                seq: BASE_SEQ + 1000 - version,
                value: (!rng.next().is_multiple_of(16)).then(|| {
                    format!(
                        r#"{{"customer":{customer},"total":{total},"state":"{}","updated":{},"rev":{}}}"#,
                        STATE[(rng.next() % 4) as usize],
                        1_700_000_000 + rng.next() % 86_400,
                        versions - version,
                    )
                    .into_bytes()
                }),
            })
            .collect()
    })
}

type Fixture = fn(usize) -> Vec<Owned>;

const GEOMETRY_BYTES: usize = 32 << 20;
const GEOMETRY_KIB: [usize; 6] = [4, 8, 16, 32, 64, 128];
const GEOMETRY_ROUNDS: usize = 9;
const GEOMETRY_PROBES: usize = 2000;

fn build_run_with(codec: Codec, target: usize, records: &[Owned]) -> Run<Vec<u8>> {
    let mut builder = RunBuilder::with_target_block_len(codec, target);
    for record in records {
        builder
            .add(&record.key, record.seq, record.value())
            .unwrap();
    }
    builder.finish()
}

/// Decompresses one frame through its library, with no reader involved.
fn decode_frame(zstd: &mut zstd_safe::DCtx<'_>, id: u8, payload: &[u8], out: &mut [u8]) {
    match id {
        1 => {
            lz4::decompress_into(payload, out).unwrap();
        }
        2 => {
            zstd.decompress(out, payload).unwrap();
        }
        _ => out.copy_from_slice(payload),
    }
}

/// Measures one dataset, block target and codec.
///
/// `get` has no decoded-block cache, so every lookup verifies and decompresses
/// its candidate block, including when two probes land in the same block. The
/// four cost components time each step alone over the same probes.
fn measure_geometry(
    dataset: &str,
    kib: usize,
    label: &str,
    codec: Codec,
    records: &[Owned],
    probes: &[&[u8]],
) {
    let target = kib << 10;
    let run = build_run_with(codec, target, records);
    let bytes = run.as_bytes();
    let view = run.view();
    let stored = frames(bytes);
    let timed = |ops, round: &mut dyn FnMut()| median_ns_of(GEOMETRY_ROUNDS, ops, round);

    let mut zstd = zstd_safe::DCtx::create();
    let decoded: Vec<Vec<u8>> = stored
        .iter()
        .map(|(id, len, payload, _)| {
            let mut out = vec![0; *len];
            decode_frame(&mut zstd, *id, payload, &mut out);
            out
        })
        .collect();
    let candidates: Vec<usize> = probes
        .iter()
        .map(|key| view.locate(key).unwrap().expect("probes are stored keys"))
        .collect();

    let build_ns = timed(records.len(), &mut || {
        black_box(build_run_with(codec, target, black_box(records)));
    });
    let validate_ns = timed(records.len(), &mut || {
        black_box(Run::new(black_box(bytes)).unwrap());
    });

    let mut workspace = ReadWorkspace::new();
    let mut scan = || {
        let mut cursor = run.cursor(&mut workspace);
        let mut total = 0usize;
        for _ in 0..records.len() {
            total += cursor.next_record().unwrap().expect("record").key.len();
        }
        black_box(total);
    };
    let scan_ns = timed(records.len(), &mut || scan());

    let locate_ns = timed(probes.len(), &mut || {
        for key in probes {
            black_box(view.locate(black_box(key)).unwrap());
        }
    });
    // Reads verify only frames they decode, so uncompressed lookups pay none.
    let checksum_ns = if codec == Codec::None {
        0.0
    } else {
        timed(probes.len(), &mut || {
            for &block in &candidates {
                let (_, _, payload, header) = stored[block];
                let mut hasher = Hasher::new();
                hasher.update(black_box(header));
                hasher.update(black_box(payload));
                black_box(hasher.finalize());
            }
        })
    };
    let largest = stored.iter().map(|f| f.1).max().unwrap_or(0);
    let mut out = vec![0; largest];
    let decompress_ns = if codec == Codec::None {
        0.0
    } else {
        timed(probes.len(), &mut || {
            for &block in &candidates {
                let (id, len, payload, _) = stored[block];
                decode_frame(&mut zstd, id, payload, &mut out[..len]);
                black_box(&out);
            }
        })
    };
    let search_ns = timed(probes.len(), &mut || {
        for (key, &block) in probes.iter().zip(&candidates) {
            let found = BlockView::open(&decoded[block])
                .unwrap()
                .get(black_box(key));
            black_box(found.unwrap());
        }
    });

    let get_ns = timed(probes.len(), &mut || {
        for key in probes {
            black_box(run.get(black_box(key), &mut workspace).unwrap());
        }
    });
    let mut latencies = Vec::with_capacity(GEOMETRY_ROUNDS * probes.len());
    for _ in 0..GEOMETRY_ROUNDS {
        for key in probes {
            let start = Instant::now();
            black_box(run.get(black_box(key), &mut workspace).unwrap());
            latencies.push(start.elapsed().as_nanos() as u64);
        }
    }
    latencies.sort_unstable();
    let percentile = |p: f64| latencies[((latencies.len() - 1) as f64 * p) as usize];
    let misses: Vec<Vec<u8>> = probes.iter().map(|k| [*k, &[0][..]].concat()).collect();
    let miss_ns = timed(misses.len(), &mut || {
        for key in &misses {
            black_box(run.get(black_box(key), &mut workspace).unwrap());
        }
    });

    // The scans above have grown the workspace to the largest block.
    let warm_allocs = allocations_during(|| {
        for key in probes {
            black_box(run.get(key, &mut workspace).unwrap());
        }
    });
    let build_peak = peak_during(|| drop(build_run_with(codec, target, records)));

    let blocks = stored.len();
    let stored_bytes: usize = stored.iter().map(|f| f.2.len()).sum();
    let headers = 12 * blocks;
    let index = view.index_len() + 28;
    println!(
        "| {dataset} | {kib} | {label} | {} | {} | {blocks} | {} | {stored_bytes} | {headers} | {index} | {build_ns:.1} | {validate_ns:.1} | {scan_ns:.1} | {locate_ns:.0} | {checksum_ns:.0} | {decompress_ns:.0} | {search_ns:.0} | {get_ns:.0} | {} | {} | {} | {} | {miss_ns:.0} | {warm_allocs} | {build_peak} |",
        records.len(),
        bytes.len(),
        stored.iter().map(|f| f.1).sum::<usize>() / blocks,
        percentile(0.5),
        percentile(0.9),
        percentile(0.99),
        percentile(0.999),
    );
}

/// What a Zstandard block spends its bytes on.
#[derive(Default)]
struct ZstdAnatomy {
    blocks: usize,
    raw_or_rle_blocks: usize,
    literals: usize,
    entropy_coded_literals: usize,
    sequences: usize,
}

/// Parses the documented Zstandard frame layout of one frame into `anatomy`.
fn zstd_anatomy(frame: &[u8], anatomy: &mut ZstdAnatomy) {
    let descriptor = frame[4];
    let single_segment = descriptor & 0x20 != 0;
    let dictionary = [0, 1, 2, 4][(descriptor & 3) as usize];
    let content_size = match descriptor >> 6 {
        0 => usize::from(single_segment),
        1 => 2,
        2 => 4,
        _ => 8,
    };
    let mut at = 5 + usize::from(!single_segment) + dictionary + content_size;
    loop {
        let header = u32::from_le_bytes([frame[at], frame[at + 1], frame[at + 2], 0]);
        let (last, kind, size) = (header & 1 == 1, (header >> 1) & 3, (header >> 3) as usize);
        at += 3;
        anatomy.blocks += 1;
        if kind == 2 {
            let block = &frame[at..at + size];
            let first = block[0] as usize;
            let literal_kind = first & 3;
            let format = (first >> 2) & 3;
            let (regenerated, section) = if literal_kind < 2 {
                match format {
                    0 | 2 => (first >> 3, 1 + (first >> 3)),
                    1 => {
                        let n = (first >> 4) | (block[1] as usize) << 4;
                        (n, 2 + n)
                    }
                    _ => {
                        let n = (first >> 4) | (block[1] as usize) << 4 | (block[2] as usize) << 12;
                        (n, 3 + n)
                    }
                }
            } else {
                let word =
                    u64::from_le_bytes([block[0], block[1], block[2], block[3], block[4], 0, 0, 0])
                        as usize;
                let (bits, header_len) = match format {
                    0 | 1 => (10, 3),
                    2 => (14, 4),
                    _ => (18, 5),
                };
                let mask = (1 << bits) - 1;
                (
                    (word >> 4) & mask,
                    header_len + ((word >> (4 + bits)) & mask),
                )
            };
            let section = if literal_kind == 1 {
                section - regenerated + 1
            } else {
                section
            };
            anatomy.literals += regenerated;
            if literal_kind >= 2 {
                anatomy.entropy_coded_literals += regenerated;
            }
            let count = block[section] as usize;
            anatomy.sequences += match count {
                0..128 => count,
                128..255 => ((count - 128) << 8) + block[section + 1] as usize,
                _ => block[section + 1] as usize + ((block[section + 2] as usize) << 8) + 0x7f00,
            };
            at += size;
        } else {
            anatomy.raw_or_rle_blocks += 1;
            at += if kind == 1 { 1 } else { size };
        }
        if last {
            // The whole frame was consumed, so the fields above were read
            // from where the format puts them.
            assert_eq!(at, frame.len());
            break;
        }
    }
}

/// Prints both codecs' cost on the blocks of `records` at one target size.
fn report_dataset_blocks(dataset: &str, kib: usize, records: &[Owned]) {
    let run = build_run_with(Codec::None, kib << 10, records);
    let blocks: Vec<Vec<u8>> = frames(run.as_bytes())
        .iter()
        .map(|f| f.2.to_vec())
        .collect();
    report_codec_costs(&format!("{dataset} {kib}"), &blocks);
}

/// Prints compress and decompress cost per block for both codecs, and what
/// each Zstandard block is made of. Every block is timed a similar number of
/// times regardless of how many there are.
fn report_codec_costs(name: &str, blocks: &[Vec<u8>]) {
    let largest = blocks.iter().map(Vec::len).max().unwrap();
    let total: usize = blocks.iter().map(Vec::len).sum();
    let repeats = ((2 << 20) / total).max(1);
    let mut cctx = zstd_safe::CCtx::create();
    let mut dctx = zstd_safe::DCtx::create();
    let timed = |round: &mut dyn FnMut()| {
        median_ns_of(GEOMETRY_ROUNDS, blocks.len() * repeats, round) / 1000.0
    };

    let mut anatomy = ZstdAnatomy::default();
    let mut zstd_frames = Vec::new();
    let mut lz4_frames = Vec::new();
    for block in blocks {
        let mut out = vec![0; zstd_safe::compress_bound(block.len())];
        let n = cctx.compress(&mut out[..], block, 3).unwrap();
        out.truncate(n);
        zstd_anatomy(&out, &mut anatomy);
        zstd_frames.push(out);
        let mut out = vec![0; lz4::get_maximum_output_size(block.len())];
        let n = lz4::compress_into(block, &mut out).unwrap();
        out.truncate(n);
        lz4_frames.push(out);
    }
    let mut output =
        vec![0; zstd_safe::compress_bound(largest).max(lz4::get_maximum_output_size(largest))];
    let zstd_c = timed(&mut || {
        for _ in 0..repeats {
            for block in blocks {
                black_box(cctx.compress(&mut output[..], black_box(block), 3).unwrap());
            }
        }
    });
    let lz4_c = timed(&mut || {
        for _ in 0..repeats {
            for block in blocks {
                black_box(lz4::compress_into(black_box(block), &mut output).unwrap());
            }
        }
    });
    let zstd_d = timed(&mut || {
        for _ in 0..repeats {
            for (frame, block) in zstd_frames.iter().zip(blocks) {
                let out = &mut output[..block.len()];
                black_box(dctx.decompress(out, black_box(frame)).unwrap());
            }
        }
    });
    let lz4_d = timed(&mut || {
        for _ in 0..repeats {
            for (frame, block) in lz4_frames.iter().zip(blocks) {
                let out = &mut output[..block.len()];
                black_box(lz4::decompress_into(black_box(frame), out).unwrap());
            }
        }
    });
    let n = blocks.len() as f64;
    let size = |f: &[Vec<u8>]| f.iter().map(Vec::len).sum::<usize>() as f64 / total as f64;
    println!(
        "| {name} | {} | {:.3} | {zstd_c:.1} | {zstd_d:.2} | {:.3} | {lz4_c:.1} | {lz4_d:.2} | {:.0} | {:.0} | {:.0} | {}/{} |",
        total / blocks.len(),
        size(&zstd_frames),
        size(&lz4_frames),
        anatomy.sequences as f64 / n,
        anatomy.entropy_coded_literals as f64 / n,
        (anatomy.literals - anatomy.entropy_coded_literals) as f64 / n,
        anatomy.raw_or_rle_blocks,
        anatomy.blocks,
    );
}

/// Blocks that vary one property at a fixed decoded length, each case holding
/// many distinct blocks so the branch predictor cannot learn a single one.
fn controlled_blocks() {
    const LEN: usize = 64 << 10;
    const COUNT: usize = 16;

    let mut rng = Rng(11);
    let mut cases: Vec<(String, Vec<Vec<u8>>)> = Vec::new();
    cases.push((
        "random bytes".into(),
        (0..COUNT).map(|_| rng.bytes(LEN)).collect(),
    ));
    for bits in [2u32, 4, 6] {
        let blocks = (0..COUNT)
            .map(|_| {
                (0..LEN)
                    .map(|_| b'!' + (rng.next() % (1 << bits)) as u8)
                    .collect()
            })
            .collect();
        cases.push((format!("{bits}-bit symbols, no structure"), blocks));
    }
    for motif in [8usize, 16, 32, 64, 128, 256] {
        let blocks = (0..COUNT)
            .map(|_| {
                let motifs: Vec<Vec<u8>> = (0..64).map(|_| rng.bytes(motif)).collect();
                let mut block = Vec::with_capacity(LEN + motif);
                while block.len() < LEN {
                    block.extend_from_slice(&motifs[(rng.next() % 64) as usize]);
                }
                block.truncate(LEN);
                block
            })
            .collect();
        cases.push((format!("{motif}-byte random motifs"), blocks));
    }
    for (name, blocks) in &cases {
        report_codec_costs(name, blocks);
    }
}

fn geometry() {
    println!(
        "run geometry; {} MiB of logical data per dataset; {GEOMETRY_ROUNDS} rounds, {GEOMETRY_PROBES} uniform probes per round",
        GEOMETRY_BYTES >> 20
    );
    println!("every get verifies and decompresses its candidate block; there is no block cache");
    println!(
        "ns per record (build, validate, scan) or lookup; percentiles pool per-operation samples"
    );
    println!();
    println!(
        "| dataset | kib | codec | records | total | blocks | avg decoded | stored | headers | index | build | validate | scan | locate | checksum | decompress | search | get | p50 | p90 | p99 | p999 | miss | warm allocs | build peak |"
    );
    println!(
        "|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|"
    );
    let datasets: [(&str, Fixture); 5] = [
        ("json-like", json_like),
        ("small-value", small_values),
        ("structured", structured_keys),
        ("version-heavy", version_heavy),
        ("incompressible", random_keys),
    ];
    let mut anatomy: Vec<(&str, Vec<Owned>)> = Vec::new();
    for (name, make) in datasets {
        let records = make(GEOMETRY_BYTES);
        let mut keys: Vec<&[u8]> = records.iter().map(|r| r.key.as_slice()).collect();
        keys.dedup();
        let mut rng = Rng(123);
        let probes: Vec<&[u8]> = (0..GEOMETRY_PROBES)
            .map(|_| keys[(rng.next() % keys.len() as u64) as usize])
            .collect();
        for kib in GEOMETRY_KIB {
            for (codec, label) in CODECS {
                measure_geometry(name, kib, label, codec, &records, &probes);
            }
        }
        drop(probes);
        drop(keys);
        // A 2 MiB sample is enough to characterize a dataset's blocks.
        let mut sample = records;
        sample.truncate(sample.len() / 16);
        anatomy.push((name, sample));
    }

    const HEADER: &str = "| data | decoded | zstd ratio | zstd c | zstd d | lz4 ratio | lz4 c | lz4 d | sequences | entropy literals | other literals | raw blocks |";
    const RULE: &str = "|---|---|---|---|---|---|---|---|---|---|---|---|";

    println!();
    println!("codec cost on real blocks; us per block, then what a Zstandard block holds");
    println!();
    println!("{HEADER}");
    println!("{RULE}");
    for (name, records) in &anatomy {
        for kib in GEOMETRY_KIB {
            report_dataset_blocks(name, kib, records);
        }
    }
    println!();
    println!("controlled blocks of 64 KiB");
    println!();
    println!("{HEADER}");
    println!("{RULE}");
    controlled_blocks();
}

// Prefix scans over hierarchical keys.

/// A key `t0000:g00:s00:d000` of the given groups, subgroups and documents, so
/// each key level names a prefix of known size.
fn tree_key(doc: u64, (groups, subs, docs): (u64, u64, u64)) -> Vec<u8> {
    let (d, doc) = (doc % docs, doc / docs);
    let (s, doc) = (doc % subs, doc / subs);
    let (g, t) = (doc % groups, doc / groups);
    format!("t{t:04}:g{g:02}:s{s:02}:d{d:03}").into_bytes()
}

const TREE_KEY_LEN: usize = 18;
const TENANT_LEN: usize = 6;
const GROUP_LEN: usize = 10;
const SUB_LEN: usize = 14;
/// 4000 records per tenant, 200 per group and 20 per subgroup.
const FLAT_TREE: (u64, u64, u64) = (20, 10, 20);
/// With about three versions a document, the same shape as `FLAT_TREE`.
const VERSIONED_TREE: (u64, u64, u64) = (20, 8, 8);
/// Keys sorting before the tree, each with a chain that spans small blocks.
const CHAINS: u64 = 64;
const CHAIN_VERSIONS: u64 = 200;

fn tree_json(target: usize) -> Vec<Owned> {
    const PLANS: [&str; 4] = ["free", "team", "business", "enterprise"];
    const REGIONS: [&str; 3] = ["us-east", "eu-west", "ap-south"];

    let mut rng = Rng(21);
    fill(target, |i| {
        let value = format!(
            r#"{{"customer":{},"plan":"{}","active":{},"region":"{}","balance":{}}}"#,
            rng.next() % 100_000,
            PLANS[(rng.next() % 4) as usize],
            rng.next().is_multiple_of(2),
            REGIONS[(rng.next() % 3) as usize],
            rng.next() % 1_000_000,
        );
        vec![Owned {
            key: tree_key(i, FLAT_TREE),
            seq: BASE_SEQ + i,
            value: Some(value.into_bytes()),
        }]
    })
}

fn tree_small(target: usize) -> Vec<Owned> {
    let mut rng = Rng(22);
    fill(target, |i| {
        vec![Owned {
            key: tree_key(i, FLAT_TREE),
            seq: BASE_SEQ + i,
            value: Some(rng.bytes(8)),
        }]
    })
}

fn tree_versions(target: usize) -> Vec<Owned> {
    const STATE: [&str; 4] = ["created", "paid", "shipped", "delivered"];

    let mut rng = Rng(23);
    fill(target, |i| {
        let (key, versions) = if i < CHAINS {
            (format!("h{i:04}").into_bytes(), CHAIN_VERSIONS)
        } else {
            (tree_key(i - CHAINS, VERSIONED_TREE), 1 + rng.next() % 5)
        };
        let customer = rng.next() % 100_000;
        (0..versions)
            .map(|version| Owned {
                key: key.clone(),
                seq: BASE_SEQ + 1000 - version,
                value: (!rng.next().is_multiple_of(16)).then(|| {
                    format!(
                        r#"{{"customer":{customer},"state":"{}","updated":{}}}"#,
                        STATE[(rng.next() % 4) as usize],
                        1_700_000_000 + rng.next() % 86_400,
                    )
                    .into_bytes()
                }),
            })
            .collect()
    })
}

const PREFIX_KIB: [usize; 4] = [8, 16, 32, 64];
const PREFIX_ROUNDS: usize = 9;
const PREFIX_PROBES: usize = 400;

/// Runs one prefix query on a fresh cursor, consuming every record. A fresh
/// cursor shares no decoded block with the previous query.
fn scan_query(run: &Run<Vec<u8>>, workspace: &mut ReadWorkspace, prefix: &[u8]) -> usize {
    let mut cursor = run.cursor(workspace);
    cursor.seek_prefix(black_box(prefix)).unwrap();
    let (mut count, mut consumed) = (0, 0u64);
    while let Some(record) = cursor.next_record().unwrap() {
        count += 1;
        let value = match record.value {
            Value::Bytes(bytes) => bytes
                .first()
                .map_or(0, |&b| u64::from(b) + bytes.len() as u64),
            Value::Tombstone => 1,
        };
        let last = record.key.last().map_or(0, |&b| u64::from(b));
        consumed = consumed.wrapping_add(record.seq ^ last ^ value);
    }
    black_box(consumed);
    count
}

/// The records a prefix selects, by the reference ordering of the input.
fn matching(records: &[Owned], prefix: &[u8]) -> usize {
    let start = records.partition_point(|r| r.key.as_slice() < prefix);
    records[start..].partition_point(|r| r.key.starts_with(prefix))
}

/// One dataset's prefixes at each scale, the same for every configuration.
fn prefix_buckets(records: &[Owned]) -> Vec<(&'static str, Vec<Vec<u8>>)> {
    let mut keys: Vec<&[u8]> = records.iter().map(|r| r.key.as_slice()).collect();
    keys.dedup();
    let (chains, tree): (Vec<&[u8]>, Vec<&[u8]>) = keys
        .iter()
        .copied()
        .partition(|key| key.len() != TREE_KEY_LEN);
    let mut rng = Rng(31);
    let mut sample = |len: usize, keys: &[&[u8]], suffix: &[u8]| -> Vec<Vec<u8>> {
        let mut levels: Vec<&[u8]> = keys.iter().map(|key| &key[..len.min(key.len())]).collect();
        levels.dedup();
        (0..PREFIX_PROBES)
            .map(|_| {
                let level = levels[(rng.next() % levels.len() as u64) as usize];
                [level, suffix].concat()
            })
            .collect()
    };
    let mut buckets = vec![
        ("0 records", sample(SUB_LEN, &tree, b"~")),
        ("1-5 records", sample(TREE_KEY_LEN, &tree, b"")),
        ("10-25 records", sample(SUB_LEN, &tree, b"")),
        ("100-250 records", sample(GROUP_LEN, &tree, b"")),
        ("1000-5000 records", sample(TENANT_LEN, &tree, b"")),
    ];
    if !chains.is_empty() {
        buckets.push(("version chain", sample(usize::MAX, &chains, b"")));
    }
    buckets
}

/// Prefixes at block edges, which depend on the block target: the whole last
/// key of a block, whose scan must load the next block to end, and the prefix
/// of a subgroup that continues into that next block.
fn boundary_buckets(decoded: &[Vec<u8>]) -> Vec<(&'static str, Vec<Vec<u8>>)> {
    let mut last_keys: Vec<Vec<u8>> = Vec::with_capacity(decoded.len());
    let mut position = BlockPosition::new();
    for block in &decoded[..decoded.len().saturating_sub(1)] {
        let view = BlockView::open(block).unwrap();
        position.reset();
        let mut last = Vec::new();
        while let Some(record) = position.next_record(view).unwrap() {
            last.clear();
            last.extend_from_slice(record.key);
        }
        if last.len() == TREE_KEY_LEN {
            last_keys.push(last);
        }
    }
    let mut rng = Rng(32);
    let (mut ends, mut crossing) = (Vec::new(), Vec::new());
    for _ in 0..PREFIX_PROBES.min(last_keys.len()) {
        let key = &last_keys[(rng.next() % last_keys.len() as u64) as usize];
        ends.push(key.clone());
        crossing.push(key[..SUB_LEN].to_vec());
    }
    vec![("block end", ends), ("block crossing", crossing)]
}

fn measure_prefix_config(
    dataset: &str,
    kib: usize,
    label: &str,
    codec: Codec,
    records: &[Owned],
    buckets: &[(&'static str, Vec<Vec<u8>>)],
) {
    let run = build_run_with(codec, kib << 10, records);
    let bytes = run.as_bytes();
    let view = run.view();
    let stored = frames(bytes);
    let timed = |ops, round: &mut dyn FnMut()| median_ns_of(PREFIX_ROUNDS, ops, round);

    let mut zstd = zstd_safe::DCtx::create();
    let decoded: Vec<Vec<u8>> = stored
        .iter()
        .map(|(id, len, payload, _)| {
            let mut out = vec![0; *len];
            decode_frame(&mut zstd, *id, payload, &mut out);
            out
        })
        .collect();
    let largest = stored.iter().map(|f| f.1).max().unwrap_or(0);
    let mut out = vec![0; largest];
    let mut workspace = ReadWorkspace::new();

    let boundaries = boundary_buckets(&decoded);
    for (bucket, probes) in buckets.iter().chain(&boundaries) {
        // The cost of each phase of a seek, on its own over the same probes.
        let candidates: Vec<(&[u8], usize)> = probes
            .iter()
            .filter_map(|prefix| Some((prefix.as_slice(), view.locate(prefix).unwrap()?)))
            .collect();
        let locate_ns = timed(probes.len(), &mut || {
            for prefix in probes {
                black_box(view.locate(black_box(prefix)).unwrap());
            }
        });
        let decode_ns = if codec == Codec::None {
            0.0
        } else {
            timed(probes.len(), &mut || {
                for &(_, block) in &candidates {
                    let (id, len, payload, header) = stored[block];
                    let mut hasher = Hasher::new();
                    hasher.update(black_box(header));
                    hasher.update(black_box(payload));
                    black_box(hasher.finalize());
                    decode_frame(&mut zstd, id, payload, &mut out[..len]);
                    black_box(&out);
                }
            })
        };
        let mut position = BlockPosition::new();
        let block_seek_ns = timed(probes.len(), &mut || {
            for &(prefix, block) in &candidates {
                let view = BlockView::open(&decoded[block]).unwrap();
                position.seek(view, black_box(prefix)).unwrap();
                black_box(&position);
            }
        });

        let seek_ns = timed(probes.len(), &mut || {
            for prefix in probes {
                let mut cursor = run.cursor(&mut workspace);
                cursor.seek_prefix(black_box(prefix)).unwrap();
                black_box(&mut cursor);
            }
        });

        // A checked pass: the reader must return what the input records hold.
        let expected: usize = probes.iter().map(|prefix| matching(records, prefix)).sum();
        let before = workspace.blocks_loaded();
        let returned: usize = probes
            .iter()
            .map(|prefix| scan_query(&run, &mut workspace, prefix))
            .sum();
        assert_eq!(returned, expected, "{dataset} {kib} {label} {bucket}");
        let loads = workspace.blocks_loaded() - before;

        let scan_ns = timed(probes.len(), &mut || {
            for prefix in probes {
                black_box(scan_query(&run, &mut workspace, prefix));
            }
        });
        let mut latencies = Vec::with_capacity(PREFIX_ROUNDS * probes.len());
        for _ in 0..PREFIX_ROUNDS {
            for prefix in probes {
                let start = Instant::now();
                black_box(scan_query(&run, &mut workspace, prefix));
                latencies.push(start.elapsed().as_nanos() as u64);
            }
        }
        latencies.sort_unstable();
        let percentile = |p: f64| latencies[((latencies.len() - 1) as f64 * p) as usize];
        let warm_allocs = allocations_during(|| {
            for prefix in probes {
                black_box(scan_query(&run, &mut workspace, prefix));
            }
        });

        let count = expected as f64 / probes.len() as f64;
        let (ns_per_record, records_per_second) = if expected == 0 {
            (0.0, 0.0)
        } else {
            (scan_ns / count, count / scan_ns * 1e9)
        };
        println!(
            "| {dataset} | {kib} | {label} | {bucket} | {count:.1} | {:.2} | {locate_ns:.0} | {decode_ns:.0} | {block_seek_ns:.0} | {seek_ns:.0} | {scan_ns:.0} | {} | {} | {ns_per_record:.1} | {records_per_second:.0} | {warm_allocs} |",
            loads as f64 / probes.len() as f64,
            percentile(0.5),
            percentile(0.99),
        );
    }
}

fn prefix_summary(dataset: &str, kib: usize, label: &str, codec: Codec, records: &[Owned]) {
    let run = build_run_with(codec, kib << 10, records);
    let logical: usize = records
        .iter()
        .map(|r| r.key.len() + r.value.as_ref().map_or(0, Vec::len))
        .sum();
    let mut workspace = ReadWorkspace::new();
    let scan_ns = median_ns_of(PREFIX_ROUNDS, records.len(), || {
        assert_eq!(scan_query(&run, &mut workspace, b""), records.len());
    });
    println!(
        "| {dataset} | {kib} | {label} | {} | {} | {:.1} | {} | {} | {} | {scan_ns:.1} | {:.0} | {} |",
        records.len(),
        run.as_bytes().len(),
        run.as_bytes().len() as f64 / records.len() as f64,
        run.block_count(),
        run.view().index_len() + 28,
        12 * run.block_count(),
        logical as f64 / records.len() as f64 / scan_ns * 1000.0,
        workspace.capacity(),
    );
}

fn prefix_scans() {
    println!(
        "prefix scans; {} MiB of logical data per dataset; {PREFIX_ROUNDS} rounds, {PREFIX_PROBES} seeded prefixes per round",
        GEOMETRY_BYTES >> 20
    );
    println!("every query uses a new cursor, so it decodes its own blocks and reuses none");
    println!("times are ns per query; records is the mean returned per query");
    println!();
    let datasets: [(&str, Fixture); 3] = [
        ("json-tree", tree_json),
        ("small-tree", tree_small),
        ("version-tree", tree_versions),
    ];
    println!(
        "| dataset | kib | codec | records | total | bytes per record | blocks | index | headers | full scan ns per record | full scan MiB/s | workspace bytes |"
    );
    println!("|---|---|---|---|---|---|---|---|---|---|---|---|");
    let mut cached = Vec::new();
    for (name, make) in datasets {
        let records = make(GEOMETRY_BYTES);
        for kib in PREFIX_KIB {
            for (codec, label) in CODECS {
                prefix_summary(name, kib, label, codec, &records);
            }
        }
        cached.push((name, records));
    }
    println!();
    println!(
        "| dataset | kib | codec | bucket | records | blocks | locate | first decode | block seek | seek | scan | p50 | p99 | ns per record | records/s | warm allocs |"
    );
    println!("|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|");
    for (name, records) in &cached {
        let buckets = prefix_buckets(records);
        for kib in PREFIX_KIB {
            for (codec, label) in CODECS {
                measure_prefix_config(name, kib, label, codec, records, &buckets);
            }
        }
    }
}

const MERGE_FANS: [usize; 5] = [1, 2, 4, 8, 16];
const MERGE_ROUNDS: usize = 9;
const MERGE_PROBES: usize = 200;
const MERGE_VARIANT_FAN: usize = 8;
const ZSTD_16: RunShape = (Codec::Zstd, 16);

/// One input's codec and block target in KiB.
type RunShape = (Codec, usize);
/// Which input receives a record: `(index, fan, total)`.
type Assign = fn(usize, usize, usize) -> usize;

/// Every input spans the whole key space and each neighbor comes from another
/// input, the worst case for the heap. Version chains are split across runs.
fn assign_overlapping(index: usize, fan: usize, _: usize) -> usize {
    index % fan
}

/// Inputs hold contiguous ranges, as a leveled store's runs would.
fn assign_disjoint(index: usize, fan: usize, total: usize) -> usize {
    index * fan / total
}

fn build_inputs(
    records: &[Owned],
    fan: usize,
    assign: Assign,
    shapes: &[RunShape],
) -> Vec<Run<Vec<u8>>> {
    let mut builders: Vec<RunBuilder> = (0..fan)
        .map(|input| {
            let (codec, kib) = shapes[input % shapes.len()];
            RunBuilder::with_target_block_len(codec, kib << 10)
        })
        .collect();
    for (index, record) in records.iter().enumerate() {
        builders[assign(index, fan, records.len())]
            .add(&record.key, record.seq, record.value())
            .unwrap();
    }
    builders.into_iter().map(RunBuilder::finish).collect()
}

fn digest(record: Record<'_>) -> u64 {
    let value = match record.value {
        Value::Bytes(bytes) => bytes
            .first()
            .map_or(0, |&b| u64::from(b) + bytes.len() as u64),
        Value::Tombstone => 1,
    };
    record.seq ^ record.key.last().map_or(0, |&b| u64::from(b)) ^ value
}

fn drain_merge(cursor: &mut MergeCursor<'_>) -> usize {
    let (mut count, mut consumed) = (0, 0u64);
    while let Some(record) = cursor.next_record().unwrap() {
        count += 1;
        consumed = consumed.wrapping_add(digest(record));
    }
    black_box(consumed);
    count
}

fn drain_run(cursor: &mut RunCursor<'_>) -> usize {
    let (mut count, mut consumed) = (0, 0u64);
    while let Some(record) = cursor.next_record().unwrap() {
        count += 1;
        consumed = consumed.wrapping_add(digest(record));
    }
    black_box(consumed);
    count
}

fn merged<R>(
    runs: &[Run<Vec<u8>>],
    workspace: &mut MergeWorkspace,
    operation: impl FnOnce(&mut MergeCursor<'_>) -> R,
) -> R {
    operation(&mut MergeCursor::new(runs.iter().map(Run::view), workspace))
}

/// Seeded keys that exist in the data, the same for every configuration.
fn seek_probes(records: &[Owned]) -> Vec<&[u8]> {
    let mut rng = Rng(47);
    (0..MERGE_PROBES)
        .map(|_| {
            records[(rng.next() % records.len() as u64) as usize]
                .key
                .as_slice()
        })
        .collect()
}

struct MergeConfig<'a> {
    dataset: &'a str,
    layout: &'a str,
    shape: &'a str,
    fan: usize,
    assign: Assign,
    shapes: &'a [RunShape],
}

/// Measures one configuration: the whole stream, then seeks, then prefixes.
fn measure_merge(
    config: &MergeConfig<'_>,
    records: &[Owned],
    buckets: &[(&'static str, Vec<Vec<u8>>)],
    prefix_rows: &mut Vec<String>,
) {
    let MergeConfig {
        dataset,
        layout,
        shape,
        fan,
        assign,
        shapes,
    } = *config;
    let runs = build_inputs(records, fan, assign, shapes);
    let stored: usize = runs.iter().map(|run| run.as_bytes().len()).sum();
    let blocks: usize = runs.iter().map(Run::block_count).sum();
    let timed = |ops, round: &mut dyn FnMut()| median_ns_of(MERGE_ROUNDS, ops, round);
    let logical: usize = records
        .iter()
        .map(|r| r.key.len() + r.value.as_ref().map_or(0, Vec::len) + 9)
        .sum();

    // A checked pass from a cold workspace: the stream must be exactly the
    // input records in order, which also gives what the merge retains.
    let live = LIVE.load(Ordering::Relaxed);
    let mut workspace = MergeWorkspace::new();
    let retained = merged(&runs, &mut workspace, |cursor| {
        let mut expected = records.iter();
        while let Some(record) = cursor.next_record().unwrap() {
            let want = expected.next().expect("no extra records");
            assert_eq!((record.key, record.seq), (want.key.as_slice(), want.seq));
            assert_eq!(record.value, want.value());
        }
        assert!(expected.next().is_none(), "no missing records");
        LIVE.load(Ordering::Relaxed) - live
    });
    let decoded = workspace.capacity();
    assert_eq!(workspace.blocks_loaded(), blocks as u64);

    let mut single = ReadWorkspace::new();
    let bare_ns = timed(records.len(), &mut || {
        let mut total = 0;
        for run in &runs {
            total += drain_run(&mut run.cursor(&mut single));
        }
        assert_eq!(total, records.len());
    });
    let merge_ns = merged(&runs, &mut workspace, |cursor| {
        timed(records.len(), &mut || {
            cursor.seek(b"").unwrap();
            assert_eq!(drain_merge(cursor), records.len());
        })
    });
    let scan_allocs = merged(&runs, &mut workspace, |cursor| {
        cursor.seek(b"").unwrap();
        allocations_during(|| {
            cursor.seek(b"").unwrap();
            black_box(drain_merge(cursor));
        })
    });
    let new_allocs = allocations_during(|| {
        merged(&runs, &mut workspace, |cursor| {
            black_box(cursor);
        });
    });

    let seeks = seek_probes(records);
    let seek_ns = merged(&runs, &mut workspace, |cursor| {
        timed(seeks.len(), &mut || {
            for key in &seeks {
                cursor.seek(black_box(key)).unwrap();
                black_box(&mut *cursor);
            }
        })
    });
    let before = workspace.blocks_loaded();
    let seek_allocs = merged(&runs, &mut workspace, |cursor| {
        allocations_during(|| {
            for key in &seeks {
                cursor.seek(black_box(key)).unwrap();
            }
        })
    });
    let seek_blocks = (workspace.blocks_loaded() - before) as f64 / seeks.len() as f64;

    println!(
        "| {dataset} | {layout} | {fan} | {shape} | {} | {:.1} | {blocks} | {bare_ns:.1} | {merge_ns:.1} | {:.2} | {:.0} | {seek_ns:.0} | {seek_blocks:.2} | {new_allocs} | {seek_allocs} | {scan_allocs} | {decoded} | {} |",
        records.len(),
        stored as f64 / (1 << 20) as f64,
        merge_ns / bare_ns,
        (logical as f64 / (1 << 20) as f64) / (merge_ns * records.len() as f64 / 1e9),
        retained - decoded as u64,
    );

    for (bucket, all_probes) in buckets {
        let probes = &all_probes[..MERGE_PROBES];
        let expected: usize = probes.iter().map(|prefix| matching(records, prefix)).sum();
        let before = workspace.blocks_loaded();
        let returned = merged(&runs, &mut workspace, |cursor| {
            let mut returned = 0;
            for prefix in probes {
                cursor.seek_prefix(prefix).unwrap();
                returned += drain_merge(cursor);
            }
            returned
        });
        assert_eq!(
            returned, expected,
            "{dataset} {layout} {fan} {shape} {bucket}"
        );
        let loads = workspace.blocks_loaded() - before;

        let (prefix_seek_ns, scan_ns, query_allocs, mut latencies) =
            merged(&runs, &mut workspace, |cursor| {
                let seek = timed(probes.len(), &mut || {
                    for prefix in probes {
                        cursor.seek_prefix(black_box(prefix)).unwrap();
                        black_box(&mut *cursor);
                    }
                });
                let scan = timed(probes.len(), &mut || {
                    for prefix in probes {
                        cursor.seek_prefix(black_box(prefix)).unwrap();
                        black_box(drain_merge(cursor));
                    }
                });
                let allocs = allocations_during(|| {
                    for prefix in probes {
                        cursor.seek_prefix(black_box(prefix)).unwrap();
                        black_box(drain_merge(cursor));
                    }
                });
                let mut latencies = Vec::with_capacity(MERGE_ROUNDS * probes.len());
                for _ in 0..MERGE_ROUNDS {
                    for prefix in probes {
                        let start = Instant::now();
                        cursor.seek_prefix(black_box(prefix)).unwrap();
                        black_box(drain_merge(cursor));
                        latencies.push(start.elapsed().as_nanos() as u64);
                    }
                }
                (seek, scan, allocs, latencies)
            });
        latencies.sort_unstable();
        let percentile = |p: f64| latencies[((latencies.len() - 1) as f64 * p) as usize];
        let bare_query_ns = timed(probes.len(), &mut || {
            for prefix in probes {
                for run in &runs {
                    let mut cursor = run.cursor(&mut single);
                    cursor.seek_prefix(black_box(prefix)).unwrap();
                    black_box(drain_run(&mut cursor));
                }
            }
        });
        let count = expected as f64 / probes.len() as f64;
        let ns_per_record = if expected == 0 { 0.0 } else { scan_ns / count };
        prefix_rows.push(format!(
            "| {dataset} | {layout} | {fan} | {shape} | {bucket} | {count:.1} | {:.2} | {prefix_seek_ns:.0} | {scan_ns:.0} | {bare_query_ns:.0} | {:.2} | {} | {} | {ns_per_record:.1} | {query_allocs} |",
            loads as f64 / probes.len() as f64,
            scan_ns / bare_query_ns,
            percentile(0.5),
            percentile(0.99),
        ));
    }
}

fn merge_scans() {
    println!(
        "merged iteration; {} MiB of logical data per dataset split across the inputs; {MERGE_ROUNDS} rounds, {MERGE_PROBES} seeded probes per round",
        GEOMETRY_BYTES >> 20
    );
    println!("bare = each input iterated alone; ratio = merged / bare");
    println!(
        "cursors are reused and repositioned; decoded = block buffers retained; state = everything else the merge retains on the Rust heap"
    );

    // Zstandard's contexts live on the C heap, outside the counting allocator.
    let sample = tree_json(1 << 20);
    let run = build_run_with(Codec::Zstd, 16 << 10, &sample);
    let mut context = zstd_safe::DCtx::create();
    let (id, len, payload, _) = frames(run.as_bytes())[0];
    decode_frame(&mut context, id, payload, &mut vec![0; len]);
    println!(
        "one Zstandard context per Zstandard input: {} bytes on the C heap",
        context.sizeof()
    );
    println!();

    let datasets: [(&str, Fixture); 2] =
        [("json-tree", tree_json), ("version-tree", tree_versions)];
    let layouts: [(&str, Assign); 2] = [
        ("overlap", assign_overlapping),
        ("disjoint", assign_disjoint),
    ];
    let variants: [(&str, &[RunShape]); 6] = [
        ("none-16", &[(Codec::None, 16)]),
        ("lz4-16", &[(Codec::Lz4, 16)]),
        ("zstd-64", &[(Codec::Zstd, 64)]),
        (
            "mixed-codecs-16",
            &[(Codec::None, 16), (Codec::Lz4, 16), (Codec::Zstd, 16)],
        ),
        (
            "zstd-mixed-4-16-64",
            &[(Codec::Zstd, 4), (Codec::Zstd, 16), (Codec::Zstd, 64)],
        ),
        (
            "mixed-both",
            &[(Codec::None, 64), (Codec::Lz4, 16), (Codec::Zstd, 4)],
        ),
    ];

    println!(
        "| dataset | layout | runs | shape | records | stored MiB | blocks | bare ns/record | merged ns/record | ratio | MiB/s | seek ns | blocks per seek | allocs new | allocs seeks | allocs scan | decoded bytes | state bytes |"
    );
    println!("|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|");
    let mut prefix_rows = Vec::new();
    for (dataset, make) in datasets {
        let records = make(GEOMETRY_BYTES);
        let buckets = prefix_buckets(&records);
        for (layout, assign) in layouts {
            for fan in MERGE_FANS {
                measure_merge(
                    &MergeConfig {
                        dataset,
                        layout,
                        shape: "zstd-16",
                        fan,
                        assign,
                        shapes: &[ZSTD_16],
                    },
                    &records,
                    &buckets,
                    &mut prefix_rows,
                );
            }
        }
        for (shape, shapes) in variants {
            measure_merge(
                &MergeConfig {
                    dataset,
                    layout: "overlap",
                    shape,
                    fan: MERGE_VARIANT_FAN,
                    assign: assign_overlapping,
                    shapes,
                },
                &records,
                &buckets,
                &mut prefix_rows,
            );
        }
    }
    println!();
    println!(
        "| dataset | layout | runs | shape | bucket | records | blocks | seek | scan | bare scan | ratio | p50 | p99 | ns per record | allocs |"
    );
    println!("|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|");
    for row in &prefix_rows {
        println!("{row}");
    }
}

const LOGICAL_FANS: [usize; 4] = [1, 4, 8, 16];

/// Half the records stay in contiguous ranges and half are dealt evenly, so
/// inputs overlap only partly.
fn assign_partial(index: usize, fan: usize, total: usize) -> usize {
    if index.is_multiple_of(2) {
        assign_disjoint(index, fan, total)
    } else {
        assign_overlapping(index, fan, total)
    }
}

/// One key in four has been deleted, keeping the value the tombstone hides.
fn tree_tombstones(target: usize) -> Vec<Owned> {
    let mut rng = Rng(24);
    fill(target, |i| {
        let key = tree_key(i, FLAT_TREE);
        let value = format!(r#"{{"id":{i},"payload":"{:016x}"}}"#, rng.next()).into_bytes();
        let old = Owned {
            key: key.clone(),
            seq: BASE_SEQ + 1000,
            value: Some(value),
        };
        if i.is_multiple_of(4) {
            vec![
                Owned {
                    key,
                    seq: BASE_SEQ + 2000,
                    value: None,
                },
                old,
            ]
        } else {
            vec![old]
        }
    })
}

/// The latest live record per key, from sorted records alone.
fn latest_live(records: &[Owned]) -> (Vec<&Owned>, Vec<&Owned>) {
    let (mut live, mut deleted) = (Vec::new(), Vec::new());
    let mut previous: Option<&[u8]> = None;
    for record in records {
        if previous != Some(record.key.as_slice()) {
            if record.value.is_some() {
                live.push(record);
            } else {
                deleted.push(record);
            }
        }
        previous = Some(&record.key);
    }
    (live, deleted)
}

fn drain_logical(cursor: &mut LogicalCursor<'_>) -> usize {
    let (mut count, mut consumed) = (0, 0u64);
    while let Some(record) = cursor.next_record().unwrap() {
        count += 1;
        let last = record.key.last().map_or(0, |&b| u64::from(b));
        let value = record.value.first().map_or(0, |&b| u64::from(b));
        consumed = consumed.wrapping_add(record.seq ^ last ^ value);
    }
    black_box(consumed);
    count
}

/// Each input's first and last key, from which a range filter would work.
fn input_bounds<'r>(records: &'r [Owned], fan: usize, assign: Assign) -> Vec<(&'r [u8], &'r [u8])> {
    let mut bounds: Vec<Option<(&'r [u8], &'r [u8])>> = vec![None; fan];
    for (index, record) in records.iter().enumerate() {
        let slot = &mut bounds[assign(index, fan, records.len())];
        let first = slot.map_or(record.key.as_slice(), |(first, _)| first);
        *slot = Some((first, &record.key));
    }
    bounds.into_iter().map(Option::unwrap).collect()
}

/// The views a filter holding each run's key range would still consult, for
/// comparison only: no such metadata exists.
fn ranged<'v>(
    views: &[RunView<'v>],
    bounds: &[(&[u8], &[u8])],
    overlaps: impl Fn(&[u8], &[u8]) -> bool,
) -> Vec<RunView<'v>> {
    views
        .iter()
        .zip(bounds)
        .filter(|(_, (first, last))| overlaps(first, last))
        .map(|(view, _)| *view)
        .collect()
}

struct LogicalTables {
    point: Vec<String>,
    prefix: Vec<String>,
}

fn measure_logical(
    config: &MergeConfig<'_>,
    records: &[Owned],
    buckets: &[(&'static str, Vec<Vec<u8>>)],
    tables: &mut LogicalTables,
) {
    let MergeConfig {
        dataset,
        layout,
        shape,
        fan,
        assign,
        shapes,
    } = *config;
    let timed = |ops, round: &mut dyn FnMut()| median_ns_of(MERGE_ROUNDS, ops, round);
    let runs = build_inputs(records, fan, assign, shapes);
    let views: Vec<RunView<'_>> = runs.iter().map(Run::view).collect();
    let set = RunSet::new(&views);
    let bounds = input_bounds(records, fan, assign);
    let (live, deleted) = latest_live(records);

    // A checked pass from a cold workspace: the stream must be exactly the
    // latest live records, which also gives what the reader retains.
    let before_live = LIVE.load(Ordering::Relaxed);
    let mut workspace = LogicalWorkspace::new();
    let retained = {
        let mut cursor = set.cursor(&mut workspace);
        let mut expected = live.iter();
        while let Some(record) = cursor.next_record().unwrap() {
            let want = expected.next().expect("no extra keys");
            assert_eq!((record.key, record.seq), (want.key.as_slice(), want.seq));
            assert_eq!(Some(record.value), want.value.as_deref());
        }
        assert!(expected.next().is_none(), "no missing keys");
        LIVE.load(Ordering::Relaxed) - before_live
    };
    let decoded = workspace.capacity();

    let physical = records.len();
    let mut single = ReadWorkspace::new();
    let bare_ns = timed(physical, &mut || {
        let total: usize = runs
            .iter()
            .map(|run| drain_run(&mut run.cursor(&mut single)))
            .sum();
        assert_eq!(total, physical);
    });
    let mut merge_workspace = MergeWorkspace::new();
    let merge_ns = merged(&runs, &mut merge_workspace, |cursor| {
        timed(physical, &mut || {
            cursor.seek(b"").unwrap();
            assert_eq!(drain_merge(cursor), physical);
        })
    });
    let (logical_ns, scan_allocs) = {
        let mut cursor = set.cursor(&mut workspace);
        let ns = timed(physical, &mut || {
            cursor.seek(b"").unwrap();
            assert_eq!(drain_logical(&mut cursor), live.len());
        });
        let allocs = allocations_during(|| {
            cursor.seek(b"").unwrap();
            black_box(drain_logical(&mut cursor));
        });
        (ns, allocs)
    };
    let live_ns = logical_ns * physical as f64 / live.len() as f64;
    println!(
        "| {dataset} | {layout} | {fan} | {shape} | {physical} | {} | {:.2} | {bare_ns:.1} | {merge_ns:.1} | {logical_ns:.1} | {:.1} | {live_ns:.1} | {:.0} | {scan_allocs} | {decoded} | {} |",
        live.len(),
        physical as f64 / live.len() as f64,
        logical_ns - merge_ns,
        1e9 / live_ns,
        retained - decoded as u64,
    );

    // Point lookups, including what a per-run key range would save.
    let mut rng = Rng(53);
    let mut pick = |from: &[&Owned]| -> Vec<Vec<u8>> {
        (0..MERGE_PROBES)
            .map(|_| from[(rng.next() % from.len() as u64) as usize].key.clone())
            .collect()
    };
    let hits = pick(&live);
    let misses: Vec<Vec<u8>> = pick(&live)
        .into_iter()
        .map(|key| [key.as_slice(), b"~"].concat())
        .collect();
    let mut kinds = vec![("hit", hits, true), ("miss", misses, false)];
    if !deleted.is_empty() {
        kinds.push(("deleted", pick(&deleted), false));
    }
    for (kind, probes, present) in &kinds {
        for key in probes {
            assert_eq!(set.get(key, &mut workspace).unwrap().is_some(), *present);
        }
        let before = workspace.blocks_loaded();
        for key in probes {
            black_box(set.get(key, &mut workspace).unwrap());
        }
        let blocks = (workspace.blocks_loaded() - before) as f64 / probes.len() as f64;
        let ns = timed(probes.len(), &mut || {
            for key in probes {
                black_box(set.get(black_box(key), &mut workspace).unwrap());
            }
        });
        let allocs = allocations_during(|| {
            for key in probes {
                black_box(set.get(key, &mut workspace).unwrap());
            }
        });
        let narrowed: Vec<Vec<RunView<'_>>> = probes
            .iter()
            .map(|key| {
                ranged(&views, &bounds, |first, last| {
                    first <= key.as_slice() && key.as_slice() <= last
                })
            })
            .collect();
        let before = workspace.blocks_loaded();
        for (key, runs) in probes.iter().zip(&narrowed) {
            black_box(RunSet::new(runs).get(key, &mut workspace).unwrap());
        }
        let ranged_blocks = (workspace.blocks_loaded() - before) as f64 / probes.len() as f64;
        let ranged_ns = timed(probes.len(), &mut || {
            for (key, runs) in probes.iter().zip(&narrowed) {
                black_box(
                    RunSet::new(runs)
                        .get(black_box(key), &mut workspace)
                        .unwrap(),
                );
            }
        });
        tables.point.push(format!(
            "| {dataset} | {layout} | {fan} | {shape} | {kind} | {ns:.0} | {blocks:.2} | {ranged_ns:.0} | {ranged_blocks:.2} | {allocs} |"
        ));
    }

    let matching_live = |prefix: &[u8]| {
        let start = live.partition_point(|r| r.key.as_slice() < prefix);
        live[start..].partition_point(|r| r.key.starts_with(prefix))
    };
    for (bucket, all_probes) in buckets {
        let probes = &all_probes[..MERGE_PROBES];
        let visited: usize = probes.iter().map(|prefix| matching(records, prefix)).sum();
        let expected: usize = probes.iter().map(|prefix| matching_live(prefix)).sum();
        let before = workspace.blocks_loaded();
        let (scan_ns, allocs, mut latencies) = {
            let mut cursor = set.cursor(&mut workspace);
            let mut returned = 0;
            for prefix in probes {
                cursor.seek_prefix(prefix).unwrap();
                returned += drain_logical(&mut cursor);
            }
            assert_eq!(returned, expected, "{dataset} {layout} {fan} {bucket}");
            let scan_ns = timed(probes.len(), &mut || {
                for prefix in probes {
                    cursor.seek_prefix(black_box(prefix)).unwrap();
                    black_box(drain_logical(&mut cursor));
                }
            });
            let allocs = allocations_during(|| {
                for prefix in probes {
                    cursor.seek_prefix(black_box(prefix)).unwrap();
                    black_box(drain_logical(&mut cursor));
                }
            });
            let mut latencies = Vec::with_capacity(MERGE_ROUNDS * probes.len());
            for _ in 0..MERGE_ROUNDS {
                for prefix in probes {
                    let start = Instant::now();
                    cursor.seek_prefix(black_box(prefix)).unwrap();
                    black_box(drain_logical(&mut cursor));
                    latencies.push(start.elapsed().as_nanos() as u64);
                }
            }
            (scan_ns, allocs, latencies)
        };
        // One checked pass of the loop above loaded these; the timed passes
        // repeat it, so divide by all of them.
        let passes = MERGE_ROUNDS + MERGE_ROUNDS + 2;
        let blocks = (workspace.blocks_loaded() - before) as f64 / (probes.len() * passes) as f64;
        latencies.sort_unstable();
        let percentile = |p: f64| latencies[((latencies.len() - 1) as f64 * p) as usize];
        let physical_ns = merged(&runs, &mut merge_workspace, |cursor| {
            timed(probes.len(), &mut || {
                for prefix in probes {
                    cursor.seek_prefix(black_box(prefix)).unwrap();
                    black_box(drain_merge(cursor));
                }
            })
        });
        let narrowed: Vec<Vec<RunView<'_>>> = probes
            .iter()
            .map(|prefix| {
                ranged(&views, &bounds, |first, last| {
                    last >= prefix.as_slice()
                        && (first <= prefix.as_slice() || first.starts_with(prefix))
                })
            })
            .collect();
        let before = workspace.blocks_loaded();
        for (prefix, runs) in probes.iter().zip(&narrowed) {
            let mut cursor = RunSet::new(runs).cursor(&mut workspace);
            cursor.seek_prefix(prefix).unwrap();
            black_box(drain_logical(&mut cursor));
        }
        let ranged_blocks = (workspace.blocks_loaded() - before) as f64 / probes.len() as f64;
        let ranged_ns = timed(probes.len(), &mut || {
            for (prefix, runs) in probes.iter().zip(&narrowed) {
                let mut cursor = RunSet::new(runs).cursor(&mut workspace);
                cursor.seek_prefix(black_box(prefix)).unwrap();
                black_box(drain_logical(&mut cursor));
            }
        });
        let queries = probes.len() as f64;
        tables.prefix.push(format!(
            "| {dataset} | {layout} | {fan} | {shape} | {bucket} | {:.1} | {:.1} | {blocks:.2} | {physical_ns:.0} | {scan_ns:.0} | {:.2} | {} | {} | {ranged_ns:.0} | {ranged_blocks:.2} | {allocs} |",
            expected as f64 / queries,
            visited as f64 / queries,
            scan_ns / physical_ns,
            percentile(0.5),
            percentile(0.99),
        ));
    }
}

fn logical_reads() {
    println!(
        "latest-state reads; {} MiB of logical data per dataset split across the inputs; {MERGE_ROUNDS} rounds, {MERGE_PROBES} seeded probes per round",
        GEOMETRY_BYTES >> 20
    );
    println!(
        "bare = each input iterated alone; merge = physical merge; logical = latest-state scan; times per physical record unless stated"
    );
    println!(
        "ranged = the same reads consulting only runs whose key range could hold the target, a comparison with no implementation behind it"
    );
    println!();
    let datasets: [(&str, Fixture); 3] = [
        ("unique", tree_json),
        ("versions", tree_versions),
        ("tombstones", tree_tombstones),
    ];
    let layouts: [(&str, Assign); 3] = [
        ("disjoint", assign_disjoint),
        ("partial", assign_partial),
        ("overlap", assign_overlapping),
    ];
    let mixed: &[RunShape] = &[(Codec::None, 64), (Codec::Lz4, 16), (Codec::Zstd, 4)];

    println!(
        "| dataset | layout | runs | shape | physical | live | physical per live | bare | merge | logical | filter | ns per live | live per s | allocs scan | decoded bytes | state bytes |"
    );
    println!("|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|");
    let mut tables = LogicalTables {
        point: Vec::new(),
        prefix: Vec::new(),
    };
    for (dataset, make) in datasets {
        let records = make(GEOMETRY_BYTES);
        let buckets = prefix_buckets(&records);
        for (layout, assign) in layouts {
            for fan in LOGICAL_FANS {
                measure_logical(
                    &MergeConfig {
                        dataset,
                        layout,
                        shape: "zstd-16",
                        fan,
                        assign,
                        shapes: &[ZSTD_16],
                    },
                    &records,
                    &buckets,
                    &mut tables,
                );
            }
        }
        measure_logical(
            &MergeConfig {
                dataset,
                layout: "overlap",
                shape: "mixed",
                fan: MERGE_VARIANT_FAN,
                assign: assign_overlapping,
                shapes: mixed,
            },
            &records,
            &buckets,
            &mut tables,
        );
    }
    println!();
    println!(
        "| dataset | layout | runs | shape | lookup | ns | blocks | ranged ns | ranged blocks | allocs |"
    );
    println!("|---|---|---|---|---|---|---|---|---|---|");
    for row in &tables.point {
        println!("{row}");
    }
    println!();
    println!(
        "| dataset | layout | runs | shape | bucket | live | physical | blocks | physical scan | logical scan | ratio | p50 | p99 | ranged scan | ranged blocks | allocs |"
    );
    println!("|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|");
    for row in &tables.prefix {
        println!("{row}");
    }
}

fn main() {
    match std::env::args().nth(1).as_deref() {
        Some("geometry") => return geometry(),
        Some("prefix") => return prefix_scans(),
        Some("merge") => return merge_scans(),
        Some("logical") => return logical_reads(),
        _ => {}
    }

    println!("median of {ROUNDS} rounds; ns per record (build, validate, scan) or per lookup");
    println!(
        "raw = key + value + 8 byte seq + 1 byte kind; key+meta = (encoded - value bytes) / record"
    );
    println!("allocs = build/validate/scan/all lookups, each counted once over the whole block");
    println!();
    println!(
        "| dataset | records | encoded | raw | encoded/raw | key+meta B | build | validate | scan | get hit | get miss | allocs |"
    );
    println!("|---|---|---|---|---|---|---|---|---|---|---|---|");
    for kib in [4, 32, 256] {
        let target = kib << 10;
        measure(&format!("structured {kib}K"), &structured_keys(target));
        measure(&format!("random {kib}K"), &random_keys(target));
        measure(&format!("small-value {kib}K"), &small_values(target));
        measure(&format!("multi-version {kib}K"), &multi_version(target));
    }

    println!();
    println!("reader paths; scratch = memory copy standing in for decompression, then lookup");
    println!("allocs = scratch lookups/warm position scan/cursor scan, per whole pass");
    println!();
    println!(
        "| dataset | validate us/block | copy us | Block::get | view open+get | scratch+view+get | scratch+validate+get | cursor scan | position scan | allocs |"
    );
    println!("|---|---|---|---|---|---|---|---|---|---|");
    for kib in [32, 256] {
        let target = kib << 10;
        measure_reader_paths(&format!("structured {kib}K"), &structured_keys(target));
        measure_reader_paths(&format!("random {kib}K"), &random_keys(target));
        measure_reader_paths(&format!("small-value {kib}K"), &small_values(target));
        measure_reader_paths(&format!("multi-version {kib}K"), &multi_version(target));
    }

    println!();
    println!(
        "runs at the default 64 KiB block target, 16 MiB of uncompressed blocks; median of {RUN_ROUNDS} rounds, {RUN_PROBES} probes per round"
    );
    println!("ns per record (build, validate, scan) or per lookup, full path with one workspace");
    println!("checksum and decompress are us per block through the libraries, without the reader");
    println!(
        "peaks are the Rust heap, counting a reallocation as a copy; allocs = build/scan/warm lookups"
    );
    println!(
        "runtime descriptor: {} bytes for Run<Vec<u8>>, {} bytes for an empty workspace",
        size_of::<Run<Vec<u8>>>(),
        size_of::<ReadWorkspace>()
    );
    let sample = vec![7; 64 << 10];
    let mut output = Vec::with_capacity(zstd_safe::compress_bound(sample.len()));
    let mut compressor = zstd_safe::CCtx::create();
    compressor.compress(&mut output, &sample, 3).unwrap();
    let mut decoded = vec![0; sample.len()];
    let mut decompressor = zstd_safe::DCtx::create();
    decompressor.decompress(&mut decoded[..], &output).unwrap();
    println!(
        "zstd contexts after one 64 KiB block (C heap, not counted above): compress {} KiB, decompress {} KiB",
        compressor.sizeof() >> 10,
        decompressor.sizeof() >> 10
    );
    println!();
    println!(
        "| dataset | codec | MiB | ratio | build | validate | scan | locate | get hit | get miss | checksum us | decompress us | avg stored | build peak MiB | validate peak MiB | allocs |"
    );
    println!("|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|");
    let target = 16 << 20;
    let datasets: [(&str, Vec<Owned>); 5] = [
        ("structured", structured_keys(target)),
        ("random", random_keys(target)),
        ("small-value", small_values(target)),
        ("multi-version", multi_version(target)),
        ("json-like", json_like(target)),
    ];
    for (name, records) in &datasets {
        let mut uncompressed = 0;
        for (codec, label) in CODECS {
            let total = measure_run(name, label, codec, records, uncompressed);
            if codec == Codec::None {
                uncompressed = total;
            }
        }
    }
}
