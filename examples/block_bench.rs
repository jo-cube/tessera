//! Repeatable block and run measurements on deterministic datasets.
//!
//! With the argument `geometry`, instead compares run block sizes and codecs.
//! With the argument `prefix`, instead compares prefix scans over those sizes.
//! With the argument `merge`, instead measures merged iteration over many runs.
//! With the argument `logical`, instead measures latest-state reads over runs.
//! With the argument `ranges`, instead measures reads that key ranges can prune.
//! With the argument `snapshot`, instead compares snapshots with read guards.
//! With the argument `snapshot-frequency`, instead measures frequent snapshots.
//!
//! Run with `scripts/validate.sh bench`. Times are the median nanoseconds per
//! record or lookup across a fixed number of rounds, and every loop has a
//! fixed bound, so a broken reader or builder cannot run away. Allocation counts come
//! from one pass with a counting global allocator, which is the only unsafe
//! code in the repository and exists solely in this example.

use std::alloc::{GlobalAlloc, Layout, System};
use std::hint::black_box;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Instant;

use crc32fast::Hasher;
use lz4_flex::block as lz4;
use tessera::block::{Block, BlockBuilder, BlockPosition, BlockView, Record, Value};
use tessera::engine::{
    CompactionOptions, Engine, EngineOptions, EngineWorkspace, SnapshotLimits, UpdateError,
};
use tessera::memtable::{FrozenMemtable, Memtable, TARGET_RUN_LEN, WriteError};
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

/// A run's footer length, which depends on the format version.
const FOOTER_LEN: usize = 32;

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
    let footer = bytes.len() - FOOTER_LEN;
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
    let index = view.index_len() + FOOTER_LEN;
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
        run.view().index_len() + FOOTER_LEN,
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

const RANGE_FANS: [usize; 5] = [1, 2, 4, 8, 16];

/// Measures one configuration through the public read API alone, so that the
/// same code can time a build from before key ranges existed.
fn measure_ranges(
    config: &MergeConfig<'_>,
    records: &[Owned],
    buckets: &[(&'static str, Vec<Vec<u8>>)],
    queries: &mut Vec<String>,
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
    let views: Vec<RunView<'_>> = runs.iter().map(Run::view).collect();
    let set = RunSet::new(&views);
    let (live, _) = latest_live(records);

    let before_live = LIVE.load(Ordering::Relaxed);
    let mut workspace = LogicalWorkspace::new();
    {
        let mut cursor = set.cursor(&mut workspace);
        assert_eq!(drain_logical(&mut cursor), live.len());
    }
    let retained = LIVE.load(Ordering::Relaxed) - before_live;

    let stored: usize = runs.iter().map(|run| run.as_bytes().len()).sum();
    let metadata: usize = views.iter().map(|view| view.index_len() + FOOTER_LEN).sum();
    println!(
        "| {dataset} | {layout} | {fan} | {shape} | {} | {stored} | {:.1} | {:.3} | {retained} | {:.3} |",
        records.len(),
        metadata as f64 / fan as f64,
        stored as f64 / records.len() as f64,
        (stored as f64 + retained as f64) / records.len() as f64,
    );

    let mut rng = Rng(59);
    let mut pick = || -> Vec<Vec<u8>> {
        (0..MERGE_PROBES)
            .map(|_| live[(rng.next() % live.len() as u64) as usize].key.clone())
            .collect()
    };
    let hits = pick();
    let misses: Vec<Vec<u8>> = pick()
        .into_iter()
        .map(|key| [key.as_slice(), b"~"].concat())
        .collect();
    let seeks = pick();
    let bucket = |name: &str| -> &[Vec<u8>] {
        let (_, probes) = buckets.iter().find(|(label, _)| *label == name).unwrap();
        &probes[..MERGE_PROBES]
    };

    let row = |query: &str, ns: f64, blocks: f64, allocs: u64, queries: &mut Vec<String>| {
        queries.push(format!(
            "| {dataset} | {layout} | {fan} | {shape} | {query} | {ns:.0} | {blocks:.2} | {allocs} |"
        ));
    };

    for (query, probes, present) in [("hit", &hits, true), ("miss", &misses, false)] {
        for key in probes {
            assert_eq!(set.get(key, &mut workspace).unwrap().is_some(), present);
        }
        let before = workspace.blocks_loaded();
        for key in probes {
            black_box(set.get(key, &mut workspace).unwrap());
        }
        let blocks = (workspace.blocks_loaded() - before) as f64 / probes.len() as f64;
        let ns = median_ns_of(MERGE_ROUNDS, probes.len(), || {
            for key in probes {
                black_box(set.get(black_box(key), &mut workspace).unwrap());
            }
        });
        let allocs = allocations_during(|| {
            for key in probes {
                black_box(set.get(key, &mut workspace).unwrap());
            }
        });
        row(query, ns, blocks, allocs, queries);
    }

    let matching_live = |prefix: &[u8]| {
        let start = live.partition_point(|r| r.key.as_slice() < prefix);
        live[start..].partition_point(|r| r.key.starts_with(prefix))
    };
    let prefixes = [
        ("short prefix", bucket("1-5 records")),
        ("medium prefix", bucket("100-250 records")),
    ];
    for (query, probes) in prefixes {
        let expected: usize = probes.iter().map(|prefix| matching_live(prefix)).sum();
        let before = workspace.blocks_loaded();
        let (ns, allocs) = {
            let mut cursor = set.cursor(&mut workspace);
            let mut returned = 0;
            for prefix in probes {
                cursor.seek_prefix(prefix).unwrap();
                returned += drain_logical(&mut cursor);
            }
            assert_eq!(returned, expected, "{dataset} {layout} {fan} {query}");
            let ns = median_ns_of(MERGE_ROUNDS, probes.len(), || {
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
            (ns, allocs)
        };
        let passes = (MERGE_ROUNDS + 2) * probes.len();
        let blocks = (workspace.blocks_loaded() - before) as f64 / passes as f64;
        row(query, ns, blocks, allocs, queries);
    }

    // A seek followed by the record it lands on, since a positioned input may
    // not be decoded until it is read.
    let before = workspace.blocks_loaded();
    let (ns, allocs) = {
        let mut cursor = set.cursor(&mut workspace);
        for key in &seeks {
            cursor.seek(key).unwrap();
            assert_eq!(
                cursor.next_record().unwrap().map(|r| r.key),
                Some(key.as_slice())
            );
        }
        let ns = median_ns_of(MERGE_ROUNDS, seeks.len(), || {
            for key in &seeks {
                cursor.seek(black_box(key)).unwrap();
                black_box(cursor.next_record().unwrap());
            }
        });
        let allocs = allocations_during(|| {
            for key in &seeks {
                cursor.seek(black_box(key)).unwrap();
                black_box(cursor.next_record().unwrap());
            }
        });
        (ns, allocs)
    };
    let passes = (MERGE_ROUNDS + 2) * seeks.len();
    let blocks = (workspace.blocks_loaded() - before) as f64 / passes as f64;
    row("forward seek", ns, blocks, allocs, queries);
}

fn range_reads() {
    println!(
        "reads that key ranges can prune; {} MiB of logical data per dataset; {MERGE_ROUNDS} rounds, {MERGE_PROBES} seeded probes per round",
        GEOMETRY_BYTES >> 20
    );
    println!(
        "metadata = table, key area and footer per run; resident = stored plus retained Rust heap per record; Zstandard contexts are outside both"
    );
    println!();
    let datasets: [(&str, Fixture); 2] = [("unique", tree_json), ("versions", tree_versions)];
    let layouts: [(&str, Assign); 3] = [
        ("disjoint", assign_disjoint),
        ("partial", assign_partial),
        ("overlap", assign_overlapping),
    ];
    println!(
        "| dataset | layout | runs | shape | records | stored bytes | metadata bytes per run | stored per record | retained bytes | resident per record |"
    );
    println!("|---|---|---|---|---|---|---|---|---|---|");
    let mut queries = Vec::new();
    for (dataset, make) in datasets {
        let records = make(GEOMETRY_BYTES);
        let buckets = prefix_buckets(&records);
        for (layout, assign) in layouts {
            for fan in RANGE_FANS {
                measure_ranges(
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
                    &mut queries,
                );
            }
        }
    }
    println!();
    println!("| dataset | layout | runs | shape | query | ns | blocks | allocs |");
    println!("|---|---|---|---|---|---|---|---|");
    for row in &queries {
        println!("{row}");
    }
}

const MEMTABLE_ROUNDS: usize = 3;
const MEMTABLE_BUDGET: usize = 1 << 34;

fn large_values(target: usize) -> Vec<Owned> {
    let mut rng = Rng(23);
    fill(target, |i| {
        let mut value = rng.bytes(2048);
        value.resize(4096, 0);
        vec![Owned {
            key: format!("record/{i:012}").into_bytes(),
            seq: BASE_SEQ + i,
            value: Some(value),
        }]
    })
}

fn memtable_dataset(name: &str) -> Vec<Owned> {
    match name {
        "json" => tree_json(16 << 20),
        "tiny" => tree_small(8 << 20),
        "large" => large_values(16 << 20),
        _ => panic!("unknown dataset {name}"),
    }
}

fn shuffled(count: usize, seed: u64) -> Vec<usize> {
    let mut rng = Rng(seed);
    let mut order: Vec<usize> = (0..count).collect();
    for at in (1..count).rev() {
        order.swap(at, (rng.next() % (at as u64 + 1)) as usize);
    }
    order
}

fn insert_all(records: &[Owned], order: &[usize], table: &mut Memtable, next_seq: &mut u64) {
    for &index in order {
        let record = &records[index];
        *next_seq += 1;
        table
            .put(&record.key, *next_seq, record.value.as_deref().unwrap())
            .unwrap();
    }
}

fn filled(records: &[Owned], order: &[usize]) -> (Memtable, u64) {
    let mut table = Memtable::new(MEMTABLE_BUDGET, None);
    let mut next_seq = 0;
    insert_all(records, order, &mut table, &mut next_seq);
    (table, next_seq)
}

/// Returns the median nanoseconds per operation of `measure`, excluding
/// `setup` and dropping the state, and allocations per operation from one more
/// pass.
fn timed<T>(
    operations: usize,
    mut setup: impl FnMut() -> T,
    mut measure: impl FnMut(&mut T),
) -> (f64, f64) {
    let mut samples: Vec<f64> = (0..MEMTABLE_ROUNDS)
        .map(|_| {
            let mut state = setup();
            let start = Instant::now();
            measure(&mut state);
            let elapsed = start.elapsed().as_nanos() as f64;
            black_box(&state);
            elapsed / operations as f64
        })
        .collect();
    samples.sort_by(f64::total_cmp);
    let mut state = setup();
    let allocations = allocations_during(|| measure(&mut state));
    (
        samples[samples.len() / 2],
        allocations as f64 / operations as f64,
    )
}

fn vm_rss_bytes() -> Option<u64> {
    let status = std::fs::read_to_string("/proc/self/status").ok()?;
    let line = status.lines().find(|line| line.starts_with("VmRSS:"))?;
    let kib: u64 = line.split_whitespace().nth(1)?.parse().ok()?;
    Some(kib << 10)
}

const MIB: f64 = (1 << 20) as f64;

fn flush_row(name: &str, frozen: &FrozenMemtable, target: usize, payload: usize) {
    let records = frozen.len();
    let (mut runs, mut stored) = (0, 0);
    let mut peak_collected = 0;
    let (flush_ns, flush_allocs) = timed(
        records,
        || (),
        |()| {
            let before = LIVE.load(Ordering::Relaxed);
            PEAK.store(before, Ordering::Relaxed);
            let output: Vec<_> = frozen.flush(target).collect();
            peak_collected = PEAK.load(Ordering::Relaxed) - before;
            runs = output.len();
            stored = output.iter().map(|run| run.as_bytes().len()).sum();
        },
    );
    let peak_streamed = peak_during(|| {
        for run in frozen.flush(target) {
            black_box(run.as_bytes().len());
        }
    });
    println!(
        "| {name} | {} | {runs} | {:.2} | {:.3} | {:.1} | {:.1} | {:.2} | {:.2} | {:.2} |",
        target >> 20,
        stored as f64 / MIB,
        stored as f64 / payload as f64,
        flush_ns * records as f64 / 1e6,
        payload as f64 / MIB / (flush_ns * records as f64 / 1e9),
        flush_allocs,
        peak_collected as f64 / MIB,
        peak_streamed as f64 / MIB,
    );
}

/// Runs in its own process, so that resident memory is not hidden by what an
/// earlier dataset left in the allocator.
fn memtable_memory() {
    let args: Vec<String> = std::env::args().collect();
    let (name, order_name) = (args[2].as_str(), args[3].as_str());
    let records = memtable_dataset(name);
    let order: Vec<usize> = match order_name {
        "sorted" => (0..records.len()).collect(),
        _ => shuffled(records.len(), 91),
    };

    let live_before = LIVE.load(Ordering::Relaxed);
    let rss_before = vm_rss_bytes();
    let mut table = Memtable::new(MEMTABLE_BUDGET, None);
    let mut next_seq = 0;
    let peak = peak_during(|| insert_all(&records, &order, &mut table, &mut next_seq));
    let requested = LIVE.load(Ordering::Relaxed) - live_before;
    let rss = vm_rss_bytes();
    let memory = table.memory();

    insert_all(
        &records,
        &shuffled(records.len(), 92),
        &mut table,
        &mut next_seq,
    );
    let rss_churned = vm_rss_bytes();

    let delete_order = shuffled(records.len(), 93);
    for &index in &delete_order {
        next_seq += 1;
        table.delete(&records[index].key, next_seq).unwrap();
    }
    let requested_deleted = LIVE.load(Ordering::Relaxed) - live_before;

    let delta = |after: Option<u64>| match (after, rss_before) {
        (Some(after), Some(before)) => format!("{:.1}", (after - before) as f64 / MIB),
        _ => "n/a".to_string(),
    };
    let payload = memory.payload as f64;
    let rss_ratio = match (rss, rss_before) {
        (Some(after), Some(before)) => format!("{:.2}", (after - before) as f64 / payload),
        _ => "n/a".to_string(),
    };
    println!(
        "| {name} | {order_name} | {} | {:.1} | {:.1} | {:.1} | {} | {:.2} | {:.2} | {} | {:.2} | {:.1} | {} | {:.1} |",
        records.len(),
        payload / MIB,
        memory.charged as f64 / MIB,
        requested as f64 / MIB,
        delta(rss),
        memory.charged as f64 / payload,
        requested as f64 / payload,
        rss_ratio,
        requested as f64 / memory.charged as f64,
        peak as f64 / MIB,
        delta(rss_churned),
        requested_deleted as f64 / MIB,
    );
}

fn memtable_baseline() {
    const DATASETS: [&str; 3] = ["json", "tiny", "large"];

    println!("memory; each row is one fresh process; requested counts Rust heap bytes");
    println!(
        "| dataset | insert order | records | payload MiB | charged MiB | requested MiB | RSS MiB | charged/payload | requested/payload | RSS/payload | requested/charged | peak MiB | RSS after overwrite | requested after delete MiB |"
    );
    println!("|---|---|---|---|---|---|---|---|---|---|---|---|---|---|");
    let program = std::env::current_exe().unwrap();
    for name in DATASETS {
        for order in ["sorted", "random"] {
            let status = std::process::Command::new(&program)
                .args(["memtable-memory", name, order])
                .status()
                .unwrap();
            assert!(status.success());
        }
    }

    println!();
    println!("median of {MEMTABLE_ROUNDS} rounds; allocations from one further round");
    println!("| dataset | workload | ns/op | million ops/s | allocs/op |");
    println!("|---|---|---|---|---|");
    for name in DATASETS {
        let records = memtable_dataset(name);
        let count = records.len();
        let sorted: Vec<usize> = (0..count).collect();
        let random = shuffled(count, 91);
        let second = shuffled(count, 92);
        let row = |workload: &str, (ns, allocs): (f64, f64)| {
            println!(
                "| {name} | {workload} | {ns:.1} | {:.2} | {allocs:.2} |",
                1e3 / ns
            );
        };

        row(
            "sequential insert",
            timed(
                count,
                || (Memtable::new(MEMTABLE_BUDGET, None), 0),
                |(table, seq)| insert_all(&records, &sorted, table, seq),
            ),
        );
        row(
            "random insert",
            timed(
                count,
                || (Memtable::new(MEMTABLE_BUDGET, None), 0),
                |(table, seq)| insert_all(&records, &random, table, seq),
            ),
        );
        row(
            "random overwrite",
            timed(
                count,
                || filled(&records, &random),
                |(table, seq)| insert_all(&records, &second, table, seq),
            ),
        );
        row(
            "random delete",
            timed(
                count,
                || filled(&records, &random),
                |(table, seq)| {
                    for &index in &second {
                        *seq += 1;
                        table.delete(&records[index].key, *seq).unwrap();
                    }
                },
            ),
        );

        let (table, _) = filled(&records, &random);
        let misses: Vec<Vec<u8>> = records
            .iter()
            .map(|record| [record.key.as_slice(), b"~"].concat())
            .collect();
        row(
            "get hit",
            timed(
                count,
                || (),
                |()| {
                    for &index in &second {
                        black_box(table.get(&records[index].key));
                    }
                },
            ),
        );
        row(
            "get miss",
            timed(
                count,
                || (),
                |()| {
                    for &index in &second {
                        black_box(table.get(&misses[index]));
                    }
                },
            ),
        );
        row(
            "ordered traversal",
            timed(
                count,
                || (),
                |()| {
                    for record in table.iter() {
                        black_box(record);
                    }
                },
            ),
        );
        drop(table);
        row(
            "freeze (per table)",
            timed(
                1,
                || (Some(filled(&records, &random).0), None),
                |(table, frozen)| *frozen = table.take().map(Memtable::freeze),
            ),
        );
        row(
            "drop frozen (per table)",
            timed(
                1,
                || Some(filled(&records, &random).0.freeze()),
                |frozen| drop(frozen.take()),
            ),
        );
    }

    println!();
    println!("flush; peaks are Rust heap above the frozen table, excluding Zstandard contexts");
    println!(
        "| dataset | target MiB | runs | stored MiB | stored/payload | flush ms | payload MiB/s | allocs/record | peak collected MiB | peak streamed MiB |"
    );
    println!("|---|---|---|---|---|---|---|---|---|---|");
    for name in DATASETS {
        let records = memtable_dataset(name);
        let order = shuffled(records.len(), 91);
        let (table, _) = filled(&records, &order);
        let payload = table.memory().payload;
        let frozen = table.freeze();
        flush_row(name, &frozen, TARGET_RUN_LEN, payload);
        flush_row(name, &frozen, 4 << 20, payload);
    }
}

fn engine_options() -> EngineOptions {
    EngineOptions {
        memtable_budget: MEMTABLE_BUDGET,
        max_runs: 4096,
        max_run_bytes: usize::MAX,
        ..EngineOptions::default()
    }
}

fn engine_put_all(engine: &Engine, records: &[Owned], order: &[usize]) {
    for &index in order {
        let record = &records[index];
        engine
            .put(&record.key, record.value.as_deref().unwrap())
            .unwrap();
    }
}

fn engine_filled(records: &[Owned], order: &[usize]) -> Engine {
    let engine = Engine::new(engine_options());
    engine_put_all(&engine, records, order);
    engine
}

fn drain_scan(engine: &Engine, prefix: &[u8], workspace: &mut EngineWorkspace) -> usize {
    let reader = engine.read();
    let mut scan = reader.scan_prefix(prefix, workspace);
    let mut records = 0;
    while let Some(record) = scan.next_record().unwrap() {
        black_box(record);
        records += 1;
    }
    records
}

fn percentile(sorted: &[u64], at: f64) -> u64 {
    if sorted.is_empty() {
        return 0;
    }
    sorted[((sorted.len() - 1) as f64 * at) as usize]
}

/// One read-write interleaving on a table of existing keys. The writer always
/// performs the same overwrites, and every other thread runs until it is done.
fn contention_row(
    name: &str,
    records: &[Owned],
    writes: usize,
    extra: impl FnOnce(&Engine, &AtomicU64) -> u64 + Send,
) {
    let random = shuffled(records.len(), 91);
    let order = shuffled(records.len(), 92);
    let engine = engine_filled(records, &random);
    let done = AtomicU64::new(0);
    let (samples, elapsed, other) = std::thread::scope(|scope| {
        let (engine_ref, done_ref) = (&engine, &done);
        let helper = scope.spawn(move || extra(engine_ref, done_ref));
        let mut samples = Vec::with_capacity(writes / 16 + 1);
        let start = Instant::now();
        for write in 0..writes {
            let record = &records[order[write % order.len()]];
            let value = record.value.as_deref().unwrap();
            if write.is_multiple_of(16) {
                let before = Instant::now();
                engine.put(&record.key, value).unwrap();
                samples.push(before.elapsed().as_nanos() as u64);
            } else {
                engine.put(&record.key, value).unwrap();
            }
            if write.is_multiple_of(64) {
                done.store(write as u64 + 1, Ordering::Relaxed);
            }
        }
        let elapsed = start.elapsed().as_secs_f64();
        done.store(u64::MAX, Ordering::Relaxed);
        (samples, elapsed, helper.join().unwrap())
    });
    let mut samples = samples;
    samples.sort_unstable();
    println!(
        "| {name} | {:.2} | {} | {} | {} | {} | {:.2} |",
        writes as f64 / elapsed / 1e6,
        percentile(&samples, 0.5),
        percentile(&samples, 0.99),
        percentile(&samples, 0.999),
        samples.last().copied().unwrap_or(0),
        other as f64 / elapsed / 1e6,
    );
}

fn engine_baseline() {
    use tessera::run::{LogicalWorkspace, RunSet};

    let records = tree_json(16 << 20);
    let count = records.len();
    let sorted: Vec<usize> = (0..count).collect();
    let random = shuffled(count, 91);
    let second = shuffled(count, 92);
    println!(
        "{count} json records; median of {MEMTABLE_ROUNDS} rounds; allocations from one further round"
    );

    println!();
    println!("| write | ns/op | million ops/s | allocs/op |");
    println!("|---|---|---|---|");
    let row = |name: &str, (ns, allocs): (f64, f64)| {
        println!("| {name} | {ns:.1} | {:.2} | {allocs:.2} |", 1e3 / ns);
    };
    row(
        "memtable sequential insert",
        timed(
            count,
            || (Memtable::new(MEMTABLE_BUDGET, None), 0),
            |(table, seq)| insert_all(&records, &sorted, table, seq),
        ),
    );
    row(
        "engine sequential put",
        timed(
            count,
            || Engine::new(engine_options()),
            |engine| engine_put_all(engine, &records, &sorted),
        ),
    );
    row(
        "memtable random insert",
        timed(
            count,
            || (Memtable::new(MEMTABLE_BUDGET, None), 0),
            |(table, seq)| insert_all(&records, &random, table, seq),
        ),
    );
    row(
        "engine random put",
        timed(
            count,
            || Engine::new(engine_options()),
            |engine| engine_put_all(engine, &records, &random),
        ),
    );
    row(
        "memtable overwrite",
        timed(
            count,
            || filled(&records, &random),
            |(table, seq)| insert_all(&records, &second, table, seq),
        ),
    );
    row(
        "engine overwrite",
        timed(
            count,
            || engine_filled(&records, &random),
            |engine| engine_put_all(engine, &records, &second),
        ),
    );
    row(
        "engine delete",
        timed(
            count,
            || engine_filled(&records, &random),
            |engine| {
                for &index in &second {
                    engine.delete(&records[index].key).unwrap();
                }
            },
        ),
    );

    println!();
    println!("| read | ns/op | million ops/s | allocs/op | blocks/op |");
    println!("|---|---|---|---|---|");
    let read_row = |name: &str, (ns, allocs): (f64, f64), blocks: f64| {
        println!(
            "| {name} | {ns:.1} | {:.2} | {allocs:.2} | {blocks:.2} |",
            1e3 / ns
        );
    };

    let (table, _) = filled(&records, &random);
    read_row(
        "memtable get",
        timed(
            count,
            || (),
            |()| {
                for &index in &second {
                    black_box(table.get(&records[index].key));
                }
            },
        ),
        0.0,
    );
    let frozen = table.freeze();
    let direct_runs: Vec<_> = frozen.flush(4 << 20).collect();
    drop(frozen);
    let views: Vec<_> = direct_runs.iter().map(|run| run.view()).collect();

    let active = engine_filled(&records, &random);
    let mut workspace = EngineWorkspace::new();
    let get_active = timed(
        count,
        || (),
        |()| {
            for &index in &second {
                black_box(active.get(&records[index].key, &mut workspace).unwrap());
            }
        },
    );
    read_row("engine get, active table", get_active, 0.0);

    // A lookup decodes a block for almost every probe here, so use fewer.
    let probes = (count / 8).max(1);
    let mut logical = LogicalWorkspace::new();
    let before = logical.blocks_loaded();
    let run_get = timed(
        probes,
        || (),
        |()| {
            for &index in &second[..probes] {
                let set = RunSet::new(&views);
                black_box(set.get(&records[index].key, &mut logical).unwrap());
            }
        },
    );
    let blocks = (logical.blocks_loaded() - before) as f64 / (4 * probes) as f64;
    read_row("run set get, 5 disjoint runs", run_get, blocks);

    let runs_engine = Engine::new(EngineOptions {
        target_run_len: 4 << 20,
        ..engine_options()
    });
    engine_put_all(&runs_engine, &records, &random);
    runs_engine.flush().unwrap();
    let mut workspace = EngineWorkspace::new();
    let before = workspace.blocks_loaded();
    let get_runs = timed(
        probes,
        || (),
        |()| {
            for &index in &second[..probes] {
                black_box(
                    runs_engine
                        .get(&records[index].key, &mut workspace)
                        .unwrap(),
                );
            }
        },
    );
    let blocks = (workspace.blocks_loaded() - before) as f64 / (4 * probes) as f64;
    read_row("engine get, 5 disjoint runs", get_runs, blocks);

    let overlapped = Engine::new(engine_options());
    for batch in 0..8 {
        let keys: Vec<usize> = (0..count).filter(|i| i % 8 == batch).collect();
        engine_put_all(&overlapped, &records, &keys);
        overlapped.flush().unwrap();
    }
    let mut workspace = EngineWorkspace::new();
    let before = workspace.blocks_loaded();
    let get_overlapped = timed(
        probes,
        || (),
        |()| {
            for &index in &second[..probes] {
                black_box(overlapped.get(&records[index].key, &mut workspace).unwrap());
            }
        },
    );
    let blocks = (workspace.blocks_loaded() - before) as f64 / (4 * probes) as f64;
    read_row("engine get, 8 overlapping runs", get_overlapped, blocks);

    println!();
    println!("| scan | ns/record | million records/s | allocs/scan | blocks/scan | records/scan |");
    println!("|---|---|---|---|---|---|");
    let scan_row =
        |name: &str, (ns, allocs): (f64, f64), blocks: f64, per_scan: f64, ops: usize| {
            println!(
                "| {name} | {ns:.1} | {:.2} | {:.2} | {blocks:.2} | {per_scan:.1} |",
                1e3 / ns,
                allocs * ops as f64,
            );
        };

    let half = engine_filled(&records, &random[..count / 2]);
    half.flush().unwrap();
    engine_put_all(&half, &records, &random[count / 2..]);

    for (name, engine) in [
        ("engine scan, active only", &active),
        ("engine scan, runs only", &runs_engine),
        ("engine scan, half active half runs", &half),
    ] {
        let mut workspace = EngineWorkspace::new();
        let before = workspace.blocks_loaded();
        let result = timed(
            count,
            || (),
            |()| {
                black_box(drain_scan(engine, b"", &mut workspace));
            },
        );
        let blocks = (workspace.blocks_loaded() - before) as f64 / 4.0;
        scan_row(name, result, blocks, count as f64, count);
    }

    let mut logical = LogicalWorkspace::new();
    let before = logical.blocks_loaded();
    let direct = timed(
        count,
        || (),
        |()| {
            let set = RunSet::new(&views);
            let mut cursor = set.cursor(&mut logical);
            while let Some(record) = cursor.next_record().unwrap() {
                black_box(record);
            }
        },
    );
    let blocks = (logical.blocks_loaded() - before) as f64 / 4.0;
    scan_row(
        "logical cursor, runs only",
        direct,
        blocks,
        count as f64,
        count,
    );

    // Short prefixes over keys held half in the table and half in runs.
    const PREFIX_SCANS: usize = 2000;
    let cut = records[0].key.len().saturating_sub(2);
    let prefixes: Vec<&[u8]> = (0..PREFIX_SCANS)
        .map(|at| &records[random[at % count]].key[..cut])
        .collect();
    let mut workspace = EngineWorkspace::new();
    let before = workspace.blocks_loaded();
    let mut found = 0;
    let (ns, allocs) = timed(
        PREFIX_SCANS,
        || (),
        |()| {
            found = 0;
            for prefix in &prefixes {
                found += drain_scan(&half, prefix, &mut workspace);
            }
        },
    );
    let blocks = (workspace.blocks_loaded() - before) as f64 / (4 * PREFIX_SCANS) as f64;
    println!(
        "| engine prefix scan, half active half runs | {ns:.0} ns per scan | {:.2} million/s | {allocs:.2} | {blocks:.2} | {:.1} |",
        1e3 / ns,
        found as f64 / PREFIX_SCANS as f64
    );

    println!();
    println!("repeated flushes from a 4 MiB table; requested heap excludes Zstandard contexts");
    println!(
        "| flushes | runs | flush ms total | ms/flush | payload MiB/s | payload MiB | stored MiB | write amp (stored/payload) | peak heap MiB | live heap MiB | resident run bytes MiB |"
    );
    println!("|---|---|---|---|---|---|---|---|---|---|---|");
    let live_before = LIVE.load(Ordering::Relaxed);
    PEAK.store(live_before, Ordering::Relaxed);
    let engine = Engine::new(EngineOptions {
        memtable_budget: 4 << 20,
        ..engine_options()
    });
    let (mut flushes, mut flush_ns, mut total_runs, mut payload) = (0, 0u128, 0, 0usize);
    let mut flush = |engine: &Engine| {
        let start = Instant::now();
        let report = engine.flush().unwrap();
        flush_ns += start.elapsed().as_nanos();
        flushes += report.tables;
        total_runs += report.runs;
    };
    for &index in &random {
        let record = &records[index];
        let value = record.value.as_deref().unwrap();
        payload += record.key.len() + value.len();
        if engine.put(&record.key, value).is_err() {
            flush(&engine);
            engine.put(&record.key, value).unwrap();
        }
    }
    flush(&engine);
    let stats = engine.stats();
    let live = LIVE.load(Ordering::Relaxed) - live_before;
    let peak = PEAK.load(Ordering::Relaxed) - live_before;
    println!(
        "| {flushes} | {total_runs} | {:.1} | {:.2} | {:.1} | {:.1} | {:.2} | {:.3} | {:.1} | {:.1} | {:.2} |",
        flush_ns as f64 / 1e6,
        flush_ns as f64 / 1e6 / flushes as f64,
        payload as f64 / MIB / (flush_ns as f64 / 1e9),
        payload as f64 / MIB,
        stats.run_bytes as f64 / MIB,
        stats.run_bytes as f64 / payload as f64,
        peak as f64 / MIB,
        live as f64 / MIB,
        stats.run_bytes as f64 / MIB,
    );
    drop(engine);

    println!();
    println!(
        "contention; latencies sample every 16th write; 2 CPUs; other thread in M ops/s, or M records/s for scans"
    );
    println!(
        "| scenario | writes M/s | p50 ns | p99 ns | p99.9 ns | max ns | other thread M ops/s |"
    );
    println!("|---|---|---|---|---|---|---|");
    contention_row("writer alone", &records, 200_000, |_, _| 0);
    contention_row(
        "writer + point reader",
        &records,
        200_000,
        |engine, done| {
            let mut workspace = EngineWorkspace::new();
            let (mut reads, mut at) = (0u64, 0);
            while done.load(Ordering::Relaxed) != u64::MAX {
                let index = second[at % count];
                black_box(engine.get(&records[index].key, &mut workspace).unwrap());
                reads += 1;
                at += 1;
            }
            reads
        },
    );
    // A write can wait for a whole scan, so this needs far fewer writes.
    contention_row("writer + full scans", &records, 2_000, |engine, done| {
        let mut workspace = EngineWorkspace::new();
        let mut scanned = 0u64;
        while done.load(Ordering::Relaxed) != u64::MAX {
            scanned += drain_scan(engine, b"", &mut workspace) as u64;
        }
        scanned
    });
    contention_row(
        "writer + flusher every 20k writes",
        &records,
        200_000,
        |engine, done| {
            let mut flushed_at = 0;
            let mut flushes = 0;
            loop {
                let written = done.load(Ordering::Relaxed);
                if written == u64::MAX {
                    return flushes;
                }
                if written >= flushed_at + 20_000 {
                    flushed_at = written;
                    flushes += engine.flush().unwrap().tables as u64;
                } else {
                    std::thread::yield_now();
                }
            }
        },
    );

    let readers = engine_filled(&records, &random);
    let start = Instant::now();
    let total: u64 = std::thread::scope(|scope| {
        let handles: Vec<_> = (0..2)
            .map(|thread| {
                let (readers, records, second) = (&readers, &records, &second);
                scope.spawn(move || {
                    let mut workspace = EngineWorkspace::new();
                    for at in 0..400_000 {
                        let index = second[(at + thread * 7919) % count];
                        black_box(readers.get(&records[index].key, &mut workspace).unwrap());
                    }
                    400_000u64
                })
            })
            .collect();
        handles
            .into_iter()
            .map(|handle| handle.join().unwrap())
            .sum()
    });
    println!(
        "| two point readers, no writer | 0 | 0 | 0 | 0 | 0 | {:.2} |",
        total as f64 / start.elapsed().as_secs_f64() / 1e6
    );
}

const SUSTAINED_PASSES: u64 = 6;
const SUSTAINED_BUDGET: usize = 4 << 20;
const SUSTAINED_ROUNDS: usize = 3;
const LATENCY_EVERY: usize = 16;
const READ_SAMPLES: usize = 200_000;
const WINDOWS: usize = 16;
const READ_PACE: std::time::Duration = std::time::Duration::from_millis(5);

/// CPU time of a thread of this process, from the Linux scheduler.
fn task_cpu_ns(task: &str) -> u64 {
    let path = match task {
        "thread-self" => "/proc/thread-self/schedstat".to_owned(),
        task => format!("/proc/self/task/{task}/schedstat"),
    };
    std::fs::read_to_string(path)
        .ok()
        .and_then(|stat| stat.split_whitespace().next()?.parse().ok())
        .unwrap_or(0)
}

/// CPU time of the whole process, including threads that have exited.
fn process_cpu_s() -> f64 {
    let stat = std::fs::read_to_string("/proc/self/stat").unwrap_or_default();
    let fields: Vec<&str> = stat
        .rsplit_once(") ")
        .map_or(Vec::new(), |(_, rest)| rest.split(' ').collect());
    // utime and stime, fields 14 and 15, in clock ticks of 1/100 s.
    let ticks = |at: usize| {
        fields
            .get(at - 3)
            .and_then(|f| f.parse::<u64>().ok())
            .unwrap_or(0)
    };
    (ticks(14) + ticks(15)) as f64 / 100.0
}

fn worker_task() -> Option<String> {
    std::fs::read_dir("/proc/self/task")
        .ok()?
        .flatten()
        .find_map(|task| {
            let comm = std::fs::read_to_string(task.path().join("comm")).ok()?;
            comm.starts_with("tessera-compact")
                .then(|| task.file_name().to_string_lossy().into_owned())
        })
}

#[derive(Clone, Copy, PartialEq)]
enum Mode {
    None,
    Sync,
    Background,
}

#[derive(Default)]
struct Sustained {
    write_s: f64,
    catch_up_s: f64,
    flush_s: f64,
    flushes: usize,
    flushed_bytes: usize,
    compactions: u64,
    rewritten: u64,
    foreground_compact_s: f64,
    compact_cpu_s: f64,
    process_cpu_s: f64,
    write_latency: Vec<u64>,
    overlap: Vec<usize>,
    peak_runs: usize,
    end_runs: usize,
    end_overlap: usize,
    end_run_bytes: usize,
    pending_runs: usize,
    pending_overlap: usize,
    catch_up_compactions: u64,
    peak_heap: u64,
    stable_heap: u64,
    gets: Vec<u64>,
    scans: Vec<u64>,
}

/// One workload through one configuration: the writing thread puts every
/// record of each pass, flushing when the memtable is full, while a reader
/// samples gets and group prefix scans. When writing ends, compaction catches
/// up to the default overlap, so every configuration ends in the same state.
fn sustained_run(
    records: &[Owned],
    orders: &[Vec<usize>],
    mode: Mode,
    probes: &[usize],
) -> Sustained {
    let options = CompactionOptions::default();
    let engine = Engine::new(EngineOptions {
        memtable_budget: SUSTAINED_BUDGET,
        max_runs: 4096,
        max_run_bytes: usize::MAX,
        background_compaction: (mode == Mode::Background).then_some(options),
        ..EngineOptions::default()
    });
    let mut out = Sustained {
        write_latency: Vec::with_capacity(records.len() * orders.len() / LATENCY_EVERY + 1),
        overlap: Vec::with_capacity(4096),
        ..Sustained::default()
    };
    let (mut gets, mut scans) = (
        Vec::with_capacity(READ_SAMPLES),
        Vec::with_capacity(READ_SAMPLES),
    );
    let mut workspace = MergeWorkspace::new();
    let writing = AtomicU64::new(1);
    let cpu = process_cpu_s();
    let base = LIVE.load(Ordering::Relaxed);
    PEAK.store(base, Ordering::Relaxed);
    let start = Instant::now();

    std::thread::scope(|scope| {
        let (engine, writing, gets, scans) = (&engine, &writing, &mut gets, &mut scans);
        scope.spawn(move || {
            let mut workspace = EngineWorkspace::new();
            let mut at = 0;
            while writing.load(Ordering::Acquire) == 1 && gets.len() < READ_SAMPLES {
                let key = &records[probes[at % probes.len()]].key;
                at += 1;
                let begin = Instant::now();
                if at % 8 == 0 {
                    black_box(drain_scan(engine, &key[..GROUP_LEN], &mut workspace));
                    scans.push(begin.elapsed().as_nanos() as u64);
                } else {
                    black_box(engine.get(key, &mut workspace).unwrap());
                    gets.push(begin.elapsed().as_nanos() as u64);
                }
                // Paced, so that reads cost writers at most a few percent
                // even where each takes most of a millisecond.
                std::thread::sleep(READ_PACE);
            }
        });

        let flush = |out: &mut Sustained, workspace: &mut MergeWorkspace| {
            let begin = Instant::now();
            let report = engine.flush().unwrap();
            out.flush_s += begin.elapsed().as_secs_f64();
            out.flushes += 1;
            out.flushed_bytes += report.run_bytes;
            if mode == Mode::Sync {
                let (begin, cpu) = (Instant::now(), task_cpu_ns("thread-self"));
                if let Some(report) = engine.compact(options, workspace).unwrap() {
                    out.compactions += 1;
                    out.rewritten += report.output_bytes as u64;
                }
                out.foreground_compact_s += begin.elapsed().as_secs_f64();
                out.compact_cpu_s += (task_cpu_ns("thread-self") - cpu) as f64 / 1e9;
            }
            let stats = engine.stats();
            out.peak_runs = out.peak_runs.max(stats.runs);
            out.overlap.push(stats.max_overlap);
        };
        let mut puts = 0;
        for order in orders {
            for &index in order {
                let record = &records[index];
                let value = record.value.as_deref().unwrap();
                let sampled = puts % LATENCY_EVERY == 0;
                let begin = sampled.then(Instant::now);
                loop {
                    match engine.put(&record.key, value) {
                        Ok(_) => break,
                        Err(UpdateError::Rejected(WriteError::BudgetExceeded { .. })) => {
                            flush(&mut out, &mut workspace);
                        }
                        Err(error) => panic!("{error}"),
                    }
                }
                if let Some(begin) = begin {
                    out.write_latency.push(begin.elapsed().as_nanos() as u64);
                }
                puts += 1;
            }
        }
        flush(&mut out, &mut workspace);
        out.write_s = start.elapsed().as_secs_f64();
        let stats = engine.stats();
        (out.pending_runs, out.pending_overlap) = (stats.runs, stats.max_overlap);

        // Catch up to the same end state in every mode.
        let begin = Instant::now();
        if mode == Mode::Background {
            let before = engine.background_status().unwrap().compactions;
            engine.wait_for_compaction().unwrap();
            out.catch_up_compactions = engine.background_status().unwrap().compactions - before;
        } else if mode == Mode::Sync {
            while let Some(report) = engine.compact(options, &mut workspace).unwrap() {
                out.catch_up_compactions += 1;
                out.compactions += 1;
                out.rewritten += report.output_bytes as u64;
            }
        }
        out.catch_up_s = begin.elapsed().as_secs_f64();
        writing.store(0, Ordering::Release);
    });
    if let Some(status) = engine.background_status() {
        out.compactions = status.compactions;
        out.rewritten = status.output_bytes;
        // The worker's whole life, which is this run.
        let worker = worker_task().expect("the worker thread is running");
        out.compact_cpu_s = task_cpu_ns(&worker) as f64 / 1e9;
    }
    out.process_cpu_s = process_cpu_s() - cpu;
    out.peak_heap = PEAK.load(Ordering::Relaxed) - base;
    let stats = engine.stats();
    (out.end_runs, out.end_overlap, out.end_run_bytes) =
        (stats.runs, stats.max_overlap, stats.run_bytes);
    drop(workspace);
    out.stable_heap = LIVE.load(Ordering::Relaxed).saturating_sub(base);
    out.write_latency.sort_unstable();
    gets.sort_unstable();
    scans.sort_unstable();
    (out.gets, out.scans) = (gets, scans);
    drop(engine);
    out
}

fn sustained_rows(workload: &str, mode: &str, round: usize, puts: usize, run: &Sustained) {
    let quarter = |at: usize| run.overlap[(run.overlap.len() - 1) * at / 4];
    let mean_overlap = run.overlap.iter().sum::<usize>() as f64 / run.overlap.len() as f64;
    println!(
        "| {workload} | {mode} | {round} | {:.2} | {} | {} | {} | {:.0} | {:.0} | {} | {:.1} | {:.2} | {:.2} | {:.3} | {}, {}, {}, {} | {:.1} | {} | {} | {}/{} | {:.0} | {:.2} | {} | {} | {} | {} | {:.1} | {:.1} |",
        puts as f64 / run.write_s / 1e6,
        percentile(&run.write_latency, 0.5),
        percentile(&run.write_latency, 0.99),
        run.write_latency.last().copied().unwrap_or(0) / 1000,
        run.flushed_bytes as f64 / MIB / run.flush_s.max(1e-9),
        run.flushed_bytes as f64 / MIB,
        run.compactions,
        run.compact_cpu_s,
        run.foreground_compact_s,
        run.process_cpu_s,
        (run.flushed_bytes as u64 + run.rewritten) as f64 / run.flushed_bytes as f64,
        quarter(1),
        quarter(2),
        quarter(3),
        quarter(4),
        mean_overlap,
        run.peak_runs,
        run.pending_overlap,
        run.pending_runs,
        run.end_runs,
        run.catch_up_s * 1e3,
        (run.write_s + run.catch_up_s),
        percentile(&run.gets, 0.5),
        percentile(&run.gets, 0.99),
        percentile(&run.scans, 0.5),
        percentile(&run.scans, 0.99),
        run.peak_heap as f64 / MIB,
        run.stable_heap as f64 / MIB,
    );
}

/// Writes, flushes and compaction under three schedules, on random overwrites
/// and on updates confined to a moving key window.
fn compaction_sustained() {
    let records = tree_json(16 << 20);
    let count = records.len();
    let puts = count * SUSTAINED_PASSES as usize;
    let random: Vec<Vec<usize>> = (0..SUSTAINED_PASSES)
        .map(|pass| shuffled(count, 300 + pass))
        .collect();
    // Each phase rewrites one sixteenth of the key space, contiguous in key
    // order, in random order; the window moves to its neighbour on every
    // phase, so a flush spanning two phases covers two adjacent windows.
    let width = count.div_ceil(WINDOWS);
    let windows = SUSTAINED_PASSES as usize * WINDOWS;
    let localized: Vec<Vec<usize>> = (0..windows)
        .map(|phase| {
            let first = (phase % WINDOWS) * width;
            let len = width.min(count - first);
            shuffled(len, 500 + phase as u64)
                .into_iter()
                .map(|i| first + i)
                .collect()
        })
        .collect();
    assert_eq!(localized.iter().map(Vec::len).sum::<usize>(), puts);
    let probes: Vec<usize> = shuffled(count, 299).into_iter().take(4096).collect();
    println!(
        "{count} json keys, {puts} puts per workload; 4 MiB memtable; default run and compaction options: {:?}",
        CompactionOptions::default()
    );
    println!(
        "random: {SUSTAINED_PASSES} passes over every key in random order; localized: {windows} phases, each rewriting the next of {WINDOWS} contiguous key windows"
    );
    println!(
        "sync compacts once after each flush on the writing thread; background has the worker; sync and background then catch up until nothing is eligible, none is left as it is"
    );
    println!(
        "write latency samples every {LATENCY_EVERY}th put, including any flush and sync compaction it triggers; a reader thread samples a get, or every 8th time a group scan, every {} ms while writing and catching up",
        READ_PACE.as_millis()
    );
    println!(
        "compaction CPU is the writing thread's for sync, the worker's for background; process CPU covers every thread including the reader"
    );
    println!();
    println!(
        "| workload | mode | round | puts M/s | write p50 ns | write p99 ns | write max us | flush stored MiB/s | flushed MiB | compactions | compaction CPU s | foreground compaction s | process CPU s | stored written / flushed | overlap at 1/4, 1/2, 3/4, end of writes | mean overlap | peak runs | overlap left | runs left/after | catch-up ms | total s | get p50 ns | get p99 ns | scan p50 ns | scan p99 ns | peak heap MiB | stable heap MiB |"
    );
    println!("|{}", "---|".repeat(28));
    for round in 0..SUSTAINED_ROUNDS {
        for (workload, orders) in [("random", &random), ("localized", &localized)] {
            for (name, mode) in [
                ("none", Mode::None),
                ("sync", Mode::Sync),
                ("background", Mode::Background),
            ] {
                let run = sustained_run(&records, orders, mode, &probes);
                sustained_rows(workload, name, round, puts, &run);
            }
        }
    }
}

const SNAP_BUDGET: usize = 4 << 20;
const SNAP_ROUNDS: usize = 3;
const SNAP_PROBES: usize = 2_000;
/// Smaller than `SNAP_BUDGET`, so that the shorter contention pass flushes
/// several times. A writer behind guard-held full scans waits for about one
/// scan per put, so a whole pass would take minutes.
const CONTENTION_BUDGET: usize = 1 << 20;
const CONTENTION_SHARE: usize = 8;
const FREQUENCY_ROUNDS: usize = 2;
const LARGE_VALUE: usize = 4 << 10;
const LARGE_KEYS: u64 = 4096;
const LARGE_PASSES: u64 = 4;

/// Without compaction every flush stays a run, so the run limit is raised;
/// with the background worker it is the default, and a flush refused at it
/// waits for compaction.
fn snapshot_options(budget: usize, background: bool) -> EngineOptions {
    EngineOptions {
        memtable_budget: budget,
        max_runs: if background {
            EngineOptions::default().max_runs
        } else {
            4096
        },
        max_run_bytes: usize::MAX,
        background_compaction: background.then(CompactionOptions::default),
        ..EngineOptions::default()
    }
}

#[derive(Default)]
struct FlushTally {
    flushes: usize,
    runs: usize,
    bytes: usize,
    /// Flushes refused at the run limit, each followed by a wait for
    /// background compaction.
    stalls: usize,
    publish_ns: Vec<u64>,
}

impl FlushTally {
    /// Flushes in stages, timing only publication: the wait for the lock.
    fn flush(&mut self, engine: &Engine) {
        for _ in 0..10_000 {
            let Some(job) = engine.begin_flush() else {
                return;
            };
            let built = job.build();
            let start = Instant::now();
            match built.publish() {
                Ok(report) => {
                    self.publish_ns.push(start.elapsed().as_nanos() as u64);
                    self.flushes += 1;
                    self.runs += report.runs;
                    self.bytes += report.run_bytes;
                }
                Err(error) => {
                    engine
                        .wait_for_compaction()
                        .unwrap_or_else(|_| panic!("{error}"));
                    self.stalls += 1;
                }
            }
        }
        panic!("flushing did not finish");
    }
}

/// Puts `value` under `key`, flushing first whenever the memtable is full.
fn put_flushing(engine: &Engine, key: &[u8], value: &[u8], tally: &mut FlushTally) {
    for _ in 0..3 {
        match engine.put(key, value) {
            Ok(_) => return,
            Err(UpdateError::Rejected(WriteError::BudgetExceeded { .. })) => tally.flush(engine),
            Err(error) => panic!("{error}"),
        }
    }
    panic!("a put failed after flushing");
}

/// Compacts with the default options until nothing is eligible, returning
/// the stored bytes rewritten and the time each publication took.
fn compact_done(engine: &Engine) -> (usize, Vec<u64>) {
    let mut workspace = MergeWorkspace::new();
    let (mut rewritten, mut publish) = (0, Vec::new());
    for _ in 0..100_000 {
        let Some(job) = engine
            .begin_compaction(CompactionOptions::default())
            .unwrap()
        else {
            return (rewritten, publish);
        };
        let built = job.build(&mut workspace).unwrap();
        let start = Instant::now();
        let report = built.publish().unwrap();
        publish.push(start.elapsed().as_nanos() as u64);
        rewritten += report.input_bytes;
    }
    panic!("compaction did not finish");
}

/// Every record in `order`, flushed and compacted, with an empty memtable
/// and, with a worker, nothing left for it to do.
fn snapshot_base(records: &[Owned], order: &[usize], options: EngineOptions) -> Engine {
    let engine = Engine::new(options);
    let mut tally = FlushTally::default();
    for &index in order {
        let record = &records[index];
        put_flushing(
            &engine,
            &record.key,
            record.value.as_deref().unwrap(),
            &mut tally,
        );
    }
    tally.flush(&engine);
    if options.background_compaction.is_some() {
        engine.wait_for_compaction().unwrap();
    }
    compact_done(&engine);
    engine
}

fn drain(mut scan: tessera::engine::Scan<'_>) -> usize {
    let mut records = 0;
    while let Some(record) = scan.next_record().unwrap() {
        black_box(record);
        records += 1;
    }
    records
}

fn mib(bytes: usize) -> f64 {
    bytes as f64 / (1 << 20) as f64
}

fn snapshot_large_values() -> Vec<Owned> {
    let mut rng = Rng(23);
    (0..LARGE_KEYS)
        .map(|i| Owned {
            key: tree_key(i, FLAT_TREE),
            seq: BASE_SEQ + i,
            value: Some(rng.bytes(LARGE_VALUE)),
        })
        .collect()
}

fn snapshot_acquisition(records: &[Owned], order: &[usize], probes: &[usize]) {
    const ACQUIRE: usize = 20_000;
    let engine = snapshot_base(records, order, snapshot_options(SNAP_BUDGET, false));
    let stats = engine.stats();
    let guard_ns = median_ns_of(SNAP_ROUNDS, ACQUIRE, || {
        for _ in 0..ACQUIRE {
            black_box(engine.read());
        }
    });
    let shared_ns = median_ns_of(SNAP_ROUNDS, ACQUIRE, || {
        for _ in 0..ACQUIRE {
            black_box(engine.snapshot().unwrap());
        }
    });
    let shared_allocs = allocations_during(|| drop(engine.snapshot().unwrap()));
    // Heap held by snapshots that share everything, per snapshot.
    let before = LIVE.load(Ordering::Relaxed);
    let held: Vec<_> = (0..15).map(|_| engine.snapshot().unwrap()).collect();
    let shared_heap = (LIVE.load(Ordering::Relaxed) - before) / 15;
    drop(held);

    // One put, then a snapshot of the table it went into.
    let mut tally = FlushTally::default();
    let mut after_put_ns = Vec::with_capacity(SNAP_PROBES);
    let mut after_put_allocs = 0;
    for (at, &index) in probes.iter().enumerate() {
        let record = &records[index];
        put_flushing(
            &engine,
            &record.key,
            record.value.as_deref().unwrap(),
            &mut tally,
        );
        let start = Instant::now();
        let snapshot = engine.snapshot().unwrap();
        after_put_ns.push(start.elapsed().as_nanos() as u64);
        drop(snapshot);
        if at == 0 {
            engine.put(&record.key, b"again").unwrap();
            after_put_allocs = allocations_during(|| drop(engine.snapshot().unwrap()));
        }
    }
    after_put_ns.sort_unstable();
    println!(
        "| {} runs, {} keys | {guard_ns:.0} | {shared_ns:.0} | {shared_allocs} | {shared_heap} | {} | {} | {after_put_allocs} | {} | {} |",
        stats.runs,
        records.len(),
        percentile(&after_put_ns, 0.5),
        percentile(&after_put_ns, 0.99),
        tally.flushes,
        engine.stats().frozen_tables,
    );
}

/// What a snapshot of an active table of a given size costs: acquisition,
/// which with one thread is the exclusive lock's hold time, and what later
/// writes pay to copy the nodes it shares.
fn snapshot_table_size(name: &str, records: &[Owned], order: &[usize], target: usize) {
    const TIMES: usize = 21;
    const OVERWRITES: usize = 1_000;
    let engine = Engine::new(snapshot_options(64 << 20, false));
    let mut filled = 0;
    for &index in order {
        if engine.stats().active.charged >= target {
            break;
        }
        let record = &records[index];
        engine
            .put(&record.key, record.value.as_deref().unwrap())
            .unwrap();
        filled += 1;
    }
    let active = engine.stats().active;
    let keys = &order[..filled];
    let mut rng = Rng(target as u64);
    let mut pick = || {
        let record = &records[keys[(rng.next() % keys.len() as u64) as usize]];
        (&record.key, record.value.as_deref().unwrap())
    };

    let mut acquire_ns = Vec::with_capacity(TIMES);
    for _ in 0..TIMES {
        let (key, value) = pick();
        engine.put(key, value).unwrap();
        let start = Instant::now();
        let snapshot = engine.snapshot().unwrap();
        acquire_ns.push(start.elapsed().as_nanos() as u64);
        drop(snapshot);
    }
    acquire_ns.sort_unstable();
    let (key, value) = pick();
    engine.put(key, value).unwrap();
    let acquire_allocs = allocations_during(|| drop(engine.snapshot().unwrap()));

    // Overwrites with no snapshot, after a fresh snapshot each, and under one
    // snapshot held throughout. Values keep their length.
    let timed = |each: &mut dyn FnMut()| {
        let start = Instant::now();
        for _ in 0..OVERWRITES {
            each();
        }
        start.elapsed().as_nanos() as f64 / OVERWRITES as f64
    };
    let plain_ns = timed(&mut || {
        let (key, value) = pick();
        engine.put(key, value).unwrap();
    });
    let mut fresh_ns = 0;
    let mut fresh_allocs = 0;
    for at in 0..OVERWRITES {
        let snapshot = engine.snapshot().unwrap();
        let (key, value) = pick();
        let start = Instant::now();
        if at == 0 {
            fresh_allocs = allocations_during(|| {
                engine.put(key, value).unwrap();
            });
        } else {
            engine.put(key, value).unwrap();
        }
        fresh_ns += start.elapsed().as_nanos();
        drop(snapshot);
    }
    let held = engine.snapshot().unwrap();
    let held_ns = timed(&mut || {
        let (key, value) = pick();
        engine.put(key, value).unwrap();
    });
    let pinned = engine.stats().active.pinned;
    let before = LIVE.load(Ordering::Relaxed);
    let start = Instant::now();
    drop(held);
    let drop_us = start.elapsed().as_nanos() as f64 / 1e3;
    let freed = before.saturating_sub(LIVE.load(Ordering::Relaxed));
    println!(
        "| {name} | {:.0} | {filled} | {} | {} | {acquire_allocs} | {plain_ns:.0} | {:.0} | {fresh_allocs} | {held_ns:.0} | {:.0} | {:.0} | {drop_us:.0} |",
        active.charged as f64 / 1024.0,
        percentile(&acquire_ns, 0.5),
        acquire_ns.last().copied().unwrap_or(0),
        fresh_ns as f64 / OVERWRITES as f64,
        pinned as f64 / OVERWRITES as f64,
        freed as f64 / OVERWRITES as f64,
    );
}

fn snapshot_reads(records: &[Owned], order: &[usize], probes: &[usize]) {
    let engine = snapshot_base(records, order, snapshot_options(SNAP_BUDGET, false));
    // Overwrite the probed keys, so that reads merge a memtable and gets
    // find them in it.
    for &index in probes {
        engine.put(&records[index].key, b"{}").unwrap();
    }
    let groups: Vec<&[u8]> = probes
        .iter()
        .map(|&index| &records[index].key[..GROUP_LEN])
        .collect();
    let snapshot = engine.snapshot().unwrap();
    let mut workspace = EngineWorkspace::new();

    let mut short = [0.0; 3];
    let mut found = 0;
    short[0] = median_ns_of(SNAP_ROUNDS, SNAP_PROBES, || {
        for group in &groups {
            let guard = engine.read();
            found = drain(guard.scan_prefix(group, &mut workspace));
        }
    });
    short[1] = median_ns_of(SNAP_ROUNDS, SNAP_PROBES, || {
        for group in &groups {
            found = drain(snapshot.scan_prefix(group, &mut workspace));
        }
    });
    short[2] = median_ns_of(SNAP_ROUNDS, SNAP_PROBES, || {
        for group in &groups {
            let taken = engine.snapshot().unwrap();
            found = drain(taken.scan_prefix(group, &mut workspace));
        }
    });
    let mut all = 0;
    let long_guard = median_ns_of(SNAP_ROUNDS, 1, || {
        all = drain(engine.read().scan(&mut workspace));
    });
    let long_snapshot = median_ns_of(SNAP_ROUNDS, 1, || {
        all = drain(snapshot.scan(&mut workspace));
    });
    let get_engine = median_ns_of(SNAP_ROUNDS, SNAP_PROBES, || {
        for &index in probes {
            black_box(engine.get(&records[index].key, &mut workspace).unwrap());
        }
    });
    let get_snapshot = median_ns_of(SNAP_ROUNDS, SNAP_PROBES, || {
        for &index in probes {
            black_box(snapshot.get(&records[index].key, &mut workspace).unwrap());
        }
    });
    let scan_allocs = allocations_during(|| {
        drain(snapshot.scan_prefix(groups[0], &mut workspace));
    });
    println!(
        "| {found} per group, {all} in all | {:.0} | {:.0} | {:.0} | {:.1} | {:.1} | {get_engine:.0} | {get_snapshot:.0} | {scan_allocs} |",
        short[0],
        short[1],
        short[2],
        long_guard / all as f64,
        long_snapshot / all as f64,
    );
}

#[derive(Clone, Copy, PartialEq)]
enum Reader {
    None,
    Guard,
    Snapshot,
}

/// What one reader thread did while the writer ran.
#[derive(Default)]
struct ReaderTally {
    scans: u64,
    retained: usize,
    frozen: usize,
    acquire_ns: Vec<u64>,
}

/// A writer overwrites one key in `CONTENTION_SHARE` once, flushing and
/// compacting synchronously, while `readers` threads repeat full scans.
fn snapshot_contention(
    records: &[Owned],
    order: &[usize],
    rewrite: &[usize],
    kind: Reader,
    readers: usize,
) {
    let engine = snapshot_base(records, order, snapshot_options(CONTENTION_BUDGET, false));
    let done = AtomicBool::new(false);
    let mut outcome = None;
    let heap = {
        let engine = &engine;
        let done = &done;
        peak_during(|| {
            outcome = Some(std::thread::scope(|scope| {
                let handles: Vec<_> = (0..if kind == Reader::None { 0 } else { readers })
                    .map(|_| {
                        scope.spawn(move || {
                            let mut workspace = EngineWorkspace::new();
                            let mut tally = ReaderTally::default();
                            while !done.load(Ordering::Relaxed) {
                                if kind == Reader::Guard {
                                    black_box(drain(engine.read().scan(&mut workspace)));
                                    tally.scans += 1;
                                    continue;
                                }
                                let start = Instant::now();
                                let snapshot = engine.snapshot().unwrap();
                                tally.acquire_ns.push(start.elapsed().as_nanos() as u64);
                                black_box(drain(snapshot.scan(&mut workspace)));
                                let stats = engine.stats();
                                tally.retained = tally.retained.max(stats.retained_bytes);
                                tally.frozen = tally.frozen.max(stats.frozen_tables);
                                tally.scans += 1;
                            }
                            tally
                        })
                    })
                    .collect();
                let start = Instant::now();
                let mut tally = FlushTally::default();
                let mut latency = Vec::with_capacity(rewrite.len() / LATENCY_EVERY + 1);
                let (mut rewritten, mut compact_publish) = (0, Vec::new());
                for (at, &index) in rewrite.iter().enumerate() {
                    let record = &records[index];
                    let flushes = tally.flushes;
                    let put = Instant::now();
                    put_flushing(
                        engine,
                        &record.key,
                        record.value.as_deref().unwrap(),
                        &mut tally,
                    );
                    if tally.flushes != flushes {
                        let (bytes, publish) = compact_done(engine);
                        rewritten += bytes;
                        compact_publish.extend(publish);
                    } else if at % LATENCY_EVERY == 0 {
                        latency.push(put.elapsed().as_nanos() as u64);
                    }
                }
                let write_s = start.elapsed().as_secs_f64();
                done.store(true, Ordering::Relaxed);
                let reads: Vec<ReaderTally> =
                    handles.into_iter().map(|h| h.join().unwrap()).collect();
                ((write_s, tally, latency, rewritten, compact_publish), reads)
            }));
        })
    };
    let (writer, reads) = outcome.unwrap();
    let (write_s, mut tally, mut latency, rewritten, mut compact_publish) = writer;
    latency.sort_unstable();
    tally.publish_ns.sort_unstable();
    compact_publish.sort_unstable();
    let scans: u64 = reads.iter().map(|r| r.scans).sum();
    let retained = reads.iter().map(|r| r.retained).max().unwrap_or(0);
    let frozen = reads.iter().map(|r| r.frozen).max().unwrap_or(0);
    let mut acquire: Vec<u64> = reads.iter().flat_map(|r| r.acquire_ns.clone()).collect();
    acquire.sort_unstable();
    let stats = engine.stats();
    let name = match kind {
        Reader::None => "no reader",
        Reader::Guard => "guard",
        Reader::Snapshot => "snapshot",
    };
    println!(
        "| {name} | {readers} | {:.2} | {} | {} | {:.0} | {} | {:.1} | {:.1} | {:.1} | {:.1} | {} | {:.1} | {:.1} | {} | {:.1} | {:.1} | {} | {frozen} | {:.1} | {:.1} |",
        rewrite.len() as f64 / write_s / 1e6,
        percentile(&latency, 0.5),
        percentile(&latency, 0.99),
        latency.last().copied().unwrap_or(0) as f64 / 1e3,
        tally.flushes,
        percentile(&tally.publish_ns, 0.5) as f64 / 1e3,
        tally.publish_ns.last().copied().unwrap_or(0) as f64 / 1e3,
        percentile(&compact_publish, 0.5) as f64 / 1e3,
        compact_publish.last().copied().unwrap_or(0) as f64 / 1e3,
        scans,
        percentile(&acquire, 0.5) as f64 / 1e3,
        acquire.last().copied().unwrap_or(0) as f64 / 1e3,
        tally.runs,
        mib(tally.bytes),
        mib(rewritten),
        stats.runs,
        mib(retained),
        heap as f64 / (1 << 20) as f64,
    );
}

/// One dataset for the frequency experiment.
struct Workload {
    name: &'static str,
    records: Vec<Owned>,
    order: Vec<usize>,
    /// The overwrites of the measured pass, in order.
    rewrite: Vec<usize>,
}

/// A writer runs `workload.rewrite`, taking a snapshot every `every` puts
/// (never if 0). Each snapshot scans one 200-key group and, every tenth
/// time, one 4,000-key tenant, gets one key, and is kept for `hold` further
/// puts (`usize::MAX`: the first snapshot is kept for the whole pass and the
/// others dropped at once). Then flushing and compaction complete, so every
/// configuration ends with nothing eligible.
fn snapshot_frequency(
    workload: &Workload,
    budget: usize,
    background: bool,
    every: usize,
    hold: usize,
) {
    let records = &workload.records;
    let engine = snapshot_base(
        records,
        &workload.order,
        snapshot_options(budget, background),
    );
    let compacted_before = engine
        .background_status()
        .map_or(0, |status| status.input_bytes);
    let mut tally = FlushTally::default();
    let mut workspace = EngineWorkspace::new();
    let mut snapshots = 0;
    let mut acquire = Vec::new();
    let mut latency = Vec::with_capacity(workload.rewrite.len() / LATENCY_EVERY + 1);
    let (mut group_ns, mut tenant_ns, mut tenants, mut get_ns, mut drop_ns) =
        (0u128, 0u128, 0, 0u128, 0u128);
    let (mut peak_frozen, mut peak_retained, mut peak_pinned, mut peak_live) = (0, 0, 0, 0);
    let mut kept: std::collections::VecDeque<(usize, tessera::engine::Snapshot)> =
        std::collections::VecDeque::new();
    let mut written_at = None;
    let mut completion = None;
    let heap = peak_during(|| {
        let start = Instant::now();
        for (at, &index) in workload.rewrite.iter().enumerate() {
            let record = &records[index];
            let put = Instant::now();
            put_flushing(
                &engine,
                &record.key,
                record.value.as_deref().unwrap(),
                &mut tally,
            );
            if at % LATENCY_EVERY == 0 {
                latency.push(put.elapsed().as_nanos() as u64);
            }
            while kept
                .front()
                .is_some_and(|&(taken, _)| hold != usize::MAX && taken + hold <= at)
            {
                let dropped = Instant::now();
                kept.pop_front();
                drop_ns += dropped.elapsed().as_nanos();
            }
            if every == 0 || at % every != every - 1 {
                continue;
            }
            let begun = Instant::now();
            let snapshot = engine.snapshot().unwrap();
            acquire.push(begun.elapsed().as_nanos() as u64);
            snapshots += 1;
            let stats = engine.stats();
            peak_frozen = peak_frozen.max(stats.frozen_tables);
            peak_retained = peak_retained.max(stats.retained_bytes);
            peak_pinned = peak_pinned.max(stats.active.pinned);
            peak_live = peak_live.max(stats.snapshots);
            let scan = Instant::now();
            black_box(drain(
                snapshot.scan_prefix(&record.key[..GROUP_LEN], &mut workspace),
            ));
            group_ns += scan.elapsed().as_nanos();
            if snapshots % 10 == 1 {
                let scan = Instant::now();
                black_box(drain(
                    snapshot.scan_prefix(&record.key[..TENANT_LEN], &mut workspace),
                ));
                tenant_ns += scan.elapsed().as_nanos();
                tenants += 1;
            }
            let get = Instant::now();
            black_box(snapshot.get(&record.key, &mut workspace).unwrap());
            get_ns += get.elapsed().as_nanos();
            if hold == 0 || (hold == usize::MAX && snapshots > 1) {
                let dropped = Instant::now();
                drop(snapshot);
                drop_ns += dropped.elapsed().as_nanos();
            } else {
                kept.push_back((at, snapshot));
            }
        }
        let write_s = start.elapsed().as_secs_f64();
        written_at = Some((write_s, engine.stats()));
        let dropped = Instant::now();
        kept.clear();
        drop_ns += dropped.elapsed().as_nanos();
        tally.flush(&engine);
        if background {
            engine.wait_for_compaction().unwrap();
        }
        let (rewritten, _) = compact_done(&engine);
        completion = Some((start.elapsed().as_secs_f64(), rewritten));
    });
    let (write_s, written) = written_at.unwrap();
    let (total_s, sync_rewrite) = completion.unwrap();
    let rewritten = sync_rewrite
        + engine
            .background_status()
            .map_or(0, |status| status.input_bytes - compacted_before) as usize;
    let end = engine.stats();
    acquire.sort_unstable();
    latency.sort_unstable();
    let puts = workload.rewrite.len();
    let mean = |total: u128, count: usize| total as f64 / count.max(1) as f64 / 1e3;
    let every = if every == 0 {
        "never".to_owned()
    } else {
        every.to_string()
    };
    let hold = match hold {
        0 => "0".to_owned(),
        usize::MAX => "first, whole pass".to_owned(),
        puts => puts.to_string(),
    };
    println!(
        "| {} | {} | {} | {every} | {hold} | {snapshots} | {peak_live} | {} | {} | {peak_frozen} | {} | {} | {:.1} | {} | {} | {} | {:.1} | {:.2} | {} | {} | {:.2} | {:.2} | {:.2} | {} | {:.0} | {:.1} | {:.0} | {:.0} | {:.1} | {:.0} | {:.1} | {:.1} | {:.1} |",
        workload.name,
        budget >> 20,
        if background { "background" } else { "catch-up" },
        percentile(&acquire, 0.5),
        percentile(&acquire, 0.99),
        tally.flushes,
        tally.runs,
        mib(tally.bytes),
        written.runs,
        written.max_overlap,
        tally.stalls,
        mib(rewritten),
        rewritten as f64 / tally.bytes.max(1) as f64,
        end.runs,
        end.max_overlap,
        write_s,
        total_s,
        puts as f64 / total_s / 1e6,
        percentile(&latency, 0.99),
        latency.last().copied().unwrap_or(0) as f64 / 1e3,
        mean(group_ns, snapshots),
        mean(tenant_ns, tenants),
        mean(get_ns, snapshots) * 1e3,
        mean(drop_ns, snapshots),
        peak_pinned as f64 / 1024.0,
        mib(peak_retained),
        mib(written.retained_bytes),
        heap as f64 / (1 << 20) as f64,
    );
}

/// What one snapshot keeps once everything it saw is replaced, and the cost
/// of freeing it.
fn snapshot_reclamation(records: &[Owned], order: &[usize], rewrite: &[usize]) {
    let engine = snapshot_base(records, order, snapshot_options(SNAP_BUDGET, false));
    let mut tally = FlushTally::default();
    for &index in rewrite.iter().step_by(16) {
        engine.put(&records[index].key, b"{}").unwrap();
    }
    let charged = engine.stats().active.charged;
    let start = Instant::now();
    let snapshot = engine.snapshot().unwrap();
    let acquire_us = start.elapsed().as_nanos() as f64 / 1e3;
    let (view_runs, view_bytes) = (engine.stats().runs, engine.stats().run_bytes);
    let mut peak_pinned = 0;
    for &index in rewrite {
        let record = &records[index];
        put_flushing(
            &engine,
            &record.key,
            record.value.as_deref().unwrap(),
            &mut tally,
        );
        peak_pinned = peak_pinned.max(engine.stats().active.pinned);
    }
    tally.flush(&engine);
    compact_done(&engine);
    let held = engine.stats();
    let before = LIVE.load(Ordering::Relaxed);
    let start = Instant::now();
    drop(snapshot);
    let drop_us = start.elapsed().as_nanos() as f64 / 1e3;
    let freed = before.saturating_sub(LIVE.load(Ordering::Relaxed));
    let after = engine.stats();
    println!(
        "| {view_runs} runs of {:.1} MiB + a {:.1} MiB table | {acquire_us:.1} | {} | {:.1} | {:.1} | {} | {:.1} | {drop_us:.0} | {} | {} |",
        mib(view_bytes),
        mib(charged),
        tally.flushes,
        mib(peak_pinned),
        mib(held.retained_bytes),
        held.retained_sources,
        mib(freed as usize),
        after.retained_sources,
        after.retained_bytes,
    );
}

fn snapshot_bench() {
    let records = tree_json(16 << 20);
    let count = records.len();
    let order = shuffled(count, 700);
    let rewrite = shuffled(count, 701);
    let probes: Vec<usize> = shuffled(count, 702).into_iter().take(SNAP_PROBES).collect();
    println!(
        "{count} json keys, flushed and compacted with default options into 4 MiB memtables; median of {SNAP_ROUNDS} rounds where timed in rounds; default snapshot limits {:?}",
        SnapshotLimits::default()
    );
    println!();
    println!(
        "acquisition: ns per guard or snapshot; heap is bytes a held snapshot allocates; after a put, the snapshot takes a new view of the active table"
    );
    println!();
    println!(
        "| state | guard ns | shared snapshot ns | allocs | heap B | after-put p50 ns | after-put p99 ns | allocs | flushes forced | frozen tables |"
    );
    println!("|{}", "---|".repeat(10));
    for _ in 0..SNAP_ROUNDS {
        snapshot_acquisition(&records, &order, &probes);
    }

    let large = snapshot_large_values();
    let large_order = shuffled(large.len(), 703);
    println!();
    println!(
        "table size: an active table filled to about the size shown; acquire follows one put, so each takes a new view (one thread, so the time is the exclusive lock's hold time); then 1,000 overwrites of random keys each with no snapshot, after a fresh snapshot each (dropped after the put), and under one snapshot held throughout; pinned and freed are per overwrite under the held snapshot; json values average about 90 bytes, large values are {LARGE_VALUE} random bytes"
    );
    println!();
    println!(
        "| values | charged KiB | keys | acquire p50 ns | acquire max ns | acquire allocs | put ns, no snapshot | put ns, fresh snapshot each | allocs | put ns, one held snapshot | pinned B/overwrite | heap freed B/overwrite | held drop us |"
    );
    println!("|{}", "---|".repeat(13));
    for target in [16 << 10, 256 << 10, 1 << 20, 4 << 20, 16 << 20] {
        snapshot_table_size("json", &records, &order, target);
        snapshot_table_size("large", &large, &large_order, target);
    }

    println!();
    println!(
        "reads with {SNAP_PROBES} keys overwritten in the memtable: ns per group scan (200 keys), ns per record of a full scan, ns per get"
    );
    println!();
    println!(
        "| records | guard group scan | held snapshot group scan | snapshot taken per group scan | guard full scan /record | snapshot full scan /record | engine get | snapshot get | allocs per snapshot scan |"
    );
    println!("|{}", "---|".repeat(9));
    for _ in 0..SNAP_ROUNDS {
        snapshot_reads(&records, &order, &probes);
    }
    println!();
    println!(
        "contention: 1 MiB memtables; the writer overwrites one key in {CONTENTION_SHARE} once, flushing at a full memtable and then compacting until nothing is eligible; readers repeat full scans until it is done"
    );
    println!(
        "write latency is every {LATENCY_EVERY}th put that did not flush; publish times are the lock wait of each flush or compaction publication; acquire is the reader's snapshot call, including any wait for the lock; heap is the Rust heap peak above the base state"
    );
    println!();
    println!(
        "| readers | threads | puts M/s | put p50 ns | put p99 ns | put max us | writer flushes | flush publish p50 us | flush publish max us | compaction publish p50 us | compaction publish max us | full scans | acquire p50 us | acquire max us | runs flushed | flushed MiB | compaction rewrite MiB | runs left | peak frozen | peak retained MiB | peak heap MiB |"
    );
    println!("|{}", "---|".repeat(21));
    let share = &rewrite[..count / CONTENTION_SHARE];
    for _ in 0..SNAP_ROUNDS {
        for (kind, readers) in [
            (Reader::None, 0),
            (Reader::Guard, 1),
            (Reader::Snapshot, 1),
            (Reader::Snapshot, 2),
        ] {
            snapshot_contention(&records, &order, share, kind, readers);
        }
    }
    println!();
    println!(
        "reclamation: a snapshot of the base state plus one memtable, then every key is overwritten, flushed and compacted away; heap freed is the Rust heap released by dropping it"
    );
    println!();
    println!(
        "| snapshot of | acquire us | flushes while held | peak pinned MiB | retained MiB | sources retained | heap freed MiB | drop us | sources left | bytes left |"
    );
    println!("|{}", "---|".repeat(10));
    for _ in 0..SNAP_ROUNDS {
        snapshot_reclamation(&records, &order, &rewrite);
    }
}

fn snapshot_frequency_bench() {
    let json = tree_json(16 << 20);
    let count = json.len();
    let workloads = [
        Workload {
            name: "json",
            order: shuffled(count, 700),
            rewrite: shuffled(count, 701),
            records: json,
        },
        Workload {
            name: "large",
            order: shuffled(LARGE_KEYS as usize, 703),
            rewrite: (0..LARGE_PASSES)
                .flat_map(|pass| shuffled(LARGE_KEYS as usize, 704 + pass))
                .collect(),
            records: snapshot_large_values(),
        },
    ];
    println!(
        "frequency: each row starts from every key flushed and compacted, then runs one overwrite pass ({count} json puts, or {} passes over {LARGE_KEYS} keys of {LARGE_VALUE}-byte values) with a snapshot every N puts",
        LARGE_PASSES
    );
    println!(
        "each snapshot scans one 200-key group, every tenth also a 4,000-key tenant, gets one key, and is kept for the puts shown (0: dropped at once)"
    );
    println!(
        "catch-up: no compaction during the pass, max_runs 4096, then compaction until nothing is eligible; background: the worker with default options and the default max_runs {}, a flush refused at it waits for compaction (a stall)",
        EngineOptions::default().max_runs
    );
    println!(
        "total s runs from the first put until flushing and compaction leave nothing eligible, and puts M/s is over that time; snapshot limits {:?}; {FREQUENCY_ROUNDS} rounds",
        SnapshotLimits::default(),
    );
    println!();
    println!(
        "| values | budget MiB | compaction | every | held for | snapshots | peak live | acquire p50 ns | acquire p99 ns | peak frozen | flushes | runs flushed | flushed MiB | runs after writes | overlap after writes | stalls | rewrite MiB | rewrite / flushed | runs at end | overlap at end | write s | total s | puts M/s | put p99 ns | put max us | group scan us | tenant scan us | get ns | drop us | peak pinned KiB | peak retained MiB | retained after writes MiB | peak heap MiB |"
    );
    println!("|{}", "---|".repeat(33));
    for _ in 0..FREQUENCY_ROUNDS {
        for workload in &workloads {
            for budget in [1 << 20, 4 << 20, 16 << 20] {
                for background in [false, true] {
                    for every in [0, 10_000, 1_000, 100] {
                        snapshot_frequency(workload, budget, background, every, 0);
                    }
                }
            }
        }
        // Lifetimes: snapshots every 1,000 puts kept for longer, up to the
        // live limit, and one kept for the whole pass beside short ones.
        let json = &workloads[0];
        for hold in [4_000, 15_000, usize::MAX] {
            for budget in [4 << 20, 16 << 20] {
                snapshot_frequency(json, budget, true, 1_000, hold);
            }
        }
    }
}

fn main() {
    match std::env::args().nth(1).as_deref() {
        Some("geometry") => return geometry(),
        Some("prefix") => return prefix_scans(),
        Some("merge") => return merge_scans(),
        Some("logical") => return logical_reads(),
        Some("ranges") => return range_reads(),
        Some("engine") => return engine_baseline(),
        Some("compaction") => return compaction_sustained(),
        Some("snapshot") => return snapshot_bench(),
        Some("snapshot-frequency") => return snapshot_frequency_bench(),
        Some("memtable") => return memtable_baseline(),
        Some("memtable-memory") => return memtable_memory(),
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
