//! Repeatable block and run measurements on deterministic datasets.
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

use tessera::block::{Block, BlockBuilder, BlockPosition, BlockView, Value};
use tessera::run::{Codec, ReadWorkspace, Run, RunBuilder};

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

/// The `(codec, decoded length, stored bytes)` of each frame, from the
/// documented layout, so codec costs can be timed apart from the reader.
fn frames(bytes: &[u8]) -> Vec<(u8, usize, &[u8])> {
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
            (bytes[start], u32_at(start + 4), &bytes[start + 12..end])
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
        for (_, _, payload) in &stored {
            black_box(crc32fast::hash(black_box(payload)));
        }
    });
    let decompress_ns = timed(stored.len(), &mut || {
        for (id, len, payload) in &stored {
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

fn main() {
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
