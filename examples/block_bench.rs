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
use tessera::run::{Run, RunBuilder};

struct CountingAllocator;

static ALLOCATIONS: AtomicU64 = AtomicU64::new(0);

// SAFETY: every call forwards unchanged to the system allocator.
unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        ALLOCATIONS.fetch_add(1, Ordering::Relaxed);
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        ALLOCATIONS.fetch_add(1, Ordering::Relaxed);
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
const RUN_ROUNDS: usize = 31;

/// Distinct keys probed per round, drawn evenly from the shuffled key list.
const RUN_PROBES: usize = 2000;

fn build_run(records: &[Owned]) -> Run<Vec<u8>> {
    let mut builder = RunBuilder::new();
    for record in records {
        builder
            .add(&record.key, record.seq, record.value())
            .unwrap();
    }
    builder.finish()
}

fn measure_run(name: &str, records: &[Owned]) {
    let run = build_run(records);
    assert_eq!(run.len(), records.len());
    let bytes = run.as_bytes();
    let view = run.view();
    let single = build(records);
    let single = Block::new(single.as_slice()).unwrap();

    let mut rng = Rng(77);
    let mut keys: Vec<&[u8]> = records.iter().map(|r| r.key.as_slice()).collect();
    keys.dedup();
    for i in (1..keys.len()).rev() {
        keys.swap(i, (rng.next() % (i as u64 + 1)) as usize);
    }
    keys.truncate(RUN_PROBES);
    let misses: Vec<Vec<u8>> = keys
        .iter()
        .map(|k| [*k, &[0][..]].concat())
        .filter(|k| run.get(k).is_none())
        .collect();

    let timed = |operations, round: &mut dyn FnMut()| median_ns_of(RUN_ROUNDS, operations, round);
    let build_ns = timed(records.len(), &mut || {
        black_box(build_run(black_box(records)));
    });
    let validate_ns = timed(records.len(), &mut || {
        black_box(Run::new(black_box(bytes)).unwrap());
    });
    let scan = || {
        let mut cursor = run.cursor();
        let mut total = 0usize;
        for _ in 0..records.len() {
            total += cursor.next_record().expect("record").key.len();
        }
        assert!(cursor.next_record().is_none());
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
            black_box(run.get(black_box(key)));
        }
    });
    let miss_ns = timed(misses.len(), &mut || {
        for key in &misses {
            black_box(run.get(black_box(key)));
        }
    });
    let single_get_ns = timed(keys.len(), &mut || {
        for key in &keys {
            black_box(single.get(black_box(key)));
        }
    });
    let single_scan_ns = timed(records.len(), &mut || {
        let mut cursor = single.cursor();
        let mut total = 0usize;
        for _ in 0..records.len() {
            total += cursor.next_record().expect("record").key.len();
        }
        black_box(total);
    });

    let build_allocs = allocations_during(|| drop(build_run(records)));
    let validate_allocs = allocations_during(|| {
        black_box(Run::new(bytes).unwrap());
    });
    let scan_allocs = allocations_during(scan);
    let lookup_allocs = allocations_during(|| {
        for key in &keys {
            black_box(run.get(key));
        }
        for key in &misses {
            black_box(run.get(key));
        }
    });

    let blocks = run.block_count();
    let index = view.index_len() + 28;
    let frames = 12 * blocks;
    let block_bytes = bytes.len() - index - frames;
    println!(
        "| {name} | {} | {} | {blocks} | {} | {frames} | {index} | {:.3}% | {build_ns:.0} | {validate_ns:.1} | {scan_ns:.1} | {locate_ns:.0} | {hit_ns:.0} | {miss_ns:.0} | {single_get_ns:.0} | {single_scan_ns:.1} | {build_allocs}/{validate_allocs}/{scan_allocs}/{lookup_allocs} |",
        records.len(),
        bytes.len(),
        block_bytes / blocks,
        (frames + index) as f64 / bytes.len() as f64 * 100.0,
    );
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
        "runs at the default 64 KiB block target; median of {RUN_ROUNDS} rounds, {RUN_PROBES} probes per round"
    );
    println!(
        "ns per record (build, validate, scan) or per lookup; one-block = same data as a single block"
    );
    println!(
        "index = table + keys + footer; allocs = build/validate/scan/all lookups, each counted once"
    );
    println!(
        "runtime descriptor: {} bytes for Run<Vec<u8>>",
        size_of::<Run<Vec<u8>>>()
    );
    println!();
    println!(
        "| dataset | records | encoded | blocks | avg block | frame hdrs | index | overhead | build | validate | scan | locate | get hit | get miss | one-block get | one-block scan | allocs |"
    );
    println!("|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|");
    for mib in [1, 16] {
        let target = mib << 20;
        measure_run(&format!("structured {mib}M"), &structured_keys(target));
        measure_run(&format!("random {mib}M"), &random_keys(target));
        measure_run(&format!("small-value {mib}M"), &small_values(target));
        measure_run(&format!("multi-version {mib}M"), &multi_version(target));
    }
}
