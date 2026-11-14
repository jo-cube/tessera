//! Read-path experiments: point lookups, snapshots, prefix scans and
//! reader-workspace memory against the number of published runs, for disjoint
//! and overlapping run layouts.
//!
//! ```text
//! read_path lookups|scans [RUNS...]
//! ```
//!
//! It uses only the public API, so the same file builds against an older
//! revision for an A/B comparison. Run it through
//! `scripts/validate.sh exec SECONDS /cache/target/release/examples/read_path MODE`
//! after a release build of the example; it writes no files.
//!
//! Method:
//!
//! - Every engine holds the same 200,000 keys, written in two phases. The
//!   first writes every key and the second overwrites one key in ten and
//!   deletes another one in ten. The writes are flushed into exactly `RUNS`
//!   runs, with no compaction. In the disjoint layout each phase writes in
//!   key order, so a run covers one key slice and the second phase's runs
//!   overlap the first's. In the overlapping layout both phases write in a
//!   fixed permutation, so every run spans the whole key space and only the
//!   filters and sequence bounds separate them.
//! - Probes are generated before timing. The mean is the median, over
//!   batches of 500 probes, of the mean time per probe in a batch. Timing a
//!   batch keeps the clock's own cost out of sub-microsecond figures. The
//!   p50 and p99 time each probe on its own, once, so they include the
//!   clock's cost and resolution.
//! - The workspace is warmed with one pass over every probe class before
//!   anything is timed or counted. Rust heap allocations are counted during
//!   the timed batches only.
//! - Workspace memory is measured on a fresh workspace. `decoded` is
//!   `EngineWorkspace::capacity`, `heap` the change in live Rust heap bytes,
//!   and `native` the change in bytes `malloc` has handed out (glibc's
//!   `mallinfo2`) minus `heap`: what Zstandard allocated for decompression
//!   contexts outside the Rust heap. A workspace never shrinks, so this is
//!   both its peak and what it retains. Resident memory is not used: the
//!   allocator reuses pages the build freed, and Zstandard does not touch
//!   every page of a context, so it under-reports.
//! - One pass is a single measurement. Compare revisions by alternating
//!   them, and only within one environment.

use std::alloc::{GlobalAlloc, Layout, System};
use std::hint::black_box;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::Instant;

use tessera::engine::{Engine, EngineOptions, EngineWorkspace};

struct Counting;

static ALLOCATIONS: AtomicU64 = AtomicU64::new(0);
static LIVE: AtomicUsize = AtomicUsize::new(0);

// SAFETY: every call forwards unchanged to the system allocator.
unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        ALLOCATIONS.fetch_add(1, Ordering::Relaxed);
        LIVE.fetch_add(layout.size(), Ordering::Relaxed);
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        LIVE.fetch_sub(layout.size(), Ordering::Relaxed);
        unsafe { System.dealloc(ptr, layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        ALLOCATIONS.fetch_add(1, Ordering::Relaxed);
        LIVE.fetch_add(new_size, Ordering::Relaxed);
        LIVE.fetch_sub(layout.size(), Ordering::Relaxed);
        unsafe { System.realloc(ptr, layout, new_size) }
    }
}

#[global_allocator]
static ALLOCATOR: Counting = Counting;

const KEYS: u64 = 200_000;
const BATCH: usize = 500;
const BATCHES: usize = 20;
/// Coprime with `KEYS`, so `i * STRIDE % KEYS` is a permutation.
const STRIDE: u64 = 104_729;

struct Rng(u64);

impl Rng {
    fn new(seed: u64) -> Self {
        Self(seed.wrapping_mul(0x9e37_79b9_7f4a_7c15) | 1)
    }

    fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.0 = x;
        x
    }

    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
}

const WORDS: [&str; 16] = [
    "alpha", "bravo", "charlie", "delta", "echo", "foxtrot", "golf", "hotel", "india", "juliet",
    "kilo", "lima", "mike", "november", "oscar", "papa",
];

/// JSON-like values of about 150 to 500 bytes. `version` changes the value
/// an overwrite stores.
fn value(index: u64, version: u64) -> Vec<u8> {
    let mut rng = Rng::new(2_000 + index * 3 + version);
    let bio: Vec<&str> = (0..rng.below(60))
        .map(|_| WORDS[rng.below(16) as usize])
        .collect();
    format!(
        r#"{{"id":{index},"v":{version},"name":"{} {}","score":{},"plan":"{}","bio":"{}"}}"#,
        WORDS[rng.below(16) as usize],
        WORDS[rng.below(16) as usize],
        rng.below(100_000),
        ["free", "pro", "team"][rng.below(3) as usize],
        bio.join(" ")
    )
    .into_bytes()
}

/// Key `index` is present; the odd keys between them are never written.
fn key(index: u64) -> Vec<u8> {
    format!("user/{:010}", 2 * index).into_bytes()
}

fn missing_key(index: u64) -> Vec<u8> {
    format!("user/{:010}", 2 * index + 1).into_bytes()
}

fn overwritten(index: u64) -> bool {
    index % 10 == 1
}

fn deleted(index: u64) -> bool {
    index % 10 == 2
}

#[derive(Clone, Copy, PartialEq)]
enum Shape {
    Disjoint,
    Overlapping,
}

impl Shape {
    fn name(self) -> &'static str {
        match self {
            Shape::Disjoint => "disjoint",
            Shape::Overlapping => "overlapping",
        }
    }

    /// The `j`th key index a phase writes.
    fn order(self, j: u64) -> u64 {
        match self {
            Shape::Disjoint => j,
            Shape::Overlapping => j * STRIDE % KEYS,
        }
    }
}

/// An engine whose two write phases are flushed into exactly `runs` runs.
fn build(shape: Shape, runs: u64) -> Engine {
    let engine = Engine::new(EngineOptions {
        memtable_budget: 1 << 30,
        max_runs: 1 << 20,
        max_run_bytes: usize::MAX / 4,
        background_compaction: None,
        ..EngineOptions::default()
    })
    .unwrap();
    let updates: Vec<u64> = (0..KEYS)
        .map(|j| shape.order(j))
        .filter(|&i| overwritten(i) || deleted(i))
        .collect();
    let writes = KEYS + updates.len() as u64;
    let mut written = 0u64;
    let mut flushed = 0u64;
    let mut wrote = |engine: &Engine| {
        written += 1;
        // Flush when the next run's share of the writes is complete.
        if written * runs >= (flushed + 1) * writes {
            engine.flush().unwrap();
            flushed += 1;
        }
    };
    for j in 0..KEYS {
        let i = shape.order(j);
        engine.put(&key(i), &value(i, 0)).unwrap();
        wrote(&engine);
    }
    for &i in &updates {
        if overwritten(i) {
            engine.put(&key(i), &value(i, 1)).unwrap();
        } else {
            engine.delete(&key(i)).unwrap();
        }
        wrote(&engine);
    }
    let stats = engine.stats();
    assert_eq!(stats.runs as u64, runs, "{} runs", shape.name());
    assert_eq!(stats.frozen_tables, 0);
    engine
}

/// The median over batches of the mean nanoseconds per call in a batch.
fn median(mut batches: Vec<f64>) -> f64 {
    batches.sort_by(f64::total_cmp);
    batches[batches.len() / 2]
}

/// Bytes `malloc` has handed out and not had back, or 0 where glibc's
/// `mallinfo2` is unavailable.
#[cfg(all(target_os = "linux", target_env = "gnu"))]
fn malloc_bytes() -> i64 {
    #[repr(C)]
    struct Mallinfo2 {
        arena: usize,
        ordblks: usize,
        smblks: usize,
        hblks: usize,
        hblkhd: usize,
        usmblks: usize,
        fsmblks: usize,
        uordblks: usize,
        fordblks: usize,
        keepcost: usize,
    }
    unsafe extern "C" {
        fn mallinfo2() -> Mallinfo2;
    }
    // SAFETY: mallinfo2 takes no arguments and returns a plain struct.
    let info = unsafe { mallinfo2() };
    (info.uordblks + info.hblkhd) as i64
}

#[cfg(not(all(target_os = "linux", target_env = "gnu")))]
fn malloc_bytes() -> i64 {
    0
}

fn live_heap() -> i64 {
    LIVE.load(Ordering::Relaxed) as i64
}

fn allocations() -> u64 {
    ALLOCATIONS.load(Ordering::Relaxed)
}

struct Probes {
    name: &'static str,
    keys: Vec<Vec<u8>>,
    /// The value length each probe must find, or `None` for absent.
    expect: Vec<Option<usize>>,
}

fn probes(seed: u64) -> Vec<Probes> {
    let mut rng = Rng::new(seed);
    let count = BATCH * BATCHES;
    let mut class =
        |name, pick: &dyn Fn(u64) -> bool, make: &dyn Fn(u64) -> (Vec<u8>, Option<usize>)| {
            let mut keys = Vec::with_capacity(count);
            let mut expect = Vec::with_capacity(count);
            while keys.len() < count {
                let i = rng.below(KEYS);
                if pick(i) {
                    let (key, found) = make(i);
                    keys.push(key);
                    expect.push(found);
                }
            }
            Probes { name, keys, expect }
        };
    vec![
        class("present", &|i| !overwritten(i) && !deleted(i), &|i| {
            (key(i), Some(value(i, 0).len()))
        }),
        class("overwritten", &overwritten, &|i| {
            (key(i), Some(value(i, 1).len()))
        }),
        class("deleted", &deleted, &|i| (key(i), None)),
        class("missing", &|_| true, &|i| (missing_key(i), None)),
    ]
}

fn run_counts() -> Vec<u64> {
    let given: Vec<u64> = std::env::args()
        .skip(2)
        .map(|arg| arg.parse().expect("a run count"))
        .collect();
    if given.is_empty() {
        vec![4, 64, 256, 1024, 2048]
    } else {
        given
    }
}

/// Times `batches` batches of `per_batch` calls of `call`, and returns the
/// median of the mean nanoseconds per call and the Rust heap allocations per
/// call. The figures go into preallocated space, so only `call` allocates.
fn time(batches: usize, per_batch: usize, mut call: impl FnMut(usize)) -> (f64, f64) {
    let mut means = Vec::with_capacity(batches);
    let before = allocations();
    for batch in 0..batches {
        let start = Instant::now();
        for index in 0..per_batch {
            call(batch * per_batch + index);
        }
        means.push(start.elapsed().as_nanos() as f64 / per_batch as f64);
    }
    let allocations = (allocations() - before) as f64 / (batches * per_batch) as f64;
    (median(means), allocations)
}

/// Times each of `calls` calls of `call` on its own and returns the p50 and
/// p99 in nanoseconds. The clock's resolution limits sub-microsecond figures.
fn percentiles(calls: usize, mut call: impl FnMut(usize)) -> (f64, f64) {
    let mut samples = Vec::with_capacity(calls);
    for index in 0..calls {
        let start = Instant::now();
        call(index);
        samples.push(start.elapsed().as_nanos() as f64);
    }
    samples.sort_by(f64::total_cmp);
    let at = |p: f64| samples[((samples.len() - 1) as f64 * p).round() as usize];
    (at(0.5), at(0.99))
}

/// Runs `pass` on a fresh workspace and returns it with the memory the pass
/// left it holding, in KiB: decoded blocks, Rust heap and native memory.
fn measured(pass: impl FnOnce(&mut EngineWorkspace)) -> (EngineWorkspace, [i64; 3]) {
    let mut workspace = EngineWorkspace::new();
    let (malloc, heap) = (malloc_bytes(), live_heap());
    pass(&mut workspace);
    let heap = live_heap() - heap;
    let native = malloc_bytes() - malloc - heap;
    let decoded = workspace.capacity() as i64;
    (workspace, [decoded >> 10, heap >> 10, native >> 10])
}

fn lookups_mode() {
    println!(
        "| layout | runs | present ns mean / p50 / p99 | overwritten | deleted | missing | allocations per lookup | snapshot ns | allocations per snapshot | decoded KiB | heap KiB | native KiB |"
    );
    println!("|---|---|---|---|---|---|---|---|---|---|---|---|");
    let classes = probes(7);
    for shape in [Shape::Disjoint, Shape::Overlapping] {
        for runs in run_counts() {
            let engine = build(shape, runs);

            // Memory, and the warm-up: one checked pass over every class.
            let (mut workspace, memory) = measured(|workspace| {
                for class in &classes {
                    for (key, expect) in class.keys.iter().zip(&class.expect) {
                        let found = engine.get(key, workspace).unwrap();
                        let found = found.map(|found| found.value.len());
                        assert_eq!(found, *expect, "{}", class.name);
                    }
                }
            });

            let mut times = [(0.0, 0.0, 0.0); 4];
            let mut lookup_allocations = 0.0;
            for (class, time_ns) in classes.iter().zip(&mut times) {
                let (p50, p99) = percentiles(class.keys.len(), |index| {
                    black_box(
                        engine
                            .get(&class.keys[index], &mut workspace)
                            .unwrap()
                            .is_some(),
                    );
                });
                let (ns, allocations) = time(BATCHES, BATCH, |index| {
                    black_box(
                        engine
                            .get(&class.keys[index], &mut workspace)
                            .unwrap()
                            .is_some(),
                    );
                });
                *time_ns = (ns, p50, p99);
                lookup_allocations += allocations / classes.len() as f64;
            }
            let (snapshot_ns, snapshot_allocations) = time(BATCHES, BATCH, |_| {
                black_box(engine.snapshot().unwrap());
            });

            println!(
                "| {} | {runs} | {} | {} | {} | {} | {lookup_allocations} | {snapshot_ns:.0} | {snapshot_allocations} | {} | {} | {} |",
                shape.name(),
                triple(times[0]),
                triple(times[1]),
                triple(times[2]),
                triple(times[3]),
                memory[0],
                memory[1],
                memory[2],
            );
            engine.close().unwrap();
        }
    }
}

fn triple((mean, p50, p99): (f64, f64, f64)) -> String {
    format!("{mean:.0} / {p50:.0} / {p99:.0}")
}

fn scans_mode() {
    println!(
        "| layout | runs | short scan µs (keys) | long scan µs (keys) | empty probe µs | allocations per scan | decoded KiB | heap KiB | native KiB |"
    );
    println!("|---|---|---|---|---|---|---|---|---|");
    for shape in [Shape::Disjoint, Shape::Overlapping] {
        for runs in run_counts() {
            let engine = build(shape, runs);
            let snapshot = engine.snapshot().unwrap();
            let mut rng = Rng::new(9);
            // A 9-digit prefix covers 10 numbers, 5 of them keys; a 7-digit
            // prefix covers 1,000, 500 of them keys. Deleted keys are skipped.
            let short: Vec<Vec<u8>> = (0..BATCH)
                .map(|_| key(rng.below(KEYS))[..14].to_vec())
                .collect();
            let long: Vec<Vec<u8>> = (0..BATCH / 10)
                .map(|_| key(rng.below(KEYS))[..12].to_vec())
                .collect();
            let empty: &[u8] = b"user/9";

            let scan = |prefix: &[u8], workspace: &mut EngineWorkspace| {
                let mut records = 0;
                let mut scan = snapshot.scan_prefix(prefix, workspace);
                while let Some(record) = scan.next_record().unwrap() {
                    black_box(record);
                    records += 1;
                }
                records
            };
            let mut counts = (0, 0);
            let (mut workspace, memory) = measured(|workspace| {
                counts.0 = short.iter().map(|p| scan(p, workspace)).sum::<usize>() / short.len();
                counts.1 = long.iter().map(|p| scan(p, workspace)).sum::<usize>() / long.len();
                assert_eq!(scan(empty, workspace), 0);
            });

            let (short_ns, short_allocations) = time(5, short.len(), |index| {
                scan(&short[index % short.len()], &mut workspace);
            });
            let (long_ns, _) = time(5, long.len(), |index| {
                scan(&long[index % long.len()], &mut workspace);
            });
            let (empty_ns, _) = time(5, BATCH, |_| {
                scan(empty, &mut workspace);
            });
            println!(
                "| {} | {runs} | {:.2} ({}) | {:.1} ({}) | {:.2} | {short_allocations} | {} | {} | {} |",
                shape.name(),
                short_ns / 1e3,
                counts.0,
                long_ns / 1e3,
                counts.1,
                empty_ns / 1e3,
                memory[0],
                memory[1],
                memory[2],
            );
            drop(snapshot);
            engine.close().unwrap();
        }
    }
}

fn main() {
    match std::env::args().nth(1).as_deref() {
        Some("lookups") => lookups_mode(),
        Some("scans") => scans_mode(),
        _ => eprintln!("read_path lookups|scans [RUNS...]"),
    }
}
