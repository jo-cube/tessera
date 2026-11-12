//! Tessera's benchmark suite: representative workloads through the public
//! engine API, one mode per invocation, each printing Markdown tables.
//!
//! ```text
//! bench MODE [DIR]
//! ```
//!
//! | mode          | measures                                                   |
//! |---------------|------------------------------------------------------------|
//! | `memtable`    | writes and reads that stay in the memtable                 |
//! | `lookups`     | point lookups by storage shape, key class and key layout   |
//! | `scans`       | prefix scans: seek and iteration by shape and result size  |
//! | `readers`     | concurrent readers and scanners, and beside a writer       |
//! | `density`     | memory per record and read cost for three payloads         |
//! | `maintenance` | flush and compaction throughput and filter construction    |
//! | `snapshots`   | snapshot cost, and writes and retained memory beside them  |
//! | `ingest`      | sustained writes with automatic flushes and compaction     |
//! | `mixed`       | concurrent readers beside sustained writers                |
//! | `workloads-*` | read-heavy, balanced and write-heavy mixes, per codec      |
//! | `durable`     | durable writes and group commit                            |
//! | `recovery`    | checkpoints, recovery and log replay                       |
//!
//! `durable` and `recovery` write under DIR, default `/tmp`. Run the suite
//! through `scripts/validate.sh bench`, which applies the resource limits and
//! keeps fixtures on tmpfs. Data comes from fixed seeds, so every run sees
//! the same keys and values; timings vary with the machine, so compare
//! configurations only within one environment and one pass of the suite.
//!
//! Two variables change the runs every mode builds: `BENCH_BLOCK_LEN`, the
//! target block length in bytes, and `BENCH_COMPRESSION`, one of `zstd`,
//! `lz4` and `none` (the `workloads-*` modes choose their own codec).

use std::alloc::{GlobalAlloc, Layout, System};
use std::collections::BTreeMap;
use std::hint::black_box;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering};
use std::thread;
use std::time::Instant;

use tessera::engine::{
    CheckpointOptions, CompactionOptions, Compression, DurableOptions, Engine, EngineOptions,
    EngineStats, EngineWorkspace, MergeWorkspace, Scan, Snapshot, SnapshotLimits,
};

/// Records in a key group, which a prefix scan of the group's first four
/// bytes returns.
const GROUP: u64 = 16;
const VALUE_LEN: usize = 100;

// Data.

/// Deterministic xorshift generator.
struct Rng(u64);

impl Rng {
    fn new(seed: u64) -> Self {
        Self(seed.wrapping_mul(0x9e37_79b9_7f4a_7c15) | 1)
    }

    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }

    fn below(&mut self, bound: u64) -> u64 {
        self.next() % bound
    }
}

/// The key of record `index`: its group, then its place in the group. Places
/// from `GROUP` up are never written, so their keys are absent but lie
/// inside every run's key range.
fn key(index: u64) -> [u8; 8] {
    let mut key = [0; 8];
    key[..4].copy_from_slice(&((index / GROUP) as u32).to_be_bytes());
    key[4..].copy_from_slice(&((index % GROUP) as u32).to_be_bytes());
    key
}

fn absent_key(index: u64) -> [u8; 8] {
    let mut key = key(index);
    key[7] |= 0x80;
    key
}

/// Values half random and half repeated, so that they compress about as
/// well as typical structured values.
struct Values(Vec<[u8; VALUE_LEN]>);

impl Values {
    fn new() -> Self {
        let mut rng = Rng::new(1);
        Self(
            (0..64)
                .map(|_| {
                    std::array::from_fn(|at| if at % 2 == 0 { rng.next() as u8 } else { b'v' })
                })
                .collect(),
        )
    }

    fn get(&self, index: u64) -> &[u8] {
        &self.0[index as usize % self.0.len()]
    }
}

/// `count` distinct indices in a seeded random order.
fn shuffled(count: u64, seed: u64) -> Vec<u64> {
    let mut order: Vec<u64> = (0..count).collect();
    let mut rng = Rng::new(seed);
    for at in (1..order.len()).rev() {
        order.swap(at, rng.below(at as u64 + 1) as usize);
    }
    order
}

// Engines.

/// Options with no run limits, no background compaction and room for many
/// snapshots, so that a mode meets only the limits it sets.
fn options(memtable_budget: usize) -> EngineOptions {
    EngineOptions {
        memtable_budget,
        target_block_len: block_len(),
        compression: compression(),
        max_runs: usize::MAX,
        max_run_bytes: usize::MAX,
        snapshots: SnapshotLimits {
            max_live: 1 << 10,
            max_retained_bytes: usize::MAX,
        },
        ..EngineOptions::default()
    }
}

/// `BENCH_BLOCK_LEN` bytes, or the engine's default.
fn block_len() -> usize {
    std::env::var("BENCH_BLOCK_LEN").map_or(EngineOptions::default().target_block_len, |len| {
        len.parse().expect("BENCH_BLOCK_LEN is a byte count")
    })
}

/// `BENCH_COMPRESSION`, or the engine's default.
fn compression() -> Compression {
    match std::env::var("BENCH_COMPRESSION").as_deref() {
        Err(_) => Compression::default(),
        Ok("zstd") => Compression::Zstd,
        Ok("lz4") => Compression::Lz4,
        Ok("none") => Compression::Uncompressed,
        Ok(other) => panic!("BENCH_COMPRESSION {other:?} is not zstd, lz4 or none"),
    }
}

fn fill(engine: &Engine, order: &[u64], values: &Values) {
    for &index in order {
        engine.put(&key(index), values.get(index)).unwrap();
    }
}

/// Compacts until no key lies in more than one run.
fn consolidate(engine: &Engine) {
    let options = CompactionOptions {
        max_overlap: 1,
        max_inputs: usize::MAX,
        max_input_bytes: usize::MAX,
        max_output_bytes: usize::MAX,
        ..CompactionOptions::default()
    };
    let mut workspace = MergeWorkspace::new();
    while engine.compact(options, &mut workspace).unwrap().is_some() {}
}

/// A fresh directory under `base`, removed again when dropped.
struct Scratch(PathBuf);

impl Scratch {
    fn new(base: &Path, name: &str) -> Self {
        let path = base.join(format!("tessera-bench-{name}"));
        let _ = std::fs::remove_dir_all(&path);
        Self(path)
    }
}

impl Drop for Scratch {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

// Measurement.

/// Nanoseconds per operation of `round`, which runs `operations`, as the
/// median of `rounds` runs.
fn ns_per_op(rounds: usize, operations: usize, mut round: impl FnMut()) -> f64 {
    ns_per_op_after(rounds, operations, || (), |()| round())
}

/// As [`ns_per_op`], timing only `round`, which takes what an untimed
/// `setup` made.
fn ns_per_op_after<T>(
    rounds: usize,
    operations: usize,
    mut setup: impl FnMut() -> T,
    mut round: impl FnMut(T),
) -> f64 {
    let mut times: Vec<f64> = (0..rounds)
        .map(|_| {
            let input = setup();
            let start = Instant::now();
            round(input);
            start.elapsed().as_nanos() as f64 / operations as f64
        })
        .collect();
    times.sort_by(f64::total_cmp);
    times[times.len() / 2]
}

/// The median of `rounds` timings of `round`, in seconds, with the result of
/// the median round.
fn median_secs<T>(rounds: usize, mut round: impl FnMut() -> (f64, T)) -> (f64, T) {
    let mut runs: Vec<(f64, T)> = (0..rounds).map(|_| round()).collect();
    runs.sort_by(|a, b| a.0.total_cmp(&b.0));
    runs.swap_remove(rounds / 2)
}

/// Latency samples in nanoseconds.
#[derive(Default)]
struct Latencies(Vec<u64>);

impl Latencies {
    fn time<T>(&mut self, operation: impl FnOnce() -> T) -> T {
        let start = Instant::now();
        let out = operation();
        self.0.push(start.elapsed().as_nanos() as u64);
        out
    }

    fn merge(all: impl IntoIterator<Item = Latencies>) -> Self {
        let mut merged: Vec<u64> = all.into_iter().flat_map(|l| l.0).collect();
        merged.sort_unstable();
        Self(merged)
    }

    /// Percentile `p` in microseconds; the samples must be sorted.
    fn us(&self, p: f64) -> f64 {
        if self.0.is_empty() {
            return 0.0;
        }
        self.0[((self.0.len() - 1) as f64 * p) as usize] as f64 / 1e3
    }

    fn max_us(&self) -> f64 {
        self.0.iter().max().copied().unwrap_or(0) as f64 / 1e3
    }
}

fn mib(bytes: impl Into<u64>) -> f64 {
    bytes.into() as f64 / f64::from(1 << 20)
}

/// The process's peak resident memory in MiB, where Linux reports it.
fn peak_rss_mib() -> f64 {
    std::fs::read_to_string("/proc/self/status")
        .ok()
        .and_then(|status| {
            let line = status.lines().find(|line| line.starts_with("VmHWM:"))?;
            line.split_whitespace().nth(1)?.parse::<f64>().ok()
        })
        .map_or(0.0, |kib| kib / 1024.0)
}

fn table(title: &str, header: &[&str]) {
    println!("\n### {title}\n");
    println!("| {} |", header.join(" | "));
    println!("|{}", "---|".repeat(header.len()));
}

// Modes.

/// Writes and reads of a table that never fills, so nothing is flushed.
fn memtable() {
    const RECORDS: u64 = 500_000;
    const ROUNDS: usize = 5;
    let values = Values::new();
    let random = shuffled(RECORDS, 2);
    let sequential: Vec<u64> = (0..RECORDS).collect();
    let budget = 1 << 30;
    let loaded = || {
        let engine = Engine::new(options(budget));
        fill(&engine, &random, &values);
        engine
    };

    table(
        &format!("Memtable: {RECORDS} records of 8 + {VALUE_LEN} bytes"),
        &["operation", "ns/op", "M ops/s"],
    );
    let row = |name: &str, ns: f64| println!("| {name} | {ns:.0} | {:.2} |", 1e3 / ns);
    let n = RECORDS as usize;

    row(
        "insert, ascending keys",
        ns_per_op(ROUNDS, n, || {
            fill(&Engine::new(options(budget)), &sequential, &values)
        }),
    );
    row(
        "insert, random keys",
        ns_per_op(ROUNDS, n, || {
            fill(&Engine::new(options(budget)), &random, &values)
        }),
    );
    let engine = loaded();
    let overwrite = shuffled(RECORDS, 3);
    row(
        "overwrite, random keys",
        ns_per_op(ROUNDS, n, || fill(&engine, &overwrite, &values)),
    );
    let mut workspace = EngineWorkspace::new();
    row(
        "get, present",
        ns_per_op(ROUNDS, n, || {
            for &index in &overwrite {
                black_box(engine.get(&key(index), &mut workspace).unwrap());
            }
        }),
    );
    row(
        "get, absent",
        ns_per_op(ROUNDS, n, || {
            for &index in &overwrite {
                black_box(engine.get(&absent_key(index), &mut workspace).unwrap());
            }
        }),
    );
    row(
        "scan, per record",
        ns_per_op(ROUNDS, n, || {
            let guard = engine.read();
            let mut scan = guard.scan(&mut workspace);
            while let Some(record) = scan.next_record().unwrap() {
                black_box(record.value.len());
            }
        }),
    );
    row(
        "delete, random keys",
        ns_per_op_after(ROUNDS, n, loaded, |engine| {
            for &index in &overwrite {
                engine.delete(&key(index)).unwrap();
            }
        }),
    );
    let stats = engine.stats();
    println!(
        "\ncharged {:.1} MiB for {:.1} MiB of keys and values",
        mib(stats.active.charged as u64),
        mib(stats.active.payload as u64)
    );
}

// Read baseline: shared by `lookups`, `scans`, `readers` and `density`.

/// Counts allocations and live heap bytes while [`COUNTING`] is set. Unset,
/// it adds one relaxed load to each allocation.
struct Counting;

static COUNTING: AtomicBool = AtomicBool::new(false);
static ALLOCATIONS: AtomicU64 = AtomicU64::new(0);
static LIVE: AtomicI64 = AtomicI64::new(0);

#[global_allocator]
static ALLOCATOR: Counting = Counting;

// SAFETY: every call is forwarded unchanged to the system allocator; only
// counters are updated beside it.
unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        if COUNTING.load(Ordering::Relaxed) {
            ALLOCATIONS.fetch_add(1, Ordering::Relaxed);
            LIVE.fetch_add(layout.size() as i64, Ordering::Relaxed);
        }
        // SAFETY: the caller's contract is the system allocator's.
        unsafe { System.alloc(layout) }
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        if COUNTING.load(Ordering::Relaxed) {
            ALLOCATIONS.fetch_add(1, Ordering::Relaxed);
            LIVE.fetch_add(layout.size() as i64, Ordering::Relaxed);
        }
        // SAFETY: as for `alloc`.
        unsafe { System.alloc_zeroed(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        if COUNTING.load(Ordering::Relaxed) {
            LIVE.fetch_sub(layout.size() as i64, Ordering::Relaxed);
        }
        // SAFETY: as for `alloc`.
        unsafe { System.dealloc(ptr, layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        if COUNTING.load(Ordering::Relaxed) {
            ALLOCATIONS.fetch_add(1, Ordering::Relaxed);
            LIVE.fetch_add(new_size as i64 - layout.size() as i64, Ordering::Relaxed);
        }
        // SAFETY: as for `alloc`.
        unsafe { System.realloc(ptr, layout, new_size) }
    }
}

/// Runs `f` with allocations counted, returning how many it made.
fn counted<T>(f: impl FnOnce() -> T) -> (T, u64) {
    let before = ALLOCATIONS.load(Ordering::Relaxed);
    COUNTING.store(true, Ordering::Relaxed);
    let out = f();
    COUNTING.store(false, Ordering::Relaxed);
    (out, ALLOCATIONS.load(Ordering::Relaxed) - before)
}

/// Live heap bytes allocated while counting, less those freed.
fn live_heap() -> i64 {
    LIVE.load(Ordering::Relaxed)
}

/// The point layout as owned bytes.
fn point_key(index: u64) -> Vec<u8> {
    key(index).to_vec()
}

/// 31-byte keys sharing long prefixes, as hierarchical names do.
fn shared_key(index: u64) -> Vec<u8> {
    format!("tenant/{:04}/user/{index:08}/profile", index / 8192).into_bytes()
}

/// Keys per top-level group of the scan layout.
const SCAN_GROUP: u64 = 16_384;
const SCAN_GROUPS: u64 = 30;

/// The scan layout, 8 bytes: a 2-byte group (`[g, 0x00]` or `[g, 0xFF]`
/// alternately, the last `[0xFF, 0xFF]`), then 32 subgroups of 512 keys
/// (2 bytes, the first shared by 4 subgroups), 16 of 32 (`[0x00, n]`) and a
/// place (`[n, 0xFF]`). Prefixes of 6, 4, 3 and 2 bytes select 32, 512,
/// 2,048 and 16,384 keys, and include 0x00 and 0xFF bytes.
fn scan_key(index: u64) -> Vec<u8> {
    let (group, rest) = (index / SCAN_GROUP, index % SCAN_GROUP);
    let (sub, rest) = (rest / 512, rest % 512);
    let (leaf, place) = (rest / 32, rest % 32);
    let group = if group == SCAN_GROUPS - 1 {
        [0xFF, 0xFF]
    } else {
        [group as u8, if group % 2 == 1 { 0xFF } else { 0x00 }]
    };
    vec![
        group[0],
        group[1],
        (sub / 4) as u8,
        (sub % 4 * 0x40) as u8,
        0x00,
        leaf as u8,
        place as u8,
        0xFF,
    ]
}

/// Which keys the fixture updates after loading: every 7th from 3 is
/// deleted, and otherwise every 5th is overwritten.
fn value_seed(index: u64) -> Option<u64> {
    if index % 7 == 3 {
        None
    } else if index.is_multiple_of(5) {
        Some(index + 1)
    } else {
        Some(index)
    }
}

/// How the fixture's records are spread over memtables and runs.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Shape {
    /// Everything in the active memtable.
    Memtable,
    /// One flush and full compaction: one version of each key in disjoint runs.
    Consolidated,
    /// Loaded in 8 flushes over the whole key range, then updates flushed:
    /// 9 runs overlapping everywhere, with superseded versions.
    Overlapping,
    /// One run, then a run of the updates over it.
    NewerRun,
    /// One run, with the updates in the memtable.
    MemtableOverRuns,
    /// A snapshot of `MemtableOverRuns`, read while later writes overwrite a
    /// third of the keys in the engine.
    Snapshot,
}

impl Shape {
    const ALL: [Shape; 6] = [
        Shape::Memtable,
        Shape::Consolidated,
        Shape::Overlapping,
        Shape::NewerRun,
        Shape::MemtableOverRuns,
        Shape::Snapshot,
    ];

    fn name(self) -> &'static str {
        match self {
            Shape::Memtable => "memtable",
            Shape::Consolidated => "consolidated",
            Shape::Overlapping => "9 overlapping runs",
            Shape::NewerRun => "run + newer run",
            Shape::MemtableOverRuns => "run + memtable",
            Shape::Snapshot => "snapshot of run + memtable",
        }
    }

    /// Versions a scan meets per key in a range, from how the fixture was
    /// built, where that is known: 1 per key, plus 1 per updated key where
    /// the updates sit apart from the load. A consolidated run may or may
    /// not keep its tombstones, so it is not given.
    fn versions(self, keys: u64, updated: u64) -> Option<u64> {
        match self {
            Shape::Memtable => Some(keys),
            Shape::Consolidated => None,
            _ => Some(keys + updated),
        }
    }
}

/// An engine in one [`Shape`], read through the engine or its snapshot.
struct Fixture {
    engine: Engine,
    snapshot: Option<Snapshot>,
}

impl Fixture {
    fn new(shape: Shape, count: u64, key: fn(u64) -> Vec<u8>, values: &Values) -> Self {
        let engine = Engine::new(options(1 << 30));
        let load = |order: &[u64]| {
            for &index in order {
                engine.put(&key(index), values.get(index)).unwrap();
            }
        };
        let update = || {
            for index in shuffled(count, 51) {
                match value_seed(index) {
                    None => {
                        engine.delete(&key(index)).unwrap();
                    }
                    Some(seed) if seed != index => {
                        engine.put(&key(index), values.get(seed)).unwrap();
                    }
                    Some(_) => {}
                }
            }
        };
        let order = shuffled(count, 50);
        match shape {
            Shape::Memtable => {
                load(&order);
                update();
            }
            Shape::Consolidated => {
                load(&order);
                update();
                engine.flush().unwrap();
                consolidate(&engine);
            }
            Shape::Overlapping => {
                for chunk in order.chunks(order.len().div_ceil(8)) {
                    load(chunk);
                    engine.flush().unwrap();
                }
                update();
                engine.flush().unwrap();
            }
            Shape::NewerRun => {
                load(&order);
                engine.flush().unwrap();
                update();
                engine.flush().unwrap();
            }
            Shape::MemtableOverRuns | Shape::Snapshot => {
                load(&order);
                engine.flush().unwrap();
                update();
            }
        }
        let snapshot = (shape == Shape::Snapshot).then(|| {
            let snapshot = engine.snapshot().unwrap();
            for index in (1..count).step_by(3) {
                engine.put(&key(index), values.get(index + 2)).unwrap();
            }
            snapshot
        });
        Self { engine, snapshot }
    }

    fn get<'r>(&'r self, key: &[u8], workspace: &'r mut EngineWorkspace) -> Option<&'r [u8]> {
        match &self.snapshot {
            Some(snapshot) => snapshot
                .get(key, workspace)
                .unwrap()
                .map(|found| found.value),
            None => self
                .engine
                .get(key, workspace)
                .unwrap()
                .map(|found| found.value),
        }
    }

    /// Runs `f` on a scan of `prefix`, through the snapshot or a read guard
    /// taken for it.
    fn scan<T>(
        &self,
        prefix: &[u8],
        workspace: &mut EngineWorkspace,
        f: impl FnOnce(Scan<'_>) -> T,
    ) -> T {
        match &self.snapshot {
            Some(snapshot) => f(snapshot.scan_prefix(prefix, workspace)),
            None => {
                let guard = self.engine.read();
                f(guard.scan_prefix(prefix, workspace))
            }
        }
    }

    fn sources(&self) -> String {
        let stats = self.engine.stats();
        format!("{} / {}", stats.runs, stats.max_overlap)
    }
}

/// Probes: a key, and the seed of the value it must read, if any.
type Probes = Vec<(Vec<u8>, Option<u64>)>;

/// The lookup classes over `count` records of a layout.
fn point_probes(
    count: u64,
    key: fn(u64) -> Vec<u8>,
    absent: fn(u64) -> Vec<u8>,
    probes: usize,
) -> Vec<(&'static str, Probes)> {
    let order = shuffled(count, 52);
    let pick = |keep: &dyn Fn(u64) -> bool| -> Vec<u64> {
        order
            .iter()
            .copied()
            .filter(|&index| keep(index))
            .take(probes)
            .collect()
    };
    let with_model = |indices: &[u64]| -> Probes {
        indices
            .iter()
            .map(|&index| (key(index), value_seed(index)))
            .collect()
    };
    let present = pick(&|index| value_seed(index).is_some());
    let mut ascending = present.clone();
    ascending.sort_unstable();
    let mut rng = Rng::new(53);
    let hot: Vec<u64> = std::iter::repeat_with(|| count / 2 + rng.below(4_096))
        .filter(|&index| value_seed(index).is_some())
        .take(probes)
        .collect();
    vec![
        ("present, random order", with_model(&present)),
        ("present, ascending order", with_model(&ascending)),
        ("present, hot 4,096-key range", with_model(&hot)),
        (
            "overwritten",
            with_model(&pick(&|index| {
                value_seed(index).is_some_and(|seed| seed != index)
            })),
        ),
        (
            "deleted",
            with_model(&pick(&|index| value_seed(index).is_none())),
        ),
        (
            "absent, inside run ranges",
            order[..probes].iter().map(|&i| (absent(i), None)).collect(),
        ),
        (
            "absent, beyond the last key",
            (0..probes as u64)
                .map(|i| ([&[0xFF; 4][..], &(i as u32).to_be_bytes()].concat(), None))
                .collect(),
        ),
    ]
}

fn point_absent(index: u64) -> Vec<u8> {
    absent_key(index).to_vec()
}

fn shared_absent(index: u64) -> Vec<u8> {
    let mut key = shared_key(index);
    *key.last_mut().unwrap() = b'f';
    key
}

/// Cold and warm cost of `probes` through `fixture`, checked against the
/// model on the way.
fn measure_gets(fixture: &Fixture, probes: &Probes, values: &Values) -> String {
    let mut workspace = EngineWorkspace::new();
    let n = probes.len();
    let pass = |workspace: &mut EngineWorkspace| {
        for (key, _) in probes {
            black_box(fixture.get(key, workspace));
        }
    };
    let start = Instant::now();
    pass(&mut workspace);
    let cold = start.elapsed().as_nanos() as f64 / n as f64;

    let before = workspace.blocks_loaded();
    for (key, seed) in probes {
        let expected = seed.map(|seed| values.get(seed));
        assert_eq!(fixture.get(key, &mut workspace), expected, "{key:?}");
    }
    let blocks = (workspace.blocks_loaded() - before) as f64 / n as f64;

    let warm = ns_per_op(3, n, || pass(&mut workspace));
    let mut latency = Latencies::default();
    for (key, _) in probes {
        latency.time(|| {
            black_box(fixture.get(key, &mut workspace));
        });
    }
    latency.0.sort_unstable();
    let ((), allocations) = counted(|| pass(&mut workspace));
    format!(
        "{cold:.0} | {warm:.0} | {:.2} | {:.2} | {blocks:.2} | {:.2}",
        latency.us(0.5),
        latency.us(0.99),
        allocations as f64 / n as f64
    )
}

/// Point lookups by storage shape, lookup class and key layout.
fn lookups() {
    const RECORDS: u64 = 500_000;
    const PROBES: usize = 50_000;
    let values = Values::new();
    table(
        &format!(
            "Point lookups: {RECORDS} records of 8 + {VALUE_LEN} bytes, {PROBES} probes per class"
        ),
        &[
            "shape",
            "runs / max overlap",
            "lookup",
            "cold ns",
            "warm ns",
            "p50 µs",
            "p99 µs",
            "blocks/get",
            "allocs/get",
        ],
    );
    for shape in Shape::ALL {
        let fixture = Fixture::new(shape, RECORDS, point_key, &values);
        for (class, probes) in point_probes(RECORDS, point_key, point_absent, PROBES) {
            println!(
                "| {} | {} | {class} | {} |",
                shape.name(),
                fixture.sources(),
                measure_gets(&fixture, &probes, &values)
            );
        }
    }

    // A deep history: a consolidated load, then the same eighth of the keys
    // overwritten in each of 8 flushes, so those keys have a version in 9
    // runs. A snapshot taken after the fourth flush sees the fourth version.
    {
        let engine = Engine::new(options(1 << 30));
        for index in shuffled(RECORDS, 50) {
            engine.put(&point_key(index), values.get(index)).unwrap();
        }
        engine.flush().unwrap();
        consolidate(&engine);
        let mut snapshot = None;
        for round in 1..=8 {
            for index in (0..RECORDS).step_by(8) {
                engine
                    .put(&point_key(index), values.get(index + round))
                    .unwrap();
            }
            engine.flush().unwrap();
            if round == 4 {
                snapshot = Some(engine.snapshot().unwrap());
            }
        }
        let order = shuffled(RECORDS, 52);
        let probes = |deep: bool, seed: u64| -> Probes {
            order
                .iter()
                .filter(|&&index| (index % 8 == 0) == deep)
                .take(PROBES)
                .map(|&index| (point_key(index), Some(index + if deep { seed } else { 0 })))
                .collect()
        };
        let mut fixture = Fixture {
            engine,
            snapshot: None,
        };
        let sources = fixture.sources();
        for (class, probes) in [
            ("overwritten in 8 runs", probes(true, 8)),
            ("present, never updated", probes(false, 0)),
        ] {
            let row = measure_gets(&fixture, &probes, &values);
            println!("| 9-run history | {sources} | {class} | {row} |");
        }
        fixture.snapshot = snapshot;
        let row = measure_gets(&fixture, &probes(true, 4), &values);
        println!(
            "| 9-run history, snapshot after 4 | {sources} | overwritten in 4 of its runs | {row} |"
        );
    }

    // The same lookups on 31-byte keys with long shared prefixes.
    for shape in [Shape::Consolidated, Shape::Overlapping] {
        let fixture = Fixture::new(shape, RECORDS, shared_key, &values);
        for (class, probes) in point_probes(RECORDS, shared_key, shared_absent, PROBES) {
            if class == "present, random order" || class == "absent, inside run ranges" {
                println!(
                    "| {}, 31-byte shared-prefix keys | {} | {class} | {} |",
                    shape.name(),
                    fixture.sources(),
                    measure_gets(&fixture, &probes, &values)
                );
            }
        }
    }

    // Reference: the live records in a BTreeMap, uncompressed, with no
    // versions or snapshots.
    let map: BTreeMap<Vec<u8>, Vec<u8>> = (0..RECORDS)
        .filter_map(|index| Some((point_key(index), values.get(value_seed(index)?).to_vec())))
        .collect();
    for (class, probes) in point_probes(RECORDS, point_key, point_absent, PROBES) {
        let n = probes.len();
        let pass = || {
            for (key, _) in &probes {
                black_box(map.get(key));
            }
        };
        let start = Instant::now();
        pass();
        let cold = start.elapsed().as_nanos() as f64 / n as f64;
        let warm = ns_per_op(3, n, pass);
        let mut latency = Latencies::default();
        for (key, _) in &probes {
            latency.time(|| black_box(map.get(key)).is_some());
        }
        latency.0.sort_unstable();
        println!(
            "| BTreeMap (reference) | – | {class} | {cold:.0} | {warm:.0} | {:.2} | {:.2} | – | 0 |",
            latency.us(0.5),
            latency.us(0.99)
        );
    }
}

/// Prefix scans by storage shape and prefix class, timing the seek to the
/// first record apart from the iteration after it.
fn scans() {
    const RECORDS: u64 = SCAN_GROUP * SCAN_GROUPS;
    let values = Values::new();
    let mut rng = Rng::new(60);
    let mut prefixes = |len: usize, scans: usize, group: &dyn Fn(&mut Rng) -> u64| {
        (0..scans)
            .map(|_| {
                let index = group(&mut rng) * SCAN_GROUP + rng.below(SCAN_GROUP);
                scan_key(index)[..len].to_vec()
            })
            .collect::<Vec<_>>()
    };
    let any = |rng: &mut Rng| rng.below(SCAN_GROUPS);
    let odd = |rng: &mut Rng| rng.below(SCAN_GROUPS / 2 - 1) * 2 + 1;
    let even = |rng: &mut Rng| rng.below(SCAN_GROUPS / 2) * 2;
    let inner = |rng: &mut Rng| rng.below(SCAN_GROUPS - 1);
    let classes: Vec<(&str, Vec<Vec<u8>>)> = vec![
        ("6-byte prefix, 32 keys", prefixes(6, 2_000, &any)),
        ("4-byte prefix, 512 keys", prefixes(4, 200, &any)),
        ("3-byte prefix, 2,048 keys", prefixes(3, 50, &any)),
        (
            "2-byte prefix ending 0xFF, 16,384 keys",
            prefixes(2, 8, &odd),
        ),
        (
            "2-byte prefix ending 0x00, 16,384 keys",
            prefixes(2, 8, &even),
        ),
        (
            "prefix FF FF, 16,384 keys, no upper bound",
            vec![vec![0xFF, 0xFF]; 4],
        ),
        ("empty prefix, every key", vec![Vec::new()]),
        (
            "no match, inside the key range",
            prefixes(3, 2_000, &inner)
                .into_iter()
                .map(|mut prefix| {
                    prefix[2] = 0x7F;
                    prefix
                })
                .collect(),
        ),
        (
            "no match, beyond the last key",
            (0..2_000u32)
                .map(|i| [&[0xFF, 0xFF, 0xFF][..], &i.to_be_bytes()].concat())
                .collect(),
        ),
    ];

    // The model: live records, and per key whether it was updated.
    let model: BTreeMap<Vec<u8>, (Option<u64>, bool)> = (0..RECORDS)
        .map(|index| {
            let seed = value_seed(index);
            (scan_key(index), (seed, seed != Some(index)))
        })
        .collect();
    fn in_range<'m>(
        model: &'m BTreeMap<Vec<u8>, (Option<u64>, bool)>,
        prefix: &'m [u8],
    ) -> impl Iterator<Item = (&'m Vec<u8>, &'m (Option<u64>, bool))> {
        model
            .range(prefix.to_vec()..)
            .take_while(move |(key, _)| key.starts_with(prefix))
    }

    table(
        &format!(
            "Prefix scans: {RECORDS} records of 8 + {VALUE_LEN} bytes (1 in 7 deleted, 1 in 5 of the rest overwritten)"
        ),
        &[
            "shape",
            "runs / max overlap",
            "prefix",
            "scans",
            "records/scan",
            "seek µs",
            "scan µs",
            "ns/record after seek",
            "M records/s after seek",
            "blocks/scan",
            "versions/record (model)",
            "workspace KiB",
            "allocs/scan",
        ],
    );
    for shape in Shape::ALL {
        let fixture = Fixture::new(shape, RECORDS, scan_key, &values);
        for (class, prefixes) in &classes {
            let mut workspace = EngineWorkspace::new();
            // Checked against the model, which also gives the versions.
            let (mut records, mut keys, mut updated) = (0u64, 0u64, 0u64);
            let before = workspace.blocks_loaded();
            for prefix in prefixes {
                let mut expected = in_range(&model, prefix);
                fixture.scan(prefix, &mut workspace, |mut scan| {
                    while let Some(record) = scan.next_record().unwrap() {
                        let (key, seed) = loop {
                            let (key, (seed, was_updated)) = expected.next().expect("extra record");
                            keys += 1;
                            updated += u64::from(*was_updated);
                            if let Some(seed) = seed {
                                break (key, *seed);
                            }
                        };
                        assert_eq!(record.key, key.as_slice());
                        assert_eq!(record.value, values.get(seed));
                        records += 1;
                    }
                });
                for (_, (seed, was_updated)) in expected {
                    assert!(seed.is_none(), "missing record in {prefix:?}");
                    keys += 1;
                    updated += u64::from(*was_updated);
                }
            }
            let blocks = (workspace.blocks_loaded() - before) as f64 / prefixes.len() as f64;

            let round = |workspace: &mut EngineWorkspace| {
                let (mut seek, mut total, mut returned) = (0u128, 0u128, 0u64);
                for prefix in prefixes {
                    let start = Instant::now();
                    fixture.scan(prefix, workspace, |mut scan| {
                        let first = scan
                            .next_record()
                            .unwrap()
                            .map(|r| black_box(r.value.len()));
                        seek += start.elapsed().as_nanos();
                        returned += u64::from(first.is_some());
                        while let Some(record) = scan.next_record().unwrap() {
                            black_box(record.value.len());
                            returned += 1;
                        }
                    });
                    total += start.elapsed().as_nanos();
                }
                (seek, total, returned)
            };
            round(&mut workspace);
            let mut rounds: Vec<_> = (0..3).map(|_| round(&mut workspace)).collect();
            rounds.sort_by_key(|&(_, total, _)| total);
            let (seek, total, returned) = rounds[1];
            assert_eq!(returned, records);
            let ((_, _, _), allocations) = counted(|| round(&mut workspace));
            let scans = prefixes.len() as f64;
            let after = returned.saturating_sub(prefixes.len() as u64).max(1) as f64;
            let per_record = (total - seek) as f64 / after;
            println!(
                "| {} | {} | {class} | {} | {:.1} | {:.2} | {:.1} | {} | {} | {blocks:.1} | {} | {:.0} | {:.1} |",
                shape.name(),
                fixture.sources(),
                prefixes.len(),
                records as f64 / scans,
                seek as f64 / scans / 1e3,
                total as f64 / scans / 1e3,
                if returned > prefixes.len() as u64 {
                    format!("{per_record:.0}")
                } else {
                    "–".into()
                },
                if returned > prefixes.len() as u64 {
                    format!("{:.1}", 1e3 / per_record)
                } else {
                    "–".into()
                },
                match (shape.versions(keys, updated), records) {
                    (Some(versions), 1..) => format!("{:.2}", versions as f64 / records as f64),
                    _ => "–".into(),
                },
                workspace.capacity() as f64 / 1024.0,
                allocations as f64 / scans,
            );
        }
    }

    // Reference: the live records in a BTreeMap.
    let map: BTreeMap<Vec<u8>, Vec<u8>> = (0..RECORDS)
        .filter_map(|index| Some((scan_key(index), values.get(value_seed(index)?).to_vec())))
        .collect();
    for (class, prefixes) in &classes {
        let round = || {
            let (mut seek, mut total, mut returned) = (0u128, 0u128, 0u64);
            for prefix in prefixes {
                let start = Instant::now();
                let mut range = map
                    .range(prefix.clone()..)
                    .take_while(|(key, _)| key.starts_with(prefix));
                let first = range.next().map(|(_, value)| black_box(value.len()));
                seek += start.elapsed().as_nanos();
                returned += u64::from(first.is_some());
                for (_, value) in range {
                    black_box(value.len());
                    returned += 1;
                }
                total += start.elapsed().as_nanos();
            }
            (seek, total, returned)
        };
        round();
        let mut rounds: Vec<_> = (0..3).map(|_| round()).collect();
        rounds.sort_by_key(|&(_, total, _)| total);
        let (seek, total, returned) = rounds[1];
        let scans = prefixes.len() as f64;
        let after = returned.saturating_sub(prefixes.len() as u64).max(1) as f64;
        let per_record = (total - seek) as f64 / after;
        println!(
            "| BTreeMap (reference) | – | {class} | {} | {:.1} | {:.2} | {:.1} | {} | {} | – | 1.00 | – | – |",
            prefixes.len(),
            returned as f64 / scans,
            seek as f64 / scans / 1e3,
            total as f64 / scans / 1e3,
            if returned > prefixes.len() as u64 {
                format!("{per_record:.0}")
            } else {
                "–".into()
            },
            if returned > prefixes.len() as u64 {
                format!("{:.1}", 1e3 / per_record)
            } else {
                "–".into()
            },
        );
    }
}

/// What one thread of [`readers`] does.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Role {
    Get,
    SnapshotGet,
    /// A prefix scan of 4,096 keys through a read guard.
    Scan,
    /// The same scans through a snapshot.
    SnapshotScan,
    Write,
}

impl Role {
    fn name(self) -> &'static str {
        match self {
            Role::Get => "get",
            Role::SnapshotGet => "snapshot get",
            Role::Scan => "guard scan, 4,096 keys",
            Role::SnapshotScan => "snapshot scan, 4,096 keys",
            Role::Write => "put",
        }
    }
}

/// Concurrent readers, scanners and a writer on a run with updates in the
/// memtable, each for a fixed time.
fn readers() {
    const RECORDS: u64 = 500_000;
    const SECS: f64 = 1.5;
    let values = Values::new();
    let configs: &[&[Role]] = &[
        &[Role::Get],
        &[Role::Get, Role::Get],
        &[Role::SnapshotGet],
        &[Role::SnapshotGet, Role::SnapshotGet],
        &[Role::Scan],
        &[Role::Scan, Role::Scan],
        &[Role::SnapshotScan, Role::SnapshotScan],
        &[Role::Get, Role::Scan],
        &[Role::Write],
        &[Role::Get, Role::Write],
        &[Role::Scan, Role::Write],
        &[Role::SnapshotScan, Role::Write],
    ];
    table(
        &format!(
            "Concurrent reads: {RECORDS} records in a run with updates in the memtable, {SECS} s per row, 2 CPUs"
        ),
        &["threads", "role", "ops/s", "p50 µs", "p99 µs"],
    );
    for roles in configs {
        let fixture = Fixture::new(Shape::MemtableOverRuns, RECORDS, point_key, &values);
        let engine = &fixture.engine;
        let snapshot = engine.snapshot().unwrap();
        let running = AtomicBool::new(true);
        let results = thread::scope(|scope| {
            let handles: Vec<_> = roles
                .iter()
                .enumerate()
                .map(|(at, &role)| {
                    let (snapshot, running, values) = (&snapshot, &running, &values);
                    scope.spawn(move || {
                        let mut rng = Rng::new(70 + at as u64);
                        let mut workspace = EngineWorkspace::new();
                        let mut latency = Latencies::default();
                        while running.load(Ordering::Relaxed) {
                            let index = rng.below(RECORDS);
                            let prefix = &key(index)[..3];
                            match role {
                                Role::Get => latency.time(|| {
                                    black_box(engine.get(&key(index), &mut workspace).unwrap());
                                }),
                                Role::SnapshotGet => latency.time(|| {
                                    black_box(snapshot.get(&key(index), &mut workspace).unwrap());
                                }),
                                Role::Scan => latency.time(|| {
                                    let guard = engine.read();
                                    let mut scan = guard.scan_prefix(prefix, &mut workspace);
                                    while let Some(record) = scan.next_record().unwrap() {
                                        black_box(record.value.len());
                                    }
                                }),
                                Role::SnapshotScan => latency.time(|| {
                                    let mut scan = snapshot.scan_prefix(prefix, &mut workspace);
                                    while let Some(record) = scan.next_record().unwrap() {
                                        black_box(record.value.len());
                                    }
                                }),
                                Role::Write => latency.time(|| {
                                    engine.put(&key(index), values.get(index + 3)).unwrap();
                                }),
                            }
                        }
                        latency
                    })
                })
                .collect();
            thread::sleep(std::time::Duration::from_secs_f64(SECS));
            running.store(false, Ordering::Relaxed);
            handles
                .into_iter()
                .map(|handle| handle.join().unwrap())
                .collect::<Vec<_>>()
        });
        let threads = roles
            .iter()
            .map(|role| role.name())
            .collect::<Vec<_>>()
            .join(" + ");
        for (role, mut latency) in roles.iter().zip(results) {
            latency.0.sort_unstable();
            println!(
                "| {threads} | {} | {:.0} | {:.2} | {:.2} |",
                role.name(),
                latency.0.len() as f64 / SECS,
                latency.us(0.5),
                latency.us(0.99),
            );
        }
    }
}

// Density payloads.

const WORDS: [&str; 32] = [
    "alpha", "bravo", "charlie", "delta", "echo", "foxtrot", "golf", "hotel", "india", "juliet",
    "kilo", "lima", "mike", "november", "oscar", "papa", "quebec", "romeo", "sierra", "tango",
    "uniform", "victor", "whiskey", "xray", "yankee", "zulu", "red", "green", "blue", "amber",
    "north", "south",
];

fn words(rng: &mut Rng, count: u64) -> String {
    (0..count)
        .map(|_| WORDS[rng.below(WORDS.len() as u64) as usize])
        .collect::<Vec<_>>()
        .join(" ")
}

/// A sensor reading: a 12-byte key, and 14 bytes of timestamp, sensor and
/// status with up to 18 random bytes.
fn small_record(index: u64) -> (Vec<u8>, Vec<u8>) {
    let mut rng = Rng::new(1_000 + index);
    let key = [&b"rd/\0"[..], &index.to_be_bytes()].concat();
    let mut value = Vec::with_capacity(32);
    value.extend_from_slice(&(1_700_000_000_000 + index * 250).to_be_bytes());
    value.extend_from_slice(&((index % 1_000) as u32).to_be_bytes());
    value.extend_from_slice(&[(rng.below(4)) as u8, 0]);
    let extra = rng.below(19) as usize;
    value.extend((0..extra).map(|_| rng.next() as u8));
    (key, value)
}

/// A user profile as JSON, about 150 bytes to 1 KiB.
fn json_record(index: u64) -> (Vec<u8>, Vec<u8>) {
    let mut rng = Rng::new(2_000 + index);
    let (first, last) = (WORDS[rng.below(32) as usize], WORDS[rng.below(32) as usize]);
    let bio_words = rng.below(120);
    let value = format!(
        r#"{{"id":{index},"name":"{first} {last}","email":"{first}.{last}{}@example.com","active":{},"score":{},"plan":"{}","tags":["{}","{}"],"created":"2026-{:02}-{:02}T{:02}:{:02}:00Z","bio":"{}"}}"#,
        rng.below(100),
        rng.below(2) == 0,
        rng.below(100_000),
        ["free", "pro", "team"][rng.below(3) as usize],
        WORDS[rng.below(32) as usize],
        WORDS[rng.below(32) as usize],
        1 + rng.below(12),
        1 + rng.below(28),
        rng.below(24),
        rng.below(60),
        words(&mut rng, bio_words),
    );
    (format!("user/{index:08}").into_bytes(), value.into_bytes())
}

/// An order document with 10 to 60 line items, about 1 to 6 KiB.
fn document_record(index: u64) -> (Vec<u8>, Vec<u8>) {
    let mut rng = Rng::new(3_000 + index);
    let items = 10 + rng.below(51);
    let lines: Vec<String> = (0..items)
        .map(|_| {
            let (sku, qty, euros, cents) = (
                rng.below(50_000),
                1 + rng.below(9),
                rng.below(500),
                rng.below(100),
            );
            let note_words = rng.below(6);
            format!(
                r#"{{"sku":"SKU-{sku:06}","qty":{qty},"price":{euros}.{cents:02},"note":"{}"}}"#,
                words(&mut rng, note_words),
            )
        })
        .collect();
    let value = format!(
        r#"{{"order":{index},"customer":"user/{:08}","status":"{}","currency":"EUR","items":[{}]}}"#,
        rng.below(1_000_000),
        ["open", "paid", "shipped", "closed"][rng.below(4) as usize],
        lines.join(","),
    );
    (format!("order/{index:08}").into_bytes(), value.into_bytes())
}

/// Memory per record and access cost for three payloads, in a memtable and
/// in consolidated runs, beside a BTreeMap holding the same records.
fn density() {
    const PROBES: usize = 50_000;
    type Record = fn(u64) -> (Vec<u8>, Vec<u8>);
    let payloads: [(&str, u64, Record); 3] = [
        ("sensor readings, 14–32 B", 1_500_000, small_record),
        ("JSON profiles, ~150 B–1 KiB", 120_000, json_record),
        ("order documents, ~1–6 KiB", 15_000, document_record),
    ];
    table(
        "Memory density: records loaded in random order, then flushed and consolidated",
        &[
            "payload",
            "records",
            "key + value B",
            "logical MiB",
            "memtable charged MiB",
            "memtable heap MiB",
            "run MiB",
            "filter MiB",
            "heap in runs MiB",
            "logical / run bytes",
            "heap B/record in runs",
            "BTreeMap heap MiB",
            "get ns memtable",
            "get ns runs",
            "blocks/get",
            "scan ns/record runs",
            "get ns BTreeMap",
        ],
    );
    // Counted throughout: the engine's heap is the live bytes it adds.
    COUNTING.store(true, Ordering::Relaxed);
    for (name, count, record) in payloads {
        let probes: Vec<Vec<u8>> = shuffled(count, 71)
            .into_iter()
            .take(PROBES)
            .map(|index| record(index).0)
            .collect();
        let get_ns = |get: &mut dyn FnMut(&[u8]) -> bool| {
            ns_per_op(3, probes.len(), || {
                for key in &probes {
                    assert!(get(key));
                }
            })
        };

        let heap = live_heap();
        let engine = Engine::new(options(1 << 30));
        let mut logical = 0usize;
        for index in shuffled(count, 72) {
            let (key, value) = record(index);
            logical += key.len() + value.len();
            engine.put(&key, &value).unwrap();
        }
        let memtable_heap = live_heap() - heap;
        let charged = engine.stats().active.charged;
        let mut workspace = EngineWorkspace::new();
        let get_memtable = get_ns(&mut |key| engine.get(key, &mut workspace).unwrap().is_some());
        drop(workspace);
        engine.flush().unwrap();
        consolidate(&engine);
        let runs_heap = live_heap() - heap;
        let stats = engine.stats();
        let mut workspace = EngineWorkspace::new();
        let before = workspace.blocks_loaded();
        let get_runs = get_ns(&mut |key| engine.get(key, &mut workspace).unwrap().is_some());
        let blocks = (workspace.blocks_loaded() - before) as f64 / (3 * probes.len()) as f64;
        let scan = ns_per_op(3, count as usize, || {
            let guard = engine.read();
            let mut scan = guard.scan(&mut workspace);
            while let Some(record) = scan.next_record().unwrap() {
                black_box(record.value.len());
            }
        });
        drop(workspace);
        drop(engine);

        let heap = live_heap();
        let map: BTreeMap<Vec<u8>, Vec<u8>> = (0..count).map(record).collect();
        let map_heap = live_heap() - heap;
        let get_map = get_ns(&mut |key| black_box(map.get(key)).is_some());
        drop(map);

        println!(
            "| {name} | {count} | {:.0} | {:.1} | {:.1} | {:.1} | {:.1} | {:.1} | {:.1} | {:.2} | {:.0} | {:.1} | {get_memtable:.0} | {get_runs:.0} | {blocks:.2} | {scan:.0} | {get_map:.0} |",
            logical as f64 / count as f64,
            mib(logical as u64),
            mib(charged as u64),
            mib(memtable_heap as u64),
            mib(stats.run_bytes as u64),
            mib(stats.held_bytes as u64),
            mib(runs_heap as u64),
            logical as f64 / stats.run_bytes as f64,
            runs_heap as f64 / count as f64,
            mib(map_heap as u64),
        );
    }
    COUNTING.store(false, Ordering::Relaxed);
}

/// Flushes of a full memtable and compactions of overlapping runs. Each
/// output run builds its key filter as it is written, so the times include
/// filter construction and the reports give its memory.
fn maintenance() {
    const RECORDS: u64 = 250_000;
    const RUNS: u64 = 8;
    const ROUNDS: usize = 3;
    let values = Values::new();
    let payload = (8 + VALUE_LEN) as f64;

    table(
        &format!("Flush: {RECORDS} records of 8 + {VALUE_LEN} bytes"),
        &[
            "ms",
            "MB/s of keys and values",
            "stored / payload",
            "runs",
            "filter bits/key",
            "filter build KiB",
        ],
    );
    let order = shuffled(RECORDS, 6);
    let (secs, report) = median_secs(ROUNDS, || {
        let engine = Engine::new(options(1 << 30));
        fill(&engine, &order, &values);
        let start = Instant::now();
        let report = engine.flush().unwrap();
        (start.elapsed().as_secs_f64(), report)
    });
    println!(
        "| {:.1} | {:.0} | {:.2} | {} | {:.1} | {} |",
        secs * 1e3,
        RECORDS as f64 * payload / secs / 1e6,
        report.run_bytes as f64 / (RECORDS as f64 * payload),
        report.runs,
        report.held_bytes as f64 * 8.0 / RECORDS as f64,
        report.filter_build_bytes >> 10,
    );

    table(
        &format!("Compaction: {RUNS} overlapping runs of {RECORDS} random keys each"),
        &[
            "ms",
            "MB/s of input runs",
            "input MiB",
            "output MiB",
            "versions dropped",
            "filter build KiB",
            "workspace KiB",
        ],
    );
    let (secs, report) = median_secs(ROUNDS, || {
        let engine = Engine::new(options(1 << 30));
        let mut rng = Rng::new(7);
        let key_space = RECORDS * RUNS / 2;
        for _ in 0..RUNS {
            for _ in 0..RECORDS {
                let index = rng.below(key_space);
                engine.put(&key(index), values.get(index)).unwrap();
            }
            engine.flush().unwrap();
        }
        let options = CompactionOptions {
            max_overlap: 1,
            max_inputs: RUNS as usize,
            ..CompactionOptions::default()
        };
        let mut workspace = MergeWorkspace::new();
        let start = Instant::now();
        let report = engine.compact(options, &mut workspace).unwrap().unwrap();
        (start.elapsed().as_secs_f64(), report)
    });
    println!(
        "| {:.1} | {:.0} | {:.1} | {:.1} | {} | {} | {} |",
        secs * 1e3,
        report.input_bytes as f64 / secs / 1e6,
        mib(report.input_bytes as u64),
        mib(report.output_bytes as u64),
        report.stats.versions_dropped,
        report.filter_build_bytes >> 10,
        report.workspace_bytes >> 10,
    );
}

/// What a snapshot costs to take, and what live snapshots cost writes and
/// memory while they are refreshed under overwrites.
fn snapshots() {
    let values = Values::new();

    table(
        "Taking a snapshot after one write",
        &["records in memtable", "runs", "snapshot ns"],
    );
    for records in [1_000u64, 100_000, 1_000_000] {
        let engine = Engine::new(options(1 << 30));
        fill(&engine, &shuffled(records, 8), &values);
        if records == 1_000_000 {
            engine.flush().unwrap();
            fill(&engine, &shuffled(records / 10, 9), &values);
        }
        let mut at = 0;
        let ns = ns_per_op(5, 10_000, || {
            for _ in 0..10_000 {
                at += 1;
                engine.put(&key(at % records), values.get(at)).unwrap();
                black_box(engine.snapshot().unwrap());
            }
        }) - ns_per_op(5, 10_000, || {
            for _ in 0..10_000 {
                at += 1;
                engine.put(&key(at % records), values.get(at)).unwrap();
            }
        });
        println!("| {records} | {} | {ns:.0} |", engine.stats().runs);
    }

    const KEYS: u64 = 200_000;
    const WRITES: u64 = 1_000_000;
    const REFRESH: u64 = 20_000;
    table(
        &format!(
            "Overwrites of {KEYS} keys with live snapshots, one replaced every {REFRESH} writes, 16 MiB memtable"
        ),
        &[
            "live snapshots",
            "writes k/s",
            "p99 µs",
            "flushes",
            "peak retained MiB",
            "peak pinned MiB",
            "retained after drop",
        ],
    );
    for live in [0usize, 1, 4, 16] {
        let engine = Engine::new(options(16 << 20));
        fill(&engine, &shuffled(KEYS, 10), &values);
        engine.flush().unwrap();
        let flushes = engine.stats().flushes;
        let mut held = std::collections::VecDeque::new();
        let mut rng = Rng::new(11);
        let mut latency = Latencies::default();
        let (mut retained, mut pinned) = (0, 0);
        let start = Instant::now();
        for write in 0..WRITES {
            let index = rng.below(KEYS);
            latency.time(|| engine.put(&key(index), values.get(write)).unwrap());
            if live > 0 && write % REFRESH == 0 {
                // Sampled before the oldest is replaced, when most is held.
                let stats = engine.stats();
                retained = retained.max(stats.retained_bytes);
                pinned = pinned.max(stats.active.pinned);
                if held.len() == live {
                    held.pop_front();
                }
                held.push_back(engine.snapshot().unwrap());
            }
        }
        let secs = start.elapsed().as_secs_f64();
        drop(held);
        let stats = engine.stats();
        latency.0.sort_unstable();
        println!(
            "| {live} | {:.0} | {:.1} | {} | {:.1} | {:.1} | {} |",
            WRITES as f64 / secs / 1e3,
            latency.us(0.99),
            stats.flushes - flushes,
            mib(retained as u64),
            mib(pinned as u64),
            stats.retained_bytes,
        );
    }
}

/// Writers putting random keys into a small memtable, which their writes
/// flush, with background compaction.
fn ingest() {
    const WRITES: u64 = 1_000_000;
    const KEYS: u64 = 400_000;
    let values = Values::new();
    table(
        &format!(
            "Sustained ingestion: {WRITES} writes over {KEYS} keys, 4 MiB memtable, background compaction"
        ),
        &[
            "writers",
            "writes k/s",
            "p50 µs",
            "p99 µs",
            "max µs",
            "flushes",
            "compactions",
            "runs at end",
            "max overlap",
            "run MiB",
            "peak RSS MiB",
        ],
    );
    for writers in [1u64, 2, 4] {
        let engine = Engine::new(EngineOptions {
            background_compaction: Some(CompactionOptions::default()),
            ..options(4 << 20)
        });
        let start = Instant::now();
        let latency = Latencies::merge(thread::scope(|scope| {
            let handles: Vec<_> = (0..writers)
                .map(|writer| {
                    let (engine, values) = (&engine, &values);
                    scope.spawn(move || {
                        let mut rng = Rng::new(20 + writer);
                        let mut latency = Latencies::default();
                        for _ in 0..WRITES / writers {
                            let index = rng.below(KEYS);
                            latency.time(|| engine.put(&key(index), values.get(index)).unwrap());
                        }
                        latency
                    })
                })
                .collect();
            handles
                .into_iter()
                .map(|handle| handle.join().unwrap())
                .collect::<Vec<_>>()
        }));
        let secs = start.elapsed().as_secs_f64();
        engine.wait_for_compaction().unwrap();
        let stats = engine.stats();
        let background = engine.background_status().unwrap();
        println!(
            "| {writers} | {:.0} | {:.1} | {:.1} | {:.0} | {} | {} | {} | {} | {:.1} | {:.0} |",
            WRITES as f64 / secs / 1e3,
            latency.us(0.5),
            latency.us(0.99),
            latency.max_us(),
            stats.flushes,
            background.compactions,
            stats.runs,
            stats.max_overlap,
            mib(stats.run_bytes as u64),
            peak_rss_mib(),
        );
    }
}

/// Readers doing point lookups and prefix scans beside sustained writers.
fn mixed() {
    const WRITES: u64 = 500_000;
    const KEYS: u64 = 400_000;
    let values = Values::new();
    table(
        &format!(
            "Readers beside writers: {WRITES} writes over {KEYS} preloaded keys, 4 MiB memtable, background compaction; one read in 16 is a prefix scan"
        ),
        &[
            "writers",
            "readers",
            "writes k/s",
            "write p99 µs",
            "gets k/s",
            "get p99 µs",
            "scan p99 µs",
        ],
    );
    for (writers, readers) in [(1u64, 1u64), (1, 3), (2, 2)] {
        let engine = Engine::new(EngineOptions {
            background_compaction: Some(CompactionOptions::default()),
            ..options(4 << 20)
        });
        fill(&engine, &shuffled(KEYS, 12), &values);
        engine.wait_for_compaction().unwrap();
        let writing = AtomicBool::new(true);
        let start = Instant::now();
        let (writes, (gets, scans), write_secs) = thread::scope(|scope| {
            let readers: Vec<_> = (0..readers)
                .map(|reader| {
                    let (engine, writing) = (&engine, &writing);
                    scope.spawn(move || {
                        let mut rng = Rng::new(30 + reader);
                        let mut workspace = EngineWorkspace::new();
                        let (mut gets, mut scans) = (Latencies::default(), Latencies::default());
                        let mut at = 0u64;
                        while writing.load(Ordering::Relaxed) {
                            let index = rng.below(KEYS);
                            if at.is_multiple_of(16) {
                                scans.time(|| {
                                    let guard = engine.read();
                                    let mut scan =
                                        guard.scan_prefix(&key(index)[..4], &mut workspace);
                                    while let Some(record) = scan.next_record().unwrap() {
                                        black_box(record.value.len());
                                    }
                                });
                            } else {
                                gets.time(|| {
                                    black_box(engine.get(&key(index), &mut workspace).unwrap())
                                });
                            }
                            at += 1;
                        }
                        (gets, scans)
                    })
                })
                .collect();
            let writers: Vec<_> = (0..writers)
                .map(|writer| {
                    let (engine, values) = (&engine, &values);
                    scope.spawn(move || {
                        let mut rng = Rng::new(40 + writer);
                        let mut latency = Latencies::default();
                        for _ in 0..WRITES / writers {
                            let index = rng.below(KEYS);
                            latency.time(|| engine.put(&key(index), values.get(index)).unwrap());
                        }
                        latency
                    })
                })
                .collect();
            let writes: Vec<_> = writers.into_iter().map(|w| w.join().unwrap()).collect();
            let write_secs = start.elapsed().as_secs_f64();
            writing.store(false, Ordering::Relaxed);
            let (gets, scans): (Vec<_>, Vec<_>) =
                readers.into_iter().map(|r| r.join().unwrap()).unzip();
            (
                Latencies::merge(writes),
                (Latencies::merge(gets), Latencies::merge(scans)),
                write_secs,
            )
        });
        println!(
            "| {writers} | {readers} | {:.0} | {:.1} | {:.0} | {:.1} | {:.1} |",
            writes.0.len() as f64 / write_secs / 1e3,
            writes.us(0.99),
            gets.0.len() as f64 / write_secs / 1e3,
            gets.us(0.99),
            scans.us(0.99),
        );
    }
}

// Mixed workloads.

/// One client operation of [`workloads`], on an index of the scan layout.
#[derive(Clone, Copy)]
enum Op {
    Get(u64),
    Absent(u64),
    Short(u64),
    Medium(u64),
    Put(u64, u64),
    Delete(u64),
}

/// What [`workloads`] reports per operation: the first four are point
/// lookups, by what the key was before the timed run.
const CLASSES: [&str; 7] = [
    "get, present",
    "get, overwritten before the run",
    "get, deleted before the run",
    "get, absent",
    "scan, 6-byte prefix",
    "scan of a snapshot, 4-byte prefix",
    "write",
];
const WRITE: usize = 6;

/// A mix of operations, per mille; the rest are writes.
struct Scenario {
    name: &'static str,
    ops_per_client: usize,
    gets: u64,
    short: u64,
    medium: u64,
    /// Whether a snapshot of the starting state is held throughout, as a
    /// long-lived reader would, and checked afterwards.
    long_snapshot: bool,
}

const SCENARIOS: [Scenario; 3] = [
    Scenario {
        name: "read-heavy",
        ops_per_client: 400_000,
        gets: 800,
        short: 120,
        medium: 30,
        long_snapshot: false,
    },
    Scenario {
        name: "balanced",
        ops_per_client: 200_000,
        gets: 400,
        short: 80,
        medium: 20,
        long_snapshot: true,
    },
    Scenario {
        name: "write-heavy",
        ops_per_client: 400_000,
        gets: 40,
        short: 10,
        medium: 5,
        long_snapshot: false,
    },
];

const CLIENTS: u64 = 2;

/// A client's operations, from its own seed. Client `c` writes only indices
/// congruent to `c` modulo [`CLIENTS`], so the final state does not depend on
/// how the clients interleave. One get in 8 is of an absent key.
fn plan(scenario: &Scenario, client: u64) -> Vec<Op> {
    let records = SCAN_GROUP * SCAN_GROUPS;
    let mut rng = Rng::new(70 + client);
    (0..scenario.ops_per_client)
        .map(|_| {
            let roll = rng.below(1000);
            let index = rng.below(records);
            if roll < scenario.gets {
                if index % 8 == 7 {
                    Op::Absent(index)
                } else {
                    Op::Get(index)
                }
            } else if roll < scenario.gets + scenario.short {
                Op::Short(index)
            } else if roll < scenario.gets + scenario.short + scenario.medium {
                Op::Medium(index)
            } else {
                let index = index / CLIENTS * CLIENTS + client;
                if rng.below(10) == 0 {
                    Op::Delete(index)
                } else {
                    Op::Put(index, rng.next())
                }
            }
        })
        .collect()
}

/// A key between two keys of the scan layout, inside the dataset's range.
fn absent_scan_key(index: u64) -> Vec<u8> {
    let mut key = scan_key(index);
    key[7] = 0x7F;
    key
}

/// CPU seconds from a `/proc` stat file: user plus system, at 100 ticks a
/// second; 0 where Linux does not report it.
fn cpu_secs(path: &str) -> f64 {
    std::fs::read_to_string(path)
        .ok()
        .and_then(|stat| {
            let fields: Vec<&str> = stat.rsplit_once(')')?.1.split_whitespace().collect();
            let user: f64 = fields.get(11)?.parse().ok()?;
            let system: f64 = fields.get(12)?.parse().ok()?;
            Some((user + system) / 100.0)
        })
        .unwrap_or(0.0)
}

/// The process's current resident memory in MiB, where Linux reports it.
fn rss_mib() -> f64 {
    std::fs::read_to_string("/proc/self/status")
        .ok()
        .and_then(|status| {
            let line = status.lines().find(|line| line.starts_with("VmRSS:"))?;
            line.split_whitespace().nth(1)?.parse::<f64>().ok()
        })
        .map_or(0.0, |kib| kib / 1024.0)
}

/// What one client measured.
#[derive(Default)]
struct Client {
    latency: [Latencies; 7],
    /// Time to the first record (or to the end of an empty scan).
    first: [Latencies; 7],
    blocks: [u64; 7],
    records: [u64; 7],
    cpu: f64,
}

/// Drains `scan`, recording the time from `start` to its first record.
fn drain(mut scan: Scan<'_>, start: Instant, first: &mut Latencies) -> u64 {
    let mut records = 0;
    while let Some(record) = scan.next_record().unwrap() {
        if records == 0 {
            first.0.push(start.elapsed().as_nanos() as u64);
        }
        black_box(record.value.len());
        records += 1;
    }
    if records == 0 {
        first.0.push(start.elapsed().as_nanos() as u64);
    }
    records
}

fn client(engine: &Engine, ops: &[Op], values: &Values) -> Client {
    let mut out = Client::default();
    let mut workspace = EngineWorkspace::new();
    for &op in ops {
        let before = workspace.blocks_loaded();
        let start = Instant::now();
        let (class, records) = match op {
            Op::Get(index) => {
                let key = scan_key(index);
                let class = match value_seed(index) {
                    None => 2,
                    Some(seed) if seed != index => 1,
                    Some(_) => 0,
                };
                let start = Instant::now();
                let found = engine.get(&key, &mut workspace).unwrap();
                black_box(found.map(|found| found.value.len()));
                out.latency[class].0.push(start.elapsed().as_nanos() as u64);
                (class, 0)
            }
            Op::Absent(index) => {
                let key = absent_scan_key(index);
                let start = Instant::now();
                assert!(engine.get(&key, &mut workspace).unwrap().is_none());
                out.latency[3].0.push(start.elapsed().as_nanos() as u64);
                (3, 0)
            }
            Op::Short(index) => {
                let prefix = scan_key(index);
                let guard = engine.read();
                let records = drain(
                    guard.scan_prefix(&prefix[..6], &mut workspace),
                    start,
                    &mut out.first[4],
                );
                drop(guard);
                out.latency[4].0.push(start.elapsed().as_nanos() as u64);
                (4, records)
            }
            Op::Medium(index) => {
                let prefix = scan_key(index);
                let snapshot = engine.snapshot().unwrap();
                let records = drain(
                    snapshot.scan_prefix(&prefix[..4], &mut workspace),
                    start,
                    &mut out.first[5],
                );
                drop(snapshot);
                out.latency[5].0.push(start.elapsed().as_nanos() as u64);
                (5, records)
            }
            Op::Put(index, seed) => {
                engine.put(&scan_key(index), values.get(seed)).unwrap();
                out.latency[WRITE].0.push(start.elapsed().as_nanos() as u64);
                (WRITE, 0)
            }
            Op::Delete(index) => {
                engine.delete(&scan_key(index)).unwrap();
                out.latency[WRITE].0.push(start.elapsed().as_nanos() as u64);
                (WRITE, 0)
            }
        };
        out.blocks[class] += workspace.blocks_loaded() - before;
        out.records[class] += records;
    }
    out.cpu = cpu_secs("/proc/thread-self/stat");
    out
}

/// Maxima of the engine's state, sampled while the clients run.
#[derive(Default)]
struct Peaks {
    runs: usize,
    overlap: usize,
    run_bytes: usize,
    memtables: usize,
    retained: usize,
}

impl Peaks {
    fn sample(&mut self, stats: &EngineStats) {
        self.runs = self.runs.max(stats.runs);
        self.overlap = self.overlap.max(stats.max_overlap);
        self.run_bytes = self.run_bytes.max(stats.run_bytes + stats.held_bytes);
        let frozen = stats.frozen.map_or(0, |frozen| frozen.charged);
        self.memtables = self.memtables.max(stats.active.charged + frozen);
        self.retained = self.retained.max(stats.retained_bytes);
    }
}

/// Every live record a view returns for `prefix`, checked against the model
/// of value seeds.
fn check_scan(
    scan: Scan<'_>,
    model: &BTreeMap<Vec<u8>, u64>,
    prefix: &[u8],
    values: &Values,
) -> usize {
    let mut expected = model
        .range(prefix.to_vec()..)
        .take_while(|(key, _)| key.starts_with(prefix));
    let mut scan = scan;
    let mut records = 0;
    while let Some(record) = scan.next_record().unwrap() {
        let (key, &seed) = expected.next().expect("extra record");
        assert_eq!(record.key, key.as_slice());
        assert_eq!(record.value, values.get(seed));
        records += 1;
    }
    assert!(expected.next().is_none(), "missing record in {prefix:?}");
    records
}

/// Read-heavy, balanced and write-heavy mixes from two clients beside
/// background compaction, on one codec; see `docs/benchmarks.md`. Each
/// codec runs in its own process, so that resident memory is its own.
fn workloads(compression: Compression) {
    const RECORDS: u64 = SCAN_GROUP * SCAN_GROUPS;
    let values = Values::new();
    let preload: BTreeMap<Vec<u8>, u64> = (0..RECORDS)
        .filter_map(|index| Some((scan_key(index), value_seed(index)?)))
        .collect();
    let codec = format!("{compression:?}");
    let (mut reads, mut writes, mut memory) = (Vec::new(), Vec::new(), Vec::new());
    for scenario in &SCENARIOS {
        let engine = Engine::new(EngineOptions {
            background_compaction: Some(CompactionOptions::default()),
            compression,
            ..options(4 << 20)
        });
        // The starting state: every key in one run, then a seventh deleted
        // and a fifth of the rest overwritten in newer runs.
        for &index in &shuffled(RECORDS, 12) {
            engine.put(&scan_key(index), values.get(index)).unwrap();
        }
        engine.flush().unwrap();
        engine.wait_for_compaction().unwrap();
        consolidate(&engine);
        for index in 0..RECORDS {
            match value_seed(index) {
                None => {
                    engine.delete(&scan_key(index)).unwrap();
                }
                Some(seed) if seed != index => {
                    engine.put(&scan_key(index), values.get(seed)).unwrap();
                }
                Some(_) => {}
            }
        }
        engine.flush().unwrap();
        engine.wait_for_compaction().unwrap();
        let initial = engine.stats();
        let plans: Vec<Vec<Op>> = (0..CLIENTS).map(|c| plan(scenario, c)).collect();
        let held = scenario.long_snapshot.then(|| engine.snapshot().unwrap());
        let background = engine.background_status().unwrap();
        let mut peaks = Peaks::default();
        peaks.sample(&initial);
        let running = std::sync::atomic::AtomicUsize::new(plans.len());

        let cpu = cpu_secs("/proc/self/stat");
        let start = Instant::now();
        let clients: Vec<Client> = thread::scope(|scope| {
            let handles: Vec<_> = plans
                .iter()
                .map(|ops| {
                    let (engine, values, running) = (&engine, &values, &running);
                    scope.spawn(move || {
                        let out = client(engine, ops, values);
                        running.fetch_sub(1, Ordering::Relaxed);
                        out
                    })
                })
                .collect();
            while running.load(Ordering::Relaxed) > 0 {
                peaks.sample(&engine.stats());
                thread::sleep(std::time::Duration::from_millis(10));
            }
            handles.into_iter().map(|h| h.join().unwrap()).collect()
        });
        let wall = start.elapsed().as_secs_f64();
        let settling = Instant::now();
        engine.wait_for_compaction().unwrap();
        let settle = settling.elapsed().as_secs_f64();
        let cpu = cpu_secs("/proc/self/stat") - cpu;
        let client_cpu: f64 = clients.iter().map(|c| c.cpu).sum();
        let stats = engine.stats();
        peaks.sample(&stats);
        let after = engine.background_status().unwrap();
        assert_eq!(after.failures, 0, "{:?}", after.last_failure);

        // Untimed: the final state, a held snapshot and prefix boundaries
        // against the model.
        let mut model = preload.clone();
        for op in plans.iter().flatten() {
            match *op {
                Op::Put(index, seed) => {
                    model.insert(scan_key(index), seed);
                }
                Op::Delete(index) => {
                    model.remove(&scan_key(index));
                }
                _ => {}
            }
        }
        let mut workspace = EngineWorkspace::new();
        let guard = engine.read();
        assert_eq!(
            check_scan(guard.scan_prefix(&[], &mut workspace), &model, &[], &values),
            model.len()
        );
        let mut rng = Rng::new(80);
        for len in [2, 3, 4, 6, 8] {
            for _ in 0..32 {
                let prefix = scan_key(rng.below(RECORDS))[..len].to_vec();
                check_scan(
                    guard.scan_prefix(&prefix, &mut workspace),
                    &model,
                    &prefix,
                    &values,
                );
            }
        }
        for prefix in [
            &[0xFF, 0xFF][..],
            &[0x00],
            &[0x01, 0xFF],
            &[0xFF, 0xFF, 0xFF],
        ] {
            check_scan(
                guard.scan_prefix(prefix, &mut workspace),
                &model,
                prefix,
                &values,
            );
        }
        drop(guard);
        if let Some(held) = &held {
            check_scan(
                held.scan_prefix(&[], &mut workspace),
                &preload,
                &[],
                &values,
            );
        }

        let merged: Vec<Latencies> = (0..CLASSES.len())
            .map(|class| {
                Latencies::merge(
                    clients
                        .iter()
                        .map(|c| Latencies(c.latency[class].0.clone())),
                )
            })
            .collect();
        for (class, name) in CLASSES.iter().enumerate().take(WRITE) {
            let latency = &merged[class];
            let ops = latency.0.len();
            if ops == 0 {
                continue;
            }
            let blocks: u64 = clients.iter().map(|c| c.blocks[class]).sum();
            let records: u64 = clients.iter().map(|c| c.records[class]).sum();
            let (scan_columns, first) = if class >= 4 {
                let first =
                    Latencies::merge(clients.iter().map(|c| Latencies(c.first[class].0.clone())));
                let total: u64 = latency.0.iter().sum();
                let to_first: u64 = first.0.iter().sum();
                let after = records.saturating_sub(ops as u64).max(1);
                (
                    format!(
                        "{:.1} | {:.0}",
                        records as f64 / ops as f64,
                        (total - to_first) as f64 / after as f64
                    ),
                    format!("{:.2}", first.us(0.5)),
                )
            } else {
                ("– | –".into(), "–".into())
            };
            reads.push(format!(
                "| {} | {codec} | {name} | {ops} | {:.0} | {:.2} | {:.2} | {:.2} | {first} | {scan_columns} |",
                scenario.name,
                ops as f64 / wall / 1e3,
                latency.us(0.5),
                latency.us(0.99),
                blocks as f64 / ops as f64,
            ));
        }
        let written = &merged[WRITE];
        writes.push(format!(
            "| {} | {codec} | {wall:.2} | {:.0} | {:.1} | {:.1} | {:.0} | {} | {} | {:.1} → {:.1} | {settle:.2} | {} → {} (peak {}) | {} | {client_cpu:.1} | {:.1} |",
            scenario.name,
            written.0.len() as f64 / wall / 1e3,
            written.us(0.5),
            written.us(0.99),
            written.max_us(),
            stats.flushes - initial.flushes,
            after.compactions - background.compactions,
            mib(after.input_bytes - background.input_bytes),
            mib(after.output_bytes - background.output_bytes),
            initial.runs,
            stats.runs,
            peaks.runs,
            peaks.overlap,
            cpu - client_cpu,
        ));
        let logical: usize = model.len() * (8 + VALUE_LEN);
        memory.push(format!(
            "| {} | {codec} | {:.1} | {:.1} | {:.1} | {:.2} | {:.1} | {:.1} | {:.1} | {:.0} | {:.0} |",
            scenario.name,
            mib(logical as u64),
            mib(stats.run_bytes as u64),
            mib(stats.held_bytes as u64),
            logical as f64 / stats.run_bytes as f64,
            mib(peaks.run_bytes as u64),
            mib(peaks.memtables as u64),
            mib(peaks.retained as u64),
            rss_mib(),
            peak_rss_mib(),
        ));
        drop(held);
    }

    table(
        &format!(
            "Mixed workloads, reads: {RECORDS} records of 8 + {VALUE_LEN} bytes, {CLIENTS} clients, 4 MiB memtable, background compaction"
        ),
        &[
            "scenario",
            "codec",
            "operation",
            "ops",
            "k ops/s",
            "p50 µs",
            "p99 µs",
            "blocks/op",
            "first record p50 µs",
            "records/scan",
            "ns/record after first",
        ],
    );
    reads.iter().for_each(|row| println!("{row}"));
    table(
        "Mixed workloads, writes and maintenance",
        &[
            "scenario",
            "codec",
            "wall s",
            "writes k/s",
            "write p50 µs",
            "write p99 µs",
            "write max µs",
            "flushes",
            "compactions",
            "compacted MiB in → out",
            "settle s",
            "runs start → end",
            "max overlap",
            "client CPU s",
            "other CPU s",
        ],
    );
    writes.iter().for_each(|row| println!("{row}"));
    table(
        "Mixed workloads, memory",
        &[
            "scenario",
            "codec",
            "live logical MiB",
            "run MiB",
            "filter MiB",
            "logical / run",
            "peak runs + filters MiB",
            "peak memtables MiB",
            "peak retained MiB",
            "RSS MiB",
            "peak RSS MiB",
        ],
    );
    memory.iter().for_each(|row| println!("{row}"));
}

/// Durable writes from several writers, which group commit batches into
/// shared syncs.
fn durable(base: &Path) {
    const WRITES: u64 = 200_000;
    const KEYS: u64 = 400_000;
    let values = Values::new();
    table(
        &format!(
            "Durable writes: {WRITES} writes, 16 MiB memtable, log on {}",
            base.display()
        ),
        &[
            "writers",
            "writes k/s",
            "p50 µs",
            "p99 µs",
            "syncs",
            "writes/sync",
            "flushes",
            "log MiB",
        ],
    );
    for writers in [1u64, 4, 8] {
        let scratch = Scratch::new(base, "durable");
        let durable = DurableOptions {
            max_log_bytes: u64::MAX,
            ..DurableOptions::default()
        };
        let engine = Engine::create(&scratch.0, options(16 << 20), durable).unwrap();
        let start = Instant::now();
        let latency = Latencies::merge(thread::scope(|scope| {
            let handles: Vec<_> = (0..writers)
                .map(|writer| {
                    let (engine, values) = (&engine, &values);
                    scope.spawn(move || {
                        let mut rng = Rng::new(50 + writer);
                        let mut latency = Latencies::default();
                        for _ in 0..WRITES / writers {
                            let index = rng.below(KEYS);
                            latency.time(|| engine.put(&key(index), values.get(index)).unwrap());
                        }
                        latency
                    })
                })
                .collect();
            handles
                .into_iter()
                .map(|handle| handle.join().unwrap())
                .collect::<Vec<_>>()
        }));
        let secs = start.elapsed().as_secs_f64();
        let log = engine.log_status().unwrap();
        println!(
            "| {writers} | {:.0} | {:.1} | {:.1} | {} | {:.1} | {} | {:.1} |",
            WRITES as f64 / secs / 1e3,
            latency.us(0.5),
            latency.us(0.99),
            log.syncs,
            log.records as f64 / log.syncs.max(1) as f64,
            engine.stats().flushes,
            mib(log.bytes),
        );
    }
}

/// Checkpoints of RAM-only and durable engines, recovery from them, and
/// replay of the log written after a checkpoint.
fn recovery(base: &Path) {
    const RECORDS: u64 = 500_000;
    const TAIL: u64 = 100_000;
    let values = Values::new();
    let engine_options = || options(64 << 20);

    table(
        &format!(
            "Checkpoint and recovery: {RECORDS} records, files on {}",
            base.display()
        ),
        &["engine", "step", "ms", "MiB", "records/s"],
    );
    let row = |engine: &str, step: &str, start: Instant, bytes: u64, records: u64| {
        let secs = start.elapsed().as_secs_f64();
        println!(
            "| {engine} | {step} | {:.1} | {:.1} | {:.0} |",
            secs * 1e3,
            mib(bytes),
            records as f64 / secs
        );
    };

    let scratch = Scratch::new(base, "checkpoint");
    let engine = Engine::new(engine_options());
    fill(&engine, &shuffled(RECORDS, 13), &values);
    let start = Instant::now();
    let report = engine
        .checkpoint(&scratch.0, CheckpointOptions::default())
        .unwrap();
    row("RAM-only", "checkpoint", start, report.bytes, RECORDS);
    drop(engine);
    let start = Instant::now();
    let engine = Engine::recover(&scratch.0, engine_options()).unwrap();
    row("RAM-only", "recover", start, report.bytes, RECORDS);
    assert_eq!(engine.stats().last_seq, RECORDS);
    drop((engine, scratch));

    let scratch = Scratch::new(base, "recovery");
    let durable = DurableOptions {
        max_log_bytes: u64::MAX,
        ..DurableOptions::default()
    };
    let engine = Engine::create(&scratch.0, engine_options(), durable).unwrap();
    fill(&engine, &shuffled(RECORDS, 14), &values);
    let start = Instant::now();
    let report = engine
        .durable_checkpoint(CheckpointOptions::default())
        .unwrap();
    row("durable", "checkpoint", start, report.bytes, RECORDS);
    let tail = shuffled(TAIL, 15);
    fill(&engine, &tail, &values);
    let log_bytes = engine.log_status().unwrap().bytes;
    drop(engine);
    let start = Instant::now();
    let (engine, opened) = Engine::open(&scratch.0, engine_options(), durable).unwrap();
    row(
        "durable",
        &format!("open, replaying {} records", opened.replayed),
        start,
        report.bytes + log_bytes,
        RECORDS + opened.replayed,
    );
    assert_eq!(opened.replayed, TAIL);
    drop(engine);
}

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let base = PathBuf::from(args.get(1).map_or("/tmp", String::as_str));
    println!(
        "Runs built with {:?} in {} B blocks.",
        compression(),
        block_len()
    );
    let start = Instant::now();
    match args.first().map(String::as_str) {
        Some("memtable") => memtable(),
        Some("lookups") => lookups(),
        Some("scans") => scans(),
        Some("readers") => readers(),
        Some("density") => density(),
        Some("maintenance") => maintenance(),
        Some("snapshots") => snapshots(),
        Some("ingest") => ingest(),
        Some("mixed") => mixed(),
        Some("workloads-zstd") => workloads(Compression::Zstd),
        Some("workloads-lz4") => workloads(Compression::Lz4),
        Some("workloads-none") => workloads(Compression::Uncompressed),
        Some("durable") => durable(&base),
        Some("recovery") => recovery(&base),
        _ => {
            eprintln!(
                "usage: bench MODE [DIR], MODE one of memtable lookups scans readers density \
                 maintenance snapshots ingest mixed workloads-zstd workloads-lz4 workloads-none \
                 durable recovery"
            );
            std::process::exit(2);
        }
    }
    println!(
        "\n{} took {:.1} s; peak RSS {:.0} MiB",
        args[0],
        start.elapsed().as_secs_f64(),
        peak_rss_mib()
    );
}
