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
//! | `reads`       | point lookups, absent keys (key filters) and prefix scans  |
//! | `maintenance` | flush and compaction throughput and filter construction    |
//! | `snapshots`   | snapshot cost, and writes and retained memory beside them  |
//! | `ingest`      | sustained writes with automatic flushes and compaction     |
//! | `mixed`       | concurrent readers beside sustained writers                |
//! | `durable`     | durable writes and group commit                            |
//! | `recovery`    | checkpoints, recovery and log replay                       |
//!
//! `durable` and `recovery` write under DIR, default `/tmp`. Run the suite
//! through `scripts/validate.sh bench`, which applies the resource limits and
//! keeps fixtures on tmpfs. Data comes from fixed seeds, so every run sees
//! the same keys and values; timings vary with the machine, so compare
//! configurations only within one environment and one pass of the suite.

use std::hint::black_box;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::Instant;

use tessera::engine::{
    CheckpointOptions, CompactionOptions, DurableOptions, Engine, EngineOptions, EngineWorkspace,
    MergeWorkspace, SnapshotLimits,
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
        max_runs: usize::MAX,
        max_run_bytes: usize::MAX,
        snapshots: SnapshotLimits {
            max_live: 1 << 10,
            max_retained_bytes: usize::MAX,
        },
        ..EngineOptions::default()
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

/// Point lookups and prefix scans over runs, as flushed and once
/// consolidated, with key filters deciding most absent keys.
fn reads() {
    const RECORDS: u64 = 1_000_000;
    const PROBES: u64 = 200_000;
    const ROUNDS: usize = 5;
    let values = Values::new();
    let engine = Engine::new(options(16 << 20));
    fill(&engine, &shuffled(RECORDS, 4), &values);
    engine.flush().unwrap();
    let probes = shuffled(RECORDS, 5);
    let probes = &probes[..PROBES as usize];
    let groups: Vec<u64> = probes.iter().map(|index| index / GROUP * GROUP).collect();

    table(
        &format!("Reads: {RECORDS} records in runs, {PROBES} probes"),
        &[
            "runs",
            "max overlap",
            "filter bits/key",
            "get present ns",
            "blocks/get",
            "get absent ns",
            "blocks/absent get",
            "prefix scan ns (16 records)",
            "full scan ns/record",
        ],
    );
    for shape in ["as flushed", "consolidated"] {
        if shape == "consolidated" {
            consolidate(&engine);
        }
        let stats = engine.stats();
        let mut workspace = EngineWorkspace::new();
        let blocks = |workspace: &EngineWorkspace, before: u64| {
            (workspace.blocks_loaded() - before) as f64 / (PROBES as f64 * ROUNDS as f64)
        };
        let before = workspace.blocks_loaded();
        let present = ns_per_op(ROUNDS, probes.len(), || {
            for &index in probes {
                black_box(engine.get(&key(index), &mut workspace).unwrap().unwrap());
            }
        });
        let present_blocks = blocks(&workspace, before);
        let before = workspace.blocks_loaded();
        let absent = ns_per_op(ROUNDS, probes.len(), || {
            for &index in probes {
                assert!(
                    engine
                        .get(&absent_key(index), &mut workspace)
                        .unwrap()
                        .is_none()
                );
            }
        });
        let absent_blocks = blocks(&workspace, before);
        let scans = ns_per_op(ROUNDS, groups.len() / 10, || {
            let guard = engine.read();
            for &group in &groups[..groups.len() / 10] {
                let mut scan = guard.scan_prefix(&key(group)[..4], &mut workspace);
                while let Some(record) = scan.next_record().unwrap() {
                    black_box(record.value.len());
                }
            }
        });
        let full = ns_per_op(3, RECORDS as usize, || {
            let guard = engine.read();
            let mut scan = guard.scan(&mut workspace);
            while let Some(record) = scan.next_record().unwrap() {
                black_box(record.value.len());
            }
        });
        println!(
            "| {} ({shape}) | {} | {:.1} | {present:.0} | {present_blocks:.2} | {absent:.0} | {absent_blocks:.3} | {scans:.0} | {full:.1} |",
            stats.runs,
            stats.max_overlap,
            stats.filter_bytes as f64 * 8.0 / RECORDS as f64,
        );
    }
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
        report.filter_bytes as f64 * 8.0 / RECORDS as f64,
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
    let start = Instant::now();
    match args.first().map(String::as_str) {
        Some("memtable") => memtable(),
        Some("reads") => reads(),
        Some("maintenance") => maintenance(),
        Some("snapshots") => snapshots(),
        Some("ingest") => ingest(),
        Some("mixed") => mixed(),
        Some("durable") => durable(&base),
        Some("recovery") => recovery(&base),
        _ => {
            eprintln!(
                "usage: bench MODE [DIR], MODE one of memtable reads maintenance snapshots \
                 ingest mixed durable recovery"
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
