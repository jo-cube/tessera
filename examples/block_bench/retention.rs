//! Milestone 5F: what copy-on-write snapshots keep under sustained writes,
//! how admission behaves when they keep too much, what holding them costs the
//! persistent tree, and why frequent snapshots raise background rewrite.

use std::hint::black_box;
use std::sync::atomic::Ordering;
use std::time::Instant;

use tessera::engine::{
    CompactionError, CompactionOptions, Engine, EngineOptions, EngineStats, EngineWorkspace,
    Snapshot, SnapshotLimits, UpdateError,
};
use tessera::memtable::{FrozenMemtable, Memtable, TARGET_RUN_LEN, WriteError};

use super::{
    FlushTally, GROUP_LEN, LIVE, MEMTABLE_BUDGET, MEMTABLE_ROUNDS, Owned, Rng, TENANT_LEN,
    allocations_during, compact_done, drain, insert_all, mib, percentile, put_flushing, shuffled,
    snapshot_base, snapshot_options, timed, tree_json, tree_small, vm_rss_bytes,
};

const BUDGET: usize = 4 << 20;
const PUTS: usize = 300_000;
const SAMPLE_EVERY: usize = 2_000;
const HELD: usize = 8;

/// Heap bytes freed by `operation`.
fn freed_by(operation: impl FnOnce()) -> usize {
    let before = LIVE.load(Ordering::Relaxed);
    operation();
    before.saturating_sub(LIVE.load(Ordering::Relaxed)) as usize
}

/// The puts of one pass: uniform over every key, or 90% to a hot tenth.
struct Workload {
    name: &'static str,
    records: Vec<Owned>,
    order: Vec<usize>,
    puts: Vec<usize>,
}

fn workload(name: &'static str, records: Vec<Owned>, hot: bool, seed: u64) -> Workload {
    let count = records.len();
    let order = shuffled(count, seed);
    let hot_keys = &order[..count / 10];
    let mut rng = Rng(seed + 1);
    let puts = (0..PUTS)
        .map(|_| {
            if hot && !rng.next().is_multiple_of(10) {
                hot_keys[(rng.next() % hot_keys.len() as u64) as usize]
            } else {
                (rng.next() % count as u64) as usize
            }
        })
        .collect();
    Workload {
        name,
        records,
        order,
        puts,
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Pattern {
    None,
    /// One every 100 puts, scanning a 200-key group, dropped at once.
    Frequent,
    /// One every 1,000 puts, up to eight alive, a random one dropped when a
    /// ninth is taken, so releases are out of order.
    Several,
    /// `Several`, and each new snapshot also scans a 4,000-key tenant
    /// through a random older one.
    Readers,
    /// One taken before the first put and kept to the end of the writes.
    LongLived,
}

impl Pattern {
    fn name(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::Frequent => "frequent, every 100",
            Self::Several => "8 held, every 1,000",
            Self::Readers => "8 held + tenant scans",
            Self::LongLived => "one for the whole pass",
        }
    }
}

/// Peaks of what the engine and its snapshots hold, sampled during writes.
#[derive(Default)]
struct Peaks {
    engine: usize,
    pinned: usize,
    tables: usize,
    runs: usize,
    retained: usize,
    heap: usize,
    rss: u64,
}

impl Peaks {
    fn sample(&mut self, stats: &EngineStats, heap_base: u64, rss_base: Option<u64>) {
        let (frozen_charged, frozen_pinned) = stats
            .frozen
            .map_or((0, 0), |frozen| (frozen.charged, frozen.pinned));
        let pinned = stats.active.pinned + frozen_pinned;
        let engine = stats.active.charged - stats.active.pinned + frozen_charged - frozen_pinned
            + stats.run_bytes;
        self.engine = self.engine.max(engine);
        self.pinned = self.pinned.max(pinned);
        self.runs = self.runs.max(stats.retained_run_bytes);
        self.tables = self
            .tables
            .max(stats.retained_bytes - pinned - stats.retained_run_bytes);
        self.retained = self.retained.max(stats.retained_bytes);
        let heap = LIVE.load(Ordering::Relaxed).saturating_sub(heap_base);
        self.heap = self.heap.max(heap as usize);
        if let (Some(now), Some(base)) = (vm_rss_bytes(), rss_base) {
            self.rss = self.rss.max(now.saturating_sub(base));
        }
    }
}

fn sustained(workload: &Workload, pattern: Pattern) {
    let records = &workload.records;
    let mut options = snapshot_options(BUDGET, true);
    options.snapshots.max_live = 64;
    let engine = snapshot_base(records, &workload.order, options);
    let before = engine.background_status().unwrap();
    let mut tally = FlushTally::default();
    let mut workspace = EngineWorkspace::new();
    let mut rng = Rng(77);
    let mut held: Vec<Snapshot> = Vec::with_capacity(HELD + 1);
    let mut peaks = Peaks::default();
    let (mut taken, mut refused) = (0, 0);
    let mut latency = Vec::with_capacity(PUTS / 16 + 1);
    let heap_base = LIVE.load(Ordering::Relaxed);
    let rss_base = vm_rss_bytes();
    if pattern == Pattern::LongLived {
        held.push(engine.snapshot().unwrap());
        taken += 1;
    }
    let start = Instant::now();
    for (at, &index) in workload.puts.iter().enumerate() {
        let record = &records[index];
        let value = records[(rng.next() % records.len() as u64) as usize]
            .value
            .as_deref()
            .unwrap();
        let put = Instant::now();
        put_flushing(&engine, &record.key, value, &mut tally);
        if at % 16 == 0 {
            latency.push(put.elapsed().as_nanos() as u64);
        }
        let every = match pattern {
            Pattern::Frequent => 100,
            Pattern::Several | Pattern::Readers => 1_000,
            Pattern::None | Pattern::LongLived => usize::MAX,
        };
        if at % every == every - 1 {
            match engine.snapshot() {
                Ok(snapshot) => {
                    taken += 1;
                    if pattern == Pattern::Frequent {
                        black_box(drain(
                            snapshot.scan_prefix(&record.key[..GROUP_LEN], &mut workspace),
                        ));
                    } else {
                        if pattern == Pattern::Readers && !held.is_empty() {
                            let old = &held[(rng.next() % held.len() as u64) as usize];
                            black_box(drain(
                                old.scan_prefix(&record.key[..TENANT_LEN], &mut workspace),
                            ));
                            black_box(drain(
                                snapshot.scan_prefix(&record.key[..GROUP_LEN], &mut workspace),
                            ));
                        }
                        black_box(snapshot.get(&record.key, &mut workspace).unwrap());
                        held.push(snapshot);
                        if held.len() > HELD {
                            let at = (rng.next() % held.len() as u64) as usize;
                            drop(held.swap_remove(at));
                        }
                    }
                }
                Err(_) => refused += 1,
            }
        }
        if at % SAMPLE_EVERY == 0 {
            peaks.sample(&engine.stats(), heap_base, rss_base);
        }
    }
    let write_s = start.elapsed().as_secs_f64();
    let written = engine.stats();
    peaks.sample(&written, heap_base, rss_base);
    let count = held.len();
    let dropped = Instant::now();
    let freed = freed_by(|| held.clear());
    let drop_us = dropped.elapsed().as_nanos() as f64 / 1e3;
    tally.flush(&engine);
    engine.wait_for_compaction().unwrap();
    let (sync_rewrite, _) = compact_done(&engine);
    let status = engine.background_status().unwrap();
    let rewritten = (status.input_bytes - before.input_bytes) as usize + sync_rewrite;
    let heap_after = LIVE.load(Ordering::Relaxed).saturating_sub(heap_base);
    latency.sort_unstable();
    println!(
        "| {} | {} | {:.2} | {} | {taken} | {refused} | {} | {} | {:.2} | {:.1} | {:.2} | {:.2} | {:.2} | {:.1} | {:.1} | {:.0} | {:.1} | {count} | {:.2} | {:.2} | {:.2} | {drop_us:.0} | {:.1} |",
        workload.name,
        pattern.name(),
        PUTS as f64 / write_s / 1e6,
        percentile(&latency, 0.99),
        tally.flushes,
        tally.stalls,
        rewritten as f64 / tally.bytes.max(1) as f64,
        mib(peaks.engine),
        mib(peaks.pinned),
        mib(peaks.tables),
        mib(peaks.runs),
        mib(peaks.retained),
        mib(peaks.heap),
        rss_base.unwrap_or(0) as f64 / (1 << 20) as f64,
        peaks.rss as f64 / (1 << 20) as f64,
        mib(written.retained_bytes),
        mib(freed),
        if written.retained_bytes == 0 {
            0.0
        } else {
            freed as f64 / written.retained_bytes as f64
        },
        heap_after as f64 / (1 << 20) as f64,
    );
}

/// A long-lived snapshot under a small retained limit and no worker: when
/// each refusal begins, what is held then, and recovery once it is dropped.
fn backpressure(workload: &Workload) {
    const LIMIT: usize = 2 << 20;
    const MAX_RUNS: usize = 16;
    let records = &workload.records;
    let options = EngineOptions {
        memtable_budget: 1 << 20,
        max_runs: 4096,
        max_run_bytes: usize::MAX,
        snapshots: SnapshotLimits {
            max_live: 16,
            max_retained_bytes: LIMIT,
        },
        ..EngineOptions::default()
    };
    let engine = snapshot_base(records, &workload.order, options);
    engine.set_run_limits(MAX_RUNS, usize::MAX);
    let heap_base = LIVE.load(Ordering::Relaxed);
    let event = |name: &str, at: usize, engine: &Engine| {
        let stats = engine.stats();
        println!(
            "| {name} | {at} | {:.1} | {:.1} | {:.1} | {} | {} | {:.1} |",
            mib(stats.retained_bytes),
            mib(stats.retained_run_bytes),
            mib(stats.active.charged + stats.frozen.map_or(0, |frozen| frozen.charged)),
            stats.runs,
            stats.frozen_tables,
            LIVE.load(Ordering::Relaxed).saturating_sub(heap_base) as f64 / (1 << 20) as f64,
        );
    };
    let mut long = Some(engine.snapshot().unwrap());
    let mut workspace = tessera::run::MergeWorkspace::new();
    let (mut snapshot_refused, mut compaction_refused, mut flush_refused) = (false, false, false);
    let mut rng = Rng(5);
    let mut at = 0;
    let mut after_drop = None;
    while at < PUTS {
        let record = &records[workload.puts[at]];
        let value = records[(rng.next() % records.len() as u64) as usize]
            .value
            .as_deref()
            .unwrap();
        match engine.put(&record.key, value) {
            Ok(_) => at += 1,
            Err(UpdateError::Rejected(WriteError::BudgetExceeded { .. })) => {
                if engine.flush().is_ok() {
                    loop {
                        match engine.compact(CompactionOptions::default(), &mut workspace) {
                            Ok(Some(_)) => {}
                            Ok(None) => break,
                            Err(CompactionError::Retained { .. }) => {
                                if !compaction_refused {
                                    compaction_refused = true;
                                    event("first compaction refused", at, &engine);
                                }
                                break;
                            }
                            Err(error) => panic!("{error}"),
                        }
                    }
                    continue;
                }
                if !flush_refused {
                    flush_refused = true;
                    event("first flush refused", at, &engine);
                }
                if engine.put(&record.key, value).is_ok() {
                    at += 1;
                } else if long.is_some() {
                    event("writes refused", at, &engine);
                    let snapshot = long.take().unwrap();
                    let start = Instant::now();
                    let freed = freed_by(|| drop(snapshot));
                    println!(
                        "| snapshot dropped: {:.1} MiB freed in {:.0} us | {at} | | | | | | |",
                        mib(freed),
                        start.elapsed().as_nanos() as f64 / 1e3
                    );
                    compact_done(&engine);
                    after_drop = Some(at);
                } else {
                    panic!("writes were refused with no snapshot held");
                }
            }
            Err(error) => panic!("{error}"),
        }
        if at % 1_000 == 0 && !snapshot_refused {
            match engine.snapshot() {
                Ok(snapshot) => drop(snapshot),
                Err(_) => {
                    snapshot_refused = true;
                    event("first snapshot refused", at, &engine);
                }
            }
        }
        if after_drop.is_some_and(|dropped| at == dropped + 50_000) {
            event("50,000 puts after the drop", at, &engine);
        }
    }
    event("end", at, &engine);
}

const WORKLOADS: usize = 4;
const PATTERNS: [Pattern; 5] = [
    Pattern::None,
    Pattern::Frequent,
    Pattern::Several,
    Pattern::Readers,
    Pattern::LongLived,
];

fn make_workload(index: usize) -> Workload {
    match index {
        0 => workload("json, 174k keys, uniform", tree_json(16 << 20), false, 900),
        1 => workload(
            "json, 174k keys, 90% to 10%",
            tree_json(16 << 20),
            true,
            901,
        ),
        2 => workload("json, 17k keys, uniform", tree_json(16 << 17), false, 902),
        _ => workload(
            "8-byte, 214k keys, uniform",
            tree_small(4 << 20),
            false,
            903,
        ),
    }
}

/// One sustained row in this process, which should be fresh so that its
/// RSS is its own.
pub(super) fn retention_row(workload: &str, pattern: &str) {
    let workload = make_workload(workload.parse().unwrap());
    sustained(&workload, PATTERNS[pattern.parse::<usize>().unwrap()]);
}

pub(super) fn retention_bench() {
    println!(
        "sustained: each row starts from every key flushed and compacted into {} MiB memtables, then {PUTS} puts with values drawn from the dataset, with the background worker and default options; peaks are sampled every {SAMPLE_EVERY} puts",
        BUDGET >> 20
    );
    println!(
        "engine is what the live engine reaches: memtable charges without pinned history, plus published run bytes; pinned is memtable history only snapshots need; tables and runs are released sources snapshots hold; heap and RSS are growth over the pass's start, each row in a fresh process whose RSS at the start includes the dataset and building the base state"
    );
    println!(
        "at the end of the writes the held snapshots are dropped: freed is the heap that released; heap after is the growth left once flushing and compaction finish"
    );
    println!();
    println!(
        "| workload | snapshots | puts M/s | put p99 ns | taken | refused | flushes | stalls | rewrite / flushed | peak engine MiB | peak pinned MiB | peak tables MiB | peak runs MiB | peak retained MiB | peak heap MiB | RSS at start MiB | peak RSS MiB | retained at end MiB | held at end | freed MiB | freed / retained | drop us | heap after MiB |"
    );
    println!("|{}", "---|".repeat(23));
    let program = std::env::current_exe().unwrap();
    for workload in 0..WORKLOADS {
        for pattern in 0..PATTERNS.len() {
            let status = std::process::Command::new(&program)
                .args(["retention-row", &workload.to_string(), &pattern.to_string()])
                .status()
                .unwrap();
            assert!(status.success());
        }
    }
    println!();
    println!(
        "backpressure: json uniform, 1 MiB memtables, no worker, compaction after each flush, max_runs 16, max_retained_bytes 2 MiB, one snapshot held from the start; a snapshot is tried every 1,000 puts"
    );
    println!(
        "| event | puts | retained MiB | retained runs MiB | memtables MiB | runs | frozen | heap MiB |"
    );
    println!("|---|---|---|---|---|---|---|---|");
    backpressure(&make_workload(0));
}

/// Snapshots a memtable operation runs beside: none, one taken before it
/// and kept, or a new one every 1,000 operations replacing the last.
#[derive(Clone, Copy)]
enum Held {
    None,
    One,
    Rolling,
}

struct Batch {
    table: Memtable,
    seq: u64,
    held: Vec<FrozenMemtable>,
}

impl Batch {
    fn new(table: Memtable, seq: u64, held: Held) -> Self {
        let mut batch = Self {
            table,
            seq,
            held: Vec::new(),
        };
        if !matches!(held, Held::None) {
            batch.held.push(batch.table.snapshot());
        }
        batch
    }

    fn step(&mut self, held: Held, done: usize) {
        if matches!(held, Held::Rolling) && done % 1_000 == 999 {
            self.held.clear();
            self.held.push(self.table.snapshot());
        }
    }
}

fn fill(records: &[Owned], order: &[usize]) -> (Memtable, u64) {
    let mut table = Memtable::new(MEMTABLE_BUDGET, None);
    let mut seq = 0;
    insert_all(records, order, &mut table, &mut seq);
    (table, seq)
}

fn writes(records: &[Owned], keys: &[usize], batch: &mut Batch, held: Held, delete: bool) {
    for (done, &index) in keys.iter().enumerate() {
        let record = &records[index];
        batch.seq += 1;
        if delete {
            batch.table.delete(&record.key, batch.seq).unwrap();
        } else {
            batch
                .table
                .put(&record.key, batch.seq, record.value.as_deref().unwrap())
                .unwrap();
        }
        batch.step(held, done);
    }
}

fn prefix_scan(table: &Memtable, prefixes: &[&[u8]]) -> usize {
    let mut records = 0;
    for prefix in prefixes {
        for record in table.iter_from(prefix) {
            if !record.key.starts_with(prefix) {
                break;
            }
            black_box(record);
            records += 1;
        }
    }
    records
}

fn persistent_dataset(name: &str, records: &[Owned]) {
    let count = records.len();
    let sorted: Vec<usize> = (0..count).collect();
    let random = shuffled(count, 91);
    let second = shuffled(count, 92);
    let (even, odd): (Vec<usize>, Vec<usize>) = random.iter().partition(|&&i| i % 2 == 0);
    let held_kinds = [
        (Held::None, "none"),
        (Held::One, "one held"),
        (Held::Rolling, "rolling, every 1,000"),
    ];
    for (held, label) in held_kinds {
        let row = |workload: &str, operations: usize, (ns, allocs): (f64, f64), pin: usize| {
            println!(
                "| {name} | {label} | {workload} | {ns:.1} | {allocs:.2} | {:.0} |",
                pin as f64 / operations as f64
            );
        };
        let pin_after = |run: &mut dyn FnMut(&mut Batch), setup: &dyn Fn() -> Batch| {
            let mut batch = setup();
            run(&mut batch);
            batch.table.memory().pinned
        };

        let setup = || Batch::new(Memtable::new(MEMTABLE_BUDGET, None), 0, held);
        let mut run = |batch: &mut Batch| writes(records, &sorted, batch, held, false);
        let pin = pin_after(&mut run, &setup);
        row("sequential insert", count, timed(count, setup, run), pin);

        let setup = || {
            let (table, seq) = fill(records, &even);
            Batch::new(table, seq, held)
        };
        let mut run = |batch: &mut Batch| writes(records, &odd, batch, held, false);
        let pin = pin_after(&mut run, &setup);
        row(
            "random insert, half full",
            odd.len(),
            timed(odd.len(), setup, run),
            pin,
        );

        let setup = || {
            let (table, seq) = fill(records, &random);
            Batch::new(table, seq, held)
        };
        let mut run = |batch: &mut Batch| writes(records, &second, batch, held, false);
        let pin = pin_after(&mut run, &setup);
        row("random overwrite", count, timed(count, setup, run), pin);

        let mut run = |batch: &mut Batch| writes(records, &second, batch, held, true);
        let pin = pin_after(&mut run, &setup);
        row("random delete", count, timed(count, setup, run), pin);
    }

    // Reads and flushes of a table written while snapshots were taken, all
    // dropped since, against one filled with none.
    let groups: Vec<&[u8]> = random
        .iter()
        .step_by(97)
        .take(400)
        .map(|&i| &records[i].key[..GROUP_LEN])
        .collect();
    let tenants: Vec<&[u8]> = random
        .iter()
        .step_by(4001)
        .take(40)
        .map(|&i| &records[i].key[..TENANT_LEN])
        .collect();
    for (held, label) in [
        (Held::None, "none"),
        (Held::Rolling, "rolling, every 1,000"),
    ] {
        let (table, seq) = fill(records, &random);
        let mut batch = Batch::new(table, seq, held);
        writes(records, &second, &mut batch, held, false);
        batch.held.clear();
        let table = batch.table;
        let probes = &second[..count.min(200_000)];
        let time = |operations: usize, mut read: Box<dyn FnMut() -> usize + '_>| {
            let mut samples: Vec<f64> = (0..MEMTABLE_ROUNDS)
                .map(|_| {
                    let start = Instant::now();
                    black_box(read());
                    start.elapsed().as_nanos() as f64 / operations as f64
                })
                .collect();
            samples.sort_by(f64::total_cmp);
            (
                samples[samples.len() / 2],
                allocations_during(|| {
                    black_box(read());
                }) as f64
                    / operations as f64,
            )
        };
        let get = time(
            probes.len(),
            Box::new(|| {
                probes
                    .iter()
                    .filter(|&&i| table.get(&records[i].key).is_some())
                    .count()
            }),
        );
        println!(
            "| {name} | after overwrites, {label} | get hit | {:.1} | {:.2} | |",
            get.0, get.1
        );
        let full = time(count, Box::new(|| table.iter().count()));
        println!(
            "| {name} | after overwrites, {label} | ordered iteration, per record | {:.1} | {:.2} | |",
            full.0, full.1
        );
        let short_len = prefix_scan(&table, &groups);
        let short = time(groups.len(), Box::new(|| prefix_scan(&table, &groups)));
        println!(
            "| {name} | after overwrites, {label} | group scan, {} records | {:.0} | {:.2} | |",
            short_len / groups.len(),
            short.0,
            short.1
        );
        let long_len = prefix_scan(&table, &tenants);
        let long = time(tenants.len(), Box::new(|| prefix_scan(&table, &tenants)));
        println!(
            "| {name} | after overwrites, {label} | tenant scan, {} records | {:.0} | {:.2} | |",
            long_len / tenants.len(),
            long.0,
            long.1
        );
        let frozen = table.freeze();
        let start = Instant::now();
        let runs: Vec<_> = frozen.flush(TARGET_RUN_LEN).collect();
        let ns = start.elapsed().as_nanos() as f64;
        black_box(runs);
        println!(
            "| {name} | after overwrites, {label} | flush, per record | {:.1} | | |",
            ns / count as f64
        );
    }
}

pub(super) fn persistent_bench() {
    println!(
        "persistent tree under snapshots: median of {MEMTABLE_ROUNDS} rounds, allocations from one more; pinned per op is the table's pinned bytes after the batch, divided by its operations (a rolling snapshot's pins are only the last 1,000 operations')"
    );
    println!("| dataset | snapshots | workload | ns/op | allocs/op | pinned B/op |");
    println!("|---|---|---|---|---|---|");
    persistent_dataset("json", &tree_json(16 << 20));
    persistent_dataset("8-byte", &tree_small(8 << 20));
}

/// What the background worker does when a pass of frequent snapshots runs
/// as before, with no reads, with the reads' time spent spinning instead,
/// and with nothing.
pub(super) fn pacing_bench() {
    const ROUNDS: usize = 3;
    let records = tree_json(16 << 20);
    let count = records.len();
    let order = shuffled(count, 700);
    let rewrite = shuffled(count, 701);
    println!(
        "pacing: json, 1 MiB memtables, the background worker with default options; one overwrite pass of {count} puts; an event every 100 puts; {ROUNDS} rounds"
    );
    println!(
        "reads: a snapshot scans a 200-key group, every tenth a 4,000-key tenant, and gets a key; acquire only: a snapshot is taken and dropped; spin: no snapshot, the writer spins for the reads' mean time; sleep: it sleeps for that time instead, leaving its CPU idle (sleeps overshoot); none: nothing"
    );
    println!(
        "| event | round | write s | flushes | passes | compactions | rewrite / flushed | mean input KiB | runs after writes |"
    );
    println!("|---|---|---|---|---|---|---|---|---|");
    for round in 0..ROUNDS {
        let mut spin_ns = 0;
        for event in ["reads", "acquire only", "spin", "sleep", "none"] {
            let engine = snapshot_base(&records, &order, snapshot_options(1 << 20, true));
            let before = engine.background_status().unwrap();
            let mut tally = FlushTally::default();
            let mut workspace = EngineWorkspace::new();
            let (mut read_ns, mut events) = (0u128, 0);
            let start = Instant::now();
            for (at, &index) in rewrite.iter().enumerate() {
                let record = &records[index];
                put_flushing(
                    &engine,
                    &record.key,
                    record.value.as_deref().unwrap(),
                    &mut tally,
                );
                if at % 100 != 99 {
                    continue;
                }
                events += 1;
                match event {
                    "reads" => {
                        let begun = Instant::now();
                        let snapshot = engine.snapshot().unwrap();
                        black_box(drain(
                            snapshot.scan_prefix(&record.key[..GROUP_LEN], &mut workspace),
                        ));
                        if events % 10 == 1 {
                            black_box(drain(
                                snapshot.scan_prefix(&record.key[..TENANT_LEN], &mut workspace),
                            ));
                        }
                        black_box(snapshot.get(&record.key, &mut workspace).unwrap());
                        drop(snapshot);
                        read_ns += begun.elapsed().as_nanos();
                    }
                    "acquire only" => drop(engine.snapshot().unwrap()),
                    "spin" => {
                        let begun = Instant::now();
                        while begun.elapsed().as_nanos() < spin_ns {
                            std::hint::spin_loop();
                        }
                    }
                    "sleep" => std::thread::sleep(std::time::Duration::from_nanos(
                        u64::try_from(spin_ns).unwrap(),
                    )),
                    _ => {}
                }
            }
            let write_s = start.elapsed().as_secs_f64();
            let written = engine.stats();
            tally.flush(&engine);
            engine.wait_for_compaction().unwrap();
            let (sync_rewrite, _) = compact_done(&engine);
            let status = engine.background_status().unwrap();
            let compactions = status.compactions - before.compactions;
            let input = (status.input_bytes - before.input_bytes) as usize;
            if event == "reads" {
                spin_ns = read_ns / events.max(1) as u128;
            }
            println!(
                "| {event} | {round} | {write_s:.2} | {} | {} | {compactions} | {:.2} | {:.0} | {} |",
                tally.flushes,
                status.passes - before.passes,
                (input + sync_rewrite) as f64 / tally.bytes.max(1) as f64,
                input as f64 / compactions.max(1) as f64 / 1024.0,
                written.runs,
            );
        }
    }
}
