//! Milestone 5H, investigation only: read amplification under sustained
//! ingestion with the real engine and background worker, what point lookups
//! and prefix scans do in each run, and what a bounded consolidation costs.
//!
//! Every phase has a fixed amount of work. The same file runs against the
//! previous compaction policy from a temporary copy of that commit.

use std::hint::black_box;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use tessera::engine::{CompactionOptions, Engine, EngineWorkspace};
use tessera::run::{MergeWorkspace, ReadWorkspace, Run};

use super::{
    FLAT_TREE, FlushTally, GROUP_LEN, LIVE, Owned, PEAK, Rng, SUB_LEN, TENANT_LEN, mib, percentile,
    put_flushing, shuffled, snapshot_base, snapshot_options, task_cpu_ns, tree_json, tree_key,
};

const BUDGET: usize = 1 << 20;
const PUTS: usize = 1_500_000;
const PACED_PUTS: usize = 600_000;
const PACED_RATE: u64 = 200_000;
const SAMPLE_EVERY: usize = 100_000;
const ROUNDS: usize = 2;
const PROBES: usize = 2_000;

#[derive(Clone, Copy)]
enum Workload {
    Uniform,
    Window,
    Inserts,
}

impl Workload {
    fn name(self) -> &'static str {
        match self {
            Self::Uniform => "uniform",
            Self::Window => "window",
            Self::Inserts => "new keys",
        }
    }
}

/// The key and value of put `at` of `total`.
fn put_of<'a>(
    workload: Workload,
    base: &'a [Owned],
    fresh: &'a [Vec<u8>],
    order: &[usize],
    rng: &mut Rng,
    at: usize,
    total: usize,
) -> (&'a [u8], &'a [u8]) {
    let count = base.len();
    let value = base[(rng.next() % count as u64) as usize]
        .value
        .as_deref()
        .unwrap();
    let key: &[u8] = match workload {
        Workload::Uniform => &base[(rng.next() % count as u64) as usize].key,
        Workload::Window => {
            let start = at * count / total;
            &base[(start + (rng.next() % 8_000) as usize) % count].key
        }
        Workload::Inserts => &fresh[order[at]],
    };
    (key, value)
}

/// Keys present after `puts` puts of `workload`.
fn present<'a>(
    workload: Workload,
    base: &'a [Owned],
    fresh: &'a [Vec<u8>],
    order: &[usize],
    puts: usize,
) -> Vec<&'a [u8]> {
    let mut keys: Vec<&[u8]> = base.iter().map(|record| &record.key[..]).collect();
    if let Workload::Inserts = workload {
        keys.extend(order[..puts].iter().map(|&index| &fresh[index][..]));
    }
    keys
}

/// CPU time of every compaction worker: several engines are alive at once,
/// and all but the one measured are idle.
fn workers_cpu_ns() -> u64 {
    let Ok(tasks) = std::fs::read_dir("/proc/self/task") else {
        return 0;
    };
    tasks
        .flatten()
        .filter(|task| {
            std::fs::read_to_string(task.path().join("comm"))
                .is_ok_and(|comm| comm.starts_with("tessera-compact"))
        })
        .map(|task| task_cpu_ns(&task.file_name().to_string_lossy()))
        .sum()
}

fn covers(run: &Run<Vec<u8>>, key: &[u8]) -> bool {
    run.key_range()
        .unwrap()
        .is_some_and(|range| range.min <= key && key <= range.max)
}

fn size_classes(sizes: &[usize]) -> String {
    let bounds = [256 << 10, 1 << 20, 4 << 20, 16 << 20, usize::MAX];
    let mut counts = [0; 5];
    for &size in sizes {
        counts[bounds.iter().position(|&bound| size < bound).unwrap()] += 1;
    }
    counts.map(|count| count.to_string()).join("/")
}

/// Mean and largest number of runs whose range covers each probe, and for
/// the first 200 probes the mean number of runs that hold the key.
fn overlap_at(engine: &Engine, keys: &[&[u8]], rng: &mut Rng) -> (f64, usize, f64) {
    let guard = engine.read();
    let runs: Vec<&Run<Vec<u8>>> = guard.runs().collect();
    let mut read = ReadWorkspace::new();
    let (mut sum, mut max, mut holding) = (0, 0, 0);
    for probe in 0..PROBES {
        let key = keys[(rng.next() % keys.len() as u64) as usize];
        let depth = runs.iter().filter(|run| covers(run, key)).count();
        sum += depth;
        max = max.max(depth);
        if probe < 200 {
            holding += runs
                .iter()
                .filter(|run| run.get(key, &mut read).unwrap().is_some())
                .count();
        }
    }
    (sum as f64 / PROBES as f64, max, holding as f64 / 200.0)
}

/// Point-lookup p50 and p99 in µs, and mean group and tenant scan µs.
fn read_sample(engine: &Engine, keys: &[&[u8]], rng: &mut Rng) -> (f64, f64, f64, f64) {
    let mut workspace = EngineWorkspace::new();
    let mut gets: Vec<u64> = (0..500)
        .map(|_| {
            let key = keys[(rng.next() % keys.len() as u64) as usize];
            let begun = Instant::now();
            black_box(engine.get(key, &mut workspace).unwrap());
            begun.elapsed().as_nanos() as u64
        })
        .collect();
    gets.sort_unstable();
    let mut scan = |len: usize, scans: usize| {
        let begun = Instant::now();
        for _ in 0..scans {
            let key = keys[(rng.next() % keys.len() as u64) as usize];
            black_box(scan_count(engine, &key[..len], &mut workspace));
        }
        begun.elapsed().as_secs_f64() * 1e6 / scans as f64
    };
    let group = scan(GROUP_LEN, 50);
    let tenant = scan(TENANT_LEN, 5);
    (
        percentile(&gets, 0.5) as f64 / 1e3,
        percentile(&gets, 0.99) as f64 / 1e3,
        group,
        tenant,
    )
}

fn scan_count(engine: &Engine, prefix: &[u8], workspace: &mut EngineWorkspace) -> usize {
    let guard = engine.read();
    let mut scan = guard.scan_prefix(prefix, workspace);
    let mut records = 0;
    while let Some(record) = scan.next_record().unwrap() {
        black_box(record);
        records += 1;
    }
    records
}

struct Sustained {
    write_s: f64,
    total_s: f64,
    worker_cpu_s: f64,
    input: u64,
    output: u64,
    flushed: usize,
    stalls: usize,
    runs: usize,
    max_overlap: usize,
    peak: u64,
    heap_after: u64,
    sizes: String,
}

/// One sustained pass. Samples, when `print` is set, every SAMPLE_EVERY puts;
/// time spent sampling is excluded from the write time.
#[allow(clippy::too_many_arguments)]
fn sustained(
    engine: &Engine,
    workload: Workload,
    policy: &str,
    rate: Option<u64>,
    puts: usize,
    base: &[Owned],
    fresh: &[Vec<u8>],
    order: &[usize],
    print: bool,
) -> Sustained {
    let cpu_before = workers_cpu_ns();
    let status_before = engine.background_status().unwrap();
    let mut tally = FlushTally::default();
    let mut rng = Rng(31);
    let mut probe_rng = Rng(77);
    let heap_base = LIVE.load(Ordering::Relaxed);
    PEAK.store(heap_base, Ordering::Relaxed);
    let period = rate.map(|rate| Duration::from_nanos(1_000_000_000 / rate));
    let start = Instant::now();
    let mut paused = Duration::ZERO;
    let mut last = (Instant::now(), Duration::ZERO);
    let rate_name = rate.map_or("natural".to_owned(), |rate| format!("{}k/s", rate / 1_000));
    for at in 0..puts {
        if let Some(period) = period {
            let due = start + paused + period * at as u32;
            while Instant::now() < due {
                std::hint::spin_loop();
            }
        }
        let (key, value) = put_of(workload, base, fresh, order, &mut rng, at, puts);
        put_flushing(engine, key, value, &mut tally);
        if print && at % SAMPLE_EVERY == SAMPLE_EVERY - 1 {
            let begun = Instant::now();
            let interval = (begun - last.0 - (paused - last.1)).as_secs_f64();
            let stats = engine.stats();
            let status = engine.background_status().unwrap();
            let sizes: Vec<usize> = engine
                .read()
                .runs()
                .map(|run| run.as_bytes().len())
                .collect();
            let keys = present(workload, base, fresh, order, at + 1);
            let (mean, max, holding) = overlap_at(engine, &keys, &mut probe_rng);
            let (p50, p99, group, tenant) = read_sample(engine, &keys, &mut probe_rng);
            println!(
                "| {} | {policy} | {rate_name} | {} | {:.0} | {} | {} | {mean:.2}/{max} | {holding:.2} | {} | {:.2} | {:.2} | {} | {} | {p50:.1} | {p99:.1} | {group:.0} | {tenant:.0} |",
                workload.name(),
                (at + 1) / 1_000,
                SAMPLE_EVERY as f64 / interval / 1e3,
                stats.runs,
                size_classes(&sizes),
                stats.max_overlap,
                (status.input_bytes - status_before.input_bytes) as f64 / tally.bytes.max(1) as f64,
                (workers_cpu_ns() - cpu_before) as f64 / 1e9,
                if status.busy { "busy" } else { "idle" },
                tally.stalls,
            );
            paused += begun.elapsed();
            last = (Instant::now(), paused);
        }
    }
    let write_s = (start.elapsed() - paused).as_secs_f64();
    tally.flush(engine);
    engine.wait_for_compaction().unwrap();
    let total_s = (start.elapsed() - paused).as_secs_f64();
    let status = engine.background_status().unwrap();
    assert_eq!(status.failures, status_before.failures);
    let stats = engine.stats();
    let sizes: Vec<usize> = engine
        .read()
        .runs()
        .map(|run| run.as_bytes().len())
        .collect();
    Sustained {
        write_s,
        total_s,
        worker_cpu_s: (workers_cpu_ns() - cpu_before) as f64 / 1e9,
        input: status.input_bytes - status_before.input_bytes,
        output: status.output_bytes - status_before.output_bytes,
        flushed: tally.bytes,
        stalls: tally.stalls,
        runs: stats.runs,
        max_overlap: stats.max_overlap,
        peak: PEAK.load(Ordering::Relaxed).saturating_sub(heap_base),
        heap_after: LIVE.load(Ordering::Relaxed).saturating_sub(heap_base),
        sizes: size_classes(&sizes),
    }
}

/// What point lookups do in each run, for keys present and keys absent but
/// inside the runs' ranges (a present key with a zero byte appended).
fn point_anatomy(engine: &Engine, label: &str, keys: &[&[u8]], rng: &mut Rng) {
    let guard = engine.read();
    let runs: Vec<&Run<Vec<u8>>> = guard.runs().collect();
    let records: usize = runs.iter().map(|run| run.len()).sum();
    let mut read = ReadWorkspace::new();
    let mut workspace = EngineWorkspace::new();
    for positive in [true, false] {
        let (mut excluded, mut candidates, mut holding, mut blocks) = (0, 0, 0, 0);
        let (mut hit_ns, mut hits, mut miss_ns, mut misses) = (0u128, 0, 0u128, 0);
        let mut gets = Vec::with_capacity(PROBES);
        for _ in 0..PROBES {
            let mut key = keys[(rng.next() % keys.len() as u64) as usize].to_vec();
            if !positive {
                key.push(0);
            }
            for run in &runs {
                if !covers(run, &key) {
                    excluded += 1;
                    continue;
                }
                candidates += 1;
                let before = read.blocks_loaded();
                let begun = Instant::now();
                let found = black_box(run.get(&key, &mut read).unwrap()).is_some();
                let ns = begun.elapsed().as_nanos();
                blocks += read.blocks_loaded() - before;
                if found {
                    holding += 1;
                    hit_ns += ns;
                    hits += 1;
                } else {
                    miss_ns += ns;
                    misses += 1;
                }
            }
            let begun = Instant::now();
            black_box(guard.get(&key, &mut workspace).unwrap());
            gets.push(begun.elapsed().as_nanos() as u64);
        }
        gets.sort_unstable();
        let per = |count: usize| count as f64 / PROBES as f64;
        println!(
            "| {label} | {} | {} | {:.2} | {:.2} | {:.2} | {:.2} | {:.2} | {:.0} | {:.0} | {:.1} | {:.1} |",
            if positive {
                "present"
            } else {
                "absent, in range"
            },
            runs.len(),
            per(excluded),
            per(candidates),
            per(holding),
            per(candidates - holding),
            blocks as f64 / PROBES as f64,
            hit_ns as f64 / hits.max(1) as f64,
            miss_ns as f64 / misses.max(1) as f64,
            percentile(&gets, 0.5) as f64 / 1e3,
            percentile(&gets, 0.99) as f64 / 1e3,
        );
    }
    // Bloom filter memory at n keys: n * log2(1/p) / ln 2 bits.
    let bloom = |p: f64| records as f64 * (1.0 / p).log2() / std::f64::consts::LN_2 / 8.0;
    let run_bytes: usize = runs.iter().map(|run| run.as_bytes().len()).sum();
    println!(
        "<!-- bloom {label}: {records} records in {} runs of {:.1} MiB; 10% {:.2} MiB, 1% {:.2} MiB, 0.1% {:.2} MiB -->",
        runs.len(),
        mib(run_bytes),
        mib(bloom(0.1) as usize),
        mib(bloom(0.01) as usize),
        mib(bloom(0.001) as usize),
    );
}

/// What prefix scans of each length do: logical records, physical records in
/// the participating runs, blocks, and time to the first record and after.
fn prefix_anatomy(engine: &Engine, label: &str, keys: &[&[u8]], rng: &mut Rng) {
    let guard = engine.read();
    let runs: Vec<&Run<Vec<u8>>> = guard.runs().collect();
    let mut read = ReadWorkspace::new();
    let mut workspace = EngineWorkspace::new();
    for (len, scans) in [(4, 4), (TENANT_LEN, 20), (GROUP_LEN, 200), (SUB_LEN, 400)] {
        let (mut logical, mut physical, mut blocks, mut participating, mut intersecting) =
            (0, 0, 0, 0, 0);
        let (mut first_ns, mut rest_ns, mut alone_ns) = (0u128, 0u128, 0u128);
        for _ in 0..scans {
            let prefix = &keys[(rng.next() % keys.len() as u64) as usize][..len];
            let before = workspace.blocks_loaded();
            let begun = Instant::now();
            let mut scan = guard.scan_prefix(prefix, &mut workspace);
            let mut count = usize::from(black_box(scan.next_record().unwrap()).is_some());
            let first = begun.elapsed().as_nanos();
            while let Some(record) = scan.next_record().unwrap() {
                black_box(record);
                count += 1;
            }
            rest_ns += begun.elapsed().as_nanos() - first;
            first_ns += first;
            drop(scan);
            blocks += workspace.blocks_loaded() - before;
            logical += count;
            // Each run alone: the records a merge must step over.
            for run in &runs {
                let range = run.key_range().unwrap().unwrap();
                if range.max < prefix || (range.min > prefix && !range.min.starts_with(prefix)) {
                    continue;
                }
                intersecting += 1;
                let begun = Instant::now();
                let mut cursor = run.cursor(&mut read);
                cursor.seek_prefix(prefix).unwrap();
                let mut found = 0;
                while let Some(record) = cursor.next_record().unwrap() {
                    black_box(record);
                    found += 1;
                }
                alone_ns += begun.elapsed().as_nanos();
                physical += found;
                participating += usize::from(found > 0);
            }
        }
        let per = |value: f64| value / scans as f64;
        println!(
            "| {label} | {len} | {:.0} | {:.2} | {:.1} | {:.2} | {:.2} | {:.0} | {:.0} | {:.0} | {:.0} |",
            per(logical as f64),
            physical as f64 / logical.max(1) as f64,
            per(blocks as f64),
            per(intersecting as f64),
            per(participating as f64),
            per(first_ns as f64) / 1e3,
            per(rest_ns as f64) / 1e3,
            per((first_ns + rest_ns) as f64) / 1e3,
            per(alone_ns as f64) / 1e3,
        );
    }
}

/// Compacts on this thread until no key lies in more than `max_overlap` runs,
/// within the default input and workspace limits.
fn consolidate(engine: &Engine, label: &str, max_overlap: usize) {
    let options = CompactionOptions {
        max_overlap,
        ..CompactionOptions::default()
    };
    let mut workspace = MergeWorkspace::new();
    let (mut compactions, mut input, mut output) = (0, 0, 0);
    let mut failure = None;
    let before = engine.stats();
    let cpu = task_cpu_ns("thread-self");
    let begun = Instant::now();
    let peak = super::peak_during(|| {
        for _ in 0..10_000 {
            match engine.compact(options, &mut workspace) {
                Ok(Some(report)) => {
                    compactions += 1;
                    input += report.input_bytes;
                    output += report.output_bytes;
                }
                Ok(None) => return,
                Err(error) => {
                    failure = Some(error.to_string());
                    return;
                }
            }
        }
        panic!("consolidation did not finish");
    });
    let wall = begun.elapsed().as_secs_f64();
    let cpu = (task_cpu_ns("thread-self") - cpu) as f64 / 1e9;
    let after = engine.stats();
    println!(
        "| {label} | {max_overlap} | {}/{} → {}/{} | {compactions} | {:.1} | {:.1} | {:.2} | {cpu:.2} | {wall:.2} | {:.1} | {:.1} | {} |",
        before.runs,
        before.max_overlap,
        after.runs,
        after.max_overlap,
        mib(input),
        mib(output),
        input as f64 / before.run_bytes.max(1) as f64,
        mib(peak as usize),
        mib(after.retained_run_bytes),
        failure.as_deref().unwrap_or("none"),
    );
}

pub(super) fn reads_bench() {
    let policy = std::env::args()
        .nth(2)
        .unwrap_or_else(|| "size-aware".to_owned());
    let base = tree_json(16 << 20);
    let fresh: Vec<Vec<u8>> = (base.len() as u64..(base.len() + PUTS) as u64)
        .map(|i| tree_key(i, FLAT_TREE))
        .collect();
    let fresh_order = shuffled(fresh.len(), 77);
    let options = snapshot_options(BUDGET, true);
    let workloads = [Workload::Uniform, Workload::Window, Workload::Inserts];
    let rates = [(None, PUTS), (Some(PACED_RATE), PACED_PUTS)];
    println!(
        "reads ({policy}): {} json keys put in random order, flushed and compacted, then {PUTS} puts at the natural rate or {PACED_PUTS} at {}k/s, 1 MiB memtables, default limits and background compaction",
        base.len(),
        PACED_RATE / 1_000
    );
    println!();
    println!(
        "over time (round 1): runs by size class <256K/<1M/<4M/<16M/larger; overlap at {PROBES} present keys, mean/max; rewrite is worker input per flushed byte so far"
    );
    println!(
        "| workload | policy | rate | k puts | interval k puts/s | runs | size classes | key overlap mean/max | runs holding a present key | max overlap | rewrite | worker CPU s | worker | stalls | get p50 µs | get p99 µs | group scan µs | tenant scan µs |"
    );
    println!("|{}", "---|".repeat(18));
    let mut summaries = Vec::new();
    let mut settled = Vec::new();
    for round in 0..ROUNDS {
        for workload in workloads {
            for (rate, puts) in rates {
                let engine = snapshot_base(&base, &shuffled(base.len(), 900), options);
                let result = sustained(
                    &engine,
                    workload,
                    &policy,
                    rate,
                    puts,
                    &base,
                    &fresh,
                    &fresh_order,
                    round == 0,
                );
                summaries.push((workload, rate, result));
                if round == 0 && rate.is_none() {
                    settled.push((workload, engine));
                }
            }
        }
    }
    println!();
    println!("summary: both rounds, write and total exclude sampling; total includes catch-up");
    println!(
        "| workload | policy | rate | write s | k puts/s | total s | catch-up s | worker CPU s | input MiB | output MiB | flushed MiB | rewrite | stalls | runs | size classes | max overlap | peak heap MiB | heap after MiB |"
    );
    println!("|{}", "---|".repeat(18));
    for (workload, rate, result) in &summaries {
        let puts = if rate.is_some() { PACED_PUTS } else { PUTS };
        println!(
            "| {} | {policy} | {} | {:.2} | {:.0} | {:.2} | {:.2} | {:.2} | {:.1} | {:.1} | {:.1} | {:.2} | {} | {} | {} | {} | {:.1} | {:.1} |",
            workload.name(),
            rate.map_or("natural".to_owned(), |rate| format!("{}k/s", rate / 1_000)),
            result.write_s,
            puts as f64 / result.write_s / 1e3,
            result.total_s,
            result.total_s - result.write_s,
            result.worker_cpu_s,
            mib(result.input as usize),
            mib(result.output as usize),
            mib(result.flushed),
            result.input as f64 / result.flushed.max(1) as f64,
            result.stalls,
            result.runs,
            result.sizes,
            result.max_overlap,
            mib(result.peak as usize),
            mib(result.heap_after as usize),
        );
    }

    println!();
    println!(
        "point lookups on the settled natural-rate state, {PROBES} probes each; per probe; run gets timed one by one"
    );
    println!(
        "| state | probe | runs | excluded by range | candidates | holding the key | candidates without it | blocks | ns per run holding | ns per run without | get p50 µs | get p99 µs |"
    );
    println!("|{}", "---|".repeat(12));
    let mut point_rows = Vec::new();
    for (workload, engine) in &settled {
        let keys = present(*workload, &base, &fresh, &fresh_order, PUTS);
        let label = format!("{} {policy}", workload.name());
        point_anatomy(engine, &label, &keys, &mut Rng(5));
        point_rows.push(label);
    }
    println!();
    println!(
        "prefix scans on the same states; per scan; physical counts every record each participating run holds under the prefix; alone is each run scanned by itself, summed"
    );
    println!(
        "| state | prefix bytes | records | physical per record | blocks | runs intersecting | runs holding | to first µs | after first µs | total µs | runs alone µs |"
    );
    println!("|{}", "---|".repeat(11));
    for (workload, engine) in &settled {
        let keys = present(*workload, &base, &fresh, &fresh_order, PUTS);
        prefix_anatomy(
            engine,
            &format!("{} {policy}", workload.name()),
            &keys,
            &mut Rng(6),
        );
    }

    println!();
    println!(
        "bounded consolidation on the settled states: engine.compact with a lower max_overlap and the default input limits, a snapshot held through the overlap-1 step; then the same anatomy"
    );
    println!(
        "| state | max overlap | runs/overlap | compactions | input MiB | output MiB | input per stored byte | CPU s | wall s | peak heap MiB | retained by snapshot MiB | refused |"
    );
    println!("|{}", "---|".repeat(12));
    for (workload, engine) in &settled {
        let label = format!("{} {policy}", workload.name());
        consolidate(engine, &label, 2);
        let snapshot = engine.snapshot().unwrap();
        consolidate(engine, &label, 1);
        drop(snapshot);
    }
    println!();
    println!(
        "| state | probe | runs | excluded by range | candidates | holding the key | candidates without it | blocks | ns per run holding | ns per run without | get p50 µs | get p99 µs |"
    );
    println!("|{}", "---|".repeat(12));
    for (workload, engine) in &settled {
        let keys = present(*workload, &base, &fresh, &fresh_order, PUTS);
        point_anatomy(
            engine,
            &format!("{} {policy}, consolidated", workload.name()),
            &keys,
            &mut Rng(5),
        );
    }
    println!();
    println!(
        "| state | prefix bytes | records | physical per record | blocks | runs intersecting | runs holding | to first µs | after first µs | total µs | runs alone µs |"
    );
    println!("|{}", "---|".repeat(11));
    for (workload, engine) in &settled {
        let keys = present(*workload, &base, &fresh, &fresh_order, PUTS);
        prefix_anatomy(
            engine,
            &format!("{} {policy}, consolidated", workload.name()),
            &keys,
            &mut Rng(6),
        );
    }
}
