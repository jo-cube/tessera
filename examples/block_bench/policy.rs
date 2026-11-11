//! Milestone 5G: which runs compaction selects, what that costs, and how the
//! allowed overlap and the arrival rate change it.
//!
//! A bench thread runs the background worker's loop through the public API:
//! a flush requests a pass, requests made during a pass coalesce, and a pass
//! compacts until nothing is eligible. That lets every selection be recorded.

use std::collections::HashMap;
use std::hint::black_box;
use std::sync::atomic::Ordering;
use std::sync::{Condvar, Mutex};
use std::time::{Duration, Instant};

use tessera::engine::{CompactionOptions, Engine, EngineOptions, EngineWorkspace, UpdateError};
use tessera::memtable::WriteError;
use tessera::run::MergeWorkspace;

use super::{
    FLAT_TREE, GROUP_LEN, LIVE, Owned, PEAK, Rng, TENANT_LEN, drain, mib, percentile, shuffled,
    task_cpu_ns, tree_json, tree_key,
};

const BUDGET: usize = 1 << 20;
const PUTS: usize = 350_000;
const SAMPLE_EVERY: usize = 1_000;
const ROUNDS: usize = 2;

/// What is known about a published run.
#[derive(Clone, Copy)]
struct Info {
    bytes: usize,
    born: Instant,
    /// Times its bytes were rewritten, weighted by input bytes.
    rewrites: f64,
}

#[derive(Default)]
struct Pass {
    requested: u64,
    started: u64,
    finished: u64,
    stop: bool,
}

/// One compaction as the bench saw it.
struct Event {
    inputs: usize,
    input_bytes: usize,
    largest: usize,
    largest_age_ms: f64,
    output_bytes: usize,
    records_read: usize,
    cpu_ns: u64,
    wall_ns: u64,
}

struct Shared {
    pass: Mutex<Pass>,
    changed: Condvar,
    runs: Mutex<HashMap<usize, Info>>,
    events: Mutex<Vec<Event>>,
    cpu_ns: Mutex<u64>,
}

fn key_of(run: &tessera::run::Run<Vec<u8>>) -> usize {
    run.as_bytes().as_ptr() as usize
}

impl Shared {
    fn new() -> Self {
        Self {
            pass: Mutex::new(Pass::default()),
            changed: Condvar::new(),
            runs: Mutex::new(HashMap::new()),
            events: Mutex::new(Vec::new()),
            cpu_ns: Mutex::new(0),
        }
    }

    fn request(&self) -> u64 {
        let mut pass = self.pass.lock().unwrap();
        pass.requested += 1;
        self.changed.notify_all();
        pass.requested
    }

    /// Requests a pass and waits until one that started after it finished.
    fn wait(&self) {
        let target = self.request();
        let mut pass = self.pass.lock().unwrap();
        while pass.finished < target {
            pass = self.changed.wait(pass).unwrap();
        }
    }

    fn stop(&self) {
        self.pass.lock().unwrap().stop = true;
        self.changed.notify_all();
    }

    /// The worker's loop, recording each compaction.
    fn compactor(&self, engine: &Engine, options: CompactionOptions) {
        let mut workspace = MergeWorkspace::new();
        let cpu_start = task_cpu_ns("thread-self");
        loop {
            let generation = {
                let mut pass = self.pass.lock().unwrap();
                while !pass.stop && pass.requested == pass.started {
                    pass = self.changed.wait(pass).unwrap();
                }
                if pass.stop {
                    break;
                }
                pass.started = pass.requested;
                pass.started
            };
            loop {
                let start = Instant::now();
                let cpu = task_cpu_ns("thread-self");
                let Some(job) = engine.begin_compaction(options).unwrap() else {
                    break;
                };
                let inputs: Vec<(usize, usize)> = job
                    .inputs()
                    .map(|run| (key_of(run), run.as_bytes().len()))
                    .collect();
                let built = job.build(&mut workspace).unwrap();
                let outputs: Vec<(usize, usize)> = built
                    .runs()
                    .iter()
                    .map(|run| (key_of(run), run.as_bytes().len()))
                    .collect();
                let report = built.publish().unwrap();
                let wall_ns = start.elapsed().as_nanos() as u64;
                let cpu_ns = task_cpu_ns("thread-self") - cpu;
                let now = Instant::now();
                let mut runs = self.runs.lock().unwrap();
                let infos: Vec<Info> = inputs
                    .iter()
                    .map(|(key, bytes)| {
                        runs.remove(key).unwrap_or(Info {
                            bytes: *bytes,
                            born: now,
                            rewrites: 0.0,
                        })
                    })
                    .collect();
                let largest = infos.iter().max_by_key(|info| info.bytes).unwrap();
                let total: usize = infos.iter().map(|info| info.bytes).sum();
                let rewrites = infos
                    .iter()
                    .map(|info| info.bytes as f64 * (info.rewrites + 1.0))
                    .sum::<f64>()
                    / total.max(1) as f64;
                for (key, bytes) in outputs {
                    runs.insert(
                        key,
                        Info {
                            bytes,
                            born: now,
                            rewrites,
                        },
                    );
                }
                drop(runs);
                self.events.lock().unwrap().push(Event {
                    inputs: report.inputs,
                    input_bytes: report.input_bytes,
                    largest: largest.bytes,
                    largest_age_ms: now.duration_since(largest.born).as_secs_f64() * 1e3,
                    output_bytes: report.output_bytes,
                    records_read: report.stats.records_read,
                    cpu_ns,
                    wall_ns,
                });
            }
            let mut pass = self.pass.lock().unwrap();
            pass.finished = generation;
            self.changed.notify_all();
        }
        *self.cpu_ns.lock().unwrap() = task_cpu_ns("thread-self") - cpu_start;
    }

    /// Flushes everything frozen, registering the runs, and waits for a pass
    /// whenever the run limit refuses a flush.
    fn flush(&self, engine: &Engine, flushed: &mut (usize, usize)) {
        for _ in 0..10_000 {
            let Some(job) = engine.begin_flush() else {
                return;
            };
            let built = job.build();
            let outputs: Vec<(usize, usize)> = built
                .runs()
                .iter()
                .map(|run| (key_of(run), run.as_bytes().len()))
                .collect();
            match built.publish() {
                Ok(report) => {
                    flushed.0 += 1;
                    flushed.1 += report.run_bytes;
                    let now = Instant::now();
                    let mut runs = self.runs.lock().unwrap();
                    for (key, bytes) in outputs {
                        runs.insert(
                            key,
                            Info {
                                bytes,
                                born: now,
                                rewrites: 0.0,
                            },
                        );
                    }
                    drop(runs);
                    self.request();
                }
                Err(_) => self.wait(),
            }
        }
        panic!("flushing did not finish");
    }
}

#[derive(Clone, Copy)]
enum Workload {
    Uniform,
    Window,
    Inserts,
}

impl Workload {
    fn name(self) -> &'static str {
        match self {
            Self::Uniform => "uniform overwrites",
            Self::Window => "8,000-key sliding window",
            Self::Inserts => "new keys, random order",
        }
    }
}

/// The allowed overlap, as [`CompactionOptions::max_overlap`].
const OVERLAPS: [usize; 1] = [4];

/// Puts per second, or none for as fast as the writer goes.
const RATES: [Option<u64>; 3] = [None, Some(400_000), Some(100_000)];

fn compaction_options(max_overlap: usize) -> CompactionOptions {
    CompactionOptions {
        max_overlap,
        ..CompactionOptions::default()
    }
}

/// The key and value of put `at`.
fn put_of<'a>(
    workload: Workload,
    base: &'a [Owned],
    fresh: &'a [Vec<u8>],
    order: &[usize],
    rng: &mut Rng,
    at: usize,
) -> (&'a [u8], &'a [u8]) {
    let count = base.len();
    let value = base[(rng.next() % count as u64) as usize]
        .value
        .as_deref()
        .unwrap();
    let key: &[u8] = match workload {
        Workload::Uniform => &base[(rng.next() % count as u64) as usize].key,
        Workload::Window => {
            let start = at * count / PUTS;
            &base[(start + (rng.next() % 8_000) as usize) % count].key
        }
        Workload::Inserts => &fresh[order[at]],
    };
    (key, value)
}

fn row(
    workload: Workload,
    max_overlap: usize,
    rate: Option<u64>,
    base: &[Owned],
    fresh: &[Vec<u8>],
) {
    let count = base.len();
    let options = EngineOptions {
        memtable_budget: BUDGET,
        ..EngineOptions::default()
    };
    let engine = Engine::new(options);
    // The base state: every key put in random order, flushed and compacted.
    let shared = Shared::new();
    let compaction = compaction_options(max_overlap);
    let mut flushed = (0, 0);
    let fresh_order = shuffled(fresh.len(), 77);
    let mut rng = Rng(31);
    let mut latency = Vec::with_capacity(PUTS / 16);
    let mut gets = Vec::new();
    let (mut blocks, mut get_count) = (0u64, 0u64);
    let (mut runs_sum, mut runs_max, mut overlap_sum, mut overlap_max, mut samples) =
        (0usize, 0usize, 0usize, 0usize, 0usize);
    let mut workspace = EngineWorkspace::new();
    let mut written = None;
    let mut settled = None;
    let mut heap_base = 0;
    let mut peak = 0;
    let mut writer_cpu = 0.0;
    std::thread::scope(|scope| {
        scope.spawn(|| shared.compactor(&engine, compaction));
        for &index in &shuffled(count, 900) {
            let record = &base[index];
            put(
                &engine,
                &shared,
                &record.key,
                record.value.as_deref().unwrap(),
                &mut flushed,
            );
        }
        shared.flush(&engine, &mut flushed);
        shared.wait();
        flushed = (0, 0);
        shared.events.lock().unwrap().clear();
        heap_base = LIVE.load(Ordering::Relaxed);
        PEAK.store(heap_base, Ordering::Relaxed);
        let cpu_before = task_cpu_ns("thread-self");
        let period = rate.map(|rate| Duration::from_nanos(1_000_000_000 / rate));
        let start = Instant::now();
        for at in 0..PUTS {
            if let Some(period) = period {
                let due = start + period * at as u32;
                while Instant::now() < due {
                    std::hint::spin_loop();
                }
            }
            let (key, value) = put_of(workload, base, fresh, &fresh_order, &mut rng, at);
            let begun = Instant::now();
            put(&engine, &shared, key, value, &mut flushed);
            if at % 16 == 0 {
                latency.push(begun.elapsed().as_nanos() as u64);
            }
            if at % SAMPLE_EVERY == 0 {
                let stats = engine.stats();
                runs_sum += stats.runs;
                runs_max = runs_max.max(stats.runs);
                overlap_sum += stats.max_overlap;
                overlap_max = overlap_max.max(stats.max_overlap);
                samples += 1;
                let probe = &base[(rng.next() % count as u64) as usize].key;
                let before = workspace.blocks_loaded();
                let begun = Instant::now();
                black_box(engine.get(probe, &mut workspace).unwrap());
                gets.push(begun.elapsed().as_nanos() as u64);
                blocks += workspace.blocks_loaded() - before;
                get_count += 1;
            }
        }
        let write_s = start.elapsed().as_secs_f64();
        let stats = engine.stats();
        written = Some((write_s, stats.runs, stats.max_overlap));
        let events_at_write = shared.events.lock().unwrap().len();
        let input_at_write: usize = shared
            .events
            .lock()
            .unwrap()
            .iter()
            .map(|event| event.input_bytes)
            .sum();
        shared.flush(&engine, &mut flushed);
        shared.wait();
        shared.wait();
        let total_s = start.elapsed().as_secs_f64();
        writer_cpu = (task_cpu_ns("thread-self") - cpu_before) as f64 / 1e9;
        settled = Some((total_s, events_at_write, input_at_write));
        peak = PEAK.load(Ordering::Relaxed).saturating_sub(heap_base);
        shared.stop();
    });
    let (write_s, runs_written, overlap_written) = written.unwrap();
    let (total_s, events_at_write, input_at_write) = settled.unwrap();
    let heap_after = LIVE.load(Ordering::Relaxed).saturating_sub(heap_base);
    let events = shared.events.lock().unwrap();
    let input: usize = events.iter().map(|event| event.input_bytes).sum();
    let output: usize = events.iter().map(|event| event.output_bytes).sum();
    let records: usize = events.iter().map(|event| event.records_read).sum();
    let compactor_cpu = events.iter().map(|event| event.cpu_ns).sum::<u64>() as f64 / 1e9;
    let compactor_wall = events.iter().map(|event| event.wall_ns).sum::<u64>() as f64 / 1e9;
    // Compactions in which the largest input is at least four times the rest
    // together: rewriting a large run to absorb little.
    let lopsided: Vec<&Event> = events
        .iter()
        .filter(|event| event.largest >= 4 * (event.input_bytes - event.largest))
        .collect();
    let lopsided_bytes: usize = lopsided.iter().map(|event| event.input_bytes).sum();
    let largest_share =
        events.iter().map(|event| event.largest).sum::<usize>() as f64 / input.max(1) as f64;
    let mut ages: Vec<u64> = events
        .iter()
        .map(|event| event.largest_age_ms as u64)
        .collect();
    ages.sort_unstable();
    let mean_inputs =
        events.iter().map(|event| event.inputs).sum::<usize>() as f64 / events.len().max(1) as f64;
    let runs = shared.runs.lock().unwrap();
    let mut sizes: Vec<usize> = runs.values().map(|info| info.bytes).collect();
    sizes.sort_unstable();
    let total_bytes: usize = sizes.iter().sum();
    let rewrites = runs
        .values()
        .map(|info| info.bytes as f64 * info.rewrites)
        .sum::<f64>()
        / total_bytes.max(1) as f64;
    drop(runs);

    // Reads once settled.
    let stats = engine.stats();
    let mut settled_gets = Vec::new();
    let before = workspace.blocks_loaded();
    for _ in 0..4_000 {
        let probe = &base[(rng.next() % count as u64) as usize].key;
        let begun = Instant::now();
        black_box(engine.get(probe, &mut workspace).unwrap());
        settled_gets.push(begun.elapsed().as_nanos() as u64);
    }
    let settled_blocks = (workspace.blocks_loaded() - before) as f64 / 4_000.0;
    let scan = |len: usize, scans: usize, workspace: &mut EngineWorkspace, rng: &mut Rng| {
        let begun = Instant::now();
        for _ in 0..scans {
            let key = &base[(rng.next() % count as u64) as usize].key;
            let guard = engine.read();
            black_box(drain(guard.scan_prefix(&key[..len], workspace)));
        }
        begun.elapsed().as_nanos() as f64 / scans as f64 / 1e3
    };
    let group_us = scan(GROUP_LEN, 400, &mut workspace, &mut rng);
    let tenant_us = scan(TENANT_LEN, 40, &mut workspace, &mut rng);
    gets.sort_unstable();
    settled_gets.sort_unstable();
    latency.sort_unstable();
    println!(
        "| {} | {} | {} | {write_s:.2} | {} | {total_s:.2} | {writer_cpu:.2} | {compactor_cpu:.2} | {compactor_wall:.2} | {} | {:.1} | {} | {:.1} | {:.1} | {:.2} | {:.0} | {:.1} | {} | {:.1} | {:.2} | {mean_inputs:.1} | {:.0}% | {} of {} | {:.0}% | {} | {:.1} | {}/{} | {:.1}/{} | {}/{} | {} | {} | {:.1} | {} | {:.0} | {:.0} | {:.2} | {:.0} | {:.0} | {:.1} | {:.1} |",
        workload.name(),
        max_overlap,
        rate.map_or("natural".to_owned(), |rate| format!("{}k/s", rate / 1_000)),
        percentile(&latency, 0.99),
        flushed.0,
        mib(flushed.1),
        events.len(),
        mib(input),
        mib(output),
        input as f64 / flushed.1.max(1) as f64,
        records as f64 / 1e3,
        mib(input - input_at_write),
        events.len() - events_at_write,
        mib(input_at_write),
        input_at_write as f64 / flushed.1.max(1) as f64,
        largest_share * 100.0,
        lopsided.len(),
        events.len(),
        lopsided_bytes as f64 / input.max(1) as f64 * 100.0,
        percentile(&ages, 0.5),
        rewrites,
        runs_sum / samples.max(1),
        runs_max,
        overlap_sum as f64 / samples.max(1) as f64,
        overlap_max,
        runs_written,
        overlap_written,
        stats.runs,
        stats.max_overlap,
        blocks as f64 / get_count.max(1) as f64,
        percentile(&gets, 0.5),
        percentile(&gets, 0.99) as f64 / 1e3,
        percentile(&settled_gets, 0.5) as f64,
        settled_blocks,
        sizes.first().copied().unwrap_or(0) as f64 / 1024.0,
        sizes.last().copied().unwrap_or(0) as f64 / 1024.0,
        group_us,
        tenant_us,
    );
    let _ = (peak, heap_after);
    println!(
        "<!-- memory {} {} {} | peak heap MiB {:.1} | heap after MiB {:.1} -->",
        workload.name(),
        max_overlap,
        rate.map_or("natural".to_owned(), |rate| format!("{}k/s", rate / 1_000)),
        mib(peak as usize),
        mib(heap_after as usize),
    );
}

fn put(engine: &Engine, shared: &Shared, key: &[u8], value: &[u8], flushed: &mut (usize, usize)) {
    for _ in 0..4 {
        match engine.put(key, value) {
            Ok(_) => return,
            Err(UpdateError::Rejected(WriteError::BudgetExceeded { .. })) => {
                shared.flush(engine, flushed);
            }
            Err(error) => panic!("{error}"),
        }
    }
    panic!("a put failed after flushing");
}

pub(super) fn policy_bench() {
    let base = tree_json(16 << 20);
    let fresh: Vec<Vec<u8>> = (base.len() as u64..(base.len() + PUTS) as u64)
        .map(|i| tree_key(i, FLAT_TREE))
        .collect();
    println!(
        "policy: json values, {} keys in the base state (put in random order, flushed and compacted), then {PUTS} puts with {} MiB memtables and a bench thread running the worker's loop; {ROUNDS} rounds",
        base.len(),
        BUDGET >> 20
    );
    println!(
        "total s includes catch-up until nothing is eligible; catch-up is the compaction after the last put; lopsided compactions have a largest input at least 4 times the rest; rewrites is the bytes-weighted mean times the data in the final runs was rewritten"
    );
    println!(
        "during: sampled every {SAMPLE_EVERY} puts (runs and overlap avg/max, one get each); after: 4,000 gets, 400 group and 40 tenant scans once settled"
    );
    println!();
    println!(
        "| workload | max overlap | rate | write s | put p99 ns | total s | writer CPU s | compactor CPU s | compactor wall s | flushes | flushed MiB | compactions | input MiB | output MiB | rewrite / flushed | records read k | catch-up MiB | catch-up compactions | input during writes MiB | rewrite / flushed during writes | mean inputs | largest input share | lopsided | lopsided share of input | median age of largest ms | rewrites | runs avg/max | overlap avg/max | runs/overlap after writes | runs at end | overlap at end | blocks per get during | get p50 ns during | get p99 us during | get p50 ns after | blocks per get after | smallest run KiB | largest run KiB | group scan us | tenant scan us |"
    );
    println!("|{}", "---|".repeat(41));
    for _ in 0..ROUNDS {
        for workload in [Workload::Uniform, Workload::Window, Workload::Inserts] {
            for rate in RATES {
                for max_overlap in OVERLAPS {
                    row(workload, max_overlap, rate, &base, &fresh);
                }
            }
        }
    }
}
