//! Milestone 5M: the cost of durable writes against RAM-only ones, recovery
//! from the log and from a checkpoint, and checkpoints beside a durable
//! writer. The directory is the argument after `durable`; databases are
//! created under it and removed here.

use std::fs;
use std::hint::black_box;
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::{Duration, Instant};

use tessera::engine::{
    CheckpointOptions, CompactionOptions, DurableOptions, Engine, EngineOptions, EngineWorkspace,
};

use super::{FlushTally, LIVE, PEAK, Rng, mib, percentile, put_flushing, tree_json, tree_key};

const KEYS: u64 = 1_000_000;
/// Each write row stops at this many puts or after this long.
const PUTS: usize = 300_000;
const ROW_TIME: Duration = Duration::from_secs(20);

fn key(index: u64) -> Vec<u8> {
    tree_key(index, (1_000, 100, 1_000))
}

fn options() -> EngineOptions {
    EngineOptions {
        memtable_budget: 16 << 20,
        background_compaction: Some(CompactionOptions::default()),
        ..EngineOptions::default()
    }
}

fn durable(segment_bytes: u64) -> DurableOptions {
    DurableOptions {
        segment_bytes,
        max_log_bytes: u64::MAX,
    }
}

/// Heap peak above the level at entry while `op` runs.
fn peak<T>(op: impl FnOnce() -> T) -> (T, usize) {
    let start = LIVE.load(Ordering::Relaxed);
    PEAK.store(start, Ordering::Relaxed);
    let out = op();
    (
        out,
        PEAK.load(Ordering::Relaxed).saturating_sub(start) as usize,
    )
}

fn us(ns: u64) -> f64 {
    ns as f64 / 1e3
}

/// One writer of uniform overwrites, timing every put.
fn writes(name: &str, engine: &Engine, values: &[Vec<u8>]) -> usize {
    let mut rng = Rng(11);
    let mut tally = FlushTally::default();
    let mut latency = Vec::with_capacity(PUTS);
    let start = Instant::now();
    for at in 0..PUTS {
        let key = key(rng.next() % KEYS);
        let t = Instant::now();
        put_flushing(engine, &key, &values[at % values.len()], &mut tally);
        latency.push(t.elapsed().as_nanos() as u64);
        if at % 1024 == 0 && start.elapsed() > ROW_TIME {
            break;
        }
    }
    let wall = start.elapsed().as_secs_f64();
    let puts = latency.len();
    latency.sort_unstable();
    let (log, segments) = engine.log_status().map_or((0.0, 0), |status| {
        (mib(status.bytes as usize), status.segments)
    });
    println!(
        "| {name} | {puts} | {:.1} | {:.1} | {:.1} | {:.1} | {:.0} | {:.1} | {segments} |",
        puts as f64 / wall / 1e3,
        us(percentile(&latency, 0.5)),
        us(percentile(&latency, 0.99)),
        us(percentile(&latency, 0.999)),
        us(*latency.last().unwrap()),
        log,
    );
    puts
}

fn recovery(dir: &Path, values: &[Vec<u8>]) {
    let _ = fs::remove_dir_all(dir);
    let engine = Engine::create(dir, options(), durable(64 << 20)).unwrap();
    let puts = writes("(load)", &engine, values);
    drop(engine);
    println!();
    println!(
        "| recovery of {puts} puts | s | log MiB | log MiB/s | replayed | heap peak MiB | log MiB after |"
    );
    println!("|{}", "---|".repeat(7));
    let start = Instant::now();
    let ((engine, report), heap) =
        peak(|| Engine::open(dir, options(), durable(64 << 20)).unwrap());
    let s = start.elapsed().as_secs_f64();
    let log = mib(report.log_bytes as usize);
    println!(
        "| from the log alone | {s:.2} | {log:.1} | {:.0} | {} | {:.1} | |",
        log / s,
        report.replayed,
        mib(heap),
    );
    let start = Instant::now();
    let checkpoint = engine
        .durable_checkpoint(CheckpointOptions::default())
        .unwrap();
    let checkpoint_s = start.elapsed().as_secs_f64();
    let after = mib(engine.log_status().unwrap().bytes as usize);
    println!(
        "| (durable checkpoint: {checkpoint_s:.2} s, {:.1} MiB, {} segments pruned) | | | | | | {after:.1} |",
        mib(checkpoint.bytes as usize),
        checkpoint.pruned_segments,
    );
    drop(engine);
    let start = Instant::now();
    let ((engine, report), heap) =
        peak(|| Engine::open(dir, options(), durable(64 << 20)).unwrap());
    let s = start.elapsed().as_secs_f64();
    println!(
        "| from the checkpoint | {s:.2} | {:.1} | | {} | {:.1} | |",
        mib(report.log_bytes as usize),
        report.replayed,
        mib(heap),
    );
    drop(engine);
    let _ = fs::remove_dir_all(dir);
}

/// A durable writer and a reader with the worker, with or without durable
/// checkpoints back to back.
fn beside(dir: &Path, values: &[Vec<u8>], checkpoints: bool) {
    let _ = fs::remove_dir_all(dir);
    // Small memtables, so that flushes and compactions happen beside the log.
    let options = EngineOptions {
        memtable_budget: 2 << 20,
        ..options()
    };
    let engine = Engine::create(dir, options, durable(16 << 20)).unwrap();
    let status0 = engine.background_status().unwrap();
    let done = AtomicBool::new(false);
    let start = Instant::now();
    let (mut taken, mut pruned, mut most_log) = (0, 0, 0u64);
    let ((mut puts, wall), mut gets) = thread::scope(|scope| {
        let writer = scope.spawn(|| {
            let (mut rng, mut tally, mut latency) = (Rng(7), FlushTally::default(), Vec::new());
            for at in 0..PUTS {
                let key = key(rng.next() % KEYS);
                let t = Instant::now();
                put_flushing(&engine, &key, &values[at % values.len()], &mut tally);
                latency.push(t.elapsed().as_nanos() as u64);
                if at % 1024 == 0 && start.elapsed() > ROW_TIME {
                    break;
                }
            }
            done.store(true, Ordering::Relaxed);
            (latency, start.elapsed().as_secs_f64())
        });
        let reader = scope.spawn(|| {
            let (mut rng, mut workspace, mut latency) =
                (Rng(8), EngineWorkspace::new(), Vec::new());
            while !done.load(Ordering::Relaxed) {
                let key = key(rng.next() % KEYS);
                let t = Instant::now();
                black_box(engine.get(&key, &mut workspace).unwrap());
                latency.push(t.elapsed().as_nanos() as u64);
            }
            latency
        });
        while !writer.is_finished() {
            most_log = most_log.max(engine.log_status().unwrap().bytes);
            if checkpoints {
                let report = engine
                    .durable_checkpoint(CheckpointOptions::default())
                    .unwrap();
                taken += 1;
                pruned += report.pruned_segments;
            } else {
                thread::sleep(Duration::from_millis(5));
            }
        }
        (writer.join().unwrap(), reader.join().unwrap())
    });
    let status = engine.background_status().unwrap();
    puts.sort_unstable();
    gets.sort_unstable();
    println!(
        "| {} | {:.1} | {:.1} | {:.1} | {:.1} | {:.0} | {:.0} | {taken} | {pruned} | {} ({}) | {:.1} |",
        if checkpoints { "back to back" } else { "none" },
        puts.len() as f64 / wall / 1e3,
        us(percentile(&puts, 0.99)),
        gets.len() as f64 / wall / 1e3,
        us(percentile(&gets, 0.99)),
        us(percentile(&gets, 0.999)),
        us(*gets.last().unwrap_or(&0)),
        status.compactions - status0.compactions,
        status.failures - status0.failures,
        mib(most_log as usize),
    );
    drop(engine);
    let _ = fs::remove_dir_all(dir);
}

pub(super) fn durable_bench() {
    let root = std::env::args().nth(2).expect("a directory for databases");
    let dir = Path::new(&root).join("tessera-durable-bench");
    let values: Vec<Vec<u8>> = tree_json(1 << 20)
        .into_iter()
        .take(4096)
        .map(|record| record.value.unwrap())
        .collect();
    println!("databases in {}", dir.display());
    println!();
    println!(
        "One writer, uniform json overwrites over {KEYS} keys, at most {PUTS} puts or {} s:",
        ROW_TIME.as_secs()
    );
    println!();
    println!(
        "| mode | puts | k puts/s | p50 µs | p99 µs | p99.9 µs | max µs | log MiB | segments |"
    );
    println!("|{}", "---|".repeat(9));
    writes("RAM-only", &Engine::new(options()), &values);
    for (name, segment) in [
        ("durable, 64 MiB segments", 64 << 20),
        ("durable, 1 MiB segments", 1 << 20),
    ] {
        let _ = fs::remove_dir_all(&dir);
        let engine = Engine::create(&dir, options(), durable(segment)).unwrap();
        writes(name, &engine, &values);
        drop(engine);
    }
    let _ = fs::remove_dir_all(&dir);
    println!();
    recovery(&dir, &values);
    println!();
    println!("A durable writer (2 MiB memtables, 16 MiB segments), a reader and the worker:");
    println!();
    println!(
        "| checkpoints | k puts/s | put p99 µs | k gets/s | get p99 µs | get p99.9 µs | get max µs | checkpoints | segments pruned | compactions (failures) | most log MiB |"
    );
    println!("|{}", "---|".repeat(11));
    beside(&dir, &values, false);
    beside(&dir, &values, true);
}
