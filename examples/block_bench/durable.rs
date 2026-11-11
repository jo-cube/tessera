//! Milestones 5M and 5N: durable writes against RAM-only ones by writer
//! concurrency, syncing each write alone and with group commit, beside a
//! reader; then recovery from the log and from a checkpoint. The directory
//! is the argument after `durable`; databases are created under it and
//! removed here.

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
/// Each row stops after this many writes in all, or after this long.
const WRITES: usize = 200_000;
const ROW_TIME: Duration = Duration::from_secs(10);

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

fn durable(segment_bytes: u64, batch: usize) -> DurableOptions {
    DurableOptions {
        segment_bytes,
        max_log_bytes: u64::MAX,
        max_batch_records: batch,
        ..DurableOptions::default()
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

/// CPU this thread has used, in microseconds, at the kernel's 10 ms tick,
/// or 0 where `/proc` is missing.
fn thread_cpu_us() -> u64 {
    let Ok(stat) = fs::read_to_string("/proc/thread-self/stat") else {
        return 0;
    };
    let fields: Vec<&str> = stat[stat.rfind(')').unwrap() + 2..].split(' ').collect();
    let ticks: u64 = fields[11].parse::<u64>().unwrap() + fields[12].parse::<u64>().unwrap();
    ticks * 10_000
}

enum Mode {
    Ram,
    /// Durable, with these segments and at most this many writes a batch.
    Durable(u64, usize),
}

/// `writers` threads of uniform json overwrites and one reader, optionally
/// with durable checkpoints back to back.
fn row(name: &str, dir: &Path, mode: Mode, writers: usize, values: &[Vec<u8>], checkpoints: bool) {
    let _ = fs::remove_dir_all(dir);
    let engine = match mode {
        Mode::Ram => Engine::new(options()),
        Mode::Durable(segment, batch) => {
            Engine::create(dir, options(), durable(segment, batch)).unwrap()
        }
    };
    let per_writer = WRITES / writers;
    let done = AtomicBool::new(false);
    let start = Instant::now();
    let (mut taken, mut most_queued) = (0, 0);
    let (results, mut gets, wall) = thread::scope(|scope| {
        let handles: Vec<_> = (0..writers)
            .map(|writer| {
                let engine = &engine;
                scope.spawn(move || {
                    let cpu = thread_cpu_us();
                    let (mut rng, mut tally) = (Rng(7 + writer as u64), FlushTally::default());
                    let mut latency = Vec::with_capacity(per_writer);
                    for at in 0..per_writer {
                        let key = key(rng.next() % KEYS);
                        let t = Instant::now();
                        put_flushing(engine, &key, &values[at % values.len()], &mut tally);
                        latency.push(t.elapsed().as_nanos() as u64);
                        if at % 256 == 0 && start.elapsed() > ROW_TIME {
                            break;
                        }
                    }
                    (latency, thread_cpu_us() - cpu)
                })
            })
            .collect();
        let reader = scope.spawn(|| {
            let (mut rng, mut workspace, mut latency) =
                (Rng(99), EngineWorkspace::new(), Vec::new());
            while !done.load(Ordering::Relaxed) {
                let key = key(rng.next() % KEYS);
                let t = Instant::now();
                black_box(engine.get(&key, &mut workspace).unwrap());
                latency.push(t.elapsed().as_nanos() as u64);
            }
            latency
        });
        while handles.iter().any(|handle| !handle.is_finished()) {
            if let Some(status) = engine.log_status() {
                most_queued = most_queued.max(status.queued_bytes);
            }
            if checkpoints {
                engine
                    .durable_checkpoint(CheckpointOptions::default())
                    .unwrap();
                taken += 1;
            } else {
                thread::sleep(Duration::from_millis(1));
            }
        }
        let results: Vec<_> = handles.into_iter().map(|h| h.join().unwrap()).collect();
        let wall = start.elapsed().as_secs_f64();
        done.store(true, Ordering::Relaxed);
        (results, reader.join().unwrap(), wall)
    });
    let mut latency: Vec<u64> = results
        .iter()
        .flat_map(|(l, _)| l.iter().copied())
        .collect();
    let cpu: u64 = results.iter().map(|(_, cpu)| cpu).sum();
    latency.sort_unstable();
    gets.sort_unstable();
    let writes = latency.len() as f64;
    let log = engine.log_status();
    let (syncs, batch, large, bytes) = log.map_or(Default::default(), |s| {
        let large: u64 = s.batch_sizes[3..].iter().sum();
        (
            format!("{:.3}", s.syncs as f64 / writes),
            format!("{:.1}", s.records as f64 / s.batches as f64),
            format!("{:.0}", large as f64 * 100.0 / s.batches as f64),
            format!("{:.1}", mib(s.bytes as usize)),
        )
    });
    println!(
        "| {name} | {writers} | {:.1} | {:.1} | {:.1} | {:.1} | {syncs} | {batch} | {large} | {bytes} | {:.1} | {:.0} | {:.1} | {} | {:.0} |",
        writes / wall / 1e3,
        us(percentile(&latency, 0.5)),
        us(percentile(&latency, 0.95)),
        us(percentile(&latency, 0.99)),
        cpu as f64 / writes,
        gets.len() as f64 / wall / 1e3,
        us(percentile(&gets, 0.99)),
        if checkpoints {
            taken.to_string()
        } else {
            String::new()
        },
        most_queued as f64 / 1024.0,
    );
    drop(engine);
    let _ = fs::remove_dir_all(dir);
}

fn recovery(dir: &Path, values: &[Vec<u8>]) {
    let _ = fs::remove_dir_all(dir);
    let durable = durable(64 << 20, 64);
    let engine = Engine::create(dir, options(), durable).unwrap();
    let mut tally = FlushTally::default();
    let mut rng = Rng(11);
    let start = Instant::now();
    let mut puts = 0;
    while puts < 300_000 && start.elapsed() < ROW_TIME {
        put_flushing(
            &engine,
            &key(rng.next() % KEYS),
            &values[puts % values.len()],
            &mut tally,
        );
        puts += 1;
    }
    drop(engine);
    println!("| recovery of {puts} puts | s | log MiB | log MiB/s | replayed | heap peak MiB |");
    println!("|{}", "---|".repeat(6));
    let start = Instant::now();
    let ((engine, report), heap) = peak(|| Engine::open(dir, options(), durable).unwrap());
    let s = start.elapsed().as_secs_f64();
    let log = mib(report.log_bytes as usize);
    println!(
        "| from the log alone | {s:.2} | {log:.1} | {:.0} | {} | {:.1} |",
        log / s,
        report.replayed,
        mib(heap),
    );
    let start = Instant::now();
    let checkpoint = engine
        .durable_checkpoint(CheckpointOptions::default())
        .unwrap();
    println!(
        "| (durable checkpoint: {:.2} s, {:.1} MiB, {} segments pruned, log after {:.1} MiB) | | | | | |",
        start.elapsed().as_secs_f64(),
        mib(checkpoint.bytes as usize),
        checkpoint.pruned_segments,
        mib(engine.log_status().unwrap().bytes as usize),
    );
    drop(engine);
    let start = Instant::now();
    let ((engine, report), heap) = peak(|| Engine::open(dir, options(), durable).unwrap());
    println!(
        "| from the checkpoint | {:.2} | {:.1} | | {} | {:.1} |",
        start.elapsed().as_secs_f64(),
        mib(report.log_bytes as usize),
        report.replayed,
        mib(heap),
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
        "Uniform json overwrites over {KEYS} keys from W writers, at most {WRITES} writes or {} s, beside one reader:",
        ROW_TIME.as_secs()
    );
    println!();
    println!(
        "| mode | W | k writes/s | p50 µs | p95 µs | p99 µs | syncs/write | mean batch | batches ≥5 % | log MiB | writer CPU µs/write | k gets/s | get p99 µs | checkpoints | most queued KiB |"
    );
    println!("|{}", "---|".repeat(15));
    for writers in [1, 2, 4] {
        row("RAM-only", &dir, Mode::Ram, writers, &values, false);
    }
    for writers in [1, 2, 4] {
        row(
            "sync per write",
            &dir,
            Mode::Durable(64 << 20, 1),
            writers,
            &values,
            false,
        );
    }
    for writers in [1, 2, 4, 8] {
        row(
            "group commit",
            &dir,
            Mode::Durable(64 << 20, 64),
            writers,
            &values,
            false,
        );
    }
    row(
        "group, 1 MiB segments",
        &dir,
        Mode::Durable(1 << 20, 64),
        4,
        &values,
        false,
    );
    row(
        "group, checkpoints",
        &dir,
        Mode::Durable(64 << 20, 64),
        4,
        &values,
        true,
    );
    println!();
    recovery(&dir, &values);
}
