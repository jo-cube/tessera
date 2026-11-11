//! Milestone 5L: checkpoint and recovery cost, on an idle engine and beside
//! a writer, a reader and the background worker. The directory is the
//! argument after `checkpoint`; it is created and removed here.

use std::fs;
use std::hint::black_box;
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::Instant;

use tessera::engine::{
    CheckpointOptions, CompactionOptions, Engine, EngineOptions, EngineWorkspace,
};

use super::{FlushTally, LIVE, PEAK, Rng, mib, percentile, put_flushing, tree_json, tree_key};

fn key(index: usize) -> Vec<u8> {
    tree_key(index as u64, (1_000, 100, 1_000))
}

fn options() -> EngineOptions {
    EngineOptions {
        memtable_budget: 16 << 20,
        background_compaction: Some(CompactionOptions::default()),
        ..EngineOptions::default()
    }
}

fn loaded(keys: usize, values: &[Vec<u8>]) -> Engine {
    let engine = Engine::new(options());
    let mut tally = FlushTally::default();
    for at in 0..keys {
        put_flushing(&engine, &key(at), &values[at % values.len()], &mut tally);
    }
    tally.flush(&engine);
    engine.wait_for_compaction().unwrap();
    engine
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

fn idle(name: &str, keys: usize, values: &[Vec<u8>], dir: &Path) {
    let engine = loaded(keys, values);
    let stats = engine.stats();
    let payload: usize = (0..keys)
        .map(|at| key(at).len() + values[at % values.len()].len())
        .sum();
    let _ = fs::remove_dir_all(dir);
    let start = Instant::now();
    let (report, heap) = peak(|| {
        engine
            .checkpoint(dir, CheckpointOptions::default())
            .unwrap()
    });
    let write_s = start.elapsed().as_secs_f64();
    drop(engine);

    let start = Instant::now();
    let mut read = 0;
    for entry in fs::read_dir(dir.join(format!("ckpt-{:020}", report.generation))).unwrap() {
        read += fs::read(entry.unwrap().path()).unwrap().len();
    }
    let read_s = start.elapsed().as_secs_f64();
    let start = Instant::now();
    let (recovered, recover_heap) = peak(|| Engine::recover(dir, options()).unwrap());
    let recover_s = start.elapsed().as_secs_f64();
    let after = recovered.stats();
    assert_eq!(after.last_seq, report.last_seq);
    let mut workspace = EngineWorkspace::new();
    let mut rng = Rng(5);
    for _ in 0..1_000 {
        let at = (rng.next() % keys as u64) as usize;
        assert!(recovered.get(&key(at), &mut workspace).unwrap().is_some());
    }
    println!(
        "| {name} | {keys} | {:.1} | {:.1} ({} runs) | {:.1} | {:.2} | {:.0} | {:.0} | {:.1} | {:.3} | {:.2} | {:.0} | {:.1} | {:.2} |",
        mib(payload),
        mib(stats.run_bytes),
        stats.runs,
        mib(report.bytes as usize),
        write_s,
        mib(payload) / write_s,
        report.records as f64 / write_s / 1e3,
        mib(heap),
        read_s,
        recover_s,
        mib(read) / recover_s,
        mib(recover_heap),
        mib(after.filter_bytes),
    );
    let _ = fs::remove_dir_all(dir);
}

/// A writer overwriting uniformly and a reader getting, with the worker,
/// with or without checkpoints back to back.
fn beside(keys: usize, values: &[Vec<u8>], dir: &Path, checkpoints: bool) {
    let engine = loaded(keys, values);
    let _ = fs::remove_dir_all(dir);
    let done = AtomicBool::new(false);
    let status0 = engine.background_status().unwrap();
    let (mut puts, mut latency, mut gets, mut taken, mut retained) =
        (0, Vec::new(), Vec::new(), Vec::new(), 0);
    let start = Instant::now();
    thread::scope(|scope| {
        let writer = scope.spawn(|| {
            let (mut rng, mut tally, mut latency) = (Rng(7), FlushTally::default(), Vec::new());
            for at in 0..1_500_000usize {
                let key = key((rng.next() % keys as u64) as usize);
                let value = &values[at % values.len()];
                if at % 16 == 0 {
                    let t = Instant::now();
                    put_flushing(&engine, &key, value, &mut tally);
                    latency.push(t.elapsed().as_nanos() as u64);
                } else {
                    put_flushing(&engine, &key, value, &mut tally);
                }
            }
            latency
        });
        let reader = scope.spawn(|| {
            let (mut rng, mut workspace, mut latency) =
                (Rng(8), EngineWorkspace::new(), Vec::new());
            let mut retained = 0;
            while !done.load(Ordering::Relaxed) {
                let key = key((rng.next() % keys as u64) as usize);
                let t = Instant::now();
                black_box(engine.get(&key, &mut workspace).unwrap());
                latency.push(t.elapsed().as_nanos() as u64);
                if latency.len() % 256 == 0 {
                    retained = retained.max(engine.stats().retained_bytes);
                }
            }
            (latency, retained)
        });
        let mut cycles = 0;
        while checkpoints && !writer.is_finished() {
            let t = Instant::now();
            engine
                .checkpoint(dir, CheckpointOptions::default())
                .unwrap();
            taken.push(t.elapsed().as_nanos() as u64);
            cycles += 1;
            assert!(cycles < 10_000);
        }
        latency = writer.join().unwrap();
        puts = 1_500_000;
        done.store(true, Ordering::Relaxed);
        (gets, retained) = reader.join().unwrap();
    });
    let wall = start.elapsed().as_secs_f64();
    let status = engine.background_status().unwrap();
    latency.sort_unstable();
    gets.sort_unstable();
    taken.sort_unstable();
    println!(
        "| {} | {:.0} | {:.1} | {:.0} | {:.1} | {} | {} | {} | {:.1} |",
        if checkpoints {
            "checkpoints back to back"
        } else {
            "none"
        },
        puts as f64 / wall / 1e3,
        percentile(&latency, 0.99) as f64 / 1e3,
        gets.len() as f64 / wall / 1e3,
        percentile(&gets, 0.99) as f64 / 1e3,
        if taken.is_empty() {
            String::new()
        } else {
            format!(
                "{} × {:.0} ms",
                taken.len(),
                percentile(&taken, 0.5) as f64 / 1e6
            )
        },
        status.compactions - status0.compactions,
        status.failures - status0.failures,
        mib(retained),
    );
    let _ = fs::remove_dir_all(dir);
}

pub(super) fn checkpoint_bench() {
    let dir = std::env::args()
        .nth(2)
        .expect("a directory for checkpoints");
    let dir = Path::new(&dir).join("tessera-checkpoint-bench");
    let json: Vec<Vec<u8>> = tree_json(1 << 20)
        .into_iter()
        .take(4096)
        .map(|record| record.value.unwrap())
        .collect();
    let mut rng = Rng(3);
    let small: Vec<Vec<u8>> = (0..4096).map(|_| rng.bytes(8)).collect();
    let large: Vec<Vec<u8>> = (0..64)
        .map(|_| {
            let mut value = rng.bytes(1024);
            value.resize(4096, 0);
            value
        })
        .collect();
    println!("checkpoint into {}", dir.display());
    println!();
    println!(
        "| values | keys | payload MiB | runs MiB | checkpoint MiB | checkpoint s | payload MiB/s | k records/s | heap peak MiB | read files s | recover s | recover MiB/s | recover heap MiB | filters MiB |"
    );
    println!("|{}", "---|".repeat(14));
    idle("json", 1_000_000, &json, &dir);
    idle("8-byte", 2_000_000, &small, &dir);
    idle("4 KiB", 50_000, &large, &dir);
    println!();
    println!("Beside 1.5M json uniform overwrites over 1M keys, a reader and the worker:");
    println!();
    println!(
        "| checkpoints | k puts/s | put p99 µs | k gets/s | get p99 µs | checkpoints taken | compactions | compaction failures | most retained MiB |"
    );
    println!("|{}", "---|".repeat(9));
    beside(1_000_000, &json, &dir, false);
    beside(1_000_000, &json, &dir, true);
}
