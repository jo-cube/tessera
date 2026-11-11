//! Milestone 5K, investigation only: the engine with its background worker
//! under sustained workloads, concurrent readers and writers, small resource
//! limits, and read-heavy phases, with a simulated decoded-block cache and a
//! deliberate consolidation. Every phase has a fixed amount of work.

use std::hint::black_box;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Barrier, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use tessera::engine::{
    BackgroundError, CompactionError, CompactionOptions, Engine, EngineOptions, EngineWorkspace,
    SnapshotLimits, UpdateError,
};
use tessera::memtable::WriteError;
use tessera::run::{MergeWorkspace, ReadWorkspace, Run};

use super::{
    FLAT_TREE, LIVE, PEAK, Rng, SUB_LEN, TENANT_LEN, mib, percentile, process_cpu_s, task_cpu_ns,
    tree_json, tree_key, vm_rss_bytes, worker_task,
};

const BUDGET: usize = 4 << 20;
const KEYS: usize = 200_000;
const PUTS: usize = 1_200_000;
const SAMPLE_EVERY: usize = 16;

/// Values by kind, drawn from a fixed pool so writers do not format them.
#[derive(Clone, Copy, PartialEq)]
enum Kind {
    Small,
    Json,
    Large,
}

impl Kind {
    fn name(self) -> &'static str {
        match self {
            Self::Small => "8-byte",
            Self::Json => "json",
            Self::Large => "4 KiB",
        }
    }

    fn pool(self) -> Vec<Vec<u8>> {
        let mut rng = Rng(51);
        match self {
            Self::Small => (0..4096).map(|_| rng.bytes(8)).collect(),
            Self::Json => tree_json(1 << 20)
                .into_iter()
                .take(4096)
                .map(|record| record.value.unwrap())
                .collect(),
            Self::Large => (0..256)
                .map(|_| {
                    let mut value = rng.bytes(1024);
                    value.resize(4096, 0);
                    value
                })
                .collect(),
        }
    }
}

fn key(index: usize) -> Vec<u8> {
    tree_key(index as u64, FLAT_TREE)
}

fn options(background: bool) -> EngineOptions {
    EngineOptions {
        memtable_budget: BUDGET,
        background_compaction: background.then(CompactionOptions::default),
        ..EngineOptions::default()
    }
}

/// What the writing threads saw.
#[derive(Default)]
struct Writes {
    latency: Vec<u64>,
    flushes: usize,
    flushed: usize,
    build_ns: u64,
    publish_max_ns: u64,
    stalls: usize,
    stall_ns: u64,
}

impl Writes {
    fn absorb(&mut self, other: Writes) {
        self.latency.extend(other.latency);
        self.flushes += other.flushes;
        self.flushed += other.flushed;
        self.build_ns += other.build_ns;
        self.publish_max_ns = self.publish_max_ns.max(other.publish_max_ns);
        self.stalls += other.stalls;
        self.stall_ns += other.stall_ns;
    }
}

/// Flushes every frozen table, waiting for compaction when refused. A pass
/// can make room and still end on a failure, so this fails only if a flush
/// is refused again after a failed wait.
fn flush(engine: &Engine, writes: &mut Writes) -> Result<(), BackgroundError> {
    let mut failed = None;
    for _ in 0..100_000 {
        let Some(job) = engine.begin_flush() else {
            return Ok(());
        };
        let start = Instant::now();
        let built = job.build();
        let published = Instant::now();
        writes.build_ns += (published - start).as_nanos() as u64;
        match built.publish() {
            Ok(report) => {
                failed = None;
                let ns = published.elapsed().as_nanos() as u64;
                writes.publish_max_ns = writes.publish_max_ns.max(ns);
                writes.flushes += 1;
                writes.flushed += report.run_bytes;
            }
            Err(_) => {
                writes.stalls += 1;
                let start = Instant::now();
                let waited = engine.wait_for_compaction();
                writes.stall_ns += start.elapsed().as_nanos() as u64;
                if let Some(error) = failed.take() {
                    return Err(error);
                }
                failed = waited.err();
            }
        }
    }
    panic!("flushing did not finish");
}

fn put(engine: &Engine, key: &[u8], value: &[u8], w: &mut Writes) -> Result<(), BackgroundError> {
    for _ in 0..3 {
        match engine.put(key, value) {
            Ok(_) => return Ok(()),
            Err(UpdateError::Rejected(WriteError::BudgetExceeded { .. })) => {
                // Another writer may be flushing; this waits for it.
                flush(engine, w)?;
            }
            Err(error) => panic!("{error}"),
        }
    }
    panic!("a put failed after flushing");
}

/// `puts` puts from `next`, which picks a key and a value from the pool,
/// timing every 16th put including any flush it runs.
fn write(engine: &Engine, puts: usize, mut next: impl FnMut(usize) -> (Vec<u8>, usize)) -> Writes {
    let mut writes = Writes::default();
    VALUES.with(|pool| {
        let pool = pool.borrow();
        for at in 0..puts {
            let (key, value) = next(at);
            let value = &pool[value % pool.len()];
            if at % SAMPLE_EVERY == 0 {
                let start = Instant::now();
                put(engine, &key, value, &mut writes).unwrap();
                writes.latency.push(start.elapsed().as_nanos() as u64);
            } else {
                put(engine, &key, value, &mut writes).unwrap();
            }
        }
    });
    writes
}

thread_local! {
    static VALUES: std::cell::RefCell<Vec<Vec<u8>>> = const { std::cell::RefCell::new(Vec::new()) };
}

fn use_values(kind: Kind) {
    let pool = kind.pool();
    VALUES.with(|values| *values.borrow_mut() = pool);
}

/// Samples a get every 2 ms and the run shape every 50 ms until stopped.
struct Sampler {
    stop: Arc<AtomicBool>,
    handle: thread::JoinHandle<(Vec<u64>, usize, usize, f64)>,
}

impl Sampler {
    fn start(engine: &Arc<Engine>, keys: usize) -> Self {
        let stop = Arc::new(AtomicBool::new(false));
        let (engine, flag) = (Arc::clone(engine), Arc::clone(&stop));
        let handle = thread::spawn(move || {
            let mut rng = Rng(61);
            let mut workspace = EngineWorkspace::new();
            let (mut latency, mut runs, mut overlap, mut sum, mut samples) =
                (Vec::new(), 0, 0, 0, 0);
            let mut tick = 0;
            while !flag.load(Ordering::Relaxed) {
                let key = key((rng.next() % keys as u64) as usize);
                let start = Instant::now();
                black_box(engine.get(&key, &mut workspace).unwrap());
                latency.push(start.elapsed().as_nanos() as u64);
                if tick % 25 == 0 {
                    let stats = engine.stats();
                    runs = runs.max(stats.runs);
                    overlap = overlap.max(stats.max_overlap);
                    sum += stats.max_overlap;
                    samples += 1;
                }
                tick += 1;
                thread::sleep(Duration::from_millis(2));
            }
            latency.sort_unstable();
            (latency, runs, overlap, sum as f64 / samples.max(1) as f64)
        });
        Self { stop, handle }
    }

    fn finish(self) -> (Vec<u64>, usize, usize, f64) {
        self.stop.store(true, Ordering::Relaxed);
        self.handle.join().unwrap()
    }
}

fn worker_cpu_s() -> f64 {
    worker_task().map_or(0.0, |task| task_cpu_ns(&task) as f64 / 1e9)
}

fn us(ns: u64) -> f64 {
    ns as f64 / 1e3
}

/// Puts the base key space in key order and waits until it is settled.
fn loaded(kind: Kind, keys: usize, background: bool) -> Arc<Engine> {
    use_values(kind);
    let engine = Arc::new(Engine::new(options(background)));
    let mut writes = write(&engine, keys, |at| (key(at), at));
    flush(&engine, &mut writes).unwrap();
    if background {
        engine.wait_for_compaction().unwrap();
    }
    engine
}

fn run_sizes(engine: &Engine) -> String {
    let guard = engine.read();
    let mut sizes: Vec<usize> = guard.runs().map(|run| run.as_bytes().len()).collect();
    sizes.sort_unstable();
    match sizes.as_slice() {
        [] => "none".into(),
        sizes => format!(
            "{} runs, {:.2}/{:.2}/{:.2}",
            sizes.len(),
            mib(sizes[0]),
            mib(sizes[sizes.len() / 2]),
            mib(sizes[sizes.len() - 1])
        ),
    }
}

#[derive(Clone, Copy)]
enum Workload {
    NewKeys,
    Uniform,
    Hot,
    Mixed,
}

impl Workload {
    fn name(self) -> &'static str {
        match self {
            Self::NewKeys => "new keys",
            Self::Uniform => "uniform overwrite",
            Self::Hot => "hot 1% takes 90%",
            Self::Mixed => "uniform + busy reader",
        }
    }
}

/// One sustained workload with the worker, from a settled base for
/// overwrites. Prints one row.
fn sustained(kind: Kind, workload: Workload, keys: usize, puts: usize) -> Arc<Engine> {
    let engine = match workload {
        Workload::NewKeys => {
            use_values(kind);
            Arc::new(Engine::new(options(true)))
        }
        _ => loaded(kind, keys, true),
    };
    let space = match workload {
        Workload::NewKeys => puts,
        _ => keys,
    };
    let hot: Vec<usize> = {
        let mut rng = Rng(71);
        (0..keys / 100)
            .map(|_| (rng.next() % keys as u64) as usize)
            .collect()
    };
    let order = super::shuffled(puts, 72);
    let flushed_before = engine.background_status().unwrap();
    let sampler = Sampler::start(&engine, space);
    let busy = matches!(workload, Workload::Mixed).then(|| {
        let engine = Arc::clone(&engine);
        let stop = Arc::new(AtomicBool::new(false));
        let flag = Arc::clone(&stop);
        let handle = thread::spawn(move || {
            let (mut rng, mut workspace, mut reads) = (Rng(73), EngineWorkspace::new(), 0u64);
            while !flag.load(Ordering::Relaxed) {
                let at = (rng.next() % keys as u64) as usize;
                if reads % 10 == 9 {
                    let prefix = &key(at)[..SUB_LEN];
                    let guard = engine.read();
                    let mut scan = guard.scan_prefix(prefix, &mut workspace);
                    while let Some(record) = scan.next_record().unwrap() {
                        black_box(record);
                    }
                } else {
                    black_box(engine.get(&key(at), &mut workspace).unwrap());
                }
                reads += 1;
            }
            reads
        });
        (stop, handle)
    });
    let (cpu0, worker0) = (process_cpu_s(), worker_cpu_s());
    PEAK.store(LIVE.load(Ordering::Relaxed), Ordering::Relaxed);
    let heap0 = LIVE.load(Ordering::Relaxed);
    let start = Instant::now();
    let mut rng = Rng(74);
    let mut writes = write(&engine, puts, |at| {
        let index = match workload {
            Workload::NewKeys => order[at] + keys,
            Workload::Hot if !rng.next().is_multiple_of(10) => {
                hot[(rng.next() % hot.len() as u64) as usize]
            }
            _ => (rng.next() % keys as u64) as usize,
        };
        (key(index), rng.next() as usize)
    });
    flush(&engine, &mut writes).unwrap();
    let wall = start.elapsed().as_secs_f64();
    let caught = Instant::now();
    engine.wait_for_compaction().unwrap();
    let catch_up = caught.elapsed().as_secs_f64();
    let peak = PEAK.load(Ordering::Relaxed).saturating_sub(heap0);
    let (cpu, worker) = (process_cpu_s() - cpu0, worker_cpu_s() - worker0);
    let reads = busy.map(|(stop, handle)| {
        stop.store(true, Ordering::Relaxed);
        handle.join().unwrap()
    });
    let (gets, runs, overlap, mean_overlap) = sampler.finish();
    let status = engine.background_status().unwrap();
    let compacted = (status.output_bytes - flushed_before.output_bytes) as usize;
    let stats = engine.stats();
    writes.latency.sort_unstable();
    println!(
        "| {} | {} | {puts} | {:.2} | {:.0} | {:.2} | {:.1} | {:.0} | {} | {:.1} | {:.2} | {} | {:.2} | {:.2} | {:.2} | {:.2} | {:.1} | {runs} | {overlap} | {:.2} | {:.0} | {:.0} | {:.1} | {:.1} | {:.2} | {:.0} | {} | {} |",
        kind.name(),
        workload.name(),
        wall,
        puts as f64 / wall / 1e3,
        us(percentile(&writes.latency, 0.5)),
        us(percentile(&writes.latency, 0.99)),
        us(*writes.latency.last().unwrap()) / 1e3,
        writes.flushes,
        writes.build_ns as f64 / writes.flushes.max(1) as f64 / 1e6,
        us(writes.publish_max_ns) / 1e3,
        writes.stalls,
        worker,
        cpu,
        compacted as f64 / writes.flushed as f64,
        (compacted + writes.flushed) as f64 / writes.flushed as f64,
        mean_overlap,
        catch_up,
        puts as f64 / (wall + catch_up) / 1e3,
        us(percentile(&gets, 0.5)),
        us(percentile(&gets, 0.99)),
        mib(peak as usize),
        mib(stats.run_bytes + stats.filter_bytes),
        vm_rss_bytes().map_or(0.0, |rss| mib(rss as usize)),
        reads.map_or("".into(), |reads| format!(
            "{:.0}",
            reads as f64 / wall / 1e3
        )),
        run_sizes(&engine),
    );
    engine
}

/// Point and scan latency on the current state from one thread, and gets per
/// second from two threads together.
fn read_phase(label: &str, engine: &Arc<Engine>, keys: usize) {
    let mut rng = Rng(81);
    let mut workspace = EngineWorkspace::new();
    let mut timed = |count: usize, op: &mut dyn FnMut(&mut Rng, &mut EngineWorkspace)| {
        let blocks = workspace.blocks_loaded();
        let mut latency: Vec<u64> = (0..count)
            .map(|_| {
                let start = Instant::now();
                op(&mut rng, &mut workspace);
                start.elapsed().as_nanos() as u64
            })
            .collect();
        latency.sort_unstable();
        let mean = latency.iter().sum::<u64>() as f64 / count as f64;
        (
            us(percentile(&latency, 0.5)),
            us(percentile(&latency, 0.99)),
            mean / 1e3,
            (workspace.blocks_loaded() - blocks) as f64 / count as f64,
        )
    };
    let present = timed(20_000, &mut |rng, workspace| {
        let key = key((rng.next() % keys as u64) as usize);
        black_box(engine.get(&key, workspace).unwrap());
    });
    let absent = timed(20_000, &mut |rng, workspace| {
        let mut key = key((rng.next() % keys as u64) as usize);
        key.push(0);
        black_box(engine.get(&key, workspace).unwrap());
    });
    let scan = |prefix_len: usize| {
        move |rng: &mut Rng, workspace: &mut EngineWorkspace| {
            let key = key((rng.next() % keys as u64) as usize);
            let guard = engine.read();
            let mut scan = guard.scan_prefix(&key[..prefix_len], workspace);
            while let Some(record) = scan.next_record().unwrap() {
                black_box(record);
            }
        }
    };
    let short = timed(2_000, &mut scan(SUB_LEN));
    let long = timed(100, &mut scan(TENANT_LEN));
    let total = Arc::new(AtomicU64::new(0));
    let start = Instant::now();
    thread::scope(|scope| {
        for seed in 0..2 {
            let total = Arc::clone(&total);
            scope.spawn(move || {
                let (mut rng, mut workspace) = (Rng(90 + seed), EngineWorkspace::new());
                for _ in 0..50_000 {
                    let key = key((rng.next() % keys as u64) as usize);
                    black_box(engine.get(&key, &mut workspace).unwrap());
                }
                total.fetch_add(50_000, Ordering::Relaxed);
            });
        }
    });
    let two = total.load(Ordering::Relaxed) as f64 / start.elapsed().as_secs_f64() / 1e3;
    let stats = engine.stats();
    println!(
        "| {label} | {} | {} | {:.1} | {:.1} | {:.2} | {:.1} | {:.2} | {:.0} | {:.1} | {:.0} | {:.0} | {:.1} |",
        stats.runs,
        stats.max_overlap,
        present.0,
        present.1,
        present.3,
        absent.0,
        absent.3,
        1e3 / present.2,
        short.2,
        long.2,
        two,
        long.3,
    );
}

/// Replays lookups against the blocks the engine would decode, through an
/// LRU of decoded blocks, and returns the hit rate for each capacity.
fn cache_hits(
    engine: &Engine,
    lookups: &[Vec<u8>],
    capacities: &[usize],
) -> (Vec<f64>, usize, usize) {
    let guard = engine.read();
    let runs: Vec<&Run<Vec<u8>>> = guard.runs().collect();
    let total: usize = runs.iter().map(|run| run.block_count()).sum();
    let mut read = ReadWorkspace::new();
    // With filters, a lookup decodes a block only in runs holding the key
    // (and in about 1% of the others, left out here).
    let accesses: Vec<(usize, usize)> = lookups
        .iter()
        .flat_map(|key| {
            runs.iter()
                .enumerate()
                .filter(|(_, run)| run.get(key, &mut read).unwrap().is_some())
                .map(|(index, run)| (index, run.view().locate(key).unwrap().unwrap()))
                .collect::<Vec<_>>()
        })
        .collect();
    let distinct = {
        let mut all = accesses.clone();
        all.sort_unstable();
        all.dedup();
        all.len()
    };
    let rates = capacities
        .iter()
        .map(|&capacity| {
            let mut cache: std::collections::HashMap<(usize, usize), usize> =
                std::collections::HashMap::new();
            let (mut hits, mut counted) = (0, 0);
            let warm = accesses.len() / 5;
            for (at, &block) in accesses.iter().enumerate() {
                let hit = cache.insert(block, at).is_some();
                if cache.len() > capacity {
                    let oldest = *cache.iter().min_by_key(|(_, used)| **used).unwrap().0;
                    cache.remove(&oldest);
                }
                if at >= warm {
                    counted += 1;
                    hits += usize::from(hit);
                }
            }
            hits as f64 / counted as f64 * 100.0
        })
        .collect();
    (rates, distinct, total)
}

fn cache_rows(label: &str, engine: &Engine, keys: usize) {
    let mut rng = Rng(101);
    let hot: Vec<usize> = (0..keys / 100)
        .map(|_| (rng.next() % keys as u64) as usize)
        .collect();
    let start = (rng.next() % (keys - keys / 100) as u64) as usize;
    let lookups = |pick: &mut dyn FnMut(&mut Rng) -> usize| -> Vec<Vec<u8>> {
        let mut rng = Rng(102);
        (0..30_000).map(|_| key(pick(&mut rng))).collect()
    };
    // Blocks of 16 KiB decoded: 1, 4 and 16 MiB of cache.
    let capacities = [64, 256, 1024];
    for (name, keys) in [
        (
            "uniform",
            lookups(&mut |rng| (rng.next() % keys as u64) as usize),
        ),
        (
            "90% to 1% of keys, scattered",
            lookups(&mut |rng| {
                if !rng.next().is_multiple_of(10) {
                    hot[(rng.next() % hot.len() as u64) as usize]
                } else {
                    (rng.next() % keys as u64) as usize
                }
            }),
        ),
        (
            "90% to 1% of keys, contiguous",
            lookups(&mut |rng| {
                if !rng.next().is_multiple_of(10) {
                    start + (rng.next() % (keys / 100) as u64) as usize
                } else {
                    (rng.next() % keys as u64) as usize
                }
            }),
        ),
    ] {
        let (rates, distinct, total) = cache_hits(engine, &keys, &capacities);
        println!(
            "| {label} | {name} | {total} | {distinct} | {:.1}% | {:.1}% | {:.1}% |",
            rates[0], rates[1], rates[2]
        );
    }
}

/// Writers splitting `puts` uniform overwrites, beside `readers` threads
/// reading as fast as they can, and optionally a thread scanning everything
/// through snapshots back to back. (Back-to-back scans through a read guard
/// let a writer in about once per scan, as documented, and are left out.)
#[derive(Clone, Copy, PartialEq)]
enum Scanner {
    None,
    Snapshot,
}

fn concurrent(writers: usize, readers: usize, scanner: Scanner, background: bool, puts: usize) {
    let engine = loaded(Kind::Json, KEYS, background);
    let done = AtomicBool::new(false);
    let reads = AtomicU64::new(0);
    let scans = Mutex::new(Vec::new());
    let all = Mutex::new(Writes::default());
    let barrier = Barrier::new(writers + 1);
    let (cpu0, worker0) = (process_cpu_s(), worker_cpu_s());
    let mut wall = 0.0;
    thread::scope(|scope| {
        for reader in 0..readers {
            let (engine, done, reads) = (&engine, &done, &reads);
            scope.spawn(move || {
                let (mut rng, mut workspace, mut count) =
                    (Rng(110 + reader as u64), EngineWorkspace::new(), 0);
                while !done.load(Ordering::Relaxed) {
                    let key = key((rng.next() % KEYS as u64) as usize);
                    black_box(engine.get(&key, &mut workspace).unwrap());
                    count += 1;
                }
                reads.fetch_add(count, Ordering::Relaxed);
            });
        }
        if scanner != Scanner::None {
            let (engine, done, scans) = (&engine, &done, &scans);
            scope.spawn(move || {
                let mut workspace = EngineWorkspace::new();
                while !done.load(Ordering::Relaxed) {
                    let start = Instant::now();
                    let snapshot = engine.snapshot().unwrap();
                    let mut scan = snapshot.scan(&mut workspace);
                    let mut count = 0;
                    while scan.next_record().unwrap().is_some() {
                        count += 1;
                    }
                    assert_eq!(count, KEYS);
                    scans
                        .lock()
                        .unwrap()
                        .push(start.elapsed().as_nanos() as u64);
                }
            });
        }
        let handles: Vec<_> = (0..writers)
            .map(|writer| {
                let (engine, all, barrier) = (&engine, &all, &barrier);
                scope.spawn(move || {
                    use_values(Kind::Json);
                    let mut rng = Rng(120 + writer as u64);
                    barrier.wait();
                    let writes = write(engine, puts / writers, |_| {
                        (
                            (key((rng.next() % KEYS as u64) as usize)),
                            rng.next() as usize,
                        )
                    });
                    all.lock().unwrap().absorb(writes);
                })
            })
            .collect();
        barrier.wait();
        let start = Instant::now();
        for handle in handles {
            handle.join().unwrap();
        }
        wall = start.elapsed().as_secs_f64();
        done.store(true, Ordering::Relaxed);
    });
    let (cpu, worker) = (process_cpu_s() - cpu0, worker_cpu_s() - worker0);
    let mut writes = all.into_inner().unwrap();
    writes.latency.sort_unstable();
    let mut scans = scans.into_inner().unwrap();
    scans.sort_unstable();
    println!(
        "| {writers} | {readers} | {} | {} | {:.0} | {:.1} | {:.1} | {:.2} | {:.0} | {} | {:.2} | {:.2} | {} | {} |",
        match scanner {
            Scanner::None => "",
            Scanner::Snapshot => "snapshot full scans",
        },
        if background { "on" } else { "off" },
        puts as f64 / wall / 1e3,
        us(percentile(&writes.latency, 0.5)),
        us(percentile(&writes.latency, 0.99)),
        us(*writes.latency.last().unwrap()) / 1e3,
        reads.load(Ordering::Relaxed) as f64 / wall / 1e3,
        writes.flushes,
        worker,
        cpu,
        if scans.is_empty() {
            "".into()
        } else {
            format!(
                "{} × {:.0} ms",
                scans.len(),
                us(percentile(&scans, 0.5)) / 1e3
            )
        },
        writes.stalls,
    );
}

/// New keys into a 1 MiB memtable with only 8 runs allowed, so flushes are
/// refused whenever compaction falls behind.
fn run_limit_pressure() {
    use_values(Kind::Json);
    let engine = Engine::new(EngineOptions {
        memtable_budget: 1 << 20,
        max_runs: 8,
        ..options(true)
    });
    let order = super::shuffled(600_000, 131);
    let start = Instant::now();
    let mut writes = write(&engine, 600_000, |at| (key(order[at]), at));
    flush(&engine, &mut writes).unwrap();
    engine.wait_for_compaction().unwrap();
    let wall = start.elapsed().as_secs_f64();
    let stats = engine.stats();
    let status = engine.background_status().unwrap();
    println!(
        "| run limit 8, 1 MiB memtable, 600k new keys | {:.2} s, {:.0} k puts/s | {} flushes, {} refused, {:.2} s waiting | {} compactions, {} failures | {} runs, overlap {} |",
        wall,
        600_000.0 / wall / 1e3,
        writes.flushes,
        writes.stalls,
        writes.stall_ns as f64 / 1e9,
        status.compactions,
        status.failures,
        stats.runs,
        stats.max_overlap,
    );
}

/// Overwrites while one snapshot of the settled 3.9 MiB state is held and at
/// most 2 MiB may be retained (it can retain at most what it saw),
/// until compaction and then flushing are refused; then the snapshot is
/// dropped and only flushes are retried.
fn retention_pressure() {
    let engine = Engine::new(EngineOptions {
        max_runs: 16,
        snapshots: SnapshotLimits {
            max_retained_bytes: 2 << 20,
            ..SnapshotLimits::default()
        },
        ..options(true)
    });
    use_values(Kind::Json);
    let mut writes = write(&engine, KEYS, |at| (key(at), at));
    flush(&engine, &mut writes).unwrap();
    engine.wait_for_compaction().unwrap();
    let snapshot = engine.snapshot().unwrap();
    let mut rng = Rng(141);
    let mut writes = Writes::default();
    let (mut puts, mut most_retained) = (0, 0);
    let start = Instant::now();
    let blocked = loop {
        let value = VALUES.with(|pool| pool.borrow()[puts % 4096].clone());
        match put(
            &engine,
            &key((rng.next() % KEYS as u64) as usize),
            &value,
            &mut writes,
        ) {
            Ok(()) => puts += 1,
            Err(BackgroundError::Failed(CompactionError::Retained { .. })) => break true,
            Err(error) => panic!("{error}"),
        }
        if puts % 10_000 == 0 {
            most_retained = most_retained.max(engine.stats().retained_bytes);
        }
        assert!(puts < 5_000_000, "never blocked");
    };
    let blocked_after = start.elapsed().as_secs_f64();
    let stats = engine.stats();
    most_retained = most_retained.max(stats.retained_bytes);
    drop(snapshot);
    let dropped = Instant::now();
    let mut retries = 0;
    let deadline = dropped + Duration::from_secs(60);
    while engine.flush().is_err() {
        retries += 1;
        assert!(Instant::now() < deadline, "no recovery");
        thread::yield_now();
    }
    let recovered = dropped.elapsed().as_secs_f64();
    let mut rng = Rng(142);
    let after = write(&engine, 200_000, |at| {
        ((key((rng.next() % KEYS as u64) as usize)), at)
    });
    engine.wait_for_compaction().unwrap();
    println!(
        "| 1 snapshot, 2 MiB retention limit, 16 runs, uniform json | blocked: {blocked} after {puts} puts, {blocked_after:.2} s | retained at most {:.1} MiB (run {:.1} + filter {:.2}), {} runs | recovered {:.1} ms after the drop, {retries} flush retries | 200k more puts, {} refused flushes; retained {:.1} MiB |",
        mib(most_retained),
        mib(stats.retained_run_bytes),
        mib(stats.retained_filter_bytes),
        stats.runs,
        recovered * 1e3,
        after.stalls,
        mib(engine.stats().retained_bytes),
    );
}

/// Drops an engine with a compaction backlog while another thread is
/// scanning a snapshot of it.
fn shutdown_pressure() {
    use_values(Kind::Json);
    let engine = Engine::new(EngineOptions {
        memtable_budget: 1 << 20,
        ..options(true)
    });
    let order = super::shuffled(400_000, 151);
    let mut writes = write(&engine, 400_000, |at| (key(order[at]), at));
    flush(&engine, &mut writes).unwrap();
    let backlog = engine.stats();
    let busy = engine.background_status().unwrap().busy;
    let snapshot = engine.snapshot().unwrap();
    let started = Barrier::new(2);
    thread::scope(|scope| {
        let scanner = scope.spawn(|| {
            let mut workspace = EngineWorkspace::new();
            let mut scan = snapshot.scan(&mut workspace);
            let mut count = 0;
            started.wait();
            while scan.next_record().unwrap().is_some() {
                count += 1;
            }
            count
        });
        started.wait();
        let start = Instant::now();
        drop(engine);
        let dropped = start.elapsed().as_secs_f64();
        let count = scanner.join().unwrap();
        println!(
            "| drop with backlog ({} runs, overlap {}, worker busy {busy}) and a snapshot scan running | dropped in {:.1} ms | scan finished with {count} of 400000 records | | |",
            backlog.runs,
            backlog.max_overlap,
            dropped * 1e3,
        );
    });
}

/// Runs every section, or only `concurrency` or `pressure` when that is the
/// next argument.
pub(super) fn operate_bench() {
    match std::env::args().nth(2).as_deref() {
        Some("concurrency") => return concurrency_rows(),
        Some("pressure") => return pressure_rows(),
        _ => {}
    }
    println!(
        "operate: {BUDGET}-byte memtables, default limits and compaction, background worker; overwrite workloads start from {KEYS} settled keys; a sampler gets one key every 2 ms"
    );
    println!();
    println!(
        "| values | workload | puts | write s | k puts/s | put p50 µs | put p99 µs | put max ms | flushes | build ms/flush | publish max ms | refused flushes | worker CPU s | process CPU s | compacted per flushed byte | total written per flushed byte | mean overlap | max runs | max overlap | catch-up s | sustainable k puts/s | get p50 µs | get p99 µs | heap peak MiB | runs+filters MiB | RSS MiB | busy reader k/s | runs, MiB min/median/max |"
    );
    println!("|{}", "---|".repeat(28));
    for kind in [Kind::Small, Kind::Json] {
        for workload in [
            Workload::NewKeys,
            Workload::Uniform,
            Workload::Hot,
            Workload::Mixed,
        ] {
            drop(sustained(kind, workload, KEYS, PUTS));
        }
    }
    drop(sustained(Kind::Large, Workload::NewKeys, 20_000, 60_000));
    drop(sustained(Kind::Large, Workload::Uniform, 20_000, 60_000));

    println!();
    println!(
        "Read-heavy phase after uniform json overwrites, then consolidated with max_overlap 1."
    );
    println!();
    println!(
        "| state | runs | overlap | get p50 µs | get p99 µs | blocks/get | absent p50 µs | absent blocks | k gets/s, 1 thread | 20-key scan µs | 4000-key scan µs | k gets/s, 2 threads | blocks/long scan |"
    );
    println!("|{}", "---|".repeat(13));
    let engine = sustained(Kind::Json, Workload::Uniform, KEYS, PUTS);
    read_phase("settled after overwrites", &engine, KEYS);
    println!();
    println!(
        "| state | lookups | blocks in runs | distinct blocks decoded | hit, 64 blocks (1 MiB) | 256 (4 MiB) | 1024 (16 MiB) |"
    );
    println!("|---|---|---|---|---|---|---|");
    cache_rows("settled", &engine, KEYS);
    let before = engine.stats();
    let mut workspace = MergeWorkspace::new();
    let (mut rewritten, mut written, mut count) = (0, 0, 0);
    let cpu = task_cpu_ns("thread-self");
    PEAK.store(LIVE.load(Ordering::Relaxed), Ordering::Relaxed);
    let heap0 = LIVE.load(Ordering::Relaxed);
    let start = Instant::now();
    let consolidate = CompactionOptions {
        max_overlap: 1,
        ..CompactionOptions::default()
    };
    while let Some(report) = engine.compact(consolidate, &mut workspace).unwrap() {
        rewritten += report.input_bytes;
        written += report.output_bytes;
        count += 1;
    }
    let elapsed = start.elapsed().as_secs_f64();
    let cpu = (task_cpu_ns("thread-self") - cpu) as f64 / 1e9;
    let peak = PEAK.load(Ordering::Relaxed).saturating_sub(heap0);
    println!();
    println!(
        "Consolidation: {count} compactions, {:.1} MiB read and {:.1} MiB written ({:.2} of the {:.1} MiB stored), {elapsed:.2} s, {cpu:.2} CPU s, heap peak {:.1} MiB above the start",
        mib(rewritten),
        mib(written),
        rewritten as f64 / before.run_bytes as f64,
        mib(before.run_bytes),
        mib(peak as usize),
    );
    println!();
    println!(
        "| state | runs | overlap | get p50 µs | get p99 µs | blocks/get | absent p50 µs | absent blocks | k gets/s, 1 thread | 20-key scan µs | 4000-key scan µs | k gets/s, 2 threads | blocks/long scan |"
    );
    println!("|{}", "---|".repeat(13));
    read_phase("consolidated", &engine, KEYS);
    println!();
    println!(
        "| state | lookups | blocks in runs | distinct blocks decoded | hit, 64 blocks (1 MiB) | 256 (4 MiB) | 1024 (16 MiB) |"
    );
    println!("|---|---|---|---|---|---|---|");
    cache_rows("consolidated", &engine, KEYS);
    drop(engine);

    concurrency_rows();
    pressure_rows();
}

fn concurrency_rows() {
    println!();
    println!(
        "Concurrency: json uniform overwrites from {KEYS} settled keys, 600k puts split between the writers; 2 CPUs."
    );
    println!();
    println!(
        "| writers | readers | scanner | worker | k puts/s | put p50 µs | put p99 µs | put max ms | k gets/s | flushes | worker CPU s | process CPU s | scans | refused flushes |"
    );
    println!("|{}", "---|".repeat(14));
    for (writers, readers, scanner, background) in [
        (1, 0, Scanner::None, false),
        (1, 0, Scanner::None, true),
        (2, 0, Scanner::None, true),
        (4, 0, Scanner::None, true),
        (1, 1, Scanner::None, true),
        (1, 2, Scanner::None, true),
        (2, 2, Scanner::None, true),
        (1, 0, Scanner::Snapshot, true),
    ] {
        concurrent(writers, readers, scanner, background, 600_000);
    }
}

fn pressure_rows() {
    println!();
    println!("Resource pressure.");
    println!();
    println!("| scenario | | | | |");
    println!("|---|---|---|---|---|");
    run_limit_pressure();
    retention_pressure();
    shutdown_pressure();
}
