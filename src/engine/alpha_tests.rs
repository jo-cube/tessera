//! Alpha-readiness measurements after 5AU: sustained background leveled
//! compaction at run targets above the 64 KiB experiments, with each
//! flush's phases timed; output growth after reopening under another
//! codec; and checkpoint costs. All are ignored research runs: run one
//! test, and one `ALPHA_FILTER` selection, per process with `--release
//! --ignored --nocapture`, so that the peak resident memory reported is the
//! run's own.
//!
//! Every publication in a test build also checks the level invariants
//! (`Levels::check`), so these runs check them throughout, at a cost that a
//! release build of the library does not pay.

use std::collections::BTreeMap;
use std::sync::Mutex as StdMutex;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Instant;

use super::capacity_tests::{Rng, Workload, index};
use super::*;

fn env(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

/// A field of `/proc/self/status`, such as `VmRSS:`, in MiB; 0 elsewhere.
fn status_mib(field: &str) -> f64 {
    std::fs::read_to_string("/proc/self/status")
        .ok()
        .and_then(|status| {
            let line = status.lines().find(|line| line.starts_with(field))?;
            let kib: f64 = line.split_whitespace().nth(1)?.parse().ok()?;
            Some(kib / 1024.0)
        })
        .unwrap_or(0.0)
}

fn key(index: u64) -> Vec<u8> {
    format!("user/{index:010}").into_bytes()
}

/// A text value of `len` bytes, from the capacity tests' vocabulary.
fn text(seed: u64, len: usize) -> Vec<u8> {
    let mut out = Vec::with_capacity(len + 100);
    let mut part = seed;
    while out.len() < len {
        out.extend_from_slice(&super::capacity_tests::value_for(part));
        part = part.wrapping_add(0x51_7cc1_b727_220a);
    }
    out.truncate(len);
    out
}

/// `len` pseudo-random bytes, which no codec shrinks.
fn noise(seed: u64, len: usize) -> Vec<u8> {
    let mut state = seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1;
    (0..len)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state as u8
        })
        .collect()
}

/// Percentiles of nanosecond samples, in µs: (p50, p99, p99.9, max).
fn percentiles(samples: &mut [u64]) -> (f64, f64, f64, f64) {
    if samples.is_empty() {
        return (0.0, 0.0, 0.0, 0.0);
    }
    samples.sort_unstable();
    let at = |q: f64| samples[((samples.len() - 1) as f64 * q) as usize] as f64 / 1e3;
    (
        at(0.5),
        at(0.99),
        at(0.999),
        *samples.last().unwrap() as f64 / 1e3,
    )
}

fn fmt_p((p50, p99, p999, max): (f64, f64, f64, f64)) -> String {
    format!("{p50:.2}/{p99:.1}/{p999:.0}/{max:.0}")
}

/// Each level's (runs, KiB), and the bytes above each level's target plus
/// level 0's bytes once it is due: the debt compaction owes.
fn shape_and_debt(engine: &Engine, fit: &Fit) -> (String, usize) {
    let state = engine.core.read_state();
    let shape = state.levels.shape();
    let targets = state.levels.targets(fit);
    let mut debt = 0;
    for (level, (_, bytes)) in shape.iter().enumerate() {
        if level == 0 {
            if state.levels.level0_flushes() >= fit.trigger {
                debt += bytes;
            }
        } else if targets[level] > 0 {
            debt += bytes.saturating_sub(targets[level]);
        }
    }
    let shape = shape
        .iter()
        .map(|(runs, bytes)| format!("{runs}:{}", bytes >> 10))
        .collect::<Vec<_>>()
        .join("/");
    (shape, debt)
}

/// The stored bytes of `engine`'s live records, built into runs of the
/// same options without history and dropped as they are measured: the
/// denominator of space amplification.
fn live_stored(engine: &Engine, options: &EngineOptions) -> usize {
    let codec = options.compression.codec();
    let snapshot = engine.snapshot().unwrap();
    let mut read = EngineWorkspace::new();
    let mut scan = snapshot.scan(&mut read);
    let (mut total, mut input, mut seq) = (0, 0, 0);
    let mut builder = None::<crate::run::RunBuilder>;
    let finish = |builder: crate::run::RunBuilder| {
        let run = builder.finish();
        run.as_bytes().len() + run.held_bytes()
    };
    while let Some(record) = scan.next_record().unwrap() {
        seq += 1;
        builder
            .get_or_insert_with(|| {
                crate::run::RunBuilder::with_target_block_len(codec, options.target_block_len)
            })
            .add(record.key, seq, crate::block::Value::Bytes(record.value))
            .unwrap();
        input += record.key.len() + record.value.len() + crate::run::RECORD_INPUT_OVERHEAD;
        if input >= options.target_run_len {
            total += finish(builder.take().unwrap());
            input = 0;
        }
    }
    total + builder.map_or(0, finish)
}

/// Nanosecond samples in buckets of an eighth of a power of two: within
/// 12.5% at any percentile, in constant memory.
#[derive(Default)]
struct Histogram {
    counts: Vec<u64>,
    max: u64,
}

impl Histogram {
    fn bucket(nanos: u64) -> usize {
        if nanos < 8 {
            return nanos as usize;
        }
        let exponent = 63 - nanos.leading_zeros() as usize;
        let mantissa = ((nanos >> (exponent - 3)) & 7) as usize;
        (exponent - 2) * 8 + mantissa
    }

    fn lower(bucket: usize) -> u64 {
        if bucket < 8 {
            return bucket as u64;
        }
        let (exponent, mantissa) = (bucket / 8 + 2, bucket % 8);
        (8 + mantissa as u64) << (exponent - 3)
    }

    fn record(&mut self, nanos: u64) {
        let bucket = Self::bucket(nanos);
        if self.counts.len() <= bucket {
            self.counts.resize(bucket + 1, 0);
        }
        self.counts[bucket] += 1;
        self.max = self.max.max(nanos);
    }

    fn len(&self) -> u64 {
        self.counts.iter().sum()
    }

    /// (p50, p99, p99.9, max) in µs.
    fn percentiles(&self) -> (f64, f64, f64, f64) {
        let total = self.len();
        let at = |q: f64| {
            let rank = (total as f64 * q).ceil() as u64;
            let mut seen = 0;
            for (bucket, count) in self.counts.iter().enumerate() {
                seen += count;
                if seen >= rank.max(1) {
                    return Self::lower(bucket) as f64 / 1e3;
                }
            }
            self.max as f64 / 1e3
        };
        (at(0.5), at(0.99), at(0.999), self.max as f64 / 1e3)
    }
}

/// Point reads (µs, mean) and prefix scans of ten keys and of ten
/// thousand (µs and blocks loaded per scan, mean), over keys `probe`
/// draws.
fn read_costs(
    engine: &Engine,
    rng: &mut Rng,
    probe: &dyn Fn(&mut Rng) -> u64,
) -> (f64, f64, f64, f64, f64) {
    let mut read = EngineWorkspace::new();
    let probes = 20_000u64;
    let start = Instant::now();
    for _ in 0..probes {
        let index = probe(rng);
        std::hint::black_box(engine.get(&key(index), &mut read).unwrap().is_some());
    }
    let point = start.elapsed().as_secs_f64() * 1e6 / probes as f64;
    let mut prefix = |length: usize, count: u64, rng: &mut Rng| {
        let loaded = read.blocks_loaded();
        let start = Instant::now();
        for _ in 0..count {
            let index = probe(rng);
            let prefix = &key(index)[..length];
            let guard = engine.read();
            let mut scan = guard.scan_prefix(prefix, &mut read);
            while let Some(record) = scan.next_record().unwrap() {
                std::hint::black_box(record.key);
            }
        }
        (
            start.elapsed().as_secs_f64() * 1e6 / count as f64,
            (read.blocks_loaded() - loaded) as f64 / count as f64,
        )
    };
    let (short, short_blocks) = prefix(14, 2_000, rng);
    let (long, _) = prefix(11, 100, rng);
    (
        point,
        short,
        short_blocks,
        long,
        read.context_bytes() as f64 / 1024.0,
    )
}

#[derive(Clone, Copy, Debug, PartialEq)]
enum History {
    Ascending,
    UniformInsert,
    UniformOverwrite,
    Hot,
    MixedDeletes,
}

/// Sustained writes under the background worker, at a run target of
/// `ALPHA_RUN_KIB` (1024 by default) with jobs of four runs, as the
/// defaults are of four 64 MiB runs; `ALPHA_RECORDS` writes (1,000,000) of
/// `ALPHA_VALUE` bytes (100) after a 15-byte key; `ALPHA_READER=1` (the
/// default) adds a thread of point reads and short scans beside the writer.
/// Reports the operating endpoint, as the writes left it, and the drained
/// one, after a final flush and a pass with nothing left due.
#[test]
#[ignore = "research: run with --release --ignored --nocapture"]
fn alpha_sustained_background() {
    let run = env("ALPHA_RUN_KIB", 1024) as usize * 1024;
    let writes = env("ALPHA_RECORDS", 1_000_000);
    let value_len = env("ALPHA_VALUE", 100) as usize;
    let reader = env("ALPHA_READER", 1) == 1;
    let jobs = CompactionOptions {
        max_input_bytes: 4 * run,
        max_output_bytes: 4 * run,
        ..CompactionOptions::default()
    };
    let options = EngineOptions {
        memtable_budget: run,
        target_run_len: run,
        max_runs: usize::MAX / 4,
        max_run_bytes: usize::MAX / 4,
        background_compaction: Some(jobs),
        ..EngineOptions::default()
    };
    let fit = jobs.fit(run, levels::LEVEL0_STOP);
    let histories = [
        History::Ascending,
        History::UniformInsert,
        History::UniformOverwrite,
        History::Hot,
        History::MixedDeletes,
    ];
    for history in histories {
        for incompressible in [false, true] {
            let values = if incompressible {
                "incompressible"
            } else {
                "text"
            };
            let label = format!("{history:?}|{values}");
            if let Ok(filter) = std::env::var("ALPHA_FILTER")
                && !label.contains(&filter)
            {
                continue;
            }
            let engine = Engine::new(options).unwrap();
            *engine.core.step_log.lock().unwrap() = Some(Vec::new());
            *engine.core.flush_log.lock().unwrap() = Some(Vec::new());
            let keys = match history {
                History::UniformOverwrite | History::Hot => writes / 4,
                _ => writes,
            };
            let workload = match history {
                History::Ascending => Some(Workload::Ascending),
                History::UniformInsert => Some(Workload::UniformInsert),
                History::UniformOverwrite => Some(Workload::UniformUpdate),
                History::Hot => Some(Workload::Hot),
                History::MixedDeletes => None,
            };
            let written = AtomicU64::new(0);
            let done = AtomicBool::new(false);
            let read_samples = StdMutex::new(Histogram::default());
            let started = Instant::now();
            let mut rng = Rng(11);
            let mut live: Vec<u64> = Vec::new();
            let mut put_nanos = Histogram::default();
            let (mut logical, mut refused_writes) = (0u64, 0u64);
            let (mut l0_max, mut l0_sum, mut l0_samples, mut at_stop) = (0, 0u64, 0u64, 0u64);
            let (mut debt_max, mut debt_sum) = (0usize, 0u64);
            let mut levels_max = 0;
            std::thread::scope(|scope| {
                if reader {
                    scope.spawn(|| {
                        let mut rng = Rng(99);
                        let mut read = EngineWorkspace::new();
                        let mut samples = Histogram::default();
                        let mut reads = 0u64;
                        while !done.load(Ordering::Relaxed) {
                            let upto = written.load(Ordering::Relaxed).max(1);
                            let index = match workload {
                                Some(Workload::UniformInsert) => {
                                    index(Workload::UniformInsert, rng.below(upto), keys, &mut rng)
                                }
                                Some(Workload::Ascending) => rng.below(upto),
                                Some(_) => rng.below(keys),
                                None => rng.below(10_000_000_000),
                            };
                            let start = Instant::now();
                            if reads % 64 == 63 {
                                let guard = engine.read();
                                let mut scan = guard.scan_prefix(&key(index)[..14], &mut read);
                                while let Some(record) = scan.next_record().unwrap() {
                                    std::hint::black_box(record.key);
                                }
                            } else {
                                std::hint::black_box(
                                    engine.get(&key(index), &mut read).unwrap().is_some(),
                                );
                                samples.record(start.elapsed().as_nanos() as u64);
                            }
                            reads += 1;
                        }
                        *read_samples.lock().unwrap() = samples;
                    });
                }
                for write in 0..writes {
                    let (index, delete) = match workload {
                        Some(workload) => (index(workload, write, keys, &mut rng), false),
                        None => {
                            let roll = rng.below(10);
                            if roll < 5 || live.is_empty() {
                                let index = rng.below(10_000_000_000);
                                live.push(index);
                                (index, false)
                            } else if roll < 8 {
                                (live[rng.below(live.len() as u64) as usize], false)
                            } else {
                                let at = rng.below(live.len() as u64) as usize;
                                (live.swap_remove(at), true)
                            }
                        }
                    };
                    let key = key(index);
                    let value = if incompressible {
                        noise(write, value_len)
                    } else {
                        text(write, value_len)
                    };
                    let start = Instant::now();
                    let result = if delete {
                        logical += key.len() as u64;
                        engine.delete(&key)
                    } else {
                        logical += (key.len() + value.len()) as u64;
                        engine.put(&key, &value)
                    };
                    put_nanos.record(start.elapsed().as_nanos() as u64);
                    written.store(write, Ordering::Relaxed);
                    if let Err(error) = result {
                        if refused_writes == 0 {
                            eprintln!(
                                "ALPHA refused | {label} | {error} | {:?}",
                                engine.background_status()
                            );
                        }
                        refused_writes += 1;
                        let _ = engine.wait_for_compaction();
                    }
                    if write % 1024 == 0 {
                        let stats = engine.stats();
                        l0_max = l0_max.max(stats.level0_flushes);
                        l0_sum += stats.level0_flushes as u64;
                        l0_samples += 1;
                        at_stop += u64::from(stats.level0_flushes >= levels::LEVEL0_STOP);
                        levels_max = levels_max.max(stats.levels);
                        if write % (64 * 1024) == 0 {
                            let (_, debt) = shape_and_debt(&engine, &fit);
                            debt_max = debt_max.max(debt);
                            debt_sum += debt as u64;
                        }
                    }
                }
                done.store(true, Ordering::Relaxed);
            });
            let write_seconds = started.elapsed().as_secs_f64();
            let debt_samples = writes.div_ceil(64 * 1024).max(1);
            // The operating endpoint.
            let stats = engine.stats();
            let (operating_shape, operating_debt) = shape_and_debt(&engine, &fit);
            let operating_stored = stats.run_bytes + stats.held_bytes;
            let operating_memtables = stats.active.charged + stats.frozen.map_or(0, |f| f.charged);
            let operating_rss = status_mib("VmRSS:");
            let operating_flushed = stats.flushed_bytes;
            let operating_out = engine.background_status().unwrap().output_bytes;
            let probe = |rng: &mut Rng| match history {
                History::MixedDeletes if !live.is_empty() => {
                    live[rng.below(live.len() as u64) as usize]
                }
                History::Ascending | History::UniformInsert => {
                    index(Workload::UniformInsert, rng.below(writes), keys, rng)
                }
                _ => rng.below(keys),
            };
            let operating_reads = read_costs(&engine, &mut rng, &probe);
            // Drain.
            let start = Instant::now();
            while engine.flush().is_err() {
                refused_writes += 1;
                let _ = engine.wait_for_compaction();
            }
            let drain = engine.wait_for_compaction();
            let drain_seconds = start.elapsed().as_secs_f64();
            let elapsed = started.elapsed().as_secs_f64();
            let stats = engine.stats();
            let status = engine.background_status().unwrap();
            let (drained_shape, drained_debt) = shape_and_debt(&engine, &fit);
            let drained_stored = stats.run_bytes + stats.held_bytes;
            let drained_reads = read_costs(&engine, &mut rng, &probe);
            let drained_rss = status_mib("VmRSS:");
            let peak = status_mib("VmHWM:");
            let live = live_stored(&engine, &options);
            let flushed = stats.flushed_bytes as f64;
            let log = engine.core.step_log.lock().unwrap().take().unwrap();
            let flushes = engine.core.flush_log.lock().unwrap().take().unwrap();
            // Per-level rewrites: merges' output by source level, against
            // the bytes flushed.
            let depth = log
                .iter()
                .map(|step| step.level)
                .filter(|&l| l != usize::MAX);
            let depth = depth.max().unwrap_or(0) + 1;
            let mut per_level = vec![(0usize, 0usize, 0usize, 0u64); depth];
            let mut kinds = BTreeMap::<String, usize>::new();
            let (mut busy, mut failed) = (0u64, 0usize);
            for step in &log {
                busy += step.nanos;
                if step.error.is_some() {
                    failed += 1;
                }
                let Some(kind) = step.kind else { continue };
                *kinds.entry(format!("{kind:?}")).or_default() += 1;
                let entry = &mut per_level[step.level];
                entry.0 += 1;
                entry.1 += step.input_bytes;
                entry.2 += step.output_bytes;
                entry.3 += step.nanos;
            }
            let compaction_in: usize = per_level.iter().map(|level| level.1).sum();
            let compaction_out: usize = per_level.iter().map(|level| level.2).sum();
            let per_level = per_level
                .iter()
                .enumerate()
                .map(|(level, (jobs, input, output, _))| {
                    format!(
                        "L{level} {jobs}x {:.1}>{:.1} MiB WA+{:.2}",
                        *input as f64 / 1048576.0,
                        *output as f64 / 1048576.0,
                        *output as f64 / flushed.max(1.0)
                    )
                })
                .collect::<Vec<_>>()
                .join(", ");
            let put_p = put_nanos.percentiles();
            let reads = read_samples.into_inner().unwrap();
            let read_p = reads.percentiles();
            let phase = |f: &dyn Fn(&FlushRecord) -> u64| {
                let mut samples: Vec<u64> = flushes.iter().map(f).collect();
                let mean = samples.iter().sum::<u64>() as f64 / samples.len().max(1) as f64 / 1e3;
                let (_, p99, _, max) = percentiles(&mut samples);
                format!("{:.0}/{p99:.0}/{max:.0}", mean)
            };
            let flush_refused = flushes.iter().filter(|flush| flush.refused).count();
            let mib = |bytes: f64| bytes / 1048576.0;
            println!(
                "ALPHA | {label} | records {writes} value {value_len} B run {} KiB reader {reader} | logical {:.1} MiB live stored {:.1} MiB | write {write_seconds:.1} s ({:.0} k writes/s) drain {drain_seconds:.2} s {drain:?} |",
                run >> 10,
                mib(logical as f64),
                mib(live as f64),
                writes as f64 / write_seconds / 1e3,
            );
            println!(
                "ALPHA-WA | {label} | operating WA {:.2} | drained WA {:.2} vs logical {:.3} | {per_level} | jobs {kinds:?} failed {failed} | rewrites per live byte {:.2} |",
                (operating_flushed as f64 + operating_out as f64)
                    / (operating_flushed as f64).max(1.0),
                (flushed + compaction_out as f64) / flushed.max(1.0),
                (flushed + compaction_out as f64) / logical.max(1) as f64,
                compaction_out as f64 / live.max(1) as f64,
            );
            println!(
                "ALPHA-SHAPE | {label} | operating {operating_shape} space {:.2} (+memtables {:.1} MiB) debt {:.1} MiB | drained {drained_shape} space {:.2} debt {:.1} MiB | levels max {levels_max} | L0 flushes max {l0_max} mean {:.2} at stop {:.2}% | debt max {:.1} mean {:.1} MiB |",
                operating_stored as f64 / live.max(1) as f64,
                mib(operating_memtables as f64),
                mib(operating_debt as f64),
                drained_stored as f64 / live.max(1) as f64,
                mib(drained_debt as f64),
                l0_sum as f64 / l0_samples.max(1) as f64,
                100.0 * at_stop as f64 / l0_samples.max(1) as f64,
                mib(debt_max as f64),
                mib(debt_sum as f64) / debt_samples as f64,
            );
            println!(
                "ALPHA-STALL | {label} | level-0 waits {} ({:.2} s) | refused writes {refused_writes} flushes {flush_refused} | worker passes {} failures {} last {:?} | put µs p50/p99/p99.9/max {} | worker busy {:.1} s ({:.0}% of {elapsed:.1} s to drained), {:.1} MiB/s in, {:.1} MiB/s out |",
                engine.core.level0_waits.load(Ordering::SeqCst),
                engine.core.level0_wait_nanos.load(Ordering::SeqCst) as f64 / 1e9,
                status.passes,
                status.failures,
                status.last_failure,
                fmt_p(put_p),
                busy as f64 / 1e9,
                100.0 * busy as f64 / 1e9 / elapsed,
                mib(compaction_in as f64) / (busy as f64 / 1e9).max(1e-9),
                mib(compaction_out as f64) / (busy as f64 / 1e9).max(1e-9),
            );
            println!(
                "ALPHA-FLUSH | {label} | flushes {} runs per flush {:.2} | mean/p99/max µs: wait {} freeze {} encode {} finish {} publish {} (under the state lock {}) |",
                flushes.len(),
                flushes.iter().map(|flush| flush.runs).sum::<usize>() as f64
                    / flushes.len().max(1) as f64,
                phase(&|flush| flush.wait),
                phase(&|flush| flush.freeze),
                phase(&|flush| flush.encode),
                phase(&|flush| flush.finish),
                phase(&|flush| flush.publish),
                phase(&|flush| flush.locked),
            );
            println!(
                "ALPHA-READ | {label} | under load: point µs p50/p99/p99.9/max {} ({} samples) | operating: point {:.2} µs, short {:.1} µs ({:.2} blocks), long {:.0} µs | drained: point {:.2} µs, short {:.1} µs ({:.2} blocks), long {:.0} µs | reader contexts {:.0} KiB |",
                fmt_p(read_p),
                reads.len(),
                operating_reads.0,
                operating_reads.1,
                operating_reads.2,
                operating_reads.3,
                drained_reads.0,
                drained_reads.1,
                drained_reads.2,
                drained_reads.3,
                drained_reads.4,
            );
            println!(
                "ALPHA-MEM | {label} | RSS operating {operating_rss:.0} MiB (engine {:.0} MiB) drained {drained_rss:.0} MiB (engine {:.0} MiB) | peak {:.0} MiB |",
                mib((operating_stored + operating_memtables) as f64),
                mib(drained_stored as f64),
                peak,
            );
            engine.close().unwrap();
        }
    }
}

/// A directory under the system's temporary directory, removed on drop.
struct Scratch(std::path::PathBuf);

impl Scratch {
    fn new(name: &str) -> Self {
        let path =
            std::env::temp_dir().join(format!("tessera-alpha-{name}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&path);
        Self(path)
    }
}

impl Drop for Scratch {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn contents(engine: &Engine) -> BTreeMap<Vec<u8>, Vec<u8>> {
    let mut workspace = EngineWorkspace::new();
    let guard = engine.read();
    let mut scan = guard.scan(&mut workspace);
    let mut out = BTreeMap::new();
    while let Some(record) = scan.next_record().unwrap() {
        out.insert(record.key.to_vec(), record.value.to_vec());
    }
    out
}

/// Output growth under realistic pressure: a leveled database built and
/// checkpointed under one compression, recovered under another whose
/// output is larger, then overwritten under the background worker. Every
/// merge of recovered runs can outgrow its inputs by the codec's ratio,
/// beyond the 1/32 headroom selection keeps, so jobs fail at the output
/// limit after reading their inputs, and each source's allowance halves.
/// Reports the failures, refusals, the allowances held (by live and by
/// retired sources), write refusals and level-0 waits, whether work kept
/// completing after the first failure, the shape and the small runs left,
/// and checks every record against a model. `ALPHA_GROWTH_RECORDS` (300,000)
/// records of 100-byte text, then half as many overwrites, in runs of
/// `ALPHA_RUN_KIB` (256 here) with jobs of four runs.
#[test]
#[ignore = "research: run with --release --ignored --nocapture"]
fn alpha_output_growth_after_codec_switch() {
    let run = env("ALPHA_RUN_KIB", 256) as usize * 1024;
    let records = env("ALPHA_GROWTH_RECORDS", 300_000);
    // Jobs of `ALPHA_GROWTH_JOB_RUNS` runs (4).
    let job_runs = env("ALPHA_GROWTH_JOB_RUNS", 4) as usize;
    let jobs = CompactionOptions {
        max_input_bytes: job_runs * run,
        max_output_bytes: job_runs * run,
        ..CompactionOptions::default()
    };
    let switches = [
        (Compression::Zstd, Compression::Uncompressed, false),
        (Compression::Zstd, Compression::Lz4, false),
        (Compression::ZstdDictionary, Compression::Zstd, false),
        (Compression::Zstd, Compression::Uncompressed, true),
    ];
    for (from, to, incompressible) in switches {
        let label = format!(
            "{from:?}>{to:?}|{}{}",
            if incompressible {
                "incompressible"
            } else {
                "text"
            },
            if env("ALPHA_GROWTH_SYNC", 0) == 1 {
                "|backlog"
            } else {
                ""
            }
        ) + &format!("|jobs of {job_runs} runs");
        if let Ok(filter) = std::env::var("ALPHA_FILTER")
            && !label.contains(&filter)
        {
            continue;
        }
        let with = |compression, background: bool| EngineOptions {
            memtable_budget: run,
            target_run_len: run,
            max_runs: usize::MAX / 4,
            max_run_bytes: usize::MAX / 4,
            compression,
            background_compaction: background.then_some(jobs),
            ..EngineOptions::default()
        };
        let value = |seed: u64| {
            if incompressible {
                noise(seed, 100)
            } else {
                text(seed, 100)
            }
        };
        let scratch = Scratch::new("growth");
        let mut model = BTreeMap::new();
        let before = {
            let engine = Engine::new(with(from, true)).unwrap();
            let mut rng = Rng(5);
            for write in 0..records {
                let index = index(Workload::UniformInsert, write, records, &mut rng);
                let (key, value) = (key(index), value(write));
                while engine.put(&key, &value).is_err() {
                    let _ = engine.wait_for_compaction();
                }
                model.insert(key, value);
            }
            engine.flush().unwrap();
            engine.wait_for_compaction().unwrap();
            let shape = shape_and_debt(&engine, &jobs.fit(run, levels::LEVEL0_STOP)).0;
            engine
                .checkpoint(&scratch.0, CheckpointOptions::default())
                .unwrap();
            let stats = engine.stats();
            engine.close().unwrap();
            (shape, stats.run_bytes + stats.held_bytes)
        };
        // With `ALPHA_GROWTH_SYNC=1` the recovered engine has no worker: the
        // overwrites pile up in level 0 over the recovered runs, and are
        // compacted after, so that level-0 merges take recovered runs
        // before neighbour merges have re-encoded them.
        let sync = env("ALPHA_GROWTH_SYNC", 0) == 1;
        let engine = Engine::recover(&scratch.0, with(to, !sync)).unwrap();
        *engine.core.step_log.lock().unwrap() = Some(Vec::new());
        let fit = jobs.fit(run, levels::LEVEL0_STOP);
        let recovered = shape_and_debt(&engine, &fit).0;
        let mut rng = Rng(6);
        let (mut refused_writes, mut first_refusal, mut l0_max) = (0u64, None, 0);
        let mut allowances_max = (0, 0);
        let start = Instant::now();
        for write in 0..records / 2 {
            let index = rng.below(records);
            let (key, value) = (key(index), value(records + write));
            loop {
                match engine.put(&key, &value) {
                    Ok(_) => break,
                    Err(error) => {
                        refused_writes += 1;
                        first_refusal.get_or_insert_with(|| error.to_string());
                        if refused_writes > 1_000 {
                            break;
                        }
                        let _ = engine.wait_for_compaction();
                    }
                }
            }
            if refused_writes > 1_000 {
                break;
            }
            model.insert(key, value);
            if write % 1024 == 0 {
                l0_max = l0_max.max(engine.stats().level0_flushes);
                let entries = engine.core.read_state().levels.outgrown_entries();
                allowances_max.0 = allowances_max.0.max(entries.0);
                allowances_max.1 = allowances_max.1.max(entries.1);
            }
        }
        let write_seconds = start.elapsed().as_secs_f64();
        // Drain: flush, then wait for passes until one ends with nothing
        // due or the same failure ends two in a row.
        let mut last = None;
        let mut drained = false;
        if sync {
            engine.flush().unwrap();
            let mut workspace = MergeWorkspace::new();
            let mut repeated = 0;
            loop {
                match engine.compact(jobs, &mut workspace) {
                    Ok(Some(_)) => repeated = 0,
                    Ok(None) => {
                        drained = true;
                        break;
                    }
                    Err(error) => {
                        let text = error.to_string();
                        repeated = if last.as_ref() == Some(&text) {
                            repeated + 1
                        } else {
                            0
                        };
                        last = Some(text);
                        if repeated == 3 {
                            break;
                        }
                    }
                }
                let entries = engine.core.read_state().levels.outgrown_entries();
                allowances_max.0 = allowances_max.0.max(entries.0);
                allowances_max.1 = allowances_max.1.max(entries.1);
            }
        }
        for _ in 0..(if sync { 0 } else { 50 }) {
            let _ = engine.flush();
            match engine.wait_for_compaction() {
                Ok(()) if engine.stats().frozen_tables == 0 => {
                    drained = true;
                    break;
                }
                Ok(()) => {}
                Err(error) => {
                    let text = error.to_string();
                    if last.as_ref() == Some(&text) {
                        break;
                    }
                    last = Some(text);
                }
            }
        }
        let stats = engine.stats();
        let status = engine.background_status().unwrap_or_default();
        let log = engine.core.step_log.lock().unwrap().take().unwrap();
        let mut errors = BTreeMap::<String, usize>::new();
        let mut first_error = None;
        let mut after_first = 0;
        let mut out = 0usize;
        for (at, step) in log.iter().enumerate() {
            if let Some(error) = &step.error {
                let name = format!("{error:?}");
                let name = name[..name.find([' ', '(', '{']).unwrap_or(name.len())].to_string();
                *errors.entry(name).or_default() += 1;
                first_error.get_or_insert(at);
            } else if step.kind.is_some() {
                out += step.output_bytes;
                if first_error.is_some() {
                    after_first += 1;
                }
            }
        }
        let merges: Vec<_> = log
            .iter()
            .filter(|step| step.kind == Some(CompactionKind::Merge))
            .collect();
        let largest_out = merges
            .iter()
            .map(|step| step.output_bytes + step.output_held)
            .max();
        let largest_in = merges
            .iter()
            .map(|step| step.input_bytes + step.input_held)
            .max();
        let ratio = merges
            .iter()
            .map(|step| step.output_bytes as f64 / step.input_bytes.max(1) as f64)
            .fold(0.0, f64::max);
        let most_inputs = merges
            .iter()
            .map(|step| step.level0_inputs + step.lower_inputs)
            .max();
        let mut by_kind = BTreeMap::<String, (usize, usize, usize, f64)>::new();
        for step in &log {
            let Some(kind) = step.kind else { continue };
            let entry = by_kind.entry(format!("{kind:?}")).or_default();
            entry.0 += 1;
            entry.1 += step.input_bytes >> 10;
            entry.2 += step.output_bytes >> 10;
            entry.3 = entry
                .3
                .max(step.output_bytes as f64 / step.input_bytes.max(1) as f64);
        }
        println!("GROWTH-KINDS | {label} | (steps, KiB in, KiB out, largest out/in) {by_kind:?} |");
        println!(
            "GROWTH-MERGES | {label} | merges {} | largest input {largest_in:?} output {largest_out:?} of {} | largest output/input {ratio:.2} | most inputs {most_inputs:?} |",
            merges.len(),
            jobs.max_output_bytes,
        );
        let (shape, debt) = shape_and_debt(&engine, &fit);
        let small: Vec<usize> = engine
            .core
            .read_state()
            .levels
            .runs()
            .iter()
            .map(|runs| {
                runs.iter()
                    .filter(|r| r.run.as_bytes().len() < run / 8)
                    .count()
            })
            .collect();
        println!(
            "GROWTH-ALLOWANCES | {label} | (allowance, output that set it, level, source bytes, smallest run below) {:?} of fit {} |",
            engine.core.read_state().levels.allowances(),
            fit.max_bytes,
        );
        let entries = engine.core.read_state().levels.outgrown_entries();
        let correct = contents(&engine) == model;
        println!(
            "GROWTH | {label} | built {} ({:.1} MiB) recovered {recovered} | writes {} in {write_seconds:.1} s, refused {refused_writes} first {first_refusal:?} | L0 max {l0_max} waits {} ({:.2} s) |",
            before.0,
            before.1 as f64 / 1048576.0,
            records / 2,
            engine.core.level0_waits.load(Ordering::SeqCst),
            engine.core.level0_wait_nanos.load(Ordering::SeqCst) as f64 / 1e9,
        );
        println!(
            "GROWTH-JOBS | {label} | steps {} after first failure {after_first} | failed {errors:?} built-then-overflowed {} | passes {} failures {} last {:?} | allowances max live {} retired {} at end {entries:?} | WA {:.2} | drained {drained} {last:?} |",
            log.len(),
            engine.core.overflows.load(Ordering::SeqCst),
            status.passes,
            status.failures,
            status.last_failure,
            allowances_max.0,
            allowances_max.1,
            (stats.flushed_bytes as f64 + out as f64) / (stats.flushed_bytes as f64).max(1.0),
        );
        println!(
            "GROWTH-END | {label} | {shape} debt {:.1} MiB | runs under 1/8 target per level {small:?} | stored {:.1} MiB | contents match model {correct} |",
            debt as f64 / 1048576.0,
            (stats.run_bytes + stats.held_bytes) as f64 / 1048576.0,
        );
        assert!(correct, "{label}");
        engine.close().unwrap();
    }
}

/// What a full checkpoint costs as the database grows: the time and bytes
/// of a checkpoint of `n` records after a drain, of the next after 1% of
/// them are overwritten (every checkpoint rewrites every run), and of
/// recovery. Sizes from `ALPHA_CHECKPOINT_SIZES` (`250000,1000000`), text
/// values of 100 bytes, runs of `ALPHA_RUN_KIB` (1024). The directory is
/// under the temporary directory, a tmpfs in the bounded environment, so
/// the times include no device writes or flushes to stable storage.
#[test]
#[ignore = "research: run with --release --ignored --nocapture"]
fn alpha_checkpoint_costs() {
    let run = env("ALPHA_RUN_KIB", 1024) as usize * 1024;
    let sizes: Vec<u64> = std::env::var("ALPHA_CHECKPOINT_SIZES")
        .unwrap_or_else(|_| "250000,1000000".into())
        .split(',')
        .filter_map(|size| size.parse().ok())
        .collect();
    let jobs = CompactionOptions {
        max_input_bytes: 4 * run,
        max_output_bytes: 4 * run,
        ..CompactionOptions::default()
    };
    let options = EngineOptions {
        memtable_budget: run,
        target_run_len: run,
        max_runs: usize::MAX / 4,
        max_run_bytes: usize::MAX / 4,
        background_compaction: Some(jobs),
        ..EngineOptions::default()
    };
    for n in sizes {
        let scratch = Scratch::new("checkpoint");
        let engine = Engine::new(options).unwrap();
        let mut rng = Rng(7);
        for write in 0..n {
            let index = index(Workload::UniformInsert, write, n, &mut rng);
            while engine.put(&key(index), &text(write, 100)).is_err() {
                let _ = engine.wait_for_compaction();
            }
        }
        engine.flush().unwrap();
        engine.wait_for_compaction().unwrap();
        let checkpoint = |engine: &Engine| {
            let start = Instant::now();
            let report = engine
                .checkpoint(&scratch.0, CheckpointOptions::default())
                .unwrap();
            (start.elapsed().as_secs_f64(), report)
        };
        let (first, report) = checkpoint(&engine);
        for write in 0..n / 100 {
            let index = rng.below(n);
            engine.put(&key(index), &text(n + write, 100)).unwrap();
        }
        let (second, again) = checkpoint(&engine);
        let stats = engine.stats();
        engine.close().unwrap();
        let start = Instant::now();
        let recovered = Engine::recover(&scratch.0, options).unwrap();
        let recovery = start.elapsed().as_secs_f64();
        let records = recovered.stats().last_seq;
        recovered.close().unwrap();
        println!(
            "CHECKPOINT | {n} records, {:.1} MiB stored in {} runs | first {first:.3} s {:.1} MiB {} runs | after 1% overwrites {second:.3} s {:.1} MiB {} runs (memtable {:.1} MiB) | recovery {recovery:.3} s (last seq {records}) | {:.0} MiB/s written |",
            (stats.run_bytes + stats.held_bytes) as f64 / 1048576.0,
            stats.runs,
            report.bytes as f64 / 1048576.0,
            report.runs,
            again.bytes as f64 / 1048576.0,
            again.runs,
            stats.active.charged as f64 / 1048576.0,
            again.bytes as f64 / 1048576.0 / second,
        );
    }
}
