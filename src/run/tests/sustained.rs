//! Background compaction under sustained writes (5AH): one paced,
//! deterministic workload through the engine, sampled while it writes, while
//! compaction settles, and while the same reads continue afterwards.
//!
//! An ignored measurement, not a check. Run it in release, one at a time:
//!
//! ```text
//! cargo test --release --lib run::tests::sustained -- --ignored --nocapture
//! ```
//!
//! `SUSTAIN_RATE` writes per second (5,000) for `SUSTAIN_SECS` (180), then
//! `SUSTAIN_READ_SECS` (30) of the same reads at the same pace without the
//! writes, on `json:SUSTAIN_GROUPS` (64 groups of 16,384 keys).
//! `SUSTAIN_OVERLAP` is the worker's `max_overlap` (4, the default),
//! `SUSTAIN_BLOCK` the block length (4 KiB), and `SUSTAIN_PLAIN` selects
//! plain Zstandard instead of dictionaries. `SUSTAIN_READS=0` writes
//! without reading during the active phase, to follow many compaction
//! cycles quickly: the policy depends on the writes, not on the time.
//! `SUSTAIN_APPEND=1` loads the lowest quarter of the keys and then inserts
//! the rest in ascending key order, so that flushed runs never overlap;
//! with `SUSTAIN_UPDATES` (0) percent of the writes updating a random
//! inserted key instead. `SUSTAIN_HOT=1` puts nine writes in ten within the
//! lowest eighth of the keys.

use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use crate::engine::{CompactionOptions, Compression, Engine, EngineOptions, EngineWorkspace};
use crate::run::MergeWorkspace;

use super::costs::{Rng, compare};

const GROUP: u64 = 16_384;

fn env(name: &str, default: u64) -> u64 {
    std::env::var(name).map_or(default, |value| value.parse().unwrap())
}

fn mib(bytes: impl Into<u64>) -> f64 {
    bytes.into() as f64 / f64::from(1 << 20)
}

/// CPU seconds of the threads named `name`, from `/proc`; 0 elsewhere.
fn thread_cpu(name: &str) -> f64 {
    let Ok(tasks) = std::fs::read_dir("/proc/self/task") else {
        return 0.0;
    };
    tasks
        .filter_map(|task| {
            let path = task.ok()?.path();
            let comm = std::fs::read_to_string(path.join("comm")).ok()?;
            // Linux keeps the first 15 bytes of a thread's name.
            name.starts_with(comm.trim()).then_some(())?;
            let stat = std::fs::read_to_string(path.join("stat")).ok()?;
            let fields: Vec<&str> = stat.rsplit_once(')')?.1.split_whitespace().collect();
            let user: f64 = fields.get(11)?.parse().ok()?;
            let system: f64 = fields.get(12)?.parse().ok()?;
            Some((user + system) / 100.0)
        })
        .sum()
}

/// A line of `/proc/self/status` in MiB, such as `VmRSS` or `VmHWM`.
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

#[derive(Clone, Copy, Debug, Default)]
struct Sample {
    at: f64,
    runs: usize,
    overlap: usize,
    dictionary_runs: usize,
    dictionary_bytes: usize,
    prepared: usize,
    run_bytes: usize,
    memtables: usize,
    flushes: u64,
    flushed_bytes: u64,
    busy: bool,
    compactions: u64,
    failures: u64,
    input_bytes: u64,
    output_bytes: u64,
    worker_cpu: f64,
    rss: f64,
}

fn sample(engine: &Engine, start: Instant) -> Sample {
    let (dictionary_runs, prepared, _, dictionary_bytes) = engine.dictionary_counts();
    let stats = engine.stats();
    let status = engine.background_status().unwrap();
    Sample {
        at: start.elapsed().as_secs_f64(),
        runs: stats.runs,
        overlap: stats.max_overlap,
        dictionary_runs,
        dictionary_bytes,
        prepared,
        run_bytes: stats.run_bytes,
        memtables: stats.active.charged + stats.frozen.map_or(0, |frozen| frozen.charged),
        flushes: stats.flushes,
        flushed_bytes: stats.flushed_bytes,
        busy: status.busy,
        compactions: status.compactions,
        failures: status.failures,
        input_bytes: status.input_bytes,
        output_bytes: status.output_bytes,
        worker_cpu: thread_cpu("tessera-compaction"),
        rss: status_mib("VmRSS"),
    }
}

fn row(label: &str, s: &Sample) {
    println!(
        "| {label} | {:.1} | {} | {} | {} | {:.1} | {:.1} | {} | {:.1} | {} | {} | {} | {:.1} -> {:.1} | {:.1} | {:.0} |",
        s.at,
        s.runs,
        s.overlap,
        s.dictionary_runs,
        mib(s.dictionary_bytes as u64),
        mib((s.run_bytes - s.dictionary_bytes) as u64),
        s.prepared,
        mib(s.memtables as u64),
        if s.busy { "busy" } else { "idle" },
        s.compactions,
        s.failures,
        mib(s.input_bytes),
        mib(s.output_bytes),
        s.worker_cpu,
        s.rss,
    );
}

fn percentiles(samples: &mut [u64]) -> String {
    samples.sort_unstable();
    let at = |q: usize| samples[(samples.len() * q / 100).min(samples.len() - 1)] as f64 / 1e3;
    format!("p50 {:.2} µs, p99 {:.2} µs", at(50), at(99))
}

/// The paced operations and what they measured. Each step writes once,
/// unless writing is off, then looks up four uniform keys, checking each
/// value, and scans one short prefix of about 30 records.
struct Workload {
    groups: u64,
    rng: Rng,
    /// Each key's version, 0 while it is absent.
    versions: Vec<u32>,
    workspace: EngineWorkspace,
    /// Whether steps read as well as write.
    reads: bool,
    /// The next key to insert, when writes append in key order instead.
    append: Option<u64>,
    /// When appending, the percentage of writes that update an inserted key.
    updates_share: u64,
    /// Whether nine writes in ten fall within the lowest eighth of the keys.
    hot: bool,
    puts: Vec<u64>,
    gets: Vec<u64>,
    scans: Vec<u64>,
    inserts: u64,
    updates: u64,
    deletes: u64,
    logical: u64,
    lag: Duration,
}

impl Workload {
    fn key(&self, index: u64) -> [u8; 8] {
        compare::key(self.groups, index)
    }

    /// `steps` steps at `rate` per second, numbered from `first`.
    fn run(&mut self, engine: &Engine, rate: u64, first: u64, steps: u64, write: bool) -> Duration {
        for laps in [&mut self.puts, &mut self.gets, &mut self.scans] {
            laps.clear();
        }
        self.lag = Duration::ZERO;
        let begin = Instant::now();
        for step in 0..steps {
            let due = Duration::from_secs_f64(step as f64 / rate as f64);
            let now = begin.elapsed();
            if now + Duration::from_millis(1) < due {
                std::thread::sleep(due - now);
            } else if now > due {
                self.lag = self.lag.max(now - due);
            }
            if write {
                self.write(engine, first + step);
            }
            if self.reads || !write {
                self.read(engine);
            }
        }
        begin.elapsed()
    }

    /// An update, or 15% of the time a delete, of a random key; an insert if
    /// the key is absent.
    fn write(&mut self, engine: &Engine, step: u64) {
        let records = self.versions.len() as u64;
        let index = match &mut self.append {
            Some(next) if self.rng.below(100) < self.updates_share => self.rng.below(*next),
            Some(next) => {
                *next += 1;
                *next - 1
            }
            None if self.hot && self.rng.below(10) < 9 => self.rng.below(records / 8),
            None => self.rng.below(records),
        };
        let key = self.key(index);
        let present = self.versions[index as usize] != 0;
        if present && self.append.is_none() && self.rng.below(100) < 15 {
            let begin = Instant::now();
            engine.delete(&key).unwrap();
            self.puts.push(begin.elapsed().as_nanos() as u64);
            self.versions[index as usize] = 0;
            self.deletes += 1;
            self.logical += key.len() as u64;
        } else {
            let version = 2 + step as u32;
            let value = compare::json(index, u64::from(version));
            let begin = Instant::now();
            engine.put(&key, &value).unwrap();
            self.puts.push(begin.elapsed().as_nanos() as u64);
            self.versions[index as usize] = version;
            *if present {
                &mut self.updates
            } else {
                &mut self.inserts
            } += 1;
            self.logical += (key.len() + value.len()) as u64;
        }
    }

    fn read(&mut self, engine: &Engine) {
        for _ in 0..4 {
            let index = self.rng.below(self.versions.len() as u64);
            let version = self.versions[index as usize];
            let expected = (version != 0).then(|| compare::json(index, u64::from(version)));
            let key = self.key(index);
            let begin = Instant::now();
            let found = engine.get(&key, &mut self.workspace).unwrap();
            self.gets.push(begin.elapsed().as_nanos() as u64);
            assert_eq!(found.map(|found| found.value.to_vec()), expected, "{index}");
        }
        let index = self.rng.below(self.versions.len() as u64);
        let prefix = self.key(index);
        let begin = Instant::now();
        let guard = engine.read();
        let mut scan = guard.scan_prefix(&prefix[..6], &mut self.workspace);
        let mut found = 0;
        while scan.next_record().unwrap().is_some() {
            found += 1;
        }
        drop(scan);
        drop(guard);
        self.scans.push(begin.elapsed().as_nanos() as u64);
        std::hint::black_box(found);
    }

    fn report(&mut self, label: &str, elapsed: Duration, rate: u64) {
        let steps = self.puts.len().max(self.scans.len());
        let laps = |laps: &mut Vec<u64>| {
            if laps.is_empty() {
                String::from("-")
            } else {
                percentiles(laps)
            }
        };
        println!(
            "{label}\tput {}\tget {}\tshort scan {}\t{:.0} steps/s over {:.1} s (target {rate}/s, worst lag {:.1} ms)",
            laps(&mut self.puts),
            laps(&mut self.gets),
            laps(&mut self.scans),
            steps as f64 / elapsed.as_secs_f64(),
            elapsed.as_secs_f64(),
            self.lag.as_secs_f64() * 1e3,
        );
    }
}

#[test]
#[ignore = "a measurement: run in release with --ignored --nocapture"]
fn sustained_compaction() {
    let groups = env("SUSTAIN_GROUPS", 64);
    let rate = env("SUSTAIN_RATE", 5_000);
    let secs = env("SUSTAIN_SECS", 180);
    let read_secs = env("SUSTAIN_READ_SECS", 30);
    let max_overlap = env("SUSTAIN_OVERLAP", 4) as usize;
    let block = env("SUSTAIN_BLOCK", 4 << 10) as usize;
    let compression = if std::env::var("SUSTAIN_PLAIN").is_ok() {
        Compression::Zstd
    } else {
        Compression::ZstdDictionary
    };
    let records = groups * GROUP;
    println!(
        "config\trate={rate}\tsecs={secs}\tread_secs={read_secs}\tmax_overlap={max_overlap}\tblock={block}\t{compression:?}\trecords={records}"
    );

    let engine = Engine::new(EngineOptions {
        max_runs: 256,
        background_compaction: Some(CompactionOptions {
            max_overlap,
            ..CompactionOptions::default()
        }),
        compression,
        target_block_len: block,
        ..EngineOptions::default()
    });

    // Seven keys in eight, in a fixed random order, consolidated as the
    // comparison harness consolidates its load; when appending, the lowest
    // quarter of the keys instead.
    let mut load = Workload {
        groups,
        rng: Rng::new(50_000),
        versions: vec![0; records as usize],
        workspace: EngineWorkspace::new(),
        reads: env("SUSTAIN_READS", 1) != 0,
        append: (env("SUSTAIN_APPEND", 0) != 0).then_some(records / 4),
        updates_share: env("SUSTAIN_UPDATES", 0),
        hot: env("SUSTAIN_HOT", 0) != 0,
        puts: Vec::new(),
        gets: Vec::new(),
        scans: Vec::new(),
        inserts: 0,
        updates: 0,
        deletes: 0,
        logical: 0,
        lag: Duration::ZERO,
    };
    for j in 0..records {
        let index = (j.wrapping_mul(0x9E37_79B1).wrapping_add(0x7F4A_7C15)) & (records - 1);
        let loaded = match load.append {
            Some(first) => index < first,
            None => index % 8 != 7,
        };
        if loaded {
            engine
                .put(&load.key(index), &compare::json(index, 1))
                .unwrap();
            load.versions[index as usize] = 1;
        }
    }
    engine.flush().unwrap();
    engine.wait_for_compaction().unwrap();
    let consolidate = CompactionOptions {
        max_overlap: 1,
        max_inputs: usize::MAX,
        max_input_bytes: usize::MAX,
        max_output_bytes: usize::MAX,
        ..CompactionOptions::default()
    };
    let mut merge = MergeWorkspace::new();
    while engine.compact(consolidate, &mut merge).unwrap().is_some() {}
    engine.wait_for_compaction().unwrap();
    drop(merge);

    println!(
        "| phase | s | runs | overlap | dict runs | dict MiB | plain MiB | prepared | memtables MiB | worker | compactions | failures | rewritten MiB | worker CPU s | RSS MiB |"
    );
    println!("|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|");
    if let Some(first) = load.append {
        assert!(rate * secs <= records - first, "appends past the last key");
    }
    let start = Instant::now();
    let base = sample(&engine, start);
    row("base", &base);

    // The same reads before any write, as a baseline in this state.
    let before = load.run(&engine, rate, 0, rate * read_secs, false);
    load.report("reads before", before, rate);

    let samples = Mutex::new(Vec::new());
    let stop = AtomicBool::new(false);
    let start = Instant::now();
    let (active, settle) = std::thread::scope(|scope| {
        scope.spawn(|| {
            while !stop.load(Ordering::Relaxed) {
                let s = sample(&engine, start);
                samples.lock().unwrap().push(s);
                std::thread::sleep(Duration::from_millis(50));
            }
        });
        let active = load.run(&engine, rate, 0, rate * secs, true);
        let settling = Instant::now();
        engine.wait_for_compaction().unwrap();
        let settle = settling.elapsed();
        // A last sample once idle.
        std::thread::sleep(Duration::from_millis(100));
        stop.store(true, Ordering::Relaxed);
        (active, settle)
    });
    let base = Sample { at: 0.0, ..base };
    let samples = samples.into_inner().unwrap();
    let end = *samples.last().unwrap();
    let active_samples: Vec<&Sample> = samples
        .iter()
        .filter(|s| s.at <= active.as_secs_f64())
        .collect();
    let active_end = **active_samples.last().unwrap();

    // The timeline: every change of runs, overlap, compactions, failures or
    // the worker's state.
    let mut previous = base;
    for s in &samples {
        let shape = |s: &Sample| (s.runs, s.overlap, s.compactions, s.busy, s.failures);
        if shape(s) != shape(&previous) {
            let label = if s.at <= active.as_secs_f64() {
                "active"
            } else {
                "settle"
            };
            row(label, s);
            previous = *s;
        }
    }
    row("active end", &active_end);
    row("settled", &end);
    load.report("active", active, rate);

    // Busy periods, and how long after the overlap first exceeded the
    // allowance the worker started each.
    let (mut busy_periods, mut waits) = (Vec::new(), Vec::new());
    let (mut began, mut eligible): (Option<f64>, Option<f64>) = (None, None);
    for s in &samples {
        if s.overlap > max_overlap && eligible.is_none() && !s.busy && began.is_none() {
            eligible = Some(s.at);
        }
        match (s.busy, began) {
            (true, None) => {
                began = Some(s.at);
                if let Some(at) = eligible.take() {
                    waits.push(s.at - at);
                }
            }
            (false, Some(at)) => {
                busy_periods.push(s.at - at);
                began = None;
            }
            _ => {}
        }
    }
    let busy_share =
        active_samples.iter().filter(|s| s.busy).count() as f64 / active_samples.len() as f64;
    let writes = load.inserts + load.updates + load.deletes;
    let flushed = end.flushed_bytes - base.flushed_bytes;
    let (rewritten_in, rewritten_out) = (
        end.input_bytes - base.input_bytes,
        end.output_bytes - base.output_bytes,
    );
    println!(
        "writes\t{writes} ({} inserts, {} updates, {} deletes)\t{:.0}/s\tlogical {:.1} MiB",
        load.inserts,
        load.updates,
        load.deletes,
        writes as f64 / active.as_secs_f64(),
        mib(load.logical),
    );
    println!(
        "amplification\tflushed {:.1} MiB in {} flushes, compacted {:.1} -> {:.1} MiB in {} compactions\tstored bytes written per logical byte: flush {:.2}, compaction {:.2}, total {:.2}",
        mib(flushed),
        end.flushes - base.flushes,
        mib(rewritten_in),
        mib(rewritten_out),
        end.compactions - base.compactions,
        flushed as f64 / load.logical as f64,
        rewritten_out as f64 / load.logical as f64,
        (flushed + rewritten_out) as f64 / load.logical as f64,
    );
    println!(
        "worker\tbusy {:.0}% of the active phase\tCPU {:.1} s active, {:.1} s settling\tbusy periods {:?} s\tstarted after eligible {:?} s\tfailures {}\tsettle {:.2} s",
        busy_share * 100.0,
        active_end.worker_cpu - base.worker_cpu,
        end.worker_cpu - active_end.worker_cpu,
        busy_periods
            .iter()
            .map(|p| (p * 10.0).round() / 10.0)
            .collect::<Vec<_>>(),
        waits
            .iter()
            .map(|w| (w * 100.0).round() / 100.0)
            .collect::<Vec<_>>(),
        end.failures - base.failures,
        settle.as_secs_f64(),
    );
    let mean = |f: fn(&Sample) -> f64| {
        active_samples.iter().map(|s| f(s)).sum::<f64>() / active_samples.len() as f64
    };
    println!(
        "memory\tactive mean: runs {:.1}, overlap {:.1}, run MiB {:.1} ({:.0}% with a dictionary), memtables MiB {:.1}, RSS {:.0}\tsettled: runs {}, run MiB {:.1} ({:.0}% with a dictionary), RSS {:.0}\tpeak RSS sampled {:.0}, process high-water {:.0}",
        mean(|s| s.runs as f64),
        mean(|s| s.overlap as f64),
        mean(|s| mib(s.run_bytes as u64)),
        mean(|s| s.dictionary_bytes as f64 / s.run_bytes as f64) * 100.0,
        mean(|s| mib(s.memtables as u64)),
        mean(|s| s.rss),
        end.runs,
        mib(end.run_bytes as u64),
        end.dictionary_bytes as f64 / end.run_bytes as f64 * 100.0,
        end.rss,
        samples.iter().map(|s| s.rss).fold(0.0, f64::max),
        status_mib("VmHWM"),
    );

    // The same reads once compaction has settled.
    let after = load.run(&engine, rate, 0, rate * read_secs, false);
    load.report("reads after", after, rate);

    // Every live record, in key order, and nothing else.
    let mut live: Vec<u64> = (0..records)
        .filter(|&index| load.versions[index as usize] != 0)
        .collect();
    live.sort_by_key(|&index| load.key(index));
    let mut workspace = EngineWorkspace::new();
    let guard = engine.read();
    let mut scan = guard.scan(&mut workspace);
    let mut checked = 0usize;
    while let Some(record) = scan.next_record().unwrap() {
        let index = live[checked];
        assert_eq!(record.key, load.key(index));
        let version = u64::from(load.versions[index as usize]);
        assert_eq!(record.value, compare::json(index, version));
        checked += 1;
    }
    assert_eq!(checked, live.len());
    println!("full scan\t{checked} records verified");
}
