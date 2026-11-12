//! Tessera against RocksDB on identical data and operations: the 5Y
//! baseline in `docs/benchmarks.md`.
//!
//! ```text
//! compare ENGINE DATASET DIR
//! ```
//!
//! ENGINE is `tessera`, `tessera-lz4`, `rocksdb`, `rocksdb-lz4` or
//! `rocksdb-tuned`. DATASET is `json:GROUPS` or `sensor:GROUPS`, with a
//! power of two of at most 256 groups of 16,384 records. DIR is where
//! RocksDB keeps its files, a tmpfs. One engine per process, so resident
//! memory is its own.
//!
//! Each run loads the records in a fixed random order with background
//! compaction, consolidates them, then overwrites one key in 16 and deletes
//! another one in 16 and flushes: the *overlapping* state, a consolidated
//! base under the newer runs or files of the updates. It then consolidates
//! again: the *consolidated* state. Reads are timed in both states with no
//! background work, then repeated untimed with counters and checked against
//! the generator, which holds no copy of the data, followed by a full
//! ordered scan checked record by record. Output is tab-separated lines for
//! a summary script.

mod rocks;

use std::hint::black_box;
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering::Relaxed};
use std::time::{Duration, Instant};

use tessera::engine::{
    CompactionOptions, Compression, Engine, EngineOptions, EngineWorkspace, MergeWorkspace,
};

// Data.

/// Deterministic xorshift generator, as in Tessera's benchmark.
struct Rng(u64);

impl Rng {
    fn new(seed: u64) -> Self {
        Self(seed.wrapping_mul(0x9e37_79b9_7f4a_7c15) | 1)
    }

    fn below(&mut self, bound: u64) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0 % bound
    }
}

const WORDS: [&str; 32] = [
    "alpha", "bravo", "charlie", "delta", "echo", "foxtrot", "golf", "hotel", "india", "juliet",
    "kilo", "lima", "mike", "november", "oscar", "papa", "quebec", "romeo", "sierra", "tango",
    "uniform", "victor", "whiskey", "xray", "yankee", "zulu", "red", "green", "blue", "amber",
    "north", "south",
];

fn words(rng: &mut Rng, count: u64) -> String {
    (0..count)
        .map(|_| WORDS[rng.below(32) as usize])
        .collect::<Vec<_>>()
        .join(" ")
}

#[derive(Clone, Copy, Debug)]
enum Payload {
    /// A user profile, as the benchmark's `json_record`: about 200 bytes to
    /// 1 KiB of JSON.
    Json,
    /// A sensor reading of 14 to 32 bytes, mostly binary.
    Sensor,
}

/// The value of record `index` at `version`.
fn value(payload: Payload, index: u64, version: u64) -> Vec<u8> {
    let seed = index + (version << 40);
    match payload {
        Payload::Json => {
            let mut rng = Rng::new(2_000 + seed);
            let (first, last) = (WORDS[rng.below(32) as usize], WORDS[rng.below(32) as usize]);
            let bio_words = rng.below(120);
            format!(
                r#"{{"id":{index},"name":"{first} {last}","email":"{first}.{last}{}@example.com","active":{},"score":{},"plan":"{}","tags":["{}","{}"],"created":"2026-{:02}-{:02}T{:02}:{:02}:00Z","bio":"{}"}}"#,
                rng.below(100),
                rng.below(2) == 0,
                rng.below(100_000),
                ["free", "pro", "team"][rng.below(3) as usize],
                WORDS[rng.below(32) as usize],
                WORDS[rng.below(32) as usize],
                1 + rng.below(12),
                1 + rng.below(28),
                rng.below(24),
                rng.below(60),
                words(&mut rng, bio_words),
            )
            .into_bytes()
        }
        Payload::Sensor => {
            let mut rng = Rng::new(1_000 + seed);
            let mut value = Vec::with_capacity(32);
            value.extend_from_slice(&(1_700_000_000_000 + index * 250 + version).to_be_bytes());
            value.extend_from_slice(&((index % 1_000) as u32).to_be_bytes());
            value.extend_from_slice(&[rng.below(4) as u8, 0]);
            let extra = rng.below(19);
            value.extend((0..extra).map(|_| rng.below(256) as u8));
            value
        }
    }
}

/// Records per group.
const GROUP: u64 = 16_384;

#[derive(Clone, Copy, Debug)]
struct Dataset {
    payload: Payload,
    groups: u64,
}

impl Dataset {
    fn parse(text: &str) -> Self {
        let (payload, groups) = text.split_once(':').expect("DATASET is PAYLOAD:GROUPS");
        let payload = match payload {
            "json" => Payload::Json,
            "sensor" => Payload::Sensor,
            other => panic!("unknown payload {other}"),
        };
        let groups: u64 = groups.parse().expect("GROUPS is a number");
        assert!(
            groups.is_power_of_two() && (2..=256).contains(&groups),
            "GROUPS is a power of two from 2 to 256"
        );
        Self { payload, groups }
    }

    fn records(self) -> u64 {
        self.groups * GROUP
    }

    /// The scan layout of Tessera's benchmark, 8 bytes: a 2-byte group
    /// (`[g, 0x00]` or `[g, 0xFF]` alternately, the last `[0xFF, 0xFF]`),
    /// 32 subgroups of 512 (2 bytes), 16 leaves of 32 (`[0x00, n]`) and a
    /// place (`[n, 0xFF]`). Prefixes of 6, 4, 3 and 2 bytes select 32, 512,
    /// 2,048 and 16,384 consecutive records.
    fn key(self, index: u64) -> [u8; 8] {
        let (group, rest) = (index / GROUP, index % GROUP);
        let (sub, rest) = (rest / 512, rest % 512);
        let (leaf, place) = (rest / 32, rest % 32);
        let head = if group == self.groups - 1 {
            [0xFF, 0xFF]
        } else {
            [group as u8, if group % 2 == 1 { 0xFF } else { 0x00 }]
        };
        [
            head[0],
            head[1],
            (sub / 4) as u8,
            (sub % 4 * 0x40) as u8,
            0x00,
            leaf as u8,
            place as u8,
            0xFF,
        ]
    }

    /// A key between two stored keys, inside the range.
    fn absent(self, index: u64) -> [u8; 8] {
        let mut key = self.key(index);
        key[7] = 0x7F;
        key
    }

    /// The `j`th record of a fixed permutation, by an odd multiplier modulo
    /// the power-of-two record count.
    fn shuffled(self, j: u64, multiplier: u64) -> u64 {
        (j.wrapping_mul(multiplier).wrapping_add(0x7F4A_7C15)) & (self.records() - 1)
    }
}

/// The version of record `index`, or `None` once deleted: after the
/// updates, one in 16 is overwritten and another one in 16 deleted.
fn version(index: u64, updated: bool) -> Option<u64> {
    match (updated, index % 16) {
        (true, 0) => Some(1),
        (true, 8) => None,
        _ => Some(0),
    }
}

/// The smallest key above every key starting with `prefix`, if any.
fn successor(prefix: &[u8]) -> Option<Vec<u8>> {
    let mut bound = prefix.to_vec();
    while bound.last() == Some(&0xFF) {
        bound.pop();
    }
    *bound.last_mut()? += 1;
    Some(bound)
}

// Engines.

/// Cumulative read counters; `None` where an engine does not report one.
#[derive(Clone, Copy, Default)]
struct Counters {
    blocks: u64,
    filter_checks: Option<u64>,
    filter_positives: Option<u64>,
    skipped: Option<u64>,
    deletes_skipped: Option<u64>,
    child_seeks: Option<u64>,
}

trait Store {
    fn put(&mut self, key: &[u8], value: &[u8]);
    fn delete(&mut self, key: &[u8]);
    fn flush(&mut self);
    /// Waits until no flush or compaction runs or is due.
    fn settle(&mut self);
    /// Flushes, then compacts until no key is in more than one run or file.
    fn consolidate(&mut self);
    fn get<T>(&mut self, key: &[u8], found: impl FnOnce(Option<&[u8]>) -> T) -> T;
    fn scan(&mut self, prefix: &[u8], visit: impl FnMut(&[u8], &[u8]));
    fn counting(&mut self, on: bool);
    fn counters(&self) -> Counters;
    /// One line of the engine's own accounting.
    fn report(&self) -> String;
    /// The decoded-block workspace a reader holds, where known.
    fn workspace_bytes(&self) -> Option<usize>;
    /// Times gets of `keys` through one read held for all of them, where
    /// the engine has such a read; the nanoseconds of each.
    fn held_gets(&mut self, _keys: &[[u8; 8]]) -> Option<Vec<u64>> {
        None
    }
    /// The directory whose files hold the data, if any.
    fn dir(&self) -> Option<PathBuf>;
    /// The engine's own breakdown of its stored bytes, where it gives one.
    fn details(&self) -> Option<String> {
        None
    }
}

struct Tessera {
    engine: Engine,
    workspace: EngineWorkspace,
}

impl Tessera {
    fn new(compression: Compression) -> Self {
        let engine = Engine::new(EngineOptions {
            // Above the provisional 64, which the largest dataset's load
            // would approach; RocksDB has no such limit.
            max_runs: 256,
            background_compaction: Some(CompactionOptions::default()),
            compression,
            ..EngineOptions::default()
        });
        Self {
            engine,
            workspace: EngineWorkspace::new(),
        }
    }
}

impl Store for Tessera {
    fn put(&mut self, key: &[u8], value: &[u8]) {
        self.engine.put(key, value).unwrap();
    }

    fn delete(&mut self, key: &[u8]) {
        self.engine.delete(key).unwrap();
    }

    fn flush(&mut self) {
        self.engine.flush().unwrap();
    }

    fn settle(&mut self) {
        self.engine.wait_for_compaction().unwrap();
    }

    fn consolidate(&mut self) {
        self.flush();
        self.settle();
        let options = CompactionOptions {
            max_overlap: 1,
            max_inputs: usize::MAX,
            max_input_bytes: usize::MAX,
            max_output_bytes: usize::MAX,
            ..CompactionOptions::default()
        };
        let mut workspace = MergeWorkspace::new();
        while self
            .engine
            .compact(options, &mut workspace)
            .unwrap()
            .is_some()
        {}
        self.settle();
    }

    fn get<T>(&mut self, key: &[u8], found: impl FnOnce(Option<&[u8]>) -> T) -> T {
        let value = self.engine.get(key, &mut self.workspace).unwrap();
        found(value.map(|value| value.value))
    }

    fn scan(&mut self, prefix: &[u8], mut visit: impl FnMut(&[u8], &[u8])) {
        let guard = self.engine.read();
        let mut scan = guard.scan_prefix(prefix, &mut self.workspace);
        while let Some(record) = scan.next_record().unwrap() {
            visit(record.key, record.value);
        }
    }

    fn counting(&mut self, _on: bool) {}

    fn counters(&self) -> Counters {
        Counters {
            blocks: self.workspace.blocks_loaded(),
            ..Counters::default()
        }
    }

    fn report(&self) -> String {
        let stats = self.engine.stats();
        let background = self.engine.background_status().unwrap_or_default();
        let memtables = stats.active.charged + stats.frozen.map_or(0, |frozen| frozen.charged);
        format!(
            "runs={}\tmax_overlap={}\trun_mib={:.2}\tfilter_mib={:.2}\tmemtable_mib={:.2}\tretained_mib={:.2}\tflushes={}\tflushed_mib={:.1}\tcompactions={}\tcompacted_in_mib={:.1}\tcompacted_out_mib={:.1}\tfailures={}",
            stats.runs,
            stats.max_overlap,
            mib(stats.run_bytes as u64),
            mib(stats.filter_bytes as u64),
            mib(memtables as u64),
            mib(stats.retained_bytes as u64),
            stats.flushes,
            mib(stats.flushed_bytes),
            background.compactions,
            mib(background.input_bytes),
            mib(background.output_bytes),
            background.failures,
        )
    }

    fn workspace_bytes(&self) -> Option<usize> {
        Some(self.workspace.capacity())
    }

    /// Through a read guard, which copies no run pointers per lookup.
    fn held_gets(&mut self, keys: &[[u8; 8]]) -> Option<Vec<u64>> {
        let guard = self.engine.read();
        Some(
            keys.iter()
                .map(|key| {
                    let start = Instant::now();
                    let found = guard.get(key, &mut self.workspace).unwrap();
                    black_box(found.map(|found| found.value.len()));
                    start.elapsed().as_nanos() as u64
                })
                .collect(),
        )
    }

    fn dir(&self) -> Option<PathBuf> {
        None
    }
}

struct Rocks {
    db: rocks::Db,
    dir: PathBuf,
}

impl Store for Rocks {
    fn put(&mut self, key: &[u8], value: &[u8]) {
        self.db.put(key, value);
    }

    fn delete(&mut self, key: &[u8]) {
        self.db.delete(key);
    }

    fn flush(&mut self) {
        self.db.flush();
    }

    fn settle(&mut self) {
        // Twice in a row, since a finished flush can schedule a compaction.
        let mut quiet = 0;
        while quiet < 2 {
            quiet = if self.db.idle() { quiet + 1 } else { 0 };
            std::thread::sleep(Duration::from_millis(10));
        }
    }

    fn consolidate(&mut self) {
        self.flush();
        self.settle();
        self.db.compact_all();
        self.settle();
    }

    fn get<T>(&mut self, key: &[u8], found: impl FnOnce(Option<&[u8]>) -> T) -> T {
        self.db.get(key, found)
    }

    fn scan(&mut self, prefix: &[u8], visit: impl FnMut(&[u8], &[u8])) {
        self.db.scan(prefix, visit);
    }

    fn counting(&mut self, on: bool) {
        self.db.counting(on);
    }

    fn counters(&self) -> Counters {
        use rocks::metric::*;
        let hits = self.db.metric(BLOOM_SST_HIT);
        Counters {
            blocks: self.db.metric(BLOCK_READ_COUNT),
            filter_checks: Some(hits + self.db.metric(BLOOM_SST_MISS)),
            filter_positives: Some(hits),
            skipped: Some(self.db.metric(INTERNAL_KEY_SKIPPED)),
            deletes_skipped: Some(self.db.metric(INTERNAL_DELETE_SKIPPED)),
            child_seeks: Some(self.db.metric(SEEK_CHILD_SEEK_COUNT)),
        }
    }

    fn report(&self) -> String {
        let levels: Vec<String> = (0..7)
            .map(|level| {
                self.db
                    .string(&format!("rocksdb.num-files-at-level{level}"))
                    .trim()
                    .to_string()
            })
            .collect();
        let stats = self.db.string("rocksdb.stats");
        let (written, read) = compaction_gib(&stats);
        format!(
            "files_per_level={}\tsst_mib={:.2}\ttable_readers_mib={:.2}\tmemtable_mib={:.2}\tblock_cache_mib={}\tflushed_mib={:.1}\tcompaction_read_mib={:.1}\tcompaction_write_mib={:.1}",
            levels.join("/"),
            mib(self.db.int("rocksdb.total-sst-files-size")),
            mib(self.db.int("rocksdb.estimate-table-readers-mem")),
            mib(self.db.int("rocksdb.size-all-mem-tables")),
            self.db
                .cache_usage()
                .map_or("-".to_string(), |bytes| format!("{:.2}", mib(bytes as u64))),
            flushed_gib(&stats) * 1024.0,
            read * 1024.0,
            written * 1024.0,
        )
    }

    fn workspace_bytes(&self) -> Option<usize> {
        None
    }

    fn dir(&self) -> Option<PathBuf> {
        Some(self.dir.clone())
    }

    /// Data, index and filter block sizes, raw key and value sizes and entry
    /// counts, summed over the live SST files.
    fn details(&self) -> Option<String> {
        Some(
            self.db
                .string("rocksdb.aggregated-table-properties")
                .trim()
                .to_string(),
        )
    }
}

/// "Flush(GB): cumulative X, ..." in RocksDB's stats text.
fn flushed_gib(stats: &str) -> f64 {
    let label = "Flush(GB): cumulative ";
    stats
        .find(label)
        .and_then(|at| {
            stats[at + label.len()..]
                .split(',')
                .next()?
                .trim()
                .parse()
                .ok()
        })
        .unwrap_or(f64::NAN)
}

/// "Cumulative compaction: X GB write, ..., Y GB read, ...": (X, Y).
fn compaction_gib(stats: &str) -> (f64, f64) {
    let label = "Cumulative compaction: ";
    let Some(at) = stats.find(label) else {
        return (f64::NAN, f64::NAN);
    };
    let line = stats[at + label.len()..].lines().next().unwrap_or("");
    let number = |unit: &str| {
        line.find(unit)
            .and_then(|end| line[..end].rsplit([' ', ',']).next()?.parse().ok())
            .unwrap_or(f64::NAN)
    };
    (number(" GB write"), number(" GB read"))
}

fn mib(bytes: u64) -> f64 {
    bytes as f64 / f64::from(1 << 20)
}

// Resources.

/// Kibibytes of this process's anonymous, file-backed and shared-memory
/// resident pages.
fn resident() -> (u64, u64, u64) {
    let status = std::fs::read_to_string("/proc/self/status").unwrap_or_default();
    let field = |name: &str| {
        status
            .lines()
            .find(|line| line.starts_with(name))
            .and_then(|line| line.split_whitespace().nth(1)?.parse().ok())
            .unwrap_or(0)
    };
    (field("RssAnon:"), field("RssFile:"), field("RssShmem:"))
}

fn dir_bytes(dir: &Path) -> u64 {
    std::fs::read_dir(dir).map_or(0, |entries| {
        entries
            .filter_map(|entry| entry.ok()?.metadata().ok())
            .map(|metadata| metadata.len())
            .sum()
    })
}

/// CPU seconds, user and system, of the process or of the calling thread.
fn cpu_secs(path: &str) -> f64 {
    std::fs::read_to_string(path)
        .ok()
        .and_then(|stat| {
            let fields: Vec<&str> = stat.rsplit_once(')')?.1.split_whitespace().collect();
            let user: f64 = fields.get(11)?.parse().ok()?;
            let system: f64 = fields.get(12)?.parse().ok()?;
            Some((user + system) / 100.0)
        })
        .unwrap_or(0.0)
}

/// Peaks of anonymous memory and of the data directory, sampled every
/// 20 ms, with their sum taken at the same instant.
struct Sampler {
    stop: AtomicBool,
    anon: AtomicU64,
    dir: AtomicU64,
    total: AtomicU64,
}

impl Sampler {
    fn start(dir: Option<PathBuf>) -> (Arc<Self>, std::thread::JoinHandle<()>) {
        let sampler = Arc::new(Self {
            stop: AtomicBool::new(false),
            anon: AtomicU64::new(0),
            dir: AtomicU64::new(0),
            total: AtomicU64::new(0),
        });
        let shared = Arc::clone(&sampler);
        let thread = std::thread::spawn(move || {
            while !shared.stop.load(Relaxed) {
                shared.sample(dir.as_deref());
                std::thread::sleep(Duration::from_millis(20));
            }
        });
        (sampler, thread)
    }

    fn sample(&self, dir: Option<&Path>) {
        let anon = resident().0 << 10;
        let files = dir.map_or(0, dir_bytes);
        self.anon.fetch_max(anon, Relaxed);
        self.dir.fetch_max(files, Relaxed);
        self.total.fetch_max(anon + files, Relaxed);
    }

    /// Returns the peaks since the last call and starts new ones.
    fn take(&self) -> (u64, u64, u64) {
        (
            self.anon.swap(0, Relaxed),
            self.dir.swap(0, Relaxed),
            self.total.swap(0, Relaxed),
        )
    }
}

/// glibc's `struct mallinfo2`.
#[repr(C)]
#[derive(Default)]
struct MallInfo {
    arena: usize,
    ordblks: usize,
    smblks: usize,
    hblks: usize,
    hblkhd: usize,
    usmblks: usize,
    fsmblks: usize,
    uordblks: usize,
    fordblks: usize,
    keepcost: usize,
}

unsafe extern "C" {
    /// glibc: returns free heap memory to the system where it can.
    fn malloc_trim(pad: usize) -> i32;
    /// glibc: the allocator's own accounting.
    fn mallinfo2() -> MallInfo;
}

/// A phase's wall time, CPU time and memory peaks.
struct Phase {
    start: Instant,
    cpu: f64,
    /// Time and main-thread CPU spent generating data, excluded below.
    generating: Duration,
    generating_cpu: f64,
}

impl Phase {
    fn start() -> Self {
        Self {
            start: Instant::now(),
            cpu: cpu_secs("/proc/self/stat"),
            generating: Duration::ZERO,
            generating_cpu: 0.0,
        }
    }

    /// Runs `generate` outside the phase's measured time.
    fn untimed<T>(&mut self, generate: impl FnOnce() -> T) -> T {
        let (start, cpu) = (Instant::now(), cpu_secs("/proc/thread-self/stat"));
        let out = generate();
        self.generating += start.elapsed();
        self.generating_cpu += cpu_secs("/proc/thread-self/stat") - cpu;
        out
    }

    fn end(self, name: &str, sampler: &Sampler, dir: Option<&Path>, extra: &str) {
        sampler.sample(dir);
        let wall = self.start.elapsed() - self.generating;
        let cpu = cpu_secs("/proc/self/stat") - self.cpu - self.generating_cpu;
        let (anon, files, total) = sampler.take();
        println!(
            "phase\t{name}\twall_s={:.3}\tcpu_s={:.2}\tpeak_anon_mib={:.1}\tpeak_dir_mib={:.1}\tpeak_total_mib={:.1}{extra}",
            wall.as_secs_f64(),
            cpu,
            mib(anon),
            mib(files),
            mib(total),
        );
    }
}

fn memory<S: Store>(store: &S, state: &str) {
    let dir = store.dir().map_or(0, |dir| dir_bytes(&dir));
    let (anon, file, shmem) = resident();
    // SAFETY: glibc's malloc_trim has no preconditions.
    unsafe { malloc_trim(0) };
    let trimmed = resident().0;
    // SAFETY: as above.
    let heap = unsafe { mallinfo2() };
    println!(
        "memory\t{state}\tanon_mib={:.1}\ttrimmed_anon_mib={:.1}\tallocated_mib={:.1}\theap_free_mib={:.1}\tfile_mib={:.1}\tshmem_mib={:.1}\tdir_mib={:.1}\ttotal_mib={:.1}\t{}",
        anon as f64 / 1024.0,
        trimmed as f64 / 1024.0,
        mib((heap.uordblks + heap.hblkhd) as u64),
        mib(heap.fordblks as u64),
        file as f64 / 1024.0,
        shmem as f64 / 1024.0,
        mib(dir),
        trimmed as f64 / 1024.0 + mib(dir),
        store.report()
    );
}

// Reads.

#[derive(Clone, Copy, Debug)]
enum Get {
    /// A record neither updated nor deleted, uniformly chosen.
    Present,
    /// A record overwritten by the updates.
    Overwritten,
    /// A record the updates deleted.
    Deleted,
    /// A key between two stored keys.
    Missing,
    /// 90% from a window of 1% of the records, the rest from all of them.
    Localized,
}

const GETS: [Get; 5] = [
    Get::Present,
    Get::Overwritten,
    Get::Deleted,
    Get::Missing,
    Get::Localized,
];

#[derive(Clone, Copy, Debug)]
enum Scan {
    /// A 5-byte prefix inside the range that matches nothing.
    Empty,
    /// A whole key as the prefix: one record, and a trailing 0xFF whose
    /// upper bound carries.
    Exact,
    /// 6 bytes: 32 records less those deleted.
    Short,
    /// 4 bytes: 512.
    Medium,
    /// 3 bytes: 2,048.
    Long,
    /// 2 bytes `[g, 0xFF]` of an odd group, or `[0xFF, 0xFF]` of the last,
    /// which has no upper bound: 16,384.
    Group,
}

const SCANS: [Scan; 6] = [
    Scan::Empty,
    Scan::Exact,
    Scan::Short,
    Scan::Medium,
    Scan::Long,
    Scan::Group,
];

impl Scan {
    /// Timed operations per round.
    fn ops(self) -> usize {
        match self {
            Self::Empty | Self::Exact => 10_000,
            Self::Short => 5_000,
            Self::Medium => 600,
            Self::Long => 150,
            Self::Group => 8,
        }
    }
}

const GET_OPS: usize = 30_000;
const ROUNDS: usize = 3;

fn get_key(data: Dataset, class: Get, rng: &mut Rng) -> ([u8; 8], Option<u64>) {
    let records = data.records();
    let index = match class {
        Get::Present => loop {
            let index = rng.below(records);
            if !matches!(index % 16, 0 | 8) {
                break index;
            }
        },
        Get::Overwritten => rng.below(records / 16) * 16,
        Get::Deleted => rng.below(records / 16) * 16 + 8,
        Get::Missing => return (data.absent(rng.below(records)), None),
        Get::Localized => {
            if rng.below(10) < 9 {
                records / 2 + rng.below(records / 100)
            } else {
                rng.below(records)
            }
        }
    };
    (data.key(index), Some(index))
}

fn scan_prefix(data: Dataset, class: Scan, rng: &mut Rng) -> (Vec<u8>, Range<u64>) {
    let index = rng.below(data.records());
    let key = data.key(index);
    let aligned = |span: u64| index / span * span..index / span * span + span;
    match class {
        Scan::Empty => ([&key[..4], &[0x01]].concat(), 0..0),
        Scan::Exact => (key.to_vec(), index..index + 1),
        Scan::Short => (key[..6].to_vec(), aligned(32)),
        Scan::Medium => (key[..4].to_vec(), aligned(512)),
        Scan::Long => (key[..3].to_vec(), aligned(2_048)),
        Scan::Group => {
            let group = rng.below(data.groups / 2) * 2 + 1;
            let first = group * GROUP;
            (data.key(first)[..2].to_vec(), first..first + GROUP)
        }
    }
}

fn percentile(sorted: &[u64], p: f64) -> u64 {
    sorted[((sorted.len() - 1) as f64 * p) as usize]
}

/// p50, p99 and mean of `samples`, sorting them.
fn summary(samples: &mut [u64]) -> (u64, u64, f64) {
    samples.sort_unstable();
    let mean = samples.iter().sum::<u64>() as f64 / samples.len() as f64;
    (percentile(samples, 0.5), percentile(samples, 0.99), mean)
}

fn timed_reads<S: Store>(store: &mut S, data: Dataset, state: &str, seed: u64) {
    for (at, class) in GETS.into_iter().enumerate() {
        let mut rng = Rng::new(seed + at as u64);
        let mut latencies = Vec::with_capacity(GET_OPS * ROUNDS);
        let mut round_p50 = Vec::new();
        // A warm-up round, then the timed ones.
        for round in 0..=ROUNDS {
            let keys: Vec<[u8; 8]> = (0..if round == 0 { GET_OPS / 4 } else { GET_OPS })
                .map(|_| get_key(data, class, &mut rng).0)
                .collect();
            let mut samples = Vec::with_capacity(keys.len());
            for key in &keys {
                let start = Instant::now();
                let len = store.get(key, |value| value.map_or(0, <[u8]>::len));
                samples.push(start.elapsed().as_nanos() as u64);
                black_box(len);
            }
            if round > 0 {
                round_p50.push(summary(&mut samples).0);
                latencies.extend(samples);
            }
        }
        let (p50, p99, mean) = summary(&mut latencies);
        round_p50.sort_unstable();
        println!(
            "get\t{state}\t{class:?}\tops={}\tp50_ns={p50}\tp99_ns={p99}\tmean_ns={mean:.0}\tops_per_s={:.0}\tround_p50_ns={}-{}",
            latencies.len(),
            1e9 / mean,
            round_p50[0],
            round_p50[ROUNDS - 1],
        );
    }
    // Missing keys through one held read, beside the per-get path above.
    let mut rng = Rng::new(seed + 50);
    let keys: Vec<[u8; 8]> = (0..GET_OPS)
        .map(|_| get_key(data, Get::Missing, &mut rng).0)
        .collect();
    let _ = store.held_gets(&keys);
    if let Some(mut samples) = store.held_gets(&keys) {
        let (p50, p99, mean) = summary(&mut samples);
        println!(
            "get\t{state}\tMissingHeld\tops={}\tp50_ns={p50}\tp99_ns={p99}\tmean_ns={mean:.0}\tops_per_s={:.0}\tround_p50_ns={p50}-{p50}",
            samples.len(),
            1e9 / mean,
        );
    }
    for (at, class) in SCANS.into_iter().enumerate() {
        let mut rng = Rng::new(seed + 100 + at as u64);
        let (mut totals, mut firsts) = (Vec::new(), Vec::new());
        let (mut records, mut after_first_ns, mut after_first_records) = (0u64, 0u64, 0u64);
        let mut round_p50 = Vec::new();
        for round in 0..=ROUNDS {
            let ops = if round == 0 {
                class.ops().div_ceil(4)
            } else {
                class.ops()
            };
            let prefixes: Vec<Vec<u8>> = (0..ops)
                .map(|_| scan_prefix(data, class, &mut rng).0)
                .collect();
            let mut samples = Vec::with_capacity(ops);
            for prefix in &prefixes {
                let start = Instant::now();
                let mut first = None;
                let mut count = 0u64;
                store.scan(prefix, |key, value| {
                    if count == 0 {
                        first = Some(start.elapsed());
                    }
                    count += 1;
                    black_box((key.len(), value.len()));
                });
                let total = start.elapsed();
                let first = first.unwrap_or(total);
                samples.push(total.as_nanos() as u64);
                if round > 0 {
                    firsts.push(first.as_nanos() as u64);
                    records += count;
                    if count > 1 {
                        after_first_ns += (total - first).as_nanos() as u64;
                        after_first_records += count - 1;
                    }
                }
            }
            if round > 0 {
                round_p50.push(summary(&mut samples).0);
                totals.extend(samples);
            }
        }
        let ops = totals.len();
        let (p50, p99, mean) = summary(&mut totals);
        let (first_p50, first_p99, _) = summary(&mut firsts);
        round_p50.sort_unstable();
        let total_ns: f64 = mean * ops as f64;
        println!(
            "scan\t{state}\t{class:?}\tops={ops}\trecords_per_op={:.1}\tp50_ns={p50}\tp99_ns={p99}\tfirst_p50_ns={first_p50}\tfirst_p99_ns={first_p99}\tns_per_record_after_first={:.0}\trecords_per_s={:.0}\tround_p50_ns={}-{}",
            records as f64 / ops as f64,
            after_first_ns as f64 / after_first_records.max(1) as f64,
            records as f64 / (total_ns / 1e9),
            round_p50[0],
            round_p50[ROUNDS - 1],
        );
    }
}

fn per_op(after: Option<u64>, before: Option<u64>, ops: usize) -> String {
    match (after, before) {
        (Some(after), Some(before)) => format!("{:.2}", (after - before) as f64 / ops as f64),
        _ => "-".to_string(),
    }
}

fn counter_line(after: Counters, before: Counters, ops: usize, records: u64) -> String {
    format!(
        "blocks_per_op={:.2}\tfilter_checks_per_op={}\tfilter_positives_per_op={}\tskipped_per_op={}\tdeletes_skipped_per_op={}\tchild_seeks_per_op={}\trecords_per_op={:.1}",
        (after.blocks - before.blocks) as f64 / ops as f64,
        per_op(after.filter_checks, before.filter_checks, ops),
        per_op(after.filter_positives, before.filter_positives, ops),
        per_op(after.skipped, before.skipped, ops),
        per_op(after.deletes_skipped, before.deletes_skipped, ops),
        per_op(after.child_seeks, before.child_seeks, ops),
        records as f64 / ops as f64,
    )
}

/// Untimed: the same classes with counters on, every result checked
/// against the generator, then a full ordered scan checked record by
/// record and timed on its own.
fn checked_reads<S: Store>(store: &mut S, data: Dataset, state: &str, seed: u64) {
    store.counting(true);
    for (at, class) in GETS.into_iter().enumerate() {
        let mut rng = Rng::new(seed + 1_000 + at as u64);
        let ops = 3_000;
        let before = store.counters();
        let mut found = 0u64;
        for _ in 0..ops {
            let (key, index) = get_key(data, class, &mut rng);
            let expected =
                index.and_then(|index| Some(value(data.payload, index, version(index, true)?)));
            let same = store.get(&key, |value| value == expected.as_deref());
            assert!(same, "{state} {class:?}: wrong value for {key:02x?}");
            found += u64::from(expected.is_some());
        }
        println!(
            "count\t{state}\tget {class:?}\t{}",
            counter_line(store.counters(), before, ops, found)
        );
    }
    for (at, class) in SCANS.into_iter().enumerate() {
        let mut rng = Rng::new(seed + 1_100 + at as u64);
        let ops = class.ops().div_ceil(10).max(4);
        let before = store.counters();
        let mut records = 0u64;
        for _ in 0..ops {
            let (prefix, range) = scan_prefix(data, class, &mut rng);
            let mut expected = range.filter_map(|index| {
                let version = version(index, true)?;
                Some((data.key(index), value(data.payload, index, version)))
            });
            store.scan(&prefix, |key, value| {
                let (want_key, want_value) = expected
                    .next()
                    .unwrap_or_else(|| panic!("{state} {class:?}: extra key {key:02x?}"));
                assert!(
                    key == want_key && value == want_value.as_slice(),
                    "{state} {class:?}: {key:02x?} where {want_key:02x?} was expected"
                );
                records += 1;
            });
            assert!(
                expected.next().is_none(),
                "{state} {class:?}: scan of {prefix:02x?} ended early"
            );
        }
        println!(
            "count\t{state}\tscan {class:?}\t{}",
            counter_line(store.counters(), before, ops, records)
        );
    }
    store.counting(false);

    let start = Instant::now();
    let mut records = 0u64;
    store.scan(&[], |key, value| {
        black_box((key.len(), value.len()));
        records += 1;
    });
    let elapsed = start.elapsed();

    let (mut index, mut logical) = (0u64, 0u64);
    let next_live = |index: &mut u64| {
        while *index < data.records() {
            let at = *index;
            *index += 1;
            if let Some(version) = version(at, true) {
                return Some((data.key(at), value(data.payload, at, version)));
            }
        }
        None
    };
    store.scan(&[], |key, value| {
        let (want_key, want_value) = next_live(&mut index).expect("an extra record");
        assert!(
            key == want_key && value == want_value.as_slice(),
            "{key:02x?}"
        );
        logical += (key.len() + value.len()) as u64;
    });
    assert!(next_live(&mut index).is_none(), "the full scan ended early");
    println!(
        "full\t{state}\trecords={records}\tlogical_mib={:.1}\twall_s={:.3}\trecords_per_s={:.0}\tworkspace_kib={}\tverified=all",
        mib(logical),
        elapsed.as_secs_f64(),
        records as f64 / elapsed.as_secs_f64(),
        store
            .workspace_bytes()
            .map_or("-".to_string(), |bytes| format!(
                "{:.0}",
                bytes as f64 / 1024.0
            )),
    );
}

// Memory of single compactions.

/// Loads without background compaction, then compacts the two smallest
/// overlapping runs at a time, reporting for each compaction its input and
/// output against the peak, sampled every 5 ms, of live allocations
/// (`mallinfo2`) and of anonymous memory above their level before it. A
/// snapshot is held through the fourth compaction, so its inputs stay.
fn compactions(data: Dataset, compression: Compression) {
    let engine = Engine::new(EngineOptions {
        max_runs: 256,
        background_compaction: None,
        compression,
        ..EngineOptions::default()
    });
    let records = data.records();
    for j in 0..records {
        let index = data.shuffled(j, 0x9E37_79B1);
        engine
            .put(&data.key(index), &value(data.payload, index, 0))
            .unwrap();
    }
    engine.flush().unwrap();
    let allocated = || {
        // SAFETY: mallinfo2 has no preconditions.
        let heap = unsafe { mallinfo2() };
        (heap.uordblks + heap.hblkhd) as u64
    };
    let peak_alloc = Arc::new(AtomicU64::new(0));
    let peak_anon = Arc::new(AtomicU64::new(0));
    let stop = Arc::new(AtomicBool::new(false));
    let thread = {
        let (peak_alloc, peak_anon, stop) = (peak_alloc.clone(), peak_anon.clone(), stop.clone());
        std::thread::spawn(move || {
            while !stop.load(Relaxed) {
                peak_alloc.fetch_max(allocated(), Relaxed);
                peak_anon.fetch_max(resident().0 << 10, Relaxed);
                std::thread::sleep(Duration::from_millis(5));
            }
        })
    };
    let stats = engine.stats();
    println!(
        "loaded\truns={}\tmax_overlap={}\trun_mib={:.1}\tallocated_mib={:.1}\tanon_mib={:.1}",
        stats.runs,
        stats.max_overlap,
        mib(stats.run_bytes as u64),
        mib(allocated()),
        resident().0 as f64 / 1024.0,
    );
    let options = CompactionOptions {
        max_overlap: 1,
        max_inputs: 2,
        max_input_bytes: usize::MAX,
        max_output_bytes: usize::MAX,
        ..CompactionOptions::default()
    };
    let mut workspace = MergeWorkspace::new();
    for at in 1..=12 {
        let snapshot = (at == 4).then(|| engine.snapshot().unwrap());
        let (alloc_before, anon_before) = (allocated(), resident().0 << 10);
        peak_alloc.store(alloc_before, Relaxed);
        peak_anon.store(anon_before, Relaxed);
        let start = Instant::now();
        let Some(report) = engine.compact(options, &mut workspace).unwrap() else {
            break;
        };
        let wall = start.elapsed();
        let (alloc_after, anon_after) = (allocated(), resident().0 << 10);
        let retained = engine.stats().retained_bytes;
        drop(snapshot);
        let freed = allocated();
        let delta = |a: u64, b: u64| (a as f64 - b as f64) / f64::from(1 << 20);
        println!(
            "compaction\t{at}\tinputs={}\tinput_mib={:.1}\toutput_mib={:.1}\tfilters_mib={:.1}\tfilter_build_mib={:.1}\tworkspace_estimate_mib={:.1}\tpeak_alloc_above_mib={:.1}\tpeak_anon_above_mib={:.1}\tafter_alloc_change_mib={:.1}\tafter_anon_change_mib={:.1}\tretained_mib={:.1}\tafter_snapshot_drop_alloc_change_mib={:.1}\twall_s={:.2}",
            report.inputs,
            mib(report.input_bytes as u64),
            mib(report.output_bytes as u64),
            mib(report.output_filter_bytes as u64),
            mib(report.filter_build_bytes as u64),
            mib(report.workspace_bytes as u64),
            delta(peak_alloc.load(Relaxed), alloc_before),
            delta(peak_anon.load(Relaxed), anon_before),
            delta(alloc_after, alloc_before),
            delta(anon_after, anon_before),
            mib(retained as u64),
            delta(freed, alloc_before),
            wall.as_secs_f64(),
        );
    }
    stop.store(true, Relaxed);
    thread.join().unwrap();
}

// Memory over repeated cycles.

/// Loads and consolidates, then repeats: overwrite a quarter of the records,
/// flush, settle and consolidate, reporting memory after each cycle without
/// trimming. A snapshot is held through cycle 3 and released in cycle 4.
fn cycles(data: Dataset, memtable_budget: Option<usize>) {
    let mut store = Tessera::new(Compression::Zstd);
    if let Some(memtable_budget) = memtable_budget {
        store.engine = Engine::new(EngineOptions {
            memtable_budget,
            max_runs: 256,
            background_compaction: Some(CompactionOptions::default()),
            ..EngineOptions::default()
        });
    }
    let (sampler, thread) = Sampler::start(None);
    let records = data.records();
    let report = |store: &Tessera, cycle: u64, phase: Phase, sampler: &Sampler| {
        let (anon, _, _) = resident();
        // SAFETY: mallinfo2 has no preconditions.
        let heap = unsafe { mallinfo2() };
        let stats = store.engine.stats();
        let (_, _, peak) = sampler.take();
        println!(
            "cycle\t{cycle}\twall_s={:.2}\tanon_mib={:.1}\tallocated_mib={:.1}\theap_free_mib={:.1}\tmmapped_mib={:.1}\tpeak_anon_mib={:.1}\truns={}\trun_mib={:.1}\tfilter_mib={:.1}\tretained_mib={:.1}",
            phase.start.elapsed().as_secs_f64(),
            anon as f64 / 1024.0,
            mib((heap.uordblks + heap.hblkhd) as u64),
            mib(heap.fordblks as u64),
            mib(heap.hblkhd as u64),
            mib(peak),
            stats.runs,
            mib(stats.run_bytes as u64),
            mib(stats.filter_bytes as u64),
            mib(stats.retained_bytes as u64),
        );
    };
    let phase = Phase::start();
    for j in 0..records {
        let index = data.shuffled(j, 0x9E37_79B1);
        store.put(&data.key(index), &value(data.payload, index, 0));
    }
    store.settle();
    store.consolidate();
    report(&store, 0, phase, &sampler);
    let mut snapshot = None;
    for cycle in 1..=6u64 {
        let phase = Phase::start();
        for j in 0..records {
            let index = data.shuffled(j, 0x85EB_CA6B);
            if index % 4 == cycle % 4 {
                store.put(&data.key(index), &value(data.payload, index, cycle));
            }
        }
        store.flush();
        store.settle();
        match cycle {
            3 => snapshot = Some(store.engine.snapshot().unwrap()),
            4 => snapshot = None,
            _ => {}
        }
        store.consolidate();
        report(&store, cycle, phase, &sampler);
    }
    drop(snapshot);
    // SAFETY: as above.
    unsafe { malloc_trim(0) };
    let (anon, _, _) = resident();
    println!("cycle\ttrimmed\tanon_mib={:.1}", anon as f64 / 1024.0);
    sampler.stop.store(true, Relaxed);
    thread.join().unwrap();
}

// The run.

fn run<S: Store>(mut store: S, data: Dataset) {
    let dir = store.dir();
    let (sampler, thread) = Sampler::start(dir.clone());
    let dir = dir.as_deref();
    let records = data.records();
    const BATCH: u64 = GROUP;

    // Load in a fixed random order, with background compaction.
    let mut phase = Phase::start();
    let mut logical = 0u64;
    for batch in 0..records / BATCH {
        let pairs: Vec<([u8; 8], Vec<u8>)> = phase.untimed(|| {
            (batch * BATCH..(batch + 1) * BATCH)
                .map(|j| {
                    let index = data.shuffled(j, 0x9E37_79B1);
                    (data.key(index), value(data.payload, index, 0))
                })
                .collect()
        });
        for (key, value) in &pairs {
            logical += (key.len() + value.len()) as u64;
            store.put(key, value);
        }
    }
    phase.end(
        "load",
        &sampler,
        dir,
        &format!("\trecords={records}\tlogical_mib={:.1}", mib(logical)),
    );
    let phase = Phase::start();
    store.settle();
    phase.end("load-settle", &sampler, dir, "");
    memory(&store, "loaded");

    let phase = Phase::start();
    store.consolidate();
    phase.end("consolidate-base", &sampler, dir, "");

    // Updates: one record in 16 overwritten, another deleted.
    let mut phase = Phase::start();
    let (mut writes, mut logical) = (0u64, 0u64);
    for batch in 0..records / BATCH {
        let updates: Vec<([u8; 8], Option<Vec<u8>>)> = phase.untimed(|| {
            (batch * BATCH..(batch + 1) * BATCH)
                .map(|j| data.shuffled(j, 0x85EB_CA6B))
                .filter(|index| matches!(index % 16, 0 | 8))
                .map(|index| {
                    let value = version(index, true).map(|v| value(data.payload, index, v));
                    (data.key(index), value)
                })
                .collect()
        });
        for (key, value) in &updates {
            writes += 1;
            match value {
                Some(value) => {
                    logical += (key.len() + value.len()) as u64;
                    store.put(key, value);
                }
                None => {
                    logical += key.len() as u64;
                    store.delete(key);
                }
            }
        }
    }
    store.flush();
    phase.end(
        "update",
        &sampler,
        dir,
        &format!("\twrites={writes}\tlogical_mib={:.1}", mib(logical)),
    );
    let phase = Phase::start();
    store.settle();
    phase.end("update-settle", &sampler, dir, "");

    for (state, seed) in [("overlapping", 10_000), ("consolidated", 20_000)] {
        if state == "consolidated" {
            let phase = Phase::start();
            store.consolidate();
            phase.end("consolidate", &sampler, dir, "");
        }
        memory(&store, state);
        if let Some(details) = store.details().filter(|_| state == "consolidated") {
            println!("details\t{state}\t{details}");
        }
        let phase = Phase::start();
        timed_reads(&mut store, data, state, seed);
        phase.end(&format!("reads-{state}"), &sampler, dir, "");
        checked_reads(&mut store, data, state, seed);
    }
    memory(&store, "final");

    sampler.stop.store(true, Relaxed);
    thread.join().unwrap();
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let [_, engine, dataset, base] = args.as_slice() else {
        eprintln!("usage: compare ENGINE DATASET DIR");
        std::process::exit(2);
    };
    let data = Dataset::parse(dataset);
    let dir = Path::new(base).join("tessera-compare-db");
    let _ = std::fs::remove_dir_all(&dir);
    println!(
        "config\tengine={engine}\tdataset={dataset}\tpayload={:?}\trecords={}\tget_ops={GET_OPS}\trounds={ROUNDS}",
        data.payload,
        data.records()
    );
    let rocks = |lz4, tuned_cache| {
        std::fs::create_dir_all(&dir).unwrap();
        Rocks {
            db: rocks::Db::open(&dir, rocks::Config { lz4, tuned_cache }),
            dir: dir.clone(),
        }
    };
    match engine.as_str() {
        "tessera" => run(Tessera::new(Compression::Zstd), data),
        "tessera-lz4" => run(Tessera::new(Compression::Lz4), data),
        "tessera-cycles" => cycles(data, None),
        "tessera-cycles-16m" => cycles(data, Some(16 << 20)),
        "tessera-compactions" => compactions(data, Compression::Zstd),
        "tessera-compactions-lz4" => compactions(data, Compression::Lz4),
        "rocksdb" => run(rocks(false, None), data),
        "rocksdb-lz4" => run(rocks(true, None), data),
        "rocksdb-tuned" => run(rocks(false, Some(64 << 20)), data),
        other => panic!("unknown engine {other}"),
    }
    let _ = std::fs::remove_dir_all(&dir);
}
