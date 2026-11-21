//! macOS Tessera and RocksDB comparison. `tessera-bench all OUTDIR` isolates
//! each dataset and engine in a child process with a deadline and memory guard.
//! Results and oracle checks share deterministic inputs; see README.md.
mod data;
mod scaling;
mod store;
mod streaming;
mod zipf;
use data::*;
use hdrhistogram::Histogram;
use std::{
    collections::HashMap,
    fmt::Write as _,
    fs,
    hint::black_box,
    io::{BufRead, BufReader, Write},
    process::{Command, Stdio},
    sync::{
        Barrier, OnceLock,
        atomic::{AtomicBool, Ordering},
    },
    thread,
    time::{Duration, Instant},
};
use store::Store;

const MOUNT: &str = "/Volumes/tessbench";
/// Process RSS plus RAM-disk bytes beyond which a child aborts.
const LIMIT: u64 = 8 << 30;
const ENGINES: [&str; 3] = ["tessera", "r-small", "r-large"];
const SCALE_ENGINES: [&str; 3] = ["tessera", "r-small", "r-mid"];
const MISS: u32 = u32::MAX;

fn main() {
    let args: Vec<String> = std::env::args().collect();
    match args.get(1).map(String::as_str) {
        Some("all") => all(&args[2], &Filter::parse(&args[3..])),
        Some("case") if args.get(7).is_some_and(|s| s == "streaming") => streaming::run(
            args[2].chars().next().unwrap(),
            &args[3],
            &args[4],
            &args[6],
        ),
        Some("case") => case(
            args[2].chars().next().unwrap(),
            &args[3],
            &args[4],
            args[5].parse().unwrap(),
            &args[6],
        ),
        _ => panic!("usage: bench all OUTDIR [--dataset A,B] [--engine E,..] [--case C,..]"),
    }
}

// ---- measurement ----

fn rss() -> u64 {
    unsafe { store::tb_rss() }
}

fn footprint() -> u64 {
    unsafe { store::tb_footprint() }
}

fn live_heap() -> u64 {
    unsafe { store::tb_live_heap() }
}

#[derive(Clone, Copy)]
struct MemoryReference {
    rss: u64,
    heap: u64,
    footprint: u64,
    disk: u64,
}
impl MemoryReference {
    /// Capture with input/oracle allocated, before opening the engine.
    fn capture() -> Self {
        Self {
            rss: rss(),
            heap: live_heap(),
            footprint: footprint(),
            disk: disk_used(),
        }
    }
    fn emit(&self, out: &Out) {
        out.emit(
            "setup",
            "baseline_rss_mib",
            format!("{:.2}", self.rss as f64 / MIB as f64),
        );
        out.emit(
            "setup",
            "baseline_heap_mib",
            format!("{:.2}", self.heap as f64 / MIB as f64),
        );
        out.emit(
            "setup",
            "baseline_footprint_mib",
            format!("{:.2}", self.footprint as f64 / MIB as f64),
        );
    }
    fn report(&self, store: &Store, out: &Out, case: &str, live: usize) {
        let disk = if matches!(store, Store::R(_)) {
            disk_used().saturating_sub(self.disk)
        } else {
            0
        };
        let heap = live_heap().saturating_sub(self.heap) + disk;
        let ram = rss().saturating_sub(self.rss) + disk;
        let physical = footprint().saturating_sub(self.footprint) + disk;
        out.emit(case, "engine_heap_bytes", heap);
        out.emit(
            case,
            "engine_heap_mib",
            format!("{:.2}", heap as f64 / MIB as f64),
        );
        out.emit(
            case,
            "engine_ram_mib",
            format!("{:.2}", ram as f64 / MIB as f64),
        );
        out.emit(
            case,
            "engine_footprint_mib",
            format!("{:.2}", physical as f64 / MIB as f64),
        );
        out.emit(
            case,
            "ram_disk_mib",
            format!("{:.2}", disk as f64 / MIB as f64),
        );
        out.emit(case, "live_mib", format!("{:.2}", live as f64 / MIB as f64));
        out.emit(
            case,
            "live_per_heap",
            format!("{:.2}", live as f64 / heap as f64),
        );
        out.emit(
            case,
            "live_per_rss",
            format!("{:.2}", live as f64 / ram as f64),
        );
        if let Some((reserve, used, limit)) = store.pool_stats() {
            out.emit(case, "pool_reserve_bytes", reserve);
            out.emit(case, "pool_in_use_bytes", used);
            out.emit(case, "pool_reserve_limit", limit);
        }
        if let Some(accounted) = store.accounted_run_bytes() {
            out.emit(case, "accounted_run_bytes", accounted);
            assert!(
                heap >= accounted,
                "live heap {heap} below run_bytes + held_bytes {accounted} after {case}"
            );
        }
    }
}

fn disk_used() -> u64 {
    if std::env::var_os("TESSERA_BENCH_NO_DISK").is_some() {
        return 0;
    }
    let p = std::ffi::CString::new(MOUNT).unwrap();
    let mut s = std::mem::MaybeUninit::<libc::statfs>::uninit();
    assert_eq!(
        unsafe { libc::statfs(p.as_ptr(), s.as_mut_ptr()) },
        0,
        "RAM disk missing"
    );
    let s = unsafe { s.assume_init() };
    (s.f_blocks - s.f_bfree) * s.f_bsize as u64
}

fn cpu() -> (f64, f64) {
    let mut r = std::mem::MaybeUninit::<libc::rusage>::uninit();
    unsafe { libc::getrusage(libc::RUSAGE_SELF, r.as_mut_ptr()) };
    let r = unsafe { r.assume_init() };
    let s = |t: libc::timeval| t.tv_sec as f64 + t.tv_usec as f64 / 1e6;
    (s(r.ru_utime), s(r.ru_stime))
}

fn hist() -> Histogram<u64> {
    Histogram::new_with_bounds(1, 100_000_000_000, 3).unwrap()
}

fn rec(h: &mut Histogram<u64>, t: Instant) {
    h.saturating_record(t.elapsed().as_nanos() as u64)
}

pub(crate) fn trace_phase(name: &str) {
    if std::env::var_os("TESSERA_BENCH_TRACE_PHASES").is_some() {
        eprintln!("[phase] {name} pid={}", std::process::id());
    }
}

struct Out {
    prefix: String,
}
impl Out {
    fn emit(&self, case: &str, metric: &str, value: impl std::fmt::Display) {
        println!("{},{case},{metric},{value}", self.prefix);
    }
    fn lat(&self, case: &str, h: &Histogram<u64>) {
        for (m, q) in [("p50_us", 0.5), ("p99_us", 0.99), ("p999_us", 0.999)] {
            self.emit(
                case,
                m,
                format!("{:.2}", h.value_at_quantile(q) as f64 / 1e3),
            );
        }
        self.emit(case, "max_us", format!("{:.1}", h.max() as f64 / 1e3));
    }
}

struct Span {
    t: Instant,
    cpu: (f64, f64),
}
impl Span {
    fn new() -> Self {
        Self {
            t: Instant::now(),
            cpu: cpu(),
        }
    }
    /// Emits wall time, rate and CPU; returns operations per second.
    fn finish(&self, out: &Out, case: &str, ops: usize, bytes: usize) -> f64 {
        let wall = self.t.elapsed().as_secs_f64();
        let (u, s) = cpu();
        out.emit(case, "wall_s", format!("{wall:.2}"));
        out.emit(case, "ops_s", format!("{:.0}", ops as f64 / wall));
        if bytes > 0 {
            out.emit(
                case,
                "mib_s",
                format!("{:.1}", bytes as f64 / MIB as f64 / wall),
            );
        }
        out.emit(case, "cpu_user_s", format!("{:.2}", u - self.cpu.0));
        out.emit(case, "cpu_sys_s", format!("{:.2}", s - self.cpu.1));
        ops as f64 / wall
    }
}

// ---- orchestrator ----

const CASES: &str = "load,update,reads,scan,mixed,bulk";
const ALL_CASES: &str = "load,update,reads,scan,mixed,bulk,reader_scale,writer_scale,zipf";

/// Comma-separated selections; each defaults to everything.
struct Filter {
    datasets: String,
    engines: String,
    cases: String,
    streaming: bool,
}
impl Filter {
    fn parse(args: &[String]) -> Self {
        let mut f = Self {
            datasets: "A,B".into(),
            engines: ENGINES.join(","),
            cases: CASES.into(),
            streaming: false,
        };
        for pair in args.chunks(2) {
            let value = pair.get(1).cloned().expect("a filter needs a value");
            match pair[0].as_str() {
                "--mode" => {
                    assert!(value == "memory" || value == "streaming");
                    f.streaming = value == "streaming";
                }
                "--dataset" => f.datasets = value,
                "--engine" => f.engines = value,
                "--case" => f.cases = value,
                other => panic!("unknown option {other}"),
            }
        }
        if f.streaming {
            if !args.iter().any(|a| a == "--engine") {
                f.engines = SCALE_ENGINES.join(",");
            }
            if !args.iter().any(|a| a == "--case") {
                f.cases = "load,update,reads".into();
            }
            assert!(
                f.engines.split(',').all(|e| SCALE_ENGINES.contains(&e)),
                "streaming engines: tessera,r-small,r-mid"
            );
            assert!(
                f.cases
                    .split(',')
                    .all(|c| ["load", "update", "reads"].contains(&c)),
                "streaming cases: load,update,reads"
            );
            assert!(
                f.datasets.split(',').all(|d| ["A", "B"].contains(&d)),
                "streaming datasets: A,B"
            );
        } else {
            assert!(
                f.datasets.split(',').all(|d| ["A", "B", "C"].contains(&d)),
                "unknown dataset; datasets are A,B,C"
            );
        }
        assert!(
            f.engines.split(',').all(|e| if f.streaming {
                SCALE_ENGINES.contains(&e)
            } else {
                ENGINES.contains(&e)
            }),
            "unknown engine"
        );
        for c in f.cases.split(',') {
            assert!(
                wants(ALL_CASES, c),
                "unknown case {c}; cases are {ALL_CASES}"
            );
        }
        f
    }
}

fn wants(list: &str, item: &str) -> bool {
    list.split(',').any(|c| c == item)
}

fn all(outdir: &str, filter: &Filter) {
    let started = Instant::now();
    let exe = std::env::current_exe().unwrap();
    let mut csv = fs::File::create(format!("{outdir}/results.csv")).unwrap();
    writeln!(csv, "dataset,engine,case,metric,value").unwrap();
    let mut rows: Vec<[String; 5]> = Vec::new();
    let mut failed = false;
    let engines = if filter.streaming {
        &SCALE_ENGINES
    } else {
        &ENGINES
    };
    for ds in ["A", "B", "C"]
        .into_iter()
        .filter(|d| wants(&filter.datasets, d))
    {
        let mut rate = 0.0;
        for engine in engines
            .iter()
            .copied()
            .filter(|e| wants(&filter.engines, e))
        {
            eprintln!("[{:>5.0}s] {ds} {engine}", started.elapsed().as_secs_f64());
            let mut child = Command::new(&exe)
                .args([
                    "case",
                    ds,
                    engine,
                    outdir,
                    &rate.to_string(),
                    &filter.cases,
                    if filter.streaming {
                        "streaming"
                    } else {
                        "memory"
                    },
                ])
                .stdout(Stdio::piped())
                .spawn()
                .unwrap();
            let stdout = child.stdout.take().unwrap();
            let lines = thread::spawn(move || {
                BufReader::new(stdout)
                    .lines()
                    .map(Result::unwrap)
                    .collect::<Vec<_>>()
            });
            let deadline = Instant::now() + Duration::from_secs(300);
            let status = loop {
                if let Some(s) = child.try_wait().unwrap() {
                    break s.to_string();
                }
                if Instant::now() > deadline {
                    child.kill().unwrap();
                    child.wait().unwrap();
                    break "killed: timeout".into();
                }
                thread::sleep(Duration::from_millis(100));
            };
            let mut lines = lines.join().unwrap();
            if status != "exit status: 0" {
                failed = true;
                lines.push(format!(
                    "{ds},{engine},process,status,{}",
                    status.replace(',', ";")
                ));
            }
            for l in lines {
                writeln!(csv, "{l}").unwrap();
                let f: Vec<&str> = l.splitn(5, ',').collect();
                if f.len() != 5 {
                    continue;
                }
                if engine == "tessera" && f[2] == "update" && f[3] == "ops_s" {
                    rate = 0.7 * f[4].parse::<f64>().unwrap();
                }
                rows.push(std::array::from_fn(|j| f[j].to_owned()));
            }
        }
    }
    let total = started.elapsed().as_secs_f64();
    writeln!(csv, "all,all,run,wall_s,{total:.1}").unwrap();
    fs::write(
        format!("{outdir}/summary.md"),
        summary(&rows, total, engines),
    )
    .unwrap();
    eprintln!("total {total:.1} s; results in {outdir}");
    assert!(!failed, "benchmark child failed; see {outdir}/results.csv");
}

fn summary(rows: &[[String; 5]], total: f64, engines: &[&str; 3]) -> String {
    let mut s = format!(
        "# Tessera vs RocksDB {}\n\nTotal runtime {total:.0} s.\n",
        store::rocksdb_version()
    );
    for ds in ["A", "B", "C"] {
        let mut order = Vec::new();
        let mut cells = HashMap::new();
        for r in rows
            .iter()
            .filter(|r| r[0] == ds && !r[3].contains("footprint"))
        {
            let key = (r[2].clone(), r[3].clone());
            if !order.contains(&key) {
                order.push(key.clone());
            }
            cells.insert((key, r[1].clone()), r[4].clone());
        }
        if order.is_empty() {
            continue;
        }
        let _ = write!(
            s,
            "\n## Dataset {ds}\n\n| case | metric | {} |\n|---|---|{}\n",
            engines.join(" | "),
            "---|".repeat(3)
        );
        for key in order {
            let v: Vec<&str> = engines
                .iter()
                .map(|e| {
                    cells
                        .get(&(key.clone(), e.to_string()))
                        .map_or("–", String::as_str)
                })
                .collect();
            let _ = writeln!(s, "| {} | {} | {} |", key.0, key.1, v.join(" | "));
        }
    }
    s
}

// ---- one dataset × engine ----

/// Runs the selected cases. Reads, scans and the mixed case run on the
/// updated DB, so they also run the update, which runs the load. Without
/// Tessera's update rate, the mixed case offers 70% of the engine's own.
fn case(ds: char, engine: &str, outdir: &str, rate: f64, cases: &str) {
    let on_updated = ["reads", "scan", "mixed", "reader_scale", "zipf"]
        .iter()
        .any(|c| wants(cases, c));
    let update = on_updated || wants(cases, "update");
    let load = update || wants(cases, "load");
    let out = Out {
        prefix: format!("{ds},{engine}"),
    };
    let t = Instant::now();
    let mut data = generate(ds);
    let n = data.recs.len();
    let mut rng = Rng(0x62656e6368 ^ ds as u64);
    let mut perm: Vec<u32> = (0..n as u32).collect();
    rng.shuffle(&mut perm);
    out.emit("setup", "records", n);
    out.emit(
        "setup",
        "raw_mib",
        format!("{:.0}", data.raw as f64 / MIB as f64),
    );
    out.emit(
        "setup",
        "generate_s",
        format!("{:.1}", t.elapsed().as_secs_f64()),
    );
    let memory = MemoryReference::capture();
    memory.emit(&out);
    thread::spawn(|| {
        loop {
            let used = rss() + disk_used();
            if used > LIMIT {
                eprintln!("ABORT: RSS + RAM disk {} MiB exceeds the limit", used >> 20);
                std::process::exit(79);
            }
            thread::sleep(Duration::from_millis(100));
        }
    });
    let dir = if engine == "tessera" {
        format!("{outdir}/{ds}-{engine}")
    } else {
        format!("{MOUNT}/{ds}-{engine}")
    };
    let open = |name: &str| {
        let path = format!("{dir}/{name}");
        fs::create_dir_all(&dir).unwrap();
        match engine {
            "tessera" => Store::tessera(),
            "r-small" => Store::rocks(&path, 128 << 20),
            _ => Store::rocks(&path, data.raw as u64 * 5 / 4),
        }
    };

    // 1. Shuffled load in 256-record batches, drained.
    if load {
        let store = open("db");
        if engine != "tessera" {
            copy_options(
                &format!("{dir}/db"),
                &format!("{outdir}/OPTIONS-{ds}-{engine}"),
            );
        }
        let span = Span::new();
        trace_phase("load");
        let mut b = store.batch();
        for chunk in perm.chunks(256) {
            for &i in chunk {
                b.push(data.recs[i as usize].key(), Some(data.value(i as usize)));
            }
            store.write(&mut b);
        }
        store.drain();
        span.finish(&out, "load", n, data.raw);
        let c = store.counters();
        amplification(&out, "load", [0; 3], c);
        memory.report(&store, &out, "load", data.live_bytes());
        gate(&store, &data, &mut rng, &out, "load");

        if update {
            // 3. Update half the keys, then delete 2%, drained.
            let c0 = store.counters();
            let span = Span::new();
            trace_phase("update");
            let mut bytes = 0;
            for chunk in data.updates.chunks(256) {
                for &i in chunk {
                    let v = data.update(i as usize);
                    bytes += data.key_len + v.len();
                    b.push(data.recs[i as usize].key(), Some(v));
                }
                store.write(&mut b);
            }
            for chunk in data.deletes.chunks(256) {
                for &i in chunk {
                    b.push(data.recs[i as usize].key(), None);
                }
                store.write(&mut b);
            }
            store.drain();
            let ops = data.updates.len() + data.deletes.len();
            let update_rate = span.finish(&out, "update", ops, bytes);
            amplification(&out, "update", c0, store.counters());
            for &i in &data.updates {
                data.state[i as usize] = UPDATED;
            }
            for &i in &data.deletes {
                data.state[i as usize] = DELETED;
            }
            memory.report(&store, &out, "update", data.live_bytes());
            gate(&store, &data, &mut rng, &out, "update");

            // 4–6. Reads, scans, scaling, zipf and the mixed workload on the updated DB.
            if wants(cases, "reads") {
                reads(&store, &data, &mut rng, &out);
            }
            if wants(cases, "scan") {
                scans(&store, &data, &mut rng, &out);
            }
            if wants(cases, "reader_scale") {
                scaling::reader_scale(&store, &data, &mut rng, &out);
            }
            if wants(cases, "zipf") {
                scaling::zipf_workloads(&store, &data, &mut rng, &out);
            }
            if engine != "r-large" && wants(cases, "mixed") {
                let rate = if rate > 0.0 { rate } else { 0.7 * update_rate };
                mixed(&store, &data, &mut rng, &out, rate);
            }
        }
        drop(store);
        let _ = fs::remove_dir_all(&dir);
    }

    if wants(cases, "writer_scale") {
        scaling::writer_scale(engine, &data, &dir, &out);
        let _ = fs::remove_dir_all(&dir);
    }

    // 2. Sorted bulk load in ~64 MiB batches into a fresh DB, drained.
    if engine != "r-large" && wants(cases, "bulk") {
        data.state.fill(ORIGINAL);
        let store = open("bulk");
        fs::create_dir_all(format!("{dir}/sst")).unwrap();
        let span = Span::new();
        trace_phase("bulk");
        let mut b = store.batch();
        let (mut bytes, mut files) = (0, 0);
        for i in 0..n {
            let v = data.value(i);
            bytes += data.key_len + v.len();
            b.push(data.recs[i].key(), Some(v));
            if bytes >= 64 * MIB || i + 1 == n {
                store.ingest(&mut b, &format!("{dir}/sst/{files}.sst"));
                (bytes, files) = (0, files + 1);
            }
        }
        store.drain();
        span.finish(&out, "bulk", n, data.raw);
        let c = store.counters();
        out.emit("bulk", "compaction_mib", c[1] >> 20);
        gate(&store, &data, &mut rng, &out, "bulk");
        drop(store);
        let _ = fs::remove_dir_all(&dir);
    }
}

fn copy_options(db: &str, to: &str) {
    let mut names: Vec<String> = fs::read_dir(db)
        .unwrap()
        .map(|e| e.unwrap().file_name().into_string().unwrap())
        .filter(|n| n.starts_with("OPTIONS-"))
        .collect();
    names.sort_by_key(|n| n[8..].parse::<u64>().unwrap_or(0));
    fs::copy(format!("{db}/{}", names.last().unwrap()), to).unwrap();
}

/// (flush + compaction bytes written) ÷ flush bytes, over one case.
fn amplification(out: &Out, case: &str, before: [u64; 3], after: [u64; 3]) {
    let (flush, compact) = (after[0] - before[0], after[1] - before[1]);
    out.emit(case, "flush_mib", flush >> 20);
    out.emit(case, "compaction_mib", compact >> 20);
    out.emit(
        case,
        "write_amp",
        format!("{:.2}", (flush + compact) as f64 / flush as f64),
    );
    out.emit(case, "stored_mib", after[2] >> 20);
    out.emit(case, "stored_bytes", after[2]);
}

/// Checks 20k sampled keys and 200 group scans against the oracle.
fn gate(store: &Store, data: &Data, rng: &mut Rng, out: &Out, case: &str) {
    let mut r = store.reader();
    let mut bad = 0;
    for _ in 0..20_000 {
        let i = rng.n(data.recs.len());
        bad += store.get(data.recs[i].key(), &mut r, |v| v != data.live(i)) as usize;
    }
    for _ in 0..200 {
        let (s, e) = data.groups[rng.n(data.groups.len())];
        let mut want = (s..e).filter_map(|i| data.live(i).map(|v| (data.recs[i].key(), v)));
        let mut ok = true;
        store.scan(&data.recs[s].key()[..8], &mut r, |k, v| {
            ok &= want.next().is_some_and(|(wk, wv)| k == wk && v == wv);
        });
        bad += (!ok || want.next().is_some()) as usize;
    }
    out.emit(
        case,
        "gate",
        if bad == 0 {
            "pass".to_string()
        } else {
            format!("FAIL({bad})")
        },
    );
}

fn expect(data: &Data, i: u32) -> Option<usize> {
    (i != MISS)
        .then(|| data.live(i as usize).map(<[u8]>::len))
        .flatten()
}

/// 200k random live keys plus 10% misses, in random order.
fn read_keys(data: &Data, rng: &mut Rng) -> (Vec<KeyBuf>, Vec<u32>) {
    let mut v = Vec::with_capacity(220_000);
    while v.len() < 200_000 {
        let i = rng.n(data.recs.len());
        if data.state[i] != DELETED {
            v.push((data.recs[i].key_buf(), i as u32));
        }
    }
    for _ in 0..20_000 {
        v.push((data.miss(rng.n(data.recs.len())), MISS));
    }
    rng.shuffle(&mut v);
    v.into_iter().unzip()
}

fn timed_reads(
    store: &Store,
    queries: (&[KeyBuf], &[u32]),
    threads: usize,
    batch_size: usize,
    case: &str,
    out: &Out,
    op: impl Fn(&mut store::Reader<'_>, &[KeyBuf], &[u32], &mut Histogram<u64>) -> usize + Sync,
) {
    let (keys, want) = queries;
    let chunk = keys.len().div_ceil(threads);
    let ready = Barrier::new(threads);
    let start = OnceLock::new();
    let parts = thread::scope(|scope| {
        let handles: Vec<_> = keys
            .chunks(chunk)
            .zip(want.chunks(chunk))
            .map(|(keys, want)| {
                let (ready, start, op) = (&ready, &start, &op);
                scope.spawn(move || {
                    let mut reader = store.reader();
                    let mut h = hist();
                    let mut batches = keys.chunks(batch_size).zip(want.chunks(batch_size)).cycle();
                    let warm_until = Instant::now() + Duration::from_millis(150);
                    while Instant::now() < warm_until {
                        let (keys, want) = batches.next().unwrap();
                        op(&mut reader, keys, want, &mut h);
                    }
                    h.reset();
                    ready.wait();
                    let span = start.get_or_init(|| {
                        trace_phase(case);
                        Span::new()
                    });
                    let until = span.t + Duration::from_millis(1500);
                    let (mut ops, mut bad) = (0, 0);
                    while Instant::now() < until {
                        let (keys, want) = batches.next().unwrap();
                        bad += op(&mut reader, keys, want, &mut h);
                        ops += keys.len();
                    }
                    (h, ops, bad)
                })
            })
            .collect();
        handles
            .into_iter()
            .map(|h| h.join().unwrap())
            .collect::<Vec<_>>()
    });
    start
        .get()
        .unwrap()
        .finish(out, case, parts.iter().map(|p| p.1).sum(), 0);
    let mut h = hist();
    for (part, _, _) in &parts {
        h.add(part).unwrap();
    }
    out.lat(case, &h);
    out.emit(case, "errors", parts.iter().map(|p| p.2).sum::<usize>());
}

fn reads(store: &Store, data: &Data, rng: &mut Rng, out: &Out) {
    let (keys, want) = read_keys(data, rng);
    let check = |bad: usize, case: &str| out.emit(case, "errors", bad);
    // (a) one thread, get.
    let mut r = store.reader();
    let (mut h, mut bad) = (hist(), 0);
    let span = Span::new();
    for (k, &i) in keys.iter().zip(&want) {
        let t = Instant::now();
        let got = store.get(k.as_slice(), &mut r, |v| v.map(<[u8]>::len));
        rec(&mut h, t);
        bad += (got != expect(data, i)) as usize;
    }
    span.finish(out, "get_1t", keys.len(), 0);
    out.lat("get_1t", &h);
    check(bad, "get_1t");
    // (b) four threads, get, the same keys split four ways.
    timed_reads(
        store,
        (&keys, &want),
        4,
        1,
        "get_4t",
        out,
        |r, keys, want, h| {
            let t = Instant::now();
            let got = store.get(keys[0].as_slice(), r, |v| v.map(<[u8]>::len));
            rec(h, t);
            usize::from(got != expect(data, want[0]))
        },
    );
    // (c) multi_get, 100 random keys per batch.
    timed_reads(
        store,
        (&keys, &want),
        1,
        100,
        "mget_random",
        out,
        |r, keys, want, h| {
            let mut bad = 0;
            let t = Instant::now();
            store.multi_get(keys, false, r, |j, v| {
                bad += usize::from(v.map(<[u8]>::len) != expect(data, want[j]));
            });
            rec(h, t);
            bad
        },
    );
    // (d) multi_get, 100 adjacent keys per batch, sorted.
    let starts: Vec<usize> = (0..2000).map(|_| rng.n(data.recs.len() - 100)).collect();
    let sorted: Vec<KeyBuf> = starts
        .iter()
        .flat_map(|&s| (s..s + 100).map(|i| data.recs[i].key_buf()))
        .collect();
    let (mut h, mut bad) = (hist(), 0);
    let span = Span::new();
    for (ks, &s) in sorted.chunks(100).zip(&starts) {
        let t = Instant::now();
        store.multi_get(ks, true, &mut r, |j, v| {
            bad += (v.map(<[u8]>::len) != expect(data, (s + j) as u32)) as usize
        });
        rec(&mut h, t);
    }
    span.finish(out, "mget_sorted", sorted.len(), 0);
    out.lat("mget_sorted", &h);
    check(bad, "mget_sorted");
}

fn scans(store: &Store, data: &Data, rng: &mut Rng, out: &Out) {
    let count: usize =
        std::env::var("TESSERA_BENCH_SCAN_COUNT").map_or(20_000, |s| s.parse().unwrap());
    let picks: Vec<usize> = (0..count)
        .map(|_| data.scan_groups[rng.n(data.scan_groups.len())])
        .collect();
    let want: Vec<usize> = picks
        .iter()
        .map(|&g| {
            (data.groups[g].0..data.groups[g].1)
                .filter(|&i| data.state[i] != DELETED)
                .count()
        })
        .collect();
    let mut r = store.reader();
    let (mut h, mut bad, mut total) = (hist(), 0, 0);
    let span = Span::new();
    trace_phase("scan");
    for (&g, &w) in picks.iter().zip(&want) {
        let mut count = 0;
        let t = Instant::now();
        store.scan(&data.recs[data.groups[g].0].key()[..8], &mut r, |_, v| {
            black_box(v);
            count += 1;
        });
        rec(&mut h, t);
        bad += (count != w) as usize;
        total += count;
    }
    span.finish(out, "scan", picks.len(), 0);
    out.emit(
        "scan",
        "records_per_scan",
        format!("{:.1}", total as f64 / picks.len() as f64),
    );
    out.lat("scan", &h);
    out.emit("scan", "errors", bad);
}

/// 20 s: four closed-loop readers; one writer offering `rate` records/s in
/// 256-record batches during 1 s bursts separated by 1 s idle.
fn mixed(store: &Store, data: &Data, rng: &mut Rng, out: &Out, rate: f64) {
    let (keys, _) = read_keys(data, rng);
    let stop = AtomicBool::new(false);
    let (readers, writer) = thread::scope(|s| {
        let readers: Vec<_> = (0..4u64)
            .map(|t| {
                let (keys, stop) = (&keys, &stop);
                s.spawn(move || {
                    let mut r = store.reader();
                    let mut rng = Rng(0x726561646572 + t);
                    let (mut h, mut n) = (hist(), 0u64);
                    while !stop.load(Ordering::Relaxed) {
                        let t = Instant::now();
                        store.get(keys[rng.n(keys.len())].as_slice(), &mut r, |v| {
                            black_box(v.is_some())
                        });
                        rec(&mut h, t);
                        n += 1;
                    }
                    (h, n)
                })
            })
            .collect();
        let interval = Duration::from_secs_f64(256.0 / rate);
        let start = Instant::now();
        let (mut max, mut lag, mut written, mut next) =
            (Duration::ZERO, Duration::ZERO, 0usize, 0usize);
        let mut batch_latencies = hist();
        let mut b = store.batch();
        for burst in (0..20).step_by(2) {
            let burst = start + Duration::from_secs(burst);
            let mut due = burst;
            while due < burst + Duration::from_secs(1) {
                let now = Instant::now();
                if due > now {
                    thread::sleep(due - now);
                } else {
                    lag = lag.max(now - due);
                }
                for _ in 0..256 {
                    let i = data.updates[next % data.updates.len()] as usize;
                    b.push(data.recs[i].key(), Some(data.update(i)));
                    next += 1;
                }
                let t = Instant::now();
                store.write(&mut b);
                let elapsed = t.elapsed();
                max = max.max(elapsed);
                batch_latencies.saturating_record(elapsed.as_nanos() as u64);
                written += 256;
                due += interval;
            }
        }
        let end = start + Duration::from_secs(20);
        if let Some(d) = end.checked_duration_since(Instant::now()) {
            thread::sleep(d);
        }
        stop.store(true, Ordering::Relaxed);
        let wall = start.elapsed().as_secs_f64();
        let readers: Vec<_> = readers.into_iter().map(|h| h.join().unwrap()).collect();
        (
            readers,
            (
                batch_latencies.value_at_quantile(0.99),
                max,
                lag,
                written,
                wall,
            ),
        )
    });
    let mut h = hist();
    readers.iter().for_each(|r| h.add(&r.0).unwrap());
    let (p99_ns, max, lag, written, wall) = writer;
    out.emit("mixed", "offered_rec_s", format!("{rate:.0}"));
    out.emit("mixed", "written_records", written);
    out.emit(
        "mixed",
        "writer_p99_batch_ms",
        format!("{:.2}", p99_ns as f64 / 1e6),
    );
    out.emit(
        "mixed",
        "writer_max_batch_ms",
        format!("{:.2}", max.as_secs_f64() * 1e3),
    );
    out.emit(
        "mixed",
        "writer_max_lag_ms",
        format!("{:.1}", lag.as_secs_f64() * 1e3),
    );
    out.emit(
        "mixed",
        "reader_ops_s",
        format!(
            "{:.0}",
            readers.iter().map(|r| r.1).sum::<u64>() as f64 / wall
        ),
    );
    out.lat("mixed", &h);
}

#[cfg(test)]
mod memory_tests {
    use super::*;

    #[test]
    fn live_heap_counts_and_releases_large_allocations() {
        // Well above the other harness tests' bounded scratch/noise.
        let before = live_heap();
        let allocation = vec![7u8; 32 * MIB];
        black_box(&allocation);
        let allocated = live_heap();
        assert!(allocated >= before + 24 * MIB as u64);
        drop(allocation);
        assert!(live_heap() + 16 * (MIB as u64) < allocated);
    }

    #[test]
    fn summaries_use_heap_and_rss_and_leave_footprint_in_csv_only() {
        let rows = [
            "engine_heap_mib",
            "engine_ram_mib",
            "engine_footprint_mib",
            "baseline_footprint_mib",
            "live_per_heap",
            "live_per_rss",
        ]
        .map(|metric| {
            [
                "A".into(),
                "tessera".into(),
                "update".into(),
                metric.into(),
                "1".into(),
            ]
        });
        let text = summary(&rows, 0.0, &SCALE_ENGINES);
        assert!(
            text.contains("engine_heap_mib")
                && text.contains("live_per_heap")
                && text.contains("live_per_rss")
        );
        assert!(!text.contains("footprint"));
    }
}
