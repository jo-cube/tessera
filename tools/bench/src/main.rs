//! Tessera vs RocksDB comparison. `bench all OUTDIR` runs one child process
//! per dataset and engine (`bench case ...`) and writes results.csv and
//! summary.md. See README.md.
mod data;
mod store;
use data::*;
use hdrhistogram::Histogram;
use std::{
    collections::HashMap,
    fmt::Write as _,
    fs,
    hint::black_box,
    io::{BufRead, BufReader, Write},
    process::{Command, Stdio},
    sync::atomic::{AtomicBool, Ordering},
    thread,
    time::{Duration, Instant},
};
use store::{Reader, Store};

const MOUNT: &str = "/Volumes/tessbench";
/// Process RSS plus RAM-disk bytes beyond which a child aborts.
const LIMIT: u64 = 8 << 30;
const ENGINES: [&str; 3] = ["tessera", "r-small", "r-large"];
const MISS: u32 = u32::MAX;

fn main() {
    let args: Vec<String> = std::env::args().collect();
    match args.get(1).map(String::as_str) {
        Some("all") => all(&args[2], &Filter::parse(&args[3..])),
        Some("case") => case(args[2].chars().next().unwrap(), &args[3], &args[4], args[5].parse().unwrap(), &args[6]),
        _ => panic!("usage: bench all OUTDIR [--dataset A,B] [--engine E,..] [--case C,..]"),
    }
}

// ---- measurement ----

fn rss() -> u64 {
    unsafe { store::tb_rss() }
}

fn disk_used() -> u64 {
    let p = std::ffi::CString::new(MOUNT).unwrap();
    let mut s = std::mem::MaybeUninit::<libc::statfs>::uninit();
    assert_eq!(unsafe { libc::statfs(p.as_ptr(), s.as_mut_ptr()) }, 0, "RAM disk missing");
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

struct Out {
    prefix: String,
}
impl Out {
    fn emit(&self, case: &str, metric: &str, value: impl std::fmt::Display) {
        println!("{},{case},{metric},{value}", self.prefix);
    }
    fn lat(&self, case: &str, h: &Histogram<u64>) {
        for (m, q) in [("p50_us", 0.5), ("p99_us", 0.99), ("p999_us", 0.999)] {
            self.emit(case, m, format!("{:.2}", h.value_at_quantile(q) as f64 / 1e3));
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
        Self { t: Instant::now(), cpu: cpu() }
    }
    /// Emits wall time, rate and CPU; returns operations per second.
    fn finish(&self, out: &Out, case: &str, ops: usize, bytes: usize) -> f64 {
        let wall = self.t.elapsed().as_secs_f64();
        let (u, s) = cpu();
        out.emit(case, "wall_s", format!("{wall:.2}"));
        out.emit(case, "ops_s", format!("{:.0}", ops as f64 / wall));
        if bytes > 0 {
            out.emit(case, "mib_s", format!("{:.1}", bytes as f64 / MIB as f64 / wall));
        }
        out.emit(case, "cpu_user_s", format!("{:.2}", u - self.cpu.0));
        out.emit(case, "cpu_sys_s", format!("{:.2}", s - self.cpu.1));
        ops as f64 / wall
    }
}

// ---- orchestrator ----

const CASES: &str = "load,update,reads,scan,mixed,bulk";

/// Comma-separated selections; each defaults to everything.
struct Filter {
    datasets: String,
    engines: String,
    cases: String,
}
impl Filter {
    fn parse(args: &[String]) -> Self {
        let mut f = Self { datasets: "A,B".into(), engines: ENGINES.join(","), cases: CASES.into() };
        for pair in args.chunks(2) {
            let value = pair.get(1).cloned().expect("a filter needs a value");
            match pair[0].as_str() {
                "--dataset" => f.datasets = value,
                "--engine" => f.engines = value,
                "--case" => f.cases = value,
                other => panic!("unknown option {other}"),
            }
        }
        for c in f.cases.split(',') {
            assert!(wants(CASES, c), "unknown case {c}; cases are {CASES}");
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
    for ds in ["A", "B"].into_iter().filter(|d| wants(&filter.datasets, d)) {
        let mut rate = 0.0;
        for engine in ENGINES.into_iter().filter(|e| wants(&filter.engines, e)) {
            eprintln!("[{:>5.0}s] {ds} {engine}", started.elapsed().as_secs_f64());
            let mut child = Command::new(&exe)
                .args(["case", ds, engine, outdir, &rate.to_string(), &filter.cases])
                .stdout(Stdio::piped())
                .spawn()
                .unwrap();
            let stdout = child.stdout.take().unwrap();
            let lines = thread::spawn(move || BufReader::new(stdout).lines().map(Result::unwrap).collect::<Vec<_>>());
            let deadline = Instant::now() + Duration::from_secs(300);
            let status = loop {
                if let Some(s) = child.try_wait().unwrap() {
                    break s.to_string();
                }
                if Instant::now() > deadline || started.elapsed() > Duration::from_secs(900) {
                    child.kill().unwrap();
                    child.wait().unwrap();
                    break "killed: timeout".into();
                }
                thread::sleep(Duration::from_millis(100));
            };
            let mut lines = lines.join().unwrap();
            if status != "exit status: 0" {
                lines.push(format!("{ds},{engine},process,status,{}", status.replace(',', ";")));
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
    fs::write(format!("{outdir}/summary.md"), summary(&rows, total)).unwrap();
    eprintln!("total {total:.1} s; results in {outdir}");
}

fn summary(rows: &[[String; 5]], total: f64) -> String {
    let mut s = format!("# Tessera vs RocksDB {}\n\nTotal runtime {total:.0} s.\n", store::rocksdb_version());
    for ds in ["A", "B"] {
        let mut order = Vec::new();
        let mut cells = HashMap::new();
        for r in rows.iter().filter(|r| r[0] == ds) {
            let key = (r[2].clone(), r[3].clone());
            if !order.contains(&key) {
                order.push(key.clone());
            }
            cells.insert((key, r[1].clone()), r[4].clone());
        }
        let _ = write!(s, "\n## Dataset {ds}\n\n| case | metric | {} |\n|---|---|{}\n", ENGINES.join(" | "), "---|".repeat(3));
        for key in order {
            let v: Vec<&str> = ENGINES.iter().map(|e| cells.get(&(key.clone(), e.to_string())).map_or("–", String::as_str)).collect();
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
    let on_updated = ["reads", "scan", "mixed"].iter().any(|c| wants(cases, c));
    let update = on_updated || wants(cases, "update");
    let load = update || wants(cases, "load");
    let out = Out { prefix: format!("{ds},{engine}") };
    let t = Instant::now();
    let mut data = generate(ds);
    let n = data.recs.len();
    let mut rng = Rng(0x62656e6368 ^ ds as u64);
    let mut perm: Vec<u32> = (0..n as u32).collect();
    rng.shuffle(&mut perm);
    out.emit("setup", "records", n);
    out.emit("setup", "raw_mib", format!("{:.0}", data.raw as f64 / MIB as f64));
    out.emit("setup", "generate_s", format!("{:.1}", t.elapsed().as_secs_f64()));
    let (base_rss, base_disk) = (rss(), disk_used());
    out.emit("setup", "baseline_rss_mib", base_rss >> 20);
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
    let dir = format!("{MOUNT}/{ds}-{engine}");
    let open = |name: &str| {
        let path = format!("{dir}/{name}");
        fs::create_dir_all(&dir).unwrap();
        match engine {
            "tessera" => Store::tessera(),
            "r-small" => Store::rocks(&path, 128 << 20),
            _ => Store::rocks(&path, data.raw as u64 * 5 / 4),
        }
    };
    let mem = |out: &Out, case: &str, data: &Data| {
        let disk = disk_used().saturating_sub(base_disk);
        let ram = rss().saturating_sub(base_rss) + if engine == "tessera" { 0 } else { disk };
        let live = data.live_bytes();
        out.emit(case, "engine_ram_mib", ram >> 20);
        out.emit(case, "ram_disk_mib", disk >> 20);
        out.emit(case, "live_mib", live >> 20);
        out.emit(case, "live_per_ram", format!("{:.2}", live as f64 / ram as f64));
    };

    // 1. Shuffled load in 256-record batches, drained.
    if load {
        let store = open("db");
        if engine != "tessera" {
            copy_options(&format!("{dir}/db"), &format!("{outdir}/OPTIONS-{ds}-{engine}"));
        }
        let span = Span::new();
        let mut b = store.batch();
        for chunk in perm.chunks(256) {
            for &i in chunk {
                b.push(&data.recs[i as usize].key, Some(data.value(i as usize)));
            }
            store.write(&mut b);
        }
        store.drain();
        span.finish(&out, "load", n, data.raw);
        let c = store.counters();
        amplification(&out, "load", [0; 3], c);
        mem(&out, "load", &data);
        gate(&store, &data, &mut rng, &out, "load");

        if update {
            // 3. Update half the keys, then delete 2%, drained.
            let c0 = store.counters();
            let span = Span::new();
            let mut bytes = 0;
            for chunk in data.updates.chunks(256) {
                for &i in chunk {
                    let v = data.update(i as usize);
                    bytes += 16 + v.len();
                    b.push(&data.recs[i as usize].key, Some(v));
                }
                store.write(&mut b);
            }
            for chunk in data.deletes.chunks(256) {
                for &i in chunk {
                    b.push(&data.recs[i as usize].key, None);
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
            mem(&out, "update", &data);
            gate(&store, &data, &mut rng, &out, "update");

            // 4–6. Reads, scans and the mixed workload on the updated DB.
            if wants(cases, "reads") {
                reads(&store, &data, &mut rng, &out);
            }
            if wants(cases, "scan") {
                scans(&store, &data, &mut rng, &out);
            }
            if engine != "r-large" && wants(cases, "mixed") {
                let rate = if rate > 0.0 { rate } else { 0.7 * update_rate };
                mixed(&store, &data, &mut rng, &out, rate);
            }
        }
        drop(store);
        let _ = fs::remove_dir_all(&dir);
    }

    // 2. Sorted bulk load in ~64 MiB batches into a fresh DB, drained.
    if engine != "r-large" && wants(cases, "bulk") {
        data.state.fill(ORIGINAL);
        let store = open("bulk");
        fs::create_dir_all(format!("{dir}/sst")).unwrap();
        let span = Span::new();
        let mut b = store.batch();
        let (mut bytes, mut files) = (0, 0);
        for i in 0..n {
            let v = data.value(i);
            bytes += 16 + v.len();
            b.push(&data.recs[i].key, Some(v));
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
    out.emit(case, "write_amp", format!("{:.2}", (flush + compact) as f64 / flush as f64));
    out.emit(case, "stored_mib", after[2] >> 20);
}

/// Checks 20k sampled keys and 200 group scans against the oracle.
fn gate(store: &Store, data: &Data, rng: &mut Rng, out: &Out, case: &str) {
    let mut r = Reader::new();
    let mut bad = 0;
    for _ in 0..20_000 {
        let i = rng.n(data.recs.len());
        bad += store.get(&data.recs[i].key, &mut r, |v| v != data.live(i)) as usize;
    }
    for _ in 0..200 {
        let (s, e) = data.groups[rng.n(data.groups.len())];
        let mut want = (s..e).filter_map(|i| data.live(i).map(|v| (&data.recs[i].key, v)));
        let mut ok = true;
        store.scan(&data.recs[s].key[..8], &mut r, |k, v| {
            ok &= want.next().is_some_and(|(wk, wv)| k == wk && v == wv);
        });
        bad += (!ok || want.next().is_some()) as usize;
    }
    out.emit(case, "gate", if bad == 0 { "pass".to_string() } else { format!("FAIL({bad})") });
}

fn expect(data: &Data, i: u32) -> Option<usize> {
    (i != MISS).then(|| data.live(i as usize).map(<[u8]>::len)).flatten()
}

/// 200k random live keys plus 10% misses, in random order.
fn read_keys(data: &Data, rng: &mut Rng) -> (Vec<[u8; 16]>, Vec<u32>) {
    let mut v = Vec::with_capacity(220_000);
    while v.len() < 200_000 {
        let i = rng.n(data.recs.len());
        if data.state[i] != DELETED {
            v.push((data.recs[i].key, i as u32));
        }
    }
    for _ in 0..20_000 {
        v.push((data.miss(rng.n(data.recs.len())), MISS));
    }
    rng.shuffle(&mut v);
    v.into_iter().unzip()
}

fn reads(store: &Store, data: &Data, rng: &mut Rng, out: &Out) {
    let (keys, want) = read_keys(data, rng);
    let check = |bad: usize, case: &str| out.emit(case, "errors", bad);
    // (a) one thread, get.
    let mut r = Reader::new();
    let (mut h, mut bad) = (hist(), 0);
    let span = Span::new();
    for (k, &i) in keys.iter().zip(&want) {
        let t = Instant::now();
        let got = store.get(k, &mut r, |v| v.map(<[u8]>::len));
        rec(&mut h, t);
        bad += (got != expect(data, i)) as usize;
    }
    span.finish(out, "get_1t", keys.len(), 0);
    out.lat("get_1t", &h);
    check(bad, "get_1t");
    // (b) four threads, get, the same keys split four ways.
    let span = Span::new();
    let parts: Vec<(Histogram<u64>, usize)> = thread::scope(|s| {
        let q = keys.len().div_ceil(4);
        let handles: Vec<_> = keys
            .chunks(q)
            .zip(want.chunks(q))
            .map(|(ks, ws)| {
                s.spawn(move || {
                    let mut r = Reader::new();
                    let (mut h, mut bad) = (hist(), 0);
                    for (k, &i) in ks.iter().zip(ws) {
                        let t = Instant::now();
                        let got = store.get(k, &mut r, |v| v.map(<[u8]>::len));
                        rec(&mut h, t);
                        bad += (got != expect(data, i)) as usize;
                    }
                    (h, bad)
                })
            })
            .collect();
        handles.into_iter().map(|h| h.join().unwrap()).collect()
    });
    span.finish(out, "get_4t", keys.len(), 0);
    let mut h = hist();
    parts.iter().for_each(|p| h.add(&p.0).unwrap());
    out.lat("get_4t", &h);
    check(parts.iter().map(|p| p.1).sum(), "get_4t");
    // (c) multi_get, 100 random keys per batch.
    let (mut h, mut bad) = (hist(), 0);
    let span = Span::new();
    for (ks, ws) in keys.chunks(100).zip(want.chunks(100)) {
        let t = Instant::now();
        store.multi_get(ks, false, &mut r, |j, v| bad += (v.map(<[u8]>::len) != expect(data, ws[j])) as usize);
        rec(&mut h, t);
    }
    span.finish(out, "mget_random", keys.len(), 0);
    out.lat("mget_random", &h);
    check(bad, "mget_random");
    // (d) multi_get, 100 adjacent keys per batch, sorted.
    let starts: Vec<usize> = (0..2000).map(|_| rng.n(data.recs.len() - 100)).collect();
    let sorted: Vec<[u8; 16]> = starts.iter().flat_map(|&s| (s..s + 100).map(|i| data.recs[i].key)).collect();
    let (mut h, mut bad) = (hist(), 0);
    let span = Span::new();
    for (ks, &s) in sorted.chunks(100).zip(&starts) {
        let t = Instant::now();
        store.multi_get(ks, true, &mut r, |j, v| bad += (v.map(<[u8]>::len) != expect(data, (s + j) as u32)) as usize);
        rec(&mut h, t);
    }
    span.finish(out, "mget_sorted", sorted.len(), 0);
    out.lat("mget_sorted", &h);
    check(bad, "mget_sorted");
}

fn scans(store: &Store, data: &Data, rng: &mut Rng, out: &Out) {
    let picks: Vec<usize> = (0..20_000).map(|_| data.scan_groups[rng.n(data.scan_groups.len())]).collect();
    let want: Vec<usize> = picks
        .iter()
        .map(|&g| (data.groups[g].0..data.groups[g].1).filter(|&i| data.state[i] != DELETED).count())
        .collect();
    let mut r = Reader::new();
    let (mut h, mut bad, mut total) = (hist(), 0, 0);
    let span = Span::new();
    for (&g, &w) in picks.iter().zip(&want) {
        let mut count = 0;
        let t = Instant::now();
        store.scan(&data.recs[data.groups[g].0].key[..8], &mut r, |_, v| {
            black_box(v);
            count += 1;
        });
        rec(&mut h, t);
        bad += (count != w) as usize;
        total += count;
    }
    span.finish(out, "scan", picks.len(), 0);
    out.emit("scan", "records_per_scan", format!("{:.1}", total as f64 / picks.len() as f64));
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
                    let mut r = Reader::new();
                    let mut rng = Rng(0x726561646572 + t);
                    let (mut h, mut n) = (hist(), 0u64);
                    while !stop.load(Ordering::Relaxed) {
                        let t = Instant::now();
                        store.get(&keys[rng.n(keys.len())], &mut r, |v| black_box(v.is_some()));
                        rec(&mut h, t);
                        n += 1;
                    }
                    (h, n)
                })
            })
            .collect();
        let interval = Duration::from_secs_f64(256.0 / rate);
        let start = Instant::now();
        let (mut max, mut lag, mut written, mut next) = (Duration::ZERO, Duration::ZERO, 0usize, 0usize);
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
                    b.push(&data.recs[i].key, Some(data.update(i)));
                    next += 1;
                }
                let t = Instant::now();
                store.write(&mut b);
                max = max.max(t.elapsed());
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
        (readers, (max, lag, written, wall))
    });
    let mut h = hist();
    readers.iter().for_each(|r| h.add(&r.0).unwrap());
    let (max, lag, written, wall) = writer;
    out.emit("mixed", "offered_rec_s", format!("{rate:.0}"));
    out.emit("mixed", "written_records", written);
    out.emit("mixed", "writer_max_batch_ms", format!("{:.2}", max.as_secs_f64() * 1e3));
    out.emit("mixed", "writer_max_lag_ms", format!("{:.1}", lag.as_secs_f64() * 1e3));
    out.emit("mixed", "reader_ops_s", format!("{:.0}", readers.iter().map(|r| r.1).sum::<u64>() as f64 / wall));
    out.lat("mixed", &h);
}
