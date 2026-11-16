//! Bounded input buffers and deterministic regeneration of the regular datasets.
//! Only group boundaries and RNG checkpoints are retained (no record arena,
//! per-key oracle, or shuffled index array).
use super::*;
use std::sync::{Condvar, Mutex};

const STEP: u64 = 0x9e3779b97f4a7c15;

struct Group {
    start: usize,
    original: u64,
    update: u64,
    base_len: usize,
}

struct StreamData {
    name: char,
    groups: Vec<Group>,
    n: usize,
    raw: usize,
}

fn advance(rng: &mut Rng, n: usize) {
    rng.0 = rng.0.wrapping_add(STEP.wrapping_mul(n as u64));
}

fn updated(i: usize) -> bool {
    mix(0x75706464 ^ i as u64) & 1 == 0
}

fn deleted(i: usize) -> bool {
    (mix(0x75706464 ^ i as u64) >> 8).is_multiple_of(50)
}

fn digits(n: usize) -> usize {
    if n == 0 { 1 } else { n.ilog10() as usize + 1 }
}

impl StreamData {
    fn new(name: char, target: usize) -> Self {
        let mut data = Self { name, groups: Vec::new(), n: 0, raw: 0 };
        let mut rng = Rng(0x4c53504b ^ name as u64);
        let mut urng = Rng(0x55504454 ^ name as u64);
        while data.raw < target {
            let n = if name == 'A' {
                let q = rng.n(45);
                20 + q * q
            } else {
                100
            };
            let start = data.n;
            data.groups.push(Group { start, original: rng.0, update: urng.0, base_len: 0 });
            data.n += n;
            // One representative establishes the JSON's fixed-length portion.
            let len = data.record(start, ORIGINAL).1.len();
            let g = data.groups.last_mut().unwrap();
            g.base_len = if name == 'A' {
                len - 1 - 1 - 6
            } else {
                let ts = mix(g.original.wrapping_add(STEP.wrapping_mul(680 + 507))) % 2_000_000_000;
                len - digits(ts as usize)
            };
            data.raw += (start..data.n).map(|i| 16 + data.original_len(i)).sum::<usize>();
            advance(&mut rng, 680 + n * if name == 'A' { 8 } else { 507 });
            advance(&mut urng, (start..data.n).filter(|&i| updated(i)).count() * if name == 'A' { 12 } else { 101 });
        }
        data
    }

    fn group(&self, i: usize) -> (usize, &Group, usize) {
        assert!(i < self.n);
        let g = self.groups.partition_point(|g| g.start <= i) - 1;
        (g, &self.groups[g], i - self.groups[g].start)
    }

    fn key(&self, i: usize) -> [u8; 16] {
        let (g, _, member) = self.group(i);
        let mut key = [0; 16];
        key[..8].copy_from_slice(&(g as u64).to_be_bytes());
        key[8..].copy_from_slice(&(member as u64).to_be_bytes());
        key
    }

    fn original_len(&self, i: usize) -> usize {
        let (_, g, member) = self.group(i);
        if self.name == 'A' {
            g.base_len + digits(member) + digits(member % 17) + if member % 3 == 1 { 7 } else { 6 }
        } else {
            let ts = mix(g.original.wrapping_add(STEP.wrapping_mul((680 + (member + 1) * 507) as u64))) % 2_000_000_000;
            g.base_len + digits(ts as usize)
        }
    }

    fn record(&self, i: usize, version: u8) -> ([u8; 16], Vec<u8>) {
        let name = self.name;
        let (g, group, member) = self.group(i);
        let mut rng = Rng(group.original);
        let (a, b, c) = if name == 'A' {
            (rng.text(360), rng.text(260), rng.text(60))
        } else {
            advance(&mut rng, 680);
            (String::new(), String::new(), String::new())
        };
        advance(&mut rng, member * if name == 'A' { 8 } else { 507 });
        let upd = version == UPDATED;
        let mut urng = Rng(group.update);
        if upd {
            advance(&mut urng, (group.start..i).filter(|&j| updated(j)).count() * if name == 'A' { 12 } else { 101 });
        }
        let (value, update) = if name == 'A' {
            let json = |ts: usize, q: usize, p: usize, st: &str, tok: String| {
                format!(
                    r#"{{"group_id":{g},"description":"{a}","attributes":"{b}","category_text":"{c}","member_id":{member},"timestamp":{ts},"quantity":{q},"price":{p},"status":"{st}","token":"{tok}","preferences":{{"notifications":true,"locale":"en-US","timezone":"UTC","delivery":"standard","visibility":"members","version":1}},"source":"synthetic-group-members","kind":"membership-record"}}"#
                )
            };
            let st = ["active", "pending", "paused"];
            let v = json(1700000000 + member, member % 17, 1000 + member % 250, st[member % 3], rng.text(8));
            let u = upd.then(|| json(1700086400 + urng.n(86400), urng.n(17), 1000 + urng.n(250), st[urng.n(3)], urng.text(8)));
            (v, u)
        } else {
            let (id, nm, em, dom) = (rng.hex(32), rng.hex(24), rng.hex(16), rng.hex(12));
            let (free, city, acct, notes) = (rng.hex(270), rng.hex(20), rng.hex(32), rng.hex(100));
            let ts = rng.next() % 2000000000;
            let json = |free: &str, notes: &str, ts: u64, st: &str| {
                format!(
                    r#"{{"id":"{id}","name":"{nm}","email":"{em}@{dom}.org","free_text":"{free}","city":"{city}","account":"{acct}","notes":"{notes}","timestamp":{ts},"state":"{st}"}}"#
                )
            };
            let v = json(&free, &notes, ts, "active");
            let u = upd.then(|| json(&free, &urng.hex(100), urng.next() % 2000000000, "updated"));
            (v, u)
        };
        (self.key(i), if upd { update.unwrap() } else { value }.into_bytes())
    }

    fn expected(&self, i: usize, changed: bool) -> Option<Vec<u8>> {
        if changed && deleted(i) { None } else { Some(self.record(i, if changed && updated(i) { UPDATED } else { ORIGINAL }).1) }
    }
}

/// Balanced Feistel on the next even-bit power-of-two domain. Cycle walking
/// restricts a bijection to 0..n, including non-power-of-two dataset sizes.
struct Permutation {
    n: usize,
    half: u32,
    mask: u64,
    seed: u64,
}
impl Permutation {
    fn new(n: usize, seed: u64) -> Self {
        assert!(n > 0);
        let bits = (usize::BITS - (n - 1).leading_zeros()).max(2).next_multiple_of(2);
        let half = bits / 2;
        Self { n, half, mask: (1u64 << half) - 1, seed }
    }
    fn at(&self, i: usize) -> usize {
        assert!(i < self.n);
        let mut x = i as u64;
        loop {
            let (mut l, mut r) = (x >> self.half, x & self.mask);
            for round in 0..6 {
                (l, r) = (r, l ^ (mix(r ^ self.seed.wrapping_add(STEP.wrapping_mul(round))) & self.mask));
            }
            x = (l << self.half) | r;
            if x < self.n as u64 {
                return x as usize;
            }
        }
    }
}

/// Sample through drains too. The condition variable stops the sampler without
/// waiting for a sampling interval; all buffers are bounded by the run timeout.
fn measured<T>(store: &Store, trace: &mut fs::File, base: MemoryReference, origin: Instant, phase: &str, f: impl FnOnce() -> T) -> T {
    let done = (Mutex::new(false), Condvar::new());
    thread::scope(|scope| {
        let done_ref = &done;
        let sampler = scope.spawn(move || {
            let sample = |trace: &mut fs::File| {
                let heap = live_heap().saturating_sub(base.heap);
                let ram = rss().saturating_sub(base.rss);
                let physical = footprint().saturating_sub(base.footprint);
                let stored = store.counters()[2];
                writeln!(trace, "{:.2},{phase},{heap},{ram},{physical},{stored},{}", origin.elapsed().as_secs_f64(), heap as i64 - stored as i64)
                    .unwrap();
            };
            sample(trace);
            let mut stopped = done_ref.0.lock().unwrap();
            while !*stopped {
                let (guard, _) = done_ref.1.wait_timeout(stopped, Duration::from_secs(2)).unwrap();
                stopped = guard;
                if !*stopped {
                    sample(trace);
                }
            }
            sample(trace);
        });
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(f));
        *done.0.lock().unwrap() = true;
        done.1.notify_one();
        sampler.join().unwrap();
        result.unwrap_or_else(|panic| std::panic::resume_unwind(panic))
    })
}

pub(super) fn run(ds: char, engine: &str, outdir: &str, cases: &str) {
    let scale: usize = std::env::var("TESSERA_BENCH_SCALE").map_or(1, |s| s.parse().unwrap());
    assert!(scale > 0);
    let target = if ds == 'A' { 8192 * MIB } else { 4096 * MIB } / scale;
    let out = Out { prefix: format!("{ds},{engine}") };
    let setup = Instant::now();
    let data = StreamData::new(ds, target);
    let memory = MemoryReference::capture();
    out.emit("setup", "records", data.n);
    out.emit("setup", "raw_mib", format!("{:.2}", data.raw as f64 / MIB as f64));
    out.emit("setup", "generate_s", format!("{:.2}", setup.elapsed().as_secs_f64()));
    out.emit("setup", "group_index_bytes", data.groups.capacity() * std::mem::size_of::<Group>());
    memory.emit(&out);
    let peak = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));
    let sampled_peak = peak.clone();
    thread::spawn(move || {
        loop {
            let used = rss() + disk_used();
            sampled_peak.fetch_max(used, Ordering::Relaxed);
            if used >= 12 << 30 {
                eprintln!("ABORT: 12 GiB RSS + RAM disk limit");
                std::process::exit(79);
            }
            thread::sleep(Duration::from_millis(100));
        }
    });
    let dir = if engine == "tessera" { format!("{outdir}/{ds}-{engine}") } else { format!("{MOUNT}/{ds}-{engine}") };
    fs::create_dir_all(&dir).unwrap();
    let store = match engine {
        "tessera" => Store::tessera(),
        "r-small" => Store::rocks(&format!("{dir}/db"), 128 << 20),
        "r-mid" => Store::rocks(&format!("{dir}/db"), 1 << 30),
        _ => panic!("unsupported streaming engine {engine}"),
    };
    if engine != "tessera" {
        copy_options(&format!("{dir}/db"), &format!("{outdir}/OPTIONS-{ds}-{engine}"));
    }
    let mut trace = fs::File::create(format!("{outdir}/samples-{ds}-{engine}.csv")).unwrap();
    writeln!(trace, "time_s,phase,live_heap_bytes,rss_bytes,footprint_bytes,stored_bytes,heap_overhead_bytes").unwrap();
    let origin = Instant::now();
    let mut live = data.raw;
    let mut b = store.batch();
    let mut records = Vec::with_capacity(256);
    for phase in ["load", "update"] {
        if phase == "update" && !(wants(cases, "update") || wants(cases, "reads")) {
            break;
        }
        let before = store.counters();
        let mut write = || {
            let span = Span::new();
            let (mut ops, mut bytes) = (0, 0);
            let kinds: &[u8] = if phase == "load" { &[ORIGINAL] } else { &[UPDATED, DELETED] };
            for &kind in kinds {
                let perm = Permutation::new(data.n, 0x62656e6368 ^ ds as u64 ^ ((kind as u64) << 32));
                for index in 0..data.n {
                    let i = perm.at(index);
                    if kind == UPDATED && !updated(i) || kind == DELETED && !deleted(i) {
                        continue;
                    }
                    let rec = data.record(
                        i,
                        if kind == DELETED && updated(i) {
                            UPDATED
                        } else if kind == DELETED {
                            ORIGINAL
                        } else {
                            kind
                        },
                    );
                    if kind == UPDATED {
                        live = live + rec.1.len() - data.original_len(i);
                    }
                    if kind == DELETED {
                        live -= 16 + rec.1.len();
                    }
                    bytes += 16 + if kind == DELETED { 0 } else { rec.1.len() };
                    records.push(rec);
                    ops += 1;
                    if records.len() == 256 {
                        for (key, value) in &records {
                            b.push(key, (kind != DELETED).then_some(value.as_slice()));
                        }
                        store.write(&mut b);
                        records.clear();
                    }
                }
                if !records.is_empty() {
                    for (key, value) in &records {
                        b.push(key, (kind != DELETED).then_some(value.as_slice()));
                    }
                    store.write(&mut b);
                    records.clear();
                }
            }
            store.drain();
            span.finish(&out, phase, ops, bytes);
        };
        if engine == "tessera" {
            measured(&store, &mut trace, memory, origin, phase, write);
        } else {
            write();
        }
        amplification(&out, phase, before, store.counters());
        memory.report(&store, &out, phase, live);
        let mut rng = Rng(0x67617465 ^ ds as u64);
        let mut reader = Reader::new();
        let mut bad = 0;
        for _ in 0..20_000 {
            let i = rng.n(data.n);
            let expected = data.expected(i, phase == "update");
            bad += store.get(&data.key(i), &mut reader, |v| v != expected.as_deref()) as usize;
        }
        out.emit(phase, "gate", if bad == 0 { "pass" } else { "FAIL" });
        assert_eq!(bad, 0, "streaming correctness gate");
    }
    if wants(cases, "reads") {
        point_reads(&store, &data, &out);
    }
    let mut usage = std::mem::MaybeUninit::<libc::rusage>::uninit();
    assert_eq!(unsafe { libc::getrusage(libc::RUSAGE_SELF, usage.as_mut_ptr()) }, 0);
    out.emit("process", "peak_rss_mib", unsafe { usage.assume_init() }.ru_maxrss as u64 >> 20);
    out.emit("process", "peak_rss_plus_disk_mib", peak.load(Ordering::Relaxed) >> 20);
    drop(store);
    fs::remove_dir_all(dir).unwrap();
}

fn point_reads(store: &Store, data: &StreamData, out: &Out) {
    let mut rng = Rng(0x7265616473 ^ data.name as u64);
    // A bounded 200k query sample is independent of dataset size. Regenerate
    // expectations before timing, and check full values in the gates above.
    let queries: Vec<_> = (0..200_000)
        .map(|j| {
            let i = rng.n(data.n);
            let mut key = data.key(i);
            let expected = if j % 10 == 0 {
                key[8..].copy_from_slice(&u64::MAX.to_be_bytes());
                None
            } else {
                data.expected(i, true).map(|v| v.len())
            };
            (key, expected)
        })
        .collect();
    for threads in [1, 4] {
        let span = Span::new();
        let bad: usize = thread::scope(|scope| {
            let handles: Vec<_> = queries
                .chunks(queries.len() / threads)
                .map(|chunk| {
                    scope.spawn(move || {
                        let mut reader = Reader::new();
                        chunk.iter().filter(|(key, expected)| store.get(key, &mut reader, |v| v.map(<[u8]>::len)) != *expected).count()
                    })
                })
                .collect();
            handles.into_iter().map(|h| h.join().unwrap()).sum()
        });
        let case = format!("get_{threads}t");
        span.finish(out, &case, queries.len(), 0);
        out.emit(&case, "errors", bad);
        assert_eq!(bad, 0, "streaming timed reads");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn regeneration_matches_the_regular_arena_and_oracle() {
        for name in ['A', 'B'] {
            let original = generate_target(name, 2 * MIB);
            let stream = StreamData::new(name, 2 * MIB);
            assert_eq!(stream.n, original.recs.len());
            assert_eq!(stream.raw, original.raw);
            let mut live = stream.raw;
            for i in 0..stream.n {
                assert_eq!(stream.key(i), original.recs[i].key);
                assert_eq!(stream.record(i, ORIGINAL).1, original.value(i));
                assert_eq!(stream.original_len(i), original.value(i).len());
                assert_eq!(updated(i), original.updates.contains(&(i as u32)));
                assert_eq!(deleted(i), original.deletes.contains(&(i as u32)));
                if updated(i) {
                    assert_eq!(stream.record(i, UPDATED).1, original.update(i));
                    live = live + original.update(i).len() - original.value(i).len();
                }
                let expected = if deleted(i) {
                    None
                } else if updated(i) {
                    Some(original.update(i))
                } else {
                    Some(original.value(i))
                };
                assert_eq!(stream.expected(i, true).as_deref(), expected);
            }
            for &i in &original.deletes {
                live -= 16 + if updated(i as usize) { original.update(i as usize).len() } else { original.value(i as usize).len() };
            }
            assert_eq!(live, (0..stream.n).filter_map(|i| stream.expected(i, true)).map(|v| 16 + v.len()).sum::<usize>());
        }
    }

    #[test]
    fn cycle_walk_visits_each_index_once_without_an_order_array() {
        for n in [1, 2, 3, 4, 17, 255, 256, 257, 10007] {
            for seed in [0, 42, u64::MAX] {
                let p = Permutation::new(n, seed);
                let mut seen = vec![false; n];
                for i in 0..n {
                    let j = p.at(i);
                    assert!(!seen[j]);
                    seen[j] = true;
                    assert_eq!(j, p.at(i));
                }
                assert!(seen.into_iter().all(|v| v));
            }
        }
    }
}
