//! Work and memory counters by level over deterministic inputs, using the
//! hidden `diagnostics` feature. Each scenario loads keys in random order with
//! automatic flushes, updates half of them at random, waits for background
//! compaction, checks every read path against a version oracle and prints
//! long-format CSV rows. See tools/README.md.
//!
//! `counters [--variant LABEL] [--settle] [--scale SHIFT] [--copy-audit]
//! [--c2] [--dict-scope] [--write-only] [--memtable VALUE_LEN]
//! [--memtable-key KEY_LEN] [--worst] [--zipf] [--read-shapes] [--reuse] [--keyspaces N]
//! [SCENARIO...]`
//!
//! With `--keyspaces N`, record `i` goes to keyspace `i % N`, so the same
//! records spread over N keyspaces; every read is checked in its keyspace.
use std::cell::RefCell;
use std::rc::Rc;
use std::time::Instant;
use tessera::diagnostics::{
    self, EngineDiagnostics, MergeWorkspace, ReaderDiagnostics, SegmentSample,
};
use tessera::{Engine, Keyspace, Options};

pub(crate) mod c2;
#[cfg(all(feature = "tools-memory", target_os = "linux"))]
mod memory;
mod zipf;

const MEMBERS: usize = 64;
struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
    fn unit(&mut self) -> f64 {
        (self.next() >> 11) as f64 * (1.0 / ((1u64 << 53) as f64))
    }
    fn shuffle<T>(&mut self, values: &mut [T]) {
        for i in (1..values.len()).rev() {
            values.swap(i, self.next() as usize % (i + 1));
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Kind {
    Grouped,
    /// Grouped JSON whose members share their group's random strings.
    GroupedRandom,
    Weak,
    Small,
}

fn key_bytes(i: usize, kind: Kind) -> Vec<u8> {
    match kind {
        Kind::Small => {
            // 20-byte structured key: 8B entity + 8B member + 4B attribute (64 keys/entity).
            let mut key = vec![0u8; 20];
            key[..8].copy_from_slice(&(i as u64 / MEMBERS as u64).to_be_bytes());
            key[8..16].copy_from_slice(&((i % MEMBERS) as u64 / 4).to_be_bytes());
            key[16..20].copy_from_slice(&((i % 4) as u32).to_be_bytes());
            key
        }
        Kind::Grouped | Kind::GroupedRandom | Kind::Weak => {
            let mut key = vec![0u8; 16];
            key[..8].copy_from_slice(&(i as u64 / MEMBERS as u64).to_be_bytes());
            key[8..16].copy_from_slice(&(i as u64 % MEMBERS as u64).to_be_bytes());
            key
        }
    }
}

fn value(i: usize, version: u8, kind: Kind, len: usize) -> Vec<u8> {
    match kind {
        Kind::Small => {
            const STATUS: [&str; 3] = ["active", "idle", "warm"];
            const REGION: [&str; 4] = ["us-e1", "us-w2", "eu-w1", "ap-s1"];
            let mut rng = Rng(
                (i as u64 + 1).wrapping_mul(0x9e37_79b9_7f4a_7c15) ^ (u64::from(version) << 40)
            );
            let count = rng.next() % 10_000;
            let sum = rng.next() % 1_000_000;
            format!(
                r#"{{"v":{version},"c":{count},"s":{sum},"st":"{}","r":"{}"}}"#,
                STATUS[(i + usize::from(version)) % STATUS.len()],
                REGION[(i / MEMBERS) % REGION.len()],
            )
            .into_bytes()
        }
        Kind::GroupedRandom => grouped_random(i, version),
        Kind::Weak => {
            let mut rng = Rng(
                (i as u64 + 1).wrapping_mul(0x9e37_79b9_7f4a_7c15) ^ (u64::from(version) << 40)
            );
            let mut out: Vec<u8> = (0..600)
                .map(|_| b"abcdefghijklmnopqrstuvwxyz0123456789"[rng.next() as usize % 36])
                .collect();
            out.truncate(len);
            out
        }
        Kind::Grouped => {
            let prefix = format!(
                "{{\"entity\":{},\"member\":{},\"version\":{},\"status\":\"active\",\"region\":\"north\",\"description\":\"",
                i / MEMBERS,
                i % MEMBERS,
                version
            );
            let mut out = prefix.into_bytes();
            while out.len() < 598 {
                out.extend_from_slice(
                    b"stable member profile with shared field names and repeated content ",
                );
            }
            out.truncate(598);
            out.extend_from_slice(b"\"}");
            out.truncate(len);
            out
        }
    }
}
/// The comparison harness's grouped JSON (dataset A): 360-, 260- and 60-byte
/// random strings shared by the group's members, then per-member fields and a
/// random token. Updates redraw the timestamp, quantity, price, status and
/// token. Groups have `MEMBERS` members.
fn grouped_random(i: usize, version: u8) -> Vec<u8> {
    fn text(rng: &mut Rng, n: usize) -> String {
        (0..n)
            .map(|_| b"abcdefghijklmnopqrstuvwxyz0123456789"[rng.next() as usize % 36] as char)
            .collect()
    }
    let (group, member) = (i / MEMBERS, i % MEMBERS);
    let mut shared = Rng((group as u64 + 1).wrapping_mul(0x9e37_79b9_7f4a_7c15));
    let (a, b, c) = (
        text(&mut shared, 360),
        text(&mut shared, 260),
        text(&mut shared, 60),
    );
    let mut rng =
        Rng((i as u64 + 1).wrapping_mul(0xbf58_476d_1ce4_e5b9) ^ (u64::from(version) << 40));
    const STATUS: [&str; 3] = ["active", "pending", "paused"];
    let (ts, q, p, st) = if version == 0 {
        (
            1_700_000_000 + member,
            member % 17,
            1000 + member % 250,
            member % 3,
        )
    } else {
        let mut n = |m: u64| (rng.next() % m) as usize;
        (1_700_086_400 + n(86_400), n(17), 1000 + n(250), n(3))
    };
    let token = text(&mut rng, 8);
    format!(
        r#"{{"group_id":{group},"description":"{a}","attributes":"{b}","category_text":"{c}","member_id":{member},"timestamp":{ts},"quantity":{q},"price":{p},"status":"{}","token":"{token}","preferences":{{"notifications":true,"locale":"en-US","timezone":"UTC","delivery":"standard","visibility":"members","version":1}},"source":"synthetic-group-members","kind":"membership-record"}}"#,
        STATUS[st]
    )
    .into_bytes()
}
fn ratio(a: u64, b: u64) -> f64 {
    if b == 0 { 0.0 } else { a as f64 / b as f64 }
}
fn slot_name(slot: usize) -> String {
    match slot {
        0 => "flush".into(),
        1..8 => format!("L{slot}"),
        _ => format!("L{}-bottom", slot - 8),
    }
}

struct Scenario {
    name: &'static str,
    kind: Kind,
    records: usize,
    value_len: usize,
    /// Time-ordered load, then updates to the newest 10% of entities in order.
    clustered: bool,
}

type SampleProbe = fn(&str, &[SegmentSample]);

struct Flags {
    copy_audit: bool,
    c2: bool,
    dict_scope: bool,
    sample_probe: Option<SampleProbe>,
    write_only: bool,
    worst: bool,
    zipf: bool,
    read_shapes: bool,
    reuse: bool,
    keyspaces: usize,
}

impl Default for Flags {
    fn default() -> Self {
        Self {
            copy_audit: false,
            c2: false,
            dict_scope: false,
            sample_probe: None,
            write_only: false,
            worst: false,
            zipf: false,
            read_shapes: false,
            reuse: false,
            keyspaces: 1,
        }
    }
}

static BOTTOM_JOBS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

fn level_label(level: usize, depth: usize) -> String {
    if level == depth {
        format!("L{level}-bottom")
    } else {
        format!("L{level}")
    }
}

fn emit_per_record(name: &str, variant: &str, levels: &[diagnostics::LevelMemory]) {
    let mut records = 0usize;
    let (
        mut raw,
        mut frames,
        mut dicts,
        mut seg_table,
        mut index,
        mut filters,
        mut footer,
        mut phys,
    ) = (
        0usize, 0usize, 0usize, 0usize, 0usize, 0usize, 0usize, 0usize,
    );
    for m in levels {
        records += m.records;
        raw += m.raw;
        frames += m.frames;
        dicts += m.dictionaries;
        seg_table += m.segment_table;
        index += m.index;
        filters += m.filters;
        footer += m.footer;
        phys += m.physical;
    }
    let stored = frames + dicts + seg_table + index + filters + footer;
    let total = stored + phys;
    let per = |n: usize| {
        if records == 0 {
            0.0
        } else {
            n as f64 / records as f64
        }
    };
    println!(
        "per_record,{name},{variant},{records},{:.2},{:.2},{:.2},{:.2},{:.2},{:.2},{:.2},{:.2},{:.2},{:.2}",
        per(raw),
        per(stored),
        per(frames),
        per(dicts),
        per(seg_table),
        per(index),
        per(filters),
        per(footer),
        per(phys),
        per(total),
    );
}

/// Point and scan work per operation: hits and misses on spread keys, then
/// short scans from spread 7-byte prefixes (one seek, then 100 records).
fn probe_reads(
    engine: &Engine,
    ks: &Keyspace,
    s: &Scenario,
    variant: &str,
    point: &str,
    expect: &dyn Fn(usize) -> Vec<u8>,
) {
    let (name, kind, records) = (s.name, s.kind, s.records);
    const GETS: usize = 10_000;
    const SEEKS: usize = 200;
    let mut hits = engine.reader();
    for at in 0..GETS {
        let i = at * 7919 % records;
        assert_eq!(
            hits.get(ks, &key_bytes(i, kind)).unwrap().unwrap(),
            expect(i)
        );
    }
    let mut misses = engine.reader();
    for at in 0..GETS {
        let i = at * 7919 % (records - 1);
        let mut missing = key_bytes(i, kind);
        missing.push(0);
        assert!(misses.get(ks, &missing).unwrap().is_none());
    }
    let (mut seek_blocks, mut scan_blocks) = (0, 0);
    let mut ws = engine.reader();
    for at in 0..SEEKS {
        // Prefixes of 256 entities (16,384 keys); start well before the end.
        let i = (at * 104_729) % (records - 20_000);
        let k = key_bytes(i, kind);
        let prefix = &k[..7];
        // One seek alone, then a fresh scan of 100 records from it.
        let before = ws.counters().blocks_loaded;
        assert!(ws.prefix(ks, prefix).next().unwrap().is_some());
        seek_blocks += ws.counters().blocks_loaded - before;
        let before = ws.counters().blocks_loaded;
        let mut scan = ws.prefix(ks, prefix);
        for _ in 0..100 {
            assert!(scan.next().unwrap().is_some());
        }
        drop(scan);
        scan_blocks += ws.counters().blocks_loaded - before;
    }
    let stats = engine.engine_stats();
    let levels = diagnostics::levels(engine);
    let stored_records: usize = levels.iter().map(|m| m.records).sum();
    let l0 = levels[0];
    let l0_stored = l0.frames
        + l0.dictionaries
        + l0.segment_table
        + l0.index
        + l0.filters
        + l0.footer
        + l0.physical;
    let per = |n: u64, d: usize| n as f64 / d as f64;
    println!(
        "probe,{name},{variant},{point},{:.3},{:.3},{:.1},{:.3},{:.3},{:.1},{},{:.3},{:.3},{},{},{},{},{},{:.4}",
        per(hits.counters().filter_probes, GETS),
        per(hits.counters().blocks_loaded, GETS),
        per(hits.counters().decoded_bytes, GETS),
        per(misses.counters().filter_probes, GETS),
        per(misses.counters().blocks_loaded, GETS),
        per(misses.counters().decoded_bytes, GETS),
        stats.max_overlap + stats.frozen_tables + 1,
        per(seek_blocks, SEEKS),
        per(scan_blocks, SEEKS),
        diagnostics::level0_flushes(engine),
        l0.runs,
        stats.run_bytes,
        l0_stored,
        stored_records,
        if stored_records == 0 {
            0.0
        } else {
            stored_records.saturating_sub(records) as f64 / stored_records as f64
        },
    );
}

/// Miss-heavy point gets (0%, 50%, 100% misses) and scan shapes (absent prefix
/// and short scans of 1, 5, 10 records), with per-level decode breakdowns.
fn probe_read_shapes(
    engine: &Engine,
    ks: &Keyspace,
    s: &Scenario,
    variant: &str,
    expect: &dyn Fn(usize) -> Vec<u8>,
) {
    let (name, kind, records) = (s.name, s.kind, s.records);
    const GETS: usize = 10_000;
    const SCANS: usize = 500;
    let (depth, run_map) = diagnostics::run_levels(engine);
    let levels = depth + 1;

    for (mix_name, miss_pct) in [("miss_0", 0usize), ("miss_50", 50), ("miss_100", 100)] {
        let per_level = Rc::new(RefCell::new(vec![(0u64, 0u64); levels]));
        let miss_decoded = Rc::new(RefCell::new(0u64));
        let in_miss = Rc::new(RefCell::new(false));
        let map_ref = Rc::new(run_map.clone());
        let lvl_sink = Rc::clone(&per_level);
        let miss_sink = Rc::clone(&miss_decoded);
        let flag_sink = Rc::clone(&in_miss);
        let map_sink = Rc::clone(&map_ref);

        let mut reader = engine.reader();
        let mut misses = 0usize;
        diagnostics::with_block_observer(
            Box::new(move |ev| {
                let lvl = map_sink.get(&ev.run_id).copied().unwrap_or(depth);
                let mut counts = lvl_sink.borrow_mut();
                if lvl < counts.len() {
                    counts[lvl].0 += 1;
                    counts[lvl].1 += ev.decoded_bytes as u64;
                }
                if *flag_sink.borrow() {
                    *miss_sink.borrow_mut() += ev.decoded_bytes as u64;
                }
            }),
            || {
                for at in 0..GETS {
                    let is_miss = (at % 100) < miss_pct;
                    *in_miss.borrow_mut() = is_miss;
                    if is_miss {
                        misses += 1;
                        let i = at * 7919 % (records - 1);
                        let mut missing = key_bytes(i, kind);
                        missing.push(0);
                        assert!(reader.get(ks, &missing).unwrap().is_none());
                    } else {
                        let i = at * 7919 % records;
                        assert_eq!(
                            reader.get(ks, &key_bytes(i, kind)).unwrap().unwrap(),
                            expect(i)
                        );
                    }
                }
            },
        );
        let c = reader.counters();
        let fp_rate = if c.filter_probes == 0 {
            0.0
        } else {
            c.filter_false_positives as f64 / c.filter_probes as f64
        };
        let dec_per_miss = if misses == 0 {
            0.0
        } else {
            *miss_decoded.borrow() as f64 / misses as f64
        };
        println!(
            "miss_get,{name},{variant},{mix_name},{GETS},{misses},{:.3},{:.4},{:.5},{:.3},{:.1},{:.1},{:.1}",
            c.filter_probes as f64 / GETS as f64,
            c.filter_false_positives as f64 / GETS as f64,
            fp_rate,
            c.blocks_loaded as f64 / GETS as f64,
            c.decoded_bytes as f64 / GETS as f64,
            dec_per_miss,
            c.crc_bytes as f64 / GETS as f64,
        );
        for (lvl, &(blocks, bytes)) in per_level.borrow().iter().enumerate() {
            println!(
                "miss_get_level,{name},{variant},{mix_name},{},{:.4},{:.1}",
                level_label(lvl, depth),
                blocks as f64 / GETS as f64,
                bytes as f64 / GETS as f64,
            );
        }
    }

    let shapes: &[(&str, usize)] = &[
        ("prefix_absent", 0),
        ("short_1", 1),
        ("short_5", 5),
        ("short_10", 10),
        ("prefix_10", 10),
    ];
    for &(shape, take_n) in shapes {
        let per_level = Rc::new(RefCell::new(vec![(0u64, 0u64); levels]));
        let lvl_sink = Rc::clone(&per_level);
        let map_sink = run_map.clone();
        let mut reader = engine.reader();
        let mut yielded = 0usize;
        diagnostics::with_block_observer(
            Box::new(move |ev| {
                let lvl = map_sink.get(&ev.run_id).copied().unwrap_or(depth);
                let mut counts = lvl_sink.borrow_mut();
                if lvl < counts.len() {
                    counts[lvl].0 += 1;
                    counts[lvl].1 += ev.decoded_bytes as u64;
                }
            }),
            || {
                let entities = (records / MEMBERS).max(2);
                for at in 0..SCANS {
                    match shape {
                        "prefix_absent" => {
                            // 12-byte prefix with bytes[8..12] = 1 sorts strictly
                            // between entity e's members and entity e+1, so 0 hits.
                            let e = (at * 7919) % (entities - 1);
                            let mut prefix = [0u8; 12];
                            prefix[..8].copy_from_slice(&(e as u64).to_be_bytes());
                            prefix[11] = 1;
                            let mut scan = reader.prefix(ks, &prefix);
                            assert!(scan.next().unwrap().is_none());
                        }
                        "prefix_10" => {
                            let e = (at * 7919) % entities;
                            let prefix = (e as u64).to_be_bytes();
                            let mut scan = reader.prefix(ks, &prefix);
                            for step in 0..take_n {
                                let i = e * MEMBERS + step;
                                let (k, v) = scan.next().unwrap().unwrap();
                                assert_eq!(k, key_bytes(i, kind));
                                assert_eq!(v, expect(i));
                                yielded += 1;
                            }
                        }
                        _ => {
                            let start = (at * 104_729) % (records - take_n - 1);
                            let start_key = key_bytes(start, kind);
                            let mut scan = reader.range(ks, start_key.as_slice()..);
                            for step in 0..take_n {
                                let i = start + step;
                                let (k, v) = scan.next().unwrap().unwrap();
                                assert_eq!(k, key_bytes(i, kind));
                                assert_eq!(v, expect(i));
                                yielded += 1;
                            }
                        }
                    }
                }
            },
        );
        let c = reader.counters();
        println!(
            "scan_shape,{name},{variant},{shape},{SCANS},{:.2},{:.3},{:.1},{:.1}",
            yielded as f64 / SCANS as f64,
            c.blocks_loaded as f64 / SCANS as f64,
            c.decoded_bytes as f64 / SCANS as f64,
            c.crc_bytes as f64 / SCANS as f64,
        );
        for (lvl, &(blocks, bytes)) in per_level.borrow().iter().enumerate() {
            println!(
                "scan_shape_level,{name},{variant},{shape},{},{:.4},{:.1}",
                level_label(lvl, depth),
                blocks as f64 / SCANS as f64,
                bytes as f64 / SCANS as f64,
            );
        }
    }
}

/// Scrambled Zipfian gets at theta = 0.99 and 0.80, computing the exact
/// block-reuse curve and S3-FIFO hit rate from decoded (run, block) identities.
fn probe_zipf_reads(
    engine: &Engine,
    ks: &Keyspace,
    s: &Scenario,
    variant: &str,
    phase: &str,
    expect: &dyn Fn(usize) -> Vec<u8>,
) {
    let (name, kind, records) = (s.name, s.kind, s.records);
    const ZIPF_GETS: usize = 50_000;
    for theta in [0.99_f64, 0.80_f64] {
        let zipf_gen = zipf::ScrambledZipfian::new(records, theta, 0x5a69_7066_0001);
        let mut rng = Rng(0x7a69_7066_1234 ^ theta.to_bits());
        let tracker = Rc::new(RefCell::new(zipf::ReuseTracker::new(ZIPF_GETS * 2)));
        let sink = Rc::clone(&tracker);
        let mut reader = engine.reader();
        diagnostics::with_block_observer(
            Box::new(move |ev| {
                sink.borrow_mut()
                    .record_block(ev.run_id, ev.block, ev.decoded_bytes);
            }),
            || {
                for _ in 0..ZIPF_GETS {
                    let i = zipf_gen.sample(rng.unit());
                    tracker.borrow_mut().begin_get();
                    assert_eq!(
                        reader.get(ks, &key_bytes(i, kind)).unwrap().unwrap(),
                        expect(i)
                    );
                    tracker.borrow_mut().end_get();
                }
            },
        );
        let c = reader.counters();
        let t = tracker.borrow();
        println!(
            "zipf_get,{name},{variant},{phase},{theta:.2},{ZIPF_GETS},{:.3},{:.1},{:.1},{:.3},{:.4},{}",
            c.blocks_loaded as f64 / ZIPF_GETS as f64,
            c.decoded_bytes as f64 / ZIPF_GETS as f64,
            c.crc_bytes as f64 / ZIPF_GETS as f64,
            c.filter_probes as f64 / ZIPF_GETS as f64,
            c.filter_false_positives as f64 / ZIPF_GETS as f64,
            t.distinct_blocks(),
        );
        for p in t.curve() {
            println!(
                "zipf_reuse,{name},{variant},{phase},{theta:.2},{},{:.4},{:.4},{:.4},{},{}",
                p.n_blocks,
                p.get_hit_rate,
                p.block_hit_rate,
                p.s3fifo_hit_rate,
                p.lru_bytes,
                p.mean_n_bytes,
            );
        }
    }
}

/// Per-entry charge of a simulated record cache beyond key and value bytes.
const RECORD_OVERHEAD: usize = 64;

/// On the settled tree: work per get for uniform hits and in-range misses and
/// for Zipf hits and misses (absent keys drawn over a miss space as large as
/// the live set), then byte-capacity cache curves. The block cache is charged
/// decoded bytes; the record cache key + value + `RECORD_OVERHEAD`, and the
/// negative cache key + `RECORD_OVERHEAD`. Each reuse stream holds ten times
/// the largest capacity's entry count; the first half warms the caches.
fn probe_reuse(
    engine: &Engine,
    ks: &Keyspace,
    s: &Scenario,
    variant: &str,
    expect: &dyn Fn(usize) -> Vec<u8>,
) {
    settle_bottom(engine);
    let (name, kind, records) = (s.name, s.kind, s.records);
    let (depth, run_map) = diagnostics::run_levels(engine);
    let stored = engine.engine_stats().run_bytes as f64;
    let charge = |i: usize, miss: bool| {
        key_bytes(i, kind).len() + RECORD_OVERHEAD + if miss { 1 } else { expect(i).len() }
    };
    let mean = |miss| (0..1024).map(|i| charge(i, miss)).sum::<usize>() as f64 / 1024.0;
    let length = |miss| 10 * (zipf::CAPACITY_MIB[4] * f64::from(1u32 << 20) / mean(miss)) as usize;
    let long = length(false);
    println!(
        "reuse_meta,{name},{variant},{records},{:.1},{long},{stored}",
        mean(false)
    );
    let emit = |cache: &str, workload: &str, theta: f64, curves: &zipf::Curves| {
        for (policy, hits) in ["lru", "s3fifo"].iter().zip(&curves.hits) {
            for (at, &mib) in zipf::CAPACITY_MIB.iter().enumerate() {
                let pct = 100.0 * mib * f64::from(1u32 << 20) / stored;
                let (rate, n) = (ratio(hits[at], curves.counted as u64), curves.counted);
                println!(
                    "reuse,{name},{variant},{cache},{workload},{theta:.2},{policy},{mib},{pct:.3},{rate:.4},{n}"
                );
            }
        }
    };
    let seed = |theta: f64, miss: bool| 0x7265_7573_6500 ^ theta.to_bits() ^ u64::from(miss);
    let seen = Rc::new(RefCell::new(Vec::new()));
    let sink = Rc::clone(&seen);
    let mut reader = engine.reader();
    diagnostics::with_block_observer(Box::new(move |ev| sink.borrow_mut().push(ev)), || {
        let streams = [
            ("uniform_hit", 0.0, false),
            ("uniform_miss", 0.0, true),
            ("zipf_miss", 0.99, true),
            ("zipf_hit", 0.99, false),
            ("zipf_hit", 0.80, false),
        ];
        for (stream, theta, miss) in streams {
            let reuse = stream == "zipf_hit";
            let gets = if reuse { long } else { 50_000 };
            let zipf = (theta > 0.0)
                .then(|| zipf::ScrambledZipfian::new(records, theta, seed(theta, miss)));
            let mut rng = Rng(seed(theta, miss));
            let before = reader.counters();
            let mut levels = vec![(0u64, 0u64); depth + 1];
            let mut curves = zipf::Curves::new(if reuse { gets * (depth + 1) } else { 0 });
            for at in 0..gets {
                let i = zipf
                    .as_ref()
                    .map_or(at * 7919 % (records - usize::from(miss)), |z| {
                        z.sample(rng.unit())
                    });
                let mut key = key_bytes(i, kind);
                if miss {
                    key.push(0);
                    assert!(reader.get(ks, &key).unwrap().is_none());
                } else {
                    assert_eq!(reader.get(ks, &key).unwrap().unwrap(), expect(i));
                }
                let mut all = [[true; 5]; 2];
                for ev in seen.borrow_mut().drain(..) {
                    let level = run_map.get(&ev.run_id).copied().unwrap_or(depth);
                    levels[level].0 += 1;
                    levels[level].1 += ev.decoded_bytes as u64;
                    if reuse {
                        let block = ((ev.run_id as u64) << 32) | ev.block as u64;
                        let hit = curves.access(block, ev.decoded_bytes as u32);
                        all.iter_mut()
                            .flatten()
                            .zip(hit.iter().flatten())
                            .for_each(|(a, h)| *a &= h);
                    }
                }
                curves.count(all, at >= gets / 2);
            }
            let c = reader.counters();
            let per = |a: u64, b: u64| (a - b) as f64 / gets as f64;
            println!(
                "get_work,{name},{variant},{stream},{theta:.2},{gets},{:.4},{:.1},{:.1},{:.4}",
                per(c.blocks_loaded, before.blocks_loaded),
                per(c.decoded_bytes, before.decoded_bytes),
                per(c.crc_bytes, before.crc_bytes),
                per(c.filter_probes, before.filter_probes),
            );
            for (level, &(blocks, bytes)) in levels.iter().enumerate() {
                println!(
                    "get_work_level,{name},{variant},{stream},{theta:.2},{},{:.4},{:.1},{:.1}",
                    level_label(level, depth),
                    blocks as f64 / gets as f64,
                    bytes as f64 / gets as f64,
                    ratio(bytes, blocks),
                );
            }
            if reuse {
                emit("block", "read", theta, &curves);
            }
        }
    });
    // Record and negative caches need only key indices and sizes. The read
    // streams repeat the block-cache streams; an update removes the record.
    for (workload, theta, write_pct, miss) in [
        ("read", 0.99, 0, false),
        ("read", 0.80, 0, false),
        ("mix_95_5", 0.99, 5, false),
        ("mix_50_50", 0.99, 50, false),
        ("miss", 0.99, 0, true),
    ] {
        let draw = zipf::ScrambledZipfian::new(records, theta, seed(theta, miss));
        let mut rng = Rng(seed(theta, miss));
        let mut writes = Rng(0x7570_6461_7465 ^ write_pct);
        let n = length(miss);
        let mut curves = zipf::Curves::new(n);
        for at in 0..n {
            let i = draw.sample(rng.unit());
            if writes.next() % 100 < write_pct {
                curves.invalidate(i as u64);
            } else {
                let hit = curves.access(i as u64, charge(i, miss) as u32);
                curves.count(hit, at >= n / 2);
            }
        }
        emit(
            if miss { "negative" } else { "record" },
            workload,
            theta,
            &curves,
        );
    }
}

/// One row per audited bottom input segment, then the bottom work totals.
fn print_copy_audit(name: &str, variant: &str) {
    let (segments, other_raw) = diagnostics::take_copy_audit();
    println!("copy_other,{name},{variant},{other_raw}");
    for s in segments {
        println!(
            "copy_segment,{name},{variant},{},{},{},{},{},{},{},{},{},{},{}",
            s.blocks,
            s.raw,
            s.stored,
            s.copyable_blocks,
            s.copyable_raw,
            s.copyable_stored,
            s.only_seq,
            s.only_tombstone,
            s.seq_and_tombstone,
            s.touched,
            s.edge
        );
    }
}

fn sample_bottom(engine: &Engine) -> Vec<SegmentSample> {
    const RAW_TARGET: usize = 64 << 20;
    let raw = diagnostics::bottom_raw(engine);
    let every = (raw / RAW_TARGET).max(1);
    let samples = diagnostics::bottom_segments(engine, every);
    let sampled: usize = samples.iter().flat_map(|s| &s.blocks).map(Vec::len).sum();
    if every > 1 && sampled < RAW_TARGET {
        diagnostics::bottom_segments(engine, every - 1)
    } else {
        samples
    }
}

fn settle_bottom(engine: &Engine) {
    let mut merge = MergeWorkspace::new();
    while diagnostics::compact_step(engine, &mut merge).unwrap() {}
    engine.wait_for_compaction().unwrap();
}

fn run(s: &Scenario, variant: &str, settle: bool, threads: usize, flags: &Flags) {
    let started = Instant::now();
    let (name, kind, records, len) = (s.name, s.kind, s.records, s.value_len);
    let engine = std::sync::Arc::new(
        Engine::in_memory({
            let mut options = Options::default();
            if let Ok(mib) = std::env::var("TESSERA_COUNTERS_MEMTABLE_MIB") {
                options.memtable_size = mib
                    .parse::<usize>()
                    .expect("integer memtable MiB")
                    .checked_mul(1 << 20)
                    .expect("memtable size fits usize");
            }
            options.background_threads = threads;
            options
        })
        .unwrap(),
    );
    let spaces: Vec<Keyspace> = match flags.keyspaces {
        1 => vec![engine.keyspace("data").unwrap()],
        n => (0..n)
            .map(|j| engine.keyspace(&format!("data{j}")).unwrap())
            .collect(),
    };
    let ks = &spaces[0];
    let single = spaces.len() == 1;
    let owner = |i: usize| &spaces[i % spaces.len()];
    #[cfg(all(feature = "tools-memory", target_os = "linux"))]
    let sampler = memory::Sampler::start(std::sync::Arc::clone(&engine), name, variant);
    #[cfg(all(feature = "tools-memory", target_os = "linux"))]
    diagnostics::set_memory_observer(memory::sample_stats);
    let mut ingested = 0u64;
    let mut order: Vec<u32> = (0..records as u32).collect();
    let mut rng = Rng(0x123456789abcdef);
    if !s.clustered {
        rng.shuffle(&mut order);
    }
    for &i in &order {
        let i = i as usize;
        let k = key_bytes(i, kind);
        let v = value(i, 0, kind, len);
        owner(i).put(&k, &v).unwrap();
        ingested += (k.len() + v.len()) as u64;
    }
    #[cfg(all(feature = "tools-memory", target_os = "linux"))]
    memory::phase_end(&engine, "load_end", 1);
    let mut version = vec![0u8; records];
    let mut updates = order;
    if s.clustered {
        updates = (records / MEMBERS * 9 / 10 * MEMBERS..records)
            .map(|i| i as u32)
            .collect();
    } else {
        rng.shuffle(&mut updates);
        updates.truncate(records / 2);
    }
    for &i in &updates {
        let i = i as usize;
        let k = key_bytes(i, kind);
        let v = value(i, 1, kind, len);
        owner(i).put(&k, &v).unwrap();
        ingested += (k.len() + v.len()) as u64;
        version[i] = 1;
    }
    #[cfg(all(feature = "tools-memory", target_os = "linux"))]
    memory::phase_end(&engine, "update_end", 2);
    drop(updates);
    // Steady state: the final memtable flushes on its own only when full,
    // so publish it, then let the worker settle without eager compaction.
    engine.flush().unwrap();
    engine.wait_for_compaction().unwrap();
    #[cfg(all(feature = "tools-memory", target_os = "linux"))]
    memory::phase_end(&engine, "drain_end", 3);
    let load_seconds = started.elapsed().as_secs_f64();
    if flags.write_only {
        eprintln!("write_only,{name},{load_seconds:.1}");
        std::sync::Arc::try_unwrap(engine).unwrap().close().unwrap();
        return;
    }

    if flags.dict_scope {
        settle_bottom(&engine);
        let samples = diagnostics::bottom_segments(&engine, 1);
        #[cfg(all(feature = "tools-memory", target_os = "linux"))]
        sampler.stop();
        std::sync::Arc::try_unwrap(engine).unwrap().close().unwrap();
        c2::run_dict_scope(name, &samples);
        return;
    }

    if let Some(probe) = flags.sample_probe {
        settle_bottom(&engine);
        let samples = sample_bottom(&engine);
        #[cfg(all(feature = "tools-memory", target_os = "linux"))]
        sampler.stop();
        std::sync::Arc::try_unwrap(engine).unwrap().close().unwrap();
        probe(name, &samples);
        return;
    }

    let expect = |i: usize| value(i, version[i], kind, len);
    let mut scans = engine.reader();
    for _ in 0..100 {
        let mut scan = scans.range(ks, ..);
        for _ in 0..100 {
            assert!(scan.next().unwrap().is_some());
        }
    }
    // Every reader path against the oracle: sampled gets, then multi-get
    // and a full scan over every record.
    let mut ws = engine.reader();
    for i in (0..records).step_by(61) {
        assert_eq!(
            ws.get(owner(i), &key_bytes(i, kind)).unwrap().unwrap(),
            expect(i)
        );
    }
    let n = spaces.len();
    for start in (0..records).step_by(128 * n) {
        for (j, space) in spaces.iter().enumerate() {
            let keys: Vec<usize> = (start..(start + 128 * n).min(records))
                .filter(|i| i % n == j)
                .rev()
                .collect();
            let bytes: Vec<_> = keys.iter().map(|&i| key_bytes(i, kind)).collect();
            for (at, got) in ws.multi_get(space, &bytes).unwrap().iter().enumerate() {
                assert_eq!(got.unwrap(), expect(keys[at]));
            }
        }
    }
    for (j, space) in spaces.iter().enumerate() {
        let mut scan = ws.range(space, ..);
        for i in (j..records).step_by(n) {
            let (found, value) = scan.next().unwrap().unwrap();
            assert_eq!(found, key_bytes(i, kind));
            assert_eq!(value, expect(i));
        }
        assert!(scan.next().unwrap().is_none());
    }
    let mut hits = engine.reader();
    for at in 0..10_000 {
        let i = at * 7919 % records;
        assert_eq!(
            hits.get(owner(i), &key_bytes(i, kind)).unwrap().unwrap(),
            expect(i)
        );
    }
    let mut misses = engine.reader();
    for at in 0..1_000 {
        let i = at * 7919 % (records - 1);
        let mut missing = key_bytes(i, kind);
        missing.push(0);
        assert!(misses.get(owner(i), &missing).unwrap().is_none());
    }
    let stats = engine.engine_stats();
    assert_eq!(stats.build.raw_bytes_ingested, ingested);
    let background = engine.background_status().unwrap();
    assert_eq!(background.failures, 0);
    assert!(background.panic.is_none());

    let work = diagnostics::work(&engine);
    for (slot, w) in work.iter().enumerate() {
        if w.raw_out > 0 {
            println!(
                "work,{name},{variant},{},{},{},{}",
                slot_name(slot),
                w.raw_in,
                w.raw_out,
                w.stored
            );
        }
    }
    let c = diagnostics::copy_forward(&engine);
    println!(
        "copy,{name},{variant},{},{},{},{},{},{},{},{}",
        c.blocks,
        c.raw,
        c.stored,
        c.segments,
        c.inherited,
        c.input_blocks,
        c.input_raw,
        c.input_segments
    );
    let e = diagnostics::erasure(&engine);
    println!(
        "erasure,{name},{variant},{},{},{},{},{}",
        e.records, e.erased, e.probes, e.kept_unfiltered, e.kept_filtered
    );
    let levels = diagnostics::levels(&engine);
    let depth = levels.len() - 1;
    for (level, m) in levels.iter().enumerate() {
        println!(
            "level,{name},{variant},{level}{},{},{},{},{},{},{},{},{},{},{}",
            if level == depth { "-bottom" } else { "" },
            m.runs,
            m.records,
            m.raw,
            m.frames,
            m.dictionaries,
            m.segment_table,
            m.index,
            m.filters,
            m.footer,
            m.physical
        );
    }
    emit_per_record(name, variant, &levels);
    if flags.copy_audit {
        print_copy_audit(name, variant);
    }
    if flags.c2 {
        c2::run(name, &sample_bottom(&engine));
    }
    let flush_raw = work[0].raw_out;
    let raw: u64 = work.iter().map(|w| w.raw_out).sum();
    let stored: u64 = work.iter().map(|w| w.stored).sum();
    println!(
        "reads,{name},{variant},{},{},{:.1},{:.1},{:.3},{:.3},{:.3},{},{},{depth},{load_seconds:.1},{:.1}",
        stats.run_bytes,
        stats.held_bytes,
        ratio(hits.counters().decoded_bytes, 10_000),
        ratio(misses.counters().decoded_bytes, 1_000),
        ratio(scans.counters().blocks_loaded, 100),
        ratio(stored, work[0].stored),
        ratio(raw, flush_raw),
        stats.flushes,
        stats.runs,
        started.elapsed().as_secs_f64()
    );
    let m = diagnostics::maintenance(&engine);
    println!(
        "waits,{name},{variant},{},{:.3},{:.3},{},{},{:.3},{:.3},{},{}",
        m.level0_waits,
        m.level0_wait_ns as f64 / 1e9,
        m.writer_wait_ns as f64 / 1e9,
        background.compactions,
        BOTTOM_JOBS.load(std::sync::atomic::Ordering::Relaxed),
        m.flush_wait_ns as f64 / 1e9,
        m.memory_wait_ns as f64 / 1e9,
        m.gate_waits,
        m.write_locks,
    );
    // The probes, read shapes, zipf and worst point read one keyspace's key layout.
    if single {
        probe_reads(&engine, ks, s, variant, "end", &expect);
        if flags.read_shapes {
            probe_read_shapes(&engine, ks, s, variant, &expect);
        }
        if flags.zipf {
            probe_zipf_reads(&engine, ks, s, variant, "post_update", &expect);
        }
        if flags.reuse {
            probe_reuse(&engine, ks, s, variant, &expect);
        }
    }
    if flags.zipf && single {
        let work_before = diagnostics::work(&engine);
        let copy_before = diagnostics::copy_forward(&engine);
        let zipf_updates = (records / 4).max(1_024);
        let zipf_gen = zipf::ScrambledZipfian::new(records, 0.99, 0x5a69_7066_0002);
        let mut zrng = Rng(0x7a69_7066_9999);
        for _ in 0..zipf_updates {
            let i = zipf_gen.sample(zrng.unit());
            let k = key_bytes(i, kind);
            let v = value(i, 2, kind, len);
            ks.put(&k, &v).unwrap();
            version[i] = 2;
        }
        engine.flush().unwrap();
        engine.wait_for_compaction().unwrap();
        let work_after = diagnostics::work(&engine);
        let copy_after = diagnostics::copy_forward(&engine);
        let raw_in_delta: u64 = work_after
            .iter()
            .zip(&work_before)
            .map(|(a, b)| a.raw_in - b.raw_in)
            .sum();
        let raw_out_delta: u64 = work_after
            .iter()
            .zip(&work_before)
            .map(|(a, b)| a.raw_out - b.raw_out)
            .sum();
        let stored_delta: u64 = work_after
            .iter()
            .zip(&work_before)
            .map(|(a, b)| a.stored - b.stored)
            .sum();
        let expect_after = |i: usize| value(i, version[i], kind, len);
        let mut check_r = engine.reader();
        for at in 0..10_000 {
            let i = at * 7919 % records;
            assert_eq!(
                check_r.get(ks, &key_bytes(i, kind)).unwrap().unwrap(),
                expect_after(i)
            );
        }
        let zstats = engine.engine_stats();
        println!(
            "zipf_update,{name},{variant},0.99,{zipf_updates},{raw_in_delta},{raw_out_delta},{stored_delta},{},{},{},{},{:.1}",
            copy_after.blocks - copy_before.blocks,
            copy_after.input_blocks - copy_before.input_blocks,
            zstats.run_bytes,
            zstats.held_bytes,
            ratio(check_r.counters().decoded_bytes, 10_000),
        );
        probe_zipf_reads(&engine, ks, s, variant, "post_zipf_update", &expect_after);
    }
    let expect = |i: usize| value(i, version[i], kind, len);
    if settle {
        // A control without timing effects: rewrite everything into the
        // bottom level, where sequences are erased, then measure again.
        settle_bottom(&engine);
        let mut hits = engine.reader();
        for at in 0..10_000 {
            let i = at * 7919 % records;
            assert_eq!(
                hits.get(owner(i), &key_bytes(i, kind)).unwrap().unwrap(),
                expect(i)
            );
        }
        let mut scans = engine.reader();
        for _ in 0..100 {
            let mut scan = scans.range(ks, ..);
            for _ in 0..100 {
                assert!(scan.next().unwrap().is_some());
            }
        }
        let levels = diagnostics::levels(&engine);
        let depth = levels.len() - 1;
        for (level, m) in levels.iter().enumerate() {
            println!(
                "level,{name},{variant}+settled,{level}{},{},{},{},{},{},{},{},{},{},{}",
                if level == depth { "-bottom" } else { "" },
                m.runs,
                m.records,
                m.raw,
                m.frames,
                m.dictionaries,
                m.segment_table,
                m.index,
                m.filters,
                m.footer,
                m.physical
            );
        }
        emit_per_record(name, &format!("{variant}+settled"), &levels);
        if flags.copy_audit {
            print_copy_audit(name, &format!("{variant}+settled"));
            for (slot, w) in diagnostics::work(&engine).iter().enumerate() {
                if w.raw_out > 0 {
                    println!(
                        "work,{name},{variant}+settled,{},{},{},{}",
                        slot_name(slot),
                        w.raw_in,
                        w.raw_out,
                        w.stored
                    );
                }
            }
        }
        println!(
            "settled,{name},{variant},{},{},{:.1},{:.3}",
            engine.engine_stats().run_bytes,
            engine.engine_stats().held_bytes,
            ratio(hits.counters().decoded_bytes, 10_000),
            ratio(scans.counters().blocks_loaded, 100),
        );
    }
    if flags.worst && single {
        worst_point(&engine, ks, s, variant, &version);
    }
    #[cfg(all(feature = "tools-memory", target_os = "linux"))]
    {
        println!(
            "reader_memory,{name},{variant},{},{}",
            scans.counters().capacity
                + ws.counters().capacity
                + hits.counters().capacity
                + misses.counters().capacity,
            scans.counters().context_bytes
                + ws.counters().context_bytes
                + hits.counters().context_bytes
                + misses.counters().context_bytes
        );
        sampler.stop();
        memory::sample_stats("final", engine.engine_stats());
    }
    drop((scans, ws, hits, misses));
    std::sync::Arc::try_unwrap(engine).unwrap().close().unwrap();
}

/// The engine's writer stop at level 0, in flush groups.
const LEVEL0_STOP: usize = 12;

/// Level 0 at its fullest before a job: with new jobs held off, update
/// random keys until level 0 holds `LEVEL0_STOP` flush groups, staying below
/// the stop (a writer at the stop would wait for the held compaction), then
/// measure reads and memory.
fn worst_point(
    engine: &Engine,
    ks: &Keyspace,
    s: &Scenario,
    variant: &str,
    initial_version: &[u8],
) {
    let stop = LEVEL0_STOP;
    let (name, kind, records, len) = (s.name, s.kind, s.records, s.value_len);
    let mut version: Vec<u8> = initial_version.to_vec();
    let held = diagnostics::hold_compaction(engine);
    let mut rng = Rng(0xfeed_face_cafe_f00d);
    let mut written = 0usize;
    loop {
        let frozen = engine.engine_stats().frozen_tables;
        let flushes = diagnostics::level0_flushes(engine);
        if flushes >= stop {
            break;
        }
        if flushes + frozen >= stop {
            // Let the frozen tables publish, then look again.
            std::thread::sleep(std::time::Duration::from_millis(5));
            continue;
        }
        // At most one rotation between checks: a memtable holds far more.
        for _ in 0..256 {
            let i = rng.next() as usize % records;
            ks.put(&key_bytes(i, kind), &value(i, 3, kind, len))
                .unwrap();
            version[i] = 3;
            written += 1;
        }
    }
    let expect = |i: usize| value(i, version[i], kind, len);
    probe_reads(engine, ks, s, variant, "worst", &expect);
    eprintln!("worst,{name},{variant},written={written}");
    #[cfg(all(feature = "tools-memory", target_os = "linux"))]
    memory::sample_stats("worst", engine.engine_stats());
    drop(held);
}

fn main() {
    entry(None);
}

pub fn entry(sample_probe: Option<SampleProbe>) {
    diagnostics::set_bottom_job_observer(|job| {
        BOTTOM_JOBS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        eprintln!(
            "bottom_job,{},{},{},{}",
            job.pieces, job.bottom_bytes, job.output_bytes, job.peak_output_bytes
        )
    });

    let mut variant = String::from("default");
    let mut settle = false;
    let mut threads = 1;
    let mut scale = 0;
    let mut flags = Flags {
        sample_probe,
        ..Flags::default()
    };
    let mut memtable = None;
    let mut memtable_key = 16usize;
    let mut names = Vec::new();
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--variant" => variant = args.next().expect("LABEL"),
            "--settle" => settle = true,
            "--threads" => {
                let value = args.next().expect("COUNT or default");
                threads = if value == "default" {
                    Options::default().background_threads
                } else {
                    value.parse().expect("positive thread count")
                };
            }
            "--scale" => scale = args.next().expect("SHIFT").parse().unwrap(),
            "--copy-audit" => flags.copy_audit = true,
            "--c2" => flags.c2 = true,
            "--dict-scope" => flags.dict_scope = true,
            "--write-only" => flags.write_only = true,
            "--worst" => flags.worst = true,
            "--zipf" => flags.zipf = true,
            "--read-shapes" => flags.read_shapes = true,
            "--reuse" => flags.reuse = true,
            "--keyspaces" => {
                flags.keyspaces = args.next().expect("N").parse().expect("positive N");
                assert!(flags.keyspaces > 0, "--keyspaces needs at least 1");
            }
            "--memtable" => memtable = Some(args.next().expect("VALUE_LEN").parse().unwrap()),
            "--memtable-key" => memtable_key = args.next().expect("KEY_LEN").parse().unwrap(),
            _ => names.push(arg),
        }
    }
    let scenarios = [
        Scenario {
            name: "A",
            kind: Kind::Grouped,
            records: 2_097_152 >> scale,
            value_len: 600,
            clustered: false,
        },
        Scenario {
            name: "grouped-random",
            kind: Kind::GroupedRandom,
            records: 1_048_576 >> scale,
            value_len: 0,
            clustered: false,
        },
        Scenario {
            name: "B",
            kind: Kind::Weak,
            records: 2_097_152 >> scale,
            value_len: 600,
            clustered: false,
        },
        Scenario {
            name: "A-large",
            kind: Kind::Grouped,
            records: 4_194_304 >> scale,
            value_len: 48,
            clustered: false,
        },
        Scenario {
            name: "small",
            kind: Kind::Small,
            records: 16_777_216 >> scale,
            value_len: 54,
            clustered: false,
        },
        Scenario {
            name: "clustered",
            kind: Kind::Grouped,
            records: 2_097_152 >> scale,
            value_len: 600,
            clustered: true,
        },
    ];
    if let Some(len) = memtable {
        memtable_overhead(memtable_key, len);
        return;
    }
    if flags.copy_audit {
        diagnostics::enable_copy_audit();
        println!(
            "copy_segment,scenario,variant,blocks,raw,stored,copyable_blocks,copyable_raw,copyable_stored,only_seq,only_tombstone,seq_and_tombstone,touched,edge"
        );
    }
    if flags.c2 {
        println!(
            "c2,scenario,setting,stored,stored_ratio,decoded_per_record,zstd_blocks_per_frame,decompress_time_ratio,raw_per_record"
        );
    }
    if flags.dict_scope {
        println!(
            "scope_offset,scenario,offset,segs_measured,base_payload,recomp_payload,drift_pct"
        );
        println!(
            "scope_spike,scenario,rule,segments,blocks,records,raw_bytes,scopes,mean_segs,max_segs,frames_payload,frames_hdr,dicts,total_bytes,b_per_rec,delta_pct,frame_drift_pct,decode_ns_per_frame"
        );
    }
    println!("work,scenario,variant,destination,raw_in,raw_out,stored");
    println!(
        "copy,scenario,variant,blocks,raw,stored,segments,inherited,input_blocks,input_raw,input_segments"
    );
    println!("erasure,scenario,variant,records,erased,probes,kept_unfiltered,kept_filtered");
    println!(
        "level,scenario,variant,level,runs,records,raw,frames,dictionaries,segment_table,index,filters,footer,physical"
    );
    println!(
        "per_record,scenario,variant,records,raw_per_rec,stored_per_rec,frames_per_rec,dicts_per_rec,seg_table_per_rec,index_per_rec,filters_per_rec,footer_per_rec,physical_per_rec,total_per_rec"
    );
    println!(
        "reads,scenario,variant,run_bytes,held_bytes,hit_decoded_per_get,miss_decoded_per_get,blocks_per_100_record_scan,stored_wa,raw_wa,flushes,runs,depth,load_seconds,total_seconds"
    );
    println!(
        "waits,scenario,variant,level0_waits,level0_wait_s,writer_wait_s,compactions,bottom_jobs,flush_wait_s,memory_wait_s,gate_waits,write_locks"
    );
    println!(
        "probe,scenario,variant,point,hit_probes,hit_blocks,hit_raw,miss_probes,miss_blocks,miss_raw,cursors,seek_blocks,blocks_per_100,level0_flushes,level0_runs,run_bytes,level0_stored,stored_records,superseded_record_share"
    );
    if flags.read_shapes {
        println!(
            "miss_get,scenario,variant,mix,gets,misses,filter_probes_per_get,filter_fp_per_get,fp_rate,blocks_per_get,decoded_per_get,decoded_per_miss,crc_per_get"
        );
        println!("miss_get_level,scenario,variant,mix,level,blocks_per_get,decoded_per_get");
        println!(
            "scan_shape,scenario,variant,shape,scans,records_per_scan,blocks_per_scan,decoded_per_scan,crc_per_scan"
        );
        println!("scan_shape_level,scenario,variant,shape,level,blocks_per_scan,decoded_per_scan");
    }
    if flags.zipf {
        println!(
            "zipf_get,scenario,variant,phase,theta,gets,blocks_per_get,decoded_per_get,crc_per_get,filter_probes_per_get,filter_fp_per_get,distinct_blocks"
        );
        println!(
            "zipf_reuse,scenario,variant,phase,theta,n_blocks,get_hit_rate,block_hit_rate,s3fifo_hit_rate,lru_bytes,mean_n_bytes"
        );
        println!(
            "zipf_update,scenario,variant,theta,updates,raw_in_delta,raw_out_delta,stored_out_delta,copy_blocks_delta,copy_input_blocks_delta,run_bytes,held_bytes,hit_decoded_per_get"
        );
    }
    if flags.reuse {
        println!(
            "get_work,scenario,variant,stream,theta,gets,blocks_per_get,decoded_per_get,crc_per_get,filter_probes_per_get"
        );
        println!(
            "get_work_level,scenario,variant,stream,theta,level,blocks_per_get,decoded_per_get,mean_block_raw"
        );
        println!(
            "reuse,scenario,variant,cache,workload,theta,policy,capacity_mib,pct_stored,hit_rate,accesses"
        );
        println!("reuse_meta,scenario,variant,records,mean_record_charge,stream_len,run_bytes");
    }
    if settle {
        println!(
            "settled,scenario,variant,run_bytes,held_bytes,hit_decoded_per_get,blocks_per_100_record_scan"
        );
    }
    for s in &scenarios {
        let default = !s.clustered && s.kind != Kind::GroupedRandom;
        if (names.is_empty() && default) || names.iter().any(|n| n == s.name) {
            eprintln!(
                "config,{},{variant},records={},background_threads={threads}",
                s.name, s.records
            );
            run(s, &variant, settle, threads, &flags);
        }
    }
}

/// Live Rust heap of one memtable filled to its budget, per entry beyond its
/// key and value, with random keys and no snapshots. Needs the counting
/// allocator (`tools-memory`, Linux).
fn memtable_overhead(key_len: usize, value_len: usize) {
    #[cfg(all(feature = "tools-memory", target_os = "linux"))]
    {
        assert!(key_len >= 16, "--memtable-key must be at least 16");
        let budget = Options::default().memtable_size;
        let charge = 1 + key_len + value_len + diagnostics::ENTRY_OVERHEAD;
        // Stay just inside one table: a full table rotates before flushing.
        let entries = budget / charge - 64;
        let mut order: Vec<u32> = (0..entries as u32).collect();
        Rng(0x123456789abcdef).shuffle(&mut order);
        let engine = Engine::in_memory(Options::default()).unwrap();
        let ks = engine.keyspace("data").unwrap();
        let mut k = vec![0u8; key_len];
        let mut v = vec![0u8; value_len];
        let before = memory::live();
        for &i in &order {
            let id = i as usize * 7 + 3;
            k[..8].copy_from_slice(&(id as u64 / MEMBERS as u64).to_be_bytes());
            k[8..16].copy_from_slice(&(id as u64 % MEMBERS as u64).to_be_bytes());
            if value_len >= 8 {
                v[..8].copy_from_slice(&(i as u64).to_le_bytes());
            }
            ks.put(&k, &v).unwrap();
        }
        let after = memory::live();
        let stats = engine.engine_stats();
        assert_eq!(stats.flushes, 0);
        assert_eq!(stats.frozen_tables, 0);
        let payload = entries * (key_len + value_len);
        let heap = after - before;
        let arena = stats.active.arena_bytes;
        println!(
            "memtable,key_len,value_len,entries,heap,payload,arena_used,chunk_bytes_held,charged,budget,overhead_per_entry,charged_overhead"
        );
        println!(
            "memtable,{key_len},{value_len},{entries},{heap},{payload},{arena},{},{},{budget},{:.2},{}",
            stats.chunk_bytes_held,
            stats.active.charged,
            (heap - payload) as f64 / entries as f64,
            diagnostics::ENTRY_OVERHEAD,
        );
    }
    #[cfg(not(all(feature = "tools-memory", target_os = "linux")))]
    {
        let _ = (key_len, value_len);
        panic!("--memtable needs the tools-memory feature on Linux");
    }
}
