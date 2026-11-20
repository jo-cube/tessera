//! Deterministic work and memory counters by level, using the hidden
//! `diagnostics` feature. Each scenario loads keys in random order with
//! automatic flushes, updates half of them at random, waits for background
//! compaction, checks every read path against a version oracle and prints
//! long-format CSV rows. See tools/README.md.
//!
//! `counters [--variant LABEL] [--settle] [--scale SHIFT] [--copy-audit]
//! [--c2] [--write-only] [--memtable VALUE_LEN] [--worst]
//! [SCENARIO...]`
use std::time::Instant;
use tessera::engine::diagnostics;
use tessera::engine::diagnostics::MergeWorkspace;
use tessera::engine::{Engine, EngineOptions, EngineWorkspace};

mod c2;
#[cfg(all(feature = "tools-memory", target_os = "linux"))]
mod memory;

const MEMBERS: usize = 64;
struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
    fn shuffle<T>(&mut self, values: &mut [T]) {
        for i in (1..values.len()).rev() {
            values.swap(i, self.next() as usize % (i + 1));
        }
    }
}
fn key(i: usize) -> [u8; 16] {
    let mut key = [0; 16];
    key[..8].copy_from_slice(&(i as u64 / MEMBERS as u64).to_be_bytes());
    key[8..].copy_from_slice(&(i as u64 % MEMBERS as u64).to_be_bytes());
    key
}
fn value(i: usize, version: u8, weak: bool, len: usize) -> Vec<u8> {
    let mut out = if weak {
        let mut rng =
            Rng((i as u64 + 1).wrapping_mul(0x9e3779b97f4a7c15) ^ (u64::from(version) << 40));
        (0..600)
            .map(|_| b"abcdefghijklmnopqrstuvwxyz0123456789"[rng.next() as usize % 36])
            .collect()
    } else {
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
        out
    };
    out.truncate(len);
    out
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
    weak: bool,
    records: usize,
    value_len: usize,
    /// Time-ordered load, then updates to the newest 10% of entities in order.
    clustered: bool,
}

#[derive(Default)]
struct Flags {
    copy_audit: bool,
    c2: bool,
    write_only: bool,
    worst: bool,
}

static BOTTOM_JOBS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// Point and scan work per operation: hits and misses on spread keys, then
/// short scans from spread 7-byte prefixes (one seek, then 100 records).
fn probe_reads(
    engine: &Engine,
    name: &str,
    variant: &str,
    point: &str,
    records: usize,
    expect: &dyn Fn(usize) -> Vec<u8>,
) {
    const GETS: usize = 10_000;
    const SEEKS: usize = 200;
    let mut hits = EngineWorkspace::new();
    for at in 0..GETS {
        let i = at * 7919 % records;
        assert_eq!(
            engine.get(&key(i), &mut hits).unwrap().unwrap().value,
            expect(i)
        );
    }
    let mut misses = EngineWorkspace::new();
    for at in 0..GETS {
        let i = at * 7919 % (records - 1);
        let mut missing = key(i).to_vec();
        missing.push(0);
        assert!(engine.get(&missing, &mut misses).unwrap().is_none());
    }
    let (mut seek_blocks, mut scan_blocks) = (0, 0);
    let mut ws = EngineWorkspace::new();
    for at in 0..SEEKS {
        // Prefixes of 256 entities (16,384 keys); start well before the end.
        let i = (at * 104_729) % (records - 20_000);
        let prefix = &key(i)[..7];
        let guard = engine.read();
        // One seek alone, then a fresh scan of 100 records from it.
        let before = ws.blocks_loaded();
        assert!(
            guard
                .scan_prefix(prefix, &mut ws)
                .next_record()
                .unwrap()
                .is_some()
        );
        seek_blocks += ws.blocks_loaded() - before;
        let before = ws.blocks_loaded();
        let mut scan = guard.scan_prefix(prefix, &mut ws);
        for _ in 0..100 {
            assert!(scan.next_record().unwrap().is_some());
        }
        drop(scan);
        scan_blocks += ws.blocks_loaded() - before;
    }
    let stats = engine.stats();
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
        per(hits.filter_probes(), GETS),
        per(hits.blocks_loaded(), GETS),
        per(hits.decoded_bytes(), GETS),
        per(misses.filter_probes(), GETS),
        per(misses.blocks_loaded(), GETS),
        per(misses.decoded_bytes(), GETS),
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

fn run(s: &Scenario, variant: &str, settle: bool, threads: usize, flags: &Flags) {
    let started = Instant::now();
    let (name, weak, records, len) = (s.name, s.weak, s.records, s.value_len);
    let engine = std::sync::Arc::new(
        Engine::new({
            let mut options = EngineOptions::default();
            options.background_threads = threads;
            options
        })
        .unwrap(),
    );
    #[cfg(all(feature = "tools-memory", target_os = "linux"))]
    let sampler = memory::Sampler::start(std::sync::Arc::clone(&engine), name, variant);
    #[cfg(all(feature = "tools-memory", target_os = "linux"))]
    diagnostics::set_memory_observer(memory::sample_stats);
    let mut ingested = 0u64;
    let mut order: Vec<_> = (0..records).collect();
    let mut rng = Rng(0x123456789abcdef);
    if !s.clustered {
        rng.shuffle(&mut order);
    }
    for &i in &order {
        let v = value(i, 0, weak, len);
        engine.put(&key(i), &v).unwrap();
        ingested += (16 + v.len()) as u64;
    }
    #[cfg(all(feature = "tools-memory", target_os = "linux"))]
    memory::phase_end(&engine, "load_end", 1);
    let mut updated = vec![false; records];
    let mut updates = order;
    if s.clustered {
        updates = (records / MEMBERS * 9 / 10 * MEMBERS..records).collect();
    } else {
        rng.shuffle(&mut updates);
        updates.truncate(records / 2);
    }
    for &i in &updates {
        let v = value(i, 1, weak, len);
        engine.put(&key(i), &v).unwrap();
        ingested += (16 + v.len()) as u64;
        updated[i] = true;
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

    let expect = |i: usize| value(i, u8::from(updated[i]), weak, len);
    let mut scans = EngineWorkspace::new();
    for _ in 0..100 {
        let guard = engine.read();
        let mut scan = guard.scan(&mut scans);
        for _ in 0..100 {
            assert!(scan.next_record().unwrap().is_some());
        }
    }
    // Every reader path against the oracle: sampled gets, then multi-get
    // and a full scan over every record.
    let mut ws = EngineWorkspace::new();
    for i in (0..records).step_by(61) {
        assert_eq!(
            engine.get(&key(i), &mut ws).unwrap().unwrap().value,
            expect(i)
        );
    }
    for start in (0..records).step_by(128) {
        let keys: Vec<_> = (start..(start + 128).min(records)).rev().map(key).collect();
        let last = start + keys.len() - 1;
        engine
            .multi_get(&keys, &mut ws, |at, got| {
                assert_eq!(got.unwrap(), expect(last - at));
            })
            .unwrap();
    }
    {
        let guard = engine.read();
        let mut scan = guard.scan(&mut ws);
        for i in 0..records {
            let rec = scan.next_record().unwrap().unwrap();
            assert_eq!(rec.key, key(i));
            assert_eq!(rec.value, expect(i));
        }
        assert!(scan.next_record().unwrap().is_none());
    }
    let mut hits = EngineWorkspace::new();
    for at in 0..10_000 {
        let i = at * 7919 % records;
        assert_eq!(
            engine.get(&key(i), &mut hits).unwrap().unwrap().value,
            expect(i)
        );
    }
    let mut misses = EngineWorkspace::new();
    for at in 0..1_000 {
        let i = at * 7919 % (records - 1);
        let mut missing = key(i).to_vec();
        missing.push(0);
        assert!(engine.get(&missing, &mut misses).unwrap().is_none());
    }
    let stats = engine.stats();
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
    if flags.copy_audit {
        print_copy_audit(name, variant);
    }
    if flags.c2 {
        // At least 64 MiB raw of evenly spaced bottom segments, or all.
        let every = (diagnostics::bottom_raw(&engine) / (64 << 20)).max(1);
        let samples = diagnostics::bottom_segments(&engine, every);
        c2::run(name, &samples);
    }
    let flush_raw = work[0].raw_out;
    let raw: u64 = work.iter().map(|w| w.raw_out).sum();
    let stored: u64 = work.iter().map(|w| w.stored).sum();
    println!(
        "reads,{name},{variant},{},{},{:.1},{:.1},{:.3},{:.3},{:.3},{},{},{depth},{load_seconds:.1},{:.1}",
        stats.run_bytes,
        stats.held_bytes,
        ratio(hits.decoded_bytes(), 10_000),
        ratio(misses.decoded_bytes(), 1_000),
        ratio(scans.blocks_loaded(), 100),
        ratio(stored, work[0].stored),
        ratio(raw, flush_raw),
        stats.flushes,
        stats.runs,
        started.elapsed().as_secs_f64()
    );
    let m = diagnostics::maintenance(&engine);
    println!(
        "waits,{name},{variant},{},{:.3},{:.3},{},{}",
        m.level0_waits,
        m.level0_wait_ns as f64 / 1e9,
        m.writer_wait_ns as f64 / 1e9,
        background.compactions,
        BOTTOM_JOBS.load(std::sync::atomic::Ordering::Relaxed)
    );
    probe_reads(&engine, name, variant, "end", records, &expect);
    if settle {
        // A control without timing effects: rewrite everything into the
        // bottom level, where sequences are erased, then measure again.
        let mut merge = MergeWorkspace::new();
        while diagnostics::compact_step(&engine, &mut merge).unwrap() {}
        engine.wait_for_compaction().unwrap();
        let mut hits = EngineWorkspace::new();
        for at in 0..10_000 {
            let i = at * 7919 % records;
            assert_eq!(
                engine.get(&key(i), &mut hits).unwrap().unwrap().value,
                expect(i)
            );
        }
        let mut scans = EngineWorkspace::new();
        for _ in 0..100 {
            let guard = engine.read();
            let mut scan = guard.scan(&mut scans);
            for _ in 0..100 {
                assert!(scan.next_record().unwrap().is_some());
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
            engine.stats().run_bytes,
            engine.stats().held_bytes,
            ratio(hits.decoded_bytes(), 10_000),
            ratio(scans.blocks_loaded(), 100),
        );
    }
    if flags.worst {
        worst_point(&engine, s, variant, &updated);
    }
    #[cfg(all(feature = "tools-memory", target_os = "linux"))]
    {
        println!(
            "reader_memory,{name},{variant},{},{}",
            scans.capacity() + ws.capacity() + hits.capacity() + misses.capacity(),
            scans.context_bytes()
                + ws.context_bytes()
                + hits.context_bytes()
                + misses.context_bytes()
        );
        sampler.stop();
        memory::sample_stats("final", engine.stats());
    }
    std::sync::Arc::try_unwrap(engine).unwrap().close().unwrap();
}

/// The engine's writer stop at level 0, in flush groups.
const LEVEL0_STOP: usize = 12;

/// Level 0 at its fullest before a job: with new jobs held off, update
/// random keys until level 0 holds `LEVEL0_STOP` flush groups, staying below
/// the stop (a writer at the stop would wait for the held compaction), then
/// measure reads and memory.
fn worst_point(engine: &Engine, s: &Scenario, variant: &str, updated: &[bool]) {
    let stop = LEVEL0_STOP;
    let (name, weak, records, len) = (s.name, s.weak, s.records, s.value_len);
    let mut version: Vec<u8> = updated.iter().map(|&u| u8::from(u)).collect();
    let held = diagnostics::hold_compaction(engine);
    let mut rng = Rng(0xfeed_face_cafe_f00d);
    let mut written = 0usize;
    loop {
        let frozen = engine.stats().frozen_tables;
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
            engine.put(&key(i), &value(i, 2, weak, len)).unwrap();
            version[i] = 2;
            written += 1;
        }
    }
    let expect = |i: usize| value(i, version[i], weak, len);
    probe_reads(engine, name, variant, "worst", records, &expect);
    eprintln!("worst,{name},{variant},written={written}");
    #[cfg(all(feature = "tools-memory", target_os = "linux"))]
    memory::sample_stats("worst", engine.stats());
    drop(held);
}

fn main() {
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
    let mut flags = Flags::default();
    let mut memtable = None;
    let mut names = Vec::new();
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--variant" => variant = args.next().expect("LABEL"),
            "--settle" => settle = true,
            "--threads" => {
                let value = args.next().expect("COUNT or default");
                threads = if value == "default" {
                    EngineOptions::default().background_threads
                } else {
                    value.parse().expect("positive thread count")
                };
            }
            "--scale" => scale = args.next().expect("SHIFT").parse().unwrap(),
            "--copy-audit" => flags.copy_audit = true,
            "--c2" => flags.c2 = true,
            "--write-only" => flags.write_only = true,
            "--worst" => flags.worst = true,
            "--memtable" => memtable = Some(args.next().expect("VALUE_LEN").parse().unwrap()),
            _ => names.push(arg),
        }
    }
    let scenarios = [
        Scenario {
            name: "A",
            weak: false,
            records: 2_097_152 >> scale,
            value_len: 600,
            clustered: false,
        },
        Scenario {
            name: "B",
            weak: true,
            records: 2_097_152 >> scale,
            value_len: 600,
            clustered: false,
        },
        Scenario {
            name: "A-large",
            weak: false,
            records: 4_194_304 >> scale,
            value_len: 48,
            clustered: false,
        },
        Scenario {
            name: "clustered",
            weak: false,
            records: 2_097_152 >> scale,
            value_len: 600,
            clustered: true,
        },
    ];
    if let Some(len) = memtable {
        memtable_overhead(len);
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
    println!("work,scenario,variant,destination,raw_in,raw_out,stored");
    println!(
        "copy,scenario,variant,blocks,raw,stored,segments,inherited,input_blocks,input_raw,input_segments"
    );
    println!("erasure,scenario,variant,records,erased,probes,kept_unfiltered,kept_filtered");
    println!(
        "level,scenario,variant,level,runs,records,raw,frames,dictionaries,segment_table,index,filters,footer,physical"
    );
    println!(
        "reads,scenario,variant,run_bytes,held_bytes,hit_decoded_per_get,miss_decoded_per_get,blocks_per_100_record_scan,stored_wa,raw_wa,flushes,runs,depth,load_seconds,total_seconds"
    );
    println!(
        "waits,scenario,variant,level0_waits,level0_wait_s,writer_wait_s,compactions,bottom_jobs"
    );
    println!(
        "probe,scenario,variant,point,hit_probes,hit_blocks,hit_raw,miss_probes,miss_blocks,miss_raw,cursors,seek_blocks,blocks_per_100,level0_flushes,level0_runs,run_bytes,level0_stored,stored_records,superseded_record_share"
    );
    if settle {
        println!(
            "settled,scenario,variant,run_bytes,held_bytes,hit_decoded_per_get,blocks_per_100_record_scan"
        );
    }
    for s in &scenarios {
        if (names.is_empty() && !s.clustered) || names.iter().any(|n| n == s.name) {
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
fn memtable_overhead(value_len: usize) {
    #[cfg(all(feature = "tools-memory", target_os = "linux"))]
    {
        let budget = EngineOptions::default().memtable_budget;
        let charge = 16 + value_len + tessera::engine::ENTRY_OVERHEAD;
        // Stay just inside one table: a full table rotates before flushing.
        let entries = budget / charge - 64;
        let mut order: Vec<_> = (0..entries).collect();
        Rng(0x123456789abcdef).shuffle(&mut order);
        let engine = Engine::new(EngineOptions::default()).unwrap();
        let mut v = vec![0u8; value_len];
        let before = memory::live();
        for &i in &order {
            v[..8].copy_from_slice(&(i as u64).to_le_bytes());
            engine.put(&key(i * 7 + 3), &v).unwrap();
        }
        let after = memory::live();
        let stats = engine.stats();
        assert_eq!(stats.flushes, 0);
        assert_eq!(stats.frozen_tables, 0);
        let payload = entries * (16 + value_len);
        let heap = after - before;
        let arena = stats.active.arena_bytes;
        println!(
            "memtable,value_len,entries,heap,payload,arena_used,chunk_bytes_held,charged,budget,overhead_per_entry,charged_overhead"
        );
        println!(
            "memtable,{value_len},{entries},{heap},{payload},{arena},{},{},{budget},{:.2},{}",
            stats.chunk_bytes_held,
            stats.active.charged,
            (heap - payload) as f64 / entries as f64,
            tessera::engine::ENTRY_OVERHEAD,
        );
    }
    #[cfg(not(all(feature = "tools-memory", target_os = "linux")))]
    {
        let _ = value_len;
        panic!("--memtable needs the tools-memory feature on Linux");
    }
}
