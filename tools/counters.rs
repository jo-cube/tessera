//! Deterministic work and memory counters by level, using the hidden
//! `diagnostics` feature. Each scenario loads keys in random order with
//! automatic flushes, updates half of them at random, waits for background
//! compaction, checks every read path against a version oracle and prints
//! long-format CSV rows. See tools/README.md.
//!
//! `counters [--variant LABEL] [--settle] [--scale SHIFT] [--copy-audit]
//! [--c2] [--write-only] [--memtable VALUE_LEN] [SCENARIO...]`
use std::time::Instant;
use tessera::engine::diagnostics;
use tessera::engine::{Engine, EngineOptions, EngineWorkspace, MergeWorkspace};

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
        Engine::new(EngineOptions {
            background_threads: threads,
            ..EngineOptions::default()
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
    if settle {
        // A control without timing effects: rewrite everything into the
        // bottom level, where sequences are erased, then measure again.
        let mut merge = MergeWorkspace::new();
        while engine.compact(&mut merge).unwrap().is_some() {}
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

fn main() {
    diagnostics::set_bottom_job_observer(|job| {
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
    println!("erasure,scenario,variant,records,erased,probes,kept_unfiltered,kept_filtered");
    println!(
        "level,scenario,variant,level,runs,records,raw,frames,dictionaries,segment_table,index,filters,footer,physical"
    );
    println!(
        "reads,scenario,variant,run_bytes,held_bytes,hit_decoded_per_get,miss_decoded_per_get,blocks_per_100_record_scan,stored_wa,raw_wa,flushes,runs,depth,load_seconds,total_seconds"
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
