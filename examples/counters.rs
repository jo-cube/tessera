//! Deterministic, counter-only compaction scenarios; no clocks or timings.
//!
//! No arguments: fixed 4,096-record flushes for locality comparisons.
//! `buffers 64` (or 32/16): automatic flushes and 384 MiB of A-large ingestion.
//! Frozen-buffer comparisons change the private MAX_FROZEN_MEMTABLES constant
//! between builds; it intentionally has no public configuration switch.
use tessera::engine::{Engine, EngineOptions, EngineWorkspace, MergeWorkspace};
// Measure the same private filter implementation without expanding the engine API.
#[allow(dead_code)]
#[path = "../src/run/filter.rs"]
mod filter;
const ENTITIES: usize = 512;
const MEMBERS: usize = 64;
const RECORDS: usize = ENTITIES * MEMBERS;
const FLUSH: usize = 4096;
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
fn value(i: usize, version: u8, weak: bool) -> Vec<u8> {
    if weak {
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
    }
}
fn flush(engine: &Engine) {
    engine.flush().unwrap();
    engine.wait_for_compaction().unwrap();
}
fn ratio(a: u64, b: u64) -> f64 {
    if b == 0 { 0.0 } else { a as f64 / b as f64 }
}
fn scenario(
    name: &str,
    weak: bool,
    clustered: bool,
    records: usize,
    small: bool,
    budget: Option<usize>,
) {
    let value = |i, version, weak| {
        let mut out = value(i, version, weak);
        if small {
            out.truncate(48);
        }
        out
    };
    let engine = Engine::new(EngineOptions {
        memtable_budget: budget.unwrap_or(EngineOptions::default().memtable_budget),
        background_threads: 1,
        ..EngineOptions::default()
    })
    .unwrap();
    let mut peak = 0;
    let mut ingested = 0u64;
    let mut order: Vec<_> = (0..records).collect();
    let mut rng = Rng(0x123456789abcdef);
    if !clustered {
        rng.shuffle(&mut order);
    }
    for (at, &i) in order.iter().enumerate() {
        let v = value(i, 0, weak);
        engine.put(&key(i), &v).unwrap();
        ingested += (16 + v.len()) as u64;
        if budget.is_some() {
            peak = peak.max(engine.stats().memtable_bytes);
        }
        if budget.is_none() && (at + 1) % FLUSH == 0 {
            flush(&engine);
        }
    }
    let mut updated = vec![false; records];
    let mut updates: Vec<_> = if clustered {
        (records / MEMBERS * 9 / 10 * MEMBERS..records).collect()
    } else {
        (0..records).collect()
    };
    if !clustered {
        rng.shuffle(&mut updates);
        updates.truncate(records / 2);
    }
    for (at, &i) in updates.iter().enumerate() {
        let v = value(i, 1, weak);
        engine.put(&key(i), &v).unwrap();
        ingested += (16 + v.len()) as u64;
        if budget.is_some() {
            peak = peak.max(engine.stats().memtable_bytes);
        }
        updated[i] = true;
        if budget.is_none() && (at + 1) % FLUSH == 0 {
            flush(&engine);
        }
    }
    flush(&engine);
    let mut merge = MergeWorkspace::new();
    let mut manual_output = 0u64;
    while let Some(report) = engine.compact(&mut merge).unwrap() {
        manual_output += report.output_bytes as u64;
    }
    engine.wait_for_compaction().unwrap();
    let mut scans = EngineWorkspace::new();
    for _ in 0..100 {
        let guard = engine.read();
        let mut scan = guard.scan(&mut scans);
        for _ in 0..100 {
            assert!(scan.next_record().unwrap().is_some());
        }
    }
    // A standalone filter over the scenario's distinct keys, including when
    // bottom-level runs omit filters. The million probes are all absent.
    let mut hashes = filter::KeyHashes::new();
    for i in 0..records {
        hashes.add(&key(i));
    }
    let filter = hashes.finish().unwrap();
    let positives = (0..1_000_000)
        .filter(|&at| {
            let mut absent = key(at % records).to_vec();
            absent.extend_from_slice(&(at as u64).to_be_bytes());
            filter.may_contain(filter::hash(&absent))
        })
        .count();
    // All reader paths are checked against the independent version oracle.
    let mut ws = EngineWorkspace::new();
    // Multi-get and the scan below check every record. Sample individual gets
    // on the scaled buffer workload to avoid decoding hundreds of GiB again.
    let stride = if budget.is_some() {
        (records / RECORDS).max(1)
    } else {
        1
    };
    for (i, &changed) in updated.iter().enumerate().step_by(stride) {
        assert_eq!(
            engine.get(&key(i), &mut ws).unwrap().unwrap().value,
            value(i, u8::from(changed), weak)
        );
    }
    for start in (0..records).step_by(128) {
        let keys: Vec<_> = (start..start + 128).rev().map(key).collect();
        engine
            .multi_get(&keys, &mut ws, |at, got| {
                let i = start + 127 - at;
                assert_eq!(got.unwrap(), value(i, u8::from(updated[i]), weak));
            })
            .unwrap();
    }
    let guard = engine.read();
    let mut scan = guard.scan(&mut ws);
    for (i, &changed) in updated.iter().enumerate() {
        let rec = scan.next_record().unwrap().unwrap();
        assert_eq!(rec.key, key(i));
        assert_eq!(rec.value, value(i, u8::from(changed), weak));
    }
    assert!(scan.next_record().unwrap().is_none());
    drop(scan);
    drop(guard);
    // Fixed present keys and in-range gaps. Each get has its own block load;
    // workspace buffers are reused, but decoded blocks are never cached.
    let mut hits = EngineWorkspace::new();
    for at in 0..10_000 {
        let i = at * 7919 % records;
        assert_eq!(
            engine.get(&key(i), &mut hits).unwrap().unwrap().value,
            value(i, u8::from(updated[i]), weak)
        );
    }
    let mut misses = EngineWorkspace::new();
    for at in 0..1_000 {
        let i = at * 7919 % (records - 1);
        let mut missing = key(i).to_vec();
        missing.push(0);
        assert!(engine.get(&missing, &mut misses).unwrap().is_none());
    }
    let s = engine.stats();
    assert_eq!(s.build.raw_bytes_ingested, ingested);
    let background = engine.background_status().unwrap();
    assert_eq!(background.failures, 0);
    assert!(background.panic.is_none());
    println!(
        "{name},{},{},{},{},{:.3},{:.3},{},{},{:.3},{:.6},{peak},{},{},{},{},{:.6},{:.6},{ingested}",
        s.run_bytes,
        s.filter_bytes,
        s.index_bytes,
        s.block_bytes_live,
        ratio(hits.decoded_bytes(), 10_000),
        ratio(misses.decoded_bytes(), 1_000),
        hits.blocks_loaded(),
        misses.blocks_loaded(),
        ratio(scans.blocks_loaded(), 100),
        ratio(positives as u64, 1_000_000),
        s.flushes,
        s.runs,
        s.level0_flushes,
        s.level0_runs,
        ratio(
            s.flushed_bytes + background.output_bytes + manual_output,
            s.flushed_bytes
        ),
        ratio(s.build.raw_bytes_compressed, ingested),
    );
    engine.close().unwrap();
}
fn main() {
    let mut args = std::env::args().skip(1);
    let budget = args.next().map(|mode| {
        assert_eq!(mode, "buffers", "usage: counters [buffers BUDGET_MiB]");
        args.next()
            .expect("budget in MiB")
            .parse::<usize>()
            .unwrap()
            << 20
    });
    println!(
        "scenario,stored_bytes,filter_bytes,index_bytes,frame_bytes,hit_decoded_bytes_per_get,miss_decoded_bytes_per_get,hit_blocks,miss_blocks,blocks_per_100_record_scan,standalone_filter_fp,peak_sampled_memtable_charge,flushes,runs,l0_depth,l0_runs,write_amplification,raw_bytes_compressed_per_ingested_byte,ingested_bytes"
    );
    scenario("B-random", true, false, RECORDS, false, budget);
    scenario("clustered", false, true, RECORDS, false, budget);
    scenario("A-random", false, false, RECORDS, false, budget);
    scenario(
        "A-large",
        false,
        false,
        if budget.is_some() { 4_194_304 } else { 200_704 },
        true,
        budget,
    );
}
