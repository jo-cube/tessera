//! Milestone 5J: what building a run's key filter costs, by component.
//!
//! The filter steps are copied here from `src/run/filter.rs` (crate-private)
//! and timed alone on the same sorted records that `RunBuilder` encodes: the
//! hash, the hash with distinct-key tracking and buffer growth, and filter
//! finalization (allocating the bits and setting seven probes per key).
//! Finalization is also timed with a cache-local layout, every probe of a key
//! in one 64-byte line, to separate the probes' memory traffic from their
//! arithmetic. `RunBuilder` time includes the filter, so the time of a build
//! without one comes from running this benchmark on a copy whose
//! `RunBuilder::add` skips `key_hashes.add` and whose `finish` passes no
//! filter (`TESSERA_SOURCE=<copy> scripts/validate.sh filtercost`).
//!
//! First, one large flush, snapshot and compaction report the filter's
//! resident and transient bytes through the engine's own statistics.

use std::hash::{DefaultHasher, Hasher};
use std::hint::black_box;
use std::time::Instant;

use tessera::engine::{CompactionOptions, Engine, EngineOptions, UpdateError};
use tessera::memtable::{TARGET_RUN_LEN, WriteError};
use tessera::run::{MergeWorkspace, Run, RunBuilder};

use super::{
    BASE_SEQ, FLAT_TREE, Owned, Rng, fill, filled, large_values, mib, peak_during, shuffled,
    tree_json, tree_key, tree_small,
};

const PROBES: u64 = 7;
const BITS_PER_KEY: usize = 10;

fn sip(key: &[u8]) -> u64 {
    let mut hasher = DefaultHasher::new();
    hasher.write(key);
    hasher.finish()
}

fn reduce(hash: u64, n: u64) -> usize {
    ((u128::from(hash) * u128::from(n)) >> 64) as usize
}

/// Distinct-key tracking and buffer growth as in `KeyHashes::add`.
fn collect(records: &[Owned], hash: impl Fn(&[u8]) -> u64) -> Vec<u64> {
    let mut hashes: Vec<u64> = Vec::new();
    for record in records {
        let h = hash(&record.key);
        if hashes.last() == Some(&h) {
            continue;
        }
        let len = hashes.len();
        if len == hashes.capacity() {
            hashes.reserve_exact((len * 2).max(1024) - len);
        }
        hashes.push(h);
    }
    hashes
}

/// Finalization as in `KeyHashes::finish`.
fn finish(hashes: &[u64]) -> Box<[u64]> {
    let mut words = vec![0u64; (hashes.len() * BITS_PER_KEY).div_ceil(64)].into_boxed_slice();
    let bits = words.len() as u64 * 64;
    for &h in hashes {
        let step = h.rotate_left(32) | 1;
        for i in 0..PROBES {
            let bit = reduce(h.wrapping_add(i.wrapping_mul(step)), bits);
            words[bit / 64] |= 1 << (bit % 64);
        }
    }
    words
}

/// Whether the filter from [`finish`] passes `h`.
fn passes(words: &[u64], h: u64) -> bool {
    let bits = words.len() as u64 * 64;
    let step = h.rotate_left(32) | 1;
    (0..PROBES).all(|i| {
        let bit = reduce(h.wrapping_add(i.wrapping_mul(step)), bits);
        words[bit / 64] & (1 << (bit % 64)) != 0
    })
}

/// The same probes confined to one 512-bit line per key.
fn finish_local(hashes: &[u64]) -> Box<[u64]> {
    let lines = (hashes.len() * BITS_PER_KEY).div_ceil(512).max(1);
    let mut words = vec![0u64; lines * 8].into_boxed_slice();
    for &h in hashes {
        let base = reduce(h, lines as u64) * 8;
        let step = h.rotate_left(32) | 1;
        for i in 0..PROBES {
            let bit = (h.wrapping_add(i.wrapping_mul(step)) >> 55) as usize;
            words[base + bit / 64] |= 1 << (bit % 64);
        }
    }
    words
}

fn median_ns(rounds: usize, records: usize, mut op: impl FnMut()) -> f64 {
    let mut samples: Vec<f64> = (0..rounds)
        .map(|_| {
            let start = Instant::now();
            op();
            start.elapsed().as_nanos() as f64 / records as f64
        })
        .collect();
    samples.sort_by(f64::total_cmp);
    samples[rounds / 2]
}

fn build(records: &[Owned]) -> Run<Vec<u8>> {
    let mut builder = RunBuilder::default();
    for record in records {
        builder
            .add(&record.key, record.seq, record.value())
            .unwrap();
    }
    builder.finish()
}

/// Sorted random 16-byte keys with 8-byte values, about `target` bytes of
/// keys and values.
fn binary_keys(target: usize) -> Vec<Owned> {
    let mut rng = Rng(31);
    let mut records: Vec<Owned> = (0..target as u64 / 24)
        .map(|i| Owned {
            key: rng.bytes(16),
            seq: BASE_SEQ + i,
            value: Some(rng.bytes(8)),
        })
        .collect();
    records.sort_by(|a, b| a.key.cmp(&b.key));
    records.dedup_by(|a, b| a.key == b.key);
    records
}

/// Tree keys under a long shared prefix with 32-byte values.
fn prefixed_32(target: usize) -> Vec<Owned> {
    let mut rng = Rng(32);
    fill(target, |i| {
        let mut key = b"tenant/acme-corporation/orders/".to_vec();
        key.extend_from_slice(&tree_key(i, FLAT_TREE));
        vec![Owned {
            key,
            seq: BASE_SEQ + i,
            value: Some(rng.bytes(32)),
        }]
    })
}

pub(super) fn filter_cost_bench() {
    filter_memory_bench();
    println!();
    println!(
        "filtercost: ns per record, median of rounds; build includes the filter; components are the filter steps alone"
    );
    println!();
    println!(
        "| dataset | input MiB | records | flush | build | hash | hash+track | finish | finish local | filter steps | of build | filter KiB | of stored | build KiB | false positive |"
    );
    println!("|{}", "---|".repeat(15));
    type Dataset = fn(usize) -> Vec<Owned>;
    let datasets: [(&str, Dataset); 5] = [
        ("8-byte, tree keys", tree_small),
        ("32-byte, shared prefix", prefixed_32),
        ("json, tree keys", tree_json),
        ("4 KiB values", large_values),
        ("8-byte, binary keys", binary_keys),
    ];
    for (name, dataset) in datasets {
        for (target, rounds) in [(1usize << 20, 15), (16 << 20, 5)] {
            let records = dataset(target);
            let count = records.len();
            let input: usize = records
                .iter()
                .map(|r| r.key.len() + r.value.as_ref().map_or(0, Vec::len))
                .sum();
            let run = build(&records);
            let build_ns = median_ns(rounds, count, || {
                black_box(build(&records));
            });
            let hash_ns = median_ns(rounds, count, || {
                let mut sum = 0u64;
                for record in &records {
                    sum = sum.wrapping_add(sip(black_box(&record.key)));
                }
                black_box(sum);
            });
            let track_ns = median_ns(rounds, count, || {
                black_box(collect(&records, sip));
            });
            let hashes = collect(&records, sip);
            let finish_ns = median_ns(rounds, count, || {
                black_box(finish(black_box(&hashes)));
            });
            let local_ns = median_ns(rounds, count, || {
                black_box(finish_local(black_box(&hashes)));
            });
            let steps = track_ns + finish_ns;
            // Absent keys: each stored key with a zero byte appended, which
            // no dataset here stores.
            let words = finish(&hashes);
            let false_positive = records
                .iter()
                .filter(|r| {
                    let mut key = r.key.clone();
                    key.push(0);
                    passes(&words, sip(&key))
                })
                .count() as f64
                / count as f64
                * 100.0;
            // The same records flushed from a memtable, as the engine does.
            let (table, _) = filled(&records, &shuffled(count, 5));
            let frozen = table.freeze();
            let flush_ns = median_ns(rounds, count, || {
                black_box(frozen.flush(TARGET_RUN_LEN).count());
            });
            drop(frozen);
            println!(
                "| {name} | {:.1} | {count} | {flush_ns:.0} | {build_ns:.0} | {hash_ns:.1} | {track_ns:.1} | {finish_ns:.1} | {local_ns:.1} | {steps:.1} | {:.0}% | {:.0} | {:.1}% | {:.0} | {false_positive:.2}% |",
                input as f64 / (1 << 20) as f64,
                steps / build_ns * 100.0,
                run.filter_bytes() as f64 / 1024.0,
                run.filter_bytes() as f64 / run.as_bytes().len() as f64 * 100.0,
                hashes.capacity() as f64 * 8.0 / 1024.0,
            );
        }
    }
}

/// Puts values from `value` under the tree keys `first`, `first + step`, …
/// until the memtable is full, and returns how many were written.
fn fill_table(engine: &Engine, first: u64, step: u64, value: &mut impl FnMut() -> Vec<u8>) -> u64 {
    for i in 0..100_000_000u64 {
        let key = tree_key(first + i * step, (1_000, 100, 1_000));
        match engine.put(&key, &value()) {
            Ok(_) => {}
            Err(UpdateError::Rejected(WriteError::BudgetExceeded { .. })) => return i,
            Err(error) => panic!("{error}"),
        }
    }
    panic!("the memtable did not fill");
}

/// One large flush, a snapshot of it, an overwriting flush, and a full
/// compaction into one run, with what the engine reports about filters and
/// the peak Rust heap above the starting level during each step.
fn large_runs(name: &str, budget: usize, mut value: impl FnMut() -> Vec<u8>) {
    let engine = Engine::new(EngineOptions {
        memtable_budget: budget,
        target_run_len: 1 << 30,
        ..EngineOptions::default()
    });
    let written = fill_table(&engine, 0, 2, &mut value);
    let mut flushed = None;
    let start = Instant::now();
    let flush_peak = peak_during(|| flushed = Some(engine.flush().unwrap()));
    let flush_s = start.elapsed().as_secs_f64();
    let flushed = flushed.unwrap();
    let snapshot = engine.snapshot().unwrap();
    // Every other key of the first flush, and as many new ones between.
    fill_table(&engine, 1, 1, &mut value);
    engine.flush().unwrap();
    let options = CompactionOptions {
        max_overlap: 1,
        max_input_bytes: 1 << 30,
        max_output_bytes: 1 << 30,
        ..CompactionOptions::default()
    };
    let mut workspace = MergeWorkspace::new();
    let mut compacted = None;
    let start = Instant::now();
    let compact_peak = peak_during(|| {
        compacted = engine.compact(options, &mut workspace).unwrap();
    });
    let compact_s = start.elapsed().as_secs_f64();
    let compacted = compacted.expect("two overlapping runs");
    let held = engine.stats();
    drop(snapshot);
    let after = engine.stats();
    println!(
        "| {name} | {written} | {:.1} | {:.2} | {:.2} | {:.1} | {:.0} | {:.1} | {} | {:.1} | {:.2} | {:.2} | {:.1} | {:.0} | {:.1} | {:.1} | {:.2} | {:.2} |",
        mib(flushed.run_bytes),
        mib(flushed.filter_bytes),
        mib(flushed.filter_build_bytes),
        mib(flush_peak as usize),
        flushed.records as f64 / flush_s / 1e3,
        mib(compacted.output_bytes),
        compacted.stats.records_written,
        mib(compacted.output_filter_bytes),
        mib(compacted.filter_build_bytes),
        mib(compacted.workspace_bytes),
        mib(compact_peak as usize),
        compacted.stats.records_read as f64 / compact_s / 1e3,
        mib(held.retained_run_bytes),
        mib(held.retained_filter_bytes),
        mib(after.retained_bytes),
        mib(after.filter_bytes),
    );
}

pub(super) fn filter_memory_bench() {
    println!(
        "filtermem: one flush of a full memtable (target run 1 GiB), a snapshot, an overwriting flush, one full compaction; MiB from engine reports, peaks are the Rust heap above the start"
    );
    println!();
    println!(
        "| dataset | flushed records | flush stored | flush filter | flush hash buffer | flush peak | flush k rec/s | compacted stored | compacted records | compacted filter | compaction hash buffer | workspace estimate | compaction peak | compaction k rec/s | snapshot retains stored | snapshot retains filters | retained after drop | filters after |"
    );
    println!("|{}", "---|".repeat(18));
    let mut rng = Rng(41);
    large_runs("8-byte, 256 MiB table", 256 << 20, || rng.bytes(8));
    let mut rng = Rng(42);
    large_runs("json-sized 100 bytes, 256 MiB table", 256 << 20, || {
        let mut value = rng.bytes(20);
        value.resize(100, b' ');
        value
    });
    let mut rng = Rng(43);
    large_runs("4 KiB, 256 MiB table", 256 << 20, || {
        let mut value = rng.bytes(1024);
        value.resize(4096, 0);
        value
    });
}
