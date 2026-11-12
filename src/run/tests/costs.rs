//! Measurements rather than tests, on the payloads of the benchmark's
//! `density` mode: what each codec costs and saves within one run, and how
//! often a decoded-block cache, which the engine does not have, would spare a
//! decode. Run them in release:
//!
//! ```text
//! cargo test --release --lib run::tests::costs -- --ignored --nocapture
//! ```

use std::collections::{BTreeMap, HashMap};
use std::hint::black_box;
use std::time::Instant;

use crate::block::Value;
use crate::run::{Codec, ReadWorkspace, Run, RunBuilder};

/// Deterministic xorshift generator, as in the benchmark.
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

const WORDS: [&str; 8] = [
    "alpha", "bravo", "charlie", "delta", "north", "south", "amber", "green",
];

fn words(rng: &mut Rng, count: u64) -> String {
    (0..count)
        .map(|_| WORDS[rng.below(8) as usize])
        .collect::<Vec<_>>()
        .join(" ")
}

/// A sensor reading of 14 to 32 bytes.
fn small(index: u64) -> Vec<u8> {
    let mut rng = Rng::new(1_000 + index);
    let mut value = Vec::with_capacity(32);
    value.extend_from_slice(&(1_700_000_000_000 + index * 250).to_be_bytes());
    value.extend_from_slice(&((index % 1_000) as u32).to_be_bytes());
    value.extend_from_slice(&[rng.below(4) as u8, 0]);
    let extra = rng.below(19);
    value.extend((0..extra).map(|_| rng.below(256) as u8));
    value
}

/// A JSON profile of about 150 bytes to 1 KiB.
fn json(index: u64) -> Vec<u8> {
    let mut rng = Rng::new(2_000 + index);
    let (score, bio) = (rng.below(100_000), rng.below(120));
    format!(
        r#"{{"id":{index},"name":"user {index}","active":true,"score":{score},"plan":"pro","bio":"{}"}}"#,
        words(&mut rng, bio)
    )
    .into_bytes()
}

/// An order document of about 1 to 6 KiB.
fn document(index: u64) -> Vec<u8> {
    let mut rng = Rng::new(3_000 + index);
    let items = 10 + rng.below(51);
    let lines: Vec<String> = (0..items)
        .map(|_| {
            let (sku, qty, note) = (rng.below(50_000), 1 + rng.below(9), rng.below(6));
            format!(
                r#"{{"sku":"SKU-{sku:06}","qty":{qty},"note":"{}"}}"#,
                words(&mut rng, note)
            )
        })
        .collect();
    format!(
        r#"{{"order":{index},"status":"paid","items":[{}]}}"#,
        lines.join(",")
    )
    .into_bytes()
}

fn median(mut samples: Vec<f64>) -> f64 {
    samples.sort_by(f64::total_cmp);
    samples[samples.len() / 2]
}

#[test]
#[ignore = "a measurement: run in release with --ignored --nocapture"]
fn codec_size_and_read_costs() {
    type Payload = fn(u64) -> Vec<u8>;
    let payloads: [(&str, u64, Payload); 3] = [
        ("sensor readings", 600_000, small),
        ("JSON profiles", 50_000, json),
        ("order documents", 6_000, document),
    ];
    println!(
        "\n| payload | codec | logical MiB | run MiB | index KiB | blocks | logical / run | build ms | get ns | blocks/get | scan ns/record |"
    );
    println!("|{}", "---|".repeat(11));
    for (name, count, value) in payloads {
        let records: Vec<(Vec<u8>, Vec<u8>)> = (0..count)
            .map(|index| (format!("k/{index:08}").into_bytes(), value(index)))
            .collect();
        let logical: usize = records.iter().map(|(k, v)| k.len() + v.len()).sum();
        let mut rng = Rng::new(9);
        let probes: Vec<usize> = (0..20_000).map(|_| rng.below(count) as usize).collect();
        for codec in [Codec::None, Codec::Lz4, Codec::Zstd] {
            let start = Instant::now();
            let mut builder = RunBuilder::new(codec);
            for (key, value) in &records {
                builder.add(key, 1, Value::Bytes(value)).unwrap();
            }
            let run: Run<Vec<u8>> = builder.finish();
            let build_ms = start.elapsed().as_secs_f64() * 1e3;

            let mut workspace = ReadWorkspace::new();
            let mut gets = Vec::new();
            let mut blocks = 0;
            for round in 0..4 {
                let before = workspace.blocks_loaded();
                let start = Instant::now();
                for &at in &probes {
                    let found = run.get(&records[at].0, &mut workspace).unwrap().unwrap();
                    black_box(found.value);
                }
                let ns = start.elapsed().as_nanos() as f64 / probes.len() as f64;
                // The first round warms up.
                if round > 0 {
                    gets.push(ns);
                    blocks = workspace.blocks_loaded() - before;
                }
            }
            let scans = (0..3)
                .map(|_| {
                    let start = Instant::now();
                    let mut cursor = run.cursor(&mut workspace);
                    let mut seen = 0usize;
                    while let Some(record) = cursor.next_record().unwrap() {
                        black_box(record.value);
                        seen += 1;
                    }
                    assert_eq!(seen, records.len());
                    start.elapsed().as_nanos() as f64 / seen as f64
                })
                .collect();
            let stored = run.as_bytes().len();
            println!(
                "| {name} | {codec:?} | {:.1} | {:.1} | {:.0} | {} | {:.2} | {build_ms:.0} | {:.0} | {:.2} | {:.1} |",
                logical as f64 / f64::from(1 << 20),
                stored as f64 / f64::from(1 << 20),
                run.view().index_len() as f64 / 1024.0,
                run.block_count(),
                logical as f64 / stored as f64,
                median(gets),
                blocks as f64 / probes.len() as f64,
                median(scans),
            );
        }
    }
}

/// A least-recently-used set of decoded blocks, counted in bytes.
struct Lru {
    budget: usize,
    block_bytes: usize,
    clock: u64,
    stamps: HashMap<usize, u64>,
    order: BTreeMap<u64, usize>,
    hits: u64,
    misses: u64,
}

impl Lru {
    fn new(budget: usize, block_bytes: usize) -> Self {
        Self {
            budget,
            block_bytes,
            clock: 0,
            stamps: HashMap::new(),
            order: BTreeMap::new(),
            hits: 0,
            misses: 0,
        }
    }

    fn touch(&mut self, block: usize) {
        self.clock += 1;
        if let Some(stamp) = self.stamps.insert(block, self.clock) {
            self.order.remove(&stamp);
            self.hits += 1;
        } else {
            self.misses += 1;
            if self.stamps.len() * self.block_bytes > self.budget {
                let (_, evicted) = self.order.pop_first().unwrap();
                self.stamps.remove(&evicted);
            }
        }
        self.order.insert(self.clock, block);
    }
}

/// Zipf(1) ranks over `count` items, drawn by inverting the cumulative sum.
fn zipf(count: usize) -> impl FnMut(&mut Rng) -> usize {
    let mut total = 0.0;
    let cumulative: Vec<f64> = (1..=count)
        .map(|rank| {
            total += 1.0 / rank as f64;
            total
        })
        .collect();
    move |rng: &mut Rng| {
        let target = rng.below(1 << 52) as f64 / (1u64 << 52) as f64 * total;
        cumulative
            .partition_point(|&sum| sum < target)
            .min(count - 1)
    }
}

/// How often a decoded-block cache of a given size would have spared a
/// decode: an LRU simulation over the blocks that reads touch, without a
/// cache in the engine. Each cached block is charged the run's largest
/// decoded block plus a small entry overhead.
#[test]
#[ignore = "a measurement: run in release with --ignored --nocapture"]
fn block_cache_hit_rates() {
    const OPS: usize = 200_000;
    const ENTRY_OVERHEAD: usize = 64;
    type Payload = fn(u64) -> Vec<u8>;
    let payloads: [(&str, u64, Payload); 2] = [
        ("sensor readings", 600_000, small),
        ("JSON profiles", 50_000, json),
    ];
    println!(
        "\n| payload | trace | blocks/op | 1 MiB | 4 MiB | 16 MiB | 64 MiB | decoded run MiB |"
    );
    println!("|{}", "---|".repeat(8));
    for (name, count, value) in payloads {
        let keys: Vec<Vec<u8>> = (0..count)
            .map(|index| format!("k/{index:08}").into_bytes())
            .collect();
        let mut builder = RunBuilder::new(Codec::Zstd);
        for (index, key) in keys.iter().enumerate() {
            builder
                .add(key, 1, Value::Bytes(&value(index as u64)))
                .unwrap();
        }
        let run: Run<Vec<u8>> = builder.finish();
        let view = run.view();
        let block_bytes = view.largest_block_len().unwrap() + ENTRY_OVERHEAD;
        let decoded = run.block_count() * block_bytes;
        let block = |key: &[u8]| view.locate(key).unwrap().unwrap();
        let count = count as usize;

        // Popularity follows key order within the hot window, and a random
        // permutation of keys under Zipf, so popular keys share few blocks
        // only by chance.
        let mut rng = Rng::new(11);
        let mut permutation: Vec<usize> = (0..count).collect();
        for at in (1..count).rev() {
            permutation.swap(at, rng.below(at as u64 + 1) as usize);
        }
        let mut rank = zipf(count);
        let window = count / 100;
        let traces: [(&str, Vec<Vec<usize>>); 4] = [
            (
                "uniform gets",
                (0..OPS)
                    .map(|_| vec![block(&keys[rng.below(count as u64) as usize])])
                    .collect(),
            ),
            (
                "90% of gets in 1% of keys",
                (0..OPS)
                    .map(|_| {
                        let at = if rng.below(10) < 9 {
                            count / 2 + rng.below(window as u64) as usize
                        } else {
                            rng.below(count as u64) as usize
                        };
                        vec![block(&keys[at])]
                    })
                    .collect(),
            ),
            (
                "Zipf gets, scattered",
                (0..OPS)
                    .map(|_| vec![block(&keys[permutation[rank(&mut rng)]])])
                    .collect(),
            ),
            (
                "Zipf 32-key scans",
                (0..OPS)
                    .map(|_| {
                        let first = permutation[rank(&mut rng)].min(count - 32);
                        (block(&keys[first])..=block(&keys[first + 31])).collect()
                    })
                    .collect(),
            ),
        ];
        for (trace_name, trace) in traces {
            let touches: usize = trace.iter().map(Vec::len).sum();
            let rates: Vec<String> = [1usize, 4, 16, 64]
                .into_iter()
                .map(|mib| {
                    let mut lru = Lru::new(mib << 20, block_bytes);
                    for blocks in &trace {
                        for &at in blocks {
                            lru.touch(at);
                        }
                    }
                    format!(
                        "{:.0}%",
                        100.0 * lru.hits as f64 / (lru.hits + lru.misses) as f64
                    )
                })
                .collect();
            println!(
                "| {name} | {trace_name} | {:.2} | {} | {:.1} |",
                touches as f64 / OPS as f64,
                rates.join(" | "),
                decoded as f64 / f64::from(1 << 20),
            );
        }
    }
}
