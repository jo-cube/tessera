//! What compression costs and saves, per codec, on the payloads of the
//! benchmark's `density` mode. A measurement rather than a test: the engine
//! always uses the default codec, so this is the only place the others can be
//! compared. Run it in release:
//!
//! ```text
//! cargo test --release --lib run::tests::costs -- --ignored --nocapture
//! ```

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
