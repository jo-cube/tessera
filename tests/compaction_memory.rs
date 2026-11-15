//! What one compaction of up to 32 inputs allocates on the Rust heap and in
//! native decoder contexts, against its workspace estimate and output,
//! under each codec, compressible and incompressible values, records much
//! larger than a block, and inputs of mixed codecs (5AT, 5AU). Blocks are
//! adaptive, with the 256 KiB raw cap where records allow.
//!
//! Zstandard's contexts are allocated through `malloc`, not the Rust
//! allocator: they are reported by the workspace's `context_bytes` (the
//! decompression contexts, kept across merges) and bounded per context by
//! the estimate's allowance (a test in `src/run/tests/compact.rs`). The
//! output's compression context lives in the builder and is freed with it.
//!
//! The counting allocator sees every thread, so this binary holds one test,
//! and the engine runs no worker.

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicUsize, Ordering};

use tessera::engine::{
    CheckpointOptions, CompactionKind, CompactionOptions, Compression, Engine, EngineOptions,
    MergeWorkspace,
};

struct Counting;

static LIVE: AtomicUsize = AtomicUsize::new(0);
static PEAK: AtomicUsize = AtomicUsize::new(0);

fn grow(bytes: usize) {
    let live = LIVE.fetch_add(bytes, Ordering::Relaxed) + bytes;
    PEAK.fetch_max(live, Ordering::Relaxed);
}

// SAFETY: every call forwards unchanged to the system allocator.
unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        grow(layout.size());
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        LIVE.fetch_sub(layout.size(), Ordering::Relaxed);
        unsafe { System.dealloc(ptr, layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        if new_size > layout.size() {
            grow(new_size - layout.size());
        } else {
            LIVE.fetch_sub(layout.size() - new_size, Ordering::Relaxed);
        }
        unsafe { System.realloc(ptr, layout, new_size) }
    }
}

#[global_allocator]
static ALLOCATOR: Counting = Counting;

/// Text-like values of about 600 bytes from a small vocabulary, with a
/// varying number, so that blocks compress and a dictionary can help.
fn text(seed: u64) -> Vec<u8> {
    const WORDS: [&str; 12] = [
        "alpha", "bravo", "charlie", "delta", "echo", "foxtrot", "golf", "hotel", "india",
        "juliet", "kilo", "lima",
    ];
    let mut state = seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1;
    let mut out = format!("{{\"id\":{seed},\"text\":\"");
    while out.len() < 600 {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        out.push_str(WORDS[(state % 12) as usize]);
        out.push(' ');
    }
    out.push_str("\"}");
    out.into_bytes()
}

/// `len` pseudo-random bytes, which no codec shrinks.
fn noise(seed: u64, len: usize) -> Vec<u8> {
    let mut state = seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1;
    (0..len)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state as u8
        })
        .collect()
}

struct Case {
    name: &'static str,
    compression: Compression,
    /// Recover the level-1 runs from a checkpoint under this setting
    /// before level 0 is flushed, so that the job mixes codecs.
    recovered_as: Option<Compression>,
    inputs: u64,
    /// Records per flush, about 1.4 MiB of input.
    keys: u64,
    value: fn(u64) -> Vec<u8>,
}

fn engine_options(compression: Compression) -> EngineOptions {
    EngineOptions {
        memtable_budget: 2 << 20,
        target_run_len: 2 << 20,
        target_block_compressed: 64 << 10,
        compression,
        background_compaction: None,
        ..EngineOptions::default()
    }
}

/// Builds level 1 from disjoint flushes (moves) and level 0 from at most 11
/// small flushes over the whole key range, then measures the one merge of
/// them all and prints its row.
fn measure(case: &Case) {
    let mut engine = Engine::new(engine_options(case.compression)).unwrap();
    let level0 = (case.inputs / 2).min(11);
    let level1 = case.inputs - level0;
    let keys = case.keys;
    let mut workspace = MergeWorkspace::new();
    let options = CompactionOptions {
        max_overlap: 1,
        ..CompactionOptions::default()
    };
    for flush in 0..level1 {
        for index in 0..keys {
            let key = format!("key/{:08}", (flush * keys + index) * 16);
            engine
                .put(key.as_bytes(), &(case.value)(flush * keys + index))
                .unwrap();
        }
        engine.flush().unwrap();
        while let Some(report) = engine.compact(options, &mut workspace).unwrap() {
            assert_eq!(report.kind, CompactionKind::Move, "{}", case.name);
        }
    }
    if let Some(later) = case.recovered_as {
        let dir = std::env::temp_dir().join(format!(
            "tessera-compaction-memory-{}-{}",
            std::process::id(),
            case.name.replace(' ', "-")
        ));
        let _ = std::fs::remove_dir_all(&dir);
        engine
            .checkpoint(&dir, CheckpointOptions::default())
            .unwrap();
        engine.close().unwrap();
        engine = Engine::recover(&dir, engine_options(later)).unwrap();
        std::fs::remove_dir_all(&dir).unwrap();
    }
    // Level-0 flushes under two eighths of a run length of input are not
    // split at level 1's boundaries, so that each spans the whole range and
    // one job takes every run: the largest merge the limits admit.
    // With records much larger than that, two small ones at the ends.
    let (level0_keys, level0_value): (u64, fn(u64) -> Vec<u8>) = if keys < 100 {
        (2, text)
    } else {
        (100, case.value)
    };
    let stride = level1 * keys * 16 / (level0_keys - 1);
    for flush in 0..level0 {
        for index in 0..level0_keys {
            let key = format!("key/{:08}", index * stride + flush + 1);
            engine
                .put(key.as_bytes(), &level0_value(index + flush))
                .unwrap();
        }
        engine.flush().unwrap();
    }
    let runs = engine.stats().runs;
    let baseline = LIVE.load(Ordering::Relaxed);
    PEAK.store(baseline, Ordering::Relaxed);
    let report = engine.compact(options, &mut workspace).unwrap().unwrap();
    let peak = PEAK.load(Ordering::Relaxed) - baseline;
    assert_eq!(report.kind, CompactionKind::Merge, "{}", case.name);
    assert_eq!(report.inputs, runs, "{}: one job of every run", case.name);
    let output = report.output_bytes + report.output_held_bytes;
    let mib = |bytes: usize| bytes as f64 / (1 << 20) as f64;
    println!(
        "MERGE MEMORY | {} | {} | {:.1} | {:.1} | {:.1} | {:.1} | {:.1} | {:.1} |",
        case.name,
        report.inputs,
        mib(report.input_bytes + report.input_held_bytes),
        mib(output),
        mib(report.workspace_bytes),
        mib(peak),
        mib(peak.saturating_sub(output)),
        mib(workspace.context_bytes()),
    );
    // Within the workspace limit, and the Rust heap beside the output
    // (whose buffers may double) within the estimate.
    assert!(report.workspace_bytes <= options.max_workspace_bytes);
    assert!(
        peak <= 2 * output + report.workspace_bytes,
        "{}: peak {peak}, output {output}, estimate {}",
        case.name,
        report.workspace_bytes
    );
    // Native decompression contexts, outside the Rust heap: within the
    // estimate's allowance of 1 MiB per input.
    assert!(
        workspace.context_bytes() <= report.inputs * (1 << 20),
        "{}: {}",
        case.name,
        workspace.context_bytes()
    );
    engine.close().unwrap();
}

#[test]
fn merges_stay_within_their_estimate_and_output() {
    println!(
        "MERGE MEMORY | case | inputs | input MiB | output MiB (stored + held) \
         | workspace estimate MiB | peak Rust heap above the baseline MiB \
         | that minus the output MiB | native decompression contexts MiB |"
    );
    let large = |seed: u64| noise(seed, 512 << 10);
    let cases = [
        (
            "zstd, text",
            Compression::Zstd,
            None,
            8,
            2_400,
            text as fn(u64) -> Vec<u8>,
        ),
        ("zstd, text", Compression::Zstd, None, 16, 2_400, text),
        ("zstd, text", Compression::Zstd, None, 32, 2_400, text),
        (
            "uncompressed, text",
            Compression::Uncompressed,
            None,
            32,
            2_400,
            text,
        ),
        (
            "zstd, incompressible",
            Compression::Zstd,
            None,
            32,
            2_400,
            |seed| noise(seed, 600),
        ),
        (
            "uncompressed, incompressible",
            Compression::Uncompressed,
            None,
            32,
            2_400,
            |seed| noise(seed, 600),
        ),
        (
            "zstd, 512 KiB records",
            Compression::Zstd,
            None,
            16,
            3,
            large,
        ),
        (
            "zstd runs recovered as uncompressed",
            Compression::Zstd,
            Some(Compression::Uncompressed),
            16,
            2_400,
            text,
        ),
        (
            "uncompressed runs recovered as zstd",
            Compression::Uncompressed,
            Some(Compression::Zstd),
            16,
            2_400,
            text,
        ),
    ];
    for (name, compression, recovered_as, inputs, keys, value) in cases {
        measure(&Case {
            name,
            compression,
            recovered_as,
            inputs,
            keys,
            value,
        });
    }
}
