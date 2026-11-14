//! What a compaction of 8, 16 and 32 inputs allocates on the Rust heap,
//! against its workspace estimate and output (5AT). Each input has blocks of
//! the largest length the engine builds, 64 KiB, and the output is
//! compressed with a trained dictionary when that saves enough: the worst
//! case the default limits admit.
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
    CompactionKind, CompactionOptions, Compression, DictionaryChoice, Engine, EngineOptions,
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
fn value(seed: u64) -> Vec<u8> {
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

#[test]
fn a_merge_of_up_to_32_inputs_stays_within_its_estimate_and_output() {
    println!(
        "MERGE MEMORY | inputs | input MiB | output MiB (stored + held) | workspace estimate MiB \
         | dictionary | peak Rust heap above the baseline MiB | that minus the output MiB \
         | decompression contexts MiB |"
    );
    for inputs in [8u64, 16, 32] {
        let engine = Engine::new(EngineOptions {
            memtable_budget: 2 << 20,
            target_run_len: 2 << 20,
            target_block_len: 64 << 10,
            compression: Compression::ZstdDictionary,
            background_compaction: None,
            ..EngineOptions::default()
        })
        .unwrap();
        // Disjoint flushes, one run each, which move into level 1; then
        // flushes over the whole key range, at most 11 (level 0 stops at 12),
        // so that one level-0 job takes them all with every level-1 run.
        let keys = 2_400;
        let level0 = (inputs / 2).min(11);
        let level1 = inputs - level0;
        let mut workspace = MergeWorkspace::new();
        let options = CompactionOptions {
            max_inputs: 32,
            max_overlap: 1,
            ..CompactionOptions::default()
        };
        for flush in 0..level1 {
            for index in 0..keys {
                let key = format!("key/{:08}", (flush * keys + index) * 16);
                engine
                    .put(key.as_bytes(), &value(flush * keys + index))
                    .unwrap();
            }
            engine.flush().unwrap();
            while let Some(report) = engine.compact(options, &mut workspace).unwrap() {
                assert_eq!(report.kind, CompactionKind::Move);
            }
        }
        for flush in 0..level0 {
            for index in 0..keys {
                let key = format!("key/{:08}", index * level1 * 16 + flush + 1);
                engine.put(key.as_bytes(), &value(index + flush)).unwrap();
            }
            engine.flush().unwrap();
        }
        let stats = engine.stats();
        assert_eq!(
            (stats.level0_flushes as u64, stats.runs as u64),
            (level0, inputs)
        );
        let baseline = LIVE.load(Ordering::Relaxed);
        PEAK.store(baseline, Ordering::Relaxed);
        let report = engine.compact(options, &mut workspace).unwrap().unwrap();
        let peak = PEAK.load(Ordering::Relaxed) - baseline;
        assert_eq!(report.kind, CompactionKind::Merge);
        assert_eq!(report.inputs as u64, inputs);
        let output = report.output_bytes + report.output_held_bytes;
        let mib = |bytes: usize| bytes as f64 / (1 << 20) as f64;
        println!(
            "MERGE MEMORY | {} | {:.1} | {:.1} | {:.1} | {:?} | {:.1} | {:.1} | {:.1} |",
            report.inputs,
            mib(report.input_bytes + report.input_held_bytes),
            mib(output),
            mib(report.workspace_bytes),
            report.stats.dictionary,
            mib(peak),
            mib(peak.saturating_sub(output)),
            mib(workspace.context_bytes()),
        );
        assert!(
            matches!(
                report.stats.dictionary,
                DictionaryChoice::Used { .. } | DictionaryChoice::NotBeneficial { .. }
            ),
            "{:?}",
            report.stats.dictionary
        );
        // Within the workspace limit, and the Rust heap beside the output
        // (whose buffers may double) within the estimate.
        assert!(report.workspace_bytes <= options.max_workspace_bytes);
        assert!(
            peak <= 2 * output + report.workspace_bytes,
            "peak {peak}, output {output}, estimate {}",
            report.workspace_bytes
        );
        assert!(
            workspace.context_bytes() <= report.inputs * (1 << 20),
            "{}",
            workspace.context_bytes()
        );
        engine.close().unwrap();
    }
}
