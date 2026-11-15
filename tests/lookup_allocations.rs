//! Point lookups allocate nothing on the Rust heap once their workspace is
//! warm, and a present key decodes exactly one block once runs are
//! consolidated, with and without dictionaries.
//!
//! Zstandard's own allocations go through `malloc` and are not visible here:
//! a lookup reuses the workspace's decompression context, and decoding with
//! loading raw-content dictionaries allocates nothing in Zstandard 1.5.
//!
//! The counting allocator sees every thread, so this binary holds one test,
//! and the engine runs no worker.

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicUsize, Ordering};

use tessera::engine::{
    CompactionOptions, Compression, Engine, EngineOptions, EngineWorkspace, MergeWorkspace,
};

/// The defaults without the background worker: these tests flush and
/// compact by hand and count what each step leaves.
fn manual() -> EngineOptions {
    EngineOptions {
        background_compaction: None,
        ..EngineOptions::default()
    }
}

struct Counting;

static ALLOCATIONS: AtomicUsize = AtomicUsize::new(0);

// SAFETY: every call forwards unchanged to the system allocator.
unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        ALLOCATIONS.fetch_add(1, Ordering::Relaxed);
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        ALLOCATIONS.fetch_add(1, Ordering::Relaxed);
        unsafe { System.realloc(ptr, layout, new_size) }
    }
}

#[global_allocator]
static ALLOCATOR: Counting = Counting;

const RECORDS: u32 = 24_000;

fn key(index: u32) -> [u8; 8] {
    (u64::from(index) * 2 + 1).to_be_bytes()
}

/// JSON-like values of about 450 bytes, which a dictionary compresses well.
fn value(index: u32) -> Vec<u8> {
    const WORDS: [&str; 16] = [
        "alpha", "bravo", "charlie", "delta", "echo", "foxtrot", "golf", "hotel", "india",
        "juliet", "kilo", "lima", "mike", "november", "oscar", "papa",
    ];
    let mut state = (u64::from(index) + 1).wrapping_mul(0x9E37_79B9_7F4A_7C15);
    let mut next = |bound: u64| {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        state % bound
    };
    let bio: Vec<&str> = (0..20 + next(100))
        .map(|_| WORDS[next(16) as usize])
        .collect();
    format!(
        r#"{{"id":{index},"name":"{} {}","active":{},"score":{},"plan":"{}","created":"2026-{:02}-{:02}","bio":"{}"}}"#,
        WORDS[next(16) as usize],
        WORDS[next(16) as usize],
        next(2) == 0,
        next(100_000),
        ["free", "pro", "team"][next(3) as usize],
        1 + next(12),
        1 + next(28),
        bio.join(" "),
    )
    .into_bytes()
}

/// An engine of overlapping flushed runs consolidated by compaction, and the
/// resulting engine after compactions.
fn consolidated(compression: Compression) -> Engine {
    let engine = Engine::new(EngineOptions {
        target_block_compressed: 4 << 10,
        compression,
        max_runs: usize::MAX,
        max_run_bytes: usize::MAX,
        ..manual()
    })
    .unwrap();
    // A fixed permutation, flushed in four parts whose key ranges overlap.
    for j in 0..RECORDS {
        let index = j.wrapping_mul(7_919) % RECORDS;
        engine.put(&key(index), &value(index)).unwrap();
        if (j + 1) % (RECORDS / 4) == 0 {
            engine.flush().unwrap();
        }
    }
    let options = CompactionOptions {
        max_overlap: 1,
        max_inputs: usize::MAX,
        max_input_bytes: usize::MAX,
        max_output_bytes: usize::MAX,
        ..CompactionOptions::default()
    };
    let mut workspace = MergeWorkspace::new();
    while engine.compact(options, &mut workspace).unwrap().is_some() {}
    engine
}

#[test]
fn warm_lookups_allocate_nothing_and_decode_one_block() {
    for compression in [Compression::Zstd, Compression::Zstd] {
        let engine = consolidated(compression);
        assert_eq!(engine.stats().max_overlap, 1);
        let lookups = |workspace: &mut EngineWorkspace, from: u32| {
            for j in from..from + 4_000 {
                let index = j.wrapping_mul(104_729) % RECORDS;
                let found = engine.get(&key(index), workspace).unwrap();
                assert_eq!(
                    found.map(|found| found.value.len()),
                    Some(value(index).len())
                );
            }
        };
        let mut workspace = EngineWorkspace::new();
        lookups(&mut workspace, 0);

        // `value` allocates for the check, so count only the lookups.
        let (mut allocations, blocks_before) = (0, workspace.blocks_loaded());
        for j in 4_000..8_000_u32 {
            let index = j.wrapping_mul(104_729) % RECORDS;
            let (expected, key) = (value(index).len(), key(index));
            let before = ALLOCATIONS.load(Ordering::Relaxed);
            let found = engine.get(&key, &mut workspace).unwrap();
            allocations += ALLOCATIONS.load(Ordering::Relaxed) - before;
            assert_eq!(found.map(|found| found.value.len()), Some(expected));
        }
        assert_eq!(allocations, 0, "{compression:?}");
        assert_eq!(
            workspace.blocks_loaded() - blocks_before,
            4_000,
            "{compression:?}"
        );
    }
}
