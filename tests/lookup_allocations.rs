//! Point lookups allocate nothing on the Rust heap once their reader is
//! warm, and a present key decodes exactly one block once runs are
//! consolidated, compressed and uncompressed.
//!
//! Zstandard's own allocations go through `malloc` and are not visible here:
//! a lookup reuses the reader's decompression context, and decoding with
//! loading raw-content dictionaries allocates nothing in Zstandard 1.5.
//!
//! The counting allocator sees every thread, so this binary holds one test,
//! with compaction settled before measuring.

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicUsize, Ordering};

use tessera::diagnostics::{self, EngineDiagnostics, MergeWorkspace, ReaderDiagnostics};
use tessera::{Compression, Engine, Keyspace, Options, Reader};

fn options() -> Options {
    Options::default()
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
fn consolidated(compression: Compression) -> (Engine, Keyspace) {
    let mut options = options();
    options.block_size = 4 << 10;
    options.compression = compression;
    let engine = Engine::in_memory(options).unwrap();
    let ks = engine.keyspace("users").unwrap();
    // A fixed permutation, flushed in four parts whose key ranges overlap.
    for j in 0..RECORDS {
        let index = j.wrapping_mul(7_919) % RECORDS;
        ks.put(&key(index), &value(index)).unwrap();
        if (j + 1) % (RECORDS / 4) == 0 {
            engine.flush().unwrap();
        }
    }
    let mut workspace = MergeWorkspace::new();
    while diagnostics::compact_step(&engine, &mut workspace).unwrap() {}
    engine.wait_for_compaction().unwrap();
    (engine, ks)
}

#[test]
fn warm_lookups_allocate_nothing_and_decode_one_block() {
    for compression in [Compression::Zstd, Compression::Uncompressed] {
        let (engine, ks) = consolidated(compression);
        assert_eq!(engine.engine_stats().max_overlap, 1);
        let lookups = |reader: &mut Reader<'_>, from: u32| {
            for j in from..from + 4_000 {
                let index = j.wrapping_mul(104_729) % RECORDS;
                let found = reader.get(&ks, &key(index)).unwrap();
                assert_eq!(found.map(<[u8]>::len), Some(value(index).len()));
            }
        };
        let snapshot = engine.snapshot();
        for latest in [true, false] {
            let mut reader = if latest {
                engine.reader()
            } else {
                snapshot.reader()
            };
            lookups(&mut reader, 0);

            // `value` allocates for the check, so count only the lookups.
            let (mut allocations, blocks_before) = (0, reader.counters().blocks_loaded);
            for j in 4_000..8_000_u32 {
                let index = j.wrapping_mul(104_729) % RECORDS;
                let (expected, key) = (value(index).len(), key(index));
                engine.wait_for_compaction().unwrap();
                let before = ALLOCATIONS.load(Ordering::Relaxed);
                let found = reader.get(&ks, &key).unwrap().map(<[u8]>::len);
                allocations += ALLOCATIONS.load(Ordering::Relaxed) - before;
                assert_eq!(found, Some(expected));
            }
            assert_eq!(allocations, 0, "{compression:?}, latest {latest}");
            assert_eq!(
                reader.counters().blocks_loaded - blocks_before,
                4_000,
                "{compression:?}, latest {latest}"
            );
            let keys: Vec<_> = (0..100).map(|i| key(i * 31)).collect();
            for _ in 0..2 {
                std::hint::black_box(&*reader.multi_get(&ks, &keys).unwrap());
            }
            engine.wait_for_compaction().unwrap();
            let before = ALLOCATIONS.load(Ordering::Relaxed);
            std::hint::black_box(&*reader.multi_get(&ks, &keys).unwrap());
            assert_eq!(
                ALLOCATIONS.load(Ordering::Relaxed) - before,
                0,
                "warm multi-get, latest {latest}"
            );
        }
    }
}
