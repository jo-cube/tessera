//! What snapshots keep, checked against the allocator: the bytes dropping
//! every snapshot frees never exceed what the accounting reported as kept
//! only for them, after random histories of writes, snapshots taken at many
//! generations and released out of order, flushes and compactions.
//!
//! The counting allocator sees every thread, so this binary holds one test,
//! with compaction settled before measuring.

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicUsize, Ordering};

#[allow(dead_code)]
#[path = "../src/test_support.rs"]
mod test_support;

use tessera::engine::diagnostics::{self, MergeWorkspace};
use tessera::engine::{Engine, EngineOptions, EngineWorkspace, Snapshot};

fn options() -> EngineOptions {
    EngineOptions::default()
}

struct Counting;

static LIVE: AtomicUsize = AtomicUsize::new(0);

// SAFETY: every call forwards unchanged to the system allocator.
unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        LIVE.fetch_add(layout.size(), Ordering::Relaxed);
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        LIVE.fetch_sub(layout.size(), Ordering::Relaxed);
        unsafe { System.dealloc(ptr, layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        LIVE.fetch_add(new_size, Ordering::Relaxed);
        LIVE.fetch_sub(layout.size(), Ordering::Relaxed);
        unsafe { System.realloc(ptr, layout, new_size) }
    }
}

#[global_allocator]
static ALLOCATOR: Counting = Counting;

/// Heap bytes freed by `operation`.
fn freed_by(operation: impl FnOnce()) -> usize {
    let before = LIVE.load(Ordering::Relaxed);
    operation();
    before.saturating_sub(LIVE.load(Ordering::Relaxed))
}

struct Rng(u64);

impl Rng {
    fn below(&mut self, bound: u64) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0 % bound
    }

    fn chance(&mut self, percent: u64) -> bool {
        self.below(100) < percent
    }
}

fn key(rng: &mut Rng, keys: u64) -> Vec<u8> {
    format!("key{:06}", rng.below(keys)).into_bytes()
}

fn value(rng: &mut Rng) -> Vec<u8> {
    vec![b'v'; rng.below(300) as usize]
}

/// Drops one held snapshot chosen at random, so that releases are out of
/// order.
fn drop_one<T>(rng: &mut Rng, held: &mut Vec<T>) {
    if !held.is_empty() {
        let at = rng.below(held.len() as u64) as usize;
        drop(held.swap_remove(at));
    }
}

/// A snapshot's own small allocations, its source list and its view of the
/// active table, are not charged, so each is allowed this many bytes and a
/// pointer per source.
const OWN: usize = 1 << 10;

/// An engine whose memtable never fills in these tests, so that only the
/// history its snapshots pin is retained.
fn memtable_only() -> Engine {
    let mut options = options();
    options.memtable_budget = 64 << 20;
    Engine::new(options).unwrap()
}

/// The active table's pinned bytes cover what only its snapshots hold.
fn memtable_checkpoints(seed: u64) -> usize {
    let mut rng = Rng(seed);
    let engine = memtable_only();
    let mut held: Vec<Snapshot> = Vec::with_capacity(64);
    let mut checked = 0;
    for step in 0..12_000 {
        let key = key(&mut rng, 1_500);
        if rng.chance(15) {
            engine.delete(&key).unwrap();
        } else {
            engine.put(&key, &value(&mut rng)).unwrap();
        }
        if rng.chance(2) && held.len() < 60 {
            held.push(engine.snapshot());
        }
        if rng.chance(1) {
            drop_one(&mut rng, &mut held);
        }
        if step % 1_500 == 1_499 {
            let stats = engine.stats();
            assert_eq!(stats.runs, 0);
            let (pinned, count) = (stats.active.pinned, held.len());
            assert_eq!(stats.retained_bytes, pinned);
            let freed = freed_by(|| held.clear());
            assert!(
                freed <= pinned + count * (OWN + 8),
                "seed {seed} step {step}: dropping {count} snapshots freed {freed} bytes, \
                 {pinned} were pinned"
            );
            assert_eq!(engine.stats().active.pinned, 0);
            checked += 1;
        }
    }
    checked
}

/// The tight case: one snapshot of a whole table, then every entry replaced
/// once. Dropping it frees the detached nodes and returns the copied partial
/// chunk to the pool. Used arena bytes and chunk capacity are accounted separately.
fn whole_table_replaced() {
    let engine = memtable_only();
    let keys: Vec<Vec<u8>> = (0..20_000u32)
        .map(|i| format!("key{i:06}").into_bytes())
        .collect();
    for key in &keys {
        engine.put(key, b"12345678").unwrap();
    }
    let arena_used = engine.stats().active.arena_bytes;
    let snapshot = engine.snapshot();
    for key in &keys {
        engine.put(key, b"abcdefgh").unwrap();
    }
    let before = engine.stats();
    let pinned = before.active.pinned;
    let freed = freed_by(|| drop(snapshot));
    let after = engine.stats();
    assert_eq!(before.pool_in_use_bytes - after.pool_in_use_bytes, 1 << 20);
    assert_eq!(
        after.pool_reserve_bytes - before.pool_reserve_bytes,
        1 << 20
    );
    assert!(
        freed <= pinned + OWN + 8,
        "freed {freed} bytes, {pinned} were pinned"
    );
    assert!(
        freed * 100 >= (pinned - arena_used) * 95,
        "freed {freed} bytes, {pinned} were pinned"
    );
}

fn engine_options() -> EngineOptions {
    let mut options = options();
    options.memtable_budget = 96 << 10;
    options
}

fn compact_some(engine: &Engine, rng: &mut Rng, workspace: &mut MergeWorkspace) {
    for _ in 0..rng.below(4) {
        if !diagnostics::compact_step(engine, workspace).unwrap() {
            break;
        }
    }
}

/// The engine's retained bytes cover what only its snapshots hold: pinned
/// memtable history and released tables and runs.
fn engine_checkpoints(seed: u64) -> usize {
    let mut rng = Rng(seed);
    let engine = Engine::new(engine_options()).unwrap();
    let mut held: Vec<Snapshot> = Vec::with_capacity(64);
    let mut merge = MergeWorkspace::new();
    let mut workspace = EngineWorkspace::new();
    let mut checked = 0;
    for step in 0..8_000 {
        let key = key(&mut rng, 2_000);
        let flushes = engine.stats().flushes;
        if rng.chance(10) {
            engine.delete(&key).unwrap();
        } else {
            engine.put(&key, &value(&mut rng)).unwrap();
        }
        // The write flushed a full table.
        if engine.stats().flushes != flushes {
            compact_some(&engine, &mut rng, &mut merge);
        }
        if rng.chance(3) && held.len() < 48 {
            held.push(engine.snapshot());
        }
        if rng.chance(2) {
            drop_one(&mut rng, &mut held);
        }
        if rng.chance(1) {
            engine.flush().unwrap();
        }
        if rng.chance(1) {
            compact_some(&engine, &mut rng, &mut merge);
        }
        if step % 1_000 == 999 {
            // Fill the workspace now, so that the checkpoint frees none of it.
            for snapshot in &held {
                let _ = snapshot.get(&key, &mut workspace).unwrap();
            }
            let stats = engine.stats();
            let sources: usize = stats.runs + stats.frozen_tables + 1;
            let allowance = held.len() * (OWN + 8 * sources);
            let freed = freed_by(|| held.clear());
            assert!(
                freed <= stats.retained_bytes + allowance,
                "seed {seed} step {step}: dropping snapshots freed {freed} bytes, \
                 {} were reported retained",
                stats.retained_bytes
            );
            test_support::wait_until(|| {
                let stats = engine.stats();
                (stats.retained_bytes, stats.retained_sources) == (0, 0)
            });
            let after = engine.stats();
            assert_eq!((after.retained_bytes, after.retained_sources), (0, 0));
            checked += 1;
        }
    }
    checked
}

#[test]
fn dropping_snapshots_frees_no_more_than_was_reported_retained() {
    whole_table_replaced();
    let mut checked = 0;
    for seed in 1..=6 {
        checked += memtable_checkpoints(seed * 0x9e37_79b9);
        checked += engine_checkpoints(seed * 0x51ed_270b);
    }
    assert_eq!(checked, 6 * (8 + 8));
}
