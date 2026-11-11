//! What snapshots keep, checked against the allocator: the bytes dropping
//! every snapshot frees never exceed what the accounting reported as kept
//! only for them, after random histories of writes, snapshots taken at many
//! generations and released out of order, flushes and compactions.
//!
//! The counting allocator sees every thread, so this binary holds one test,
//! and the engine runs no worker.

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicUsize, Ordering};

use tessera::engine::{
    CompactionOptions, Engine, EngineOptions, EngineWorkspace, Snapshot, SnapshotLimits,
    UpdateError,
};
use tessera::memtable::{FrozenMemtable, Memtable, WriteError};
use tessera::run::MergeWorkspace;

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

/// A table's pinned bytes cover what only its snapshots hold. Each snapshot
/// is a `FrozenMemtable` held inline, whose own allocation is its pin.
fn memtable_checkpoints(seed: u64) -> usize {
    const PIN: usize = 16;
    let mut rng = Rng(seed);
    let mut table = Memtable::new(64 << 20, None);
    let mut held: Vec<FrozenMemtable> = Vec::with_capacity(64);
    let mut seq = 0;
    let mut checked = 0;
    for step in 0..12_000 {
        seq += 1;
        let key = key(&mut rng, 1_500);
        if rng.chance(15) {
            table.delete(&key, seq).unwrap();
        } else {
            table.put(&key, seq, &value(&mut rng)).unwrap();
        }
        if rng.chance(2) && held.len() < 60 {
            held.push(table.snapshot());
        }
        if rng.chance(1) {
            drop_one(&mut rng, &mut held);
        }
        if step % 1_500 == 1_499 {
            let pinned = table.memory().pinned;
            let count = held.len();
            let freed = freed_by(|| held.clear());
            assert!(
                freed <= pinned + count * PIN,
                "seed {seed} step {step}: dropping {count} snapshots freed {freed} bytes, \
                 {pinned} were pinned"
            );
            assert_eq!(table.memory().pinned, 0);
            checked += 1;
        }
    }
    checked
}

fn engine_options() -> EngineOptions {
    EngineOptions {
        memtable_budget: 96 << 10,
        target_run_len: 32 << 10,
        max_runs: 1 << 12,
        max_run_bytes: usize::MAX,
        snapshots: SnapshotLimits {
            max_live: 64,
            max_retained_bytes: usize::MAX,
        },
        ..EngineOptions::default()
    }
}

fn compact_some(engine: &Engine, rng: &mut Rng, workspace: &mut MergeWorkspace) {
    let options = CompactionOptions {
        max_overlap: 1 + rng.below(3) as usize,
        ..CompactionOptions::default()
    };
    for _ in 0..rng.below(4) {
        if engine.compact(options, workspace).unwrap().is_none() {
            break;
        }
    }
}

/// The engine's retained bytes cover what only its snapshots hold: pinned
/// memtable history and released tables and runs. A snapshot's own small
/// allocations, its source lists and its view of the active table, are not
/// charged, so each is allowed `OWN` bytes and a pointer per source.
fn engine_checkpoints(seed: u64) -> usize {
    const OWN: usize = 1 << 10;
    let mut rng = Rng(seed);
    let engine = Engine::new(engine_options());
    let mut held: Vec<Snapshot> = Vec::with_capacity(64);
    let mut merge = MergeWorkspace::new();
    let mut workspace = EngineWorkspace::new();
    let mut checked = 0;
    for step in 0..8_000 {
        let key = key(&mut rng, 2_000);
        let written = if rng.chance(10) {
            engine.delete(&key)
        } else {
            engine.put(&key, &value(&mut rng))
        };
        match written {
            Ok(_) => {}
            Err(UpdateError::Rejected(WriteError::BudgetExceeded { .. })) => {
                engine.flush().unwrap();
                compact_some(&engine, &mut rng, &mut merge);
            }
            Err(error) => panic!("{error}"),
        }
        if rng.chance(3) && held.len() < 48 {
            held.push(engine.snapshot().unwrap());
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
            let after = engine.stats();
            assert_eq!((after.retained_bytes, after.retained_sources), (0, 0));
            checked += 1;
        }
    }
    checked
}

#[test]
fn dropping_snapshots_frees_no_more_than_was_reported_retained() {
    let mut checked = 0;
    for seed in 1..=6 {
        checked += memtable_checkpoints(seed * 0x9e37_79b9);
        checked += engine_checkpoints(seed * 0x51ed_270b);
    }
    assert_eq!(checked, 6 * (8 + 8));
}
