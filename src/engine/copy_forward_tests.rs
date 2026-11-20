//! Copying settled blocks forward must not change what any reader sees.
//! Identical randomized histories run with copying on and off, through
//! level-0 → bottom merges, forced pieces, concurrent builders, snapshots,
//! deletes, sorted ingestion and checkpoint recovery, and must read alike
//! and agree with a model.

use super::*;
use crate::test_support::Scratch;
use std::collections::BTreeMap;
use std::sync::atomic::Ordering;

const KEYS: u64 = 3_000;

type Model = BTreeMap<Vec<u8>, Vec<u8>>;

fn key(index: u64) -> Vec<u8> {
    format!("user/{index:06}").into_bytes()
}

/// A 300-byte value that compresses little, so that blocks hold a few
/// records each and most stay untouched by a round of updates.
fn value(index: u64, version: u64) -> Vec<u8> {
    let mut rng =
        crate::test_support::Rng((index << 20 | version).wrapping_mul(0x9e37_79b9_7f4a_7c15) | 1);
    let mut out = format!("{index}:{version}:").into_bytes();
    out.extend((0..300).map(|_| b"abcdefghijklmnopqrstuvwxyz0123456789"[rng.below(36)]));
    out
}

fn options(threads: usize) -> EngineOptions {
    EngineOptions {
        memtable_budget: 64 << 10,
        target_run_len: 32 << 10,
        background_threads: threads,
        background_compaction: None,
        ..EngineOptions::default()
    }
}

/// Every key through every reader of `snapshot`, in one comparable form.
fn observe(snapshot: &Snapshot) -> Vec<(Vec<u8>, Option<Vec<u8>>)> {
    let mut ws = EngineWorkspace::new();
    let keys: Vec<_> = (0..KEYS).map(key).collect();
    let out: Vec<(Vec<u8>, Option<Vec<u8>>)> = keys
        .iter()
        .map(|key| {
            let found = snapshot.get(key, &mut ws).unwrap();
            (key.clone(), found.map(|live| live.value.to_vec()))
        })
        .collect();
    snapshot
        .multi_get(&keys, &mut ws, |at, found| {
            assert_eq!(found.map(<[u8]>::to_vec), out[at].1, "multi_get {at}");
        })
        .unwrap();
    let mut scanned = Vec::new();
    {
        let mut scan = snapshot.scan(&mut ws);
        while let Some(record) = scan.next_record().unwrap() {
            scanned.push((record.key.to_vec(), record.value.to_vec()));
        }
    }
    let live: Vec<_> = out
        .iter()
        .filter_map(|(key, value)| value.clone().map(|value| (key.clone(), value)))
        .collect();
    assert_eq!(scanned, live, "scan agrees with point reads");
    // A prefix scan from the middle exercises seeks into copied blocks.
    let prefix = b"user/0015";
    let mut scan = snapshot.scan_prefix(prefix, &mut ws);
    let mut expected = live.iter().filter(|(key, _)| key.starts_with(prefix));
    while let Some(record) = scan.next_record().unwrap() {
        let (key, value) = expected.next().unwrap();
        assert_eq!(
            (record.key, record.value),
            (key.as_slice(), value.as_slice())
        );
    }
    assert!(expected.next().is_none());
    out
}

fn compact_all(engine: &Engine) {
    let mut ws = MergeWorkspace::new();
    while engine.compact(&mut ws).unwrap().is_some() {}
}

/// Runs one history and returns every observation, each checked against
/// the model, and the blocks copied.
type Observed = Vec<Vec<(Vec<u8>, Option<Vec<u8>>)>>;
fn history(copy: bool, threads: usize, piece_budget: usize, seed: u64) -> (Observed, u64) {
    let scratch = Scratch::new("copy-forward");
    let set_up = |engine: &Engine| {
        engine.core.copy_forward.store(copy, Ordering::Relaxed);
        engine
            .core
            .piece_budget
            .store(piece_budget, Ordering::Relaxed);
    };
    let mut engine = Engine::new(options(threads)).unwrap();
    set_up(&engine);
    let mut rng = crate::test_support::Rng(seed);
    let mut model = Model::new();
    let mut log = Vec::new();
    let mut snapshots: Vec<(Snapshot, Model)> = Vec::new();
    let mut reused = 0;
    let check = |snapshot: &Snapshot, model: &Model| {
        let seen = observe(snapshot);
        for (key, value) in &seen {
            assert_eq!(
                value.as_ref(),
                model.get(key),
                "{}",
                String::from_utf8_lossy(key)
            );
        }
        seen
    };
    for version in 0..14u64 {
        let writes = if version == 0 { KEYS as usize } else { 250 };
        for at in 0..writes {
            let index = if version == 0 {
                // A shuffled load, so that level 0 merges into the bottom.
                (at as u64).wrapping_mul(1_237) % KEYS
            } else {
                rng.below(KEYS as usize) as u64
            };
            if version > 0 && rng.below(6) == 0 {
                engine.delete(&key(index)).unwrap();
                model.remove(&key(index));
            } else {
                engine.put(&key(index), &value(index, version)).unwrap();
                model.insert(key(index), value(index, version));
            }
        }
        if version % 4 == 2 {
            // Sorted ingestion publishes runs whose sequences are assigned
            // by override, which are never copied forward.
            let mut batch = WriteBatch::new();
            let start = rng.below(KEYS as usize - 100) as u64;
            for index in start..start + 60 {
                if index % 7 == 0 {
                    batch.delete(&key(index));
                    model.remove(&key(index));
                } else {
                    batch.put(&key(index), &value(index, version + 100));
                    model.insert(key(index), value(index, version + 100));
                }
            }
            engine.ingest_sorted(&batch).unwrap();
        }
        engine.flush().unwrap();
        compact_all(&engine);
        if version % 3 == 1 {
            snapshots.push((engine.snapshot(), model.clone()));
        }
        if version % 5 == 3 {
            reused += engine.stats().build.blocks_reused;
            drop(std::mem::take(&mut snapshots));
            let dir = scratch.0.join(format!("v{version}"));
            engine
                .checkpoint(&dir, CheckpointOptions::default())
                .unwrap();
            engine.close().unwrap();
            engine = Engine::recover(&dir, options(threads)).unwrap();
            set_up(&engine);
        }
        log.push(check(&engine.snapshot(), &model));
        for (snapshot, seen) in &snapshots {
            log.push(check(snapshot, seen));
        }
    }
    reused += engine.stats().build.blocks_reused;
    engine.close().unwrap();
    (log, reused)
}

#[test]
fn copying_forward_reads_like_rewriting() {
    for (threads, piece_budget, seed) in [(1, 0, 3), (1, 6 << 10, 5), (4, 0, 7), (4, 6 << 10, 11)] {
        let (off, rewritten) = history(false, threads, piece_budget, seed);
        let (on, copied) = history(true, threads, piece_budget, seed);
        assert_eq!(on, off);
        assert_eq!(rewritten, 0);
        assert!(
            copied > 0,
            "{threads} threads, piece budget {piece_budget}: nothing copied"
        );
    }
}

/// Runs this small give concurrent pieces whose bottom group, at its 128 KiB
/// floor, exceeds an allowance of four estimated runs. Default budgets must
/// build them, copying or not.
#[test]
fn tiny_runs_fit_the_default_concurrent_allowance() {
    for copy in [false, true] {
        history(copy, 4, 0, 7);
    }
}
