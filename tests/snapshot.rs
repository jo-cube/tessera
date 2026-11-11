//! Snapshots: views that hold no lock, what they keep alive, and the limits on
//! taking them.
//!
//! Merging, flushing and compaction have their own tests and are not repeated.
//! The compaction model test also retains snapshots across random histories,
//! and the memtable tests check the copy-on-write tree on its own.
//! Concurrency tests use barriers and joins and never sleep.

use std::sync::Barrier;
use std::thread;

use tessera::engine::{
    CompactionError, CompactionOptions, Engine, EngineOptions, EngineWorkspace, Snapshot,
    SnapshotError, SnapshotLimits, UpdateError,
};
use tessera::memtable::WriteError;
use tessera::run::MergeWorkspace;

type Pairs = Vec<(Vec<u8>, Vec<u8>)>;

fn pairs(items: &[(&[u8], &[u8])]) -> Pairs {
    items
        .iter()
        .map(|&(k, v)| (k.to_vec(), v.to_vec()))
        .collect()
}

/// Every live record under `prefix`, checking that none is newer than the
/// snapshot.
fn scanned(snapshot: &Snapshot, prefix: &[u8]) -> Pairs {
    let mut workspace = EngineWorkspace::new();
    let mut scan = snapshot.scan_prefix(prefix, &mut workspace);
    let mut out = Vec::new();
    while let Some(record) = scan.next_record().unwrap() {
        assert!(record.seq <= snapshot.last_seq());
        out.push((record.key.to_vec(), record.value.to_vec()));
    }
    out
}

fn value(snapshot: &Snapshot, key: &[u8]) -> Option<Vec<u8>> {
    let mut workspace = EngineWorkspace::new();
    let found = snapshot.get(key, &mut workspace).unwrap();
    found.map(|found| found.value.to_vec())
}

fn merge_all() -> CompactionOptions {
    CompactionOptions {
        max_overlap: 1,
        ..CompactionOptions::default()
    }
}

/// Compacts until no key is in more than one run, returning how many
/// compactions that took.
fn compact_all(engine: &Engine) -> usize {
    let mut workspace = MergeWorkspace::new();
    let mut done = 0;
    while engine
        .compact(merge_all(), &mut workspace)
        .unwrap()
        .is_some()
    {
        done += 1;
    }
    done
}

#[test]
fn a_snapshot_keeps_its_state_through_writes_flushes_and_compactions() {
    let engine = Engine::new(EngineOptions::default());
    for key in [&b"a"[..], b"b", b"c", b"\xff\x00"] {
        engine.put(key, b"1").unwrap();
    }
    engine.flush().unwrap();
    engine.put(b"a", b"2").unwrap();
    // A tombstone in a table over a value in a run.
    engine.delete(b"b").unwrap();
    let first = engine.snapshot().unwrap();
    // Nothing was frozen, flushed or copied.
    let stats = engine.stats();
    assert_eq!((stats.frozen_tables, stats.runs), (0, 1));
    assert_eq!((stats.retained_sources, stats.retained_bytes), (0, 0));
    let then = pairs(&[(b"a", b"2"), (b"c", b"1"), (b"\xff\x00", b"1")]);

    engine.put(b"a", b"3").unwrap();
    engine.put(b"b", b"2").unwrap();
    engine.delete(b"c").unwrap();
    engine.put(b"ab", b"1").unwrap();
    let second = engine.snapshot().unwrap();
    let later = pairs(&[
        (b"a", b"3"),
        (b"ab", b"1"),
        (b"b", b"2"),
        (b"\xff\x00", b"1"),
    ]);
    assert!(second.last_seq() > first.last_seq());

    engine.delete(b"b").unwrap();
    engine.flush().unwrap();
    engine.put(b"c", b"4").unwrap();
    engine.flush().unwrap();
    // Merging every run drops the tombstones both snapshots depend on.
    assert!(compact_all(&engine) > 0);
    assert_eq!(engine.stats().runs, 1);

    for (snapshot, expected) in [(&first, &then), (&second, &later)] {
        assert_eq!(&scanned(snapshot, b""), expected);
        for (key, value_then) in expected {
            assert_eq!(value(snapshot, key).as_ref(), Some(value_then));
        }
    }
    assert_eq!(value(&first, b"b"), None);
    assert_eq!(value(&second, b"c"), None);
    assert_eq!(scanned(&first, b"a"), pairs(&[(b"a", b"2")]));
    assert_eq!(
        scanned(&second, b"a"),
        pairs(&[(b"a", b"3"), (b"ab", b"1")])
    );
    assert_eq!(scanned(&second, b"\xff"), pairs(&[(b"\xff\x00", b"1")]));

    let now = engine.snapshot().unwrap();
    assert_eq!(
        scanned(&now, b""),
        pairs(&[
            (b"a", b"3"),
            (b"ab", b"1"),
            (b"c", b"4"),
            (b"\xff\x00", b"1")
        ])
    );
}

#[test]
fn a_long_scan_lets_writes_flushes_and_compactions_publish() {
    const KEYS: usize = 200;
    let key = |i: usize| format!("k{i:03}").into_bytes();
    let engine = Engine::new(EngineOptions::default());
    for round in [b"0", b"1"] {
        for i in 0..KEYS {
            engine.put(&key(i), round).unwrap();
        }
        engine.flush().unwrap();
    }
    engine.put(&key(0), b"2").unwrap();

    let snapshot = engine.snapshot().unwrap();
    let mut workspace = EngineWorkspace::new();
    let mut scan = snapshot.scan(&mut workspace);
    assert_eq!(scan.next_record().unwrap().unwrap().value, b"2");

    // With the scan part way, everything that waits for a ReadGuard runs to
    // completion on another thread.
    thread::scope(|scope| {
        scope
            .spawn(|| {
                for i in 0..KEYS {
                    engine.put(&key(i), b"3").unwrap();
                }
                engine.delete(&key(1)).unwrap();
                engine.flush().unwrap();
                assert!(compact_all(&engine) > 0);
            })
            .join()
            .unwrap();
    });
    let stats = engine.stats();
    assert_eq!((stats.runs, stats.frozen_tables), (1, 0));
    assert!(stats.retained_sources >= 3);

    let mut rest = 0;
    while let Some(record) = scan.next_record().unwrap() {
        assert_eq!(record.value, b"1");
        rest += 1;
    }
    assert_eq!(rest, KEYS - 1);
}

#[test]
fn released_sources_are_freed_after_the_last_snapshot_holding_them() {
    let engine = Engine::new(EngineOptions::default());
    engine.put(b"a", b"1").unwrap();
    engine.flush().unwrap();
    engine.put(b"a", b"2").unwrap();
    let first = engine.snapshot().unwrap();
    // Nothing written since: the second shares the first's view.
    let second = engine.snapshot().unwrap();
    let stats = engine.stats();
    assert_eq!((stats.snapshots, stats.frozen_tables), (2, 0));
    assert_eq!((stats.retained_sources, stats.retained_bytes), (0, 0));

    // The overwrite copies the leaf the snapshots share and keeps the value
    // they see, both pinned in the active table.
    engine.put(b"a", b"3").unwrap();
    let pinned = engine.stats().active.pinned;
    assert!(pinned > 0);
    assert_eq!(engine.stats().retained_bytes, pinned);
    let report = engine.flush().unwrap();
    assert_eq!(report.tables, 1);
    assert!(compact_all(&engine) > 0);
    let held = engine.stats();
    // The flushed table, charged as the snapshots' view with what it
    // pinned, and the run it shadowed, since merged away. The run flushed after the snapshots
    // was freed by the compaction.
    assert_eq!(held.retained_sources, 2);
    assert!(held.retained_bytes > pinned);

    drop(first);
    let stats = engine.stats();
    assert_eq!(stats.snapshots, 1);
    assert_eq!(
        (stats.retained_sources, stats.retained_bytes),
        (held.retained_sources, held.retained_bytes)
    );
    assert_eq!(value(&second, b"a").unwrap(), b"2");

    drop(second);
    let stats = engine.stats();
    assert_eq!((stats.snapshots, stats.retained_sources), (0, 0));
    assert_eq!(stats.retained_bytes, 0);
}

#[test]
fn a_released_table_is_charged_only_what_its_snapshots_can_hold() {
    let engine = Engine::new(EngineOptions {
        memtable_budget: 256 << 10,
        ..EngineOptions::default()
    });
    engine.put(b"seen", b"1").unwrap();
    let small = engine.snapshot().unwrap();
    let seen = engine.stats().active.charged;
    let mut key = 0u32;
    while engine.put(&key.to_be_bytes(), &[7; 100]).is_ok() {
        key += 1;
    }
    let full = engine.stats().active.charged;
    let pinned = engine.stats().active.pinned;
    engine.flush().unwrap();
    // Only the small view and the leaves the inserts copied away from it
    // remain once the table is freed; the table's own charge is not kept.
    let retained = engine.stats().retained_bytes;
    assert_eq!(retained, seen + pinned);
    assert!(retained * 20 < full, "{retained} of {full}");

    // A snapshot holding the frozen table itself keeps all of it.
    engine.put(b"seen", b"2").unwrap();
    let job = engine.begin_flush().unwrap();
    let whole = engine.snapshot().unwrap();
    let frozen = job
        .tables()
        .map(|table| table.memory().charged)
        .sum::<usize>();
    job.build().publish().unwrap();
    assert_eq!(engine.stats().retained_bytes, retained + frozen);

    assert_eq!(value(&small, b"seen").unwrap(), b"1");
    assert_eq!(value(&whole, b"seen").unwrap(), b"2");
    drop((small, whole));
    assert_eq!(engine.stats().retained_bytes, 0);
}

#[test]
fn each_limit_refuses_with_its_own_error_and_changes_nothing() {
    let engine = Engine::new(EngineOptions {
        snapshots: SnapshotLimits {
            max_live: 2,
            max_retained_bytes: 0,
        },
        ..EngineOptions::default()
    });
    for (key, value) in [(b"a", b"1"), (b"z", b"1"), (b"a", b"2"), (b"z", b"2")] {
        engine.put(key, value).unwrap();
        engine.flush().unwrap();
    }
    engine.put(b"m", b"1").unwrap();
    let first = engine.snapshot().unwrap();
    let second = engine.snapshot().unwrap();

    let before = engine.stats();
    let refused = engine.snapshot().unwrap_err();
    assert_eq!(refused, SnapshotError::Readers { live: 2, max: 2 });
    assert_eq!(engine.stats(), before);
    drop(second);

    // The write pins the snapshot's leaf, which is over the limit of zero.
    engine.put(b"m", b"2").unwrap();
    let before = engine.stats();
    let retained = before.retained_bytes;
    assert!(retained > 0);
    let refused = engine.snapshot().unwrap_err();
    assert_eq!(
        refused,
        SnapshotError::Retained {
            bytes: retained,
            max: 0
        }
    );
    assert!(refused.to_string().contains("retain"));
    // Compaction waits too, since snapshots could hold its inputs.
    let refused = engine
        .compact(merge_all(), &mut MergeWorkspace::new())
        .unwrap_err();
    assert_eq!(
        refused,
        CompactionError::Retained {
            bytes: retained,
            max: 0
        }
    );
    assert_eq!(engine.stats(), before);

    drop(first);
    assert_eq!(engine.stats().retained_bytes, 0);
    assert!(compact_all(&engine) > 0);
    let now = engine.snapshot().unwrap();
    assert_eq!(
        scanned(&now, b""),
        pairs(&[(b"a", b"2"), (b"m", b"2"), (b"z", b"2")])
    );
}

/// Snapshots after every write cost no generation: nothing is frozen or
/// flushed for them, and a write after each copies one leaf at most.
#[test]
fn frequent_snapshots_freeze_and_flush_nothing() {
    let engine = Engine::new(EngineOptions::default());
    let key = |i: usize| format!("k{i:05}").into_bytes();
    for i in 0..2_000 {
        engine.put(&key(i), b"0").unwrap();
    }
    for i in 0..2_000 {
        let snapshot = engine.snapshot().unwrap();
        engine.put(&key(i * 7 % 2_000), b"1").unwrap();
        assert!(engine.stats().active.pinned > 0);
        assert_eq!(value(&snapshot, &key(i * 7 % 2_000)).unwrap(), b"0");
    }
    let stats = engine.stats();
    assert_eq!((stats.frozen_tables, stats.runs), (0, 0));
    assert_eq!((stats.snapshots, stats.active.pinned), (0, 0));
    assert_eq!(stats.retained_bytes, 0);
}

/// A table's budget covers the history its snapshots pin, so a long-lived
/// snapshot brings the next flush forward, and dropping it frees the room
/// without a flush.
#[test]
fn pinned_history_is_charged_to_the_budget_until_the_snapshot_is_dropped() {
    const BUDGET: usize = 64 << 10;
    let engine = Engine::new(EngineOptions {
        memtable_budget: BUDGET,
        ..EngineOptions::default()
    });
    let key = |i: usize| format!("k{i:03}").into_bytes();
    for i in 0..200 {
        engine.put(&key(i), &[0; 100]).unwrap();
    }
    let live = engine.stats().active.charged;
    let snapshot = engine.snapshot().unwrap();
    let mut written = 0;
    let refused = loop {
        match engine.put(&key(written % 200), &[1; 100]) {
            Ok(_) => written += 1,
            Err(error) => break error,
        }
    };
    assert!(matches!(
        refused,
        UpdateError::Rejected(WriteError::BudgetExceeded { .. })
    ));
    let stats = engine.stats();
    assert!(stats.active.charged <= BUDGET);
    assert_eq!(stats.active.charged, live + stats.active.pinned);
    // Every key was overwritten at most once: the leaves and values the
    // snapshot shares are pinned once each.
    assert!(written <= 200, "{written}");
    assert_eq!(scanned(&snapshot, b"k").len(), 200);
    assert!(scanned(&snapshot, b"k").iter().all(|(_, v)| v == &[0; 100]));

    drop(snapshot);
    assert_eq!(engine.stats().active.pinned, 0);
    engine.put(&key(written % 200), &[1; 100]).unwrap();
    assert_eq!(engine.stats().frozen_tables, 0);
}

/// A snapshot taken while a flush or compaction is part way through sees
/// the same state as one taken before or after it.
#[test]
fn no_source_transition_leaves_a_gap() {
    let engine = Engine::new(EngineOptions::default());
    let mut expected: Pairs = Vec::new();
    let mut held: Vec<(Snapshot, Pairs)> = Vec::new();
    let mut take = |engine: &Engine, expected: &Pairs| {
        let snapshot = engine.snapshot().unwrap();
        assert_eq!(&scanned(&snapshot, b""), expected);
        held.push((snapshot, expected.clone()));
    };
    for round in 0..3u8 {
        for key in [&b"a"[..], b"m", b"z"] {
            engine.put(key, &[round]).unwrap();
        }
        engine.delete(b"m").unwrap();
        expected = pairs(&[(b"a", &[round]), (b"z", &[round])]);
        take(&engine, &expected);
        let job = engine.begin_flush().unwrap();
        take(&engine, &expected);
        let built = job.build();
        take(&engine, &expected);
        built.publish().unwrap();
        take(&engine, &expected);
    }
    let job = engine.begin_compaction(merge_all()).unwrap().unwrap();
    take(&engine, &expected);
    let built = job.build(&mut MergeWorkspace::new()).unwrap();
    take(&engine, &expected);
    built.publish().unwrap();
    take(&engine, &expected);
    assert_eq!(engine.stats().runs, 1);
    for (snapshot, then) in &held {
        assert_eq!(&scanned(snapshot, b""), then);
        assert_eq!(value(snapshot, b"m"), None);
    }
}

/// A writer appends keys in order, flushing as it goes, while snapshots are
/// taken beside it. Each snapshot sees exactly the writes up to its
/// sequence number: a prefix of the keys, never with a gap.
#[test]
fn concurrent_snapshots_see_a_prefix_of_the_writes() {
    const WRITES: usize = 3_000;
    let key = |i: usize| format!("k{i:05}").into_bytes();
    let engine = Engine::new(EngineOptions {
        background_compaction: Some(merge_all()),
        ..EngineOptions::default()
    });
    let start = Barrier::new(2);
    thread::scope(|scope| {
        scope.spawn(|| {
            start.wait();
            for i in 0..WRITES {
                assert_eq!(engine.put(&key(i), &i.to_le_bytes()).unwrap(), i as u64 + 1);
                if i % 500 == 499 {
                    engine.flush().unwrap();
                }
            }
        });
        start.wait();
        let mut seen = 0;
        while seen < WRITES {
            let snapshot = engine.snapshot().unwrap();
            let records = scanned(&snapshot, b"");
            let len = records.len();
            assert_eq!(len as u64, snapshot.last_seq());
            assert!(len >= seen);
            for (i, (k, v)) in records.iter().enumerate() {
                assert_eq!((k, v.as_slice()), (&key(i), &i.to_le_bytes()[..]));
            }
            if len > 0 {
                assert!(value(&snapshot, &key(len - 1)).is_some());
            }
            assert_eq!(value(&snapshot, &key(len)), None);
            seen = len;
        }
    });
}

#[test]
fn snapshots_are_shared_across_threads_and_outlive_the_engine() {
    fn shareable<T: Send + Sync>(_: &T) {}
    const READERS: usize = 2;
    let key = |i: usize| format!("k{i:03}").into_bytes();
    let engine = Engine::new(EngineOptions {
        target_run_len: 1 << 10,
        background_compaction: Some(CompactionOptions {
            max_overlap: 1,
            ..CompactionOptions::default()
        }),
        ..EngineOptions::default()
    });
    for round in 0..3u8 {
        for i in 0..100 {
            engine.put(&key(i), &[round]).unwrap();
        }
        engine.flush().unwrap();
    }
    let snapshot = engine.snapshot().unwrap();
    shareable(&snapshot);
    let expected: Pairs = (0..100).map(|i| (key(i), vec![2])).collect();

    let start = Barrier::new(READERS + 1);
    thread::scope(|scope| {
        for _ in 0..READERS {
            scope.spawn(|| {
                start.wait();
                assert_eq!(scanned(&snapshot, b""), expected);
            });
        }
        start.wait();
        for i in 0..100 {
            engine.delete(&key(i)).unwrap();
        }
        engine.flush().unwrap();
        engine.wait_for_compaction().unwrap();
    });
    assert!(engine.background_status().unwrap().compactions > 0);

    // Dropping the engine stops its worker; the snapshot keeps its sources.
    drop(engine);
    let reader = thread::spawn(move || scanned(&snapshot, b""));
    assert_eq!(reader.join().unwrap(), expected);
}
