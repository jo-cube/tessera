//! The public handles end to end: keyspaces, batches, readers and their
//! range scans against a `BTreeMap` oracle, pooled buffers, error kinds,
//! directories and their lock, checkpoints and close.

use std::collections::BTreeMap;
use std::ops::Bound::{self, Excluded, Included, Unbounded};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;

use crate::engine::{Engine, ErrorKind, Keyspace, Options, WriteBatch};
use crate::{MAX_KEY_LEN, MAX_METADATA_LEN, MAX_VALUE_LEN};

type Model = BTreeMap<Vec<u8>, Vec<u8>>;

/// What a scan found, and the bounds the oracle applies.
type Case<'a> = (Model, Bound<&'a [u8]>, Bound<&'a [u8]>);

fn manual() -> Options {
    Options {
        background_compaction: None,
        ..Options::default()
    }
}

struct Scratch(PathBuf);

impl Scratch {
    fn new(name: &str) -> Self {
        let path = std::env::temp_dir().join(format!("tessera-api-{name}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&path);
        Self(path)
    }
}

impl Drop for Scratch {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn collect(mut scan: crate::Scan<'_>) -> Model {
    let mut out = Model::new();
    while let Some((key, value)) = scan.next().unwrap() {
        out.insert(key.to_vec(), value.to_vec());
    }
    out
}

fn oracle(model: &Model, start: Bound<&[u8]>, end: Bound<&[u8]>) -> Model {
    let inverted = match (start, end) {
        (Included(a) | Excluded(a), Included(b) | Excluded(b)) => {
            a > b || (a == b && !matches!((start, end), (Included(_), Included(_))))
        }
        _ => false,
    };
    if inverted {
        return Model::new();
    }
    model
        .range::<[u8], _>((start, end))
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect()
}

/// Keys of two to five bytes from a small alphabet, so that bounds often
/// equal, neighbour or fall between stored keys.
fn random_key(state: &mut u64) -> Vec<u8> {
    let mut next = || {
        *state ^= *state << 13;
        *state ^= *state >> 7;
        *state ^= *state << 17;
        *state
    };
    let len = 2 + next() % 4;
    (0..len).map(|_| b"abcde"[(next() % 5) as usize]).collect()
}

/// An engine whose keys spread over flushed runs of many small blocks, a
/// frozen-then-flushed table with deletes, and the active memtable.
fn layered() -> (Engine, Keyspace, Model) {
    let engine = Engine::in_memory(Options {
        block_size: 64,
        target_run_len: 64,
        ..manual()
    })
    .unwrap();
    let ks = engine.keyspace("data").unwrap();
    let mut model = Model::new();
    let mut state = 0x2545_f491_4f6c_dd1d;
    for round in 0..4u8 {
        for _ in 0..300 {
            let key = random_key(&mut state);
            if state % 7 == 0 {
                ks.delete(&key).unwrap();
                model.remove(&key);
            } else {
                let value = [&key[..], &[round]].concat();
                ks.put(&key, &value).unwrap();
                model.insert(key, value);
            }
        }
        if round < 3 {
            engine.flush().unwrap();
        }
    }
    assert!(engine.engine_stats().runs >= 3);
    (engine, ks, model)
}

#[test]
fn every_range_form_matches_the_oracle_across_blocks_runs_and_memtables() {
    let (engine, ks, model) = layered();
    let snapshot = engine.snapshot();
    let mut state = 0x9e37_79b9_7f4a_7c15;
    for latest in [true, false] {
        let mut reader = if latest {
            engine.reader()
        } else {
            snapshot.reader()
        };
        assert_eq!(collect(reader.range(&ks, ..)), model);
        assert_eq!(collect(reader.prefix(&ks, b"")), model);
        for _ in 0..400 {
            let (a, b) = (random_key(&mut state), random_key(&mut state));
            let (a, b) = (&a[..], &b[..]);
            let cases: [Case<'_>; 9] = [
                (collect(reader.range(&ks, a..b)), Included(a), Excluded(b)),
                (collect(reader.range(&ks, a..=b)), Included(a), Included(b)),
                (collect(reader.range(&ks, a..)), Included(a), Unbounded),
                (collect(reader.range(&ks, ..b)), Unbounded, Excluded(b)),
                (collect(reader.range(&ks, ..=b)), Unbounded, Included(b)),
                (
                    collect(reader.range(&ks, (Excluded(a), Excluded(b)))),
                    Excluded(a),
                    Excluded(b),
                ),
                (
                    collect(reader.range(&ks, (Excluded(a), Included(b)))),
                    Excluded(a),
                    Included(b),
                ),
                (
                    collect(reader.range(&ks, (Excluded(a), Unbounded))),
                    Excluded(a),
                    Unbounded,
                ),
                (collect(reader.range(&ks, a..a)), Included(a), Excluded(a)),
            ];
            for (found, start, end) in cases {
                assert_eq!(found, oracle(&model, start, end), "{start:?}..{end:?}");
            }
            let prefix = &a[..2];
            let expected: Model = model
                .iter()
                .filter(|(key, _)| key.starts_with(prefix))
                .map(|(key, value)| (key.clone(), value.clone()))
                .collect();
            assert_eq!(collect(reader.prefix(&ks, prefix)), expected);
        }
    }
}

#[test]
fn empty_inverted_and_owned_ranges() {
    let (engine, ks, model) = layered();
    let mut reader = engine.reader();
    assert!(collect(reader.range(&ks, &b"c"[..]..&b"b"[..])).is_empty());
    assert!(collect(reader.range(&ks, &b"c"[..]..=&b"b"[..])).is_empty());
    assert!(collect(reader.range(&ks, (Excluded(&b"cc"[..]), Excluded(&b"cc"[..])))).is_empty());
    assert!(collect(reader.range(&ks, (Excluded(&b"cc"[..]), Included(&b"cc"[..])))).is_empty());
    assert!(collect(reader.range(&ks, &b"f"[..]..)).is_empty());
    assert!(collect(reader.range(&ks, ..&b""[..])).is_empty());
    assert!(collect(reader.prefix(&ks, b"z")).is_empty());
    // Owned bounds, and an upper bound longer than any key.
    let long = vec![b'e'; MAX_KEY_LEN + 10];
    assert_eq!(collect(reader.range(&ks, b"a".to_vec()..long)), model);
    let first = model.keys().next().unwrap().clone();
    assert_eq!(
        collect(reader.range(&ks, first.clone()..=first.clone())).len(),
        1
    );
    // A scan that has ended keeps ending.
    let mut scan = reader.range(&ks, &b"f"[..]..);
    assert_eq!(scan.next().unwrap(), None);
    assert_eq!(scan.next().unwrap(), None);
    drop(scan);
    // An empty keyspace scans empty.
    let other = Engine::in_memory(manual()).unwrap();
    let empty = other.keyspace("empty").unwrap();
    assert!(collect(other.reader().range(&empty, ..)).is_empty());
}

/// Each batch writes `a/i` and `b/i` together; every scan of the latest
/// state sees both or neither, and reads one state from start to end even
/// while writers run.
#[test]
fn a_latest_state_scan_pins_one_state_while_writers_run() {
    let engine = Engine::in_memory(Options {
        memtable_size: 64 << 10,
        ..manual()
    })
    .unwrap();
    let ks = engine.keyspace("pairs").unwrap();
    let done = Arc::new(AtomicBool::new(false));
    let writer = {
        let (engine, ks, done) = (engine.clone(), ks.clone(), Arc::clone(&done));
        thread::spawn(move || {
            let mut batch = WriteBatch::new();
            for i in 0..4_000u32 {
                batch.clear();
                let value = i.to_be_bytes();
                batch.put(&ks, format!("a/{i:05}").as_bytes(), &value);
                batch.put(&ks, format!("b/{i:05}").as_bytes(), &value);
                engine.write(&batch).unwrap();
                if i % 1_000 == 999 {
                    engine.flush().unwrap();
                }
            }
            done.store(true, Ordering::Release);
        })
    };
    let mut reader = engine.reader();
    let mut scans = 0;
    loop {
        let finished = done.load(Ordering::Acquire);
        let seen = collect(reader.range(&ks, ..));
        let a: Vec<_> = seen.keys().filter(|key| key.starts_with(b"a/")).collect();
        let b: Vec<_> = seen.keys().filter(|key| key.starts_with(b"b/")).collect();
        assert_eq!(a.len(), b.len(), "scan {scans}");
        for (a, b) in a.iter().zip(&b) {
            assert_eq!(a[2..], b[2..]);
        }
        scans += 1;
        if finished {
            assert_eq!(seen.len(), 8_000);
            break;
        }
    }
    writer.join().unwrap();
    assert!(scans > 1);
}

#[test]
fn multi_get_keeps_input_order_and_reuses_its_result_vector() {
    let (engine, ks, model) = layered();
    let mut keys: Vec<Vec<u8>> = model.keys().rev().take(50).cloned().collect();
    keys.push(b"missing".to_vec());
    keys.push(keys[0].clone());
    let expected: Vec<Option<Vec<u8>>> = keys.iter().map(|key| model.get(key).cloned()).collect();
    let snapshot = engine.snapshot();
    for latest in [true, false] {
        let mut reader = if latest {
            engine.reader()
        } else {
            snapshot.reader()
        };
        let found: Vec<_> = reader
            .multi_get(&ks, &keys)
            .unwrap()
            .iter()
            .map(|value| value.map(<[u8]>::to_vec))
            .collect();
        assert_eq!(found, expected);
        let capacity = reader.result_capacity();
        assert!(capacity >= keys.len());
        for _ in 0..3 {
            assert_eq!(reader.multi_get(&ks, &keys).unwrap().len(), keys.len());
            assert_eq!(reader.result_capacity(), capacity);
        }
        assert!(reader.multi_get(&ks, &[] as &[&[u8]]).unwrap().is_empty());
        assert_eq!(reader.result_capacity(), capacity);
    }
}

#[test]
fn readers_return_their_buffers_to_the_pool() {
    let (engine, ks, _) = layered();
    let pool = &engine.shared.pool;
    let free = pool.len();
    let mut reader = engine.reader();
    assert_eq!(pool.len(), free.saturating_sub(1));
    reader.get(&ks, b"zzzz").unwrap();
    for key in [&b"aa"[..], b"bb", b"cc"] {
        reader.get(&ks, key).unwrap();
    }
    let buffer = reader.value_buffer();
    drop(reader);
    let after = pool.len();
    assert!(after >= 1);
    // The next reader takes the buffers the last one returned.
    let reader = engine.reader();
    assert_eq!(reader.value_buffer(), buffer);
    assert_eq!(pool.len(), after - 1);
    drop(reader);
    // Snapshot readers share the engine's pool.
    let snapshot = engine.snapshot();
    let reader = snapshot.reader();
    assert_eq!(reader.value_buffer(), buffer);
    drop(reader);
    // `Keyspace::get` uses a pooled reader and leaves the pool as it was.
    let before = pool.len();
    ks.get(b"aa").unwrap();
    assert_eq!(pool.len(), before);
}

#[test]
fn error_kinds_by_failure() {
    let engine = Engine::in_memory(manual()).unwrap();
    let ks = engine.keyspace("one").unwrap();
    let kind = |result: crate::Result<()>| result.unwrap_err().kind();

    for name in [
        "",
        "has space",
        "slash/",
        &"n".repeat(crate::MAX_KEYSPACE_NAME_LEN + 1),
    ] {
        assert_eq!(
            engine.keyspace(name).unwrap_err().kind(),
            ErrorKind::InvalidInput
        );
        assert_eq!(
            engine.drop_keyspace(name).unwrap_err().kind(),
            ErrorKind::InvalidInput
        );
    }
    let max_name = "n".repeat(crate::MAX_KEYSPACE_NAME_LEN);
    let longest = engine.keyspace(&max_name).unwrap();
    assert_eq!(longest.name(), max_name);

    let long_key = vec![0; MAX_KEY_LEN + 1];
    assert_eq!(kind(ks.put(&long_key, b"")), ErrorKind::InvalidInput);
    assert_eq!(kind(ks.delete(&long_key)), ErrorKind::InvalidInput);
    assert_eq!(
        kind(ks.put(b"k", &vec![0; MAX_VALUE_LEN + 1])),
        ErrorKind::InvalidInput
    );
    let mut batch = WriteBatch::new();
    batch.put(&ks, b"fine", b"");
    batch.put(&ks, &long_key, b"");
    assert_eq!(kind(engine.write(&batch)), ErrorKind::InvalidInput);
    assert_eq!(ks.get(b"fine").unwrap(), None);

    // A keyspace of another engine.
    let other = Engine::in_memory(manual()).unwrap();
    let foreign = other.keyspace("one").unwrap();
    let mut reader = engine.reader();
    assert_eq!(
        reader.get(&foreign, b"k").unwrap_err().kind(),
        ErrorKind::UnknownKeyspace
    );
    assert_eq!(
        reader.multi_get(&foreign, &[b"k"]).unwrap_err().kind(),
        ErrorKind::UnknownKeyspace
    );
    assert_eq!(
        reader.range(&foreign, ..).next().unwrap_err().kind(),
        ErrorKind::UnknownKeyspace
    );
    drop(reader);
    let snapshot = engine.snapshot();
    assert_eq!(
        snapshot.reader().get(&foreign, b"k").unwrap_err().kind(),
        ErrorKind::UnknownKeyspace
    );
    batch.clear();
    batch.put(&foreign, b"k", b"v");
    assert_eq!(kind(engine.write(&batch)), ErrorKind::UnknownKeyspace);

    // Options outside their range.
    for options in [
        Options {
            block_size: 0,
            ..manual()
        },
        Options {
            level_ratio: 1,
            ..manual()
        },
        Options {
            background_threads: 0,
            ..manual()
        },
    ] {
        assert_eq!(
            Engine::in_memory(options).unwrap_err().kind(),
            ErrorKind::Config
        );
    }

    // Checkpoints need a directory, and metadata has a limit.
    assert_eq!(kind(engine.checkpoint()), ErrorKind::Config);
    assert_eq!(kind(engine.checkpoint_with(b"m")), ErrorKind::Config);
    assert_eq!(engine.path(), None);
    let scratch = Scratch::new("errors");
    let on_disk = Engine::open(&scratch.0, manual()).unwrap();
    assert_eq!(
        kind(on_disk.checkpoint_with(&vec![0; MAX_METADATA_LEN + 1])),
        ErrorKind::InvalidInput
    );
    assert_eq!(on_disk.metadata(), None);
}

#[test]
fn a_dropped_keyspace_refuses_its_handles_and_its_name_can_be_reused() {
    let engine = Engine::in_memory(manual()).unwrap();
    let users = engine.keyspace("users").unwrap();
    users.put(b"k", b"v1").unwrap();
    assert_eq!(
        engine.keyspace("users").unwrap().get(b"k").unwrap(),
        Some(b"v1".to_vec())
    );
    let before = engine.snapshot();
    assert!(engine.drop_keyspace("users").unwrap());
    assert!(!engine.drop_keyspace("users").unwrap());
    assert!(!engine.drop_keyspace("never").unwrap());
    assert!(engine.keyspaces().is_empty());

    let dropped = |result: crate::Result<()>| result.unwrap_err().kind();
    assert_eq!(dropped(users.put(b"k", b"v")), ErrorKind::KeyspaceDropped);
    assert_eq!(dropped(users.delete(b"k")), ErrorKind::KeyspaceDropped);
    assert_eq!(
        users.get(b"k").unwrap_err().kind(),
        ErrorKind::KeyspaceDropped
    );
    assert_eq!(
        users.stats().unwrap_err().kind(),
        ErrorKind::KeyspaceDropped
    );
    let mut batch = WriteBatch::new();
    batch.put(&users, b"k", b"v");
    assert_eq!(dropped(engine.write(&batch)), ErrorKind::KeyspaceDropped);
    assert_eq!(
        engine.reader().range(&users, ..).next().unwrap_err().kind(),
        ErrorKind::KeyspaceDropped
    );
    // A snapshot taken before the drop still reads it.
    assert_eq!(before.reader().get(&users, b"k").unwrap(), Some(&b"v1"[..]));

    // The name is free again, for a keyspace that starts empty; a snapshot
    // older than it sees it empty.
    let again = engine.keyspace("users").unwrap();
    assert_eq!(again.get(b"k").unwrap(), None);
    again.put(b"k", b"v2").unwrap();
    assert_eq!(before.reader().get(&again, b"k").unwrap(), None);
    assert!(collect(before.reader().range(&again, ..)).is_empty());
    assert!(before.reader().multi_get(&again, &[b"k"]).unwrap()[0].is_none());
    assert_eq!(dropped(users.put(b"k", b"v")), ErrorKind::KeyspaceDropped);
    assert_eq!(engine.keyspaces(), ["users"]);
    engine.keyspace("orders").unwrap();
    assert_eq!(engine.keyspaces(), ["orders", "users"]);
}

#[test]
fn existing_keyspace_finds_a_keyspace_and_never_creates_one() {
    let engine = Engine::in_memory(manual()).unwrap();
    assert!(engine.existing_keyspace("users").unwrap().is_none());
    assert!(engine.keyspaces().is_empty());

    let users = engine.keyspace("users").unwrap();
    users.put(b"k", b"v").unwrap();
    let found = engine.existing_keyspace("users").unwrap().unwrap();
    assert_eq!(found.name(), "users");
    assert_eq!(found.get(b"k").unwrap(), Some(b"v".to_vec()));
    found.put(b"j", b"w").unwrap();
    assert_eq!(users.get(b"j").unwrap(), Some(b"w".to_vec()));

    // Names compare exactly, and a bad name is refused rather than missing.
    assert!(engine.existing_keyspace("Users").unwrap().is_none());
    let long = "n".repeat(crate::MAX_KEYSPACE_NAME_LEN + 1);
    for name in ["", "a b", "ü", &long] {
        assert_eq!(
            engine.existing_keyspace(name).unwrap_err().kind(),
            ErrorKind::InvalidInput
        );
    }
    assert_eq!(engine.keyspaces(), ["users"]);
}

#[test]
fn existing_keyspace_misses_a_dropped_keyspace_until_it_is_recreated() {
    let engine = Engine::in_memory(manual()).unwrap();
    let users = engine.keyspace("users").unwrap();
    users.put(b"k", b"v1").unwrap();
    assert!(engine.drop_keyspace("users").unwrap());
    assert!(engine.existing_keyspace("users").unwrap().is_none());
    assert!(engine.keyspaces().is_empty());

    engine.keyspace("users").unwrap();
    let again = engine.existing_keyspace("users").unwrap().unwrap();
    assert_eq!(again.get(b"k").unwrap(), None);
    assert_eq!(
        users.get(b"k").unwrap_err().kind(),
        ErrorKind::KeyspaceDropped
    );
}

#[test]
fn existing_keyspace_finds_checkpointed_keyspaces_after_reopening() {
    let scratch = Scratch::new("existing");
    let engine = Engine::open(&scratch.0, manual()).unwrap();
    engine.keyspace("kept").unwrap().put(b"k", b"v").unwrap();
    engine.keyspace("empty").unwrap();
    engine.checkpoint().unwrap();
    engine.keyspace("unsaved").unwrap();
    engine.close().unwrap();

    let engine = Engine::open(&scratch.0, manual()).unwrap();
    let kept = engine.existing_keyspace("kept").unwrap().unwrap();
    assert_eq!(kept.get(b"k").unwrap(), Some(b"v".to_vec()));
    assert!(engine.existing_keyspace("empty").unwrap().is_some());
    assert!(engine.existing_keyspace("unsaved").unwrap().is_none());
    assert_eq!(engine.keyspaces(), ["empty", "kept"]);
    engine.close().unwrap();
}

#[test]
fn existing_keyspace_returns_a_keyspace_an_older_snapshot_reads_as_empty() {
    let engine = Engine::in_memory(manual()).unwrap();
    let before = engine.snapshot();
    engine.keyspace("late").unwrap().put(b"k", b"v").unwrap();

    let late = engine.existing_keyspace("late").unwrap().unwrap();
    assert_eq!(late.get(b"k").unwrap(), Some(b"v".to_vec()));
    assert_eq!(before.reader().get(&late, b"k").unwrap(), None);
    assert!(collect(before.reader().range(&late, ..)).is_empty());
    assert_eq!(
        engine.snapshot().reader().get(&late, b"k").unwrap(),
        Some(&b"v"[..])
    );
}

#[test]
fn stats_cover_each_live_keyspace() {
    let (engine, ks, _) = layered();
    let stats = engine.stats();
    assert_eq!(stats.memory_limit, None);
    assert!(stats.memory_used >= stats.stored_bytes);
    assert_eq!(stats.keyspaces.len(), 1);
    assert_eq!(stats.keyspaces[0].name, "data");
    assert_eq!(stats.keyspaces[0].stored_bytes, stats.stored_bytes);
    assert_eq!(ks.stats().unwrap(), stats.keyspaces[0]);
    assert!(stats.stored_bytes > 0);
}

#[test]
fn a_snapshot_clone_reads_the_same_state_after_the_original_is_dropped() {
    let engine = Engine::in_memory(manual()).unwrap();
    let ks = engine.keyspace("data").unwrap();
    ks.put(b"k", b"old").unwrap();
    let snapshot = engine.snapshot();
    let clone = snapshot.clone();
    drop(snapshot);
    ks.put(b"k", b"newer").unwrap();
    engine.flush().unwrap();
    let held = thread::spawn(move || clone.reader().get(&ks, b"k").unwrap().map(<[u8]>::to_vec))
        .join()
        .unwrap();
    assert_eq!(held, Some(b"old".to_vec()));
}

#[test]
fn open_takes_the_directory_lock_until_close() {
    let scratch = Scratch::new("lock");
    let engine = Engine::open(&scratch.0, manual()).unwrap();
    assert_eq!(engine.path(), Some(scratch.0.as_path()));
    let locked = Engine::open(&scratch.0, manual()).unwrap_err();
    assert_eq!(locked.kind(), ErrorKind::Locked, "{locked}");
    assert_eq!(locked.path(), Some(scratch.0.as_path()));
    // Another handle keeps the lock; closing any handle releases it.
    let clone = engine.clone();
    drop(engine);
    assert_eq!(
        Engine::open(&scratch.0, manual()).unwrap_err().kind(),
        ErrorKind::Locked
    );
    clone.close().unwrap();
    Engine::open(&scratch.0, manual()).unwrap().close().unwrap();
    // Dropping the last handle releases it as well.
    drop(Engine::open(&scratch.0, manual()).unwrap());
    Engine::open(&scratch.0, manual()).unwrap();
}

#[test]
fn plain_checkpoints_carry_metadata_forward() {
    let scratch = Scratch::new("metadata");
    let engine = Engine::open(&scratch.0, manual()).unwrap();
    let ks = engine.keyspace("data").unwrap();
    ks.put(b"a", b"1").unwrap();
    engine.checkpoint().unwrap();
    assert_eq!(engine.metadata(), None);
    engine.checkpoint_with(b"schema=1").unwrap();
    ks.put(b"b", b"2").unwrap();
    engine.checkpoint().unwrap();
    assert_eq!(engine.metadata(), Some(b"schema=1".to_vec()));
    engine.close().unwrap();

    let engine = Engine::open(&scratch.0, manual()).unwrap();
    assert_eq!(engine.metadata(), Some(b"schema=1".to_vec()));
    assert_eq!(engine.keyspaces(), ["data"]);
    let ks = engine.keyspace("data").unwrap();
    assert_eq!(ks.get(b"b").unwrap(), Some(b"2".to_vec()));
    engine.checkpoint_with(b"").unwrap();
    engine.close().unwrap();
    let engine = Engine::open(&scratch.0, manual()).unwrap();
    assert_eq!(engine.metadata(), Some(Vec::new()));
}

#[test]
fn close_stops_every_handle_but_snapshots_keep_reading() {
    let scratch = Scratch::new("close");
    let engine = Engine::open(&scratch.0, manual()).unwrap();
    let ks = engine.keyspace("data").unwrap();
    ks.put(b"k", b"v").unwrap();
    let other = engine.clone();
    let snapshot = engine.snapshot();
    engine.close().unwrap();

    let stopped = |result: crate::Result<()>| result.unwrap_err().kind();
    assert_eq!(stopped(ks.put(b"k", b"w")), ErrorKind::Stopped);
    assert_eq!(stopped(ks.delete(b"k")), ErrorKind::Stopped);
    assert_eq!(ks.get(b"k").unwrap_err().kind(), ErrorKind::Stopped);
    assert_eq!(ks.stats().unwrap_err().kind(), ErrorKind::Stopped);
    assert_eq!(
        other.keyspace("data").unwrap_err().kind(),
        ErrorKind::Stopped
    );
    assert_eq!(
        other.drop_keyspace("data").unwrap_err().kind(),
        ErrorKind::Stopped
    );
    assert_eq!(
        other.existing_keyspace("data").unwrap_err().kind(),
        ErrorKind::Stopped
    );
    assert_eq!(stopped(other.checkpoint()), ErrorKind::Stopped);
    let mut batch = WriteBatch::new();
    batch.put(&ks, b"k", b"w");
    assert_eq!(stopped(other.write(&batch)), ErrorKind::Stopped);
    let mut reader = other.reader();
    assert_eq!(
        reader.get(&ks, b"k").unwrap_err().kind(),
        ErrorKind::Stopped
    );
    assert_eq!(
        reader.multi_get(&ks, &[b"k"]).unwrap_err().kind(),
        ErrorKind::Stopped
    );
    assert_eq!(
        reader.range(&ks, ..).next().unwrap_err().kind(),
        ErrorKind::Stopped
    );
    drop(reader);

    assert_eq!(snapshot.reader().get(&ks, b"k").unwrap(), Some(&b"v"[..]));
    assert_eq!(collect(snapshot.reader().prefix(&ks, b"")).len(), 1);
    // Closing again does nothing; the lock is free.
    other.close().unwrap();
    drop(Engine::open(&scratch.0, manual()).unwrap());
}

#[test]
fn background_compaction_runs_for_a_handle_and_stops_with_it() {
    let engine = Engine::in_memory(Options {
        memtable_size: 32 << 10,
        ..Options::default()
    })
    .unwrap();
    let ks = engine.keyspace("data").unwrap();
    for i in 0..20_000u32 {
        ks.put(&i.to_be_bytes(), &[7; 16]).unwrap();
    }
    engine.flush().unwrap();
    engine.wait_for_compaction().unwrap();
    assert_eq!(collect(engine.reader().range(&ks, ..)).len(), 20_000);
    drop(ks);
    engine.close().unwrap();
}
