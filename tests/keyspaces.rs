//! Many keyspaces in one engine: isolation of keys, scans and statistics,
//! atomic batches across keyspaces, drop and recreate, checkpoints and
//! recovery, compaction fairness, the keyspace limit and a randomized model.

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;

use crate::engine::{CompactionPolicy, Engine, ErrorKind, Keyspace, Options, WriteBatch};
use crate::test_support::Rng;

type Model = BTreeMap<Vec<u8>, Vec<u8>>;

fn manual() -> Options {
    Options {
        background_compaction: None,
        ..Options::default()
    }
}

/// Small memtables and runs, so that a few hundred records flush and
/// compact, with the background worker.
fn small() -> Options {
    Options {
        memtable_size: 32 << 10,
        target_run_len: 8 << 10,
        background_compaction: Some(CompactionPolicy::default()),
        ..Options::default()
    }
}

struct Scratch(PathBuf);

impl Scratch {
    fn new(name: &str) -> Self {
        let path =
            std::env::temp_dir().join(format!("tessera-keyspaces-{name}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&path);
        Self(path)
    }
}

impl Drop for Scratch {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// Every live record of `ks` as `reader` sees it, by a full scan, checked
/// against point reads and a multi-get.
fn contents(reader: &mut crate::Reader<'_>, ks: &Keyspace) -> Model {
    let mut out = Model::new();
    let mut scan = reader.range(ks, ..);
    while let Some((key, value)) = scan.next().unwrap() {
        assert!(out.insert(key.to_vec(), value.to_vec()).is_none());
    }
    drop(scan);
    let keys: Vec<&Vec<u8>> = out.keys().collect();
    let values: Vec<Option<Vec<u8>>> = reader
        .multi_get(ks, &keys)
        .unwrap()
        .iter()
        .map(|value| value.map(<[u8]>::to_vec))
        .collect();
    for (key, value) in keys.iter().zip(values) {
        assert_eq!(value.as_ref(), out.get(*key));
        assert_eq!(
            reader.get(ks, key).unwrap(),
            out.get(*key).map(Vec::as_slice)
        );
    }
    out
}

fn scan(reader: &mut crate::Reader<'_>, ks: &Keyspace, prefix: &[u8]) -> Vec<Vec<u8>> {
    let mut keys = Vec::new();
    let mut scan = reader.prefix(ks, prefix);
    while let Some((key, _)) = scan.next().unwrap() {
        keys.push(key.to_vec());
    }
    keys
}

/// The same keys in keyspaces whose IDs take one and two prefix bytes, in
/// memtables and in runs, read back apart; scans of each stay in it; and
/// dropping one leaves the others' data and figures alone.
#[test]
fn keyspaces_hold_the_same_keys_apart() {
    let engine = Engine::in_memory(manual()).unwrap();
    let all: Vec<Keyspace> = (0..130)
        .map(|i| engine.keyspace(&format!("ks{i:03}")).unwrap())
        .collect();
    let picked = [&all[0], &all[1], &all[127], &all[128], &all[129]];
    assert_eq!(all[129].id(), 129);
    for (round, flush) in [(0u8, true), (1, false)] {
        for (i, ks) in picked.iter().enumerate() {
            for key in [&b"a"[..], b"ab", b"b", b"\xff"] {
                ks.put(key, &[i as u8, round]).unwrap();
            }
            ks.put(format!("own{i}").as_bytes(), b"").unwrap();
        }
        if flush {
            engine.flush().unwrap();
        }
    }
    picked[2].delete(b"b").unwrap();
    let check = |reader: &mut crate::Reader<'_>| {
        for (i, ks) in picked.iter().enumerate() {
            let model = contents(reader, ks);
            let mut expected: Model = [&b"a"[..], b"ab", b"b", b"\xff"]
                .into_iter()
                .filter(|key| i != 2 || *key != b"b")
                .map(|key| (key.to_vec(), vec![i as u8, 1]))
                .collect();
            expected.insert(format!("own{i}").into_bytes(), Vec::new());
            assert_eq!(model, expected, "keyspace {i}");
            assert_eq!(scan(reader, ks, b"a"), [b"a".to_vec(), b"ab".to_vec()]);
            assert_eq!(scan(reader, ks, b"own"), [format!("own{i}").into_bytes()]);
            let mut range = reader.range(ks, &b"b"[..]..);
            let mut keys = Vec::new();
            while let Some((key, _)) = range.next().unwrap() {
                keys.push(key.to_vec());
            }
            // "b", "\xff" and "own{i}", but "b" is deleted in keyspace 2.
            assert_eq!(keys.len(), if i == 2 { 2 } else { 3 }, "keyspace {i}");
        }
        // Keyspaces never written to stay empty in every read path.
        assert!(contents(reader, &all[2]).is_empty());
        assert_eq!(reader.get(&all[126], b"a").unwrap(), None);
    };
    check(&mut engine.reader());
    let snapshot = engine.snapshot();
    check(&mut snapshot.reader());
    engine.flush().unwrap();
    while engine
        .compact(&mut crate::engine::MergeWorkspace::new())
        .unwrap()
        .is_some()
    {}
    check(&mut engine.reader());

    let stats = engine.stats();
    assert_eq!(stats.keyspaces.len(), 130);
    let stored: usize = stats.keyspaces.iter().map(|ks| ks.stored_bytes).sum();
    assert_eq!(stored, stats.stored_bytes);
    let before: Vec<_> = stats
        .keyspaces
        .iter()
        .filter(|ks| ks.name != "ks128")
        .cloned()
        .collect();
    assert!(engine.drop_keyspace("ks128").unwrap());
    let after = engine.stats();
    assert_eq!(after.keyspaces, before);
    assert_eq!(
        after.stored_bytes,
        before.iter().map(|ks| ks.stored_bytes).sum::<usize>()
    );
    let mut reader = engine.reader();
    for (i, ks) in picked.iter().enumerate().filter(|(i, _)| *i != 3) {
        assert_eq!(reader.get(ks, b"a").unwrap(), Some(&[i as u8, 1][..]));
    }
    assert_eq!(
        reader.get(picked[3], b"a").unwrap_err().kind(),
        ErrorKind::KeyspaceDropped
    );
}

/// Batches that set one key in two keyspaces to the same value are seen
/// whole or not at all by concurrent snapshots, through flushes, large
/// batches that become runs, and compaction.
#[test]
fn a_batch_across_keyspaces_is_atomic_under_concurrent_snapshots() {
    let engine = Engine::in_memory(small()).unwrap();
    let (a, b) = (engine.keyspace("a").unwrap(), engine.keyspace("b").unwrap());
    let done = AtomicBool::new(false);
    thread::scope(|scope| {
        let readers: Vec<_> = (0..2)
            .map(|_| {
                scope.spawn(|| {
                    let mut checked = 0;
                    while !done.load(Ordering::SeqCst) || checked == 0 {
                        let snapshot = engine.snapshot();
                        let mut reader = snapshot.reader();
                        let left = contents(&mut reader, &a);
                        let right = contents(&mut reader, &b);
                        assert_eq!(left, right);
                        checked += 1;
                    }
                    checked
                })
            })
            .collect();
        let mut batch = WriteBatch::new();
        for round in 0..300u32 {
            batch.clear();
            // Every tenth round is large enough to be built into runs.
            let keys = if round % 10 == 9 { 600 } else { 4 };
            for key in 0..keys {
                let key = format!("k{:04}", (round * 7 + key) % 1000);
                let value = round.to_be_bytes();
                if round % 3 == 2 && key.ends_with('0') {
                    batch.delete(&a, key.as_bytes());
                    batch.delete(&b, key.as_bytes());
                } else {
                    batch.put(&a, key.as_bytes(), &value);
                    batch.put(&b, key.as_bytes(), &value);
                }
            }
            engine.write(&batch).unwrap();
        }
        done.store(true, Ordering::SeqCst);
        for reader in readers {
            assert!(reader.join().unwrap() > 0);
        }
    });
    engine.wait_for_compaction().unwrap();
    let mut reader = engine.reader();
    assert_eq!(contents(&mut reader, &a), contents(&mut reader, &b));
}

/// A batch naming a dropped keyspace fails whole, small or large, and
/// changes nothing in the others.
#[test]
fn a_batch_naming_a_dropped_keyspace_fails_whole() {
    let engine = Engine::in_memory(manual()).unwrap();
    let (a, gone) = (
        engine.keyspace("a").unwrap(),
        engine.keyspace("gone").unwrap(),
    );
    a.put(b"k", b"before").unwrap();
    engine.drop_keyspace("gone").unwrap();
    for len in [1, 2, 20_000] {
        let mut batch = WriteBatch::new();
        for i in 0..len {
            batch.put(&a, format!("k{i:06}").as_bytes(), b"after");
        }
        batch.put(&a, b"k", b"after");
        batch.put(&gone, b"k", b"after");
        let error = engine.write(&batch).unwrap_err();
        assert_eq!(error.kind(), ErrorKind::KeyspaceDropped, "{len}");
        assert_eq!(a.get(b"k").unwrap().unwrap(), b"before");
        assert_eq!(a.get(b"k000000").unwrap(), None);
    }
    assert_eq!(engine.engine_stats().last_seq, 1);
}

/// Handles of a dropped keyspace fail; the name comes back empty with a
/// higher ID; a snapshot from before the drop reads the dropped data; and
/// the next flush leaves the dropped records out, so memory falls.
#[test]
fn a_dropped_keyspace_is_recreated_empty_and_its_records_are_freed() {
    let engine = Engine::in_memory(manual()).unwrap();
    let (keep, dropped) = (
        engine.keyspace("keep").unwrap(),
        engine.keyspace("users").unwrap(),
    );
    for i in 0..2000u32 {
        dropped.put(&i.to_be_bytes(), &[1; 64]).unwrap();
    }
    engine.flush().unwrap();
    for i in 0..2000u32 {
        dropped.put(&i.to_be_bytes(), &[2; 64]).unwrap();
    }
    keep.put(b"k", b"v").unwrap();
    let snapshot = engine.snapshot();
    let memtables = engine.engine_stats().memtable_bytes;
    assert!(engine.drop_keyspace("users").unwrap());
    // Still charged: the memtable keeps the records until a flush.
    assert_eq!(engine.engine_stats().memtable_bytes, memtables);
    assert_eq!(
        dropped.put(b"x", b"y").unwrap_err().kind(),
        ErrorKind::KeyspaceDropped
    );
    let again = engine.keyspace("users").unwrap();
    assert!(again.id() > dropped.id());
    assert!(contents(&mut engine.reader(), &again).is_empty());
    assert_eq!(again.stats().unwrap().stored_bytes, 0);
    // The snapshot predates the recreated keyspace and reads it as empty.
    assert_eq!(snapshot.reader().get(&again, b"x").unwrap(), None);

    let report = engine.flush().unwrap();
    // Only the live keyspace's record was written.
    assert_eq!(report.runs, 1);
    let stats = engine.engine_stats();
    assert!(stats.memtable_bytes < memtables / 4, "{stats:?}");
    assert!(contents(&mut engine.reader(), &again).is_empty());
    assert_eq!(keep.get(b"k").unwrap().unwrap(), b"v");
    // The snapshot from before the drop reads the dropped keyspace in full.
    let mut reader = snapshot.reader();
    let seen = contents(&mut reader, &dropped);
    assert_eq!(seen.len(), 2000);
    assert!(seen.values().all(|value| value == &[2; 64]));
    assert!(contents(&mut reader, &again).is_empty());
    drop(reader);
    drop(snapshot);
    // Without the snapshot, the dropped keyspace's runs are released.
    assert_eq!(engine.engine_stats().snapshot_bytes, 0);
    assert_eq!(
        engine.stats().stored_bytes,
        keep.stats().unwrap().stored_bytes
    );
}

/// A checkpoint of many keyspaces, one dropped and created again before it,
/// recovers every keyspace by name with its ID and data, and the next ID
/// continues where it was.
#[test]
fn many_keyspaces_survive_a_checkpoint_with_their_ids() {
    let scratch = Scratch::new("recovery");
    let engine = Engine::open(&scratch.0, small()).unwrap();
    let mut model: BTreeMap<String, Model> = BTreeMap::new();
    for i in 0..40u32 {
        let name = format!("space-{i}");
        let ks = engine.keyspace(&name).unwrap();
        let records = model.entry(name).or_default();
        for key in 0..(i * 7) {
            let key = format!("{key:05}").into_bytes();
            let value = format!("{i}:{key:?}").into_bytes();
            ks.put(&key, &value).unwrap();
            records.insert(key, value);
        }
        if i % 9 == 0 {
            engine.flush().unwrap();
        }
    }
    engine.drop_keyspace("space-3").unwrap();
    engine.drop_keyspace("space-30").unwrap();
    model.remove("space-30");
    let again = engine.keyspace("space-3").unwrap();
    again.put(b"fresh", b"1").unwrap();
    model.insert(
        "space-3".into(),
        Model::from([(b"fresh".to_vec(), b"1".to_vec())]),
    );
    let ids: BTreeMap<String, u32> = engine
        .keyspaces()
        .into_iter()
        .map(|name| {
            let id = engine.keyspace(&name).unwrap().id();
            (name, id)
        })
        .collect();
    assert_eq!(ids["space-3"], 40);
    engine.checkpoint().unwrap();
    // Written after the checkpoint, and lost with the engine.
    again.put(b"later", b"2").unwrap();
    engine.keyspace("late").unwrap();
    engine.close().unwrap();

    let engine = Engine::open(&scratch.0, small()).unwrap();
    assert_eq!(
        engine.keyspaces(),
        model.keys().cloned().collect::<Vec<_>>()
    );
    let mut reader = engine.reader();
    for (name, records) in &model {
        let ks = engine.keyspace(name).unwrap();
        assert_eq!(ks.id(), ids[name], "{name}");
        assert_eq!(&contents(&mut reader, &ks), records, "{name}");
    }
    drop(reader);
    assert_eq!(engine.keyspace("after").unwrap().id(), 41);
    engine.close().unwrap();
}

/// One hot keyspace takes most writes while cold ones get a few records per
/// flush: every cold keyspace's level 0 is compacted, and writers finish.
#[test]
fn compaction_serves_cold_keyspaces_beside_a_hot_one() {
    let engine = Engine::in_memory(small()).unwrap();
    let hot = engine.keyspace("hot").unwrap();
    let cold: Vec<Keyspace> = (0..6)
        .map(|i| engine.keyspace(&format!("cold{i}")).unwrap())
        .collect();
    for round in 0..60u32 {
        for key in 0..200u32 {
            hot.put(&(round * 200 + key).to_be_bytes(), &[7; 48])
                .unwrap();
        }
        for ks in &cold {
            ks.put(&round.to_be_bytes(), &[1; 16]).unwrap();
        }
        engine.flush().unwrap();
    }
    engine.wait_for_compaction().unwrap();
    let details = engine.core.keyspace_stats();
    assert_eq!(details.len(), 7);
    for detail in &details {
        let trigger = crate::engine::COMPACTION_TRIGGER;
        assert!(detail.level0_flushes < trigger, "{detail:?}");
        assert!(detail.runs > 0, "{detail:?}");
    }
    let mut reader = engine.reader();
    for ks in &cold {
        assert_eq!(contents(&mut reader, ks).len(), 60);
    }
    assert_eq!(contents(&mut reader, &hot).len(), 12_000);
    assert!(engine.background_status().unwrap().compactions > 0);
}

/// 4096 live keyspaces, then a refusal; a drop makes room. Names follow the
/// rules, and a refused name creates nothing.
#[test]
fn the_live_keyspace_limit_and_the_name_rules_hold() {
    let engine = Engine::in_memory(manual()).unwrap();
    for i in 0..4096 {
        engine.keyspace(&format!("k{i}")).unwrap();
    }
    let error = engine.keyspace("one-more").unwrap_err();
    assert_eq!(error.kind(), ErrorKind::InvalidInput);
    assert!(error.to_string().contains("4096"), "{error}");
    // An existing name is still opened at the limit.
    assert_eq!(engine.keyspace("k7").unwrap().name(), "k7");
    assert!(engine.drop_keyspace("k0").unwrap());
    assert_eq!(engine.keyspace("one-more").unwrap().id(), 4096);
    assert_eq!(engine.stats().keyspaces.len(), 4096);
    let engine = Engine::in_memory(manual()).unwrap();
    for name in ["A-z_0.9", "-", ".", "_", &"x".repeat(64)] {
        engine.keyspace(name).unwrap();
    }
    for name in ["", "a b", "é", "a/b", &"x".repeat(65)] {
        assert_eq!(
            engine.keyspace(name).unwrap_err().kind(),
            ErrorKind::InvalidInput
        );
    }
    assert_eq!(engine.keyspaces().len(), 5);
}

/// What a snapshot held: each keyspace it may read and its records, if live.
type Seen = Vec<(Keyspace, Option<Model>)>;

/// One step of the model test.
enum Step {
    Put,
    Delete,
    Batch,
    Drop,
    Snapshot,
    Check,
    Checkpoint,
}

/// Random puts, deletes, cross-keyspace batches, drops, recreations,
/// snapshots and checkpoint/recover cycles over several keyspaces, checked
/// against a model, including snapshots read long after they were taken.
fn keyspace_model(seed: u64, steps: usize) {
    let scratch = Scratch::new(&format!("model-{seed}"));
    let options = small();
    let mut engine = Engine::open(&scratch.0, options).unwrap();
    let mut rng = Rng(seed);
    let names = ["a", "b", "c", "d", "e"];
    let mut model: BTreeMap<&str, Model> = BTreeMap::new();
    let mut handles: BTreeMap<&str, Keyspace> = BTreeMap::new();
    let mut snapshots: Vec<(crate::Snapshot, Seen)> = Vec::new();
    let key = |rng: &mut Rng| format!("key{:03}", rng.below(200)).into_bytes();
    for step in 0..steps {
        let name = names[rng.below(names.len())];
        let ks = handles
            .entry(name)
            .or_insert_with(|| engine.keyspace(name).unwrap())
            .clone();
        model.entry(name).or_default();
        let choice = match rng.below(100) {
            0..40 => Step::Put,
            40..55 => Step::Delete,
            55..80 => Step::Batch,
            80..83 => Step::Drop,
            83..88 => Step::Snapshot,
            88..98 => Step::Check,
            _ => Step::Checkpoint,
        };
        match choice {
            Step::Put => {
                let (key, value) = (key(&mut rng), step.to_be_bytes().to_vec());
                ks.put(&key, &value).unwrap();
                model.get_mut(name).unwrap().insert(key, value);
            }
            Step::Delete => {
                let key = key(&mut rng);
                ks.delete(&key).unwrap();
                model.get_mut(name).unwrap().remove(&key);
            }
            Step::Batch => {
                let mut batch = WriteBatch::new();
                let mut changes = Vec::new();
                let len = if rng.below(20) == 0 {
                    400
                } else {
                    1 + rng.below(8)
                };
                for _ in 0..len {
                    let other = names[rng.below(names.len())];
                    let target = handles
                        .entry(other)
                        .or_insert_with(|| engine.keyspace(other).unwrap())
                        .clone();
                    model.entry(other).or_default();
                    let key = key(&mut rng);
                    if rng.below(4) == 0 {
                        batch.delete(&target, &key);
                        changes.push((other, key, None));
                    } else {
                        let value = [step.to_be_bytes(), changes.len().to_be_bytes()].concat();
                        batch.put(&target, &key, &value);
                        changes.push((other, key, Some(value)));
                    }
                }
                engine.write(&batch).unwrap();
                for (other, key, value) in changes {
                    let records = model.get_mut(other).unwrap();
                    match value {
                        Some(value) => records.insert(key, value),
                        None => records.remove(&key),
                    };
                }
            }
            Step::Drop => {
                assert!(engine.drop_keyspace(name).unwrap());
                assert_eq!(
                    ks.put(b"x", b"y").unwrap_err().kind(),
                    ErrorKind::KeyspaceDropped
                );
                handles.remove(name);
                model.remove(name);
            }
            Step::Snapshot => {
                let snapshot = engine.snapshot();
                let seen = handles
                    .iter()
                    .map(|(name, ks)| (ks.clone(), model.get(name).cloned()))
                    .collect();
                snapshots.push((snapshot, seen));
                if snapshots.len() > 4 {
                    snapshots.remove(rng.below(snapshots.len()));
                }
            }
            Step::Check => {
                let mut reader = engine.reader();
                for (name, records) in &model {
                    let ks = handles
                        .entry(name)
                        .or_insert_with(|| engine.keyspace(name).unwrap());
                    assert_eq!(&contents(&mut reader, ks), records, "step {step}");
                }
                drop(reader);
                for (snapshot, seen) in &snapshots {
                    let mut reader = snapshot.reader();
                    for (ks, records) in seen {
                        assert_eq!(
                            contents(&mut reader, ks),
                            records.clone().unwrap_or_default(),
                            "step {step}"
                        );
                    }
                }
                let stats = engine.stats();
                assert_eq!(
                    stats
                        .keyspaces
                        .iter()
                        .map(|ks| ks.name.as_str())
                        .collect::<Vec<_>>(),
                    model.keys().copied().collect::<Vec<_>>()
                );
            }
            Step::Checkpoint => {
                engine.checkpoint().unwrap();
                let checkpointed = model.clone();
                if rng.below(2) == 0 {
                    // Writes after the checkpoint are lost on close.
                    ks.put(b"lost", b"lost").unwrap();
                    snapshots.clear();
                    handles.clear();
                    engine.close().unwrap();
                    engine = Engine::open(&scratch.0, options).unwrap();
                    model = checkpointed;
                    let mut names: Vec<&str> = model.keys().copied().collect();
                    names.sort_unstable();
                    assert_eq!(engine.keyspaces(), names);
                }
            }
        }
    }
    engine.close().unwrap();
}

#[test]
fn a_keyspace_model_agrees_through_drops_snapshots_and_recovery() {
    for seed in [1, 2] {
        keyspace_model(seed, 1500);
    }
}

#[test]
#[ignore = "long keyspace model campaign"]
fn long_keyspace_model() {
    for seed in 3..9 {
        keyspace_model(seed, 3000);
    }
}
