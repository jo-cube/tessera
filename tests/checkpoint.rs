//! Checkpoints and recovery through the public API: what a checkpoint holds,
//! what recovery accepts and rejects, and what a failed checkpoint leaves.
//! Interrupted publication is tested beside the code, where crashes can be
//! injected. Files go to a fresh directory under the system temporary
//! directory, which the bounded runner makes a small tmpfs.

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;

use tessera::engine::{
    CheckpointError, CheckpointOptions, CompactionError, CompactionOptions, Compression, Damage,
    Engine, EngineOptions, EngineWorkspace, MergeWorkspace, RecoveryError, Snapshot,
    SnapshotLimits,
};

type Model = BTreeMap<Vec<u8>, Vec<u8>>;

struct Scratch(PathBuf);

impl Scratch {
    fn new(name: &str) -> Self {
        let path = std::env::temp_dir().join(format!("tessera-{name}-{}", std::process::id()));
        let _ = fs::remove_dir_all(&path);
        Self(path)
    }
}

impl Drop for Scratch {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

fn contents(engine: &Engine, prefix: &[u8]) -> Model {
    let mut workspace = EngineWorkspace::new();
    let guard = engine.read();
    let mut scan = guard.scan_prefix(prefix, &mut workspace);
    let mut out = Model::new();
    while let Some(record) = scan.next_record().unwrap() {
        out.insert(record.key.to_vec(), record.value.to_vec());
    }
    out
}

fn get(engine: &Engine, key: &[u8]) -> Option<Vec<u8>> {
    let mut workspace = EngineWorkspace::new();
    engine
        .get(key, &mut workspace)
        .unwrap()
        .map(|found| found.value.to_vec())
}

fn checkpoint_dirs(dir: &Path) -> Vec<String> {
    let mut names: Vec<String> = fs::read_dir(dir)
        .unwrap()
        .map(|entry| entry.unwrap().file_name().into_string().unwrap())
        .filter(|name| name != "LOCK")
        .collect();
    names.sort();
    names
}

fn compact_all(engine: &Engine) {
    let options = CompactionOptions {
        max_overlap: 1,
        ..CompactionOptions::default()
    };
    let mut workspace = MergeWorkspace::new();
    while engine.compact(options, &mut workspace).unwrap().is_some() {}
}

/// State in runs, in a frozen table left by a refused flush, and in the
/// active table, with binary and empty keys, a large value, and deletes that
/// hide older values in runs.
fn varied_engine() -> (Engine, Model) {
    let engine = Engine::new(EngineOptions {
        target_run_len: 4096,
        ..EngineOptions::default()
    });
    let mut model = Model::new();
    let mut put = |key: Vec<u8>, value: Vec<u8>| {
        engine.put(&key, &value).unwrap();
        model.insert(key, value);
    };
    for i in 0..2_000u32 {
        put(i.to_be_bytes().to_vec(), format!("v{i}").into_bytes());
    }
    put(Vec::new(), b"empty key".to_vec());
    put(vec![0xFF; 40], vec![0xAB; 100_000]);
    put(vec![0], Vec::new());
    engine.flush().unwrap();
    for i in (0..2_000u32).step_by(3) {
        engine.delete(&i.to_be_bytes()).unwrap();
        model.remove(&i.to_be_bytes()[..]);
    }
    engine.flush().unwrap();
    for i in (1..2_000u32).step_by(7) {
        let (key, value) = (i.to_be_bytes().to_vec(), b"newer".to_vec());
        engine.put(&key, &value).unwrap();
        model.insert(key, value);
    }
    engine.delete(&[0xFF; 40]).unwrap();
    model.remove(&[0xFF; 40][..]);
    (engine, model)
}

#[test]
fn recovery_reproduces_the_checkpointed_state_and_continues_its_sequence() {
    let scratch = Scratch::new("round-trip");
    let (engine, model) = varied_engine();
    let report = engine
        .checkpoint(&scratch.0, CheckpointOptions::default())
        .unwrap();
    assert_eq!(report.last_seq, engine.stats().last_seq);
    assert_eq!(report.records as usize, model.len());
    assert!(report.runs > 1, "split at the target run length");
    assert_eq!(
        checkpoint_dirs(&scratch.0),
        ["CURRENT", "ckpt-00000000000000000001"]
    );

    let recovered = Engine::recover(&scratch.0, EngineOptions::default()).unwrap();
    assert_eq!(contents(&recovered, b""), model);
    assert_eq!(
        contents(&recovered, &[0, 0, 3]),
        contents(&engine, &[0, 0, 3])
    );
    for key in [
        &b""[..],
        &[0],
        &[0xFF; 40],
        &3u32.to_be_bytes(),
        &4u32.to_be_bytes(),
    ] {
        assert_eq!(get(&recovered, key), model.get(key).cloned(), "{key:?}");
    }
    let stats = recovered.stats();
    assert_eq!(stats.last_seq, report.last_seq);
    assert!(stats.filter_bytes > 0, "filters are rebuilt");
    assert_eq!(recovered.put(b"next", b"1").unwrap(), report.last_seq + 1);

    // A larger configured sequence number wins; a smaller one never does.
    let ahead = Engine::recover(
        &scratch.0,
        EngineOptions {
            last_seq: report.last_seq + 100,
            ..EngineOptions::default()
        },
    )
    .unwrap();
    assert_eq!(ahead.put(b"k", b"v").unwrap(), report.last_seq + 101);

    // The recovered engine flushes, compacts and checkpoints like any other.
    recovered.delete(&4u32.to_be_bytes()).unwrap();
    recovered.flush().unwrap();
    compact_all(&recovered);
    let second = recovered
        .checkpoint(&scratch.0, CheckpointOptions::default())
        .unwrap();
    assert_eq!((second.generation, second.reclaimed), (2, 1));
    assert_eq!(
        checkpoint_dirs(&scratch.0),
        ["CURRENT", "ckpt-00000000000000000002"]
    );
    let again = Engine::recover(&scratch.0, EngineOptions::default()).unwrap();
    assert_eq!(contents(&again, b""), contents(&recovered, b""));
    assert_eq!(get(&again, &4u32.to_be_bytes()), None);
}

/// Each block records its codec, so the configured compression decides only
/// what new runs use: a checkpoint written under one setting recovers under
/// any other, and compaction rewrites what it merges.
#[test]
fn checkpoints_recover_under_any_compression() {
    const ALL: [Compression; 3] = [
        Compression::Zstd,
        Compression::Lz4,
        Compression::Uncompressed,
    ];
    let with = |compression| EngineOptions {
        target_run_len: 4096,
        compression,
        ..EngineOptions::default()
    };
    let mut run_bytes = Vec::new();
    for written in ALL {
        let scratch = Scratch::new(&format!("compression-{written:?}"));
        let engine = Engine::new(with(written));
        let mut model = Model::new();
        for i in 0..3_000u32 {
            let (key, value) = (i.to_be_bytes().to_vec(), format!("value {} of {i}", i % 10));
            engine.put(&key, value.as_bytes()).unwrap();
            model.insert(key, value.into_bytes());
        }
        engine.flush().unwrap();
        for i in (0..3_000u32).step_by(3) {
            engine.delete(&i.to_be_bytes()).unwrap();
            model.remove(&i.to_be_bytes()[..]);
        }
        engine.flush().unwrap();
        assert_eq!(contents(&engine, b""), model);
        run_bytes.push(engine.stats().run_bytes);
        engine
            .checkpoint(&scratch.0, CheckpointOptions::default())
            .unwrap();

        for read in ALL {
            let recovered = Engine::recover(&scratch.0, with(read)).unwrap();
            assert_eq!(contents(&recovered, b""), model, "{written:?} as {read:?}");
            assert_eq!(
                get(&recovered, &4u32.to_be_bytes()),
                model.get(&4u32.to_be_bytes()[..]).cloned()
            );
            assert_eq!(get(&recovered, &3u32.to_be_bytes()), None);
            // A newer run in the configured codec beside the recovered ones:
            // overwrites, a delete and a key a delete had hidden.
            let mut newer = model.clone();
            for i in [1u32, 3, 4, 2_999] {
                let (key, value) = (i.to_be_bytes().to_vec(), format!("newer {i}"));
                recovered.put(&key, value.as_bytes()).unwrap();
                newer.insert(key, value.into_bytes());
            }
            recovered.delete(&2u32.to_be_bytes()).unwrap();
            newer.remove(&2u32.to_be_bytes()[..]);
            recovered.flush().unwrap();
            assert!(recovered.stats().runs > 1);
            let snapshot = recovered.snapshot().unwrap();
            // Compaction writes the merged runs with the configured codec;
            // the snapshot keeps reading the runs it replaced.
            compact_all(&recovered);
            assert_eq!(contents(&recovered, b""), newer, "{written:?} as {read:?}");
            let mut workspace = EngineWorkspace::new();
            let mut scan = snapshot.scan_prefix(b"", &mut workspace);
            let mut seen = Model::new();
            while let Some(record) = scan.next_record().unwrap() {
                seen.insert(record.key.to_vec(), record.value.to_vec());
            }
            assert_eq!(seen, newer, "snapshot, {written:?} as {read:?}");
        }
    }
    assert!(
        run_bytes[0] < run_bytes[1] && run_bytes[1] < run_bytes[2],
        "{run_bytes:?}"
    );
}

/// After a switch to a less compact codec, a compaction that would grow the
/// runs past `max_run_bytes` is refused and leaves the runs, their contents
/// and a snapshot's view as they were; lifting the limit lets it through.
#[test]
fn a_compaction_refused_after_a_codec_switch_changes_nothing() {
    let scratch = Scratch::new("codec-switch-limit");
    let with = |compression| EngineOptions {
        target_run_len: 4096,
        compression,
        ..EngineOptions::default()
    };
    let engine = Engine::new(with(Compression::Zstd));
    let mut model = Model::new();
    for i in 0..3_000u32 {
        let (key, value) = (i.to_be_bytes().to_vec(), format!("value {} of {i}", i % 10));
        engine.put(&key, value.as_bytes()).unwrap();
        model.insert(key, value.into_bytes());
    }
    engine.flush().unwrap();
    engine
        .checkpoint(&scratch.0, CheckpointOptions::default())
        .unwrap();

    // Recovered Zstandard runs under a newer uncompressed run of overwrites.
    let recovered = Engine::recover(&scratch.0, with(Compression::Uncompressed)).unwrap();
    for i in (0..3_000u32).step_by(5) {
        let (key, value) = (i.to_be_bytes().to_vec(), format!("newer {i}"));
        recovered.put(&key, value.as_bytes()).unwrap();
        model.insert(key, value.into_bytes());
    }
    recovered.flush().unwrap();
    let snapshot = recovered.snapshot().unwrap();
    let before = recovered.stats();
    assert!(before.max_overlap > 1, "{before:?}");
    recovered.set_run_limits(usize::MAX, before.run_bytes + before.filter_bytes);
    let options = CompactionOptions {
        max_overlap: 1,
        ..CompactionOptions::default()
    };
    let refused = recovered.compact(options, &mut MergeWorkspace::new());
    assert!(
        matches!(refused, Err(CompactionError::RunLimit(_))),
        "{refused:?}"
    );
    assert_eq!(recovered.stats(), before);
    assert_eq!(contents(&recovered, b""), model);
    let snapshot_contents = |snapshot: &Snapshot| {
        let mut workspace = EngineWorkspace::new();
        let mut scan = snapshot.scan_prefix(b"", &mut workspace);
        let mut seen = Model::new();
        while let Some(record) = scan.next_record().unwrap() {
            seen.insert(record.key.to_vec(), record.value.to_vec());
        }
        seen
    };
    assert_eq!(snapshot_contents(&snapshot), model);

    recovered.set_run_limits(usize::MAX, usize::MAX);
    compact_all(&recovered);
    assert!(recovered.stats().run_bytes > before.run_bytes);
    assert_eq!(contents(&recovered, b""), model);
    assert_eq!(snapshot_contents(&snapshot), model);
}

#[test]
fn an_empty_engine_checkpoints_and_recovers_empty_but_nothing_is_not_a_checkpoint() {
    let scratch = Scratch::new("empty");
    assert!(matches!(
        Engine::recover(&scratch.0, EngineOptions::default()),
        Err(RecoveryError::NoCheckpoint(_))
    ));
    fs::create_dir_all(&scratch.0).unwrap();
    assert!(matches!(
        Engine::recover(&scratch.0, EngineOptions::default()),
        Err(RecoveryError::NoCheckpoint(_))
    ));
    let engine = Engine::new(EngineOptions {
        last_seq: 41,
        ..EngineOptions::default()
    });
    let report = engine
        .checkpoint(&scratch.0, CheckpointOptions::default())
        .unwrap();
    assert_eq!((report.records, report.runs, report.last_seq), (0, 0, 41));
    let recovered = Engine::recover(&scratch.0, EngineOptions::default()).unwrap();
    assert!(contents(&recovered, b"").is_empty());
    assert_eq!(recovered.put(b"a", b"1").unwrap(), 42);
}

/// Flushes, waiting for the worker whenever the run limit refuses.
fn flush(engine: &Engine) {
    for _ in 0..1_000 {
        if engine.flush().is_ok() {
            return;
        }
        engine.wait_for_compaction().unwrap();
    }
    panic!("compaction never made room");
}

/// Writes, flushes and compactions continue during the checkpoint, which
/// holds exactly the writes up to its ceiling.
#[test]
fn a_checkpoint_beside_writes_flushes_and_compactions_holds_one_state() {
    let scratch = Scratch::new("concurrent");
    let engine = Engine::new(EngineOptions {
        memtable_budget: 256 << 10,
        target_run_len: 64 << 10,
        background_compaction: Some(CompactionOptions::default()),
        ..EngineOptions::default()
    });
    let done = AtomicBool::new(false);
    let mut log: Vec<(u64, Vec<u8>, Option<Vec<u8>>)> = Vec::new();
    let mut reports = Vec::new();
    thread::scope(|scope| {
        let writer = scope.spawn(|| {
            let mut log = Vec::new();
            let mut i = 0u64;
            while !done.load(Ordering::Relaxed) || i < 50_000 {
                let key = (i.wrapping_mul(0x9E37_79B9) % 5_000).to_be_bytes().to_vec();
                let seq = if i % 5 == 4 {
                    let seq = engine.delete(&key);
                    seq.map(|seq| (seq, None))
                } else {
                    let value = i.to_le_bytes().to_vec();
                    engine.put(&key, &value).map(|seq| (seq, Some(value)))
                };
                match seq {
                    Ok((seq, value)) => log.push((seq, key, value)),
                    Err(_) => {
                        flush(&engine);
                        continue;
                    }
                }
                if i.is_multiple_of(2_000) {
                    flush(&engine);
                }
                i += 1;
            }
            log
        });
        for _ in 0..3 {
            reports.push(
                engine
                    .checkpoint(&scratch.0, CheckpointOptions::default())
                    .unwrap(),
            );
        }
        done.store(true, Ordering::Relaxed);
        log = writer.join().unwrap();
    });
    let last = reports.last().unwrap();
    let mut model = Model::new();
    for (seq, key, value) in &log {
        if *seq > last.last_seq {
            continue;
        }
        match value {
            Some(value) => model.insert(key.clone(), value.clone()),
            None => model.remove(key),
        };
    }
    let recovered = Engine::recover(&scratch.0, EngineOptions::default()).unwrap();
    assert_eq!(contents(&recovered, b""), model);
    assert_eq!(recovered.stats().last_seq, last.last_seq);
    assert!(
        reports
            .windows(2)
            .all(|pair| pair[0].last_seq <= pair[1].last_seq)
    );
    assert_eq!(checkpoint_dirs(&scratch.0).len(), 2);
}

#[test]
fn damaged_or_missing_files_are_rejected_and_never_replaced_by_an_empty_engine() {
    let scratch = Scratch::new("damage");
    let (engine, _) = varied_engine();
    engine
        .checkpoint(&scratch.0, CheckpointOptions::default())
        .unwrap();
    let ckpt = scratch.0.join("ckpt-00000000000000000001");
    let pristine: Vec<(PathBuf, Vec<u8>)> = fs::read_dir(&ckpt)
        .unwrap()
        .map(|entry| {
            let path = entry.unwrap().path();
            let bytes = fs::read(&path).unwrap();
            (path, bytes)
        })
        .chain([(
            scratch.0.join("CURRENT"),
            fs::read(scratch.0.join("CURRENT")).unwrap(),
        )])
        .collect();
    let restore = || {
        for (path, bytes) in &pristine {
            fs::write(path, bytes).unwrap();
        }
    };
    let damage = |path: &Path| match Engine::recover(&scratch.0, EngineOptions::default()) {
        Err(RecoveryError::Corrupt { path: at, damage }) => {
            assert_eq!(at, path);
            damage
        }
        other => panic!("{:?}", other.map(|_| ())),
    };
    let run = ckpt.join("run-000001");
    let manifest = ckpt.join("MANIFEST");
    let current = scratch.0.join("CURRENT");

    let mut bytes = fs::read(&run).unwrap();
    let middle = bytes.len() / 2;
    bytes[middle] ^= 0x10;
    fs::write(&run, &bytes).unwrap();
    assert_eq!(damage(&run), Damage::Checksum);
    restore();

    fs::write(&run, &bytes[..middle]).unwrap();
    assert!(matches!(damage(&run), Damage::Length { .. }));
    restore();

    fs::remove_file(&run).unwrap();
    assert_eq!(damage(&run), Damage::Missing);
    restore();

    let bytes = fs::read(&manifest).unwrap();
    fs::write(&manifest, &bytes[..bytes.len() - 3]).unwrap();
    assert!(matches!(damage(&manifest), Damage::Length { .. }));
    let mut flipped = bytes.clone();
    flipped[30] ^= 1;
    fs::write(&manifest, &flipped).unwrap();
    assert_eq!(damage(&manifest), Damage::Checksum);
    fs::remove_file(&manifest).unwrap();
    assert_eq!(damage(&manifest), Damage::Missing);
    restore();

    fs::write(&current, b"not a checkpoint").unwrap();
    assert!(matches!(damage(&current), Damage::Length { .. }));
    let mut bytes = pristine.last().unwrap().1.clone();
    bytes[0] ^= 1;
    fs::write(&current, &bytes).unwrap();
    assert_eq!(damage(&current), Damage::BadMagic);
    restore();

    // Recovery changed nothing, and the restored checkpoint recovers.
    Engine::recover(&scratch.0, EngineOptions::default()).unwrap();
}

#[test]
fn a_refused_or_failed_checkpoint_leaves_the_engine_and_the_previous_checkpoint() {
    let scratch = Scratch::new("refused");
    let (engine, model) = varied_engine();
    let first = engine
        .checkpoint(&scratch.0, CheckpointOptions::default())
        .unwrap();
    engine.put(b"later", b"x").unwrap();
    let before = engine.stats();

    let error = engine
        .checkpoint(&scratch.0, CheckpointOptions { max_bytes: 10_000 })
        .unwrap_err();
    assert!(
        matches!(error, CheckpointError::DiskLimit { bytes, max_bytes: 10_000 } if bytes > 10_000)
    );
    assert_eq!(engine.stats(), before);
    assert_eq!(
        checkpoint_dirs(&scratch.0),
        ["CURRENT", "ckpt-00000000000000000001"]
    );
    let recovered = Engine::recover(&scratch.0, EngineOptions::default()).unwrap();
    assert_eq!(contents(&recovered, b""), model);
    assert_eq!(recovered.stats().last_seq, first.last_seq);

    // Without a snapshot to take, nothing is written.
    let limited = Engine::new(EngineOptions {
        snapshots: SnapshotLimits {
            max_live: 0,
            ..SnapshotLimits::default()
        },
        ..EngineOptions::default()
    });
    assert!(matches!(
        limited.checkpoint(&scratch.0, CheckpointOptions::default()),
        Err(CheckpointError::Snapshot(_))
    ));
    assert_eq!(
        checkpoint_dirs(&scratch.0),
        ["CURRENT", "ckpt-00000000000000000001"]
    );

    // The next checkpoint takes a new generation and succeeds.
    let next = engine
        .checkpoint(&scratch.0, CheckpointOptions::default())
        .unwrap();
    assert!(next.generation > first.generation);
    assert_eq!(
        get(
            &Engine::recover(&scratch.0, EngineOptions::default()).unwrap(),
            b"later"
        ),
        Some(b"x".to_vec())
    );
}

#[test]
fn recovery_refuses_more_runs_than_the_options_allow() {
    let scratch = Scratch::new("limits");
    let (engine, _) = varied_engine();
    let report = engine
        .checkpoint(&scratch.0, CheckpointOptions::default())
        .unwrap();
    let error = Engine::recover(
        &scratch.0,
        EngineOptions {
            max_runs: report.runs - 1,
            ..EngineOptions::default()
        },
    );
    assert!(matches!(error, Err(RecoveryError::RunLimit(_))));
    // Refused from the manifest's lengths, before reading a run.
    let error = Engine::recover(
        &scratch.0,
        EngineOptions {
            max_run_bytes: report.largest_run_bytes,
            ..EngineOptions::default()
        },
    );
    assert!(matches!(error, Err(RecoveryError::RunLimit(_))));
}

/// A file longer than its manifest entry or format allows is refused before
/// it is read into memory: here sparse files far larger than the memory the
/// tests run in.
#[test]
fn oversized_files_are_refused_before_they_are_read() {
    let scratch = Scratch::new("oversized");
    let (engine, _) = varied_engine();
    engine
        .checkpoint(&scratch.0, CheckpointOptions::default())
        .unwrap();
    let generation = scratch.0.join(format!("ckpt-{:020}", 1));
    for path in [
        scratch.0.join("CURRENT"),
        generation.join("MANIFEST"),
        generation.join("run-000000"),
    ] {
        let original = fs::read(&path).unwrap();
        fs::OpenOptions::new()
            .write(true)
            .open(&path)
            .unwrap()
            .set_len(64 << 30)
            .unwrap();
        let error = Engine::recover(&scratch.0, EngineOptions::default());
        assert!(
            matches!(
                error,
                Err(RecoveryError::Corrupt {
                    damage: Damage::Length {
                        found: 68_719_476_736,
                        ..
                    },
                    ..
                })
            ),
            "{}: {error:?}",
            path.display()
        );
        fs::write(&path, original).unwrap();
    }
    Engine::recover(&scratch.0, EngineOptions::default()).unwrap();
}
