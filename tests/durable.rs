//! Durable engines through the public API: what `open` recovers from a
//! checkpoint and the log, and what it rejects (a killed process is in `tests/process.rs`).
//! Injected append and checkpoint failures are tested beside the code.
//! Files go to fresh directories under the system temporary directory, which
//! the bounded runner makes a small tmpfs.

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::thread;

use tessera::engine::{
    CheckpointError, CheckpointOptions, CompactionOptions, DurableOptions, Engine, EngineOptions,
    EngineWorkspace, LogDamage, MAX_KEY_LEN, MAX_VALUE_LEN, RecoveryError, UpdateError, Verdict,
    WriteError,
};

type Model = BTreeMap<Vec<u8>, Vec<u8>>;

struct Scratch(PathBuf);

impl Scratch {
    fn new(name: &str) -> Self {
        let path =
            std::env::temp_dir().join(format!("tessera-durable-{name}-{}", std::process::id()));
        let _ = fs::remove_dir_all(&path);
        Self(path)
    }
}

impl Drop for Scratch {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

fn contents(engine: &Engine) -> Model {
    let mut workspace = EngineWorkspace::new();
    let guard = engine.read();
    let mut scan = guard.scan(&mut workspace);
    let mut out = Model::new();
    while let Some(record) = scan.next_record().unwrap() {
        out.insert(record.key.to_vec(), record.value.to_vec());
    }
    out
}

fn segments(dir: &Path) -> Vec<PathBuf> {
    let mut found: Vec<PathBuf> = fs::read_dir(dir)
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .filter(|path| {
            path.file_name()
                .unwrap()
                .to_str()
                .unwrap()
                .starts_with("wal-")
        })
        .collect();
    found.sort();
    found
}

fn small() -> DurableOptions {
    DurableOptions {
        segment_bytes: 4096,
        ..DurableOptions::default()
    }
}

fn open(dir: &Path) -> Engine {
    Engine::open(dir, EngineOptions::default(), small())
        .unwrap()
        .0
}

/// Writes (or deletes, without a value), waiting for compaction when the
/// run limits refuse the flush the write needs.
fn write(engine: &Engine, key: &[u8], value: Option<&[u8]>) -> u64 {
    for _ in 0..100 {
        let result = match value {
            Some(value) => engine.put(key, value),
            None => engine.delete(key),
        };
        match result {
            Ok(seq) => return seq,
            Err(UpdateError::FlushRefused(_)) => engine.wait_for_compaction().unwrap(),
            Err(error) => panic!("{error}"),
        }
    }
    panic!("compaction made no room");
}

#[test]
fn reopening_recovers_puts_overwrites_deletes_and_reinsertions_and_continues_the_sequence() {
    let scratch = Scratch::new("round-trip");
    let engine = Engine::create(&scratch.0, EngineOptions::default(), small()).unwrap();
    let mut model = Model::new();
    let keys: Vec<Vec<u8>> = vec![
        b"".to_vec(),
        vec![0],
        vec![0xff; 40],
        b"apple".to_vec(),
        (0..=255).collect(),
    ];
    let mut seq = 0;
    for round in 0..40u32 {
        for (index, key) in keys.iter().enumerate() {
            seq += 1;
            if (round as usize + index).is_multiple_of(3) {
                assert_eq!(engine.delete(key).unwrap(), seq);
                model.remove(key);
            } else {
                let value = if round.is_multiple_of(5) {
                    Vec::new()
                } else {
                    vec![round as u8; round as usize * 7]
                };
                assert_eq!(engine.put(key, &value).unwrap(), seq);
                model.insert(key.clone(), value);
            }
        }
        if round == 20 {
            engine.flush().unwrap();
        }
    }
    assert!(segments(&scratch.0).len() > 2);
    let status = engine.log_status().unwrap();
    assert_eq!((status.batches, status.syncs), (seq, seq));
    drop(engine);

    for reopen in 0..2 {
        let (engine, report) = Engine::open(&scratch.0, EngineOptions::default(), small()).unwrap();
        assert_eq!(report.checkpoint_seq, 0);
        assert_eq!(report.last_seq, seq);
        assert_eq!(report.replayed, seq);
        assert_eq!(report.torn_tail, None);
        assert_eq!(contents(&engine), model, "reopen {reopen}");
        assert_eq!(engine.stats().last_seq, seq);
        // Continues the sequence, and a reinsertion after a delete survives.
        seq += 1;
        assert_eq!(engine.put(b"apple", b"again").unwrap(), seq);
        model.insert(b"apple".to_vec(), b"again".to_vec());
        drop(engine);
    }
}

#[test]
fn the_largest_key_and_value_are_logged_and_recovered() {
    let scratch = Scratch::new("largest");
    let options = EngineOptions {
        memtable_budget: MAX_VALUE_LEN + (1 << 20),
        ..EngineOptions::default()
    };
    let engine = Engine::create(&scratch.0, options, DurableOptions::default()).unwrap();
    let key = vec![0xab; MAX_KEY_LEN];
    let value: Vec<u8> = (0..MAX_VALUE_LEN).map(|at| (at % 251) as u8).collect();
    engine.put(&key, &value).unwrap();
    engine.put(b"after", b"").unwrap();
    // One past each limit is refused before anything is logged.
    let bytes = engine.log_status().unwrap().bytes;
    assert!(engine.put(&vec![0; MAX_KEY_LEN + 1], b"").is_err());
    assert_eq!(engine.log_status().unwrap().bytes, bytes);
    drop(engine);
    let (engine, report) = Engine::open(&scratch.0, options, DurableOptions::default()).unwrap();
    assert_eq!(report.last_seq, 2);
    let mut workspace = EngineWorkspace::new();
    assert_eq!(
        engine.get(&key, &mut workspace).unwrap().unwrap().value,
        &value[..]
    );
}

#[test]
fn checkpoints_prune_covered_segments_and_recovery_spans_checkpoint_and_log() {
    let scratch = Scratch::new("prune");
    let options = EngineOptions {
        target_run_len: 16 << 10,
        ..EngineOptions::default()
    };
    let engine = Engine::create(&scratch.0, options, small()).unwrap();
    let mut model = Model::new();
    let mut checkpoints = Vec::new();
    for round in 0..4u32 {
        for at in 0..500u32 {
            let key = (at * 7 % 300).to_be_bytes().to_vec();
            let value = format!("{round}-{at}").into_bytes();
            engine.put(&key, &value).unwrap();
            model.insert(key, value);
        }
        let before = segments(&scratch.0).len();
        let report = engine
            .durable_checkpoint(CheckpointOptions::default())
            .unwrap();
        assert!(report.pruned_segments > 0, "{report:?}");
        assert_eq!(segments(&scratch.0).len(), before - report.pruned_segments);
        checkpoints.push(report);
    }
    // Records after the last checkpoint, across several segments.
    for at in 0..400u32 {
        let key = at.to_be_bytes().to_vec();
        engine.delete(&key).unwrap();
        model.remove(&key);
    }
    let last = engine.stats().last_seq;
    let status = engine.log_status().unwrap();
    assert_eq!(status.segments, segments(&scratch.0).len());
    drop(engine);

    let (engine, report) = Engine::open(&scratch.0, options, small()).unwrap();
    assert_eq!(report.checkpoint_seq, checkpoints.last().unwrap().last_seq);
    assert_eq!(report.last_seq, last);
    assert_eq!(report.replayed, last - report.checkpoint_seq);
    assert_eq!(contents(&engine), model);
}

#[test]
fn checkpoints_beside_writes_flushes_and_compactions_lose_no_acknowledged_write() {
    let scratch = Scratch::new("beside");
    let options = EngineOptions {
        memtable_budget: 256 << 10,
        target_run_len: 64 << 10,
        background_compaction: Some(CompactionOptions::default()),
        ..EngineOptions::default()
    };
    let durable = DurableOptions {
        segment_bytes: 32 << 10,
        ..DurableOptions::default()
    };
    let engine = Engine::create(&scratch.0, options, durable).unwrap();
    let model = thread::scope(|scope| {
        let writer = scope.spawn(|| {
            let mut model = Model::new();
            for at in 0..20_000u32 {
                let key = (at % 3_000).to_be_bytes().to_vec();
                if at.is_multiple_of(11) {
                    write(&engine, &key, None);
                    model.remove(&key);
                } else {
                    let value = at.to_le_bytes().repeat(1 + at as usize % 8);
                    write(&engine, &key, Some(&value));
                    model.insert(key, value);
                }
            }
            model
        });
        let mut taken = 0;
        while !writer.is_finished() {
            engine
                .durable_checkpoint(CheckpointOptions::default())
                .unwrap();
            taken += 1;
        }
        assert!(taken >= 2, "{taken} checkpoints");
        writer.join().unwrap()
    });
    let last = engine.stats().last_seq;
    drop(engine);
    let (engine, report) = Engine::open(&scratch.0, options, durable).unwrap();
    assert_eq!(report.last_seq, last);
    assert_eq!(contents(&engine), model);
}

/// Flips one byte at `at` in `path`.
fn flip(path: &Path, at: u64) {
    let mut bytes = fs::read(path).unwrap();
    bytes[at as usize] ^= 0x40;
    fs::write(path, bytes).unwrap();
}

fn log_damage(result: Result<(Engine, tessera::engine::OpenReport), RecoveryError>) -> LogDamage {
    match result {
        Err(RecoveryError::Log { damage, .. }) => damage,
        Err(error) => panic!("{error}"),
        Ok(_) => panic!("damage was accepted"),
    }
}

/// A database with records in several segments after its checkpoint, and a
/// copy of its files to restore between damages.
fn damaged_fixture(dir: &Path) -> Vec<(PathBuf, Vec<u8>)> {
    let engine = Engine::create(dir, EngineOptions::default(), small()).unwrap();
    for at in 0..300u32 {
        engine.put(&at.to_be_bytes(), &[at as u8; 40]).unwrap();
    }
    engine
        .durable_checkpoint(CheckpointOptions::default())
        .unwrap();
    for at in 0..300u32 {
        engine.put(&at.to_be_bytes(), &[1; 30]).unwrap();
    }
    drop(engine);
    let mut files = Vec::new();
    for path in segments(dir) {
        files.push((path.clone(), fs::read(&path).unwrap()));
    }
    files
}

fn restore(dir: &Path, files: &[(PathBuf, Vec<u8>)]) {
    for path in segments(dir) {
        fs::remove_file(path).unwrap();
    }
    for (path, bytes) in files {
        fs::write(path, bytes).unwrap();
    }
}

#[test]
fn damage_to_acknowledged_history_is_rejected_but_a_torn_tail_is_truncated() {
    let scratch = Scratch::new("damage");
    let dir = &scratch.0;
    let files = damaged_fixture(dir);
    let open = || Engine::open(dir, EngineOptions::default(), small());
    assert!(files.len() >= 4);
    let (middle, middle_bytes) = &files[files.len() / 2];
    let (last, last_bytes) = files.last().unwrap();
    let len = last_bytes.len() as u64;

    // A byte inside a record in the middle of the log.
    flip(middle, middle_bytes.len() as u64 / 2);
    assert!(matches!(
        log_damage(open()),
        LogDamage::Checksum | LogDamage::Frame | LogDamage::Length(_)
    ));
    restore(dir, &files);
    // A complete final record with a bad checksum is damage, not a torn tail.
    flip(last, len - 1);
    assert_eq!(log_damage(open()), LogDamage::Checksum);
    restore(dir, &files);
    // A segment header.
    flip(middle, 20);
    assert_eq!(log_damage(open()), LogDamage::Header);
    restore(dir, &files);
    // A truncated segment that is not the last.
    fs::write(middle, &middle_bytes[..middle_bytes.len() - 5]).unwrap();
    assert_eq!(log_damage(open()), LogDamage::Truncated);
    restore(dir, &files);
    // A missing segment.
    fs::remove_file(middle).unwrap();
    assert!(matches!(log_damage(open()), LogDamage::Missing { .. }));
    restore(dir, &files);
    // No segment at all.
    for (path, _) in &files {
        fs::remove_file(path).unwrap();
    }
    assert_eq!(log_damage(open()), LogDamage::NoSegments);
    restore(dir, &files);

    // An interrupted append: part of the last record, then zeros.
    let (engine, report) = open().unwrap();
    let full = report.last_seq;
    drop(engine);
    for tail in [
        &last_bytes[..last_bytes.len() - 3],
        &[last_bytes.as_slice(), &[0; 50]].concat(),
    ] {
        restore(dir, &files);
        fs::write(last, tail).unwrap();
        // Opening adds a segment; remove it again for the next case.
        let (engine, report) = open().unwrap();
        let torn = report.torn_tail.clone().unwrap();
        assert_eq!(&torn.path, last);
        let lost = if tail.len() as u64 > len { 0 } else { 1 };
        assert_eq!(report.last_seq, full - lost);
        assert_eq!(engine.stats().last_seq, full - lost);
        drop(engine);
        assert_eq!(fs::metadata(last).unwrap().len(), torn.offset);
        let (_, report) = open().unwrap();
        assert_eq!(report.torn_tail, None);
    }
}

/// Every file under `dir` with its bytes, to show that nothing changed.
fn listing(dir: &Path) -> BTreeMap<PathBuf, Vec<u8>> {
    let mut out = BTreeMap::new();
    for entry in fs::read_dir(dir).unwrap() {
        let path = entry.unwrap().path();
        if path.is_dir() {
            out.extend(listing(&path));
        } else {
            out.insert(path.clone(), fs::read(&path).unwrap());
        }
    }
    out
}

/// `inspect` reports what open would do, where the valid log ends and where
/// it is damaged, and changes nothing: a clean log, a torn tail open would
/// truncate, a hole of zeros before the end of the last segment, which open
/// refuses, an interrupted creation and a directory that lost `CURRENT`.
#[test]
fn inspection_reports_what_open_would_do_and_changes_nothing() {
    let scratch = Scratch::new("inspect");
    let dir = &scratch.0;
    let files = damaged_fixture(dir);
    let (last, last_bytes) = files.last().unwrap();
    let len = last_bytes.len() as u64;
    let inspect = |dir: &Path| {
        let before = listing(dir);
        let inspection = Engine::inspect(dir, EngineOptions::default()).unwrap();
        assert_eq!(listing(dir), before, "inspect changed {}", dir.display());
        inspection
    };

    let clean = inspect(dir);
    let full = clean.last_valid.clone().unwrap();
    assert_eq!((&full.path, full.end, full.seq), (last, len, 600));
    assert_eq!(clean.checkpoint.unwrap().1, 300);
    assert_eq!(clean.segments, files.len());
    assert!(matches!(
        clean.verdict,
        Verdict::Opens {
            torn_tail: None,
            interrupted_segment: None
        }
    ));

    // Part of the last record: open would truncate it, and nothing else.
    fs::write(last, &last_bytes[..last_bytes.len() - 3]).unwrap();
    let torn = inspect(dir);
    let valid = torn.last_valid.unwrap();
    assert_eq!(valid.seq, 599);
    match torn.verdict {
        Verdict::Opens {
            torn_tail: Some(tail),
            interrupted_segment: None,
        } => assert_eq!((tail.offset, tail.bytes), (valid.end, len - 3 - valid.end)),
        verdict => panic!("{verdict:?}"),
    }
    restore(dir, &files);

    // Zeros where records were, with records after them: not a clean
    // prefix, so open refuses, and inspect says where the valid log ends.
    let mut holed = last_bytes.clone();
    holed[last_bytes.len() - 200..last_bytes.len() - 100].fill(0);
    fs::write(last, &holed).unwrap();
    // A refused open changes nothing either: no truncation, no new segment.
    let before = listing(dir);
    assert!(matches!(
        log_damage(Engine::open(dir, EngineOptions::default(), small())),
        LogDamage::Checksum | LogDamage::Frame | LogDamage::Length(_)
    ));
    assert_eq!(listing(dir), before);
    let damaged = inspect(dir);
    let valid = damaged.last_valid.unwrap();
    assert!(valid.seq < 600 && valid.end <= len - 200, "{valid:?}");
    match damaged.verdict {
        Verdict::Damaged {
            path,
            offset,
            bytes_after,
            ..
        } => assert_eq!(
            (&path, offset, bytes_after),
            (last, valid.end, len - valid.end)
        ),
        verdict => panic!("{verdict:?}"),
    }
    restore(dir, &files);

    // A durable engine holds the directory against inspection.
    let engine = open(dir);
    assert!(matches!(
        Engine::inspect(dir, EngineOptions::default()),
        Err(RecoveryError::Locked(_))
    ));
    drop(engine);

    // Interrupted creation: everything but CURRENT. Inspect creates no LOCK.
    let created = dir.join("created");
    drop(Engine::create(&created, EngineOptions::default(), small()).unwrap());
    fs::remove_file(created.join("CURRENT")).unwrap();
    fs::remove_file(created.join("LOCK")).unwrap();
    let before = listing(&created);
    assert!(matches!(
        Engine::open(&created, EngineOptions::default(), small()),
        Err(RecoveryError::NoCheckpoint(_))
    ));
    // Refused before it would create LOCK, and never initialized.
    assert_eq!(listing(&created), before);
    assert!(matches!(inspect(&created).verdict, Verdict::Uncommitted));
    // The same with a write logged has lost its CURRENT.
    let lost = dir.join("lost");
    let engine = Engine::create(&lost, EngineOptions::default(), small()).unwrap();
    engine.put(b"k", b"v").unwrap();
    drop(engine);
    fs::remove_file(lost.join("CURRENT")).unwrap();
    let inspection = inspect(&lost);
    assert!(matches!(inspection.verdict, Verdict::NoCheckpoint));
    assert_eq!(inspection.last_valid.unwrap().seq, 1);
    assert!(matches!(
        Engine::inspect(&dir.join("absent"), EngineOptions::default()),
        Err(RecoveryError::NoCheckpoint(_))
    ));
}

#[test]
fn a_log_ending_before_its_checkpoint_or_starting_after_it_is_rejected() {
    let scratch = Scratch::new("behind");
    let dir = &scratch.0;
    let engine = Engine::create(dir, EngineOptions::default(), small()).unwrap();
    for at in 0..200u32 {
        engine.put(&at.to_be_bytes(), &[0; 40]).unwrap();
    }
    engine
        .durable_checkpoint(CheckpointOptions::default())
        .unwrap();
    drop(engine);
    // The records up to the ceiling, in the active segment, are lost.
    let all = segments(dir);
    let active = all.last().unwrap();
    let header = fs::read(active).unwrap()[..36].to_vec();
    fs::write(active, &header).unwrap();
    assert!(matches!(
        log_damage(Engine::open(dir, EngineOptions::default(), small())),
        LogDamage::Behind { .. }
    ));
    // Only a segment based after the ceiling remains.
    for path in &all {
        fs::remove_file(path).unwrap();
    }
    let engine = Engine::create(
        &dir.join("other"),
        EngineOptions {
            last_seq: 1000,
            ..EngineOptions::default()
        },
        small(),
    )
    .unwrap();
    drop(engine);
    let foreign = segments(&dir.join("other")).remove(0);
    fs::copy(&foreign, dir.join("wal-00000000000000000001")).unwrap();
    assert!(matches!(
        log_damage(Engine::open(dir, EngineOptions::default(), small())),
        LogDamage::Gap { .. }
    ));
}

#[test]
fn databases_are_created_and_opened_explicitly_and_kept_from_other_uses() {
    let scratch = Scratch::new("explicit");
    let dir = &scratch.0;
    // Nothing there is not a database.
    assert!(matches!(
        Engine::open(dir, EngineOptions::default(), small()),
        Err(RecoveryError::NoCheckpoint(_))
    ));
    let engine = Engine::create(dir, EngineOptions::default(), small()).unwrap();
    engine.put(b"k", b"v").unwrap();
    // An open database is locked against a second open, a recovery and a
    // checkpoint from another engine.
    assert!(matches!(
        Engine::open(dir, EngineOptions::default(), small()),
        Err(RecoveryError::Locked(_))
    ));
    let other = Engine::new(EngineOptions::default());
    assert!(matches!(
        other.checkpoint(dir, CheckpointOptions::default()),
        Err(CheckpointError::Locked(_))
    ));
    assert!(matches!(
        other.durable_checkpoint(CheckpointOptions::default()),
        Err(CheckpointError::NotDurable)
    ));
    drop(engine);
    // Closed, it is still not a place for a checkpoint-only recovery, a
    // foreign checkpoint, or a second database.
    assert!(matches!(
        Engine::recover(dir, EngineOptions::default()),
        Err(RecoveryError::HasLog(_))
    ));
    assert!(matches!(
        other.checkpoint(dir, CheckpointOptions::default()),
        Err(CheckpointError::HasLog(_))
    ));
    assert!(matches!(
        Engine::create(dir, EngineOptions::default(), small()),
        Err(RecoveryError::NotEmpty(_))
    ));
    // A checkpoint-only directory has no log to open.
    let plain = dir.join("plain");
    other
        .checkpoint(&plain, CheckpointOptions::default())
        .unwrap();
    assert_eq!(
        log_damage(Engine::open(&plain, EngineOptions::default(), small())),
        LogDamage::NoSegments
    );
    // The database still opens, with its write.
    let engine = open(dir);
    assert_eq!(contents(&engine).get(&b"k"[..]), Some(&b"v".to_vec()));
}

#[test]
fn a_full_log_refuses_writes_until_a_checkpoint_prunes_it() {
    let scratch = Scratch::new("full");
    let durable = DurableOptions {
        segment_bytes: 2048,
        max_log_bytes: 8192,
        ..DurableOptions::default()
    };
    let engine = Engine::create(&scratch.0, EngineOptions::default(), durable).unwrap();
    let mut acked = 0u64;
    let refused = loop {
        match engine.put(&acked.to_be_bytes(), &[3; 100]) {
            Ok(seq) => acked = seq,
            Err(error) => break error,
        }
    };
    assert!(matches!(refused, UpdateError::LogFull { .. }), "{refused}");
    // Refused, not applied, and the log stays open.
    assert_eq!(engine.stats().last_seq, acked);
    assert!(engine.log_status().unwrap().bytes <= 8192);
    assert_eq!(engine.log_status().unwrap().closed, None);
    engine
        .durable_checkpoint(CheckpointOptions::default())
        .unwrap();
    assert_eq!(engine.put(b"next", b"").unwrap(), acked + 1);
    drop(engine);
    let (engine, report) = Engine::open(&scratch.0, EngineOptions::default(), durable).unwrap();
    assert_eq!(report.last_seq, acked + 1);
    assert_eq!(contents(&engine).len() as u64, acked + 1);
}

/// A durable write that fills the table flushes it and is logged once. While
/// the run limits refuse the flush a write needs, the write is refused before
/// it is logged; raising them lets the next write flush, and recovery holds
/// every acknowledged write and nothing refused.
#[test]
fn durable_writes_flush_for_room_and_log_nothing_when_refused() {
    let scratch = Scratch::new("room");
    let options = EngineOptions {
        memtable_budget: 4 << 10,
        ..EngineOptions::default()
    };
    let engine = Engine::create(&scratch.0, options, small()).unwrap();
    let mut model = Model::new();
    let mut put = |engine: &Engine, at: u32| {
        let key = (at % 100).to_be_bytes().to_vec();
        let result = engine.put(&key, &at.to_be_bytes().repeat(25));
        if result.is_ok() {
            model.insert(key, at.to_be_bytes().repeat(25));
        }
        result
    };
    for at in 0..100 {
        assert_eq!(put(&engine, at), Ok(u64::from(at) + 1));
    }
    let stats = engine.stats();
    assert!(stats.flushes > 0, "{stats:?}");
    assert_eq!(engine.log_status().unwrap().records, 100);

    engine.set_run_limits(0, 0);
    let mut at = 100;
    let refused = loop {
        match put(&engine, at) {
            Ok(_) => at += 1,
            Err(error) => break error,
        }
    };
    assert!(matches!(refused, UpdateError::FlushRefused(_)), "{refused}");
    let (stats, log) = (engine.stats(), engine.log_status().unwrap());
    assert_eq!(log.records, stats.last_seq);
    assert_eq!(put(&engine, at), Err(refused));
    assert_eq!(engine.log_status().unwrap().records, log.records);
    assert_eq!(engine.stats(), stats);

    engine.set_run_limits(64, usize::MAX);
    assert_eq!(put(&engine, at), Ok(stats.last_seq + 1));
    assert_eq!(engine.stats().flushes, stats.flushes + 1);
    drop(engine);
    let (engine, report) = Engine::open(&scratch.0, options, small()).unwrap();
    assert_eq!(report.last_seq, stats.last_seq + 1);
    assert_eq!(contents(&engine), model);
}

/// Durable writers that keep filling a small table, beside durable
/// checkpoints and readers, flush without deadlock among the queue, the log,
/// the flush slot and the state lock, and every acknowledged write is
/// recovered once.
#[test]
fn concurrent_durable_writers_flush_beside_checkpoints_and_recover() {
    const WRITERS: u8 = 4;
    const EACH: u32 = 300;
    let scratch = Scratch::new("concurrent-room");
    let options = EngineOptions {
        memtable_budget: 16 << 10,
        background_compaction: Some(CompactionOptions::default()),
        ..EngineOptions::default()
    };
    let durable = DurableOptions {
        segment_bytes: 16 << 10,
        ..DurableOptions::default()
    };
    let engine = Engine::create(&scratch.0, options, durable).unwrap();
    let key = |writer: u8, at: u32| [&[writer][..], &at.to_be_bytes()].concat();
    let done = std::sync::atomic::AtomicBool::new(false);
    let seqs = thread::scope(|scope| {
        let writers: Vec<_> = (0..WRITERS)
            .map(|writer| {
                let engine = &engine;
                scope.spawn(move || {
                    (0..EACH)
                        .map(|at| write(engine, &key(writer, at), Some(&[writer; 200])))
                        .collect::<Vec<u64>>()
                })
            })
            .collect();
        let checkpoints = scope.spawn(|| {
            let mut taken = 0;
            while !done.load(std::sync::atomic::Ordering::Acquire) {
                engine
                    .durable_checkpoint(CheckpointOptions::default())
                    .unwrap();
                taken += 1;
            }
            taken
        });
        scope.spawn(|| {
            let mut workspace = EngineWorkspace::new();
            while !done.load(std::sync::atomic::Ordering::Acquire) {
                engine.get(&key(0, 0), &mut workspace).unwrap();
                let guard = engine.read();
                let mut scan = guard.scan_prefix(&[1], &mut workspace);
                while scan.next_record().unwrap().is_some() {}
            }
        });
        let seqs: Vec<u64> = writers
            .into_iter()
            .flat_map(|writer| writer.join().unwrap())
            .collect();
        done.store(true, std::sync::atomic::Ordering::Release);
        assert!(checkpoints.join().unwrap() > 0);
        seqs
    });
    let total = u64::from(WRITERS) * u64::from(EACH);
    let mut sorted = seqs;
    sorted.sort_unstable();
    assert_eq!(sorted, (1..=total).collect::<Vec<_>>());
    assert!(engine.stats().flushes > 10, "{:?}", engine.stats());
    let expected: Model = (0..WRITERS)
        .flat_map(|writer| (0..EACH).map(move |at| (key(writer, at), vec![writer; 200])))
        .collect();
    assert_eq!(contents(&engine), expected);
    drop(engine);
    let (engine, report) = Engine::open(&scratch.0, options, durable).unwrap();
    assert_eq!(report.last_seq, total);
    assert_eq!(contents(&engine), expected);
}

/// A logged record that no memtable of a smaller budget can hold is refused
/// on replay, naming that budget, and the database opens again with the
/// budget it was written with.
#[test]
fn replay_refuses_a_record_larger_than_a_reduced_budget() {
    let scratch = Scratch::new("replay-budget");
    let engine = Engine::create(
        &scratch.0,
        EngineOptions::default(),
        DurableOptions::default(),
    )
    .unwrap();
    engine.put(b"k", &[1; 4096]).unwrap();
    drop(engine);
    let small = EngineOptions {
        memtable_budget: 1024,
        ..EngineOptions::default()
    };
    let refused = Engine::open(&scratch.0, small, DurableOptions::default()).map(drop);
    assert!(
        matches!(
            refused,
            Err(RecoveryError::Replay(WriteError::RecordExceedsBudget {
                budget: 1024,
                ..
            }))
        ),
        "{refused:?}"
    );
    let engine = open(&scratch.0);
    assert_eq!(contents(&engine).get(&b"k"[..]), Some(&vec![1; 4096]));
}

/// A checkpoint cannot prune the active segment, so a log limit that one
/// full segment and the next one's header would reach is refused before
/// anything is created.
#[test]
fn a_log_limit_without_room_beside_a_full_segment_is_refused() {
    let scratch = Scratch::new("log-too-small");
    let dir = scratch.0.join("db");
    let durable = DurableOptions {
        segment_bytes: 4096,
        max_log_bytes: 4096,
        ..DurableOptions::default()
    };
    let refused = Engine::create(&dir, EngineOptions::default(), durable).map(drop);
    assert!(
        matches!(
            refused,
            Err(RecoveryError::LogTooSmall {
                max_log_bytes: 4096,
                min_exceeded,
            }) if min_exceeded > 4096
        ),
        "{refused:?}"
    );
    assert!(!dir.exists());
    Engine::create(&dir, EngineOptions::default(), small()).unwrap();
    let refused = Engine::open(&dir, EngineOptions::default(), durable).map(drop);
    assert!(
        matches!(refused, Err(RecoveryError::LogTooSmall { .. })),
        "{refused:?}"
    );
}
