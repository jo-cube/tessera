//! Durable engines through the public API: what `open` recovers from a
//! checkpoint and the log, what it rejects, and what a killed process leaves.
//! Injected append and checkpoint failures are tested beside the code.
//! Files go to fresh directories under the system temporary directory, which
//! the bounded runner makes a small tmpfs.

use std::collections::BTreeMap;
use std::fs;
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::thread;

use tessera::block::{MAX_KEY_LEN, MAX_VALUE_LEN};
use tessera::engine::{
    CheckpointError, CheckpointOptions, CompactionOptions, DurableOptions, Engine, EngineOptions,
    EngineWorkspace, LogDamage, RecoveryError, UpdateError,
};
use tessera::memtable::WriteError;

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

/// Writes (or deletes, without a value), flushing when the memtables are
/// full.
fn write(engine: &Engine, key: &[u8], value: Option<&[u8]>) -> u64 {
    loop {
        let result = match value {
            Some(value) => engine.put(key, value),
            None => engine.delete(key),
        };
        match result {
            Ok(seq) => return seq,
            Err(UpdateError::Rejected(WriteError::BudgetExceeded { .. })) => {
                if engine.flush().is_err() {
                    engine.wait_for_compaction().unwrap();
                }
            }
            Err(error) => panic!("{error}"),
        }
    }
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

fn child_key(at: u64) -> Vec<u8> {
    format!("key-{at:08}").into_bytes()
}

fn child_value(at: u64) -> Vec<u8> {
    at.to_le_bytes().repeat(1 + at as usize % 13)
}

/// Run as a child by the test below: opens the database and writes until
/// killed, printing each acknowledged sequence number, with a checkpoint
/// every few hundred writes.
fn write_as_child(dir: &str) {
    let dir = Path::new(dir);
    let (engine, report) = Engine::open(dir, EngineOptions::default(), small()).unwrap();
    let mut out = std::io::stdout().lock();
    writeln!(out, "tessera-child-open {}", report.last_seq).unwrap();
    let mut at = report.last_seq;
    loop {
        at += 1;
        let seq = engine.put(&child_key(at), &child_value(at)).unwrap();
        assert_eq!(seq, at);
        writeln!(out, "tessera-acked {seq}").unwrap();
        out.flush().unwrap();
        if at.is_multiple_of(300) {
            engine
                .durable_checkpoint(CheckpointOptions::default())
                .unwrap();
        }
        assert!(at < 10_000_000, "the parent never killed the child");
    }
}

/// A process killed (SIGKILL) at an arbitrary write loses no write it
/// acknowledged, at most the one it had not, and holds the directory against
/// this process until it dies.
#[test]
fn a_killed_process_loses_no_acknowledged_write() {
    if let Ok(dir) = std::env::var("TESSERA_DURABLE_CHILD") {
        return write_as_child(&dir);
    }
    let scratch = Scratch::new("killed");
    drop(Engine::create(&scratch.0, EngineOptions::default(), small()).unwrap());
    let mut acked = 0;
    for (round, writes) in [2_000u64, 3_500, 1_200].into_iter().enumerate() {
        let mut child = Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "a_killed_process_loses_no_acknowledged_write",
                "--nocapture",
                "--test-threads=1",
            ])
            .env("TESSERA_DURABLE_CHILD", &scratch.0)
            .stdout(Stdio::piped())
            .spawn()
            .unwrap();
        let mut lines = BufReader::new(child.stdout.take().unwrap()).lines();
        let mut opened = None;
        let mut last = None;
        for line in lines.by_ref() {
            let line = line.unwrap();
            if let Some(seq) = line.split("tessera-child-open ").nth(1) {
                opened = Some(seq.parse::<u64>().unwrap());
            } else if let Some(seq) = line.strip_prefix("tessera-acked ") {
                let seq: u64 = seq.parse().unwrap();
                last = Some(seq);
                if seq == opened.unwrap() + writes / 2 {
                    assert!(matches!(
                        Engine::open(&scratch.0, EngineOptions::default(), small()),
                        Err(RecoveryError::Locked(_))
                    ));
                }
                if seq >= opened.unwrap() + writes {
                    break;
                }
            }
        }
        child.kill().unwrap();
        child.wait().unwrap();
        // The child went on writing; what it printed before dying is still in
        // the pipe. A write it made durable without printing is the one
        // recovery may add.
        for line in lines {
            let Ok(line) = line else { break };
            if let Some(seq) = line.strip_prefix("tessera-acked ") {
                last = Some(seq.parse().unwrap());
            }
        }
        // Every write acknowledged in an earlier round was recovered by the
        // child, and every one acknowledged in this round is recovered now.
        assert_eq!(opened, Some(acked), "round {round}");
        acked = last.unwrap();
        let (engine, report) = Engine::open(&scratch.0, EngineOptions::default(), small()).unwrap();
        assert!(
            (acked..=acked + 1).contains(&report.last_seq),
            "acknowledged {acked}, recovered {}",
            report.last_seq
        );
        let mut workspace = EngineWorkspace::new();
        for at in 1..=report.last_seq {
            let found = engine.get(&child_key(at), &mut workspace).unwrap().unwrap();
            assert_eq!(found.value, &child_value(at)[..], "write {at}");
        }
        acked = report.last_seq;
    }
}
