//! Tests that run this binary again as a child process: one holding a
//! checkpoint directory's `LOCK`, one writing to a durable engine until
//! killed, one that must not inherit the lock. They live apart from the other suites, and run one at a time,
//! because spawning forks: a child shares every lock descriptor this process
//! has open until it executes, so a test in another thread could briefly
//! find its own directory locked. Files go to fresh directories under the
//! system temporary directory, which the bounded runner makes a small tmpfs.

use std::collections::BTreeMap;
use std::fs;
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::{Mutex, MutexGuard};
use std::thread;

use tessera::engine::{
    CheckpointError, CheckpointOptions, DurableOptions, Engine, EngineOptions, EngineWorkspace,
    RecoveryError,
};

/// The defaults without the background worker: these tests flush and
/// compact by hand and count what each step leaves.
fn manual() -> EngineOptions {
    EngineOptions {
        background_compaction: None,
        ..EngineOptions::default()
    }
}

/// Held by each parent test, so that one test never forks while the other
/// has a lock open.
static ONE_AT_A_TIME: Mutex<()> = Mutex::new(());

fn one_at_a_time() -> MutexGuard<'static, ()> {
    ONE_AT_A_TIME
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

type Model = BTreeMap<Vec<u8>, Vec<u8>>;

struct Scratch(PathBuf);

impl Scratch {
    fn new(name: &str) -> Self {
        let path =
            std::env::temp_dir().join(format!("tessera-process-{name}-{}", std::process::id()));
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

/// A RAM-only engine with flushed runs, deletes and a memtable.
fn varied_engine() -> (Engine, Model) {
    let engine = Engine::new(EngineOptions {
        target_run_len: 4096,
        ..manual()
    })
    .unwrap();
    let mut model = Model::new();
    for i in 0..2_000u32 {
        let (key, value) = (i.to_be_bytes().to_vec(), format!("v{i}").into_bytes());
        engine.put(&key, &value).unwrap();
        model.insert(key, value);
    }
    engine.flush().unwrap();
    for i in (0..2_000u32).step_by(3) {
        engine.delete(&i.to_be_bytes()).unwrap();
        model.remove(&i.to_be_bytes()[..]);
    }
    (engine, model)
}

fn small() -> DurableOptions {
    DurableOptions {
        segment_bytes: 4096,
        ..DurableOptions::default()
    }
}

/// Run as a child by the test below: holds `LOCK` in `dir` as another
/// process would, until killed (or a minute passes).
fn hold_lock_as_child(dir: &str) {
    let file = fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .write(true)
        .open(Path::new(dir).join("LOCK"))
        .unwrap();
    file.try_lock().unwrap();
    println!("tessera-child-locked");
    thread::sleep(std::time::Duration::from_secs(60));
}

#[test]
fn another_process_holding_the_lock_excludes_us_until_it_dies() {
    if let Ok(dir) = std::env::var("TESSERA_LOCK_CHILD") {
        return hold_lock_as_child(&dir);
    }
    let _serial = one_at_a_time();
    let scratch = Scratch::new("cross-process");
    let (engine, model) = varied_engine();
    engine
        .checkpoint(&scratch.0, CheckpointOptions::default())
        .unwrap();
    let mut child = Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "another_process_holding_the_lock_excludes_us_until_it_dies",
            "--nocapture",
            "--test-threads=1",
        ])
        .env("TESSERA_LOCK_CHILD", &scratch.0)
        .stdout(Stdio::piped())
        .spawn()
        .unwrap();
    let mut lines = BufReader::new(child.stdout.take().unwrap()).lines();
    assert!(
        lines.any(|line| line.unwrap().contains("tessera-child-locked")),
        "the child did not take the lock"
    );

    assert!(matches!(
        engine.checkpoint(&scratch.0, CheckpointOptions::default()),
        Err(CheckpointError::Locked(_))
    ));
    assert!(matches!(
        Engine::recover(&scratch.0, manual()),
        Err(RecoveryError::Locked(_))
    ));

    // SIGKILL: no destructor runs in the child, and the kernel releases the
    // lock with its open files.
    child.kill().unwrap();
    child.wait().unwrap();
    let report = engine
        .checkpoint(&scratch.0, CheckpointOptions::default())
        .unwrap();
    assert_eq!(report.generation, 2);
    let recovered = Engine::recover(&scratch.0, manual()).unwrap();
    assert_eq!(contents(&recovered), model);
}

const CHILD_THREADS: u64 = 3;

fn child_key(round: u64, thread: u64, at: u64) -> Vec<u8> {
    format!("key-{round}-{thread}-{at:08}").into_bytes()
}

fn child_value(round: u64, thread: u64, at: u64) -> Vec<u8> {
    (round * 1_000_000 + thread * 100_000 + at)
        .to_le_bytes()
        .repeat(1 + at as usize % 13)
}

/// Run as a child by the test below: opens the database and writes from
/// several threads until killed, printing each acknowledged write and its
/// sequence number, with a checkpoint every few hundred writes.
fn write_as_child(dir: &str, round: u64) {
    let dir = Path::new(dir);
    let (engine, report) = Engine::open(dir, manual(), small()).unwrap();
    let out = std::sync::Mutex::new(std::io::stdout());
    writeln!(
        out.lock().unwrap(),
        "tessera-child-open {}",
        report.last_seq
    )
    .unwrap();
    thread::scope(|scope| {
        for thread in 0..CHILD_THREADS {
            let (engine, out) = (&engine, &out);
            scope.spawn(move || {
                for at in 1.. {
                    let seq = engine
                        .put(
                            &child_key(round, thread, at),
                            &child_value(round, thread, at),
                        )
                        .unwrap();
                    let mut printed = out.lock().unwrap();
                    writeln!(printed, "tessera-acked {thread} {at} {seq}").unwrap();
                    printed.flush().unwrap();
                    drop(printed);
                    if thread == 0 && at.is_multiple_of(300) {
                        engine
                            .durable_checkpoint(CheckpointOptions::default())
                            .unwrap();
                    }
                    if thread == 0 && at.is_multiple_of(100) {
                        let status = engine.log_status().unwrap();
                        let mut out = out.lock().unwrap();
                        writeln!(out, "tessera-log {} {}", status.batches, status.records).unwrap();
                    }
                    assert!(at < 10_000_000, "the parent never killed the child");
                }
            });
        }
    });
}

/// A process killed (SIGKILL) while several threads write loses no write it
/// acknowledged, adds at most each thread's write in flight, and holds the
/// directory against this process until it dies.
#[test]
fn a_killed_process_loses_no_acknowledged_write() {
    if let Ok(dir) = std::env::var("TESSERA_DURABLE_CHILD") {
        let round = std::env::var("TESSERA_DURABLE_ROUND")
            .unwrap()
            .parse()
            .unwrap();
        return write_as_child(&dir, round);
    }
    let _serial = one_at_a_time();
    let scratch = Scratch::new("killed");
    drop(Engine::create(&scratch.0, manual(), small()).unwrap());
    // Per round and thread, the sequence number of each acknowledged write.
    let mut acked: Vec<Vec<Vec<u64>>> = Vec::new();
    let mut last_recovered = 0;
    let mut shared = false;
    for (round, writes) in [2_000u64, 3_500, 1_200].into_iter().enumerate() {
        let round = round as u64;
        let mut child = Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "a_killed_process_loses_no_acknowledged_write",
                "--nocapture",
                "--test-threads=1",
            ])
            .env("TESSERA_DURABLE_CHILD", &scratch.0)
            .env("TESSERA_DURABLE_ROUND", round.to_string())
            .stdout(Stdio::piped())
            .spawn()
            .unwrap();
        let mut lines = BufReader::new(child.stdout.take().unwrap()).lines();
        let mut opened = None;
        let mut this = vec![Vec::new(); CHILD_THREADS as usize];
        let mut record = |line: &str, this: &mut Vec<Vec<u64>>| {
            if let Some(log) = line.split("tessera-log ").nth(1) {
                let (batches, records) = log.split_once(' ').unwrap();
                shared |= records.parse::<u64>().unwrap() > batches.parse::<u64>().unwrap();
            }
            let Some(rest) = line.split("tessera-acked ").nth(1) else {
                return 0;
            };
            let fields: Vec<u64> = rest.split(' ').map(|f| f.parse().unwrap()).collect();
            let thread = &mut this[fields[0] as usize];
            assert_eq!(fields[1], thread.len() as u64 + 1);
            thread.push(fields[2]);
            this.iter().map(Vec::len).sum::<usize>() as u64
        };
        for line in lines.by_ref() {
            let line = line.unwrap();
            if let Some(seq) = line.split("tessera-child-open ").nth(1) {
                opened = Some(seq.parse::<u64>().unwrap());
                continue;
            }
            let count = record(&line, &mut this);
            if count == writes / 2 {
                assert!(matches!(
                    Engine::open(&scratch.0, manual(), small()),
                    Err(RecoveryError::Locked(_))
                ));
            }
            if count >= writes {
                break;
            }
        }
        child.kill().unwrap();
        child.wait().unwrap();
        // The child went on writing; what it printed before dying is still in
        // the pipe. Writes it made durable without printing are those
        // recovery may add.
        for line in lines {
            let Ok(line) = line else { break };
            record(&line, &mut this);
        }
        // The child recovered everything up to where the last round's
        // recovery got.
        assert_eq!(opened, Some(last_recovered), "round {round}");
        acked.push(this);

        let (engine, report) = Engine::open(&scratch.0, manual(), small()).unwrap();
        let mut workspace = EngineWorkspace::new();
        let mut most = 0;
        for (r, threads) in acked.iter().enumerate() {
            for (t, seqs) in threads.iter().enumerate() {
                let (r, t) = (r as u64, t as u64);
                for (at, &seq) in seqs.iter().enumerate() {
                    let at = at as u64 + 1;
                    let found = engine
                        .get(&child_key(r, t, at), &mut workspace)
                        .unwrap()
                        .unwrap_or_else(|| panic!("round {r} thread {t} write {at} lost"));
                    assert_eq!((found.seq, found.value), (seq, &child_value(r, t, at)[..]));
                    most = most.max(seq);
                }
                // At most the write each thread had in flight is added.
                let beyond = seqs.len() as u64 + 2;
                assert!(
                    engine
                        .get(&child_key(r, t, beyond), &mut workspace)
                        .unwrap()
                        .is_none()
                );
            }
        }
        assert!(
            (most..=most + CHILD_THREADS).contains(&report.last_seq),
            "acknowledged up to {most}, recovered {}",
            report.last_seq
        );
        last_recovered = report.last_seq;
    }
    assert!(shared, "no batch held more than one write");
}

/// A child spawned while a durable engine holds `LOCK` does not keep it once
/// it executes its program: every descriptor is close-on-exec, and
/// `Command::spawn` returns only after the child has executed. So the
/// directory reopens while the child still runs.
#[test]
fn a_child_that_executes_another_program_does_not_keep_the_lock() {
    if std::env::var("TESSERA_SLEEP_CHILD").is_ok() {
        thread::sleep(std::time::Duration::from_secs(60));
        return;
    }
    let _serial = one_at_a_time();
    let scratch = Scratch::new("exec");
    let engine = Engine::create(&scratch.0, manual(), small()).unwrap();
    engine.put(b"k", b"v").unwrap();
    let mut child = Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "a_child_that_executes_another_program_does_not_keep_the_lock",
            "--test-threads=1",
        ])
        .env("TESSERA_SLEEP_CHILD", "1")
        .stdout(Stdio::null())
        .spawn()
        .unwrap();
    drop(engine);
    let reopened = Engine::open(&scratch.0, manual(), small());
    child.kill().unwrap();
    child.wait().unwrap();
    let (engine, report) = reopened.unwrap();
    assert_eq!(report.last_seq, 1);
    assert_eq!(contents(&engine).get(&b"k"[..]), Some(&b"v".to_vec()));
}
