//! Tests that run this binary again as a child process: one holding an
//! engine directory's `LOCK` until killed, one checking close-on-exec.
//! They live apart from the other suites and run one at a time, because
//! spawning forks: a child shares every lock descriptor this process
//! has open until it executes, so a test in another thread could briefly
//! find its own directory locked. Files go to fresh directories under the
//! system temporary directory, which the bounded runner makes a small tmpfs.

use std::collections::BTreeMap;
use std::fs;
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::{Mutex, MutexGuard};
use std::thread;

use tessera::{Engine, ErrorKind, Options};

fn options() -> Options {
    Options::default()
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
    let ks = engine.keyspace("test").unwrap();
    let mut reader = engine.reader();
    let mut scan = reader.range(&ks, ..);
    let mut out = Model::new();
    while let Some((key, value)) = scan.next().unwrap() {
        out.insert(key.to_vec(), value.to_vec());
    }
    out
}

/// Opens `dir`, writes puts and deletes, checkpoints and closes it.
fn checkpointed(dir: &Path) -> Model {
    let engine = Engine::open(dir, options()).unwrap();
    let ks = engine.keyspace("test").unwrap();
    let mut model = Model::new();
    for i in 0..2_000u32 {
        let (key, value) = (i.to_be_bytes().to_vec(), format!("v{i}").into_bytes());
        ks.put(&key, &value).unwrap();
        model.insert(key, value);
    }
    for i in (0..2_000u32).step_by(3) {
        ks.delete(&i.to_be_bytes()).unwrap();
        model.remove(&i.to_be_bytes()[..]);
    }
    engine.checkpoint().unwrap();
    engine.close().unwrap();
    model
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
    let model = checkpointed(&scratch.0);
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

    let error = Engine::open(&scratch.0, options()).unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Locked, "{error}");
    assert_eq!(error.path(), Some(scratch.0.as_path()));

    // SIGKILL: no destructor runs in the child, and the kernel releases the
    // lock with its open files.
    child.kill().unwrap();
    child.wait().unwrap();
    let recovered = Engine::open(&scratch.0, options()).unwrap();
    assert_eq!(contents(&recovered), model);
    recovered.checkpoint().unwrap();
}

/// A child spawned while an engine holds its directory does not keep the
/// lock once it executes its program: every descriptor is close-on-exec,
/// and `Command::spawn` returns only after the child has executed. So the
/// directory opens again while the child still runs.
#[test]
fn a_child_that_executes_another_program_does_not_keep_the_lock() {
    if std::env::var("TESSERA_SLEEP_CHILD").is_ok() {
        thread::sleep(std::time::Duration::from_secs(60));
        return;
    }
    let _serial = one_at_a_time();
    let scratch = Scratch::new("exec");
    let engine = Engine::open(&scratch.0, options()).unwrap();
    engine.keyspace("test").unwrap().put(b"k", b"v").unwrap();
    engine.checkpoint().unwrap();
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
    engine.close().unwrap();
    let reopened = Engine::open(&scratch.0, options());
    child.kill().unwrap();
    child.wait().unwrap();
    let engine = reopened.unwrap();
    assert_eq!(
        contents(&engine),
        Model::from([(b"k".to_vec(), b"v".to_vec())])
    );
}
