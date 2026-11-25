//! A bounded concurrent history, replayed independently at every read ceiling.
use super::*;
use crate::test_support::{Rng, Scratch, campaign_seed};
use std::collections::BTreeMap;
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Barrier, Condvar};
use std::time::{Duration, Instant};

type Model = BTreeMap<Vec<u8>, Vec<u8>>;
type Operation = (Vec<u8>, Option<Vec<u8>>);

struct Commit {
    width: u64,
    operations: Vec<Operation>,
}

#[derive(Default)]
struct History {
    commits: BTreeMap<u64, Commit>,
    complete: u64,
}

#[derive(Default)]
struct Oracle {
    history: Mutex<History>,
    changed: Condvar,
}

impl Oracle {
    // The mutex is acquired only after the engine call returns. Writers can
    // publish and return in a different order; never serialize their API calls.
    fn record(&self, ceiling: u64, commit: Commit, bound: usize) {
        let mut history = self.history.lock().unwrap();
        assert!(history.commits.len() < bound);
        assert!(history.commits.insert(ceiling, commit).is_none());
        loop {
            let next = history
                .commits
                .range((
                    std::ops::Bound::Excluded(history.complete),
                    std::ops::Bound::Unbounded,
                ))
                .next();
            let Some((&end, commit)) = next else {
                break;
            };
            if end - commit.width != history.complete {
                break;
            }
            history.complete = end;
        }
        self.changed.notify_all();
    }

    fn at(&self, ceiling: u64) -> Model {
        let mut history = self.history.lock().unwrap();
        let deadline = Instant::now() + Duration::from_secs(15);
        while history.complete < ceiling {
            let remaining = deadline.saturating_duration_since(Instant::now());
            assert!(
                !remaining.is_zero(),
                "oracle never received sequence {ceiling}"
            );
            history = self.changed.wait_timeout(history, remaining).unwrap().0;
        }
        assert!(
            ceiling == 0 || history.commits.contains_key(&ceiling),
            "snapshot split a batch at {ceiling}"
        );
        let mut model = Model::new();
        for (_, commit) in history.commits.range(..=ceiling) {
            for (key, value) in &commit.operations {
                if let Some(value) = value {
                    model.insert(key.clone(), value.clone());
                } else {
                    model.remove(key);
                }
            }
        }
        model
    }
}

fn check(snapshot: &Snapshot, ks: &Keyspace, oracle: &Oracle) {
    let expected = oracle.at(snapshot.last_seq());
    let mut reader = snapshot.reader();
    let mut scan = reader.range(ks, ..);
    let mut actual = Model::new();
    while let Some((key, value)) = scan.next().unwrap() {
        assert!(actual.len() < 128);
        assert!(actual.insert(key.to_vec(), value.to_vec()).is_none());
    }
    assert_eq!(actual, expected, "snapshot ceiling {}", snapshot.last_seq());
    drop(scan);
    let mut keys: Vec<_> = (0..32).map(|i| format!("p/{i:02}").into_bytes()).collect();
    keys.extend(expected.keys().cloned());
    keys.extend([b"missing".to_vec(), b"p/00".to_vec()]);
    keys.reverse();
    for key in &keys {
        assert_eq!(
            reader.get(ks, key).unwrap(),
            expected.get(key).map(Vec::as_slice)
        );
    }
    let values = reader.multi_get(ks, &keys).unwrap();
    assert_eq!(values.len(), keys.len());
    for (i, value) in values.iter().enumerate() {
        assert_eq!(*value, expected.get(&keys[i]).map(Vec::as_slice));
    }
    drop(values);
    for prefix in [&b"p/"[..], b"b/", b"i/", b"absent", &[255]] {
        let mut scan = reader.prefix(ks, prefix);
        let mut expected = expected.iter().filter(|(key, _)| key.starts_with(prefix));
        while let Some(record) = scan.next().unwrap() {
            let (key, value) = expected.next().unwrap();
            assert_eq!(record, (key.as_slice(), value.as_slice()));
        }
        assert!(expected.next().is_none());
    }
    // Only whole batches update these pairs, so equality checks atomicity
    // independently of the sequence replay above.
    for writer in 0..3 {
        assert_eq!(
            actual.get(format!("b/{writer}/0").as_bytes()),
            actual.get(format!("b/{writer}/1").as_bytes())
        );
    }
}

/// Runs the model. With `long`, the writers' seeds derive from
/// `TESSERA_TEST_SEED` when it is set.
fn workload(iterations: usize, long: bool) {
    let started = Instant::now();
    let scratch = Scratch::new("concurrent");
    let options = Options {
        memtable_size: 4096,
        memory_limit: Some(8 << 20),
        target_run_len: 2048,
        level_ratio: 2,
        background_threads: 2,
        block_size: 128,
        ..Options::default()
    };
    let engine = Engine::in_memory(options).unwrap();
    let oracle = Oracle::default();
    let barrier = Barrier::new(6);
    let writers = AtomicUsize::new(3);
    let reads = AtomicUsize::new(0);
    let checkpoints = AtomicUsize::new(0);
    let admitted = std::array::from_fn::<_, 4, _>(|_| AtomicUsize::new(0));
    let refused = AtomicUsize::new(0);
    std::thread::scope(|scope| {
        for writer in 0..3 {
            let (engine, oracle, barrier, writers, admitted, refused) =
                (&engine, &oracle, &barrier, &writers, &admitted, &refused);
            scope.spawn(move || {
                let seed = 0x7374_7265_7373 + writer as u64;
                let mut rng = Rng(if long { campaign_seed(seed) } else { seed });
                barrier.wait();
                for iteration in 0..iterations {
                    let kind = iteration % 4;
                    let key = format!("p/{:02}", rng.below(32)).into_bytes();
                    let value = format!("writer {writer} iteration {iteration:06}").into_bytes();
                    let operations = match kind {
                        0 => vec![(key, Some(value))],
                        1 => vec![(key, None)],
                        2 => vec![
                            (format!("b/{writer}/0").into_bytes(), Some(value.clone())),
                            (format!("b/{writer}/1").into_bytes(), Some(value)),
                            (key, None),
                        ],
                        _ => (0..4)
                            .map(|i| (format!("i/{i}").into_bytes(), Some(value.clone())))
                            .collect(),
                    };
                    let mut batch = WriteBatch::new();
                    let ks = engine.ks();
                    for (key, value) in &operations {
                        if let Some(value) = value {
                            batch.put(&ks, key, value);
                        } else {
                            batch.delete(&ks, key);
                        }
                    }
                    let result = match kind {
                        0 => ks.put_seq(&operations[0].0, operations[0].1.as_ref().unwrap()),
                        1 => ks.delete_seq(&operations[0].0),
                        2 => engine.write_seq(&batch),
                        _ => engine.ingest_sorted(&batch),
                    };
                    match result {
                        Ok(ceiling) => {
                            let width = if kind == 3 {
                                1
                            } else {
                                operations.len() as u64
                            };
                            oracle.record(ceiling, Commit { width, operations }, 3 * iterations);
                            admitted[kind].fetch_add(1, Ordering::Relaxed);
                        }
                        Err(UpdateError::MemoryLimit { .. }) => {
                            refused.fetch_add(1, Ordering::Relaxed);
                        }
                        Err(error) => panic!("writer {writer}, iteration {iteration}: {error}"),
                    }
                    if iteration % 8 == 0 {
                        std::thread::yield_now();
                    }
                }
                writers.fetch_sub(1, Ordering::SeqCst);
            });
        }
        for _ in 0..2 {
            let (engine, oracle, barrier, writers, reads) =
                (&engine, &oracle, &barrier, &writers, &reads);
            scope.spawn(move || {
                barrier.wait();
                let mut count = 0;
                while writers.load(Ordering::SeqCst) != 0 || count < 40 {
                    let snapshot = engine.snapshot();
                    std::thread::yield_now();
                    check(&snapshot, &engine.ks(), oracle);
                    count += 1;
                }
                reads.fetch_add(count, Ordering::Relaxed);
            });
        }
        scope.spawn(|| {
            barrier.wait();
            let mut count = 0;
            while writers.load(Ordering::SeqCst) != 0 || count < 4 {
                engine.flush().unwrap();
                let report = engine.checkpoint_to(&scratch.0).unwrap();
                let recovered = Engine::open(&scratch.0, options).unwrap();
                let snapshot = recovered.snapshot();
                assert_eq!(snapshot.last_seq(), report.last_seq);
                check(&snapshot, &recovered.ks(), &oracle);
                recovered.close().unwrap();
                count += 1;
            }
            checkpoints.store(count, Ordering::Relaxed);
        });
    });
    engine.flush().unwrap();
    engine.wait_for_compaction().unwrap();
    check(&engine.snapshot(), &engine.ks(), &oracle);
    for count in &admitted {
        assert!(count.load(Ordering::Relaxed) > 0);
    }
    assert!(engine.background_status().unwrap().compactions > 0);
    engine.close().unwrap();
    eprintln!(
        "concurrent model: {} writes, {} refusals, {} snapshots, {} checkpoints; {:?}",
        admitted
            .iter()
            .map(|n| n.load(Ordering::Relaxed))
            .sum::<usize>(),
        refused.load(Ordering::Relaxed),
        reads.load(Ordering::Relaxed),
        checkpoints.load(Ordering::Relaxed),
        started.elapsed()
    );
}

// A process deadline also covers deadlocks inside engine calls and joins. A
// timeout kills and reaps the child; no blocked threads escape into other tests.
fn bounded(long: bool) {
    let name = if long {
        "engine::stress_tests::long_concurrent_model"
    } else {
        "engine::stress_tests::bounded_concurrent_model"
    };
    if std::env::var("TESSERA_MODEL_CHILD").as_deref() == Ok(name) {
        workload(if long { 2000 } else { 640 }, long);
        return;
    }
    let mut child = Command::new(std::env::current_exe().unwrap())
        .args(["--exact", name, "--nocapture", "--include-ignored"])
        .env("TESSERA_MODEL_CHILD", name)
        .stdin(Stdio::null())
        .spawn()
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(if long { 120 } else { 20 });
    loop {
        if let Some(status) = child.try_wait().unwrap() {
            assert!(status.success(), "model child: {status}");
            return;
        }
        if Instant::now() >= deadline {
            child.kill().unwrap();
            child.wait().unwrap();
            panic!("concurrent model exceeded its deadline");
        }
        std::thread::sleep(Duration::from_millis(10));
    }
}

#[test]
fn bounded_concurrent_model() {
    bounded(false);
}

#[test]
#[ignore = "long concurrent sequence-history campaign"]
fn long_concurrent_model() {
    bounded(true);
}
