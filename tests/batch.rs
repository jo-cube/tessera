use tessera::engine::{Engine, EngineOptions, EngineWorkspace, UpdateError, WriteBatch};

#[test]
fn batches_are_ordered_atomic_and_reusable() {
    let engine = Engine::new(EngineOptions::default()).unwrap();
    let mut batch = WriteBatch::new();
    batch.put(b"a", b"first");
    batch.put(b"b", b"second");
    batch.delete(b"a");
    batch.put(b"a", b"last");
    assert_eq!(batch.len(), 4);
    assert_eq!(engine.write(&batch), Ok(4));
    let snapshot = engine.snapshot().unwrap();
    batch.clear();
    assert!(batch.is_empty());
    assert_eq!(engine.write(&batch), Ok(4));
    batch.delete(b"b");
    batch.put(b"c", b"new");
    assert_eq!(engine.write(&batch), Ok(6));
    let mut ws = EngineWorkspace::new();
    assert_eq!(snapshot.get(b"a", &mut ws).unwrap().unwrap().value, b"last");
    assert!(engine.get(b"b", &mut ws).unwrap().is_none());
    assert_eq!(
        snapshot.get(b"b", &mut ws).unwrap().unwrap().value,
        b"second"
    );
}

#[test]
fn exhausted_batch_changes_nothing() {
    let engine = Engine::new(EngineOptions {
        last_seq: u64::MAX - 1,
        ..EngineOptions::default()
    })
    .unwrap();
    let mut batch = WriteBatch::new();
    batch.put(b"a", b"a");
    batch.put(b"b", b"b");
    assert_eq!(engine.write(&batch), Err(UpdateError::SequencesExhausted));
    assert_eq!(engine.stats().last_seq, u64::MAX - 1);
    assert!(
        engine
            .get(b"a", &mut EngineWorkspace::new())
            .unwrap()
            .is_none()
    );
}

#[test]
fn readers_and_snapshots_see_whole_batches() {
    let engine = Engine::new(EngineOptions {
        memtable_budget: 1024,
        ..EngineOptions::default()
    })
    .unwrap();
    std::thread::scope(|scope| {
        let writer = scope.spawn(|| {
            let mut batch = WriteBatch::new();
            for round in 0..500u64 {
                batch.clear();
                for key in [b"a", b"b", b"c"] {
                    batch.put(key, &round.to_le_bytes());
                }
                engine.write(&batch).unwrap();
            }
        });
        let mut ws = EngineWorkspace::new();
        for _ in 0..500 {
            let snapshot = engine.snapshot().unwrap();
            let a = snapshot
                .get(b"a", &mut ws)
                .unwrap()
                .map(|x| x.value.to_vec());
            let b = snapshot
                .get(b"b", &mut ws)
                .unwrap()
                .map(|x| x.value.to_vec());
            let c = snapshot
                .get(b"c", &mut ws)
                .unwrap()
                .map(|x| x.value.to_vec());
            assert_eq!(a, b);
            assert_eq!(b, c);
            let guard = engine.read();
            let a = guard.get(b"a", &mut ws).unwrap().map(|x| x.value.to_vec());
            let b = guard.get(b"b", &mut ws).unwrap().map(|x| x.value.to_vec());
            assert_eq!(a, b);
        }
        writer.join().unwrap();
    });
}

#[test]
fn ingestion_drains_overlapping_tables_and_keeps_snapshots() {
    let engine = Engine::new(EngineOptions::default()).unwrap();
    engine.put(b"a", b"old").unwrap();
    engine.put(b"c", b"old").unwrap();
    let snapshot = engine.snapshot().unwrap();
    let mut batch = WriteBatch::new();
    batch.put(b"a", b"ingested");
    batch.delete(b"c");
    assert_eq!(engine.ingest_sorted(&batch), Ok(4));
    assert_eq!(engine.stats().active.charged, 0);
    assert!(engine.stats().frozen.is_none());
    let mut ws = EngineWorkspace::new();
    assert_eq!(
        engine.get(b"a", &mut ws).unwrap().unwrap().value,
        b"ingested"
    );
    assert!(engine.get(b"c", &mut ws).unwrap().is_none());
    assert_eq!(snapshot.get(b"a", &mut ws).unwrap().unwrap().value, b"old");
    engine.put(b"a", b"after").unwrap();
    engine.flush().unwrap();
    engine.wait_for_compaction().unwrap();
    assert_eq!(engine.get(b"a", &mut ws).unwrap().unwrap().value, b"after");
}

#[test]
fn nonoverlapping_ingestion_leaves_the_active_table_alone() {
    let engine = Engine::new(EngineOptions::default()).unwrap();
    engine.put(b"z", b"active").unwrap();
    let before = engine.stats().active;
    let mut batch = WriteBatch::new();
    batch.put(b"a", b"one");
    batch.put(b"b", b"two");
    engine.ingest_sorted(&batch).unwrap();
    assert_eq!(engine.stats().active, before);
    assert_eq!(engine.stats().flushes, 0);
    let mut ws = EngineWorkspace::new();
    assert_eq!(engine.get(b"z", &mut ws).unwrap().unwrap().value, b"active");
    assert_eq!(engine.get(b"b", &mut ws).unwrap().unwrap().value, b"two");
}

#[test]
fn sorted_validation_and_automatic_ingestion() {
    let engine = Engine::new(EngineOptions {
        bulk_ingest_min: Some(1),
        ..EngineOptions::default()
    })
    .unwrap();
    let mut batch = WriteBatch::new();
    batch.put(b"b", b"v");
    batch.put(b"a", b"v");
    assert!(matches!(
        engine.ingest_sorted(&batch),
        Err(UpdateError::Rejected(_))
    ));
    assert_eq!(engine.stats().last_seq, 0);
    batch.clear();
    batch.put(b"a", b"v");
    batch.put(b"a", b"w");
    assert!(matches!(
        engine.ingest_sorted(&batch),
        Err(UpdateError::Rejected(_))
    ));
    batch.clear();
    batch.put(b"a", b"v");
    batch.put(b"b", b"w");
    assert_eq!(engine.write(&batch), Ok(2));
    assert_eq!(engine.stats().active.charged, 0);
    assert_eq!(engine.stats().runs, 1);
}

#[test]
fn oversized_unsorted_batches_keep_the_last_duplicate_and_publish_atomically() {
    let engine = Engine::new(EngineOptions {
        memtable_budget: 256,
        ..EngineOptions::default()
    })
    .unwrap();
    let mut batch = WriteBatch::new();
    for index in (0..40u64).rev() {
        batch.put(&index.to_be_bytes(), &index.to_le_bytes());
    }
    batch.delete(&7u64.to_be_bytes());
    batch.put(&3u64.to_be_bytes(), b"last");
    assert_eq!(engine.write(&batch), Ok(42));
    assert_eq!(engine.stats().active.charged, 0);
    let mut ws = EngineWorkspace::new();
    assert!(engine.get(&7u64.to_be_bytes(), &mut ws).unwrap().is_none());
    assert_eq!(
        engine
            .get(&3u64.to_be_bytes(), &mut ws)
            .unwrap()
            .unwrap()
            .value,
        b"last"
    );
}
