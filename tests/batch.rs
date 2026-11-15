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
