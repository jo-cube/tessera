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
            let mut values = Vec::new();
            engine
                .multi_get(&[b"a", b"b", b"c"], &mut ws, |_, value| {
                    values.push(value.map(<[u8]>::to_vec))
                })
                .unwrap();
            assert!(values.windows(2).all(|pair| pair[0] == pair[1]));
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
    assert_eq!(engine.ingest_sorted(&batch), Ok(3));
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
    assert_eq!(engine.write(&batch), Ok(1));
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
    assert_eq!(engine.write(&batch), Ok(1));
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

#[test]
fn multi_get_preserves_order_duplicates_misses_and_deletes_in_every_view() {
    let engine = Engine::new(EngineOptions::default()).unwrap();
    for (key, value) in [(b"a", b"old"), (b"b", b"old"), (b"c", b"old")] {
        engine.put(key, value).unwrap();
    }
    engine.flush().unwrap();
    engine.delete(b"b").unwrap();
    engine.put(b"c", b"new").unwrap();
    let snapshot = engine.snapshot().unwrap();
    engine.put(b"a", b"later").unwrap();
    let keys: &[&[u8]] = &[b"c", b"missing", b"a", b"b", b"c", b"", b"a"];
    let mut ws = EngineWorkspace::new();
    let expected: Vec<_> = keys
        .iter()
        .map(|key| engine.get(key, &mut ws).unwrap().map(|x| x.value.to_vec()))
        .collect();
    let mut actual = Vec::new();
    engine
        .multi_get(keys, &mut ws, |index, value| {
            assert_eq!(index, actual.len());
            actual.push(value.map(<[u8]>::to_vec));
        })
        .unwrap();
    assert_eq!(actual, expected);
    actual.clear();
    engine
        .read()
        .multi_get(keys, &mut ws, |_, value| {
            actual.push(value.map(<[u8]>::to_vec))
        })
        .unwrap();
    assert_eq!(actual, expected);
    let expected: Vec<_> = keys
        .iter()
        .map(|key| {
            snapshot
                .get(key, &mut ws)
                .unwrap()
                .map(|x| x.value.to_vec())
        })
        .collect();
    actual.clear();
    snapshot
        .multi_get(keys, &mut ws, |_, value| {
            actual.push(value.map(<[u8]>::to_vec))
        })
        .unwrap();
    assert_eq!(actual, expected);
    let before = ws.blocks_loaded();
    engine
        .multi_get(&[b"a"; 100], &mut ws, |_, value| {
            assert_eq!(value, Some(&b"later"[..]))
        })
        .unwrap();
    assert_eq!(ws.blocks_loaded(), before, "memtable hits read no runs");
    engine.flush().unwrap();
    let before = ws.blocks_loaded();
    engine
        .multi_get(&[b"c"; 100], &mut ws, |_, value| {
            assert_eq!(value, Some(&b"new"[..]))
        })
        .unwrap();
    assert_eq!(
        ws.blocks_loaded() - before,
        1,
        "duplicate probes decode a block only once"
    );
    engine
        .multi_get::<&[u8]>(&[], &mut ws, |_, _| panic!("empty input"))
        .unwrap();
}

#[test]
fn random_batches_ingestion_snapshots_and_multi_get_match_a_map() {
    use std::collections::BTreeMap;
    use tessera::engine::{CompactionOptions, Compression, MergeWorkspace};
    for seed in 1..=12u64 {
        let mut rng = seed;
        let mut next = |bound: u64| {
            rng ^= rng << 13;
            rng ^= rng >> 7;
            rng ^= rng << 17;
            rng % bound
        };
        let engine = Engine::new(EngineOptions {
            memtable_budget: 2048,
            target_run_len: 512,
            bulk_ingest_min: Some(512),
            compression: if seed % 2 == 0 {
                Compression::Zstd
            } else {
                Compression::Uncompressed
            },
            ..EngineOptions::default()
        })
        .unwrap();
        let mut model = BTreeMap::<Vec<u8>, Vec<u8>>::new();
        let mut snapshots = Vec::new();
        let mut ws = EngineWorkspace::new();
        let mut merge = MergeWorkspace::new();
        for round in 0..250u64 {
            match next(9) {
                0 => {
                    let key = next(80).to_be_bytes().to_vec();
                    let value = round.to_le_bytes().to_vec();
                    engine.put(&key, &value).unwrap();
                    model.insert(key, value);
                }
                1 => {
                    let key = next(80).to_be_bytes().to_vec();
                    engine.delete(&key).unwrap();
                    model.remove(&key);
                }
                2..=4 => {
                    let mut operations = Vec::new();
                    for _ in 0..2 + next(18) {
                        let key = next(80).to_be_bytes().to_vec();
                        let value =
                            (next(4) != 0).then(|| vec![round as u8; (next(160) + 1) as usize]);
                        operations.push((key, value));
                    }
                    let sorted = next(2) == 0;
                    if sorted {
                        operations.sort_by(|a, b| a.0.cmp(&b.0));
                        operations.dedup_by(|a, b| a.0 == b.0);
                    }
                    let mut batch = WriteBatch::new();
                    for (key, value) in &operations {
                        match value {
                            Some(value) => batch.put(key, value),
                            None => batch.delete(key),
                        }
                    }
                    if sorted {
                        engine.ingest_sorted(&batch).unwrap();
                    } else {
                        engine.write(&batch).unwrap();
                    }
                    for (key, value) in operations {
                        match value {
                            Some(value) => {
                                model.insert(key, value);
                            }
                            None => {
                                model.remove(&key);
                            }
                        }
                    }
                }
                5 => {
                    if snapshots.len() == 4 {
                        snapshots.remove(0);
                    }
                    snapshots.push((engine.snapshot().unwrap(), model.clone()));
                }
                6 => {
                    engine.flush().unwrap();
                }
                7 => {
                    engine
                        .compact(CompactionOptions::default(), &mut merge)
                        .unwrap();
                }
                _ => {}
            }
            let mut keys: Vec<_> = (0..20).map(|_| next(100).to_be_bytes().to_vec()).collect();
            keys.push(keys[0].clone());
            keys.push(Vec::new());
            if round % 2 == 0 {
                keys.sort();
            }
            let expected: Vec<_> = keys.iter().map(|key| model.get(key).cloned()).collect();
            let gets: Vec<_> = keys
                .iter()
                .map(|key| engine.get(key, &mut ws).unwrap().map(|v| v.value.to_vec()))
                .collect();
            assert_eq!(gets, expected, "seed {seed} round {round}");
            let mut actual = Vec::new();
            engine
                .multi_get(&keys, &mut ws, |_, value| {
                    actual.push(value.map(<[u8]>::to_vec))
                })
                .unwrap();
            assert_eq!(actual, expected, "seed {seed} round {round}");
            actual.clear();
            engine
                .read()
                .multi_get(&keys, &mut ws, |_, value| {
                    actual.push(value.map(<[u8]>::to_vec))
                })
                .unwrap();
            assert_eq!(actual, expected);
            for (snapshot, model) in &snapshots {
                actual.clear();
                snapshot
                    .multi_get(&keys, &mut ws, |_, value| {
                        actual.push(value.map(<[u8]>::to_vec))
                    })
                    .unwrap();
                let expected: Vec<_> = keys.iter().map(|key| model.get(key).cloned()).collect();
                assert_eq!(actual, expected);
            }
            if round % 25 == 0 {
                let snapshot = engine.snapshot().unwrap();
                let mut scan = snapshot.scan(&mut ws);
                let mut actual = BTreeMap::new();
                while let Some(record) = scan.next_record().unwrap() {
                    actual.insert(record.key.to_vec(), record.value.to_vec());
                }
                assert_eq!(actual, model);
            }
        }
        engine.flush().unwrap();
        engine.wait_for_compaction().unwrap();
    }
}

#[test]
fn rejected_batches_and_refused_ingestion_leave_no_partial_data_or_sequences() {
    use tessera::engine::MAX_KEY_LEN;
    let engine = Engine::new(EngineOptions {
        max_run_bytes: 1,
        background_compaction: None,
        ..EngineOptions::default()
    })
    .unwrap();
    let mut batch = WriteBatch::new();
    batch.put(b"a", b"value");
    batch.put(b"b", b"value");
    assert!(matches!(
        engine.ingest_sorted(&batch),
        Err(UpdateError::FlushRefused(_))
    ));
    assert_eq!(engine.stats().last_seq, 0);
    let mut ws = EngineWorkspace::new();
    assert!(engine.get(b"a", &mut ws).unwrap().is_none());
    batch.put(&vec![b'x'; MAX_KEY_LEN + 1], b"bad");
    assert!(matches!(
        engine.write(&batch),
        Err(UpdateError::Rejected(_))
    ));
    assert_eq!(engine.stats().last_seq, 0);
    assert!(engine.get(b"a", &mut ws).unwrap().is_none());
    engine.set_run_limits(100, usize::MAX).unwrap();
    batch.clear();
    batch.put(b"a", b"value");
    batch.put(b"b", b"value");
    assert_eq!(engine.ingest_sorted(&batch), Ok(1));
}
