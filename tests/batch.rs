use crate::engine::{
    Engine, ErrorKind, Keyspace, Options, Reader, Snapshot, UpdateError, WriteBatch,
};

/// `keys` through one multi-get, as owned values.
fn multi(
    reader: &mut Reader<'_>,
    ks: &Keyspace,
    keys: &[impl AsRef<[u8]>],
) -> Vec<Option<Vec<u8>>> {
    let values = reader.multi_get(ks, keys).unwrap();
    assert_eq!(values.len(), keys.len());
    values
        .iter()
        .map(|value| value.map(<[u8]>::to_vec))
        .collect()
}

fn value(snapshot: &Snapshot, ks: &Keyspace, key: &[u8]) -> Option<Vec<u8>> {
    snapshot.reader().get(ks, key).unwrap().map(<[u8]>::to_vec)
}

#[test]
fn batches_are_ordered_atomic_and_reusable() {
    let engine = Engine::in_memory(Options::default()).unwrap();
    let ks = engine.ks();
    let mut batch = WriteBatch::new();
    batch.put(&ks, b"a", b"first");
    batch.put(&ks, b"b", b"second");
    batch.delete(&ks, b"a");
    batch.put(&ks, b"a", b"last");
    assert_eq!(batch.len(), 4);
    assert_eq!(engine.write_seq(&batch), Ok(4));
    let snapshot = engine.snapshot();
    batch.clear();
    assert!(batch.is_empty());
    assert_eq!(engine.write_seq(&batch), Ok(4));
    engine.write(&batch).unwrap();
    batch.delete(&ks, b"b");
    batch.put(&ks, b"c", b"new");
    assert_eq!(engine.write_seq(&batch), Ok(6));
    assert_eq!(value(&snapshot, &ks, b"a").unwrap(), b"last");
    assert!(ks.get(b"b").unwrap().is_none());
    assert_eq!(value(&snapshot, &ks, b"b").unwrap(), b"second");
}

#[test]
#[should_panic(expected = "sequence numbers are exhausted")]
fn a_batch_after_the_last_sequence_number_panics() {
    let engine = Engine::in_memory(Options {
        last_seq: u64::MAX - 1,
        ..Options::default()
    })
    .unwrap();
    let ks = engine.ks();
    let mut batch = WriteBatch::new();
    batch.put(&ks, b"a", b"a");
    batch.put(&ks, b"b", b"b");
    let _ = engine.write(&batch);
}

#[test]
fn readers_and_snapshots_see_whole_batches() {
    let engine = Engine::in_memory(Options {
        memtable_size: 1024,
        ..Options::default()
    })
    .unwrap();
    let ks = engine.ks();
    std::thread::scope(|scope| {
        let writer = scope.spawn(|| {
            let mut batch = WriteBatch::new();
            for round in 0..500u64 {
                batch.clear();
                for key in [b"a", b"b", b"c"] {
                    batch.put(&ks, key, &round.to_le_bytes());
                }
                engine.write(&batch).unwrap();
            }
        });
        let mut reader = engine.reader();
        for _ in 0..500 {
            let values = multi(&mut reader, &ks, &[b"a", b"b", b"c"]);
            assert!(values.windows(2).all(|pair| pair[0] == pair[1]));
            let snapshot = engine.snapshot();
            let a = value(&snapshot, &ks, b"a");
            let b = value(&snapshot, &ks, b"b");
            let c = value(&snapshot, &ks, b"c");
            assert_eq!(a, b);
            assert_eq!(b, c);
        }
        writer.join().unwrap();
    });
}

#[test]
fn ingestion_drains_overlapping_tables_and_keeps_snapshots() {
    let engine = Engine::in_memory(Options::default()).unwrap();
    let ks = engine.ks();
    ks.put(b"a", b"old").unwrap();
    ks.put(b"c", b"old").unwrap();
    let snapshot = engine.snapshot();
    let mut batch = WriteBatch::new();
    batch.put(&ks, b"a", b"ingested");
    batch.delete(&ks, b"c");
    assert_eq!(engine.ingest_sorted(&batch), Ok(3));
    assert_eq!(engine.engine_stats().active.charged, 0);
    assert!(engine.engine_stats().frozen.is_none());
    assert_eq!(ks.get(b"a").unwrap().unwrap(), b"ingested");
    assert!(ks.get(b"c").unwrap().is_none());
    assert_eq!(value(&snapshot, &ks, b"a").unwrap(), b"old");
    ks.put(b"a", b"after").unwrap();
    engine.flush().unwrap();
    engine.wait_for_compaction().unwrap();
    assert_eq!(ks.get(b"a").unwrap().unwrap(), b"after");
}

#[test]
fn nonoverlapping_ingestion_leaves_the_active_table_alone() {
    let engine = Engine::in_memory(Options::default()).unwrap();
    let ks = engine.ks();
    ks.put(b"z", b"active").unwrap();
    let before = engine.engine_stats().active;
    let mut batch = WriteBatch::new();
    batch.put(&ks, b"a", b"one");
    batch.put(&ks, b"b", b"two");
    engine.ingest_sorted(&batch).unwrap();
    assert_eq!(engine.engine_stats().active, before);
    assert_eq!(engine.engine_stats().flushes, 0);
    assert_eq!(ks.get(b"z").unwrap().unwrap(), b"active");
    assert_eq!(ks.get(b"b").unwrap().unwrap(), b"two");
}

#[test]
fn a_sorted_batch_above_the_ingestion_threshold_is_ingested() {
    let engine = Engine::in_memory(Options {
        bulk_ingest_min: Some(1),
        ..Options::default()
    })
    .unwrap();
    let ks = engine.ks();
    let mut batch = WriteBatch::new();
    batch.put(&ks, b"a", b"v");
    batch.put(&ks, b"b", b"w");
    assert_eq!(engine.write_seq(&batch), Ok(1));
    assert_eq!(engine.engine_stats().active.charged, 0);
    assert_eq!(engine.engine_stats().runs, 1);
    // Unsorted or duplicate keys go through the memtable.
    for keys in [[b"d", b"c"], [b"e", b"e"]] {
        batch.clear();
        for key in keys {
            batch.put(&ks, key, b"v");
        }
        engine.write(&batch).unwrap();
        assert!(engine.engine_stats().active.charged > 0);
        engine.flush().unwrap();
    }
    assert_eq!(ks.get(b"c").unwrap().unwrap(), b"v");
    assert_eq!(ks.get(b"e").unwrap().unwrap(), b"v");
}

#[test]
fn oversized_unsorted_batches_keep_the_last_duplicate_and_publish_atomically() {
    let engine = Engine::in_memory(Options {
        memtable_size: 256,
        ..Options::default()
    })
    .unwrap();
    let ks = engine.ks();
    let mut batch = WriteBatch::new();
    for index in (0..40u64).rev() {
        batch.put(&ks, &index.to_be_bytes(), &index.to_le_bytes());
    }
    batch.delete(&ks, &7u64.to_be_bytes());
    batch.put(&ks, &3u64.to_be_bytes(), b"last");
    assert_eq!(engine.write_seq(&batch), Ok(1));
    assert_eq!(engine.engine_stats().active.charged, 0);
    assert!(ks.get(&7u64.to_be_bytes()).unwrap().is_none());
    assert_eq!(ks.get(&3u64.to_be_bytes()).unwrap().unwrap(), b"last");
}

#[test]
fn multi_get_preserves_order_duplicates_misses_and_deletes_in_every_view() {
    let engine = Engine::in_memory(Options::default()).unwrap();
    let ks = engine.ks();
    for (key, value) in [(b"a", b"old"), (b"b", b"old"), (b"c", b"old")] {
        ks.put(key, value).unwrap();
    }
    engine.flush().unwrap();
    ks.delete(b"b").unwrap();
    ks.put(b"c", b"new").unwrap();
    let snapshot = engine.snapshot();
    ks.put(b"a", b"later").unwrap();
    let keys: &[&[u8]] = &[b"c", b"missing", b"a", b"b", b"c", b"", b"a"];
    let expected: Vec<_> = keys.iter().map(|key| ks.get(key).unwrap()).collect();
    let mut reader = engine.reader();
    assert_eq!(multi(&mut reader, &ks, keys), expected);
    let expected: Vec<_> = keys.iter().map(|key| value(&snapshot, &ks, key)).collect();
    assert_eq!(multi(&mut snapshot.reader(), &ks, keys), expected);
    let before = reader.counters().blocks_loaded;
    assert!(
        multi(&mut reader, &ks, &[b"a"; 100])
            .iter()
            .all(|value| value.as_deref() == Some(&b"later"[..]))
    );
    assert_eq!(
        reader.counters().blocks_loaded,
        before,
        "memtable hits read no runs"
    );
    engine.flush().unwrap();
    let before = reader.counters().blocks_loaded;
    assert!(
        multi(&mut reader, &ks, &[b"c"; 100])
            .iter()
            .all(|value| value.as_deref() == Some(&b"new"[..]))
    );
    assert_eq!(
        reader.counters().blocks_loaded - before,
        1,
        "duplicate probes decode a block only once"
    );
    assert!(reader.multi_get::<&[u8]>(&ks, &[]).unwrap().is_empty());
}

#[test]
fn random_batches_ingestion_snapshots_and_multi_get_match_a_map() {
    use crate::engine::{CompactionPolicy, Compression, MergeWorkspace};
    use std::collections::BTreeMap;
    for seed in 1..=12u64 {
        let mut rng = seed;
        let mut next = |bound: u64| {
            rng ^= rng << 13;
            rng ^= rng >> 7;
            rng ^= rng << 17;
            rng % bound
        };
        let engine = Engine::in_memory(Options {
            memtable_size: 2048,
            target_run_len: 512,
            bulk_ingest_min: Some(512),
            compression: if seed % 2 == 0 {
                Compression::Zstd
            } else {
                Compression::Uncompressed
            },
            ..Options::default()
        })
        .unwrap();
        let ks = engine.ks();
        let mut model = BTreeMap::<Vec<u8>, Vec<u8>>::new();
        let mut snapshots = Vec::new();
        let mut merge = MergeWorkspace::new();
        for round in 0..250u64 {
            match next(9) {
                0 => {
                    let key = next(80).to_be_bytes().to_vec();
                    let value = round.to_le_bytes().to_vec();
                    ks.put(&key, &value).unwrap();
                    model.insert(key, value);
                }
                1 => {
                    let key = next(80).to_be_bytes().to_vec();
                    ks.delete(&key).unwrap();
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
                            Some(value) => batch.put(&ks, key, value),
                            None => batch.delete(&ks, key),
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
                    snapshots.push((engine.snapshot(), model.clone()));
                }
                6 => {
                    engine.flush().unwrap();
                }
                7 => {
                    engine
                        .core
                        .compact(CompactionPolicy::default(), &mut merge)
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
            let gets: Vec<_> = keys.iter().map(|key| ks.get(key).unwrap()).collect();
            assert_eq!(gets, expected, "seed {seed} round {round}");
            assert_eq!(
                multi(&mut engine.reader(), &ks, &keys),
                expected,
                "seed {seed} round {round}"
            );
            for (snapshot, model) in &snapshots {
                let expected: Vec<_> = keys.iter().map(|key| model.get(key).cloned()).collect();
                assert_eq!(multi(&mut snapshot.reader(), &ks, &keys), expected);
            }
            if round % 25 == 0 {
                let snapshot = engine.snapshot();
                let mut reader = snapshot.reader();
                let mut scan = reader.range(&ks, ..);
                let mut actual = BTreeMap::new();
                while let Some((key, value)) = scan.next().unwrap() {
                    actual.insert(key.to_vec(), value.to_vec());
                }
                assert_eq!(actual, model);
            }
        }
        engine.flush().unwrap();
        engine.wait_for_compaction().unwrap();
    }
}

#[test]
fn rejected_batches_and_memory_limited_ingestion_leave_no_partial_data_or_sequences() {
    use crate::engine::MAX_KEY_LEN;
    let engine = Engine::in_memory(Options {
        memory_limit: Some(0),
        background_compaction: None,
        ..Options::default()
    })
    .unwrap();
    let ks = engine.ks();
    let mut batch = WriteBatch::new();
    batch.put(&ks, b"a", b"value");
    batch.put(&ks, b"b", b"value");
    assert!(matches!(
        engine.ingest_sorted(&batch),
        Err(UpdateError::MemoryLimit { .. })
    ));
    assert_eq!(
        engine.write(&batch).unwrap_err().kind(),
        ErrorKind::MemoryLimit
    );
    assert_eq!(engine.engine_stats().last_seq, 0);
    assert!(ks.get(b"a").unwrap().is_none());
    batch.put(&ks, &vec![b'x'; MAX_KEY_LEN + 1], b"bad");
    assert_eq!(
        engine.write(&batch).unwrap_err().kind(),
        ErrorKind::InvalidInput
    );
    assert_eq!(engine.engine_stats().last_seq, 0);
    assert!(ks.get(b"a").unwrap().is_none());
    let engine = Engine::in_memory(Options::default()).unwrap();
    let ks = engine.ks();
    batch.clear();
    batch.put(&ks, b"a", b"value");
    batch.put(&ks, b"b", b"value");
    assert_eq!(engine.ingest_sorted(&batch), Ok(1));
}
