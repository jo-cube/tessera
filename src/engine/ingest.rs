//! Direct run ingestion builds once with placeholder sequences. Publication
//! assigns one effective sequence to the whole batch, after draining only tables
//! that share an actual batch key. Compaction materializes it; checkpoints preserve the override.
use super::*;
use crate::run::RunBuilder;

/// Whether an ordered table shares any key with the sorted, unique batch keys.
/// Starting the table at the first batch key bounds the walk to their range.
fn overlaps<'a>(records: impl Iterator<Item = Record<'a>>, keys: &[&[u8]]) -> bool {
    let mut keys = keys.iter().copied().peekable();
    for record in records {
        while keys.peek().is_some_and(|key| *key < record.key) {
            keys.next();
        }
        match keys.peek() {
            Some(key) if *key == record.key => return true,
            None => return false,
            _ => {}
        }
    }
    false
}

impl Core {
    fn build_ingestion<'a>(
        &self,
        records: impl Iterator<Item = (&'a [u8], Value<'a>)>,
    ) -> Vec<Run<Vec<u8>>> {
        let mut runs = Vec::new();
        let mut builder = None::<RunBuilder>;
        let mut bytes = 0;
        for (key, value) in records {
            builder
                .get_or_insert_with(|| {
                    RunBuilder::in_pool(self.codec, self.target_block_compressed, &self.chunks)
                })
                .add(key, 0, value)
                .expect("validated unique sorted records");
            bytes += key.len()
                + match value {
                    Value::Bytes(v) => v.len(),
                    Value::Tombstone => 0,
                }
                + RECORD_INPUT_OVERHEAD;
            if bytes >= self.target_run_len {
                runs.push(builder.take().unwrap().finish());
                bytes = 0;
            }
        }
        runs.extend(builder.map(RunBuilder::finish));
        runs
    }

    pub(super) fn ingest_batch(
        &self,
        batch: &WriteBatch,
        sorted: bool,
    ) -> Result<u64, UpdateError> {
        let ceiling = self.read_state().last_seq;
        if batch.is_empty() {
            return Ok(ceiling);
        }
        ceiling
            .checked_add(1)
            .ok_or(UpdateError::SequencesExhausted)?;
        let state = self.admit(self.write_state())?;
        let input = batch.validate(self.memtable_budget)?;
        let mut charge = self.resources.build(
            input.saturating_mul(3),
            crate::run::workspace_estimate([]).saturating_add(if sorted { 0 } else { input }),
        );
        drop(state);
        let mut keys: Vec<_> = batch.records().map(|(key, _)| key).collect();
        let runs = if sorted {
            self.build_ingestion(batch.records())
        } else {
            // Local staging sequences choose the last duplicate operation.
            // The final unique records are built with zero placeholders too.
            keys.sort_unstable();
            keys.dedup();
            let budget = batch.validate(self.memtable_budget)?;
            let mut table = Memtable::in_pool(budget, None, &self.chunks);
            for (index, (key, value)) in batch.records().enumerate() {
                let seq = index as u64 + 1;
                match value {
                    Value::Bytes(v) => table.put(key, seq, v),
                    Value::Tombstone => table.delete(key, seq),
                }
                .expect("the private table admits the whole batch");
            }
            self.build_ingestion(table.iter().map(|record| (record.key, record.value)))
        };
        #[cfg(test)]
        {
            let hook = self.after_ingest_build.lock().unwrap().take();
            if let Some(hook) = hook {
                hook();
            }
        }
        let bytes: usize = runs.iter().map(|run| run.stored_len()).sum();
        let held: usize = runs.iter().map(Run::held_bytes).sum();
        let logical = batch
            .records()
            .map(|(key, value)| {
                key.len()
                    + match value {
                        Value::Bytes(v) => v.len(),
                        Value::Tombstone => 0,
                    }
                    + RECORD_INPUT_OVERHEAD
            })
            .sum();
        charge.finished(bytes + held);
        let mut runs: Vec<_> = published(runs).collect();
        let (lo, hi) = (keys[0], keys[keys.len() - 1]);
        let mut state = self.write_state();
        // Freeze a potentially large table cheaply, then inspect it unlocked.
        // At the frozen limit, probe active exactly below: unrelated keys must
        // not make ingestion wait for a slot just to perform a coarse check.
        if state.frozen.len() < self.max_frozen_memtables
            && state
                .active
                .iter_from(lo)
                .next()
                .is_some_and(|record| record.key <= hi)
        {
            self.freeze_active(&mut state);
            self.flush_signal.request();
        }
        let mut checked_through = 0;
        loop {
            state = self.wait_for_level0(state);
            // Immutable tables need checking only once. Last-sequence marks
            // increase strictly as non-empty active tables are frozen.
            let tables: Vec<_> = state
                .frozen
                .iter()
                .filter(|table| table.last_seq().is_some_and(|seq| seq > checked_through))
                .cloned()
                .collect();
            if let Some(last) = tables.last() {
                checked_through = last.last_seq().unwrap();
                drop(state);
                let target = tables
                    .iter()
                    .filter(|table| overlaps(table.iter_from(lo), &keys))
                    .filter_map(|table| table.last_seq())
                    .max();
                drop(tables);
                state = self.write_state();
                while target.is_some_and(|target| {
                    state
                        .frozen
                        .first()
                        .is_some_and(|table| table.last_seq().is_some_and(|seq| seq <= target))
                }) {
                    self.flush_signal.request();
                    state = self.wait_for_flush(state);
                }
                // Include tables frozen by writers while state was released.
                continue;
            }
            if overlaps(state.active.iter_from(lo), &keys) {
                state = self.make_room(state);
                continue;
            }
            break;
        }
        let seq = state
            .last_seq
            .checked_add(1)
            .ok_or(UpdateError::SequencesExhausted)?;
        for run in &mut runs {
            Arc::get_mut(run)
                .expect("unpublished run is private")
                .run
                .set_seq_override(seq);
        }
        state.levels.push_flush(&runs, logical);
        state.runs.extend(runs);
        state.run_bytes += bytes;
        state.held_bytes += held;
        state.order_runs();
        state.last_seq = seq;
        drop(charge);
        drop(state);
        self.resources.changed();
        self.notify_worker();
        Ok(seq)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Barrier;

    fn settle(engine: &Engine) {
        let options = CompactionPolicy {
            max_overlap: 1,
            ..CompactionPolicy::default()
        };
        let mut merge = MergeWorkspace::new();
        while engine.core.compact(options, &mut merge).unwrap().is_some() {}
    }

    fn check_snapshot(snapshot: &Snapshot, expected: [Option<&[u8]>; 3]) {
        let keys: &[&[u8]] = &[b"a", b"b", b"c"];
        let mut ws = EngineWorkspace::new();
        for (key, value) in keys.iter().zip(expected) {
            assert_eq!(snapshot.get(key, &mut ws).unwrap().map(|v| v.value), value);
        }
        snapshot
            .multi_get(keys, &mut ws, |i, value| assert_eq!(value, expected[i]))
            .unwrap();
        let mut scan = snapshot.scan(&mut ws);
        for (key, value) in keys.iter().zip(expected) {
            if let Some(value) = value {
                let record = scan.next_record().unwrap().unwrap();
                assert_eq!((record.key, record.value), (*key, value));
            }
        }
        assert!(scan.next_record().unwrap().is_none());
    }

    #[test]
    fn ingestion_orders_after_writes_and_compaction_completed_during_its_build() {
        for compression in [Compression::Uncompressed, Compression::Zstd] {
            let engine = Engine::new(EngineOptions {
                compression,
                background_compaction: None,
                ..EngineOptions::default()
            })
            .unwrap();
            engine.put(b"a", b"base").unwrap();
            engine.put(b"b", b"base").unwrap();
            engine.flush().unwrap();
            settle(&engine);
            let built = Arc::new(Barrier::new(2));
            let resume = Arc::new(Barrier::new(2));
            let (a, b) = (Arc::clone(&built), Arc::clone(&resume));
            *engine.core.after_ingest_build.lock().unwrap() = Some(Arc::new(move || {
                a.wait();
                b.wait();
            }));
            let snapshot = std::thread::scope(|scope| {
                let ingest = scope.spawn(|| {
                    let mut batch = WriteBatch::new();
                    batch.put(b"a", b"ingest");
                    batch.put(b"b", b"ingest");
                    batch.put(b"c", b"ingest");
                    engine.ingest_sorted(&batch).unwrap();
                });
                built.wait();
                engine.put(b"a", b"new").unwrap();
                engine.delete(b"b").unwrap();
                engine.flush().unwrap();
                settle(&engine);
                let mut ws = EngineWorkspace::new();
                let newer = engine.get(b"a", &mut ws).unwrap().unwrap();
                // Compaction can settle earlier writes while ingestion builds;
                // its later publication sequence must still win.
                assert_eq!((newer.seq, newer.value), (0, &b"new"[..]));
                // The newer deletion has also been physically discarded.
                for run in &engine.core.read_state().runs {
                    assert!(
                        run.run
                            .get(b"b", &mut crate::run::ReadWorkspace::new())
                            .unwrap()
                            .is_none()
                    );
                }
                let snapshot = engine.snapshot();
                check_snapshot(&snapshot, [Some(b"new"), None, None]);
                resume.wait();
                ingest.join().unwrap();
                snapshot
            });
            // One publication sequence orders the already-built ingestion
            // after those settled writes. All reader paths agree.
            let mut ws = EngineWorkspace::new();
            for key in [b"a", b"b", b"c"] {
                assert_eq!(engine.get(key, &mut ws).unwrap().unwrap().value, b"ingest");
            }
            engine
                .multi_get(&[b"a", b"b", b"c"], &mut ws, |_, value| {
                    assert_eq!(value, Some(&b"ingest"[..]));
                })
                .unwrap();
            check_snapshot(&engine.snapshot(), [Some(b"ingest"); 3]);
            check_snapshot(&snapshot, [Some(b"new"), None, None]);
            drop(snapshot);
            settle(&engine);
            check_snapshot(&engine.snapshot(), [Some(b"ingest"); 3]);
            assert_eq!(engine.stats().last_seq, 5);
        }
    }

    #[test]
    fn a_checkpoint_during_ingestion_recovers_without_the_unpublished_batch() {
        let engine = Arc::new(Engine::new(EngineOptions::default()).unwrap());
        engine.put(b"a", b"before").unwrap();
        let dir = std::env::temp_dir().join(format!(
            "tessera-inflight-ingest-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let checkpoint_dir = dir.clone();
        let checkpoint_engine = Arc::clone(&engine);
        *engine.core.after_ingest_build.lock().unwrap() = Some(Arc::new(move || {
            checkpoint_engine
                .checkpoint(&checkpoint_dir, CheckpointOptions::default())
                .unwrap();
        }));
        let mut batch = WriteBatch::new();
        batch.put(b"a", b"ingest");
        batch.put(b"b", b"ingest");
        assert_eq!(engine.ingest_sorted(&batch), Ok(2));
        let recovered = Engine::recover(&dir, EngineOptions::default()).unwrap();
        assert_eq!(recovered.stats().last_seq, 1);
        check_snapshot(&recovered.snapshot(), [Some(b"before"), None, None]);
        assert_eq!(recovered.put(b"c", b"after recovery"), Ok(2));
        check_snapshot(&engine.snapshot(), [Some(b"ingest"), Some(b"ingest"), None]);
        recovered.close().unwrap();
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn a_panicking_ingestion_build_publishes_nothing() {
        let engine = Engine::new(EngineOptions::default()).unwrap();
        engine.put(b"a", b"before").unwrap();
        let mut batch = WriteBatch::new();
        batch.put(b"a", b"ingest");
        batch.put(b"b", b"ingest");
        *engine.core.after_ingest_build.lock().unwrap() =
            Some(Arc::new(|| panic!("injected ingestion failure")));
        assert!(
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                engine.ingest_sorted(&batch)
            }))
            .is_err()
        );
        assert_eq!(engine.stats().last_seq, 1);
        check_snapshot(&engine.snapshot(), [Some(b"before"), None, None]);
        assert_eq!(engine.ingest_sorted(&batch), Ok(2));
        check_snapshot(&engine.snapshot(), [Some(b"ingest"), Some(b"ingest"), None]);
    }

    fn check_engine(engine: &Engine, expected: [Option<&[u8]>; 3]) {
        let mut ws = EngineWorkspace::new();
        for (key, value) in [b"a", b"b", b"c"].iter().zip(expected) {
            assert_eq!(engine.get(*key, &mut ws).unwrap().map(|v| v.value), value);
        }
        let keys = [b"c", b"a", b"b", b"c"];
        let values = [expected[2], expected[0], expected[1], expected[2]];
        engine
            .multi_get(&keys, &mut ws, |i, value| assert_eq!(value, values[i]))
            .unwrap();
        {
            let read = engine.read();
            read.multi_get(&keys, &mut ws, |i, value| assert_eq!(value, values[i]))
                .unwrap();
            for (key, value) in [b"a", b"b", b"c"].iter().zip(expected) {
                assert_eq!(read.get(*key, &mut ws).unwrap().map(|v| v.value), value);
            }
            let mut scan = read.scan(&mut ws);
            for (key, value) in [b"a", b"b", b"c"].iter().zip(expected) {
                if let Some(value) = value {
                    let record = scan.next_record().unwrap().unwrap();
                    assert_eq!((record.key, record.value), (&key[..], value));
                }
            }
            assert!(scan.next_record().unwrap().is_none());
        }
        check_snapshot(&engine.snapshot(), expected);
    }

    #[test]
    fn unrelated_writes_inside_the_batch_range_never_wait_for_the_flush_worker() {
        for full in [false, true] {
            let engine = Arc::new(
                Engine::new(EngineOptions {
                    max_frozen_memtables: 1,
                    background_compaction: None,
                    ..EngineOptions::default()
                })
                .unwrap(),
            );
            // A publication that waited for any unrelated table would deadlock
            // here. Test both a free frozen slot and an already full queue.
            let flush = engine.core.serialize_flush();
            if full {
                engine.put(b"b", b"old unrelated").unwrap();
                let mut state = engine.core.write_state();
                engine.core.freeze_active(&mut state);
                engine.core.flush_signal.request();
            }
            let during = Arc::clone(&engine);
            *engine.core.after_ingest_build.lock().unwrap() = Some(Arc::new(move || {
                during.put(b"b", b"unrelated").unwrap();
                *during.core.after_ingest_build.lock().unwrap() =
                    Some(Arc::new(|| panic!("ingestion rebuilt")));
            }));
            let mut batch = WriteBatch::new();
            batch.put(b"a", b"ingest");
            batch.put(b"c", b"ingest");
            let seq = engine.ingest_sorted(&batch).unwrap();
            assert_eq!(seq, if full { 3 } else { 2 });
            assert!(
                engine
                    .core
                    .after_ingest_build
                    .lock()
                    .unwrap()
                    .take()
                    .is_some()
            );
            assert_eq!(engine.stats().flushes, 0);
            check_engine(
                &engine,
                [Some(b"ingest"), Some(b"unrelated"), Some(b"ingest")],
            );
            drop(flush);
            engine.flush().unwrap();
            settle(&engine);
            check_engine(
                &engine,
                [Some(b"ingest"), Some(b"unrelated"), Some(b"ingest")],
            );
        }
    }

    #[test]
    fn same_key_writes_and_deletes_order_at_publication_in_every_reader() {
        for compression in [Compression::Uncompressed, Compression::Zstd] {
            let engine = Arc::new(
                Engine::new(EngineOptions {
                    compression,
                    background_compaction: None,
                    ..EngineOptions::default()
                })
                .unwrap(),
            );
            for key in [b"a", b"b", b"c"] {
                engine.put(key, b"before").unwrap();
            }
            let during = Arc::clone(&engine);
            *engine.core.after_ingest_build.lock().unwrap() = Some(Arc::new(move || {
                during.put(b"a", b"during build").unwrap();
                during.delete(b"b").unwrap();
            }));
            let during = Arc::clone(&engine);
            *engine.core.before_flush_build.lock().unwrap() = Some(Arc::new(move || {
                // Land in the new active table while ingestion waits for its
                // first frozen overlap. Publication must drain these too.
                during.put(b"a", b"during flush").unwrap();
                during.delete(b"b").unwrap();
            }));
            let mut batch = WriteBatch::new();
            for key in [b"a", b"b", b"c"] {
                batch.put(key, b"ingest");
            }
            let seq = engine.ingest_sorted(&batch).unwrap();
            assert_eq!(seq, 8);
            check_engine(&engine, [Some(b"ingest"); 3]);
            let mut ws = EngineWorkspace::new();
            for key in [b"a", b"b", b"c"] {
                assert_eq!(engine.get(key, &mut ws).unwrap().unwrap().seq, seq);
            }
            let at_publication = engine.snapshot();
            assert_eq!(engine.put(b"a", b"after"), Ok(seq + 1));
            assert_eq!(engine.delete(b"b"), Ok(seq + 2));
            check_engine(&engine, [Some(b"after"), None, Some(b"ingest")]);
            engine.flush().unwrap();
            settle(&engine);
            check_engine(&engine, [Some(b"after"), None, Some(b"ingest")]);
            check_snapshot(&at_publication, [Some(b"ingest"); 3]);
        }
    }

    #[test]
    fn effective_sequences_materialize_through_compaction_checkpoint_and_recovery() {
        for compression in [Compression::Uncompressed, Compression::Zstd] {
            for pinned in [false, true] {
                let options = EngineOptions {
                    compression,
                    target_run_len: 1,
                    background_compaction: None,
                    ..EngineOptions::default()
                };
                let engine = Engine::new(options).unwrap();
                for key in [b"a", b"b", b"c"] {
                    engine.put(key, b"old").unwrap();
                }
                engine.flush().unwrap();
                let mut batch = WriteBatch::new();
                batch.put(b"a", b"ingest");
                batch.delete(b"b");
                batch.put(b"c", b"ingest");
                let seq = engine.ingest_sorted(&batch).unwrap();
                assert_eq!(seq, 4);
                assert_eq!(
                    engine
                        .core
                        .read_state()
                        .runs
                        .iter()
                        .filter(|run| run.run.max_seq() == seq)
                        .count(),
                    3
                );
                let pin = pinned.then(|| engine.snapshot());
                // Export overrides before a compaction has materialized them,
                // then repeat after either preserving sequences or zeroing them.
                let dir = std::env::temp_dir().join(format!(
                    "tessera-effective-ingest-{}-{compression:?}-{pinned}",
                    std::process::id()
                ));
                for compacted in [false, true] {
                    if compacted {
                        settle(&engine);
                    }
                    let expected = if compacted && !pinned { 0 } else { seq };
                    check_engine(&engine, [Some(b"ingest"), None, Some(b"ingest")]);
                    let mut ws = EngineWorkspace::new();
                    for key in [b"a", b"c"] {
                        assert_eq!(engine.get(key, &mut ws).unwrap().unwrap().seq, expected);
                    }
                    engine
                        .checkpoint(&dir, CheckpointOptions::default())
                        .unwrap();
                    let recovered = Engine::recover(&dir, options).unwrap();
                    assert_eq!(recovered.stats().last_seq, seq);
                    check_engine(&recovered, [Some(b"ingest"), None, Some(b"ingest")]);
                    for key in [b"a", b"c"] {
                        assert_eq!(recovered.get(key, &mut ws).unwrap().unwrap().seq, expected);
                    }
                    assert_eq!(recovered.put(b"b", b"after recovery"), Ok(seq + 1));
                    check_engine(
                        &recovered,
                        [Some(b"ingest"), Some(b"after recovery"), Some(b"ingest")],
                    );
                    recovered.close().unwrap();
                }
                if let Some(pin) = pin {
                    check_snapshot(&pin, [Some(b"ingest"), None, Some(b"ingest")]);
                }
                std::fs::remove_dir_all(&dir).unwrap();
            }
        }
    }

    #[test]
    fn ingestion_uses_the_last_sequence_once_and_rechecks_exhaustion_at_publication() {
        let engine = Engine::new(EngineOptions {
            last_seq: u64::MAX - 1,
            ..EngineOptions::default()
        })
        .unwrap();
        let mut batch = WriteBatch::new();
        batch.put(b"a", b"ingest");
        batch.put(b"c", b"ingest");
        assert_eq!(engine.ingest_sorted(&batch), Ok(u64::MAX));
        assert_eq!(engine.ingest_sorted(&WriteBatch::new()), Ok(u64::MAX));
        assert_eq!(
            engine.ingest_sorted(&batch),
            Err(UpdateError::SequencesExhausted)
        );
        check_engine(&engine, [Some(b"ingest"), None, Some(b"ingest")]);
        let engine = Arc::new(
            Engine::new(EngineOptions {
                last_seq: u64::MAX - 1,
                ..EngineOptions::default()
            })
            .unwrap(),
        );
        let during = Arc::clone(&engine);
        *engine.core.after_ingest_build.lock().unwrap() = Some(Arc::new(move || {
            assert_eq!(during.put(b"b", b"last write"), Ok(u64::MAX));
        }));
        assert_eq!(
            engine.ingest_sorted(&batch),
            Err(UpdateError::SequencesExhausted)
        );
        assert_eq!(engine.stats().last_seq, u64::MAX);
        check_engine(&engine, [None, Some(b"last write"), None]);
    }
}
