//! Ingestion builds against an optimistic sequence ceiling, drains overlapping
//! tables, then commits its sequence range and runs in one publication.
//!
//! Reserving sequences before the build also needs compaction coordination:
//! while no ingested run is visible, compaction can erase a later write's
//! sequence or discard its tombstone. Publishing the reserved, older data
//! afterward would lose the write or resurrect the deleted key.
use super::*;
use crate::run::RunBuilder;

impl Core {
    pub(super) fn ingest_batch(
        &self,
        batch: &WriteBatch,
        sorted: bool,
    ) -> Result<u64, UpdateError> {
        if batch.is_empty() {
            return Ok(self.read_state().last_seq);
        }
        'retry: loop {
            let start = self.read_state().last_seq;
            let end = start
                .checked_add(batch.len() as u64)
                .ok_or(UpdateError::SequencesExhausted)?;
            let runs = if sorted {
                let mut runs = Vec::new();
                let mut builder = None::<RunBuilder>;
                let mut bytes = 0;
                for (index, (key, value)) in batch.records().enumerate() {
                    builder
                        .get_or_insert_with(|| {
                            RunBuilder::with_target_block_compressed(
                                self.codec,
                                self.target_block_compressed,
                            )
                        })
                        .add(key, start + index as u64 + 1, value)
                        .expect("validated sorted batch");
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
            } else {
                // An oversized unsorted batch needs ordered staging, but never
                // expands the active table or exposes a partial batch.
                let budget = batch.validate(self.memtable_budget)?;
                let mut table = Memtable::new(budget, Some(start));
                for (index, (key, value)) in batch.records().enumerate() {
                    let seq = start + index as u64 + 1;
                    match value {
                        Value::Bytes(v) => table.put(key, seq, v),
                        Value::Tombstone => table.delete(key, seq),
                    }
                    .expect("the private table admits the whole batch");
                }
                table
                    .freeze()
                    .flush_with(
                        self.target_run_len,
                        self.codec,
                        self.target_block_compressed,
                    )
                    .collect()
            };
            #[cfg(test)]
            {
                let hook = self.after_ingest_build.lock().unwrap().take();
                if let Some(hook) = hook {
                    hook();
                }
            }
            let bytes: usize = runs.iter().map(|run| run.as_bytes().len()).sum();
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
            let runs: Vec<_> = published(runs).collect();
            let lo = runs.first().unwrap().run.key_range().unwrap().min;
            let hi = runs.last().unwrap().run.key_range().unwrap().max;
            let mut state = self.write_state();
            loop {
                // Sequences are committed only at publication. If any writer or
                // another ingest won the race, rebuild against its new ceiling.
                // No global writer mutex is held during building or waiting.
                if state.last_seq != start {
                    drop(state);
                    continue 'retry;
                }
                // Checking again after each wait includes writes made while the
                // ingestion built, and closes the check/publication race.
                if state
                    .active
                    .iter_from(lo)
                    .next()
                    .is_some_and(|record| record.key <= hi)
                {
                    state = self.make_room(state)?;
                    continue;
                }
                if state.frozen.iter().any(|table| {
                    table
                        .iter_from(lo)
                        .next()
                        .is_some_and(|record| record.key <= hi)
                }) {
                    if let Some(error) = self.flush_error(&state) {
                        return Err(UpdateError::FlushRefused(error));
                    }
                    self.flush_signal.request();
                    state = self.wait_for_flush(state);
                    continue;
                }
                state = self.wait_for_level0(state);
                if state.last_seq != start {
                    drop(state);
                    continue 'retry;
                }
                // wait_for_level0 can release state, so recheck all overlaps.
                if state
                    .active
                    .iter_from(lo)
                    .next()
                    .is_some_and(|record| record.key <= hi)
                    || state.frozen.iter().any(|table| {
                        table
                            .iter_from(lo)
                            .next()
                            .is_some_and(|record| record.key <= hi)
                    })
                {
                    continue;
                }
                break;
            }
            let total_runs = state.runs.len() + runs.len();
            let total_bytes = state.run_bytes + state.held_bytes + bytes + held;
            let flushes = state.levels.level0_flushes() + 1;
            if total_runs > state.max_runs
                || total_bytes > state.max_run_bytes
                || flushes > state.level0_stop
            {
                return Err(UpdateError::FlushRefused(state.run_limit(
                    total_runs,
                    total_bytes,
                    flushes,
                )));
            }
            state.levels.push_flush(&runs, logical);
            state.runs.extend(runs);
            state.run_bytes += bytes;
            state.held_bytes += held;
            state.order_runs();
            state.last_seq = end;
            drop(state);
            self.notify_worker();
            return Ok(end);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Barrier;

    fn settle(engine: &Engine) {
        let options = CompactionOptions {
            max_overlap: 1,
            ..CompactionOptions::default()
        };
        let mut merge = MergeWorkspace::new();
        while engine.compact(options, &mut merge).unwrap().is_some() {}
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
                // Early reservation would give ingestion a positive sequence,
                // so this newer value would lose to it after publication.
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
                let snapshot = engine.snapshot().unwrap();
                check_snapshot(&snapshot, [Some(b"new"), None, None]);
                resume.wait();
                ingest.join().unwrap();
                snapshot
            });
            // Preserve publication-time ordering until compaction can account
            // for pending ingestions. All reader paths agree after publication.
            let mut ws = EngineWorkspace::new();
            for key in [b"a", b"b", b"c"] {
                assert_eq!(engine.get(key, &mut ws).unwrap().unwrap().value, b"ingest");
            }
            engine
                .multi_get(&[b"a", b"b", b"c"], &mut ws, |_, value| {
                    assert_eq!(value, Some(&b"ingest"[..]));
                })
                .unwrap();
            check_snapshot(&engine.snapshot().unwrap(), [Some(b"ingest"); 3]);
            check_snapshot(&snapshot, [Some(b"new"), None, None]);
            drop(snapshot);
            settle(&engine);
            check_snapshot(&engine.snapshot().unwrap(), [Some(b"ingest"); 3]);
            assert_eq!(engine.stats().last_seq, 7);
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
        assert_eq!(engine.ingest_sorted(&batch), Ok(3));
        let recovered = Engine::recover(&dir, EngineOptions::default()).unwrap();
        assert_eq!(recovered.stats().last_seq, 1);
        check_snapshot(
            &recovered.snapshot().unwrap(),
            [Some(b"before"), None, None],
        );
        assert_eq!(recovered.put(b"c", b"after recovery"), Ok(2));
        check_snapshot(
            &engine.snapshot().unwrap(),
            [Some(b"ingest"), Some(b"ingest"), None],
        );
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
        check_snapshot(&engine.snapshot().unwrap(), [Some(b"before"), None, None]);
        assert_eq!(engine.ingest_sorted(&batch), Ok(3));
        check_snapshot(
            &engine.snapshot().unwrap(),
            [Some(b"ingest"), Some(b"ingest"), None],
        );
    }
}
