//! Direct run ingestion builds once with placeholder sequences. Publication
//! assigns one effective sequence to the whole batch, after draining only
//! tables that share an actual batch key. Compaction materializes it;
//! checkpoints preserve the override.
use super::batch::BatchSize;
use super::*;
use crate::run::RunBuilder;
use std::ops::Range;

/// Whether an ordered table shares any key with the sorted, unique batch keys.
/// Starting the table at the first batch key bounds the walk to their range.
fn overlaps<'a, 'k>(
    records: impl Iterator<Item = Record<'a>>,
    keys: impl Iterator<Item = &'k [u8]>,
) -> bool {
    let mut keys = keys.peekable();
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

/// An operation's position in the batch and its prefixed key in scratch.
struct IndexedKey {
    operation: usize,
    span: Range<usize>,
}

/// One keyspace's part of an ingested batch: its runs and their input.
struct Ingested {
    keyspace: u32,
    runs: Vec<Run<Vec<u8>>>,
    logical: usize,
}

impl Core {
    /// Builds sorted, unique records into runs of their own keyspace, with
    /// placeholder sequences.
    fn build_ingestion<'a>(
        &self,
        records: impl Iterator<Item = (u32, &'a [u8], Value<'a>)>,
    ) -> Vec<Ingested> {
        let mut built: Vec<Ingested> = Vec::new();
        let mut builder = None::<RunBuilder>;
        let mut bytes = 0;
        for (keyspace, key, value) in records {
            if built.last().is_none_or(|last| last.keyspace != keyspace) {
                if let Some(last) = built.last_mut() {
                    last.runs.extend(builder.take().map(RunBuilder::finish));
                }
                built.push(Ingested {
                    keyspace,
                    runs: Vec::new(),
                    logical: 0,
                });
                bytes = 0;
            }
            let space = built.last_mut().expect("a keyspace was begun");
            builder
                .get_or_insert_with(|| {
                    RunBuilder::in_pool(self.encoding, self.target_block_compressed, &self.chunks)
                })
                .add(key, 0, value)
                .expect("validated unique sorted records");
            bytes += crate::run::input_len(key, value);
            if bytes >= self.target_run_len {
                space.runs.push(builder.take().unwrap().finish());
                bytes = 0;
            }
        }
        if let Some(last) = built.last_mut() {
            last.runs.extend(builder.map(RunBuilder::finish));
        }
        built
    }

    pub(super) fn ingest_batch(
        &self,
        batch: &WriteBatch,
        sorted: bool,
        size: BatchSize,
    ) -> Result<u64, UpdateError> {
        let state = self.admit(self.write_state())?;
        batch.check_keyspaces(&state)?;
        let key_workspace = size
            .key_bytes
            .saturating_add(batch.len().saturating_mul(size_of::<IndexedKey>()));
        let mut charge = self.resources.build(
            size.charge.saturating_mul(3),
            crate::run::workspace_estimate([]).saturating_add(key_workspace),
        );
        drop(state);
        // Sort the keys once for both run building and memtable overlap
        // checks. Keep the position of the last operation for each key so
        // the builder can borrow values directly from the batch.
        let mut stored = Vec::with_capacity(size.key_bytes);
        let mut indexed_keys = Vec::with_capacity(batch.len());
        let mut logical: BTreeMap<u32, usize> = BTreeMap::new();
        for (index, (keyspace, key, value)) in batch.records().enumerate() {
            let start = stored.len();
            stored.extend_from_slice(KeyPrefix::new(keyspace).as_bytes());
            stored.extend_from_slice(key);
            indexed_keys.push(IndexedKey {
                operation: index,
                span: start..stored.len(),
            });
            *logical.entry(keyspace).or_default() += crate::run::input_len(key, value);
        }
        let stored_key = |entry: &IndexedKey| &stored[entry.span.clone()];
        if !sorted {
            indexed_keys.sort_unstable_by(|a, b| {
                stored_key(a)
                    .cmp(stored_key(b))
                    .then_with(|| b.operation.cmp(&a.operation))
            });
            indexed_keys.dedup_by(|a, b| stored_key(a) == stored_key(b));
        }
        let mut built = self.build_ingestion(
            indexed_keys
                .iter()
                .map(|entry| batch.record(entry.operation)),
        );
        for space in &mut built {
            space.logical = logical.get(&space.keyspace).copied().unwrap_or(0);
        }
        drop(logical);
        #[cfg(test)]
        {
            let hook = self.after_ingest_build.lock().unwrap().take();
            if let Some(hook) = hook {
                hook();
            }
        }
        let runs = || built.iter().flat_map(|space| &space.runs);
        let bytes: usize = runs().map(|run| run.stored_len()).sum();
        let held: usize = runs().map(Run::held_bytes).sum();
        #[cfg(feature = "diagnostics")]
        let raw: usize = runs().map(Run::raw_len).sum();
        // Overlap checks still use the key scratch after the builders finish.
        charge.finished(bytes + held, key_workspace);
        let mut built: Vec<(u32, usize, Vec<SharedRun>)> = built
            .into_iter()
            .map(|space| {
                (
                    space.keyspace,
                    space.logical,
                    published(space.runs).collect(),
                )
            })
            .collect();
        let keys = || indexed_keys.iter().map(stored_key);
        let lo = stored_key(indexed_keys.first().expect("a nonempty batch"));
        let hi = stored_key(indexed_keys.last().expect("a nonempty batch"));
        let mut state = self.write_state();
        batch.check_keyspaces(&state)?;
        // Freeze a potentially large table cheaply, then inspect it unlocked.
        // At the frozen limit, probe active exactly below: unrelated keys must
        // not make ingestion wait for a slot just to perform a coarse check.
        if state.frozen.len() < self.max_frozen_memtables
            && state
                .active
                .iter_from_in(Key::plain(lo))
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
                    .filter(|table| overlaps(table.iter_from_in(Key::plain(lo)), keys()))
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
            if overlaps(state.active.iter_from_in(Key::plain(lo)), keys()) {
                state = self.make_room(state);
                continue;
            }
            break;
        }
        drop(indexed_keys);
        drop(stored);
        // A keyspace dropped while the runs were built fails the batch.
        batch.check_keyspaces(&state)?;
        let seq = state.last_seq.checked_add(1).expect(SEQUENCES);
        for (keyspace, logical, runs) in &mut built {
            for run in runs.iter_mut() {
                Arc::get_mut(run)
                    .expect("unpublished run is private")
                    .run
                    .set_seq_override(seq);
            }
            let space = state.space_mut(*keyspace);
            space.levels.push_flush(runs, *logical);
            space.runs.append(runs);
        }
        state.published(built.iter().map(|(keyspace, _, _)| *keyspace));
        state.last_seq = seq;
        #[cfg(feature = "diagnostics")]
        self.chunks.work(0, 0, raw, bytes);
        self.chunks.ingested(
            batch
                .records()
                .map(|(_, key, value)| {
                    key.len()
                        + match value {
                            Value::Bytes(bytes) => bytes.len(),
                            Value::Tombstone => 0,
                        }
                })
                .sum(),
        );
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

    fn check_snapshot(snapshot: &Snapshot, ks: &Keyspace, expected: [Option<&[u8]>; 3]) {
        check_reader(&mut snapshot.reader(), ks, expected);
    }

    /// Point reads, a multi-get and a scan of keys "a", "b" and "c" agree
    /// with `expected`.
    fn check_reader(reader: &mut Reader<'_>, ks: &Keyspace, expected: [Option<&[u8]>; 3]) {
        let keys: &[&[u8]] = &[b"a", b"b", b"c"];
        for (key, value) in keys.iter().zip(expected) {
            assert_eq!(reader.get(ks, key).unwrap(), value);
        }
        let probes = [b"c", b"a", b"b", b"c"];
        let values = [expected[2], expected[0], expected[1], expected[2]];
        assert_eq!(&*reader.multi_get(ks, &probes).unwrap(), &values);
        let mut scan = reader.range(ks, ..);
        for (key, value) in keys.iter().zip(expected) {
            if let Some(value) = value {
                assert_eq!(scan.next().unwrap().unwrap(), (*key, value));
            }
        }
        assert!(scan.next().unwrap().is_none());
    }

    #[test]
    fn ingestion_charges_key_scratch_through_overlap_checks() {
        for keys in [[b"a", b"b"], [b"b", b"a"]] {
            let engine = Engine::in_memory(Options {
                memtable_size: 4 << 10,
                background_compaction: None,
                ..Options::default()
            })
            .unwrap();
            let ks = engine.ks();
            ks.put(b"a", b"old").unwrap();

            let (sample, observed) = std::sync::mpsc::channel();
            let building = engine.clone();
            let build_sample = sample.clone();
            *engine.core.after_ingest_build.lock().unwrap() = Some(Arc::new(move || {
                build_sample
                    .send(building.engine_stats().build_workspace_bytes)
                    .unwrap();
            }));
            let flushing = engine.clone();
            // This flush only starts when ingestion tries to publish over
            // the old key. Sample before the flush itself reserves memory.
            *engine.core.before_flush_build.lock().unwrap() = Some(Arc::new(move || {
                sample
                    .send(flushing.engine_stats().build_workspace_bytes)
                    .unwrap();
            }));

            let mut batch = WriteBatch::new();
            let value = [7; 3 << 10];
            for key in keys {
                batch.put(&ks, key, &value);
            }
            assert_eq!(engine.write_seq(&batch), Ok(2));
            let prefix_len = KeyPrefix::new(ks.id()).as_bytes().len();
            let key_workspace = keys.len() * (prefix_len + 1 + size_of::<IndexedKey>());
            assert_eq!(
                observed.try_iter().collect::<Vec<_>>(),
                [
                    crate::run::workspace_estimate([]) + key_workspace,
                    key_workspace
                ]
            );
            assert_eq!(engine.engine_stats().build_workspace_bytes, 0);
        }
    }

    #[test]
    fn ingestion_orders_after_writes_and_compaction_completed_during_its_build() {
        for compression in [Compression::Uncompressed, Compression::Zstd] {
            let engine = Engine::in_memory(Options {
                compression,
                background_compaction: None,
                ..Options::default()
            })
            .unwrap();
            engine.ks().put(b"a", b"base").unwrap();
            engine.ks().put(b"b", b"base").unwrap();
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
                    batch.put(&engine.ks(), b"a", b"ingest");
                    batch.put(&engine.ks(), b"b", b"ingest");
                    batch.put(&engine.ks(), b"c", b"ingest");
                    engine.ingest_sorted(&batch).unwrap();
                });
                built.wait();
                engine.ks().put(b"a", b"new").unwrap();
                engine.ks().delete(b"b").unwrap();
                engine.flush().unwrap();
                settle(&engine);
                // Compaction can settle earlier writes while ingestion builds;
                // its later publication sequence must still win.
                assert_eq!(engine.get_seq(b"a"), Some((0, b"new".to_vec())));
                // The newer deletion has also been physically discarded.
                for run in &engine.core.read_state().only().runs {
                    assert!(
                        run.run
                            .get(b"b", &mut crate::run::ReadWorkspace::new())
                            .unwrap()
                            .is_none()
                    );
                }
                let snapshot = engine.snapshot();
                check_snapshot(&snapshot, &engine.ks(), [Some(b"new"), None, None]);
                resume.wait();
                ingest.join().unwrap();
                snapshot
            });
            // One publication sequence orders the already-built ingestion
            // after those settled writes. All reader paths agree.
            for key in [b"a", b"b", b"c"] {
                assert_eq!(engine.ks().get(key).unwrap().unwrap(), b"ingest");
            }
            let mut reader = engine.reader();
            let values = reader.multi_get(&engine.ks(), &[b"a", b"b", b"c"]).unwrap();
            assert!(values.iter().all(|value| *value == Some(&b"ingest"[..])));
            drop(values);
            check_snapshot(&engine.snapshot(), &engine.ks(), [Some(b"ingest"); 3]);
            check_snapshot(&snapshot, &engine.ks(), [Some(b"new"), None, None]);
            drop(snapshot);
            settle(&engine);
            check_snapshot(&engine.snapshot(), &engine.ks(), [Some(b"ingest"); 3]);
            assert_eq!(engine.engine_stats().last_seq, 5);
        }
    }

    #[test]
    fn a_checkpoint_during_ingestion_recovers_without_the_unpublished_batch() {
        let engine = Arc::new(Engine::in_memory(Options::default()).unwrap());
        engine.ks().put(b"a", b"before").unwrap();
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
            checkpoint_engine.checkpoint_to(&checkpoint_dir).unwrap();
        }));
        let mut batch = WriteBatch::new();
        batch.put(&engine.ks(), b"a", b"ingest");
        batch.put(&engine.ks(), b"b", b"ingest");
        assert_eq!(engine.ingest_sorted(&batch), Ok(2));
        let recovered = Engine::open(&dir, Options::default()).unwrap();
        assert_eq!(recovered.engine_stats().last_seq, 1);
        check_snapshot(
            &recovered.snapshot(),
            &recovered.ks(),
            [Some(b"before"), None, None],
        );
        assert_eq!(recovered.ks().put_seq(b"c", b"after recovery"), Ok(2));
        check_snapshot(
            &engine.snapshot(),
            &engine.ks(),
            [Some(b"ingest"), Some(b"ingest"), None],
        );
        recovered.close().unwrap();
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn a_panicking_ingestion_build_publishes_nothing() {
        let engine = Engine::in_memory(Options::default()).unwrap();
        engine.ks().put(b"a", b"before").unwrap();
        let mut batch = WriteBatch::new();
        batch.put(&engine.ks(), b"a", b"ingest");
        batch.put(&engine.ks(), b"b", b"ingest");
        *engine.core.after_ingest_build.lock().unwrap() =
            Some(Arc::new(|| panic!("injected ingestion failure")));
        assert!(
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                engine.ingest_sorted(&batch)
            }))
            .is_err()
        );
        assert_eq!(engine.engine_stats().last_seq, 1);
        check_snapshot(
            &engine.snapshot(),
            &engine.ks(),
            [Some(b"before"), None, None],
        );
        assert_eq!(engine.ingest_sorted(&batch), Ok(2));
        check_snapshot(
            &engine.snapshot(),
            &engine.ks(),
            [Some(b"ingest"), Some(b"ingest"), None],
        );
    }

    fn check_engine(engine: &Engine, expected: [Option<&[u8]>; 3]) {
        let ks = engine.ks();
        for (key, value) in [b"a", b"b", b"c"].iter().zip(expected) {
            assert_eq!(ks.get(*key).unwrap().as_deref(), value);
        }
        check_reader(&mut engine.reader(), &ks, expected);
        check_snapshot(&engine.snapshot(), &ks, expected);
    }

    #[test]
    fn unrelated_writes_inside_the_batch_range_never_wait_for_the_flush_worker() {
        for full in [false, true] {
            let engine = Arc::new(
                Engine::in_memory(Options {
                    max_frozen_memtables: 1,
                    background_compaction: None,
                    ..Options::default()
                })
                .unwrap(),
            );
            // A publication that waited for any unrelated table would deadlock
            // here. Test both a free frozen slot and an already full queue.
            let flush = engine.core.serialize_flush();
            if full {
                engine.ks().put(b"b", b"old unrelated").unwrap();
                let mut state = engine.core.write_state();
                engine.core.freeze_active(&mut state);
                engine.core.flush_signal.request();
            }
            let during = Arc::clone(&engine);
            *engine.core.after_ingest_build.lock().unwrap() = Some(Arc::new(move || {
                during.ks().put(b"b", b"unrelated").unwrap();
                *during.core.after_ingest_build.lock().unwrap() =
                    Some(Arc::new(|| panic!("ingestion rebuilt")));
            }));
            let mut batch = WriteBatch::new();
            batch.put(&engine.ks(), b"a", b"ingest");
            batch.put(&engine.ks(), b"c", b"ingest");
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
            assert_eq!(engine.engine_stats().flushes, 0);
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
                Engine::in_memory(Options {
                    compression,
                    background_compaction: None,
                    ..Options::default()
                })
                .unwrap(),
            );
            for key in [b"a", b"b", b"c"] {
                engine.ks().put(key, b"before").unwrap();
            }
            let during = Arc::clone(&engine);
            *engine.core.after_ingest_build.lock().unwrap() = Some(Arc::new(move || {
                during.ks().put(b"a", b"during build").unwrap();
                during.ks().delete(b"b").unwrap();
            }));
            let during = Arc::clone(&engine);
            *engine.core.before_flush_build.lock().unwrap() = Some(Arc::new(move || {
                // Land in the new active table while ingestion waits for its
                // first frozen overlap. Publication must drain these too.
                during.ks().put(b"a", b"during flush").unwrap();
                during.ks().delete(b"b").unwrap();
            }));
            let mut batch = WriteBatch::new();
            for key in [b"a", b"b", b"c"] {
                batch.put(&engine.ks(), key, b"ingest");
            }
            let seq = engine.ingest_sorted(&batch).unwrap();
            assert_eq!(seq, 8);
            check_engine(&engine, [Some(b"ingest"); 3]);
            for key in [b"a", b"b", b"c"] {
                assert_eq!(engine.get_seq(key).unwrap().0, seq);
            }
            let at_publication = engine.snapshot();
            assert_eq!(engine.ks().put_seq(b"a", b"after"), Ok(seq + 1));
            assert_eq!(engine.ks().delete_seq(b"b"), Ok(seq + 2));
            check_engine(&engine, [Some(b"after"), None, Some(b"ingest")]);
            engine.flush().unwrap();
            settle(&engine);
            check_engine(&engine, [Some(b"after"), None, Some(b"ingest")]);
            check_snapshot(&at_publication, &engine.ks(), [Some(b"ingest"); 3]);
        }
    }

    #[test]
    fn effective_sequences_materialize_through_compaction_checkpoint_and_recovery() {
        for compression in [Compression::Uncompressed, Compression::Zstd] {
            for pinned in [false, true] {
                let options = Options {
                    compression,
                    target_run_len: 1,
                    background_compaction: None,
                    ..Options::default()
                };
                let engine = Engine::in_memory(options).unwrap();
                for key in [b"a", b"b", b"c"] {
                    engine.ks().put(key, b"old").unwrap();
                }
                engine.flush().unwrap();
                let mut batch = WriteBatch::new();
                batch.put(&engine.ks(), b"a", b"ingest");
                batch.delete(&engine.ks(), b"b");
                batch.put(&engine.ks(), b"c", b"ingest");
                let seq = engine.ingest_sorted(&batch).unwrap();
                assert_eq!(seq, 4);
                assert_eq!(
                    engine
                        .core
                        .read_state()
                        .only()
                        .runs
                        .iter()
                        .filter(|run| run.run.max_seq() == seq)
                        .count(),
                    3
                );
                let pin = pinned.then(|| engine.snapshot());
                // Export overrides before a compaction has materialized them,
                // then repeat after zeroing them; a pinned snapshot keeps its own.
                let dir = std::env::temp_dir().join(format!(
                    "tessera-effective-ingest-{}-{compression:?}-{pinned}",
                    std::process::id()
                ));
                for compacted in [false, true] {
                    if compacted {
                        settle(&engine);
                    }
                    let expected = if compacted { 0 } else { seq };
                    check_engine(&engine, [Some(b"ingest"), None, Some(b"ingest")]);
                    for key in [b"a", b"c"] {
                        assert_eq!(engine.get_seq(key).unwrap().0, expected);
                    }
                    engine.checkpoint_to(&dir).unwrap();
                    let recovered = Engine::open(&dir, options).unwrap();
                    assert_eq!(recovered.engine_stats().last_seq, seq);
                    check_engine(&recovered, [Some(b"ingest"), None, Some(b"ingest")]);
                    for key in [b"a", b"c"] {
                        assert_eq!(recovered.get_seq(key).unwrap().0, expected);
                    }
                    assert_eq!(recovered.ks().put_seq(b"b", b"after recovery"), Ok(seq + 1));
                    check_engine(
                        &recovered,
                        [Some(b"ingest"), Some(b"after recovery"), Some(b"ingest")],
                    );
                    recovered.close().unwrap();
                }
                if let Some(pin) = pin {
                    check_snapshot(&pin, &engine.ks(), [Some(b"ingest"), None, Some(b"ingest")]);
                }
                std::fs::remove_dir_all(&dir).unwrap();
            }
        }
    }

    fn last_sequence_engine() -> (Engine, WriteBatch) {
        let engine = Engine::in_memory(Options {
            last_seq: u64::MAX - 1,
            ..Options::default()
        })
        .unwrap();
        let mut batch = WriteBatch::new();
        batch.put(&engine.ks(), b"a", b"ingest");
        batch.put(&engine.ks(), b"c", b"ingest");
        (engine, batch)
    }

    #[test]
    fn ingestion_uses_the_last_sequence_once() {
        let (engine, batch) = last_sequence_engine();
        assert_eq!(engine.ingest_sorted(&batch), Ok(u64::MAX));
        assert_eq!(engine.ingest_sorted(&WriteBatch::new()), Ok(u64::MAX));
        check_engine(&engine, [Some(b"ingest"), None, Some(b"ingest")]);
    }

    #[test]
    #[should_panic(expected = "sequence numbers are exhausted")]
    fn ingestion_after_the_last_sequence_panics() {
        let (engine, batch) = last_sequence_engine();
        engine.ingest_sorted(&batch).unwrap();
        let _ = engine.ingest_sorted(&batch);
    }

    #[test]
    #[should_panic(expected = "sequence numbers are exhausted")]
    fn ingestion_rechecks_exhaustion_at_publication() {
        let (engine, batch) = last_sequence_engine();
        let during = engine.clone();
        *engine.core.after_ingest_build.lock().unwrap() = Some(Arc::new(move || {
            assert_eq!(during.ks().put_seq(b"b", b"last write"), Ok(u64::MAX));
        }));
        let _ = engine.ingest_sorted(&batch);
    }
}
