//! Ingestion reserves an order, builds outside state, drains overlapping
//! tables, and publishes once. Compaction preserves versions during the build.
use super::*;
use crate::run::RunBuilder;

struct Pending<'a> {
    core: &'a Core,
}
impl Drop for Pending<'_> {
    fn drop(&mut self) {
        self.core.write_state().pending_ingests -= 1;
    }
}

impl Core {
    pub(super) fn ingest_batch(
        &self,
        batch: &WriteBatch,
        sorted: bool,
    ) -> Result<u64, UpdateError> {
        if batch.is_empty() {
            return Ok(self.read_state().last_seq);
        }
        // Synchronize with selection/publication of an existing compaction:
        // jobs started after this reservation must preserve sequences and
        // tombstones until every pending ingestion has published or failed.
        let serial = self
            .compacting
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        let mut state = self.write_state();
        let start = state.last_seq;
        let end = start
            .checked_add(batch.len() as u64)
            .ok_or(UpdateError::SequencesExhausted)?;
        state.last_seq = end;
        state.pending_ingests += 1;
        drop(state);
        let pending = Pending { core: self };
        drop(serial);
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
            let hook = self.after_ingest_build.lock().unwrap().clone();
            if let Some(hook) = hook {
                hook();
            }
        }
        let lo = runs.first().unwrap().key_range().unwrap().min;
        let hi = runs.last().unwrap().key_range().unwrap().max;
        let mut state = self.write_state();
        loop {
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
        let bytes: usize = runs.iter().map(|run| run.as_bytes().len()).sum();
        let held: usize = runs.iter().map(Run::held_bytes).sum();
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
        state.levels.push_flush(&runs, logical);
        state.runs.extend(runs);
        state.run_bytes += bytes;
        state.held_bytes += held;
        state.order_runs();
        drop(state);
        drop(pending);
        self.notify_worker();
        Ok(end)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Barrier;

    #[test]
    fn concurrent_writes_and_compaction_win_over_an_earlier_pending_ingest() {
        let engine = Engine::new(EngineOptions {
            background_compaction: None,
            ..EngineOptions::default()
        })
        .unwrap();
        let built = Arc::new(Barrier::new(2));
        let resume = Arc::new(Barrier::new(2));
        let (a, b) = (Arc::clone(&built), Arc::clone(&resume));
        *engine.core.after_ingest_build.lock().unwrap() = Some(Arc::new(move || {
            a.wait();
            b.wait();
        }));
        std::thread::scope(|scope| {
            let ingest = scope.spawn(|| {
                let mut batch = WriteBatch::new();
                batch.put(b"a", b"old ingest");
                batch.put(b"b", b"old ingest");
                engine.ingest_sorted(&batch).unwrap();
            });
            built.wait();
            engine.put(b"a", b"new").unwrap();
            engine.delete(b"b").unwrap();
            engine.flush().unwrap();
            let mut merge = MergeWorkspace::new();
            while engine
                .compact(CompactionOptions::default(), &mut merge)
                .unwrap()
                .is_some()
            {}
            // A snapshot during the build sees none of the ingestion.
            let snapshot = engine.snapshot().unwrap();
            let mut ws = EngineWorkspace::new();
            assert_eq!(snapshot.get(b"a", &mut ws).unwrap().unwrap().value, b"new");
            assert!(snapshot.get(b"b", &mut ws).unwrap().is_none());
            resume.wait();
            ingest.join().unwrap();
        });
        let mut ws = EngineWorkspace::new();
        assert_eq!(engine.get(b"a", &mut ws).unwrap().unwrap().value, b"new");
        assert!(engine.get(b"b", &mut ws).unwrap().is_none());
        assert_eq!(engine.core.read_state().pending_ingests, 0);
    }
}
