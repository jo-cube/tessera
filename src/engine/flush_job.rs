//! Flushes staged outside the state lock and their publication.

use super::*;

/// A flush whose tables are frozen and whose runs are not built yet: the
/// staging that lets a flush compress without the state lock.
///
/// Dropping it abandons the flush and keeps the tables frozen. Holding it
/// blocks other flushes, including a write's that needs room, but not reads,
/// writes that fit, or snapshots.
pub(super) struct FlushJob<'e> {
    pub(super) engine: &'e Core,
    pub(super) _serial: MutexGuard<'e, ()>,
    /// Oldest first: a prefix of the state's frozen tables, since only
    /// flushes, one at a time, add or remove them.
    pub(super) tables: Vec<Arc<FrozenMemtable>>,
    /// Where to split the runs, from level 1 when the flush began. Level 1
    /// may change before the flush publishes; level 0 allows any overlap,
    /// so stale cuts change only how well the runs fit, never a read.
    pub(super) cuts: FlushCuts,
}

impl<'e> FlushJob<'e> {
    /// Builds the runs, compressing without the state lock. Several tables
    /// are merged as one, keeping each key's newest update.
    pub(super) fn build(self) -> BuiltFlush<'e> {
        let estimate = self.tables.iter().fold(0usize, |sum, table| {
            sum.saturating_add(table.memory().payload.saturating_mul(4))
                .saturating_add(table.len().saturating_mul(256))
        });
        let mut charge = self
            .engine
            .resources
            .build(estimate, crate::run::workspace_estimate([]));
        let tables: Vec<&FrozenMemtable> = self.tables.iter().map(Arc::as_ref).collect();
        let runs = flush_merged(
            &tables,
            self.engine.target_run_len,
            self.engine.codec,
            self.engine.target_block_compressed,
            &self.cuts,
            &self.engine.chunks,
        );
        charge.finished(
            runs.iter()
                .map(|run| run.stored_len() + run.held_bytes())
                .sum(),
        );
        BuiltFlush {
            job: self,
            runs,
            charge,
        }
    }

    /// The records in the tables, counting a key once per table.
    fn records(&self) -> usize {
        self.tables.iter().map(|table| table.len()).sum()
    }
}

impl fmt::Debug for FlushJob<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FlushJob")
            .field("tables", &self.tables.len())
            .field("records", &self.records())
            .finish_non_exhaustive()
    }
}

/// A flush with its runs built but not visible to anyone.
pub(super) struct BuiltFlush<'e> {
    charge: resources::BuildCharge<'e>,
    job: FlushJob<'e>,
    pub(super) runs: Vec<Run<Vec<u8>>>,
}

impl BuiltFlush<'_> {
    /// Publishes the runs and removes the frozen tables in one step.
    ///
    pub(super) fn publish(self) -> FlushReport {
        self.publish_kept().0
    }

    /// The checkpoint keeps the exact outputs even if compaction replaces them.
    pub(super) fn publish_kept(self) -> (FlushReport, Vec<SharedRun>) {
        let Self { job, runs, charge } = self;
        let report = FlushReport {
            tables: job.tables.len(),
            records: job.records(),
            runs: runs.len(),
            run_bytes: runs.iter().map(|run| run.stored_len()).sum(),
            held_bytes: runs.iter().map(Run::held_bytes).sum(),
            filter_build_bytes: runs.iter().map(Run::filter_build_bytes).max().unwrap_or(0),
        };
        let mut state = job.engine.write_state();
        let flushed = job.tables.len();
        assert!(
            state.frozen[..flushed]
                .iter()
                .zip(&job.tables)
                .all(|(held, flushed)| Arc::ptr_eq(held, flushed)),
            "a flush's tables are the oldest frozen ones"
        );
        let runs: Vec<SharedRun> = published(runs).collect();
        let logical = job
            .tables
            .iter()
            .map(|table| table.memory().payload + table.len() * RECORD_INPUT_OVERHEAD)
            .sum();
        state.levels.push_flush(&runs, logical);
        #[cfg(feature = "diagnostics")]
        job.engine.chunks.work(
            0,
            0,
            runs.iter().map(|run| run.run.raw_len()).sum(),
            report.run_bytes,
        );
        state.runs.extend(runs.iter().cloned());
        state.order_runs();
        state.run_bytes += report.run_bytes;
        state.held_bytes += report.held_bytes;
        state.flushes += 1;
        state.flushed_bytes += report.run_bytes as u64;
        state.flush_totals.add(report);
        for table in state.frozen.drain(..flushed).collect::<Vec<_>>() {
            // This and the job's are the engine's references, and snapshots
            // are taken only under the lock, so any other is a snapshot's,
            // which keeps the whole table. Otherwise only views of it can
            // remain, which share its family, so this follows all of them.
            let bytes = if Arc::strong_count(&table) > 2 {
                table.memory().charged
            } else {
                table.held_by_snapshots()
            };
            state.retire(table.family() as Weak<dyn Send + Sync>, bytes, None);
        }
        drop(charge);
        // Unless a snapshot holds them, `job` has the last references, so the
        // tables are freed after this releases the lock.
        drop(state);
        job.engine.notify_worker();
        job.engine.flush_signal.changed();
        job.engine.resources.changed();
        (report, runs)
    }
}

impl fmt::Debug for BuiltFlush<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BuiltFlush")
            .field("runs", &self.runs.len())
            .finish_non_exhaustive()
    }
}
