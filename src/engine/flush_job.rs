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
    /// The keyspaces live when the flush began, sorted by prefix, and where
    /// to split each one's runs, from its level 1 then. Level 1 may change
    /// before the flush publishes; level 0 allows any overlap, so stale cuts
    /// change only how well the runs fit, never a read. Records of other
    /// keyspaces, which were dropped, are left out.
    pub(super) spaces: Vec<(u32, KeyPrefix, FlushCuts)>,
}

impl<'e> FlushJob<'e> {
    /// A flush of every frozen table in `state`, holding `serial`.
    pub(super) fn new(engine: &'e Core, serial: MutexGuard<'e, ()>, state: &State) -> Self {
        Self::with_cuts(engine, serial, state, false)
    }

    /// A quiet tail flush aligned to pending level-0 slice boundaries.
    pub(super) fn new_quiet(engine: &'e Core, serial: MutexGuard<'e, ()>, state: &State) -> Self {
        Self::with_cuts(engine, serial, state, true)
    }

    fn with_cuts(engine: &'e Core, serial: MutexGuard<'e, ()>, state: &State, quiet: bool) -> Self {
        Self {
            engine,
            _serial: serial,
            tables: state.frozen.clone(),
            spaces: state.flush_spaces(engine.target_run_len, quiet),
        }
    }

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
        let spaces: Vec<FlushSpace<'_>> = self
            .spaces
            .iter()
            .map(|(_, prefix, cuts)| FlushSpace {
                prefix: prefix.as_bytes(),
                cuts,
            })
            .collect();
        let built = flush_merged(
            &tables,
            self.engine.target_run_len,
            self.engine.encoding,
            self.engine.target_block_compressed,
            &spaces,
            &self.engine.chunks,
        );
        let spaces: Vec<(u32, crate::memtable::SpaceRuns)> = self
            .spaces
            .iter()
            .map(|(id, _, _)| *id)
            .zip(built)
            .filter(|(_, space)| !space.runs.is_empty())
            .collect();
        charge.finished(
            spaces
                .iter()
                .flat_map(|(_, space)| &space.runs)
                .map(|run| run.stored_len() + run.held_bytes())
                .sum(),
            0,
        );
        BuiltFlush {
            job: self,
            spaces,
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
    /// Each keyspace with records in the tables, and its runs.
    pub(super) spaces: Vec<(u32, crate::memtable::SpaceRuns)>,
}

/// One keyspace's runs of a published flush, and the level-0 flush they
/// were published as.
pub(super) struct Kept {
    pub(super) keyspace: u32,
    pub(super) flush: u64,
    pub(super) runs: Vec<SharedRun>,
}

impl BuiltFlush<'_> {
    /// The runs built, over every keyspace.
    #[cfg(test)]
    pub(super) fn runs(&self) -> impl Iterator<Item = &Run<Vec<u8>>> {
        self.spaces.iter().flat_map(|(_, space)| &space.runs)
    }

    /// Publishes the runs and removes the frozen tables in one step.
    ///
    pub(super) fn publish(self) -> FlushReport {
        self.publish_inner(true).0
    }

    /// Publishes a quiet tail flush run by the compaction worker itself,
    /// without queuing an ordinary compaction request ahead of its settle.
    pub(super) fn publish_quiet(self) -> FlushReport {
        self.publish_inner(false).0
    }

    /// The checkpoint keeps the exact outputs even if compaction replaces them.
    pub(super) fn publish_kept(self) -> (FlushReport, Vec<Kept>) {
        self.publish_inner(true)
    }

    fn publish_inner(self, notify_compaction: bool) -> (FlushReport, Vec<Kept>) {
        let Self {
            job,
            spaces,
            charge,
        } = self;
        let runs = || spaces.iter().flat_map(|(_, space)| &space.runs);
        let report = FlushReport {
            tables: job.tables.len(),
            records: job.records(),
            runs: runs().count(),
            run_bytes: runs().map(Run::stored_len).sum(),
            held_bytes: runs().map(Run::held_bytes).sum(),
            filter_build_bytes: runs().map(Run::filter_build_bytes).max().unwrap_or(0),
        };
        #[cfg(feature = "diagnostics")]
        let raw: usize = runs().map(Run::raw_len).sum();
        let mut state = job.engine.write_state();
        let flushed = job.tables.len();
        assert!(
            state.frozen[..flushed]
                .iter()
                .zip(&job.tables)
                .all(|(held, flushed)| Arc::ptr_eq(held, flushed)),
            "a flush's tables are the oldest frozen ones"
        );
        let mut kept = Vec::with_capacity(spaces.len());
        for (keyspace, space) in spaces {
            // Dropping a keyspace waits for flushes, so this holds; a
            // keyspace dropped before the flush began is not among them.
            let Some(target) = state.keyspaces.get_mut(&keyspace) else {
                continue;
            };
            let runs: Vec<SharedRun> = published(space.runs).collect();
            let flush = target.levels.next_flush();
            target.levels.push_flush(&runs, space.logical);
            target.runs.extend(runs.iter().cloned());
            kept.push(Kept {
                keyspace,
                flush,
                runs,
            });
        }
        if !kept.is_empty() {
            state.published(kept.iter().map(|space| space.keyspace));
        }
        #[cfg(feature = "diagnostics")]
        job.engine.chunks.work(0, 0, raw, report.run_bytes);
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
        if notify_compaction {
            // Requested under the lock, as flushes and admission do, so that a
            // reader who sees the run also sees the compaction request.
            job.engine.notify_worker();
        } else if !state.frozen.is_empty() {
            job.engine.flush_signal.request();
        }
        // Unless a snapshot holds them, `job` has the last references, so the
        // tables are freed after this releases the lock.
        drop(state);
        job.engine.flush_signal.changed();
        job.engine.resources.changed();
        (report, kept)
    }
}

impl fmt::Debug for BuiltFlush<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BuiltFlush")
            .field("keyspaces", &self.spaces.len())
            .finish_non_exhaustive()
    }
}
