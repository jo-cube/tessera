//! Compaction policy, jobs, reports and errors.

use super::*;

/// Soft bounds on optional fan-in and parallel workspace, never refusals.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct CompactionPolicy {
    pub(super) max_inputs: usize,
    pub(super) max_input_bytes: usize,
    pub(super) max_workspace_bytes: usize,
    pub(super) max_overlap: usize,
    pub(super) reclaim: bool,
}
impl Default for CompactionPolicy {
    fn default() -> Self {
        Self::for_memory(None)
    }
}
impl CompactionPolicy {
    pub(super) fn for_memory(limit: Option<usize>) -> Self {
        Self {
            max_inputs: MAX_COMPACTION_INPUTS,
            max_input_bytes: limit.map_or(COMPACTION_INPUT_BYTES, |n| {
                (n / 8).clamp(1, COMPACTION_INPUT_BYTES)
            }),
            max_workspace_bytes: limit.map_or(COMPACTION_WORKSPACE_BYTES, |n| {
                (n / 8).clamp(1, COMPACTION_WORKSPACE_BYTES)
            }),
            max_overlap: COMPACTION_TRIGGER,
            reclaim: false,
        }
    }
    pub(super) fn with_test_policy(self, policy: Self) -> Self {
        #[cfg(test)]
        {
            Self {
                max_input_bytes: self.max_input_bytes.min(policy.max_input_bytes),
                max_workspace_bytes: self.max_workspace_bytes.min(policy.max_workspace_bytes),
                ..policy
            }
        }
        #[cfg(not(test))]
        {
            let _ = policy;
            self
        }
    }
    pub(super) fn fit(&self, target_run_len: usize, level0_stop: usize) -> Fit {
        Fit {
            max_inputs: self.max_inputs,
            max_bytes: self.max_input_bytes,
            max_workspace_bytes: self.max_workspace_bytes,
            trigger: self.max_overlap,
            level0_stop,
            target_run_len,
        }
    }
}

/// Why a compaction stopped. Earlier pieces of a bottom job remain published.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub(crate) enum CompactionError {
    /// An input run is damaged, or two runs disagree about one update. The
    /// current piece is unchanged; earlier pieces stay published. Retrying
    /// damaged inputs meets the same corruption.
    Read(ReadError),
    /// The selected runs do not occupy their expected positions at
    /// publication. This piece was not published; compactions are serialized,
    /// so this is a defect, reported rather than published.
    Stale,
    /// A single indivisible range exceeds the job's bounded output allowance.
    /// Earlier pieces remain published and this piece changes nothing.
    #[non_exhaustive]
    OutputLimit {
        /// Conservative output bytes needed before the next allocation.
        bytes: usize,
        /// The whole job's unpublished-output allowance.
        limit: usize,
    },
}

impl From<crate::run::CompactBuildError> for CompactionError {
    fn from(error: crate::run::CompactBuildError) -> Self {
        match error {
            crate::run::CompactBuildError::Read(error) => Self::Read(error),
            crate::run::CompactBuildError::OutputLimit { bytes, limit } => {
                Self::OutputLimit { bytes, limit }
            }
        }
    }
}

impl From<ReadError> for CompactionError {
    fn from(error: ReadError) -> Self {
        Self::Read(error)
    }
}

impl fmt::Display for CompactionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Read(error) => error.fmt(f),
            Self::OutputLimit { bytes, limit } => write!(
                f,
                "compaction output needs {bytes} bytes, allowance is {limit}"
            ),
            Self::Stale => f.write_str("compaction piece found its inputs moved"),
        }
    }
}

impl std::error::Error for CompactionError {}

/// What kind of step a compaction took.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub(crate) enum CompactionKind {
    /// Runs of one level merged with the runs of the next that they
    /// overlap, into the next.
    Merge,
    /// A run relabelled to the next level, which it overlaps nowhere:
    /// nothing was read or written.
    Move,
    /// Two adjacent small runs of one level merged into one.
    Neighbours,
}

/// What a compaction replaced.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct CompactionReport {
    /// What kind of step it was. A move replaces nothing, and every count
    /// below is 0.
    pub kind: CompactionKind,
    /// The level the step took its runs from.
    pub level: usize,
    /// Runs removed.
    pub inputs: usize,
    /// Their stored bytes.
    pub input_bytes: usize,
    /// What they held beside that: key filters and physical segment metadata.
    pub input_held_bytes: usize,
    /// Runs published in their place.
    pub outputs: usize,
    /// Their stored bytes.
    pub output_bytes: usize,
    /// What they hold beside that: key filters and physical segment metadata.
    pub output_held_bytes: usize,
    /// The most bytes one output's filter construction held for key hashes,
    /// freed when that run was finished.
    pub filter_build_bytes: usize,
    /// Whether no other run overlapped the inputs, so that every tombstone
    /// could be dropped.
    pub complete: bool,
    /// What the merge read and wrote.
    pub stats: CompactStats,
    /// The workspace estimate checked before merging.
    pub workspace_bytes: usize,
}

/// A compaction whose inputs are chosen and whose output is not built yet.
///
/// It owns the current inputs and overlapping unselected runs. A piecewise
/// job releases each completed range's inputs before building the next.
/// Holding it blocks other compactions, including the background worker's,
/// but not reads, writes or flushes. Abandoning a build changes no directory;
/// pieces published earlier remain visible.
pub(super) struct CompactionJob<'e> {
    pub(super) engine: &'e Core,
    pub(super) _serial: MutexGuard<'e, ()>,
    /// The keyspace whose runs it compacts.
    pub(super) keyspace: u32,
    pub(super) inputs: Vec<SharedRun>,
    /// Unselected runs whose key ranges overlap the inputs' span. They may
    /// hold older versions that the inputs' tombstones hide.
    pub(super) others: Vec<SharedRun>,
    pub(super) options: CompactionPolicy,
    pub(super) kind: CompactionKind,
    pub(super) level: usize,
    /// Where the output goes.
    pub(super) dest: Dest,
    /// Output into the bottom level, which erases uncontested sequences.
    pub(super) zero_seq: bool,
    pub(super) cuts: OutputCuts,
    /// Where the source level's next job starts once this publishes.
    pub(super) cursor: Option<Cursor>,
    /// Stored size of a full target-length run, using the planner estimate.
    pub(super) estimated_run_bytes: usize,
    /// Number of leading level-0 sources, for bottom-range publication.
    pub(super) source_count: usize,
}

/// How a published job moves its source level's cursor.
#[derive(Debug)]
pub(super) enum Cursor {
    /// Past this key, the source's largest.
    After(Vec<u8>),
}

/// A compaction step chosen under the lock: a job to build, or a move,
/// already done.
pub(super) enum Begun<'e> {
    Job(CompactionJob<'e>),
    Done(CompactionReport),
}

impl<'e> CompactionJob<'e> {
    pub(super) fn input_bytes(&self) -> usize {
        self.inputs.iter().map(|run| run.run.stored_len()).sum()
    }

    /// Returns whether no other run overlaps the inputs, so that every
    /// tombstone can be dropped.
    pub(super) fn is_complete(&self) -> bool {
        self.others.is_empty()
    }

    /// Whether the merge copies settled bottom blocks forward: it merges
    /// level 0 into the bottom level.
    pub(super) fn copy_forward(&self) -> bool {
        #[cfg(test)]
        if !self
            .engine
            .copy_forward
            .load(std::sync::atomic::Ordering::Relaxed)
        {
            return false;
        }
        self.zero_seq && self.source_count > 0
    }

    /// The unselected runs that may contest erasing an output sequence,
    /// sorted by smallest key. A run whose records are all newer than every
    /// input's cannot, nor can any run once the output is not the bottom.
    #[cfg(test)]
    pub(super) fn seq_floors(&self) -> Vec<SeqFloor<'_>> {
        if !self.zero_seq {
            return Vec::new();
        }
        self.seq_floors_for(&self.inputs)
    }

    fn seq_floors_for(&self, inputs: &[SharedRun]) -> Vec<SeqFloor<'_>> {
        if !self.zero_seq {
            return Vec::new();
        }
        let newest = inputs.iter().map(|run| run.run.max_seq()).max();
        let mut floors: Vec<_> = self
            .others
            .iter()
            .filter(|run| Some(run.run.min_seq()) <= newest)
            .filter_map(|run| SeqFloor::of(&run.run))
            .collect();
        floors.sort_unstable_by_key(|floor| floor.min_key);
        floors
    }

    /// Merges the inputs without the state lock.
    ///
    /// A tombstone is kept if any other run overlapping the inputs has a key
    /// range covering its key, since that run may hold an older value; runs
    /// published later come from flushes and hold only newer updates. On an
    /// error nothing is built and the engine is unchanged.
    pub(super) fn build(
        self,
        workspace: &mut MergeWorkspace,
    ) -> Result<BuiltCompaction<'e>, CompactionError> {
        let threads = self.engine.background_threads;
        self.build_with_threads(workspace, threads)
    }

    pub(super) fn build_with_threads(
        self,
        workspace: &mut MergeWorkspace,
        threads: usize,
    ) -> Result<BuiltCompaction<'e>, CompactionError> {
        let (output, charge) = self.build_inputs(
            &self.inputs,
            workspace,
            threads,
            self.options.max_workspace_bytes,
            None,
        )?;
        Ok(BuiltCompaction {
            job: self,
            output,
            charge,
        })
    }

    pub(super) fn build_inputs(
        &self,
        inputs: &[SharedRun],
        workspace: &mut MergeWorkspace,
        threads: usize,
        workspace_allowance: usize,
        budget: Option<Arc<crate::run::OutputBudget>>,
    ) -> Result<(crate::run::Compacted, resources::BuildCharge<'e>), CompactionError> {
        let estimate = inputs.iter().fold(0usize, |sum, run| {
            sum.saturating_add(run.run.build_estimate())
        });
        let workspace_bytes = workspace_allowance.max(crate::run::workspace_estimate(
            inputs.iter().map(|run| run.largest_block),
        ));
        let mut charge = self.engine.resources.build(
            budget.as_ref().map_or(estimate, |b| b.limit()),
            workspace_bytes,
        );
        let views: Vec<RunView<'_>> = inputs.iter().map(|run| run.run.view()).collect();
        let ranges: Vec<_> = self
            .others
            .iter()
            .map(|run| run.run.key_range().expect(NOT_EMPTY))
            .collect();
        let keep_tombstone = |key: &[u8]| {
            ranges
                .iter()
                .any(|range| range.min <= key && key <= range.max)
        };
        let floors = self.seq_floors_for(inputs);
        let limits = CompactLimits {
            target_run_len: self.engine.target_run_len,
            max_workspace_bytes: workspace_bytes,
            encoding: self.engine.encoding,
            target_block_compressed: self.engine.target_block_compressed,
            zero_seq: self.zero_seq,
            copy_forward: self.copy_forward(),
        };
        #[cfg(feature = "diagnostics")]
        crate::run::copy_audit::set_bottom_inputs(
            (crate::run::copy_audit::enabled() && self.zero_seq && self.source_count > 0).then(
                || {
                    let state = self.engine.read_state();
                    let space = state
                        .keyspace(self.keyspace)
                        .expect("a compacting keyspace");
                    let bottom = space.levels.runs().last().expect("a bottom level");
                    inputs
                        .iter()
                        .map(|run| bottom.iter().any(|b| Arc::ptr_eq(b, run)))
                        .collect()
                },
            ),
        );
        let output = if let Some(budget) = budget {
            crate::run::compact_limited_in(
                &views,
                keep_tombstone,
                limits,
                &self.cuts,
                (workspace, &self.engine.chunks, &floors),
                budget,
            )
            .map_err(CompactionError::from)
        } else {
            compact_parallel_in(
                &views,
                keep_tombstone,
                limits,
                &self.cuts,
                threads,
                self.estimated_run_bytes,
                (workspace, &self.engine.chunks, &floors),
            )
            .map_err(CompactionError::from)
        };
        // The workspace's buffers and contexts are charged only as part of
        // this build, so they do not outlive it.
        *workspace = MergeWorkspace::new();
        let output = output?;
        #[cfg(feature = "diagnostics")]
        crate::run::copy_audit::set_bottom_inputs(None);
        #[cfg(test)]
        {
            let hook = self.engine.after_build.lock().unwrap().clone();
            if let Some(hook) = hook {
                hook();
            }
        }
        drop(floors);
        drop(ranges);
        drop(views);
        charge.finished(output.output_bytes + output.held_bytes, 0);
        Ok((output, charge))
    }
}

impl fmt::Debug for CompactionJob<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CompactionJob")
            .field("kind", &self.kind)
            .field("level", &self.level)
            .field("inputs", &self.inputs.len())
            .field("input_bytes", &self.input_bytes())
            .field("others", &self.others.len())
            .finish_non_exhaustive()
    }
}

/// A compaction with its output built but not visible to anyone.
pub(super) struct BuiltCompaction<'e> {
    pub(super) charge: resources::BuildCharge<'e>,
    pub(super) job: CompactionJob<'e>,
    pub(super) output: crate::run::Compacted,
}

impl BuiltCompaction<'_> {
    /// Publish an ordinary atomic job through the same path as a range piece.
    pub(super) fn publish(self) -> Result<CompactionReport, CompactionError> {
        self.job
            .publish_inputs(&self.job.inputs, self.output, self.charge, true)
    }
}

impl<'e> CompactionJob<'e> {
    pub(super) fn publish_inputs(
        &self,
        inputs: &[SharedRun],
        output: crate::run::Compacted,
        charge: resources::BuildCharge<'e>,
        _record_job: bool,
    ) -> Result<CompactionReport, CompactionError> {
        let report = CompactionReport {
            kind: self.kind,
            level: self.level,
            inputs: inputs.len(),
            input_bytes: inputs.iter().map(|r| r.run.stored_len()).sum(),
            input_held_bytes: inputs.iter().map(|run| run.run.held_bytes()).sum(),
            outputs: output.runs.len(),
            output_bytes: output.output_bytes,
            output_held_bytes: output.held_bytes,
            filter_build_bytes: output
                .runs
                .iter()
                .map(Run::filter_build_bytes)
                .max()
                .unwrap_or(0),
            complete: self.is_complete(),
            stats: output.stats,
            workspace_bytes: output.workspace_bytes,
        };
        #[cfg(feature = "diagnostics")]
        diagnostics::observe("job_before_publish", self.engine.stats());
        let is_input = |run: &SharedRun| inputs.iter().any(|input| Arc::ptr_eq(input, run));
        let mut state = self.engine.write_state();
        // Dropping a keyspace waits for compactions, so it is live.
        let space = state.space_mut(self.keyspace);
        if space.runs.iter().filter(|run| is_input(run)).count() != inputs.len() {
            drop(state);
            unreachable!("compactions are serialized, so only this one removes runs");
        }
        #[cfg(feature = "diagnostics")]
        let bottom_bytes = if self.zero_seq {
            let bottom = space.levels.runs().last().unwrap();
            if self.source_count > 0 {
                let (mut blocks, mut raw, mut segments) = (0, 0, 0);
                for run in bottom.iter().filter(|r| is_input(r)) {
                    let view = run.run.view();
                    blocks += view.block_count() as u64;
                    raw += run.run.merged_raw_len() as u64;
                    segments += view.segment_count() as u64;
                }
                self.engine.chunks.bottom_inputs(blocks, raw, segments);
            }
            bottom
                .iter()
                .filter(|r| is_input(r))
                .map(|r| r.run.stored_len())
                .sum()
        } else {
            0
        };
        #[cfg(feature = "diagnostics")]
        let copied_raw = output.copied.raw as usize;
        let outputs: Vec<SharedRun> = published(output.runs).collect();
        if space.levels.replace(inputs, &outputs, self.dest).is_err() {
            return Err(CompactionError::Stale);
        }
        match &self.cursor {
            Some(Cursor::After(key)) => space.levels.advance(self.level, key),
            None => {}
        }
        #[cfg(feature = "diagnostics")]
        #[allow(irrefutable_let_patterns, reason = "test-only destinations")]
        if let Dest::Level(level) = self.dest {
            self.engine.chunks.work(
                crate::chunk::work_slot(Some(level), self.cuts.omit_filter),
                inputs
                    .iter()
                    .map(|run| run.run.merged_raw_len())
                    .sum::<usize>()
                    - copied_raw,
                outputs.iter().map(|run| run.run.raw_len()).sum::<usize>() - copied_raw,
                report.output_bytes,
            );
        }
        let space = state.space_mut(self.keyspace);
        space.runs.retain(|run| !is_input(run));
        space.runs.extend(outputs);
        state.published([self.keyspace]);
        state.retire_runs(self.keyspace, inputs);
        drop(charge);
        drop(state);
        self.engine.resources.changed();
        self.engine.flush_signal.request();
        #[cfg(feature = "diagnostics")]
        if _record_job && self.zero_seq {
            diagnostics::bottom_job(
                1,
                bottom_bytes,
                report.output_bytes,
                report.output_bytes + report.output_held_bytes,
            );
        }
        Ok(report)
    }
}

impl fmt::Debug for BuiltCompaction<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BuiltCompaction")
            .field("runs", &self.output.runs.len())
            .field("output_bytes", &self.output.output_bytes)
            .finish_non_exhaustive()
    }
}
