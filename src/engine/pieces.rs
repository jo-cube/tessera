//! Bounded bottom-level jobs. Each publication retires one consecutive range.
use super::*;
use std::collections::VecDeque;
mod concurrent;

/// Bottom input bytes per piece, in estimated full stored runs. A single
/// oversized bottom run is indivisible and is always accepted on its own.
const PIECE_RUNS: usize = 1;
/// One ordinary maximum block, a dictionary and ownership/index metadata must
/// fit even when tiny private test runs make the ratio estimate much smaller.
const MIN_PIECE_BUDGET: usize = 128 << 10;

impl<'e> CompactionJob<'e> {
    fn piece_budget(&self) -> usize {
        #[cfg(test)]
        {
            let override_bytes = self
                .engine
                .piece_budget
                .load(std::sync::atomic::Ordering::Relaxed);
            if override_bytes != 0 {
                return override_bytes;
            }
        }
        let minimum = if self.engine.background_threads == 1 {
            1
        } else {
            MIN_PIECE_BUDGET
        };
        self.estimated_run_bytes
            .saturating_mul(PIECE_RUNS)
            .max(minimum)
    }

    /// Charges a read workspace that bounds `runs`, for as long as it is held.
    fn read_charge(&self, runs: &[SharedRun]) -> resources::BuildCharge<'e> {
        let largest = runs.iter().map(|run| run.largest_block).max().unwrap_or(0);
        let estimate = crate::run::ReadWorkspace::estimate(largest);
        self.engine.resources.build(0, estimate)
    }

    pub(super) fn execute(
        self,
        workspace: &mut MergeWorkspace,
    ) -> Result<CompactionReport, CompactionError> {
        let bottom_bytes: usize = self.inputs[self.source_count..]
            .iter()
            .map(|r| r.run.stored_len())
            .sum();
        if self.source_count == 0 || !self.zero_seq || bottom_bytes <= self.piece_budget() {
            return self.build(workspace)?.publish();
        }
        if self.engine.background_threads > 1 {
            self.execute_concurrent()
        } else {
            self.execute_pieces(workspace)
        }
    }

    fn execute_pieces(
        mut self,
        workspace: &mut MergeWorkspace,
    ) -> Result<CompactionReport, CompactionError> {
        let mut report = CompactionReport {
            kind: self.kind,
            level: self.level,
            inputs: self.inputs.len(),
            input_bytes: self.input_bytes(),
            input_held_bytes: self.inputs.iter().map(|r| r.run.held_bytes()).sum(),
            outputs: 0,
            output_bytes: 0,
            output_held_bytes: 0,
            filter_build_bytes: 0,
            complete: self.is_complete(),
            stats: CompactStats::default(),
            workspace_bytes: 0,
        };
        let budget = self.piece_budget();
        let mut sources: Vec<_> = self.inputs.drain(..self.source_count).collect();
        let mut bottom: VecDeque<_> = std::mem::take(&mut self.inputs).into();
        let mut piece_count = 0;
        #[cfg(feature = "diagnostics")]
        let mut peak_output = 0;
        #[cfg(feature = "diagnostics")]
        let rewritten_bottom = bottom.iter().map(|r| r.run.stored_len()).sum::<usize>();
        while !bottom.is_empty() {
            #[cfg(test)]
            if self
                .engine
                .fail_piece
                .load(std::sync::atomic::Ordering::Relaxed)
                == piece_count + 1
            {
                return Err(CompactionError::Stale);
            }
            let mut bytes = 0usize;
            let mut selected = Vec::new();
            while let Some(next) = bottom.front() {
                let size = next.run.stored_len();
                if !selected.is_empty() && bytes.saturating_add(size) > budget {
                    break;
                }
                bytes = bytes.saturating_add(size);
                selected.push(bottom.pop_front().unwrap());
            }
            let end = bottom
                .front()
                .map(|r| r.run.key_range().unwrap().min.to_vec());
            // Charged for the split only, so never beside this piece's build.
            let reader_charge = self.read_charge(&sources);
            let mut reader = crate::run::ReadWorkspace::new();
            let mut replacements = Vec::with_capacity(sources.len());
            for source in sources.drain(..) {
                if let Some(run) = source
                    .run
                    .bounded(None, end.as_deref(), &mut reader)
                    .map_err(ReadError::from)?
                {
                    self.inputs.push(view_of(&source, run));
                }
                let remainder = if let Some(end) = &end {
                    source
                        .run
                        .bounded(Some(end), None, &mut reader)
                        .map_err(ReadError::from)?
                        .map(|run| view_of(&source, run))
                } else {
                    None
                };
                replacements.push((source, remainder));
            }
            drop(reader);
            drop(reader_charge);
            self.inputs.extend(selected.iter().cloned());
            let built = self.build_with_threads(workspace, 1)?;
            let (part, mut job) = built.publish_piece(&replacements, &selected)?;
            // Drop every reference to this piece's bottom inputs immediately.
            job.inputs.clear();
            self = job;
            sources.extend(
                replacements
                    .into_iter()
                    .filter_map(|(_, remainder)| remainder),
            );
            piece_count += 1;
            #[cfg(feature = "diagnostics")]
            {
                peak_output = peak_output.max(part.output_bytes + part.output_held_bytes);
            }
            add_report(&mut report, part);
        }
        #[cfg(feature = "diagnostics")]
        diagnostics::bottom_job(
            piece_count,
            rewritten_bottom,
            report.output_bytes,
            peak_output,
        );
        let _ = piece_count;
        Ok(report)
    }
}

/// A new directory entry shares physical storage and its checkpoint identity.
fn view_of(source: &SharedRun, run: Run<Vec<u8>>) -> SharedRun {
    Arc::new(Published {
        id: source.id,
        checkpoint_crc: source.checkpoint_crc.clone(),
        run,
        largest_block: source.largest_block,
    })
}

fn add_report(sum: &mut CompactionReport, part: CompactionReport) {
    sum.outputs += part.outputs;
    sum.output_bytes += part.output_bytes;
    sum.output_held_bytes += part.output_held_bytes;
    sum.filter_build_bytes = sum.filter_build_bytes.max(part.filter_build_bytes);
    sum.workspace_bytes = sum.workspace_bytes.max(part.workspace_bytes);
    sum.stats.records_read += part.stats.records_read;
    sum.stats.records_written += part.stats.records_written;
    sum.stats.versions_dropped += part.stats.versions_dropped;
    sum.stats.copies_collapsed += part.stats.copies_collapsed;
    sum.stats.tombstones_dropped += part.stats.tombstones_dropped;
}

impl<'e> BuiltCompaction<'e> {
    fn publish_piece(
        self,
        sources: &[(SharedRun, Option<SharedRun>)],
        bottom: &[SharedRun],
    ) -> Result<(CompactionReport, CompactionJob<'e>), CompactionError> {
        let Self {
            job,
            output,
            charge,
        } = self;
        let report = CompactionReport {
            kind: job.kind,
            level: job.level,
            inputs: job.inputs.len(),
            input_bytes: job.input_bytes(),
            input_held_bytes: job.inputs.iter().map(|r| r.run.held_bytes()).sum(),
            outputs: output.runs.len(),
            output_bytes: output.output_bytes,
            output_held_bytes: output.held_bytes,
            filter_build_bytes: output
                .runs
                .iter()
                .map(Run::filter_build_bytes)
                .max()
                .unwrap_or(0),
            complete: job.is_complete(),
            stats: output.stats,
            workspace_bytes: output.workspace_bytes,
        };
        #[cfg(feature = "diagnostics")]
        diagnostics::observe("piece_before_publish", job.engine.stats());
        let outputs: Vec<_> = published(output.runs).collect();
        let mut state = job.engine.write_state();
        let space = state.space_mut(job.keyspace);
        space
            .levels
            .replace_piece(sources, bottom, &outputs, job.dest)
            .map_err(|_| CompactionError::Stale)?;
        let is_removed = |run: &SharedRun| {
            bottom.iter().any(|r| Arc::ptr_eq(r, run))
                || sources.iter().any(|(r, _)| Arc::ptr_eq(r, run))
        };
        space.runs.retain(|r| !is_removed(r));
        space
            .runs
            .extend(sources.iter().filter_map(|(_, r)| r.clone()));
        space.runs.extend(outputs.iter().cloned());
        state.published([job.keyspace]);
        let removed: Vec<_> = bottom
            .iter()
            .chain(sources.iter().map(|(r, _)| r))
            .cloned()
            .collect();
        state.retire_runs(job.keyspace, &removed);
        #[cfg(feature = "diagnostics")]
        {
            let (mut blocks, mut raw, mut segments) = (0, 0, 0);
            for run in bottom {
                let view = run.run.view();
                blocks += view.block_count() as u64;
                raw += run.run.merged_raw_len() as u64;
                segments += view.segment_count() as u64;
            }
            job.engine.chunks.bottom_inputs(blocks, raw, segments);
            let copied_raw = output.copied.raw as usize;
            #[allow(irrefutable_let_patterns, reason = "test-only destinations")]
            if let Dest::Level(level) = job.dest {
                job.engine.chunks.work(
                    crate::chunk::work_slot(Some(level), true),
                    job.inputs
                        .iter()
                        .map(|r| r.run.merged_raw_len())
                        .sum::<usize>()
                        - copied_raw,
                    outputs.iter().map(|r| r.run.raw_len()).sum::<usize>() - copied_raw,
                    report.output_bytes,
                );
            }
        }
        drop(charge);
        drop(state);
        job.engine.resources.changed();
        job.engine.flush_signal.request();
        #[cfg(feature = "diagnostics")]
        diagnostics::observe("piece_after_publish", job.engine.stats());
        Ok((report, job))
    }
}

#[cfg(test)]
mod tests;
