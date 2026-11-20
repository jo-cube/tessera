//! Independently published disjoint ranges, with reservations covering every
//! builder's unpublished output before it allocates stored frames.
use super::*;
use std::sync::Condvar;
#[cfg(any(test, feature = "diagnostics"))]
use std::sync::atomic::Ordering::Relaxed;

struct Piece {
    index: usize,
    inputs: Vec<SharedRun>,
    request: usize,
}
struct Queue {
    pending: VecDeque<Piece>,
    reserved: usize,
    running: usize,
    stopped: bool,
    error: Option<CompactionError>,
    report: CompactionReport,
    completed: usize,
    peak_output: usize,
}
struct Pipeline {
    queue: Mutex<Queue>,
    changed: Condvar,
    capacity: usize,
    /// Whether `capacity` is a hard limit that refuses larger pieces, as
    /// tests force. Otherwise a larger piece runs alone.
    #[cfg(test)]
    hard_cap: bool,
}
struct Lease<'a> {
    pipeline: &'a Pipeline,
    bytes: usize,
}
impl Drop for Lease<'_> {
    fn drop(&mut self) {
        let mut queue = self
            .pipeline
            .queue
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        queue.reserved -= self.bytes;
        queue.running -= 1;
        if std::thread::panicking() {
            queue.stopped = true;
        }
        self.pipeline.changed.notify_all();
    }
}
impl Pipeline {
    /// Whether a piece that exceeded its request may retry with more.
    fn grows(&self, piece: &Piece) -> bool {
        #[cfg(test)]
        if self.hard_cap {
            return piece.request < self.capacity;
        }
        let _ = piece;
        true
    }
    fn take(&self) -> Option<(Piece, Lease<'_>)> {
        let mut queue = self.queue.lock().unwrap_or_else(PoisonError::into_inner);
        loop {
            if queue.stopped || queue.pending.is_empty() {
                return None;
            }
            let request = queue.pending[0].request;
            // A piece larger than the whole allowance runs alone.
            if request <= self.capacity - queue.reserved.min(self.capacity) || queue.reserved == 0 {
                let piece = queue.pending.pop_front().unwrap();
                queue.reserved += piece.request;
                queue.running += 1;
                let lease = Lease {
                    pipeline: self,
                    bytes: piece.request,
                };
                return Some((piece, lease));
            }
            queue = self
                .changed
                .wait(queue)
                .unwrap_or_else(PoisonError::into_inner);
        }
    }
}

impl CompactionJob<'_> {
    pub(super) fn execute_concurrent(mut self) -> Result<CompactionReport, CompactionError> {
        let mut report = CompactionReport {
            kind: self.kind,
            level: self.level,
            inputs: self.inputs.len(),
            input_bytes: self.input_bytes(),
            input_held_bytes: physical_runs(&self.inputs)
                .map(|r| r.run.held_bytes())
                .sum(),
            outputs: 0,
            output_bytes: 0,
            output_held_bytes: 0,
            filter_build_bytes: 0,
            complete: self.is_complete(),
            stats: CompactStats::default(),
            workspace_bytes: 0,
        };
        let bottom_bytes: usize = self.inputs[self.source_count..]
            .iter()
            .map(|r| r.run.stored_len())
            .sum();
        let bottom_budget = self.piece_budget();
        // Each piece's allowance has the same floor as its input group, so
        // that one group always fits. Tests can force smaller input groups
        // without reducing the allowance to a few bytes.
        let output_budget = self
            .estimated_run_bytes
            .saturating_mul(PIECE_RUNS)
            .max(MIN_PIECE_BUDGET);
        #[cfg(test)]
        let forced = self.engine.piece_output_budget.load(Relaxed);
        #[cfg(test)]
        let output_budget = if forced == 0 { output_budget } else { forced };
        let capacity = output_budget.saturating_mul(self.engine.background_threads);
        let sources: Vec<_> = self.inputs.drain(..self.source_count).collect();
        let mut bottom: VecDeque<_> = std::mem::take(&mut self.inputs).into();
        let mut groups: Vec<Vec<SharedRun>> = Vec::new();
        while !bottom.is_empty() {
            let mut bytes = 0usize;
            let mut group = Vec::new();
            while let Some(next) = bottom.front() {
                let size = next.run.stored_len();
                if !group.is_empty() && bytes.saturating_add(size) > bottom_budget {
                    break;
                }
                bytes = bytes.saturating_add(size);
                group.push(bottom.pop_front().unwrap());
            }
            groups.push(group);
        }
        let starts: Vec<_> = groups
            .iter()
            .skip(1)
            .map(|g| g[0].run.key_range().unwrap().min.to_vec())
            .collect();
        let mut tasks: Vec<_> = groups
            .into_iter()
            .enumerate()
            .map(|(index, inputs)| Piece {
                index,
                inputs,
                request: 0,
            })
            .collect();
        let mut split = Vec::with_capacity(sources.len());
        let mut workspace = crate::run::ReadWorkspace::new();
        for source in sources {
            let mut aliases = Vec::new();
            for piece in &mut tasks {
                let lo = piece.index.checked_sub(1).map(|i| starts[i].as_slice());
                let hi = starts.get(piece.index).map(Vec::as_slice);
                if let Some(run) = source
                    .run
                    .bounded(lo, hi, &mut workspace)
                    .map_err(ReadError::from)?
                {
                    let alias = view_of(&source, run);
                    aliases.push(alias.clone());
                    piece.inputs.push(alias);
                }
            }
            split.push((source, aliases));
        }
        // Metadata-only publication partitions each source exactly once. Every
        // piece now owns actual directory entries and can finish in any order.
        {
            let mut state = self.engine.write_state();
            state
                .levels
                .split_sources(&split)
                .map_err(|_| CompactionError::Stale)?;
            state
                .runs
                .retain(|r| !split.iter().any(|(old, _)| Arc::ptr_eq(old, r)));
            state
                .runs
                .extend(split.iter().flat_map(|(_, views)| views.iter().cloned()));
            state.recount_runs();
            state.order_runs();
        }
        drop(split);
        drop(starts);
        drop(workspace);
        for piece in &mut tasks {
            let estimate = piece.inputs.iter().fold(0usize, |sum, r| {
                sum.saturating_add(r.run.range_stored_estimate())
            });
            // Inputs, an eighth for recompression and copied frames, and
            // each output run's footer and index minimum. It is not capped by
            // the allowance: a piece above it waits to run alone.
            let runs = estimate / self.estimated_run_bytes.max(1) + piece.inputs.len() + 1;
            piece.request = estimate
                .saturating_add(estimate / 8)
                .saturating_add(runs.saturating_mul(1024))
                .max(output_budget);
            #[cfg(test)]
            if forced != 0 {
                piece.request = piece.request.min(capacity);
            }
            #[cfg(test)]
            match self.engine.piece_request.load(Relaxed) {
                0 => {}
                n => piece.request = n,
            }
        }
        let workers = self.engine.background_threads.min(tasks.len()).max(1);
        let pipeline = Pipeline {
            queue: Mutex::new(Queue {
                pending: tasks.into(),
                reserved: 0,
                running: 0,
                stopped: false,
                error: None,
                report,
                completed: 0,
                peak_output: 0,
            }),
            changed: Condvar::new(),
            capacity,
            #[cfg(test)]
            hard_cap: forced != 0,
        };
        #[cfg(feature = "diagnostics")]
        self.engine
            .resources
            .output_limit
            .fetch_max(capacity, Relaxed);
        std::thread::scope(|scope| {
            let handles: Vec<_> = (0..workers)
                .map(|_| {
                    let job = &self;
                    let pipeline = &pipeline;
                    scope.spawn(move || job.piece_worker(pipeline, workers))
                })
                .collect();
            for handle in handles {
                handle
                    .join()
                    .unwrap_or_else(|panic| std::panic::resume_unwind(panic));
            }
        });
        let queue = pipeline
            .queue
            .into_inner()
            .unwrap_or_else(PoisonError::into_inner);
        if let Some(error) = queue.error {
            return Err(error);
        }
        report = queue.report;
        #[cfg(feature = "diagnostics")]
        diagnostics::bottom_job(
            queue.completed,
            bottom_bytes,
            report.output_bytes,
            queue.peak_output,
        );
        let _ = bottom_bytes;
        Ok(report)
    }

    fn piece_worker(&self, pipeline: &Pipeline, workers: usize) {
        let mut workspace = MergeWorkspace::new();
        while let Some((mut piece, lease)) = pipeline.take() {
            #[cfg(feature = "diagnostics")]
            {
                let queue = pipeline
                    .queue
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner);
                self.engine
                    .resources
                    .peak_reserved
                    .fetch_max(queue.reserved, Relaxed);
                self.engine
                    .resources
                    .peak_active
                    .fetch_max(queue.running, Relaxed);
            }
            #[cfg(test)]
            {
                let hook = self.engine.before_piece.lock().unwrap().clone();
                if let Some(hook) = hook {
                    hook(piece.index);
                }
            }
            let budget =
                crate::run::OutputBudget::new(&self.engine.resources.outputs, piece.request);
            let result = self.build_inputs(
                &piece.inputs,
                &mut workspace,
                1,
                self.options.max_workspace_bytes / workers,
                Some(budget.clone()),
            );
            let mut queue = pipeline
                .queue
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            if !queue.stopped {
                match result {
                    Err(CompactionError::OutputLimit { .. }) if pipeline.grows(&piece) => {
                        // Drop failed output, then retry with at least the
                        // whole allowance, so alone, and double the request
                        // each time. Output is finite, so this ends. The
                        // shared cap is unchanged, and no partial range was exposed.
                        piece.request = piece.request.saturating_mul(2).max(pipeline.capacity);
                        queue.pending.push_front(piece);
                        #[cfg(feature = "diagnostics")]
                        self.engine.resources.piece_retries.fetch_add(1, Relaxed);
                    }
                    Err(error) => {
                        queue.error = Some(error);
                        queue.stopped = true;
                    }
                    Ok((output, charge)) => {
                        let bytes = output.output_bytes + output.held_bytes;
                        match self.publish_inputs(&piece.inputs, output, charge, false) {
                            Ok(part) => {
                                queue.completed += 1;
                                queue.peak_output = queue.peak_output.max(bytes);
                                add_report(&mut queue.report, part);
                            }
                            Err(error) => {
                                queue.error = Some(error);
                                queue.stopped = true;
                            }
                        }
                    }
                }
            } else {
                drop(result);
            }
            drop(queue);
            // Release input references and output accounting before admitting
            // another piece. The lease also unwinds on a builder panic.
            drop(budget);
            drop(lease);
        }
    }
}
