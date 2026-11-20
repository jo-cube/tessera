//! Repository diagnostics: work and memory by level.
//! Built only with the `diagnostics` feature; not part of the stable API.

use super::{CompactionError, Engine};
pub use crate::chunk::Erasure;
pub use crate::run::MergeWorkspace;

/// Bytes one purpose read and wrote.
#[derive(Clone, Copy, Debug, Default)]
pub struct Work {
    /// Raw block bytes of the inputs, decoded by compaction.
    pub raw_in: u64,
    /// Raw block bytes compressed into the output.
    pub raw_out: u64,
    /// Stored bytes written.
    pub stored: u64,
}

/// Work by destination: index 0 is flush; `n` is compaction into level `n`
/// above the bottom, and `n + 8` into level `n` as the bottom level.
pub fn work(engine: &Engine) -> Vec<Work> {
    engine
        .core
        .chunks
        .work_counters()
        .iter()
        .map(|&[raw_in, raw_out, stored]| Work {
            raw_in,
            raw_out,
            stored,
        })
        .collect()
}

/// Stored bytes of one level's runs by component.
#[derive(Clone, Copy, Debug, Default)]
pub struct LevelMemory {
    /// Runs in the level.
    pub runs: usize,
    /// Stored records, counting every version.
    pub records: usize,
    /// Raw bytes of their blocks.
    pub raw: usize,
    /// Compressed frames, including headers.
    pub frames: usize,
    /// Segment dictionaries.
    pub dictionaries: usize,
    /// Segment table entries.
    pub segment_table: usize,
    /// Block index entries, keys and rebuilt restart heads.
    pub index: usize,
    /// Bloom filters.
    pub filters: usize,
    /// Footers.
    pub footer: usize,
    /// Physical chunk and piece metadata.
    pub physical: usize,
}

/// Memory of each level, level 0 first.
pub fn levels(engine: &Engine) -> Vec<LevelMemory> {
    let state = engine.core.read_state();
    state
        .levels
        .runs()
        .iter()
        .map(|runs| {
            let mut level = LevelMemory {
                runs: runs.len(),
                ..LevelMemory::default()
            };
            for run in super::physical_runs(runs) {
                let m = run.run.memory();
                level.records += m.records;
                level.raw += m.raw;
                level.frames += m.frames;
                level.dictionaries += m.dictionaries;
                level.segment_table += m.segment_table;
                level.index += m.index;
                level.filters += m.filters;
                level.footer += m.footer;
                level.physical += m.physical;
            }
            level
        })
        .collect()
}

/// Drained run shape for the streaming comparison harness.
#[derive(Clone, Debug, Default)]
pub struct StreamingShape {
    /// Published run counts, level 0 first and bottom level last.
    pub runs_per_level: Vec<usize>,
    /// Maximum number of runs whose key ranges overlap.
    pub max_overlap: usize,
    /// Number of level-0 runs.
    pub level0_runs: usize,
    /// Number of physical bottom runs.
    pub bottom_runs: usize,
    /// Mean serialized bytes per physical bottom run.
    pub bottom_mean_stored_bytes: usize,
    /// Published bounded views (aliases of a physical run).
    pub bounded_views: usize,
    /// Serialized bytes of all published physical runs.
    pub stored_bytes: usize,
}

/// Summarizes the currently published shape after the caller drains the engine.
pub fn streaming_shape(engine: &Engine) -> StreamingShape {
    let state = engine.core.read_state();
    let levels = state.levels.runs();
    let bottom = levels.last().map_or(&[][..], Vec::as_slice);
    // Aliases retain their physical file ID; this also works on Part 1,
    // where only one current view per physical run can remain.
    let mut seen = std::collections::HashSet::new();
    let stored_bytes: usize = state
        .runs
        .iter()
        .filter(|run| seen.insert(run.id))
        .map(|run| run.run.stored_len())
        .sum();
    seen.clear();
    let physical_bottom: Vec<_> = bottom.iter().filter(|run| seen.insert(run.id)).collect();
    let bottom_runs = physical_bottom.len();
    let bottom_bytes: usize = physical_bottom.iter().map(|run| run.run.stored_len()).sum();
    StreamingShape {
        runs_per_level: levels.iter().map(Vec::len).collect(),
        max_overlap: state.directory.group_count(),
        level0_runs: levels.first().map_or(0, Vec::len),
        bottom_runs,
        bottom_mean_stored_bytes: bottom_bytes.checked_div(bottom_runs).unwrap_or(0),
        bounded_views: levels
            .iter()
            .flatten()
            .filter(|run| run.run.slice().is_some())
            .count(),
        stored_bytes,
    }
}

/// What sequence erasure did to the records written into the bottom level.
pub fn erasure(engine: &Engine) -> Erasure {
    engine.core.chunks.erasure_counters()
}

pub use crate::chunk::CopyForward;

/// What merges into the bottom level copied forward.
pub fn copy_forward(engine: &Engine) -> CopyForward {
    engine.core.chunks.copy_counters()
}

pub use crate::run::SegmentSample;
pub use crate::run::copy_audit::SegmentCopy;

/// Starts auditing, for every later level-0 → bottom merge in this
/// process, which bottom input blocks could be copied verbatim.
pub fn enable_copy_audit() {
    crate::run::copy_audit::enable();
}

/// Drains the bottom input segments audited so far, one entry per segment
/// and merge (a segment split between parallel builders appears twice), and
/// the raw bytes of the other (level-0) inputs' blocks those merges decoded.
pub fn take_copy_audit() -> (Vec<SegmentCopy>, u64) {
    crate::run::copy_audit::take()
}

/// Every `every`-th Zstandard segment of the bottom level's physical runs,
/// decoded, with its dictionary and the builder's block-size hint.
pub fn bottom_segments(engine: &Engine, every: usize) -> Vec<SegmentSample> {
    let state = engine.core.read_state();
    let levels = state.levels.runs();
    let bottom = levels.last().map_or(&[][..], Vec::as_slice);
    let mut out = Vec::new();
    let mut ordinal = 0usize;
    let mut keep = || {
        ordinal += 1;
        (ordinal - 1).is_multiple_of(every.max(1))
    };
    for run in super::physical_runs(bottom) {
        run.run.segment_samples(&mut keep, &mut out);
    }
    out
}

/// Raw block bytes of the bottom level's physical runs.
pub fn bottom_raw(engine: &Engine) -> usize {
    let state = engine.core.read_state();
    let levels = state.levels.runs();
    let bottom = levels.last().map_or(&[][..], Vec::as_slice);
    super::physical_runs(bottom)
        .map(|run| run.run.raw_len())
        .sum()
}

static MEMORY_OBSERVER: std::sync::OnceLock<fn(&str, super::EngineStats)> =
    std::sync::OnceLock::new();

/// Install a process-wide maintenance boundary sampler for repository tools.
pub fn set_memory_observer(observer: fn(&str, super::EngineStats)) {
    assert!(MEMORY_OBSERVER.set(observer).is_ok());
}
pub(super) fn observe(reason: &str, stats: super::EngineStats) {
    if let Some(observer) = MEMORY_OBSERVER.get() {
        observer(reason, stats);
    }
}
/// One completed bottom job, including all its separately published pieces.
#[derive(Clone, Copy, Debug)]
pub struct BottomJob {
    /// Atomic range publications.
    pub pieces: usize,
    /// Stored bytes of bottom inputs rewritten, counted once.
    pub bottom_bytes: usize,
    /// Stored output of all pieces.
    pub output_bytes: usize,
    /// Largest piece output, including held metadata, before publication.
    pub peak_output_bytes: usize,
}
static JOB_OBSERVER: std::sync::OnceLock<fn(BottomJob)> = std::sync::OnceLock::new();
/// Install the repository tool's per-job counter sink.
pub fn set_bottom_job_observer(observer: fn(BottomJob)) {
    assert!(JOB_OBSERVER.set(observer).is_ok());
}
pub(super) fn bottom_job(
    pieces: usize,
    bottom_bytes: usize,
    output_bytes: usize,
    peak_output_bytes: usize,
) {
    if let Some(observer) = JOB_OBSERVER.get() {
        observer(BottomJob {
            pieces,
            bottom_bytes,
            output_bytes,
            peak_output_bytes,
        });
    }
}

/// Concurrent-piece memory and writer-backpressure counters.
#[derive(Clone, Copy, Debug, Default)]
pub struct Maintenance {
    /// Current unpublished bytes, including conservative metadata allowances.
    pub unpublished_bytes: usize,
    /// Peak of that counter, measured before output copies are allocated.
    pub peak_unpublished_bytes: usize,
    /// Largest job's aggregate output allowance.
    pub output_limit: usize,
    /// Largest sum of running pieces' output reservations.
    pub peak_reserved_bytes: usize,
    /// Most simultaneously building pieces.
    pub peak_active_pieces: usize,
    /// Pieces retried after an estimate proved too small.
    pub piece_retries: usize,
    /// Sum of writer nanoseconds waiting at admission, frozen tables and L0 stop.
    pub writer_wait_ns: u64,
    /// Writer waits at the level-0 stop (one per loop iteration).
    pub level0_waits: u64,
    /// Writer nanoseconds waiting at the level-0 stop.
    pub level0_wait_ns: u64,
}
/// Read counters without acquiring the engine state lock.
pub fn maintenance(engine: &Engine) -> Maintenance {
    use std::sync::atomic::Ordering::Relaxed;
    let p = &engine.core.resources;
    let (current, peak) = p.outputs.snapshot();
    Maintenance {
        unpublished_bytes: current,
        peak_unpublished_bytes: peak,
        output_limit: p.output_limit.load(Relaxed),
        peak_reserved_bytes: p.peak_reserved.load(Relaxed),
        peak_active_pieces: p.peak_active.load(Relaxed),
        piece_retries: p.piece_retries.load(Relaxed),
        writer_wait_ns: p.writer_wait_ns.load(Relaxed),
        level0_waits: p.level0_waits.load(Relaxed),
        level0_wait_ns: p.level0_wait_ns.load(Relaxed),
    }
}

/// Flush groups level 0 holds runs of now.
pub fn level0_flushes(engine: &Engine) -> usize {
    engine.core.read_state().levels.level0_flushes()
}

/// Holds off new compaction jobs until the guard drops, after any job in
/// progress has begun. Writers still stop at the level-0 stop, so a caller
/// that writes while holding it must stay below the stop or deadlock.
pub fn hold_compaction(engine: &Engine) -> std::sync::MutexGuard<'_, ()> {
    engine
        .core
        .compacting
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

/// Performs one eager compaction step with the caller's workspace, and
/// returns whether one was eligible. Serializes with other compactions.
///
/// # Errors
/// Returns [`CompactionError`] for a read failure or invalidated selection.
///
/// # Panics
/// Panics if the state lock is poisoned or a synchronous builder panics.
pub fn compact_step(
    engine: &Engine,
    workspace: &mut MergeWorkspace,
) -> Result<bool, CompactionError> {
    engine.compact(workspace).map(|report| report.is_some())
}

#[cfg(test)]
mod streaming_tests {
    use super::*;
    use crate::engine::{EngineOptions, EngineWorkspace};

    #[test]
    fn point_filter_probes_exclude_range_and_sequence_pruning() {
        let engine = Engine::new(EngineOptions {
            background_compaction: None,
            ..EngineOptions::default()
        })
        .unwrap();
        for value in [b"old", b"new"] {
            engine.put(b"a", value).unwrap();
            engine.put(b"z", value).unwrap();
            engine.flush().unwrap();
        }
        let mut ws = EngineWorkspace::new();
        assert!(engine.get(b"b", &mut ws).unwrap().is_none());
        assert_eq!(ws.filter_probes(), 2);
        assert_eq!(engine.get(b"a", &mut ws).unwrap().unwrap().value, b"new");
        assert_eq!(ws.filter_probes(), 3);
        assert!(engine.get(b"0", &mut ws).unwrap().is_none());
        assert_eq!(ws.filter_probes(), 3);
        let shape = streaming_shape(&engine);
        assert_eq!(
            shape.runs_per_level.iter().sum::<usize>(),
            engine.stats().runs
        );
        assert_eq!(shape.stored_bytes, engine.stats().run_bytes);
        assert_eq!(shape.bounded_views, 0);
    }
}
