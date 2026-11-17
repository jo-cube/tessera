//! Repository diagnostics: work and memory by level.
//! Built only with the `diagnostics` feature; not part of the stable API.

use super::Engine;
pub use crate::chunk::Erasure;

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
            for run in runs {
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

/// What sequence erasure did to the records written into the bottom level.
pub fn erasure(engine: &Engine) -> Erasure {
    engine.core.chunks.erasure_counters()
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
