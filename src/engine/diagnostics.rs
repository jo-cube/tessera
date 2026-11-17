//! Repository diagnostics: work and memory by level, and experiment policy.
//! Built only with the `diagnostics` feature; not part of the stable API.

use super::Engine;
pub use crate::run::diagnostics::LevelCost;

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

/// Sets the process-wide compression policy for runs built from now on:
/// Zstandard levels above and at the bottom, and a fixed dictionary length,
/// or the adaptive rule if `None`.
pub fn set_policy(non_bottom: i32, bottom: i32, dictionary: Option<usize>) {
    crate::run::set_policy(non_bottom, bottom, dictionary);
}

/// Copies up to `raw_limit` raw block bytes, bottom level first, with their
/// segment dictionaries, then times compressing them at each of `levels`,
/// keeping the fastest of `rounds`.
pub fn level_costs(
    engine: &Engine,
    levels: &[i32],
    raw_limit: usize,
    rounds: usize,
) -> Vec<LevelCost> {
    let mut blocks = Vec::new();
    let mut dictionaries: Vec<Vec<u8>> = Vec::new();
    let mut raw = 0;
    {
        let state = engine.core.read_state();
        'levels: for runs in state.levels.runs().iter().rev() {
            for run in runs {
                let more = run.run.blocks(|dictionary, block| {
                    if dictionaries
                        .last()
                        .is_none_or(|last| last.as_slice() != dictionary)
                    {
                        dictionaries.push(dictionary.to_vec());
                    }
                    blocks.push((dictionaries.len() - 1, block.to_vec()));
                    raw += block.len();
                    raw < raw_limit
                });
                if !more {
                    break 'levels;
                }
            }
        }
    }
    crate::run::diagnostics::level_costs(&blocks, &dictionaries, levels, rounds)
}
