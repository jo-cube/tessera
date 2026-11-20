//! Engine options, their validation and compression.

use super::*;

/// Engine configuration, fixed at construction. See [Memory](super#memory).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct EngineOptions {
    /// Writer admission budget; `None` (the default) is unlimited.
    pub memory_limit: Option<usize>,
    /// Charged bytes per memtable, at least [`ENTRY_OVERHEAD`].
    pub memtable_budget: usize,
    /// Size multiplier between adjacent levels, at least 2 (default 10).
    pub level_ratio: usize,
    /// Parallel builders per compaction, at least 1 (default half the CPUs, capped at 4).
    pub background_threads: usize,
    /// Compression used for new runs.
    pub compression: Compression,
    /// Estimated compressed block bytes, from 1 byte to 64 KiB (default 1024).
    pub target_block_compressed: usize,
    // Deterministic scheduling and small run fixtures, absent from the public build.
    #[cfg(test)]
    pub(crate) target_run_len: usize,
    #[cfg(test)]
    pub(crate) last_seq: u64,
    #[cfg(test)]
    pub(crate) max_frozen_memtables: usize,
    #[cfg(test)]
    pub(crate) bulk_ingest_min: Option<usize>,
    #[cfg(test)]
    pub(crate) background_compaction: Option<CompactionPolicy>,
}

impl Default for EngineOptions {
    fn default() -> Self {
        Self {
            memory_limit: None,
            memtable_budget: 64 << 20,
            level_ratio: 10,
            background_threads: std::thread::available_parallelism()
                .map_or(1, |cpus| (cpus.get() / 2).clamp(1, 4)),
            compression: Compression::default(),
            target_block_compressed: TARGET_BLOCK_COMPRESSED,
            #[cfg(test)]
            target_run_len: TARGET_RUN_LEN,
            #[cfg(test)]
            last_seq: 0,
            #[cfg(test)]
            max_frozen_memtables: MAX_FROZEN_MEMTABLES,
            #[cfg(test)]
            bulk_ingest_min: None,
            #[cfg(test)]
            background_compaction: Some(CompactionPolicy::default()),
        }
    }
}

impl EngineOptions {
    /// The one check of these options, made before an engine starts a
    /// worker, allocates or touches a file. See [`ConfigError`].
    pub(super) fn validate(&self) -> Result<(), ConfigError> {
        ConfigError::check(
            "EngineOptions::memtable_budget",
            self.memtable_budget as u64,
            ENTRY_OVERHEAD as u64,
            (usize::MAX / 2) as u64,
            "it must admit the smallest record, and twice it must not overflow",
        )?;
        ConfigError::check(
            "EngineOptions::background_threads",
            self.background_threads as u64,
            1,
            usize::MAX as u64,
            "at least one builder is required",
        )?;
        ConfigError::check(
            "EngineOptions::level_ratio",
            self.level_ratio as u64,
            2,
            usize::MAX as u64,
            "level sizes must grow by at least two",
        )?;
        ConfigError::check(
            "EngineOptions::target_block_compressed",
            self.target_block_compressed as u64,
            1,
            RAW_BLOCK_CAP as u64,
            "compressed targets are from 1 byte to the 64 KiB raw cap",
        )?;
        Ok(())
    }
}

/// An option lies outside its supported range. Nothing was started or written.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct ConfigError {
    /// The setting, as `Type::field`.
    pub setting: &'static str,
    /// Its value.
    pub value: u64,
    /// The least value accepted, given the other settings.
    pub min: u64,
    /// The greatest value accepted.
    pub max: u64,
    /// Why the range is what it is.
    pub reason: &'static str,
}

impl ConfigError {
    /// Refuses `value` outside `min..=max`. Callers widen `usize` settings,
    /// which fit in `u64` on every target.
    fn check(
        setting: &'static str,
        value: u64,
        min: u64,
        max: u64,
        reason: &'static str,
    ) -> Result<(), Self> {
        if (min..=max).contains(&value) {
            return Ok(());
        }
        Err(Self {
            setting,
            value,
            min,
            max,
            reason,
        })
    }
}

impl fmt::Display for ConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let Self {
            setting,
            value,
            min,
            max,
            reason,
        } = self;
        if *max == u64::MAX {
            write!(
                f,
                "{setting} is {value} but must be at least {min}: {reason}"
            )
        } else {
            write!(
                f,
                "{setting} is {value} but must be from {min} to {max}: {reason}"
            )
        }
    }
}

impl std::error::Error for ConfigError {}

/// How runs store their blocks. Zstandard uses a 16 KiB raw-content dictionary
/// per roughly 4 MiB segment, with independently decoded adaptive blocks.
/// Every decode loads the dictionary by reference into the reader's context.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum Compression {
    /// Zstandard level 1, with segment dictionaries and adaptive blocks.
    #[default]
    Zstd,
    /// Fixed 16 KiB raw blocks, read in place.
    Uncompressed,
}
impl Compression {
    pub(super) fn codec(self) -> Codec {
        match self {
            Self::Zstd => Codec::Zstd,
            Self::Uncompressed => Codec::None,
        }
    }
}
