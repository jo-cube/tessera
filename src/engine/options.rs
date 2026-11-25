//! Engine options, their validation and compression.

use super::*;

/// Engine configuration, fixed when the engine opens. Start from
/// [`Options::default`] and assign the fields to change; fields may be added.
///
/// ```
/// let mut options = tessera::Options::default();
/// options.memory_limit = Some(256 << 20);
/// options.memtable_size = 32 << 20;
/// let engine = tessera::Engine::in_memory(options)?;
/// # Ok::<(), tessera::Error>(())
/// ```
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct Options {
    /// The writer admission budget in bytes, at least 2 MiB; `None` (the
    /// default) is unlimited. See [Memory](crate#memory).
    pub memory_limit: Option<usize>,
    /// Bytes one memtable is charged before it is flushed into runs
    /// (default 64 MiB). A record is charged its key, its value and 80 bytes,
    /// so this also bounds the largest record.
    pub memtable_size: usize,
    /// The size ratio between adjacent levels, at least 2 (default 10).
    pub level_ratio: usize,
    /// Threads building one compaction in parallel, at least 1 (default half
    /// the CPUs, from 1 to 4).
    pub background_threads: usize,
    /// How new runs store their blocks (default [`Compression::Zstd`]).
    pub compression: Compression,
    /// The target **stored** bytes of a block, after compression, from 1
    /// byte to 64 KiB (default 1 KiB). Smaller blocks decode less per point
    /// read and compress worse. [`Compression::Uncompressed`] ignores it and
    /// stores 16 KiB raw blocks.
    pub block_size: usize,
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

impl Default for Options {
    fn default() -> Self {
        Self {
            memory_limit: None,
            memtable_size: 64 << 20,
            level_ratio: 10,
            background_threads: std::thread::available_parallelism()
                .map_or(1, |cpus| (cpus.get() / 2).clamp(1, 4)),
            compression: Compression::default(),
            block_size: TARGET_BLOCK_COMPRESSED,
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

/// The least `memory_limit`. The active memtable takes memory in 1 MiB pool
/// chunks, each counted in full when taken, so a smaller limit leaves no
/// room to fill one and makes nearly every write flush or fail.
pub(super) const MIN_MEMORY_LIMIT: usize = 2 * crate::chunk::CHUNK_BYTES;

impl Options {
    /// The one check of these options, made before an engine starts a
    /// worker, allocates or touches a file. See [`ConfigError`].
    pub(super) fn validate(&self) -> Result<(), ConfigError> {
        if let Some(limit) = self.memory_limit {
            ConfigError::check(
                "Options::memory_limit",
                limit as u64,
                MIN_MEMORY_LIMIT as u64,
                usize::MAX as u64,
                "the active memtable takes memory in 1 MiB chunks, so the limit must leave room to fill one",
            )?;
        }
        ConfigError::check(
            "Options::memtable_size",
            self.memtable_size as u64,
            (ENTRY_OVERHEAD + MAX_PREFIX_LEN) as u64,
            (usize::MAX / 2) as u64,
            "it must admit the smallest record in any keyspace, and twice it must not overflow",
        )?;
        ConfigError::check(
            "Options::background_threads",
            self.background_threads as u64,
            1,
            usize::MAX as u64,
            "at least one builder is required",
        )?;
        ConfigError::check(
            "Options::level_ratio",
            self.level_ratio as u64,
            2,
            usize::MAX as u64,
            "level sizes must grow by at least two",
        )?;
        ConfigError::check(
            "Options::block_size",
            self.block_size as u64,
            1,
            RAW_BLOCK_CAP as u64,
            "compressed targets are from 1 byte to the 64 KiB raw cap",
        )?;
        Ok(())
    }
}

/// An option lies outside its supported range. Nothing was started or written.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct ConfigError {
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

/// How new runs store their blocks. Zstandard uses a 16 KiB raw-content
/// dictionary per roughly 4 MiB segment, with independently decoded blocks
/// sized to [`Options::block_size`]. Every decode loads the dictionary by
/// reference into the reader's context.
///
/// Every block records how it is stored, so an engine reads runs built with
/// any setting. A change applies to newly encoded blocks: blocks that
/// compaction copies forward keep the setting they were built with.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum Compression {
    /// Zstandard level 1, with segment dictionaries and adaptive blocks.
    #[default]
    Zstd,
    /// Raw 16 KiB blocks, read in place.
    Uncompressed,
    /// Zstandard level 1 with literals stored uncompressed: faster gets,
    /// larger runs. Weakly compressible data comes out larger than with
    /// [`Uncompressed`](Self::Uncompressed), the better choice there.
    ZstdFastDecode,
}
impl Compression {
    pub(super) fn encoding(self) -> Encoding {
        match self {
            Self::Zstd => Codec::Zstd.into(),
            Self::Uncompressed => Codec::None.into(),
            Self::ZstdFastDecode => Encoding {
                codec: Codec::Zstd,
                raw_literals: true,
            },
        }
    }
}
