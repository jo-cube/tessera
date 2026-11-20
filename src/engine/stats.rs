//! Engine and flush statistics.

use super::*;

/// What a flush published.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct FlushReport {
    /// Memtables flushed.
    pub tables: usize,
    /// Records written, including tombstones.
    pub records: usize,
    /// Runs published.
    pub runs: usize,
    /// Stored bytes published.
    pub run_bytes: usize,
    /// Key filters and physical segment metadata held beside stored bytes.
    /// Segment dictionaries are included in `run_bytes`.
    pub held_bytes: usize,
    /// High-water bytes one flush run's filter construction held for key hashes,
    /// freed when that run finished. Aggregated flush reports retain the
    /// engine's high-water mark; an empty report returns zero.
    pub filter_build_bytes: usize,
}

/// A summary of an engine's state.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct EngineStats {
    /// Cumulative encoding, copy-forward and admitted input.
    pub build: BuildCounters,
    /// Physical chunk capacity held by published runs.
    pub chunk_bytes_held: usize,
    /// Frame bytes referenced by published runs.
    pub block_bytes_live: usize,
    /// Bloom filter payload bytes of published runs.
    pub filter_bytes: usize,
    /// Block index bytes (entries, keys and rebuilt restart heads) of published runs.
    pub index_bytes: usize,
    /// The last sequence number used.
    pub last_seq: u64,
    /// Total memory charged to writer admission: the sum of `memtable_bytes`,
    /// `run_bytes`, `held_bytes`, `snapshot_bytes`, `pool_reserve_bytes`,
    /// `pool_unaccounted_bytes`, `build_output_bytes` and `build_workspace_bytes`.
    pub used: usize,
    /// Configured writer admission budget, or unlimited.
    pub memory_limit: Option<usize>,
    /// Charged active and frozen memtables, including their pinned history.
    pub memtable_bytes: usize,
    /// Retired sources held by snapshots/readers, excluding history already in memtables.
    pub snapshot_bytes: usize,
    /// Checked-out chunk capacity not already counted in sources or output estimates.
    pub pool_unaccounted_bytes: usize,
    /// Estimated in-flight output; exact bytes after a build finishes.
    pub build_output_bytes: usize,
    /// Estimated in-flight build workspace, including private ingestion staging.
    pub build_workspace_bytes: usize,
    /// The active memtable's accounting.
    pub active: MemoryUsage,
    /// The frozen memtables' accounting, summed, if any await flush.
    pub frozen: Option<MemoryUsage>,
    /// Frozen memtables awaiting flush.
    pub frozen_tables: usize,
    /// Published runs.
    pub runs: usize,
    /// Stored bytes of the published runs.
    pub run_bytes: usize,
    /// Key filters, restart heads and physical segment metadata beside
    /// `run_bytes`. Dictionaries are stored inside the runs.
    pub held_bytes: usize,
    /// Unused 1 MiB chunks available for reuse by this engine.
    pub pool_reserve_bytes: usize,
    /// Capacity of checked-out pool chunks, including padding and chunks held
    /// by builders, memtables or snapshots. Exact run tails and oversized
    /// allocations are excluded.
    pub pool_in_use_bytes: usize,
    /// Maximum free reserve: 32 MiB, independent of job size. The pool does
    /// not preallocate this amount. Reserve bytes are resident engine memory.
    pub pool_reserve_limit: usize,
    /// The most published runs whose key ranges share one key: the most
    /// blocks a point read of a key absent from the memtables decodes, and
    /// the number of groups in the run directory, each one input of a scan.
    pub max_overlap: usize,
    /// Live snapshots.
    pub snapshots: usize,
    /// Released memtables and runs still held by snapshots, readers or
    /// temporary references during publication cleanup.
    pub retained_sources: usize,
    /// Bytes of retained sources, including run filters and segment metadata,
    /// plus history pinned in the engine's memtables. Sources may be held by
    /// snapshots, readers or publication cleanup. Memtable history is also
    /// reported as [`MemoryUsage::pinned`] in `active` and `frozen`.
    pub retained_bytes: usize,
    /// The part of `retained_bytes` that is runs: stored bytes of runs that
    /// compaction replaced and snapshots, readers or publication cleanup still hold.
    pub retained_run_bytes: usize,
    /// The part of `retained_bytes` that is runs' filters and segment metadata.
    pub retained_held_bytes: usize,
    /// Flushes whose runs level 0 holds. Writers wait at 12; pending flushes
    /// can still publish while they wait.
    pub level0_flushes: usize,
    /// Runs in level 0.
    pub level0_runs: usize,
    /// Levels below level 0, including empty ones, which compaction adds
    /// as the data grows.
    pub levels: usize,
    /// Flushes published since the engine started, whether a write or the
    /// caller started them.
    pub flushes: u64,
    /// Stored bytes those flushes published.
    pub flushed_bytes: u64,
}
