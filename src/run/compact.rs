//! Latest-state compaction over immutable inputs. Conflicting copies fail
//! before publication, and tombstones survive while unselected runs may hold
//! older values. Disjoint output ranges share a bounded workspace allowance.
//! A merge into the bottom level copies settled input blocks that no other
//! input touches into the output without decoding them.

use super::OutputBudget;
use crate::block::{Record, Value};
use crate::chunk::ChunkPool;
use std::sync::Arc;

use super::codec::CDICT_ALLOWANCE;
use super::dictionary::SEGMENT_RAW_LEN;
use super::filter::{self as key_filter, KeyFilter};
use super::frame::Frame;
use super::merge::Step;
use super::{
    Codec, CorruptRun, Encoding, MAX_TARGET_RUN_LEN, MergeCursor, MergeWorkspace,
    RECORD_INPUT_OVERHEAD, ReadError, Run, RunBuilder, RunView,
};

const BUILDABLE: &str = "merged records are ordered, within the limits, and fit in one run";

/// Internal run sizing and workspace policy.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CompactLimits {
    /// Input per output run, as for a flush, reduced to
    /// [`MAX_TARGET_RUN_LEN`] if larger.
    pub target_run_len: usize,
    /// Workspace allowance, raised to the minimum serial estimate. Parallel
    /// builders and filter hashes share its remaining room. A filter needing
    /// more scratch is omitted; the merge still succeeds.
    pub max_workspace_bytes: usize,
    /// How the output runs encode their blocks.
    pub encoding: Encoding,
    /// Estimated compressed bytes per output block.
    pub target_block_compressed: usize,
    /// The output goes into the bottom level: write sequence 0 for every
    /// record that no unselected run can contest (see [`SeqFloor`]). Older
    /// views retain their own runs.
    pub zero_seq: bool,
    /// Copy settled blocks forward: with `zero_seq`, an output without a
    /// key filter and no cuts beside the target length, a block whose
    /// records all have sequence 0 and a value, and whose key range holds no
    /// other input's record, is copied into the output undecoded. An input
    /// segment where at least half the blocks are expected to be copied
    /// gives the output a segment that inherits its dictionary.
    pub copy_forward: bool,
}

/// An unselected run overlapping a job into the bottom level. A record of
/// key k and sequence s keeps s if some such run covers k, may hold k by its
/// filter, and has a record as old as s; otherwise any version of k outside
/// the inputs is newer than s and wins against 0 as it does against s.
#[derive(Clone, Copy, Debug)]
pub(crate) struct SeqFloor<'a> {
    pub(crate) min_key: &'a [u8],
    pub(crate) max_key: &'a [u8],
    /// No record of the run is older.
    pub(crate) min_seq: u64,
    /// Without a filter the run may hold every key in its range.
    pub(crate) filter: Option<&'a KeyFilter>,
}

impl<'a> SeqFloor<'a> {
    /// The floor of `run`, or `None` if it has no records.
    pub(crate) fn of(run: &'a Run<Vec<u8>>) -> Option<Self> {
        let range = run.key_range()?;
        Some(Self {
            min_key: range.min,
            max_key: range.max,
            min_seq: run.min_seq(),
            filter: run.filter(),
        })
    }
}

/// Decides each output record's sequence in key order, with a forward-only
/// cursor over floors sorted by `min_key`.
struct Eraser<'f> {
    enabled: bool,
    floors: &'f [SeqFloor<'f>],
    /// The first floor whose range has not yet been entered.
    next: usize,
    /// Indexes of the entered floors whose ranges may still cover the key.
    covering: Vec<usize>,
    #[cfg(feature = "diagnostics")]
    counts: crate::chunk::Erasure,
}

impl<'f> Eraser<'f> {
    fn new(enabled: bool, floors: &'f [SeqFloor<'f>]) -> Self {
        debug_assert!(floors.is_sorted_by_key(|floor| floor.min_key));
        Self {
            enabled,
            floors,
            next: 0,
            covering: Vec::with_capacity(if enabled { floors.len() } else { 0 }),
            #[cfg(feature = "diagnostics")]
            counts: crate::chunk::Erasure::default(),
        }
    }

    /// The sequence to write for the record (`key`, `seq`). Keys must come
    /// in ascending order.
    fn seq(&mut self, key: &[u8], seq: u64) -> u64 {
        if !self.enabled {
            return seq;
        }
        #[cfg(feature = "diagnostics")]
        {
            self.counts.records += 1;
        }
        let out = self.decide(key, seq);
        #[cfg(feature = "diagnostics")]
        if out == 0 {
            self.counts.erased += 1;
        }
        out
    }

    fn decide(&mut self, key: &[u8], seq: u64) -> u64 {
        if seq == 0 || self.floors.is_empty() {
            return 0;
        }
        let floors = self.floors;
        while let Some(floor) = floors.get(self.next)
            && floor.min_key <= key
        {
            self.covering.push(self.next);
            self.next += 1;
        }
        self.covering.retain(|&at| key <= floors[at].max_key);
        let contesting = || {
            self.covering
                .iter()
                .map(|&at| &floors[at])
                .filter(|floor| floor.min_seq <= seq)
        };
        // A run without a filter refuses without a probe.
        if contesting().any(|floor| floor.filter.is_none()) {
            #[cfg(feature = "diagnostics")]
            {
                self.counts.kept_unfiltered += 1;
            }
            return seq;
        }
        let mut hash = None;
        for floor in contesting() {
            let filter = floor.filter.expect("runs without a filter refused above");
            #[cfg(feature = "diagnostics")]
            {
                self.counts.probes += 1;
            }
            if filter.may_contain(*hash.get_or_insert_with(|| key_filter::hash(key))) {
                #[cfg(feature = "diagnostics")]
                {
                    self.counts.kept_filtered += 1;
                }
                return seq;
            }
        }
        0
    }

    fn finish(self, _pool: &ChunkPool) {
        #[cfg(feature = "diagnostics")]
        if self.enabled {
            _pool.erasure(self.counts);
        }
    }
}

/// Conservative allowance for a Zstandard compression or decompression context.
pub const CONTEXT_ALLOWANCE: usize = 2 << 20;

/// Upper estimate beside the output: a block and context per input, plus
/// segment and block buffers, a compression context and one prepared segment
/// dictionary for the builder.
/// Reads the largest block lengths recorded for built and validated runs, and
/// otherwise checks frame headers without decoding. Existing retained
/// workspace capacity is excluded.
pub fn workspace_bytes(inputs: &[RunView<'_>]) -> Result<usize, CorruptRun> {
    let blocks = inputs
        .iter()
        .map(|input| input.largest_block_len())
        .collect::<Result<Vec<_>, _>>()?;
    Ok(workspace_estimate(blocks))
}

/// [`workspace_bytes`] from each input's largest decoded block length, which
/// the engine keeps for every published run so that it can size a job
/// before reading any of it.
pub fn workspace_estimate(largest_blocks: impl IntoIterator<Item = usize>) -> usize {
    workspace_for_segment(largest_blocks, SEGMENT_RAW_LEN)
}

fn workspace_for_segment(
    largest_blocks: impl IntoIterator<Item = usize>,
    segment_len: usize,
) -> usize {
    let mut total = CONTEXT_ALLOWANCE + CDICT_ALLOWANCE;
    let mut largest = 0;
    for block in largest_blocks {
        largest = largest.max(block);
        total = total.saturating_add(block.saturating_add(1 << 20));
    }
    // One segment of raw blocks plus its builder storage; output blocks can
    // grow to the raw cap even when all input blocks are small. Oversized
    // records need corresponding buffers as well. Include bounded dictionary
    // scratch.
    total
        // Buffered raw blocks (up to 2x growth) and their block list (at most
        // 8 bytes per block of at least 12 bytes, with 2x growth), with
        // room to spare, plus the open block builder, probe and frame scratch.
        .saturating_add(largest.max(segment_len).saturating_mul(6))
        .saturating_add(largest.max(super::RAW_BLOCK_CAP).saturating_mul(4))
        .saturating_add(super::dictionary::DICTIONARY_LEN + super::dictionary::SAMPLE_LEN)
}

/// Where a compaction's output runs end, beside the target length.
///
/// Every cut falls between two user keys: an output holds one version per
/// key, so no key's versions are ever split across runs, and an empty run
/// is never written.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct OutputCuts {
    /// Keys, ascending, at which a new output run starts: the first record
    /// whose key is at least a cut starts a new run unless the run being
    /// built is empty.
    pub at: Vec<Vec<u8>>,
    /// The smallest keys of the runs one level below the output's level,
    /// ascending. A run being built ends before a record once its range has
    /// reached more than `max_overlapped` of them, so that its own next
    /// merge stays bounded.
    pub overlapped: Vec<Vec<u8>>,
    /// How many of `overlapped` one output run may reach; ignored if
    /// `overlapped` is empty.
    pub max_overlapped: usize,
    /// Write one run whatever its length: for merging two small runs into
    /// one, which must lower the run count to be worth doing.
    pub single: bool,
    /// The destination is the bottom level at selection time.
    pub omit_filter: bool,
}

/// What a compaction read and wrote.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct CompactStats {
    /// Physical records read, counting every copy.
    pub records_read: usize,
    /// Records written to the output.
    pub records_written: usize,
    /// Older versions of a key dropped because a newer one was read.
    pub versions_dropped: usize,
    /// Identical copies of an internal key removed.
    pub copies_collapsed: usize,
    /// Newest versions that were tombstones and were not kept.
    pub tombstones_dropped: usize,
}

/// The runs a compaction built.
#[derive(Debug)]
pub struct Compacted {
    /// Output runs, in key order, with disjoint key ranges.
    pub runs: Vec<Run<Vec<u8>>>,
    /// Their total stored length.
    pub output_bytes: usize,
    /// Their [`Run::held_bytes`]: key filters and physical segment metadata.
    pub held_bytes: usize,
    /// The workspace estimate checked before decoding.
    pub workspace_bytes: usize,
    /// What was read and written.
    pub stats: CompactStats,
    /// Blocks copied forward.
    pub(crate) copied: CopyStats,
}

/// What copying forward did in one merge.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct CopyStats {
    /// Input blocks copied, and their raw and stored frame bytes.
    pub(crate) blocks: u64,
    pub(crate) raw: u64,
    pub(crate) stored: u64,
    /// Input segments judged, and those given an inherited output segment.
    pub(crate) segments: u64,
    pub(crate) inherited: u64,
}

impl CopyStats {
    pub(crate) fn add(&mut self, other: Self) {
        self.blocks += other.blocks;
        self.raw += other.raw;
        self.stored += other.stored;
        self.segments += other.segments;
        self.inherited += other.inherited;
    }
}

/// Merges `inputs` into new runs with the limits' codec and block length, keeping
/// only the newest version of each user key.
///
/// - Older versions are dropped. This assumes no reader needs anything but the
///   latest state of these inputs; snapshots retain their own immutable sources.
/// - Identical copies of an internal key count once. Copies that differ fail
///   the compaction at any version, newest or not, because nothing can choose
///   between them.
/// - A newest version that is a tombstone is written only if
///   `keep_tombstone(key)` returns true. The caller must keep every tombstone
///   that may hide an older value outside `inputs`, or that value returns.
///
/// Output is split like a flush and holds disjoint, ascending key ranges.
/// Beside the output, the merge holds one decoded block and decompression
/// context per input in `workspace`, and one run builder is open; see
/// [`workspace_bytes`]. The builder's key hashes are held within what the
/// estimate leaves of the workspace allowance. Inputs always fit the minimum
/// serial workspace; tighter allowances reduce concurrency and filter scratch.
/// On a read error partial output is dropped.
///
/// Output runs end before a record that would
/// take them past the target length, unless they hold no record yet.
#[cfg(test)]
pub fn compact<'a>(
    inputs: &[RunView<'a>],
    keep_tombstone: impl FnMut(&[u8]) -> bool,
    limits: CompactLimits,
    workspace: &'a mut MergeWorkspace,
) -> Result<Compacted, ReadError> {
    compact_with(
        inputs,
        keep_tombstone,
        limits,
        &OutputCuts::default(),
        workspace,
    )
}

/// [`compact`], with output runs also cut as `cuts` asks.
#[cfg(test)]
pub(crate) fn compact_with<'a>(
    inputs: &[RunView<'a>],
    keep_tombstone: impl FnMut(&[u8]) -> bool,
    limits: CompactLimits,
    cuts: &OutputCuts,
    workspace: &'a mut MergeWorkspace,
) -> Result<Compacted, ReadError> {
    compact_with_in(
        inputs,
        keep_tombstone,
        limits,
        cuts,
        workspace,
        (&ChunkPool::new(0), &[]),
    )
}

/// [`compact_with`], erasing bottom-level sequences against `floors`.
#[cfg(test)]
pub(crate) fn compact_erasing<'a>(
    inputs: &[RunView<'a>],
    keep_tombstone: impl FnMut(&[u8]) -> bool,
    limits: CompactLimits,
    cuts: &OutputCuts,
    floors: &[SeqFloor<'_>],
    workspace: &'a mut MergeWorkspace,
) -> Result<Compacted, ReadError> {
    compact_with_in(
        inputs,
        keep_tombstone,
        limits,
        cuts,
        workspace,
        (&ChunkPool::new(0), floors),
    )
}

fn compact_with_in<'a>(
    inputs: &[RunView<'a>],
    mut keep_tombstone: impl FnMut(&[u8]) -> bool,
    limits: CompactLimits,
    cuts: &OutputCuts,
    workspace: &'a mut MergeWorkspace,
    (pool, floors): (&ChunkPool, &[SeqFloor<'_>]),
) -> Result<Compacted, ReadError> {
    compact_range(
        inputs,
        &mut keep_tombstone,
        limits,
        cuts,
        workspace,
        BuildRange {
            pool,
            floors,
            start: None,
            end: None,
            segment_len: SEGMENT_RAW_LEN,
            needed: workspace_bytes(inputs)?,
        },
    )
}

/// Builds disjoint key ranges of one selected job with a shared workspace allowance.
/// Single-output neighbour merges and small jobs retain the serial path.
#[cfg(test)]
pub fn compact_parallel<'a>(
    inputs: &[RunView<'a>],
    keep_tombstone: impl Fn(&[u8]) -> bool + Sync,
    limits: CompactLimits,
    cuts: &OutputCuts,
    threads: usize,
    estimated_run_bytes: usize,
    workspace: &'a mut MergeWorkspace,
) -> Result<Compacted, ReadError> {
    compact_parallel_in(
        inputs,
        keep_tombstone,
        limits,
        cuts,
        threads,
        estimated_run_bytes,
        (workspace, &ChunkPool::new(0), &[]),
    )
}

pub(crate) fn compact_parallel_in<'a>(
    inputs: &[RunView<'a>],
    keep_tombstone: impl Fn(&[u8]) -> bool + Sync,
    limits: CompactLimits,
    cuts: &OutputCuts,
    threads: usize,
    estimated_run_bytes: usize,
    build: (&'a mut MergeWorkspace, &ChunkPool, &[SeqFloor<'_>]),
) -> Result<Compacted, ReadError> {
    let (workspace, pool, floors) = build;
    let needed = workspace_bytes(inputs)?;
    let limits = CompactLimits {
        max_workspace_bytes: limits.max_workspace_bytes.max(needed),
        ..limits
    };
    let stored: usize = inputs.iter().map(|input| input.frame_bytes()).sum();
    // Use the planner's flush-derived stored/logical ratio: a builder must
    // have at least one full target-length run to build. Ignore the final tail.
    let estimated_run_bytes = estimated_run_bytes.max(1);
    let full_runs = stored / estimated_run_bytes;
    let threads = threads.min(full_runs).min(stored / (256 << 10)).max(1);
    if let Some(compacted) = try_compact_parallel_in(
        inputs,
        &keep_tombstone,
        limits,
        needed,
        cuts,
        threads,
        estimated_run_bytes,
        (pool, floors),
    )? {
        return Ok(compacted);
    }
    compact_with_in(
        inputs,
        keep_tombstone,
        limits,
        cuts,
        workspace,
        (pool, floors),
    )
}

#[allow(clippy::too_many_arguments)]
fn try_compact_parallel_in<'a>(
    inputs: &[RunView<'a>],
    keep_tombstone: &(impl Fn(&[u8]) -> bool + Sync),
    limits: CompactLimits,
    needed: usize,
    cuts: &OutputCuts,
    threads: usize,
    estimated_run_bytes: usize,
    (pool, floors): (&ChunkPool, &[SeqFloor<'_>]),
) -> Result<Option<Compacted>, ReadError> {
    if threads == 1 || cuts.single {
        return Ok(None);
    }
    // Size the sparse-index key buffer before allocating partition scratch so
    // Vec growth cannot exceed max_workspace_bytes and any serial fallback
    // starts only after every partition buffer has been dropped.
    let count: usize = inputs.iter().map(|input| input.block_count()).sum();
    let max_metadata = limits.max_workspace_bytes.saturating_sub(needed);
    let fixed_metadata = count.saturating_mul(size_of::<(std::ops::Range<usize>, usize)>());
    if fixed_metadata > max_metadata {
        return Ok(None);
    }
    let mut key_bytes = 0usize;
    for input in inputs {
        let mut key = super::index::IndexKey::new();
        for block in 0..input.block_count() {
            input.append_index_key(block, &mut key)?;
            key_bytes = key_bytes.saturating_add(key.len());
            if fixed_metadata.saturating_add(key_bytes) > max_metadata {
                return Ok(None);
            }
        }
    }
    let mut threads = threads.min(count);
    if threads <= 1 {
        return Ok(None);
    }
    let largest = inputs
        .iter()
        .map(|input| input.largest_block_len())
        .collect::<Result<Vec<_>, _>>()?;
    // Keep selection's workspace limit. `boundaries` is dropped before workers
    // start, so only `boundary_bytes` and `starts` share the workspace limit
    // with the parallel builders.
    let (segment_len, builder_workspace) = loop {
        let segment = SEGMENT_RAW_LEN.div_ceil(threads).max(super::RAW_BLOCK_CAP);
        let builder = workspace_for_segment(largest.iter().copied(), segment);
        let starts_bytes = threads.saturating_sub(1) * size_of::<&[u8]>();
        let live = key_bytes.saturating_add(starts_bytes);
        let allowance = limits.max_workspace_bytes.saturating_sub(live) / threads;
        if fixed_metadata.saturating_add(live) <= max_metadata && builder <= allowance {
            break (segment, builder);
        }
        threads -= 1;
        if threads == 1 {
            return Ok(None);
        }
    };
    let mut boundary_bytes = Vec::new();
    boundary_bytes.reserve_exact(key_bytes);
    let mut boundaries = Vec::new();
    boundaries.reserve_exact(count);
    for input in inputs {
        let mut key = super::index::IndexKey::new();
        for block in 0..input.block_count() {
            input.append_index_key(block, &mut key)?;
            let start = boundary_bytes.len();
            key.extend_into(&mut boundary_bytes);
            boundaries.push((start..boundary_bytes.len(), input.block_bytes(block)));
        }
    }
    let starts_bytes = threads.saturating_sub(1) * size_of::<&[u8]>();
    let peak_metadata = boundary_bytes
        .capacity()
        .saturating_add(boundaries.capacity() * size_of::<(std::ops::Range<usize>, usize)>())
        .saturating_add(starts_bytes);
    if peak_metadata > max_metadata {
        return Ok(None);
    }
    boundaries
        .sort_unstable_by(|a, b| boundary_bytes[a.0.clone()].cmp(&boundary_bytes[b.0.clone()]));
    let starts = partition_with(
        boundaries.len(),
        |at| &boundary_bytes[boundaries[at].0.clone()],
        |at| boundaries[at].1,
        estimated_run_bytes,
        threads,
    );
    drop(boundaries);
    if starts.is_empty() {
        return Ok(None);
    }
    let metadata = boundary_bytes
        .capacity()
        .saturating_add(starts.capacity() * size_of::<&[u8]>());
    let per_builder = limits.max_workspace_bytes.saturating_sub(metadata) / threads;
    if metadata > max_metadata || builder_workspace > per_builder {
        return Ok(None);
    }
    let parts = std::thread::scope(|scope| {
        let handles: Vec<_> = (0..=starts.len())
            .map(|at| {
                let start = at.checked_sub(1).map(|i| starts[i]);
                let end = starts.get(at).copied();
                #[cfg(feature = "diagnostics")]
                let bottom = super::copy_audit::bottom_inputs();
                scope.spawn(move || {
                    #[cfg(feature = "diagnostics")]
                    super::copy_audit::set_bottom_inputs(bottom);
                    compact_range(
                        inputs,
                        keep_tombstone,
                        CompactLimits {
                            max_workspace_bytes: per_builder,
                            ..limits
                        },
                        cuts,
                        &mut MergeWorkspace::new(),
                        BuildRange {
                            pool,
                            floors,
                            start,
                            end,
                            segment_len,
                            needed: builder_workspace,
                        },
                    )
                })
            })
            .collect();
        handles
            .into_iter()
            .map(|handle| {
                handle
                    .join()
                    .unwrap_or_else(|panic| std::panic::resume_unwind(panic))
            })
            .collect::<Result<Vec<_>, _>>()
    })?;
    let mut out = Compacted {
        runs: Vec::new(),
        output_bytes: 0,
        held_bytes: 0,
        workspace_bytes: metadata,
        stats: CompactStats::default(),
        copied: CopyStats::default(),
    };
    for part in parts {
        out.runs.extend(part.runs);
        out.output_bytes += part.output_bytes;
        out.held_bytes += part.held_bytes;
        out.workspace_bytes += part.workspace_bytes;
        out.stats.records_read += part.stats.records_read;
        out.stats.records_written += part.stats.records_written;
        out.stats.versions_dropped += part.stats.versions_dropped;
        out.stats.copies_collapsed += part.stats.copies_collapsed;
        out.stats.tombstones_dropped += part.stats.tombstones_dropped;
        out.copied.add(part.copied);
    }
    Ok(Some(out))
}

/// Approximate serial run boundaries with the same size estimate as planning.
/// Balance whole runs, not arbitrary byte fractions: the final partial run
/// belongs to the last builder. Repeated index keys always stay together.
#[cfg(test)]
pub(super) fn partition_starts<K: AsRef<[u8]>>(
    boundaries: &[(K, usize)],
    target: usize,
    threads: usize,
) -> Vec<&[u8]> {
    partition_with(
        boundaries.len(),
        |at| boundaries[at].0.as_ref(),
        |at| boundaries[at].1,
        target,
        threads,
    )
}

fn partition_with<'a>(
    len: usize,
    key_at: impl Fn(usize) -> &'a [u8],
    bytes_at: impl Fn(usize) -> usize,
    target: usize,
    threads: usize,
) -> Vec<&'a [u8]> {
    let full_runs = (0..len).map(&bytes_at).sum::<usize>() / target;
    let mut starts = Vec::new();
    starts.reserve_exact(threads - 1);
    let mut seen = 0;
    let mut at = 0;
    while at < len && starts.len() < threads - 1 {
        let key = key_at(at);
        while at < len && key_at(at) == key {
            seen += bytes_at(at);
            at += 1;
        }
        let split_run = (full_runs as u128 * (starts.len() + 1) as u128 / threads as u128) as usize;
        if seen >= split_run * target && at < len {
            // Splitting before the first key would create an empty builder.
            if key != key_at(0) {
                starts.push(key);
            }
        }
    }
    starts
}

/// A read failure or a bounded builder exhausting its output reservation.
#[derive(Debug)]
pub(crate) enum CompactBuildError {
    Read(ReadError),
    OutputLimit { bytes: usize, limit: usize },
}
impl From<ReadError> for CompactBuildError {
    fn from(error: ReadError) -> Self {
        Self::Read(error)
    }
}
impl From<super::CorruptRun> for CompactBuildError {
    fn from(error: super::CorruptRun) -> Self {
        Self::Read(error.into())
    }
}

pub(crate) fn compact_limited_in<'a>(
    inputs: &[RunView<'a>],
    keep_tombstone: impl FnMut(&[u8]) -> bool,
    limits: CompactLimits,
    cuts: &OutputCuts,
    (workspace, pool, floors): (&'a mut MergeWorkspace, &ChunkPool, &[SeqFloor<'_>]),
    budget: Arc<OutputBudget>,
) -> Result<Compacted, CompactBuildError> {
    compact_range_checked(
        inputs,
        keep_tombstone,
        limits,
        cuts,
        workspace,
        BuildRange {
            pool,
            floors,
            start: None,
            end: None,
            segment_len: SEGMENT_RAW_LEN,
            needed: workspace_bytes(inputs)?,
        },
        Some(budget),
    )
}

struct BuildRange<'a> {
    pool: &'a ChunkPool,
    /// Sorted by `min_key`; only read when `zero_seq` is set.
    floors: &'a [SeqFloor<'a>],
    start: Option<&'a [u8]>,
    end: Option<&'a [u8]>,
    segment_len: usize,
    needed: usize,
}

fn compact_range<'a>(
    inputs: &[RunView<'a>],
    keep_tombstone: impl FnMut(&[u8]) -> bool,
    limits: CompactLimits,
    cuts: &OutputCuts,
    workspace: &'a mut MergeWorkspace,
    range: BuildRange<'_>,
) -> Result<Compacted, ReadError> {
    compact_range_checked(inputs, keep_tombstone, limits, cuts, workspace, range, None).map_err(
        |error| match error {
            CompactBuildError::Read(error) => error,
            CompactBuildError::OutputLimit { .. } => unreachable!("unlimited output"),
        },
    )
}

fn compact_range_checked<'a>(
    inputs: &[RunView<'a>],
    mut keep_tombstone: impl FnMut(&[u8]) -> bool,
    limits: CompactLimits,
    cuts: &OutputCuts,
    workspace: &'a mut MergeWorkspace,
    range: BuildRange<'_>,
    budget: Option<Arc<OutputBudget>>,
) -> Result<Compacted, CompactBuildError> {
    let limits = CompactLimits {
        max_workspace_bytes: limits.max_workspace_bytes.max(range.needed),
        ..limits
    };
    let mut merge = MergeCursor::new(inputs.iter().copied(), workspace);
    if let Some(start) = range.start {
        merge.seek(start)?;
    }
    let mut output = Output::new(&range, limits, cuts, budget.clone());
    let refused = || {
        let budget = budget.as_ref().expect("only bounded output refuses");
        CompactBuildError::OutputLimit {
            bytes: budget.refused_bytes(),
            limit: budget.limit(),
        }
    };
    let mut stats = CompactStats::default();
    let mut key = Vec::new();
    let mut has_key = false;
    let mut eraser = Eraser::new(limits.zero_seq, range.floors);
    #[cfg(feature = "diagnostics")]
    let mut audit = super::copy_audit::CopyAudit::new(inputs, range.start);
    let copy = limits.copy_forward
        && limits.zero_seq
        && limits.encoding.codec == Codec::Zstd
        && cuts.omit_filter
        && !cuts.single
        && cuts.at.is_empty()
        && cuts.overlapped.is_empty();
    let copies: Vec<bool> = inputs
        .iter()
        .map(|input| copy && input.copies_forward())
        .collect();
    let mut forward = Forward {
        end: range.end,
        judged: None,
        stats: CopyStats::default(),
        last_key: Vec::new(),
    };
    if copies.contains(&true) {
        merge.copy_blocks(copies);
    }

    loop {
        match merge.next_step()? {
            None => break,
            Some(Step::Record) => {}
            Some(Step::Block {
                input,
                block,
                clear,
            }) => {
                let records = forward
                    .offer(inputs, input, block, clear, &mut output)
                    .map_err(|halt| match halt {
                        Halt::Refused => refused(),
                        Halt::Corrupt(error) => error.into(),
                    })?;
                if records > 0 {
                    merge.take_block();
                    stats.records_read += records;
                    stats.records_written += records;
                } else {
                    merge.keep_block()?;
                }
                continue;
            }
        }
        let record = merge.current().expect("the merge stands on a record");
        if range.end.is_some_and(|end| record.key >= end) {
            break;
        }
        stats.records_read += 1;
        let newest = !has_key || record.key != key.as_slice();
        if newest {
            key.clear();
            key.extend_from_slice(record.key);
            has_key = true;
        }
        let copies = merge.collapse_copies()?;
        stats.records_read += copies;
        stats.copies_collapsed += copies;
        #[cfg(feature = "diagnostics")]
        if let Some(audit) = audit.as_mut()
            && let (Some(input), Some(record)) = (merge.current_input(), merge.current())
        {
            audit.record(
                input,
                record.key,
                record.seq,
                record.value == Value::Tombstone,
            );
        }
        if !newest {
            stats.versions_dropped += 1;
            continue;
        }
        let record = merge
            .current()
            .expect("the merge stands on the record just read");
        if record.value == Value::Tombstone && !keep_tombstone(record.key) {
            stats.tombstones_dropped += 1;
            continue;
        }
        output
            .add(Record {
                seq: eraser.seq(record.key, record.seq),
                ..record
            })
            .map_err(|()| refused())?;
        stats.records_written += 1;
    }
    output.finish_run().map_err(|()| refused())?;
    eraser.finish(range.pool);
    #[cfg(feature = "diagnostics")]
    range.pool.copied(forward.stats);
    #[cfg(feature = "diagnostics")]
    if let Some(audit) = audit {
        audit.finish(range.end);
    }
    Ok(Compacted {
        runs: output.runs,
        output_bytes: output.bytes - output.held_bytes,
        held_bytes: output.held_bytes,
        workspace_bytes: range.needed,
        stats,
        copied: forward.stats,
    })
}

/// Why offering a block to the output stopped the merge.
enum Halt {
    Refused,
    Corrupt(CorruptRun),
}

impl From<CorruptRun> for Halt {
    fn from(error: CorruptRun) -> Self {
        Self::Corrupt(error)
    }
}

/// Decides which input blocks a merge copies forward, segment by segment.
struct Forward<'e> {
    end: Option<&'e [u8]>,
    /// The input segment judged last: its input, dictionary reference and
    /// the block after its last.
    judged: Option<(usize, u32, usize)>,
    stats: CopyStats,
    last_key: Vec<u8>,
}

impl Forward<'_> {
    /// Offers block `block` of input `input`, which the merge reports before
    /// decoding it, to the output. Returns its record count if it was
    /// copied, or 0 if it must be decoded.
    ///
    /// Reaching a new input segment ends any inherited output segment and
    /// judges the new one: if at least half of its remaining blocks are
    /// expected to be copied, the output starts a segment with its
    /// dictionary. The output copies the block if its input segment is the
    /// inherited one, the block is settled and `clear`, and it lies below
    /// the builder's range.
    fn offer(
        &mut self,
        inputs: &[RunView<'_>],
        input: usize,
        block: usize,
        clear: bool,
        output: &mut Output<'_>,
    ) -> Result<usize, Halt> {
        let view = inputs[input];
        let frame = view.frame(block)?;
        if frame.codec != Codec::Zstd {
            return Ok(0);
        }
        let reference = frame.dictionary_ref;
        let judged = self
            .judged
            .is_some_and(|(i, r, end)| i == input && r == reference && block < end);
        if !judged {
            output.end_inherited().map_err(|()| Halt::Refused)?;
            let segment = InputSegment::from(view, block, reference)?;
            self.judged = Some((input, reference, segment.end));
            self.stats.segments += 1;
            if segment.worth_inheriting(inputs, input, block)? {
                let dictionary = view.inherited_dictionary(reference)?;
                let last = view.last_key(segment.end - 1)?.to_vec();
                output
                    .begin_inherited(dictionary, segment.raw, segment.stored, last)
                    .map_err(|()| Halt::Refused)?;
                self.stats.inherited += u64::from(output.inheriting());
            }
        }
        let records = view.settled_records(block);
        if !clear || records == 0 || !output.inheriting() {
            return Ok(0);
        }
        let last = view.last_key(block)?;
        if self.end.is_some_and(|end| last.cmp(end).is_ge()) {
            return Ok(0);
        }
        let first = (block == 0).then(|| view.min_key());
        last.copy_into(&mut self.last_key);
        if !output.copy(&frame, &self.last_key, records, first)? {
            return Ok(0);
        }
        self.stats.blocks += 1;
        self.stats.raw += frame.decoded_len as u64;
        self.stats.stored += view.block_bytes(block) as u64;
        Ok(records)
    }
}

/// The blocks of one input segment from the block a merge reached.
struct InputSegment {
    /// The block after the segment's last.
    end: usize,
    /// Their decoded and stored payload bytes, and the settled ones.
    raw: usize,
    stored: usize,
    settled: usize,
}

impl InputSegment {
    /// Reads the frame headers from `block` while they reference `reference`.
    fn from(view: RunView<'_>, block: usize, reference: u32) -> Result<Self, CorruptRun> {
        let mut segment = Self {
            end: block,
            raw: 0,
            stored: 0,
            settled: 0,
        };
        while segment.end < view.block_count() {
            let frame = view.frame(segment.end)?;
            if frame.codec != Codec::Zstd || frame.dictionary_ref != reference {
                break;
            }
            segment.raw += frame.decoded_len;
            segment.stored += frame.stored.len();
            segment.settled += usize::from(view.settled_records(segment.end) > 0);
            segment.end += 1;
        }
        Ok(segment)
    }

    /// Whether at least half of the blocks from `block` are expected to be
    /// copied: the settled ones, less one per record the other inputs are
    /// estimated to hold within the segment's key range, from their indexes.
    fn worth_inheriting(
        &self,
        inputs: &[RunView<'_>],
        input: usize,
        block: usize,
    ) -> Result<bool, CorruptRun> {
        let blocks = self.end - block;
        if 2 * self.settled < blocks {
            return Ok(false);
        }
        let view = inputs[input];
        let low = block.checked_sub(1).map(|b| view.last_key(b)).transpose()?;
        let low = low.map(|key| key.to_vec());
        let high = view.last_key(self.end - 1)?.to_vec();
        let mut touched = 0usize;
        for (at, other) in inputs.iter().enumerate() {
            if at == input || other.block_count() == 0 {
                continue;
            }
            let Some(range) = other.key_range()? else {
                continue;
            };
            if range.min > high.as_slice() || low.as_deref().is_some_and(|low| range.max <= low) {
                continue;
            }
            let first = other
                .locate(low.as_deref().unwrap_or(range.min))?
                .unwrap_or(other.block_count() - 1);
            let last = other.locate(&high)?.unwrap_or(other.block_count() - 1);
            let per_block = other.len().div_ceil(other.block_count());
            touched = touched.saturating_add((last.saturating_sub(first) + 1) * per_block);
        }
        Ok(2 * self.settled.saturating_sub(touched) >= blocks)
    }
}

struct Output<'c> {
    pool: ChunkPool,
    segment_len: usize,
    limits: CompactLimits,
    cuts: &'c OutputCuts,
    /// The next of `cuts.at` not yet passed.
    next_cut: usize,
    /// The next of `cuts.overlapped` not yet passed, and how many of them
    /// the run being built has reached past its first record.
    next_overlapped: usize,
    overlapped: usize,
    /// The most bytes the open builder may hold for key hashes.
    hash_bytes: usize,
    builder: Option<RunBuilder>,
    input: usize,
    runs: Vec<Run<Vec<u8>>>,
    /// Stored and filter bytes of the finished runs.
    bytes: usize,
    held_bytes: usize,
    budget: Option<Arc<OutputBudget>>,
    /// While the open builder's segment inherits an input segment's
    /// dictionary, that segment's last key. Runs then end only once a
    /// record passes it.
    inherited_last: Option<Vec<u8>>,
}

impl<'c> Output<'c> {
    fn new(
        range: &BuildRange<'_>,
        limits: CompactLimits,
        cuts: &'c OutputCuts,
        budget: Option<Arc<OutputBudget>>,
    ) -> Self {
        let target_run_len = if cuts.single {
            MAX_TARGET_RUN_LEN
        } else {
            limits.target_run_len.min(MAX_TARGET_RUN_LEN)
        };
        Self {
            pool: range.pool.clone(),
            limits: CompactLimits {
                target_run_len,
                ..limits
            },
            cuts,
            segment_len: range.segment_len,
            next_cut: 0,
            next_overlapped: 0,
            overlapped: 0,
            hash_bytes: limits.max_workspace_bytes - range.needed,
            builder: None,
            input: 0,
            runs: Vec::new(),
            bytes: 0,
            held_bytes: 0,
            budget,
            inherited_last: None,
        }
    }

    /// Adds `record`, first ending the run being built if `record` would
    /// take it past the target length, past a cut or over too many runs
    /// below. A run with no record yet takes any record, however long, so
    /// that a record longer than the target is a run of its own and every
    /// run has a record.
    fn add(&mut self, record: Record<'_>) -> Result<(), ()> {
        if self
            .inherited_last
            .as_ref()
            .is_some_and(|last| record.key > last.as_slice())
        {
            self.end_inherited()?;
        }
        let inherited = self.inherited_last.is_some();
        let len = super::input_len(record.key, record.value);
        let mut cut = false;
        while let Some(at) = self.cuts.at.get(self.next_cut)
            && at.as_slice() <= record.key
        {
            self.next_cut += 1;
            cut = true;
        }
        while let Some(start) = self.cuts.overlapped.get(self.next_overlapped)
            && start.as_slice() <= record.key
        {
            self.next_overlapped += 1;
            self.overlapped += 1;
        }
        let over = !self.cuts.single
            && !inherited
            && ((!self.cuts.overlapped.is_empty() && self.overlapped > self.cuts.max_overlapped)
                || self.input + len > self.limits.target_run_len);
        if self.builder.is_some() && !self.cuts.single && (cut || over) {
            self.finish_run()?;
        }
        if self.builder.is_none() {
            // The run starts here: only the runs below it from this key on
            // count against it.
            self.overlapped = 0;
        }
        let builder = self.builder();
        builder
            .add(record.key, record.seq, record.value)
            .expect(BUILDABLE);
        if builder.output_refused() {
            return Err(());
        }
        // The builder ends an inherited segment that reaches its limit.
        if inherited && !builder.inheriting() {
            self.inherited_last = None;
        }
        self.input += len;
        if self.inherited_last.is_none() && self.input >= self.limits.target_run_len {
            self.finish_run()?;
        }
        Ok(())
    }

    /// The open builder, created for a new run if there is none.
    fn builder(&mut self) -> &mut RunBuilder {
        self.builder.get_or_insert_with(|| {
            let mut builder = RunBuilder::in_pool(
                self.limits.encoding,
                self.limits.target_block_compressed,
                &self.pool,
            );
            builder.limit_segment_len(self.segment_len);
            if let Some(budget) = &self.budget {
                builder.limit_output(budget.clone());
            }
            builder.limit_filter_build_bytes(if self.cuts.omit_filter {
                0
            } else {
                self.hash_bytes
            });
            builder
        })
    }

    fn inheriting(&self) -> bool {
        self.inherited_last.is_some()
    }

    /// Ends the open segment and starts one inheriting `dictionary`, for an
    /// input segment of `raw` decoded and `stored` payload bytes whose last
    /// key is `last`.
    fn begin_inherited(
        &mut self,
        dictionary: &[u8],
        raw: usize,
        stored: usize,
        last: Vec<u8>,
    ) -> Result<(), ()> {
        let builder = self.builder();
        builder.begin_inherited(dictionary, raw, stored);
        if builder.output_refused() {
            return Err(());
        }
        if builder.inheriting() {
            self.inherited_last = Some(last);
        }
        Ok(())
    }

    /// Ends an inherited segment, then the run if it has reached its target.
    fn end_inherited(&mut self) -> Result<(), ()> {
        if self.inherited_last.take().is_none() {
            return Ok(());
        }
        if let Some(builder) = &mut self.builder {
            builder.end_inherited();
            if builder.output_refused() {
                return Err(());
            }
        }
        if !self.cuts.single && self.input >= self.limits.target_run_len {
            self.finish_run()?;
        }
        Ok(())
    }

    /// Copies a settled input frame into the inherited segment. Returns
    /// whether it was copied.
    fn copy(
        &mut self,
        frame: &Frame<'_>,
        last: &[u8],
        records: usize,
        first: Option<&[u8]>,
    ) -> Result<bool, Halt> {
        let Some(builder) = self.builder.as_mut() else {
            return Ok(false);
        };
        let copied = builder.copy_frame(frame, last, records, first)?;
        if builder.output_refused() {
            return Err(Halt::Refused);
        }
        if !builder.inheriting() {
            self.inherited_last = None;
        }
        if copied {
            self.input = self
                .input
                .saturating_add(frame.decoded_len + records * RECORD_INPUT_OVERHEAD);
        }
        Ok(copied)
    }

    fn finish_run(&mut self) -> Result<(), ()> {
        self.inherited_last = None;
        // A builder opened for an inherited segment may have received no
        // record, if the merge dropped every one it read.
        let Some(builder) = self.builder.take().filter(RunBuilder::has_records) else {
            return Ok(());
        };
        let run = builder.finish_checked()?;
        self.input = 0;
        self.held_bytes += run.held_bytes();
        self.bytes += run.stored_len() + run.held_bytes();
        self.runs.push(run);
        Ok(())
    }
}
