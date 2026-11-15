//! Merging runs into fewer runs that hold only what can still be read.

use std::fmt;
use std::sync::atomic::{AtomicUsize, Ordering};

use crate::block::{Record, Value};
use crate::chunk::ChunkPool;

use super::codec::CDICT_ALLOWANCE;
use super::dictionary::SEGMENT_RAW_LEN;
use super::logical::conflict;
use super::{
    Codec, CorruptRun, MAX_TARGET_RUN_LEN, MergeCursor, MergeWorkspace, RECORD_INPUT_OVERHEAD,
    ReadError, Run, RunBuilder, RunView,
};

const BUILDABLE: &str = "merged records are ordered, within the limits, and fit in one run";

/// Bounds on what one compaction may produce.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CompactLimits {
    /// Input per output run, as for a flush, reduced to
    /// [`MAX_TARGET_RUN_LEN`] if larger.
    pub target_run_len: usize,
    /// The most bytes of output, stored bytes and key filters together. It is
    /// checked after every record against the finished runs and the stored
    /// bytes of the run being built, and again as each run is finished with
    /// its filter.
    pub max_output_bytes: usize,
    /// The most bytes the merge and builder may need beside the output. The
    /// estimate from [`workspace_bytes`] is checked before anything is
    /// decoded; what it leaves is the most the open builder may hold for key
    /// hashes, and an output run whose distinct keys need more is built
    /// without a filter.
    pub max_workspace_bytes: usize,
    /// How the output runs store their blocks.
    pub codec: Codec,
    /// Estimated compressed bytes per output block.
    pub target_block_compressed: usize,
    /// Erase sequences in settled output whose older views retain their own runs.
    pub zero_seq: bool,
}

/// Conservative allowance for a Zstandard compression or decompression context.
pub const CONTEXT_ALLOWANCE: usize = 2 << 20;

/// Upper estimate beside the output: a block and context per input, plus
/// segment and block buffers, a compression context and one prepared segment
/// dictionary for the builder.
/// Checks frame metadata and dictionaries without decoding. Previously retained
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
    // One encoded segment plus its builder storage; output blocks can grow
    // to the raw cap even when all input blocks are small. Oversized records
    // need corresponding buffers as well. Include bounded dictionary scratch.
    total
        // Segment builder (up to 2x growth), raw scratch (2x), encoded
        // segment scratch (2x), plus reusable raw block and block builder.
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
}

/// Why a compaction produced nothing. The engine reports it as the
/// matching [`CompactionError`](crate::engine::CompactionError) variant.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum CompactError {
    /// An input is damaged, or two inputs hold different records under one
    /// internal key, at any version.
    Read(ReadError),
    /// The output would exceed its byte limit, in the engine
    /// [`CompactionOptions::max_output_bytes`](crate::engine::CompactionOptions::max_output_bytes).
    OutputLimit {
        /// Bytes of output, stored and filters, when the limit was crossed.
        output_bytes: usize,
        /// The limit.
        max_output_bytes: usize,
    },
    /// The workspace estimate exceeds its limit, in the engine
    /// [`CompactionOptions::max_workspace_bytes`](crate::engine::CompactionOptions::max_workspace_bytes).
    /// Nothing was decoded.
    WorkspaceLimit {
        /// The estimate.
        workspace_bytes: usize,
        /// The limit.
        max_workspace_bytes: usize,
    },
}

impl From<CorruptRun> for CompactError {
    fn from(error: CorruptRun) -> Self {
        Self::Read(ReadError::Corrupt(error))
    }
}

impl fmt::Display for CompactError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Read(error) => error.fmt(f),
            Self::OutputLimit {
                output_bytes,
                max_output_bytes,
            } => write!(
                f,
                "compaction output reached {output_bytes} bytes, over the limit of {max_output_bytes}"
            ),
            Self::WorkspaceLimit {
                workspace_bytes,
                max_workspace_bytes,
            } => write!(
                f,
                "compaction needs {workspace_bytes} workspace bytes, over the limit of {max_workspace_bytes}"
            ),
        }
    }
}

impl std::error::Error for CompactError {}

/// Merges `inputs` into new runs with the limits' codec and block length, keeping
/// only the newest version of each user key.
///
/// - Older versions are dropped. This assumes no reader needs anything but the
///   latest state, which holds while there are no snapshots.
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
/// estimate leaves of the workspace limit. Both limits are checked as
/// described on [`CompactLimits`], and on any error the partial output is
/// dropped.
///
/// Output runs end before a record that would
/// take them past the target length, unless they hold no record yet.
#[cfg(test)]
pub fn compact<'a>(
    inputs: &[RunView<'a>],
    keep_tombstone: impl FnMut(&[u8]) -> bool,
    limits: CompactLimits,
    workspace: &'a mut MergeWorkspace,
) -> Result<Compacted, CompactError> {
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
) -> Result<Compacted, CompactError> {
    compact_with_in(
        inputs,
        keep_tombstone,
        limits,
        cuts,
        workspace,
        &ChunkPool::new(0),
    )
}

fn compact_with_in<'a>(
    inputs: &[RunView<'a>],
    mut keep_tombstone: impl FnMut(&[u8]) -> bool,
    limits: CompactLimits,
    cuts: &OutputCuts,
    workspace: &'a mut MergeWorkspace,
    pool: &ChunkPool,
) -> Result<Compacted, CompactError> {
    compact_range(
        inputs,
        &mut keep_tombstone,
        limits,
        cuts,
        workspace,
        BuildRange {
            pool,
            start: None,
            end: None,
            segment_len: SEGMENT_RAW_LEN,
            needed: workspace_bytes(inputs)?,
            total: None,
        },
    )
}

/// Builds disjoint key ranges of one selected job, under the same total limits.
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
) -> Result<Compacted, CompactError> {
    compact_parallel_in(
        inputs,
        keep_tombstone,
        limits,
        cuts,
        threads,
        estimated_run_bytes,
        (workspace, &ChunkPool::new(0)),
    )
}

pub(crate) fn compact_parallel_in<'a>(
    inputs: &[RunView<'a>],
    keep_tombstone: impl Fn(&[u8]) -> bool + Sync,
    limits: CompactLimits,
    cuts: &OutputCuts,
    threads: usize,
    estimated_run_bytes: usize,
    build: (&'a mut MergeWorkspace, &ChunkPool),
) -> Result<Compacted, CompactError> {
    let (workspace, pool) = build;
    let needed = workspace_bytes(inputs)?;
    if needed > limits.max_workspace_bytes {
        return Err(CompactError::WorkspaceLimit {
            workspace_bytes: needed,
            max_workspace_bytes: limits.max_workspace_bytes,
        });
    }
    let stored: usize = inputs.iter().map(|input| input.frame_bytes()).sum();
    // Use the planner's flush-derived stored/logical ratio: a builder must
    // have at least one full target-length run to build. Ignore the final tail.
    let estimated_run_bytes = estimated_run_bytes.max(1);
    let full_runs = stored / estimated_run_bytes;
    let threads = threads.min(full_runs).min(stored / (256 << 10)).max(1);
    if threads == 1 || cuts.single {
        return compact_with_in(inputs, keep_tombstone, limits, cuts, workspace, pool);
    }
    // Borrow sparse-index keys, weighted by compressed input bytes. Equal user
    // keys stay together even when their versions span several input blocks.
    let count: usize = inputs.iter().map(|input| input.block_count()).sum();
    let metadata = count.saturating_mul(size_of::<(&[u8], usize)>());
    if metadata > limits.max_workspace_bytes.saturating_sub(needed) {
        return compact_with_in(inputs, keep_tombstone, limits, cuts, workspace, pool);
    }
    let mut boundaries = Vec::with_capacity(count);
    for input in inputs {
        for block in 0..input.block_count() {
            boundaries.push((input.last_key(block)?, input.block_bytes(block)));
        }
    }
    boundaries.sort_unstable_by(|a, b| a.0.cmp(b.0));
    let largest = inputs
        .iter()
        .map(|input| input.largest_block_len())
        .collect::<Result<Vec<_>, _>>()?;
    let mut threads = threads.min(boundaries.len());
    // Keep selection's workspace limit. Divide the segment allowance rather
    // than multiplying a full-size builder by N; reduce N for high fan-in.
    let (segment_len, per_builder) = loop {
        let segment = SEGMENT_RAW_LEN.div_ceil(threads).max(super::RAW_BLOCK_CAP);
        let builder = workspace_for_segment(largest.iter().copied(), segment);
        let allowance = limits.max_workspace_bytes.saturating_sub(metadata) / threads;
        if builder <= allowance {
            break (segment, allowance);
        }
        threads -= 1;
        if threads == 1 {
            return compact_with_in(inputs, keep_tombstone, limits, cuts, workspace, pool);
        }
    };
    let starts = partition_starts(&boundaries, estimated_run_bytes, threads);
    if starts.is_empty() {
        return compact_with_in(inputs, keep_tombstone, limits, cuts, workspace, pool);
    }
    let builder_workspace = workspace_for_segment(largest.iter().copied(), segment_len);
    let total = AtomicUsize::new(0);
    let parts = std::thread::scope(|scope| {
        let handles: Vec<_> = (0..=starts.len())
            .map(|at| {
                let start = at.checked_sub(1).map(|i| starts[i]);
                let end = starts.get(at).copied();
                let keep = &keep_tombstone;
                let total = &total;
                scope.spawn(move || {
                    compact_range(
                        inputs,
                        keep,
                        CompactLimits {
                            max_workspace_bytes: per_builder,
                            ..limits
                        },
                        cuts,
                        &mut MergeWorkspace::new(),
                        BuildRange {
                            pool,
                            start,
                            end,
                            segment_len,
                            needed: builder_workspace,
                            total: Some(total),
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
    }
    Ok(out)
}

/// Approximate serial run boundaries with the same size estimate as planning.
/// Balance whole runs, not arbitrary byte fractions: the final partial run
/// belongs to the last builder. Repeated index keys always stay together.
pub(super) fn partition_starts<'a>(
    boundaries: &[(&'a [u8], usize)],
    target: usize,
    threads: usize,
) -> Vec<&'a [u8]> {
    let full_runs = boundaries.iter().map(|(_, bytes)| bytes).sum::<usize>() / target;
    let mut starts = Vec::with_capacity(threads - 1);
    let mut seen = 0;
    let mut at = 0;
    while at < boundaries.len() && starts.len() < threads - 1 {
        let key = boundaries[at].0;
        while at < boundaries.len() && boundaries[at].0 == key {
            seen += boundaries[at].1;
            at += 1;
        }
        let split_run = (full_runs as u128 * (starts.len() + 1) as u128 / threads as u128) as usize;
        if seen >= split_run * target && at < boundaries.len() {
            // Splitting before the first key would create an empty builder.
            if key != boundaries[0].0 {
                starts.push(key);
            }
        }
    }
    starts
}

struct BuildRange<'a> {
    pool: &'a ChunkPool,
    start: Option<&'a [u8]>,
    end: Option<&'a [u8]>,
    segment_len: usize,
    needed: usize,
    total: Option<&'a AtomicUsize>,
}

fn compact_range<'a>(
    inputs: &[RunView<'a>],
    mut keep_tombstone: impl FnMut(&[u8]) -> bool,
    limits: CompactLimits,
    cuts: &OutputCuts,
    workspace: &'a mut MergeWorkspace,
    range: BuildRange<'_>,
) -> Result<Compacted, CompactError> {
    let needed = range.needed;
    if needed > limits.max_workspace_bytes {
        return Err(CompactError::WorkspaceLimit {
            workspace_bytes: needed,
            max_workspace_bytes: limits.max_workspace_bytes,
        });
    }
    let mut merge = MergeCursor::new(inputs.iter().copied(), workspace);
    if let Some(start) = range.start {
        merge.seek(start)?;
    }
    let mut output = Output::new(limits, limits.max_workspace_bytes - needed, cuts);
    output.pool = range.pool.clone();
    output.segment_len = range.segment_len;
    output.total = range.total;
    let mut stats = CompactStats::default();
    let mut key = Vec::new();
    let mut has_key = false;

    while let Some(record) = merge.next_record()? {
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
        collapse_copies(&mut merge, &mut stats)?;
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
        output.add(Record {
            seq: if limits.zero_seq { 0 } else { record.seq },
            ..record
        })?;
        stats.records_written += 1;
    }
    output.finish_run()?;
    Ok(Compacted {
        runs: output.runs,
        output_bytes: output.bytes - output.held_bytes,
        held_bytes: output.held_bytes,
        workspace_bytes: needed,
        stats,
    })
}

/// Removes identical copies of the current record and rejects any that differ.
fn collapse_copies(
    merge: &mut MergeCursor<'_>,
    stats: &mut CompactStats,
) -> Result<(), CompactError> {
    while let Some(at) = merge.next_copy() {
        if let Some([(first, record), (other, copy)]) = merge.copies(at)
            && record.value != copy.value
        {
            return Err(CompactError::Read(conflict(
                record.key, record.seq, first, other,
            )));
        }
        merge.drop_copy(at)?;
        stats.records_read += 1;
        stats.copies_collapsed += 1;
    }
    Ok(())
}

struct Output<'c> {
    pool: ChunkPool,
    segment_len: usize,
    total: Option<&'c AtomicUsize>,
    charged: usize,
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
}

impl<'c> Output<'c> {
    fn new(limits: CompactLimits, hash_bytes: usize, cuts: &'c OutputCuts) -> Self {
        let target_run_len = if cuts.single {
            MAX_TARGET_RUN_LEN
        } else {
            limits.target_run_len.min(MAX_TARGET_RUN_LEN)
        };
        Self {
            pool: ChunkPool::new(0),
            limits: CompactLimits {
                target_run_len,
                ..limits
            },
            cuts,
            segment_len: SEGMENT_RAW_LEN,
            total: None,
            charged: 0,
            next_cut: 0,
            next_overlapped: 0,
            overlapped: 0,
            hash_bytes,
            builder: None,
            input: 0,
            runs: Vec::new(),
            bytes: 0,
            held_bytes: 0,
        }
    }

    /// Adds `record`, first ending the run being built if `record` would
    /// take it past the target length, past a cut or over too many runs
    /// below. A run with no record yet takes any record, however long, so
    /// that a record longer than the target is a run of its own and every
    /// run has a record.
    fn add(&mut self, record: Record<'_>) -> Result<(), CompactError> {
        let value_len = match record.value {
            Value::Bytes(bytes) => bytes.len(),
            Value::Tombstone => 0,
        };
        let len = record.key.len() + value_len + RECORD_INPUT_OVERHEAD;
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
        let builder = self.builder.get_or_insert_with(|| {
            let mut builder = RunBuilder::in_pool(
                self.limits.codec,
                self.limits.target_block_compressed,
                &self.pool,
            );
            builder.limit_segment_len(self.segment_len);
            builder.limit_filter_build_bytes(self.hash_bytes);
            builder
        });
        builder
            .add(record.key, record.seq, record.value)
            .expect(BUILDABLE);
        let stored = builder.stored_len();
        self.input += len;
        self.check(self.bytes + stored)?;
        if self.input >= self.limits.target_run_len {
            self.finish_run()?;
        }
        Ok(())
    }

    fn check(&mut self, output_bytes: usize) -> Result<(), CompactError> {
        let output_bytes = if let Some(total) = self.total {
            let delta = output_bytes.saturating_sub(self.charged);
            self.charged = self.charged.max(output_bytes);
            if delta == 0 {
                total.load(Ordering::Relaxed)
            } else {
                total
                    .fetch_add(delta, Ordering::Relaxed)
                    .saturating_add(delta)
            }
        } else {
            output_bytes
        };
        if output_bytes > self.limits.max_output_bytes {
            return Err(CompactError::OutputLimit {
                output_bytes,
                max_output_bytes: self.limits.max_output_bytes,
            });
        }
        Ok(())
    }

    fn finish_run(&mut self) -> Result<(), CompactError> {
        let Some(builder) = self.builder.take() else {
            return Ok(());
        };
        let run = builder.finish();
        self.input = 0;
        self.held_bytes += run.held_bytes();
        self.bytes += run.stored_len() + run.held_bytes();
        self.check(self.bytes)?;
        self.runs.push(run);
        Ok(())
    }
}
