//! Merging runs into fewer runs that hold only what can still be read.

use std::fmt;

use crate::block::{Record, Value};

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
/// segment and block buffers and a compression context for the builder.
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
    let mut total = CONTEXT_ALLOWANCE;
    let mut largest = 0;
    for block in largest_blocks {
        largest = largest.max(block);
        total = total.saturating_add(block.saturating_add(1 << 20));
    }
    // One encoded segment plus its builder storage; output blocks can grow
    // to the raw cap even when all input blocks are small. Oversized records
    // need corresponding buffers as well. Include bounded dictionary scratch.
    total
        .saturating_add(largest.max(SEGMENT_RAW_LEN).saturating_mul(2))
        .saturating_add(largest.max(super::RAW_BLOCK_CAP).saturating_mul(2))
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
    /// The total bytes they hold beside that, [`Run::held_bytes`]: key filters
    /// only.
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
pub fn compact_with<'a>(
    inputs: &[RunView<'a>],
    mut keep_tombstone: impl FnMut(&[u8]) -> bool,
    limits: CompactLimits,
    cuts: &OutputCuts,
    workspace: &'a mut MergeWorkspace,
) -> Result<Compacted, CompactError> {
    let needed = workspace_bytes(inputs)?;
    if needed > limits.max_workspace_bytes {
        return Err(CompactError::WorkspaceLimit {
            workspace_bytes: needed,
            max_workspace_bytes: limits.max_workspace_bytes,
        });
    }
    let mut merge = MergeCursor::new(inputs.iter().copied(), workspace);
    let mut output = Output::new(limits, limits.max_workspace_bytes - needed, cuts);
    let mut stats = CompactStats::default();
    let mut key = Vec::new();
    let mut has_key = false;

    while let Some(record) = merge.next_record()? {
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
            limits: CompactLimits {
                target_run_len,
                ..limits
            },
            cuts,
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
            let mut builder = RunBuilder::with_target_block_compressed(
                self.limits.codec,
                self.limits.target_block_compressed,
            );
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

    fn check(&self, output_bytes: usize) -> Result<(), CompactError> {
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
        self.bytes += run.as_bytes().len() + run.held_bytes();
        self.check(self.bytes)?;
        self.runs.push(run);
        Ok(())
    }
}
