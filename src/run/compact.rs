//! Merging runs into fewer runs that hold only what can still be read.

use std::fmt;

use crate::block::{Record, Value};

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
    /// Bytes per output block before compression, as for
    /// [`RunBuilder::with_target_block_len`].
    pub target_block_len: usize,
}

/// Bytes allowed for one Zstandard context, compressing 16 KiB blocks at level
/// 3 or decompressing them. Measured contexts are well below this; a test
/// checks it.
pub const CONTEXT_ALLOWANCE: usize = 1 << 20;

/// The largest block length the engine builds. A Zstandard compression
/// context grows with the block it compresses: at level 3 it measured
/// 0.28 MB for 16 KiB blocks, 0.66 MB for 64 KiB and 1.3 MB from 256 KiB,
/// so beyond this a context would outgrow [`CONTEXT_ALLOWANCE`] and the
/// workspace estimate would understate it. A test checks it.
pub const MAX_ENGINE_BLOCK_LEN: usize = 64 << 10;

/// Returns an upper estimate of what compacting `inputs` needs beside its
/// output: per input, a decoded copy of its largest block and a decompression
/// context; for the builder, two copies of the largest block and a compression
/// context. Reads every frame header and decodes nothing.
///
/// The builder's open block holds records of the inputs' blocks, so it is no
/// larger than the largest of them. Retained capacity from earlier use of a
/// workspace is not counted.
pub fn workspace_bytes(inputs: &[RunView<'_>]) -> Result<usize, CorruptRun> {
    let mut total = CONTEXT_ALLOWANCE;
    let mut largest = 0;
    for input in inputs {
        let block = input.largest_block_len()?;
        largest = largest.max(block);
        total = total.saturating_add(block + CONTEXT_ALLOWANCE);
    }
    Ok(total.saturating_add(2 * largest))
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
    /// The total bytes of their key filters.
    pub filter_bytes: usize,
    /// The estimate from [`workspace_bytes`] that was checked.
    pub workspace_bytes: usize,
    /// What was read and written.
    pub stats: CompactStats,
}

/// Why a compaction produced nothing.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CompactError {
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
pub fn compact<'a>(
    inputs: &[RunView<'a>],
    mut keep_tombstone: impl FnMut(&[u8]) -> bool,
    limits: CompactLimits,
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
    let mut output = Output::new(limits, limits.max_workspace_bytes - needed);
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
        output.add(record)?;
        stats.records_written += 1;
    }
    output.finish_run()?;
    Ok(Compacted {
        runs: output.runs,
        output_bytes: output.bytes - output.filter_bytes,
        filter_bytes: output.filter_bytes,
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

struct Output {
    limits: CompactLimits,
    /// The most bytes the open builder may hold for key hashes.
    hash_bytes: usize,
    builder: Option<RunBuilder>,
    input: usize,
    runs: Vec<Run<Vec<u8>>>,
    /// Stored and filter bytes of the finished runs.
    bytes: usize,
    filter_bytes: usize,
}

impl Output {
    fn new(limits: CompactLimits, hash_bytes: usize) -> Self {
        Self {
            limits: CompactLimits {
                target_run_len: limits.target_run_len.min(MAX_TARGET_RUN_LEN),
                ..limits
            },
            hash_bytes,
            builder: None,
            input: 0,
            runs: Vec::new(),
            bytes: 0,
            filter_bytes: 0,
        }
    }

    fn add(&mut self, record: Record<'_>) -> Result<(), CompactError> {
        let builder = self.builder.get_or_insert_with(|| {
            let mut builder =
                RunBuilder::with_target_block_len(self.limits.codec, self.limits.target_block_len);
            builder.limit_filter_build_bytes(self.hash_bytes);
            builder
        });
        builder
            .add(record.key, record.seq, record.value)
            .expect(BUILDABLE);
        let stored = builder.stored_len();
        let value_len = match record.value {
            Value::Bytes(bytes) => bytes.len(),
            Value::Tombstone => 0,
        };
        self.input += record.key.len() + value_len + RECORD_INPUT_OVERHEAD;
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
        self.filter_bytes += run.filter_bytes();
        self.bytes += run.as_bytes().len() + run.filter_bytes();
        self.check(self.bytes)?;
        self.runs.push(run);
        Ok(())
    }
}
