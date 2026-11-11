//! The write-ahead log of a durable engine: every acknowledged write since
//! the latest committed checkpoint, so that [`Engine::open`] recovers the
//! acknowledged history.
//!
//! # Modes
//!
//! An engine from [`Engine::new`] or [`Engine::recover`] is **RAM-only**: it
//! has no log and its writes touch no file. One from [`Engine::create`] or
//! [`Engine::open`] is **durable**: it holds its directory's `LOCK` for its
//! whole life, and every write is logged and synced before it is applied.
//!
//! # Durable writes
//!
//! Durable writes are committed in batches by group commit (see
//! `src/engine/group.rs`): concurrent writers queue, and one of them, the
//! leader, validates the batch and reserves its sequence numbers under the
//! state lock, appends it and syncs it (`fdatasync`) without that lock, then
//! publishes it under the lock and acknowledges each write. A lone writer
//! forms a batch of one at once. So a write becomes **visible** only after
//! its record is durable, it is **acknowledged** (its sequence number
//! returned) only after that, no record is logged for a write the engine
//! refused, and none logged is left unapplied. Reads proceed while a batch
//! syncs, and see the state before it.
//!
//! **Recovered history.** Every acknowledged write is recovered, under the
//! filesystem assumptions below. Recovery may also include writes after them
//! that were not acknowledged: a prefix of the final batch (its sync failed,
//! the process died before it returned, or its leader panicked), as far as
//! its complete records survived. It never includes a write the engine
//! refused, and never a later write without an earlier one.
//!
//! **Failures.** If the append, a rotation or the sync fails, every write of
//! the batch returns [`UpdateError::LogFailed`] and is not applied, but their
//! records may or may not survive (a failed `fsync` may also have dropped
//! earlier pages). The log is then **closed**: queued and later writes
//! return [`UpdateError::LogClosed`] and log nothing, so records of
//! uncertain fate are always the last ones. Reads, flushes, compaction and
//! checkpoints continue; reopening the directory resumes writing. A write that would make
//! the log exceed [`DurableOptions::max_log_bytes`] returns
//! [`UpdateError::LogFull`] and logs nothing, without closing the log; a
//! [`Engine::durable_checkpoint`] frees space. Applying a logged update cannot
//! fail; if it panicked, the state lock is poisoned and the engine unusable,
//! and the logged record is recovered on the next open.
//!
//! **Sequence numbers.** Every number a durable engine uses is logged, in
//! order, without gaps: the log is the sequence authority. Recovery resumes
//! after the last record recovered. A number is reused only if its record was
//! never acknowledged and did not survive.
//!
//! # Segments
//!
//! The log is a series of files `wal-<number>` (20 decimal digits, numbers
//! consecutive), each starting with a header: the magic `TSRWAL\0\x01`,
//! version (u32, 1), flags (u32, 0), its number (u64), its **base** (u64: the
//! sequence number before its first record, which is the last record of the
//! segment before it), and a CRC-32 of the header. Records follow:
//!
//! - frame: body length (u32), CRC-32 of the body (u32), CRC-32 of those
//!   eight bytes (u32);
//! - body: operation (u8: 1 put, 2 delete), sequence number (u64), key length
//!   (u32), key, and for a put the value length (u32) and value.
//!
//! All integers are little-endian. Lengths are bounded by the memtable's key
//! and value limits, which recovery checks before allocating. A new segment
//! is started when a record would take the active one past
//! [`DurableOptions::segment_bytes`] (a record larger than that gets a
//! segment of its own), and at every open: no process appends to a segment
//! another wrote. Creating one writes and syncs the file, then syncs the
//! directory, before any record goes in.
//!
//! # Recovery
//!
//! [`Engine::open`] loads the committed checkpoint (ceiling `S`), then reads
//! the segments from the newest one whose base is at most `S` (older ones
//! hold only records the checkpoint covers and are ignored until pruned).
//! From there the numbers must be consecutive, each base must equal the last
//! sequence number before it, and the records must be numbered `base + 1`,
//! `base + 2` and so on. Records above `S` are applied to the memtables in
//! order, flushing as needed; the log must reach `S`. The engine is exposed
//! only once every record has passed.
//!
//! **Torn tails.** Every batch is synced before the next is written, and a
//! segment before the next is created, so only records of the last batch,
//! at the end of the last segment, can be incomplete. In the last segment,
//! and nowhere else, recovery accepts after the last valid record: fewer
//! bytes than a frame; a frame whose own checksum holds but whose body runs
//! past the end of the file; or bytes that are all zero. None of these can
//! hold a complete record, so they are an interrupted append, which was
//! never acknowledged; they are truncated (and reported in
//! [`OpenReport::torn_tail`]) before writing resumes. A last segment whose
//! header is incomplete or zero is an interrupted rotation and is removed.
//! Anything else is [`RecoveryError::Log`]: a bad frame or body checksum, a
//! complete record with a bad checksum even at the very end, an invalid
//! operation or length, a gap, or a log behind the checkpoint. Acknowledged
//! history is never discarded as a torn tail, provided an interrupted write
//! leaves a prefix of its bytes, possibly followed by zeros.
//!
//! A batch of several records is one such write. If a crash left a later
//! page of it without an earlier one (non-zero bytes after a hole), the
//! first incomplete record fails a checksum like corrupted history would,
//! and nothing in the log tells the two apart: there is no record of where
//! the last batch began or what was acknowledged. Recovery therefore
//! refuses it rather than truncate. [`Engine::inspect`] reports where the
//! valid log ends and how many bytes follow; truncating them is a decision
//! only someone who knows what was acknowledged can make, and Tessera
//! offers no repair.
//!
//! Marking where each batch begins would not make that truncation safe. A
//! damaged final batch may have been synced and acknowledged before the
//! damage, and nothing durable records whether its sync completed: the sync
//! leaves no trace in the file. Recording it would take a second write and
//! sync per batch, before acknowledgment (or a marker carried by the next
//! batch, which the final batch never gets). Only then could a damaged,
//! unmarked final batch be known unacknowledged. The case arises only from
//! an operating system crash or power loss during a batch that spans pages
//! written out of order; it fails safe, so the format stays as it is.
//!
//! # Checkpoints and pruning
//!
//! [`Engine::durable_checkpoint`] commits a checkpoint into the engine's own
//! directory while writes continue; its ceiling `S` is the snapshot it took.
//! Only after `CURRENT` names it durably are segments removed: oldest first,
//! each one whose successor's base is at most `S`, so that every record
//! above `S` stays. The active segment is never removed. A failed or
//! interrupted checkpoint removes no segment, so the previous checkpoint and
//! the log still form the full history. A segment that cannot be removed
//! stops pruning and is retried by the next checkpoint.
//!
//! # Crash and sync boundaries
//!
//! A durable write ends in one of these classes:
//!
//! 1. **Acknowledged**: its sequence number was returned. Its record and
//!    every earlier one were synced before it became visible, so recovery
//!    returns it, under the filesystem assumptions.
//! 2. **Uncertain**: its caller got [`UpdateError::LogFailed`], or the
//!    process died before the call returned. The record may or may not
//!    survive; recovery settles it: if the complete record survives with
//!    every earlier one it is replayed and is history from then on,
//!    otherwise it is gone and its number is reused. Read the key back
//!    after reopening to learn which. It is never applied in part.
//! 3. **Incomplete**: the bytes of a torn tail or an interrupted segment, as
//!    above. They hold no complete record; open removes them.
//! 4. **Damaged**: anything else that fails a check. The history cannot be
//!    established safely, and open refuses the directory.
//!
//! Refused writes (`Rejected`, `LogFull`, `LogClosed`, `SequencesExhausted`)
//! are in no class: nothing was logged and no number used.
//!
//! The boundaries, in order:
//!
//! - **Segment creation**: the file is created, its header written, the file
//!   and then the directory synced, before any record goes in, and the
//!   previous segment is synced before it. A crash in between leaves a last
//!   segment without a complete header, which holds no record.
//! - **Append**: one vector write per segment a batch touches. A crash or
//!   error during it leaves the batch uncertain, with incomplete bytes after
//!   its last complete record.
//! - **Sync**: success of `fdatasync` makes the batch durable, by
//!   assumption. If it fails, the batch is uncertain and the log closes. On
//!   Linux a failed writeback may drop the pages while the page cache still
//!   shows them, and a later sync on another descriptor can succeed: a
//!   reopen in the same boot may then replay records that are not on the
//!   device. A later power loss would leave the next segment based past the
//!   end of the one before, which open reports as damage (such as
//!   [`LogDamage::Base`]), never silently; but the writes acknowledged after
//!   that reopen lie beyond the damage. After a failed sync, check the device
//!   before reopening.
//! - **Acknowledgment**: after the sync and the publication, never before.
//! - **Open**: only after every check passes, the torn tail is truncated and
//!   synced, an interrupted segment removed, the last segment synced (a
//!   process that died before its sync left records only in the page cache,
//!   which must be durable before anything is based on them), and a new
//!   segment created.
//! - **Checkpoint**: the runs, the manifest and their directories are
//!   synced, then `CURRENT.tmp`, which the rename makes `CURRENT`, then the
//!   parent directory (see `src/engine/checkpoint.rs`). A crash before the
//!   rename leaves the previous checkpoint and the full log; after it, the
//!   new checkpoint and the full log.
//! - **Pruning**: after the commit is durable, and only segments the new
//!   ceiling covers. Removals are not synced: a segment that reappears after
//!   a crash is covered and ignored.
//! - **Process termination**: the kernel releases `LOCK`. Written but
//!   unsynced bytes stay in the page cache, where the next open reads and
//!   syncs them; only an operating system crash or power loss can lose them.
//!
//! What the tests show is the behaviour at each boundary when a process is
//! killed or an operation fails. That `fdatasync` reaches stable media, and
//! that an interrupted write leaves a prefix of its bytes, are assumptions;
//! only power-loss testing on a given device and filesystem can support
//! them.
//!
//! # Inspection
//!
//! [`Engine::inspect`] reads `CURRENT`, the manifest and the whole log as
//! open would, under a shared lock and without creating or changing any
//! file, and reports the last valid record, the damage if any and what open
//! would do. It tells an interrupted [`Engine::create`] (nothing committed,
//! safe to remove) from a directory that has lost `CURRENT` (not safe), and
//! reports a truncation as safe only where open itself would make it.
//!
//! # Resources
//!
//! A write encodes its record into a buffer of its own, which its batch
//! holds until it is logged; group commit bounds the queued and batched
//! bytes. Recovery reads through a 64 KiB buffer and one record buffer, at
//! most one record of the largest size, and holds one segment file open at
//! a time; it allocates a record's buffer only after the frame's checksum
//! and length bound pass. The log on disk is bounded by
//! [`DurableOptions::max_log_bytes`]; segments are pruned only by
//! checkpoints, so the log can stay near that limit until one is taken, and
//! the previous checkpoint stays on disk beside the new one until the new one
//! commits. A checkpoint can always free every segment but the active one,
//! so keep `max_log_bytes` well above two segments and the largest record.
//!
//! The filesystem assumptions of checkpoints apply: see
//! `src/engine/checkpoint.rs`.

use std::fmt;
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufReader, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, PoisonError};

use crc32fast::Hasher;

use crate::block::{MAX_KEY_LEN, MAX_VALUE_LEN, Value};
use crate::memtable::WriteError;

use super::checkpoint::{
    self, CheckpointError, CheckpointOptions, CheckpointReport, RecoveryError,
};
use super::{Core, Engine, EngineOptions, UpdateError};

const PREFIX: &str = "wal-";
const SEGMENT_MAGIC: [u8; 8] = *b"TSRWAL\0\x01";
const SEGMENT_VERSION: u32 = 1;
/// Magic, version, flags, number, base and CRC.
const SEGMENT_HEADER_LEN: usize = 8 + 4 + 4 + 8 + 8 + 4;
const FRAME_LEN: usize = 12;
const PUT: u8 = 1;
const DELETE: u8 = 2;
/// Operation, sequence number and key length.
const MIN_BODY: usize = 1 + 8 + 4;
const MAX_BODY: usize = MIN_BODY + MAX_KEY_LEN + 4 + MAX_VALUE_LEN;
const READ_BUFFER: usize = 64 << 10;

/// The log of a durable engine.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DurableOptions {
    /// A new segment is started when a record would take the active one past
    /// this many bytes.
    pub segment_bytes: u64,
    /// The most bytes all segments may hold. A write beyond it is refused
    /// with [`UpdateError::LogFull`] until a checkpoint prunes segments.
    pub max_log_bytes: u64,
    /// The most writes one batch logs with one sync, at most 256. 1 syncs
    /// every write on its own.
    pub max_batch_records: usize,
    /// The most encoded bytes one batch logs, though a single larger write
    /// forms a batch alone.
    pub max_batch_bytes: usize,
    /// The most encoded bytes of writes waiting for a batch, each counted
    /// with a fixed overhead. Writers are admitted in arrival order; one that
    /// would exceed it waits, and a single larger write is admitted alone
    /// once the queue is empty.
    pub max_queue_bytes: usize,
}

impl Default for DurableOptions {
    /// 64 MiB segments, 1 GiB of log, batches of up to 64 writes and 1 MiB,
    /// and 16 MiB of writes queued.
    fn default() -> Self {
        Self {
            segment_bytes: 64 << 20,
            max_log_bytes: 1 << 30,
            max_batch_records: 64,
            max_batch_bytes: 1 << 20,
            max_queue_bytes: 16 << 20,
        }
    }
}

/// How a log segment failed its checks.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum LogDamage {
    /// The directory holds a checkpoint but no segment.
    NoSegments,
    /// A segment header is truncated, has the wrong magic or checksum, or
    /// names another segment.
    Header,
    /// A version or flags this build does not read.
    Unsupported {
        /// The version.
        version: u32,
        /// The flags.
        flags: u32,
    },
    /// The segment after `after` is missing.
    Missing {
        /// The last segment present before the gap.
        after: u64,
    },
    /// A segment's base is not the last sequence number before it.
    Base {
        /// The last sequence number before it.
        expected: u64,
        /// Its base.
        found: u64,
    },
    /// Every segment's base is above the checkpoint's ceiling, so records
    /// between them are missing.
    Gap {
        /// The oldest base.
        base: u64,
        /// The ceiling.
        ceiling: u64,
    },
    /// The log ends before the checkpoint's ceiling.
    Behind {
        /// Its last sequence number.
        last: u64,
        /// The ceiling.
        ceiling: u64,
    },
    /// A record frame's own checksum fails.
    Frame,
    /// A record's length is out of bounds or disagrees with its body.
    Length(u64),
    /// A record body's checksum fails.
    Checksum,
    /// A record is incomplete before the end of the last segment.
    Truncated,
    /// An unknown operation.
    Operation(u8),
    /// A record's sequence number is not the next one.
    Sequence {
        /// The next one.
        expected: u64,
        /// The record's.
        found: u64,
    },
}

/// What [`Engine::open`] recovered.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OpenReport {
    /// The checkpoint's generation.
    pub generation: u64,
    /// Its ceiling.
    pub checkpoint_seq: u64,
    /// The last sequence number recovered, after which writes continue.
    pub last_seq: u64,
    /// Segments read.
    pub segments: usize,
    /// Records above the ceiling, applied.
    pub replayed: u64,
    /// Records at or below the ceiling, already in the checkpoint.
    pub skipped: u64,
    /// Log bytes read.
    pub log_bytes: u64,
    /// An interrupted append removed from the end of the last segment.
    pub torn_tail: Option<TornTail>,
    /// A segment left by an interrupted rotation, removed.
    pub interrupted_segment: Option<PathBuf>,
}

/// Bytes after the last valid record of the last segment, truncated.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TornTail {
    /// The segment.
    pub path: PathBuf,
    /// Where the valid records end.
    pub offset: u64,
    /// Bytes removed.
    pub bytes: u64,
}

/// What [`Engine::inspect`] found in a directory, changing nothing.
#[derive(Debug)]
pub struct Inspection {
    /// The generation and ceiling `CURRENT` and its manifest name (the runs
    /// are not read), or `None` without `CURRENT`.
    pub checkpoint: Option<(u64, u64)>,
    /// Log segments present.
    pub segments: usize,
    /// The last record, in log order from where recovery starts, that passed
    /// every check.
    pub last_valid: Option<LogPosition>,
    /// What [`Engine::open`] would do.
    pub verdict: Verdict,
}

/// A record in the log: its segment, the offset where it ends, and its
/// sequence number.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LogPosition {
    /// The segment.
    pub path: PathBuf,
    /// Where the record ends.
    pub end: u64,
    /// Its sequence number.
    pub seq: u64,
}

/// What [`Engine::open`] would make of a directory.
#[derive(Debug)]
pub enum Verdict {
    /// The log passes every check, and open would recover it, provided the
    /// checkpoint's runs (not read here) pass theirs and fit the options.
    /// Open first removes a torn tail and an interrupted segment, if listed
    /// here: neither holds a complete record.
    Opens {
        /// The bytes after the last valid record of the last segment.
        torn_tail: Option<TornTail>,
        /// A last segment without a complete header.
        interrupted_segment: Option<PathBuf>,
    },
    /// There is no `CURRENT`, and nothing more than an interrupted
    /// [`Engine::create`] leaves: at most the first segment without a record
    /// and the first checkpoint without a run. Nothing was committed here;
    /// removing the directory loses no write.
    Uncommitted,
    /// There is no `CURRENT`, but the directory holds more than an
    /// interrupted creation: records or checkpoints whose committed state is
    /// unknown. Nothing here is safe to remove or recover automatically.
    NoCheckpoint,
    /// Open refuses it, at this damage. No truncation is known to be safe:
    /// a complete record that fails its checks may be acknowledged history,
    /// and the records after it cannot be verified. Truncating at `offset`
    /// would discard `bytes_after` bytes of log; only the final batch's
    /// records can have been unacknowledged, so whatever else it discards
    /// may be acknowledged writes.
    Damaged {
        /// The segment, or the directory for a missing one.
        path: PathBuf,
        /// Where in it.
        offset: u64,
        /// How.
        damage: LogDamage,
        /// Log bytes from `offset` to the end of the last segment, or 0 when
        /// `path` is not a segment.
        bytes_after: u64,
    },
}

/// The state of a durable engine's log.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct LogStatus {
    /// Segments on disk, the active one included.
    pub segments: usize,
    /// Their bytes.
    pub bytes: u64,
    /// Bytes of the active segment.
    pub active_bytes: u64,
    /// Why the log closed, if it did.
    pub closed: Option<io::ErrorKind>,
    /// Syncs of record data since the engine opened: one per batch, or two if
    /// it started a segment. Segment headers and directories are synced too,
    /// and not counted.
    pub syncs: u64,
    /// Batches logged since the engine opened.
    pub batches: u64,
    /// Records logged since the engine opened.
    pub records: u64,
    /// Batches by size: 1, 2, 3–4, 5–8, 9–16, 17–32, 33–64 and more records.
    pub batch_sizes: [u64; 8],
    /// Writes waiting in the queue for a batch, now.
    pub queued_writes: usize,
    /// Their cost against [`DurableOptions::max_queue_bytes`].
    pub queued_bytes: usize,
}

fn segment_name(number: u64) -> String {
    format!("{PREFIX}{number:020}")
}

fn crc(parts: &[&[u8]]) -> u32 {
    let mut hasher = Hasher::new();
    for part in parts {
        hasher.update(part);
    }
    hasher.finalize()
}

fn sync_dir(path: &Path) -> io::Result<()> {
    File::open(path)?.sync_all()
}

/// Numbers of the segments in `dir`, ascending.
fn segment_numbers(dir: &Path) -> io::Result<Vec<u64>> {
    let mut found = Vec::new();
    for entry in fs::read_dir(dir)? {
        let name = entry?.file_name();
        if let Some(digits) = name.to_str().and_then(|name| name.strip_prefix(PREFIX))
            && digits.len() == 20
            && digits.bytes().all(|b| b.is_ascii_digit())
            && let Ok(number) = digits.parse()
        {
            found.push(number);
        }
    }
    found.sort_unstable();
    Ok(found)
}

/// Whether `dir` holds a log segment.
pub(super) fn has_log(dir: &Path) -> io::Result<bool> {
    match segment_numbers(dir) {
        Ok(found) => Ok(!found.is_empty()),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(error),
    }
}

fn encode_header(number: u64, base: u64) -> [u8; SEGMENT_HEADER_LEN] {
    let mut bytes = [0; SEGMENT_HEADER_LEN];
    bytes[..8].copy_from_slice(&SEGMENT_MAGIC);
    bytes[8..12].copy_from_slice(&SEGMENT_VERSION.to_le_bytes());
    bytes[16..24].copy_from_slice(&number.to_le_bytes());
    bytes[24..32].copy_from_slice(&base.to_le_bytes());
    let sum = crc(&[&bytes[..32]]);
    bytes[32..].copy_from_slice(&sum.to_le_bytes());
    bytes
}

/// The base of a segment from its header.
fn decode_header(bytes: &[u8; SEGMENT_HEADER_LEN], number: u64) -> Result<u64, LogDamage> {
    let u32_at = |at: usize| u32::from_le_bytes(bytes[at..at + 4].try_into().unwrap());
    let u64_at = |at: usize| u64::from_le_bytes(bytes[at..at + 8].try_into().unwrap());
    if bytes[..8] != SEGMENT_MAGIC || crc(&[&bytes[..32]]) != u32_at(32) {
        return Err(LogDamage::Header);
    }
    let (version, flags) = (u32_at(8), u32_at(12));
    if version != SEGMENT_VERSION || flags != 0 {
        return Err(LogDamage::Unsupported { version, flags });
    }
    if u64_at(16) != number {
        return Err(LogDamage::Header);
    }
    Ok(u64_at(24))
}

/// One record encoded without its sequence number and checksums, which
/// [`seal`] adds once the number is known, and the CRC state of the body
/// after the sequence number, computed by the writer's own thread.
pub(super) struct Unsealed {
    pub(super) record: Vec<u8>,
    tail: Hasher,
    pub(super) key: std::ops::Range<usize>,
    /// The value's bytes, or `None` for a delete.
    pub(super) value: Option<std::ops::Range<usize>>,
}

/// Where a body's sequence number starts in a record.
const SEQ_AT: usize = FRAME_LEN + 1;

/// The length of the record [`encode`] makes.
pub(super) fn encoded_len(key: &[u8], value: Value<'_>) -> usize {
    let value_len = match value {
        Value::Bytes(bytes) => 4 + bytes.len(),
        Value::Tombstone => 0,
    };
    FRAME_LEN + MIN_BODY + key.len() + value_len
}

pub(super) fn encode(key: &[u8], value: Value<'_>) -> Unsealed {
    let mut record = Vec::with_capacity(encoded_len(key, value));
    record.extend_from_slice(&[0; FRAME_LEN]);
    record.push(match value {
        Value::Bytes(_) => PUT,
        Value::Tombstone => DELETE,
    });
    record.extend_from_slice(&[0; 8]);
    record.extend_from_slice(&(key.len() as u32).to_le_bytes());
    let key_at = record.len();
    record.extend_from_slice(key);
    let value_range = match value {
        Value::Bytes(bytes) => {
            record.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
            let at = record.len();
            record.extend_from_slice(bytes);
            Some(at..record.len())
        }
        Value::Tombstone => None,
    };
    let mut tail = Hasher::new();
    tail.update(&record[SEQ_AT + 8..]);
    Unsealed {
        key: key_at..key_at + key.len(),
        value: value_range,
        record,
        tail,
    }
}

impl Unsealed {
    pub(super) fn key(&self) -> &[u8] {
        &self.record[self.key.clone()]
    }

    pub(super) fn value(&self) -> Value<'_> {
        match &self.value {
            Some(range) => Value::Bytes(&self.record[range.clone()]),
            None => Value::Tombstone,
        }
    }

    /// Writes `seq` and the checksums.
    pub(super) fn seal(&mut self, seq: u64) {
        let record = &mut self.record;
        record[SEQ_AT..SEQ_AT + 8].copy_from_slice(&seq.to_le_bytes());
        let mut body = Hasher::new();
        body.update(&record[FRAME_LEN..SEQ_AT + 8]);
        body.combine(&self.tail);
        let body_len = (record.len() - FRAME_LEN) as u32;
        record[..4].copy_from_slice(&body_len.to_le_bytes());
        record[4..8].copy_from_slice(&body.finalize().to_le_bytes());
        let frame_sum = crc(&[&record[..8]]);
        record[8..12].copy_from_slice(&frame_sum.to_le_bytes());
    }
}

/// Encodes one record, frame and body, into `out`.
#[cfg(test)]
fn encode_record(out: &mut Vec<u8>, seq: u64, key: &[u8], value: Value<'_>) {
    let mut unsealed = encode(key, value);
    unsealed.seal(seq);
    *out = unsealed.record;
}

/// A record body, checked.
struct Record<'b> {
    seq: u64,
    key: &'b [u8],
    value: Value<'b>,
}

fn decode_body(body: &[u8]) -> Result<Record<'_>, LogDamage> {
    let length = LogDamage::Length(body.len() as u64);
    let op = body[0];
    let seq = u64::from_le_bytes(body[1..9].try_into().unwrap());
    let key_len = u32::from_le_bytes(body[9..13].try_into().unwrap()) as usize;
    if key_len > MAX_KEY_LEN || MIN_BODY + key_len > body.len() {
        return Err(length);
    }
    let key = &body[MIN_BODY..MIN_BODY + key_len];
    let rest = &body[MIN_BODY + key_len..];
    let value = match op {
        PUT => {
            if rest.len() < 4 {
                return Err(length);
            }
            let value_len = u32::from_le_bytes(rest[..4].try_into().unwrap()) as usize;
            if value_len > MAX_VALUE_LEN || rest.len() != 4 + value_len {
                return Err(length);
            }
            Value::Bytes(&rest[4..])
        }
        DELETE if rest.is_empty() => Value::Tombstone,
        DELETE => return Err(length),
        op => return Err(LogDamage::Operation(op)),
    };
    Ok(Record { seq, key, value })
}

/// One segment on disk.
#[derive(Clone, Copy, Debug)]
struct Segment {
    number: u64,
    base: u64,
    len: u64,
}

/// Faults a test can inject into the next batch a durable engine logs;
/// always `None` outside tests.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Fault {
    /// Write the first half of the batch's first record, then fail.
    TornAppend,
    /// Write the batch, then fail instead of syncing.
    SyncFails,
    /// Create the next segment with half a header, then fail.
    TornRotation,
}

/// Runs after the next batch this thread leads is logged, before it is
/// published, without the state lock but holding the log.
#[cfg(test)]
pub(super) type Hook = Box<dyn FnOnce(&Core)>;

#[cfg(test)]
thread_local! {
    pub(super) static BEFORE_PUBLISH: std::cell::Cell<Option<Hook>> = const { std::cell::Cell::new(None) };
}

/// Each segment's length at its last successful sync, for tests that model
/// a power loss: what a sync made durable stays, and the rest may not.
#[cfg(test)]
pub(super) static SYNCED: Mutex<std::collections::BTreeMap<PathBuf, u64>> =
    Mutex::new(std::collections::BTreeMap::new());

/// Records that `file`, at `path`, was just synced.
#[cfg(test)]
fn synced(path: &Path, file: &File) {
    if let Ok(metadata) = file.metadata() {
        SYNCED
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(path.to_owned(), metadata.len());
    }
}

fn injected() -> io::Error {
    io::Error::other("fault injected by a test")
}

/// Bounds on [`Wal::write_batch`]'s vector writes, below every platform's
/// `IOV_MAX`.
pub(super) const MAX_BATCH_RECORDS: usize = 256;

/// The live log. Only a batch's leader holds it, from reservation to
/// publication; the state lock is taken inside it, never around it.
pub(super) struct Wal {
    dir: PathBuf,
    options: DurableOptions,
    /// Oldest first; the last is the active segment.
    segments: Vec<Segment>,
    file: File,
    /// Total bytes of `segments`.
    bytes: u64,
    /// Records in the active segment.
    records: u64,
    pub(super) closed: Option<io::ErrorKind>,
    stats: Stats,
}

/// What the log has done since it was opened.
#[derive(Clone, Copy, Debug, Default)]
struct Stats {
    syncs: u64,
    batches: u64,
    records: u64,
    batch_sizes: [u64; 8],
}

/// Where the next records of a batch would go: projected while it is
/// validated, so that every record given a sequence number fits the log.
pub(super) struct Plan {
    active_len: u64,
    active_records: u64,
    bytes: u64,
}

/// What a durable engine owns beside its core.
pub(super) struct Durable {
    pub(super) dir: PathBuf,
    pub(super) options: DurableOptions,
    /// Held for the engine's life; the kernel releases it if the process dies.
    _lock: File,
    pub(super) wal: Mutex<Wal>,
    pub(super) group: Mutex<super::group::Group>,
    /// Signalled whenever the group changes: a batch taken or finished.
    pub(super) turn: std::sync::Condvar,
    /// The fault for the next batch; tests only.
    pub(super) fault: Mutex<Option<Fault>>,
}

impl Durable {
    fn new(dir: &Path, options: DurableOptions, lock: File, wal: Wal) -> Self {
        Self {
            dir: dir.to_owned(),
            options,
            _lock: lock,
            wal: Mutex::new(wal),
            group: Mutex::new(super::group::Group::default()),
            turn: std::sync::Condvar::new(),
            fault: Mutex::new(None),
        }
    }
}

/// Creates segment `number` with `base`, synced with its directory entry.
/// With `torn`, a test's fault, it writes half the header and fails.
fn create_segment(dir: &Path, number: u64, base: u64, torn: bool) -> io::Result<File> {
    let path = dir.join(segment_name(number));
    let mut file = OpenOptions::new()
        .append(true)
        .create_new(true)
        .open(&path)?;
    let header = encode_header(number, base);
    let len = if torn {
        SEGMENT_HEADER_LEN / 2
    } else {
        SEGMENT_HEADER_LEN
    };
    file.write_all(&header[..len])?;
    file.sync_all()?;
    #[cfg(test)]
    synced(&path, &file);
    sync_dir(dir)?;
    if torn {
        return Err(io::Error::other("torn rotation injected by a test"));
    }
    Ok(file)
}

impl Wal {
    fn new(
        dir: &Path,
        options: DurableOptions,
        mut segments: Vec<Segment>,
        base: u64,
    ) -> io::Result<Self> {
        let number = segments.last().map_or(1, |last| last.number + 1);
        let file = create_segment(dir, number, base, false)?;
        segments.push(Segment {
            number,
            base,
            len: SEGMENT_HEADER_LEN as u64,
        });
        Ok(Self {
            dir: dir.to_owned(),
            options,
            bytes: segments.iter().map(|segment| segment.len).sum(),
            segments,
            file,
            records: 0,
            closed: None,
            stats: Stats::default(),
        })
    }

    /// The log as it stands, for a batch to be planned against.
    pub(super) fn plan(&self) -> Plan {
        Plan {
            active_len: self.segments.last().map_or(0, |segment| segment.len),
            active_records: self.records,
            bytes: self.bytes,
        }
    }

    /// Adds a record of `len` bytes to `plan` if the log has room for it,
    /// returning whether a new segment must start before it.
    pub(super) fn admit(&self, plan: &mut Plan, len: u64) -> Result<bool, UpdateError> {
        let rotate = plan.active_records > 0 && plan.active_len + len > self.options.segment_bytes;
        let needed = len + if rotate { SEGMENT_HEADER_LEN as u64 } else { 0 };
        if plan.bytes + needed > self.options.max_log_bytes {
            return Err(UpdateError::LogFull {
                bytes: plan.bytes + needed,
                max_bytes: self.options.max_log_bytes,
            });
        }
        if rotate {
            plan.active_len = SEGMENT_HEADER_LEN as u64;
            plan.active_records = 0;
        }
        plan.active_len += len;
        plan.active_records += 1;
        plan.bytes += needed;
        Ok(rotate)
    }

    /// Appends a batch planned by [`Self::admit`]: each record, with the base
    /// of a new segment to start before it if one must. The records of each
    /// segment are written together and synced once, and a segment is synced
    /// before the next is created, so a crash leaves at most the batch's
    /// last records incomplete, at the end of the last segment.
    pub(super) fn write_batch(
        &mut self,
        records: &[(&[u8], Option<u64>)],
        fault: Option<Fault>,
    ) -> io::Result<()> {
        let mut start = 0;
        for (at, &(_, rotate)) in records.iter().enumerate() {
            if let Some(base) = rotate {
                self.append(&records[start..at], fault)?;
                start = at;
                let torn = fault == Some(Fault::TornRotation);
                let number = self.segments.last().unwrap().number + 1;
                self.file = create_segment(&self.dir, number, base, torn)?;
                self.segments.push(Segment {
                    number,
                    base,
                    len: SEGMENT_HEADER_LEN as u64,
                });
                self.bytes += SEGMENT_HEADER_LEN as u64;
                self.records = 0;
            }
        }
        self.append(&records[start..], fault)?;
        let count = records.len() as u64;
        self.stats.batches += 1;
        self.stats.records += count;
        let bucket = (64 - (count - 1).leading_zeros() as usize).min(7);
        self.stats.batch_sizes[bucket] += 1;
        Ok(())
    }

    /// Writes records to the active segment with vector writes, then syncs.
    fn append(&mut self, records: &[(&[u8], Option<u64>)], fault: Option<Fault>) -> io::Result<()> {
        if records.is_empty() {
            return Ok(());
        }
        if fault == Some(Fault::TornAppend) {
            self.file
                .write_all(&records[0].0[..records[0].0.len() / 2])?;
            self.file.sync_data()?;
            #[cfg(test)]
            synced(&self.active_path(), &self.file);
            return Err(injected());
        }
        let mut slices: Vec<io::IoSlice<'_>> = records
            .iter()
            .map(|&(record, _)| io::IoSlice::new(record))
            .collect();
        let mut remaining = &mut slices[..];
        while !remaining.is_empty() {
            match self.file.write_vectored(remaining) {
                Ok(0) => return Err(io::ErrorKind::WriteZero.into()),
                Ok(written) => io::IoSlice::advance_slices(&mut remaining, written),
                Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
                Err(error) => return Err(error),
            }
        }
        let len: u64 = records.iter().map(|&(record, _)| record.len() as u64).sum();
        self.segments.last_mut().unwrap().len += len;
        self.bytes += len;
        self.records += records.len() as u64;
        if fault == Some(Fault::SyncFails) {
            return Err(injected());
        }
        self.file.sync_data()?;
        #[cfg(test)]
        synced(&self.active_path(), &self.file);
        self.stats.syncs += 1;
        Ok(())
    }

    #[cfg(test)]
    fn active_path(&self) -> PathBuf {
        self.dir
            .join(segment_name(self.segments.last().unwrap().number))
    }

    fn status(&self) -> LogStatus {
        LogStatus {
            segments: self.segments.len(),
            bytes: self.bytes,
            active_bytes: self.segments.last().map_or(0, |segment| segment.len),
            closed: self.closed,
            syncs: self.stats.syncs,
            batches: self.stats.batches,
            records: self.stats.records,
            batch_sizes: self.stats.batch_sizes,
            queued_writes: 0,
            queued_bytes: 0,
        }
    }
}

/// Removes, oldest first, the segments a checkpoint at `ceiling` covers:
/// those whose successor's base is at most `ceiling`, never the active one.
/// Files are removed without holding the log, which only appends meanwhile.
fn prune(durable: &Durable, ceiling: u64) -> usize {
    let lock = || durable.wal.lock().unwrap_or_else(PoisonError::into_inner);
    let covered: Vec<u64> = {
        let wal = lock();
        wal.segments
            .windows(2)
            .take_while(|pair| pair[1].base <= ceiling)
            .map(|pair| pair[0].number)
            .collect()
    };
    let mut removed = 0;
    for &number in &covered {
        if fs::remove_file(durable.dir.join(segment_name(number))).is_err() {
            break;
        }
        removed += 1;
    }
    if removed > 0 {
        let mut wal = lock();
        let freed: u64 = wal.segments[..removed]
            .iter()
            .map(|segment| segment.len)
            .sum();
        wal.segments.drain(..removed);
        wal.bytes -= freed;
        drop(wal);
        // Removal need not be durable: a segment that reappears is covered by
        // the checkpoint and ignored, and removed again by the next one.
        let _ = sync_dir(&durable.dir);
    }
    removed
}

/// What reading the log found.
struct Scanned {
    /// Segments to keep, the obsolete ones first, then the chain read.
    segments: Vec<Segment>,
    first_read: usize,
    last_seq: u64,
    replayed: u64,
    skipped: u64,
    log_bytes: u64,
    torn: Option<TornTail>,
    interrupted: Option<PathBuf>,
}

/// Applies a recovered record.
type Apply<'a> = dyn FnMut(u64, &[u8], Value<'_>) -> Result<(), RecoveryError> + 'a;

/// Where reading the log has got to.
struct Replay<'a> {
    /// The last sequence number read, or the chain's base.
    prev: u64,
    /// The segment being read.
    number: u64,
    /// The last record that passed its checks: its segment, where it ends,
    /// and its sequence number.
    last_valid: Option<(u64, u64, u64)>,
    ceiling: u64,
    replayed: u64,
    skipped: u64,
    /// One record body, reused.
    body: Vec<u8>,
    apply: &'a mut Apply<'a>,
}

/// Reads one segment's records after its header, checking each, and passes
/// those above `ceiling` to `apply`. Returns the valid length and whether a
/// torn tail follows it.
fn read_segment(
    path: &Path,
    file: File,
    file_len: u64,
    last: bool,
    replay: &mut Replay<'_>,
) -> Result<(u64, bool), RecoveryError> {
    let Replay {
        prev,
        number,
        last_valid,
        ceiling,
        replayed,
        skipped,
        body,
        apply,
    } = replay;
    let ceiling = *ceiling;
    let damaged = |offset: u64, damage| RecoveryError::Log {
        path: path.to_owned(),
        offset,
        damage,
    };
    let mut reader = BufReader::with_capacity(READ_BUFFER, file);
    let mut offset = SEGMENT_HEADER_LEN as u64;
    loop {
        let remaining = file_len - offset;
        if remaining == 0 {
            return Ok((offset, false));
        }
        if remaining < FRAME_LEN as u64 {
            return if last {
                Ok((offset, true))
            } else {
                Err(damaged(offset, LogDamage::Truncated))
            };
        }
        let mut frame = [0; FRAME_LEN];
        reader
            .read_exact(&mut frame)
            .map_err(|error| io_error(path, error))?;
        if frame == [0; FRAME_LEN]
            && last
            && zeros(&mut reader, remaining - FRAME_LEN as u64, path)?
        {
            return Ok((offset, true));
        }
        let u32_at = |at: usize| u32::from_le_bytes(frame[at..at + 4].try_into().unwrap());
        if crc(&[&frame[..8]]) != u32_at(8) {
            return Err(damaged(offset, LogDamage::Frame));
        }
        let len = u32_at(0) as usize;
        if !(MIN_BODY..=MAX_BODY).contains(&len) {
            return Err(damaged(offset, LogDamage::Length(len as u64)));
        }
        if (len as u64) > remaining - FRAME_LEN as u64 {
            return if last {
                Ok((offset, true))
            } else {
                Err(damaged(offset, LogDamage::Truncated))
            };
        }
        body.resize(len, 0);
        reader
            .read_exact(body.as_mut_slice())
            .map_err(|error| io_error(path, error))?;
        if crc(&[body.as_slice()]) != u32_at(4) {
            return Err(damaged(offset, LogDamage::Checksum));
        }
        let record = decode_body(body).map_err(|damage| damaged(offset, damage))?;
        let expected = prev.checked_add(1).ok_or_else(|| {
            damaged(
                offset,
                LogDamage::Sequence {
                    expected: u64::MAX,
                    found: record.seq,
                },
            )
        })?;
        if record.seq != expected {
            return Err(damaged(
                offset,
                LogDamage::Sequence {
                    expected,
                    found: record.seq,
                },
            ));
        }
        if record.seq > ceiling {
            apply(record.seq, record.key, record.value)?;
            *replayed += 1;
        } else {
            *skipped += 1;
        }
        *prev = record.seq;
        offset += (FRAME_LEN + len) as u64;
        *last_valid = Some((*number, offset, record.seq));
    }
}

/// Whether the next `len` bytes are all zero.
fn zeros(reader: &mut impl Read, len: u64, path: &Path) -> Result<bool, RecoveryError> {
    let mut chunk = [0u8; 4096];
    let mut left = len;
    while left > 0 {
        let take = left.min(chunk.len() as u64) as usize;
        reader
            .read_exact(&mut chunk[..take])
            .map_err(|error| io_error(path, error))?;
        if chunk[..take].iter().any(|&b| b != 0) {
            return Ok(false);
        }
        left -= take as u64;
    }
    Ok(true)
}

fn io_error(path: &Path, error: io::Error) -> RecoveryError {
    RecoveryError::Io {
        path: path.to_owned(),
        error,
    }
}

/// Reads and checks the log against a checkpoint at `ceiling`, passing each
/// record above it to `apply`, or without a checkpoint from the oldest
/// segment. Changes nothing on disk. `last_valid` is left at the last record
/// that passed, also when an error is returned. Segment files are opened one
/// at a time, so the number of segments is not bounded by open files.
fn scan<'a>(
    dir: &Path,
    ceiling: Option<u64>,
    apply: &'a mut Apply<'a>,
    last_valid: &mut Option<(u64, u64, u64)>,
) -> Result<Scanned, RecoveryError> {
    let damaged = |path: PathBuf, offset: u64, damage| RecoveryError::Log {
        path,
        offset,
        damage,
    };
    let numbers = segment_numbers(dir).map_err(|error| io_error(dir, error))?;
    let mut scanned = Scanned {
        segments: Vec::with_capacity(numbers.len()),
        first_read: 0,
        last_seq: 0,
        replayed: 0,
        skipped: 0,
        log_bytes: 0,
        torn: None,
        interrupted: None,
    };
    // Headers first, with the files' lengths.
    for (index, &number) in numbers.iter().enumerate() {
        let path = dir.join(segment_name(number));
        let mut file = File::open(&path).map_err(|error| io_error(&path, error))?;
        let len = file
            .metadata()
            .map_err(|error| io_error(&path, error))?
            .len();
        let mut header = [0; SEGMENT_HEADER_LEN];
        let last = index + 1 == numbers.len();
        if len < SEGMENT_HEADER_LEN as u64 {
            if last {
                scanned.interrupted = Some(path);
                break;
            }
            return Err(damaged(path, 0, LogDamage::Header));
        }
        file.read_exact(&mut header)
            .map_err(|error| io_error(&path, error))?;
        let base = match decode_header(&header, number) {
            Ok(base) => base,
            Err(_)
                if last
                    && header == [0; SEGMENT_HEADER_LEN]
                    && len == SEGMENT_HEADER_LEN as u64 =>
            {
                scanned.interrupted = Some(path);
                break;
            }
            Err(damage) => return Err(damaged(path, 0, damage)),
        };
        scanned.segments.push(Segment { number, base, len });
    }
    if scanned.segments.is_empty() {
        return Err(damaged(dir.to_owned(), 0, LogDamage::NoSegments));
    }
    // The newest segment whose base the checkpoint covers starts the chain.
    let ceiling = ceiling.unwrap_or(scanned.segments[0].base);
    let Some(start) = scanned
        .segments
        .iter()
        .rposition(|segment| segment.base <= ceiling)
    else {
        let first = scanned.segments[0];
        return Err(damaged(
            dir.join(segment_name(first.number)),
            0,
            LogDamage::Gap {
                base: first.base,
                ceiling,
            },
        ));
    };
    scanned.first_read = start;
    let count = scanned.segments.len();
    let mut replay = Replay {
        prev: scanned.segments[start].base,
        number: 0,
        last_valid: None,
        ceiling,
        replayed: 0,
        skipped: 0,
        body: Vec::new(),
        apply,
    };
    for index in start..count {
        let segment = scanned.segments[index];
        let path = dir.join(segment_name(segment.number));
        if index > start {
            let before = scanned.segments[index - 1].number;
            if segment.number != before + 1 {
                return Err(damaged(
                    dir.to_owned(),
                    0,
                    LogDamage::Missing { after: before },
                ));
            }
            if segment.base != replay.prev {
                return Err(damaged(
                    path,
                    0,
                    LogDamage::Base {
                        expected: replay.prev,
                        found: segment.base,
                    },
                ));
            }
        }
        let last = index + 1 == count;
        let mut file = File::open(&path).map_err(|error| io_error(&path, error))?;
        file.seek(SeekFrom::Start(SEGMENT_HEADER_LEN as u64))
            .map_err(|error| io_error(&path, error))?;
        replay.number = segment.number;
        let read = read_segment(&path, file, segment.len, last, &mut replay);
        *last_valid = replay.last_valid;
        let (valid, torn) = read?;
        scanned.log_bytes += segment.len;
        if torn {
            scanned.torn = Some(TornTail {
                path,
                offset: valid,
                bytes: segment.len - valid,
            });
            scanned.segments[index].len = valid;
        }
    }
    if replay.prev < ceiling {
        return Err(damaged(
            dir.join(segment_name(scanned.segments[count - 1].number)),
            0,
            LogDamage::Behind {
                last: replay.prev,
                ceiling,
            },
        ));
    }
    scanned.last_seq = replay.prev;
    scanned.replayed = replay.replayed;
    scanned.skipped = replay.skipped;
    Ok(scanned)
}

impl Engine {
    /// Creates a durable database in `dir`, which must not exist or be empty,
    /// and returns its engine. It writes the first log segment (based at
    /// [`EngineOptions::last_seq`]) and then commits an empty checkpoint,
    /// which marks the directory as a database; until that commit `open`
    /// refuses it as [`RecoveryError::NoCheckpoint`]. If creation fails or is
    /// interrupted, remove the directory before creating it again;
    /// [`Engine::inspect`] tells such a directory ([`Verdict::Uncommitted`])
    /// from one that lost its `CURRENT`.
    pub fn create(
        dir: &Path,
        options: EngineOptions,
        durable: DurableOptions,
    ) -> Result<Engine, RecoveryError> {
        let io = |path: &Path| {
            let path = path.to_owned();
            move |error| RecoveryError::Io { path, error }
        };
        fs::create_dir_all(dir).map_err(io(dir))?;
        let lock = match checkpoint::lock(dir).map_err(io(dir))? {
            Ok(file) => file,
            Err(()) => return Err(RecoveryError::Locked(dir.to_owned())),
        };
        for entry in fs::read_dir(dir).map_err(io(dir))? {
            if entry.map_err(io(dir))?.file_name() != checkpoint::LOCK {
                return Err(RecoveryError::NotEmpty(dir.to_owned()));
            }
        }
        let wal = Wal::new(dir, durable, Vec::new(), options.last_seq).map_err(io(dir))?;
        let engine = Engine::new(options);
        let published = engine.publish(dir, CheckpointOptions::default());
        let installed = Durable::new(dir, durable, lock, wal);
        match published {
            Ok(_) => {}
            Err(CheckpointError::Io {
                path,
                error,
                after_commit_point: true,
            }) => return Err(RecoveryError::Io { path, error }),
            Err(error) => {
                let _ = fs::remove_file(dir.join(segment_name(1)));
                return Err(RecoveryError::Io {
                    path: dir.to_owned(),
                    error: io::Error::other(error.to_string()),
                });
            }
        }
        let _ = engine.core.durable.set(installed);
        Ok(engine)
    }

    /// Opens the durable database in `dir`: loads its committed checkpoint,
    /// replays the log after it, and returns an engine that logs every write,
    /// holding `dir`'s lock until it is dropped. The sequence continues after
    /// the last record recovered; [`EngineOptions::last_seq`] is ignored.
    ///
    /// Everything is checked before the engine is returned; see the module
    /// source `src/engine/wal.rs` for the rules. Only then are a torn tail
    /// truncated and an interrupted segment removed, and a new segment
    /// started. A directory without `CURRENT` is
    /// [`RecoveryError::NoCheckpoint`], never a new empty database.
    pub fn open(
        dir: &Path,
        options: EngineOptions,
        durable: DurableOptions,
    ) -> Result<(Engine, OpenReport), RecoveryError> {
        let lock = checkpoint::lock_existing(dir)?;
        let loaded = checkpoint::load(dir, &options)?;
        let (generation, ceiling) = (loaded.generation, loaded.ceiling);
        let options = EngineOptions {
            last_seq: ceiling,
            ..options
        };
        let engine = Engine::start(Arc::new(loaded.into_core(&options)), &options);
        let scanned = scan(
            dir,
            Some(ceiling),
            &mut |seq, key, value| engine.replay(seq, key, value),
            &mut None,
        )?;
        debug_assert_eq!(engine.core.read_state().last_seq, scanned.last_seq);

        if let Some(torn) = &scanned.torn {
            OpenOptions::new()
                .write(true)
                .open(&torn.path)
                .and_then(|file| {
                    file.set_len(torn.offset)?;
                    file.sync_all()?;
                    #[cfg(test)]
                    synced(&torn.path, &file);
                    Ok(())
                })
                .map_err(|error| io_error(&torn.path, error))?;
        }
        if let Some(path) = &scanned.interrupted {
            fs::remove_file(path).map_err(|error| io_error(path, error))?;
        }
        // What was replayed must be durable before a segment based on it is
        // created: a process that died before its sync left records only in
        // the page cache, and a power loss could otherwise drop them but keep
        // the next segment, whose base would then point past the end.
        if scanned.torn.is_none() {
            let last = scanned.segments.last().expect("scan found a segment");
            let path = dir.join(segment_name(last.number));
            File::open(&path)
                .and_then(|file| {
                    file.sync_all()?;
                    #[cfg(test)]
                    synced(&path, &file);
                    Ok(())
                })
                .map_err(|error| io_error(&path, error))?;
        }
        let wal = Wal::new(dir, durable, scanned.segments.clone(), scanned.last_seq)
            .map_err(|error| io_error(dir, error))?;
        let report = OpenReport {
            generation,
            checkpoint_seq: ceiling,
            last_seq: scanned.last_seq,
            segments: scanned.segments.len() - scanned.first_read,
            replayed: scanned.replayed,
            skipped: scanned.skipped,
            log_bytes: scanned.log_bytes,
            torn_tail: scanned.torn,
            interrupted_segment: scanned.interrupted,
        };
        let _ = engine
            .core
            .durable
            .set(Durable::new(dir, durable, lock, wal));
        Ok((engine, report))
    }

    /// Commits a checkpoint into this durable engine's directory while writes
    /// continue, then prunes the log segments it covers. See
    /// [`Engine::checkpoint`] for the checkpoint and `src/engine/wal.rs` for
    /// pruning; a checkpoint that fails or is not committed prunes nothing.
    pub fn durable_checkpoint(
        &self,
        options: CheckpointOptions,
    ) -> Result<CheckpointReport, CheckpointError> {
        let durable = self.core.durable.get().ok_or(CheckpointError::NotDurable)?;
        let _serial = self
            .core
            .checkpointing
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        let mut report = self.publish(&durable.dir, options)?;
        #[cfg(test)]
        if checkpoint::crash(checkpoint::Crash::BeforePrune) {
            return Err(checkpoint::crashed(&durable.dir));
        }
        report.pruned_segments = prune(durable, report.last_seq);
        Ok(report)
    }

    /// Reads and checks `dir`'s checkpoint pointer and log as [`Engine::open`]
    /// with `options` would, changing nothing and creating nothing, and reports what open
    /// would do, how far the log is valid and where it is damaged. The
    /// checkpoint's runs are not read: open and [`Engine::recover`] check
    /// them. It takes a shared lock if `LOCK` exists, so it fails with
    /// [`RecoveryError::Locked`] while an engine has the directory open.
    ///
    /// Damage to `CURRENT` or the manifest is returned as the error open
    /// would return. Nothing is repaired: see [`Verdict::Damaged`].
    pub fn inspect(dir: &Path, options: &EngineOptions) -> Result<Inspection, RecoveryError> {
        match fs::metadata(dir) {
            Ok(_) => {}
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                return Err(RecoveryError::NoCheckpoint(dir.to_owned()));
            }
            Err(error) => return Err(io_error(dir, error)),
        }
        let _lock = checkpoint::lock_shared(dir)?;
        let checkpoint = checkpoint::committed(dir, options)?;
        let numbers = segment_numbers(dir).map_err(|error| io_error(dir, error))?;
        let mut last_valid = None;
        let scanned = scan(
            dir,
            checkpoint.map(|(_, ceiling)| ceiling),
            &mut |_, _, _| Ok(()),
            &mut last_valid,
        );
        let verdict = match (checkpoint, scanned) {
            (Some(_), Ok(scanned)) => Verdict::Opens {
                torn_tail: scanned.torn,
                interrupted_segment: scanned.interrupted,
            },
            (None, _) => {
                // `create` writes segment 1 and then checkpoint 1.
                let empty_log = match numbers.as_slice() {
                    [] => true,
                    [1] => {
                        let path = dir.join(segment_name(1));
                        let len = fs::metadata(&path)
                            .map_err(|error| io_error(&path, error))?
                            .len();
                        len <= SEGMENT_HEADER_LEN as u64
                    }
                    _ => false,
                };
                let empty_checkpoints = checkpoint::at_most_an_empty_first_checkpoint(dir)
                    .map_err(|error| io_error(dir, error))?;
                if empty_log && empty_checkpoints {
                    Verdict::Uncommitted
                } else {
                    Verdict::NoCheckpoint
                }
            }
            (
                Some(_),
                Err(RecoveryError::Log {
                    path,
                    offset,
                    damage,
                }),
            ) => {
                let mut bytes_after = 0;
                if let Some(at) = numbers
                    .iter()
                    .position(|&number| dir.join(segment_name(number)) == path)
                {
                    for &number in &numbers[at..] {
                        let segment = dir.join(segment_name(number));
                        bytes_after += fs::metadata(&segment)
                            .map_err(|error| io_error(&segment, error))?
                            .len();
                    }
                    bytes_after = bytes_after.saturating_sub(offset);
                }
                Verdict::Damaged {
                    path,
                    offset,
                    damage,
                    bytes_after,
                }
            }
            (Some(_), Err(error)) => return Err(error),
        };
        Ok(Inspection {
            checkpoint,
            segments: numbers.len(),
            last_valid: last_valid.map(|(number, end, seq)| LogPosition {
                path: dir.join(segment_name(number)),
                end,
                seq,
            }),
            verdict,
        })
    }

    /// The log of a durable engine, or `None` for a RAM-only one.
    pub fn log_status(&self) -> Option<LogStatus> {
        self.core.durable.get().map(|durable| {
            // One lock at a time: the log waits for a batch in progress.
            let mut status = durable
                .wal
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .status();
            (status.queued_writes, status.queued_bytes) = durable.queue_load();
            status
        })
    }

    /// Applies one logged record on recovery, flushing (and waiting for
    /// compaction) when the memtables are full.
    fn replay(&self, seq: u64, key: &[u8], value: Value<'_>) -> Result<(), RecoveryError> {
        let mut made_room = false;
        loop {
            match self.core.apply_logged(seq, key, value) {
                Ok(()) => return Ok(()),
                Err(WriteError::BudgetExceeded { .. }) if !made_room => {
                    self.make_room()?;
                    made_room = true;
                }
                Err(error) => return Err(RecoveryError::Replay(UpdateError::Rejected(error))),
            }
        }
    }

    fn make_room(&self) -> Result<(), RecoveryError> {
        match self.core.flush() {
            Ok(_) => Ok(()),
            Err(refused) => {
                if self.core.background.is_none() || self.wait_for_compaction().is_err() {
                    return Err(RecoveryError::RunLimit(refused));
                }
                self.core
                    .flush()
                    .map(|_| ())
                    .map_err(RecoveryError::RunLimit)
            }
        }
    }
}

impl Core {
    /// Applies a recovered record with its own sequence number.
    fn apply_logged(&self, seq: u64, key: &[u8], value: Value<'_>) -> Result<(), WriteError> {
        let mut state = self.write_state();
        let applied = match value {
            Value::Bytes(bytes) => state.active.put(key, seq, bytes),
            Value::Tombstone => state.active.delete(key, seq),
        };
        applied.map_err(|error| self.reason(&state, error))?;
        state.last_seq = seq;
        Ok(())
    }
}

impl fmt::Debug for Durable {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Durable")
            .field("dir", &self.dir)
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::EngineWorkspace;
    use crate::engine::checkpoint::{CRASH_AT, Crash};

    fn scratch(name: &str) -> PathBuf {
        let path = std::env::temp_dir().join(format!("tessera-wal-{name}-{}", std::process::id()));
        let _ = fs::remove_dir_all(&path);
        path
    }

    /// Makes the next batch `engine` logs fail with `fault`.
    fn inject(engine: &Engine, fault: Fault) {
        *engine.core.durable.get().unwrap().fault.lock().unwrap() = Some(fault);
    }

    fn small() -> DurableOptions {
        DurableOptions {
            segment_bytes: 1024,
            ..DurableOptions::default()
        }
    }

    fn get(engine: &Engine, key: &[u8]) -> Option<Vec<u8>> {
        let mut workspace = EngineWorkspace::new();
        engine
            .get(key, &mut workspace)
            .unwrap()
            .map(|found| found.value.to_vec())
    }

    fn state(engine: &Engine) -> Vec<(Vec<u8>, Vec<u8>)> {
        let mut workspace = EngineWorkspace::new();
        let guard = engine.read();
        let mut scan = guard.scan(&mut workspace);
        let mut out = Vec::new();
        while let Some(record) = scan.next_record().unwrap() {
            out.push((record.key.to_vec(), record.value.to_vec()));
        }
        out
    }

    #[test]
    fn records_round_trip_and_reject_damage() {
        let mut out = Vec::new();
        for (seq, key, value) in [
            (1, &b""[..], Value::Bytes(b"")),
            (u64::MAX, &[0xff; 3][..], Value::Tombstone),
            (7, &b"key"[..], Value::Bytes(&[0; 300])),
        ] {
            encode_record(&mut out, seq, key, value);
            let len = u32::from_le_bytes(out[..4].try_into().unwrap()) as usize;
            assert_eq!(len, out.len() - FRAME_LEN);
            assert_eq!(crc(&[&out[..8]]).to_le_bytes(), out[8..12]);
            let record = decode_body(&out[FRAME_LEN..]).unwrap();
            assert_eq!((record.seq, record.key, record.value), (seq, key, value));
        }
        // Lengths inside the body that disagree with it, and an unknown
        // operation, are damage.
        encode_record(&mut out, 3, b"ab", Value::Bytes(b"cd"));
        let body = &mut out[FRAME_LEN..];
        let mut long_key = body.to_vec();
        long_key[9] = 200;
        assert!(matches!(decode_body(&long_key), Err(LogDamage::Length(_))));
        let mut short_value = body.to_vec();
        short_value.pop();
        assert!(matches!(
            decode_body(&short_value),
            Err(LogDamage::Length(_))
        ));
        body[0] = 9;
        assert_eq!(decode_body(body).err(), Some(LogDamage::Operation(9)));
        let header = encode_header(4, 99);
        assert_eq!(decode_header(&header, 4), Ok(99));
        assert_eq!(decode_header(&header, 5), Err(LogDamage::Header));
        let mut damaged = header;
        damaged[25] ^= 1;
        assert_eq!(decode_header(&damaged, 4), Err(LogDamage::Header));
    }

    /// An append that fails part way, or whose sync fails, is not applied
    /// and closes the log; recovery has the acknowledged writes, with the
    /// failed one only if all of it reached the file.
    #[test]
    fn a_failed_append_closes_the_log_and_recovery_keeps_what_was_acknowledged() {
        for (fault, survives) in [(Fault::TornAppend, false), (Fault::SyncFails, true)] {
            let dir = scratch(&format!("{fault:?}"));
            let engine = Engine::create(&dir, EngineOptions::default(), small()).unwrap();
            assert_eq!(engine.put(b"a", b"1").unwrap(), 1);
            assert_eq!(engine.delete(b"a").unwrap(), 2);
            inject(&engine, fault);
            assert!(matches!(
                engine.put(b"b", b"2"),
                Err(UpdateError::LogFailed(_))
            ));
            assert_eq!(engine.put(b"c", b"3"), Err(UpdateError::LogClosed));
            assert_eq!(get(&engine, b"b"), None);
            assert_eq!(engine.stats().last_seq, 2);
            assert!(engine.log_status().unwrap().closed.is_some());
            drop(engine);

            let (engine, report) = Engine::open(&dir, EngineOptions::default(), small()).unwrap();
            assert_eq!(report.torn_tail.is_some(), !survives, "{fault:?}");
            assert_eq!(get(&engine, b"a"), None);
            assert_eq!(get(&engine, b"b").is_some(), survives);
            let next = if survives { 4 } else { 3 };
            assert_eq!(engine.put(b"d", b"4").unwrap(), next);
            drop(engine);
            let (engine, report) = Engine::open(&dir, EngineOptions::default(), small()).unwrap();
            assert_eq!(report.torn_tail, None);
            assert_eq!(report.last_seq, next);
            assert_eq!(get(&engine, b"d"), Some(b"4".to_vec()));
            drop(engine);
            fs::remove_dir_all(&dir).unwrap();
        }
    }

    /// A snapshot taken while a record syncs can leave its update no longer
    /// fitting the budget; it is logged, so it is applied, over the budget by
    /// that update, and the next write is refused as usual.
    #[test]
    fn a_logged_update_is_applied_even_if_the_budget_shrank_while_it_synced() {
        let dir = scratch("over-budget");
        let options = EngineOptions {
            memtable_budget: 16 << 10,
            ..EngineOptions::default()
        };
        let engine = Engine::create(&dir, options, small()).unwrap();
        let mut keys = 0u32;
        while engine.put(&keys.to_be_bytes(), &[1; 100]).is_ok() {
            keys += 1;
        }
        // Fits now, replacing a value of the same length...
        let snapshot = std::rc::Rc::new(std::cell::RefCell::new(None));
        let held = std::rc::Rc::clone(&snapshot);
        BEFORE_PUBLISH.with(|hook| {
            hook.set(Some(Box::new(move |core: &Core| {
                *held.borrow_mut() = Some(core.snapshot().unwrap());
            })))
        });
        // ...but not once a snapshot shares the path it copies.
        let seq = engine.put(&0u32.to_be_bytes(), &[2; 100]).unwrap();
        assert!(snapshot.borrow().is_some());
        let active = engine.stats().active;
        assert!(active.charged > active.budget, "{active:?}");
        assert_eq!(get(&engine, &0u32.to_be_bytes()), Some(vec![2; 100]));
        assert!(matches!(
            engine.put(&1u32.to_be_bytes(), &[3; 100]),
            Err(UpdateError::Rejected(WriteError::BudgetExceeded { .. }))
        ));
        drop(snapshot);
        drop(engine);
        let (engine, report) = Engine::open(&dir, options, small()).unwrap();
        assert_eq!(report.last_seq, seq);
        assert_eq!(get(&engine, &0u32.to_be_bytes()), Some(vec![2; 100]));
        drop(engine);
        fs::remove_dir_all(&dir).unwrap();
    }

    /// A table over its budget admits only an update that brings it back
    /// within it, so the excess a logged update leaves cannot grow through
    /// later updates.
    #[test]
    fn a_table_over_its_budget_admits_only_updates_that_restore_it() {
        use crate::memtable::Memtable;
        let mut table = Memtable::new(4096, None);
        let mut seq = 0u64;
        while table.put(&seq.to_be_bytes(), seq + 1, &[1; 100]).is_ok() {
            seq += 1;
        }
        // A logged update past the budget by 50 bytes, as a durable engine
        // applies one: a larger value for the first key.
        let full = table.memory();
        let len = 100 + (full.budget - full.charged) + 50;
        let key = 0u64.to_be_bytes();
        table
            .write_admitted(&key, seq + 1, Value::Bytes(&vec![2; len]))
            .unwrap();
        let over = table.memory();
        assert_eq!(over.charged, over.budget + 50);
        // Shrinking the value by less than the excess is refused...
        assert!(matches!(
            table.put(&key, seq + 2, &vec![3; len - 49]),
            Err(WriteError::BudgetExceeded { .. })
        ));
        // ...and shrinking it by the excess is admitted, within the budget.
        table.put(&key, seq + 2, &vec![3; len - 50]).unwrap();
        let after = table.memory();
        assert!(after.charged <= after.budget, "{after:?}");
    }

    /// A checkpoint committed while a write is logged but not yet published
    /// excludes it from its ceiling, and its pruning, which waits for the
    /// write, keeps the segment holding it.
    #[test]
    fn a_checkpoint_during_an_in_flight_write_keeps_its_record() {
        use std::sync::mpsc;
        let dir = scratch("in-flight");
        let engine = Engine::create(&dir, EngineOptions::default(), small()).unwrap();
        for at in 0..20u32 {
            engine.put(&at.to_be_bytes(), &[1; 100]).unwrap();
        }
        let current = || fs::read(dir.join("CURRENT")).unwrap();
        let before = current();
        let (logged, wait_logged) = mpsc::channel();
        let (go, wait_go) = mpsc::channel::<()>();
        std::thread::scope(|scope| {
            let writer = scope.spawn(|| {
                BEFORE_PUBLISH.with(|hook| {
                    hook.set(Some(Box::new(move |_: &Core| {
                        logged.send(()).unwrap();
                        wait_go.recv().unwrap();
                    })))
                });
                // Does not fit the active segment, so it starts a new one.
                engine.put(b"in-flight", &[2; 900]).unwrap()
            });
            wait_logged.recv().unwrap();
            let checkpoint = scope.spawn(|| {
                engine
                    .durable_checkpoint(CheckpointOptions::default())
                    .unwrap()
            });
            // Let the writer publish once the checkpoint has committed; its
            // pruning waits for the log until then.
            for _ in 0..10_000 {
                if current() != before {
                    break;
                }
                std::thread::sleep(std::time::Duration::from_millis(1));
            }
            assert_ne!(current(), before, "the checkpoint did not commit");
            go.send(()).unwrap();
            let seq = writer.join().unwrap();
            let report = checkpoint.join().unwrap();
            assert_eq!(report.last_seq, seq - 1);
            assert!(report.pruned_segments > 0, "{report:?}");
        });
        drop(engine);
        let (engine, report) = Engine::open(&dir, EngineOptions::default(), small()).unwrap();
        assert_eq!((report.replayed, report.last_seq), (1, 21));
        assert_eq!(get(&engine, b"in-flight"), Some(vec![2; 900]));
        drop(engine);
        fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn an_interrupted_rotation_is_removed_on_open() {
        let dir = scratch("rotation");
        let engine = Engine::create(&dir, EngineOptions::default(), small()).unwrap();
        let value = [7; 400];
        engine.put(b"a", &value).unwrap();
        engine.put(b"b", &value).unwrap();
        // The third does not fit the first segment.
        inject(&engine, Fault::TornRotation);
        assert!(matches!(
            engine.put(b"c", &value),
            Err(UpdateError::LogFailed(_))
        ));
        drop(engine);
        let (engine, report) = Engine::open(&dir, EngineOptions::default(), small()).unwrap();
        assert_eq!(report.interrupted_segment, Some(dir.join(segment_name(2))));
        assert_eq!(report.last_seq, 2);
        assert_eq!(get(&engine, b"b"), Some(value.to_vec()));
        engine.put(b"c", &value).unwrap();
        drop(engine);
        let (engine, report) = Engine::open(&dir, EngineOptions::default(), small()).unwrap();
        assert_eq!((report.last_seq, report.interrupted_segment), (3, None));
        assert_eq!(get(&engine, b"c"), Some(value.to_vec()));
        drop(engine);
        fs::remove_dir_all(&dir).unwrap();
    }

    /// Models a power loss: every segment is cut back to its length at its
    /// last sync, or with `zeros` keeps its length but loses the rest to
    /// zeros; one never synced is gone. It models only the loss of unsynced
    /// segment bytes, as a prefix: not pages of one write persisting out of
    /// order, not lost directory entries or renames, and not checkpoint
    /// files, which it takes as durable once synced.
    fn power_loss(dir: &Path, zeros: bool) {
        let synced = SYNCED.lock().unwrap().clone();
        for number in segment_numbers(dir).unwrap() {
            let path = dir.join(segment_name(number));
            let Some(&durable) = synced.get(&path) else {
                fs::remove_file(&path).unwrap();
                continue;
            };
            let mut bytes = fs::read(&path).unwrap();
            if zeros {
                bytes[durable as usize..].fill(0);
            } else {
                bytes.truncate(durable as usize);
            }
            fs::write(&path, bytes).unwrap();
        }
    }

    /// A process that dies after writing a batch but before syncing it
    /// leaves the batch in the page cache, where the next open replays it.
    /// That open syncs it before basing a new segment on it, so a power loss
    /// after further acknowledged writes loses none of them. A batch whose
    /// sync then fails is lost to the power loss, or left as zeros, and the
    /// acknowledged history before it is recovered intact.
    #[test]
    fn a_power_loss_after_a_crash_and_reopen_keeps_every_acknowledged_write() {
        for zeros in [false, true] {
            let dir = scratch(&format!("power-{zeros}"));
            let engine = Engine::create(&dir, EngineOptions::default(), small()).unwrap();
            let mut model = Vec::new();
            for at in 0..3u8 {
                engine.put(&[at], &[at; 100]).unwrap();
                model.push((vec![at], vec![at; 100]));
            }
            // Written, never synced: as if the process died before its sync.
            inject(&engine, Fault::SyncFails);
            assert!(engine.put(&[3], &[3; 100]).is_err());
            drop(engine);
            let (engine, report) = Engine::open(&dir, EngineOptions::default(), small()).unwrap();
            assert_eq!(report.last_seq, 4, "recovered from the page cache");
            model.push((vec![3], vec![3; 100]));
            // Acknowledged after the reopen, across several new segments.
            for at in 4..80u8 {
                engine.put(&[at], &[at; 100]).unwrap();
                model.push((vec![at], vec![at; 100]));
            }
            assert!(engine.log_status().unwrap().segments > 3);
            inject(&engine, Fault::SyncFails);
            assert!(engine.put(b"lost", b"").is_err());
            drop(engine);

            power_loss(&dir, zeros);
            let (engine, report) = Engine::open(&dir, EngineOptions::default(), small())
                .unwrap_or_else(|error| panic!("zeros {zeros}: {error}"));
            assert_eq!(report.last_seq, 80);
            assert_eq!(state(&engine), model);
            drop(engine);
            fs::remove_dir_all(&dir).unwrap();
        }
    }

    /// Recovery opens one segment at a time, so a log of more segments than
    /// a process may have files open (1,024 here) is recovered, and pruned.
    #[test]
    fn a_log_of_more_segments_than_open_files_is_recovered() {
        let dir = scratch("many");
        let engine = Engine::create(&dir, EngineOptions::default(), small()).unwrap();
        engine.put(b"a", b"1").unwrap();
        drop(engine);
        // What 1,200 opens without a checkpoint would leave: empty segments.
        for number in 2..=1_201 {
            fs::write(dir.join(segment_name(number)), encode_header(number, 1)).unwrap();
        }
        let (engine, report) = Engine::open(&dir, EngineOptions::default(), small()).unwrap();
        assert_eq!((report.last_seq, report.segments), (1, 1_201));
        assert_eq!(get(&engine, b"a"), Some(b"1".to_vec()));
        let checkpoint = engine
            .durable_checkpoint(CheckpointOptions::default())
            .unwrap();
        assert_eq!(checkpoint.pruned_segments, 1_201);
        assert_eq!(engine.log_status().unwrap().segments, 1);
        drop(engine);
        fs::remove_dir_all(&dir).unwrap();
    }

    /// A durable checkpoint stopped at any point, before or after its commit,
    /// leaves the full history: no segment is pruned before the commit is
    /// durable, and pruning only removes what the commit covers.
    #[test]
    fn an_interrupted_durable_checkpoint_keeps_the_full_history() {
        for crash in [
            Crash::AfterFirstRun,
            Crash::BeforeManifestSync,
            Crash::BeforeRename,
            Crash::AfterRename,
            Crash::BeforeReclaim,
            Crash::BeforePrune,
        ] {
            let dir = scratch(&format!("durable-{crash:?}"));
            let options = EngineOptions {
                target_run_len: 1024,
                ..EngineOptions::default()
            };
            let engine = Engine::create(&dir, options, small()).unwrap();
            for round in 0..3u8 {
                for at in 0..200u32 {
                    engine.put(&at.to_be_bytes(), &[round; 20]).unwrap();
                }
                engine.flush().unwrap();
                engine
                    .durable_checkpoint(CheckpointOptions::default())
                    .unwrap();
            }
            for at in 0..100u32 {
                engine.delete(&(at * 2).to_be_bytes()).unwrap();
            }
            CRASH_AT.with(|cell| cell.set(Some(crash)));
            assert!(
                engine
                    .durable_checkpoint(CheckpointOptions::default())
                    .is_err()
            );
            CRASH_AT.with(|cell| cell.set(None));
            for at in 0..50u32 {
                engine.put(&(at * 4).to_be_bytes(), b"later").unwrap();
            }
            let expected = (state(&engine), engine.stats().last_seq);
            drop(engine);
            let (engine, report) = Engine::open(&dir, options, small()).unwrap();
            assert_eq!((state(&engine), report.last_seq), expected, "{crash:?}");
            // The next checkpoint prunes what is left behind.
            let report = engine
                .durable_checkpoint(CheckpointOptions::default())
                .unwrap();
            assert_eq!(engine.log_status().unwrap().segments, 1, "{report:?}");
            drop(engine);
            let (engine, _) = Engine::open(&dir, options, small()).unwrap();
            assert_eq!((state(&engine), engine.stats().last_seq), expected);
            drop(engine);
            fs::remove_dir_all(&dir).unwrap();
        }
    }
}
