//! Explicit, opt-in checkpoints: one snapshot of the engine written to a
//! directory as runs, and recovery of the latest committed one.
//!
//! # Contract
//!
//! [`Engine::checkpoint`] takes a [`Snapshot`](super::Snapshot) and streams
//! its live keys, in key order, into runs of the ordinary run format, one run
//! in memory at a time. A checkpoint therefore holds exactly the state at the
//! snapshot's sequence number, its **ceiling**: every key live then, with the
//! value and sequence number of its newest version. A key deleted by then is
//! absent, which is its deletion state, since nothing lies beneath a full
//! checkpoint. Writes after the ceiling are not in it; nothing makes them
//! durable until a write-ahead log exists, which would replay records with
//! sequence numbers above the ceiling.
//!
//! A checkpoint is **committed** once the file `CURRENT` durably names its
//! generation. Recovery reads exactly that checkpoint and nothing else: there
//! is no fallback to an older one. Generations increase by one for each
//! attempt and are never reused.
//!
//! # Layout
//!
//! The directory belongs to Tessera, and only these names in it are read,
//! written or removed:
//!
//! - `LOCK`: held with an exclusive advisory lock while a checkpoint or
//!   recovery runs, so that two processes do not interleave.
//! - `CURRENT`: the committed generation (20 bytes, checksummed).
//! - `CURRENT.tmp`: its replacement while one is being published.
//! - `ckpt-<generation>/`: one checkpoint, 20 decimal digits, holding
//!   `run-<index>` files (six digits) and `MANIFEST`.
//!
//! `MANIFEST` (all integers little-endian): the magic `TSRCKPT\x01`, format
//! version (u32, now 1), flags (u32, now 0; a reader rejects unknown ones),
//! generation (u64), ceiling (u64), live records (u64), run count (u32), then
//! per run its length (u64) and CRC-32 (u32), and last a CRC-32 of everything
//! before it. Runs are in ascending, disjoint key order. `CURRENT`: the magic
//! `TSRCUR\0\x01`, the generation (u64) and a CRC-32 of both.
//!
//! # Publication
//!
//! 1. Under `LOCK`, directories `ckpt-*` other than the committed one are
//!    removed: they are leftovers of interrupted attempts.
//! 2. A snapshot is taken, and runs are built and written to the new
//!    `ckpt-<generation>/`, each file synced (`fsync`) after writing. The
//!    manifest is written and synced, then the checkpoint directory and the
//!    parent are synced, so that every entry is durable. The snapshot is
//!    released.
//! 3. `CURRENT.tmp` is written and synced, renamed over `CURRENT`, and the
//!    parent is synced. **The rename is the commit point**; the sync makes it
//!    durable.
//! 4. The previous checkpoint is removed. Failing to remove it leaves it for
//!    the next checkpoint and is reported, not an error.
//!
//! An error before the rename removes the new directory and leaves the
//! previous checkpoint committed. An error after it (only the final sync) is
//! returned and removes nothing, because `CURRENT` may name either
//! checkpoint and both stay intact. A crash at any point leaves `CURRENT`
//! naming a complete checkpoint, if the assumptions below hold.
//!
//! Two guarantees differ here. **Atomic visibility**: `rename` switches
//! `CURRENT` from one complete checkpoint to the next with no state in
//! between, for any other process and after this process dies, since the
//! kernel's cache survives it. That needs only POSIX rename semantics.
//! **Durability after power loss** additionally needs every `fsync` above to
//! reach stable storage; without them a power loss could leave `CURRENT`
//! naming a directory whose files never reached the disk. The second is an
//! assumption about the filesystem and device, not something the tests can
//! show.
//!
//! The previous checkpoint is removed only after the rename and the parent's
//! sync succeed, and leftovers are removed before a new attempt only when
//! `CURRENT` names a generation that is kept. While `CURRENT` cannot be read
//! nothing is removed until a new checkpoint is committed in its place, so
//! no step deletes the only checkpoint that might be recoverable.
//!
//! `LOCK` is an advisory `flock` on an open file. The kernel releases it when
//! the file is closed, including when the process is killed, so a crash never
//! leaves the directory locked; the file itself is left and is harmless.
//!
//! # Filesystem assumptions
//!
//! Supported: a local Linux filesystem such as ext4 or XFS, where `fsync` of
//! a file makes its contents durable, `fsync` of a directory makes its
//! entries (creations and renames) durable, and `rename` replaces an entry
//! atomically. On macOS, `File::sync_all` issues `F_FULLFSYNC`; directory
//! syncs are attempted the same way, but that platform is not part of the
//! tested contract. Network filesystems, filesystems or devices that
//! acknowledge `fsync` without persisting, and concurrent writers outside
//! `LOCK` are unsupported. Checksums detect corruption; they do not repair it.
//!
//! # Resources
//!
//! A checkpoint holds one snapshot until its runs are written, so sources a
//! flush or compaction replaces meanwhile stay allocated and count toward
//! [`SnapshotLimits`](super::SnapshotLimits) as retained bytes, like any
//! snapshot's. Beside them it holds one run being built (input up to the
//! engine's `target_run_len`), one decoded block per run it reads, and no
//! copy of the database. On disk it needs the new checkpoint beside the
//! previous one until the previous is removed, and
//! [`CheckpointOptions::max_bytes`] bounds the new one.
//!
//! Recovery reads one run file at a time into memory, which is the engine's
//! storage afterwards, checks it fully and rebuilds its key filter. Nothing
//! is exposed until every file has passed.

use std::fmt;
use std::fs::{self, File, OpenOptions, TryLockError};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, PoisonError};

use crc32fast::Hasher;

use crate::block::Value;
use crate::run::{
    CorruptRun, MAX_TARGET_RUN_LEN, RECORD_INPUT_OVERHEAD, ReadError, Run, RunBuilder,
};

use super::{Core, Engine, EngineOptions, EngineWorkspace, FlushError, SnapshotError, published};

const LOCK: &str = "LOCK";
const CURRENT: &str = "CURRENT";
const CURRENT_TMP: &str = "CURRENT.tmp";
const MANIFEST: &str = "MANIFEST";
const PREFIX: &str = "ckpt-";

const CURRENT_MAGIC: [u8; 8] = *b"TSRCUR\0\x01";
const CURRENT_LEN: usize = 20;
const MANIFEST_MAGIC: [u8; 8] = *b"TSRCKPT\x01";
const MANIFEST_VERSION: u32 = 1;
/// Magic, version, flags, generation, ceiling, records and run count.
const MANIFEST_HEADER_LEN: usize = 8 + 4 + 4 + 8 + 8 + 8 + 4;
const MANIFEST_ENTRY_LEN: usize = 12;

/// Bounds on one checkpoint.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CheckpointOptions {
    /// The most bytes the new checkpoint may write: the exact lengths of its
    /// run files and manifest, checked before each file is written, so the
    /// limit is never exceeded on disk. It excludes `CURRENT`, filesystem
    /// allocation overhead and the previous checkpoint, which stays on disk
    /// beside the new one until the new one is committed.
    pub max_bytes: u64,
}

impl Default for CheckpointOptions {
    /// No limit on disk.
    fn default() -> Self {
        Self {
            max_bytes: u64::MAX,
        }
    }
}

/// What a committed checkpoint holds.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CheckpointReport {
    /// Its generation, named by `CURRENT`.
    pub generation: u64,
    /// The sequence number it covers: every write up to it, none after.
    pub last_seq: u64,
    /// Live keys written.
    pub records: u64,
    /// Run files written.
    pub runs: usize,
    /// Bytes written: run files and manifest.
    pub bytes: u64,
    /// The largest run, the most run bytes held in memory at once.
    pub largest_run_bytes: usize,
    /// Older checkpoint directories removed: the previous one and leftovers
    /// of interrupted attempts.
    pub reclaimed: usize,
    /// Older checkpoint directories that could not be removed; the next
    /// checkpoint tries again.
    pub unreclaimed: usize,
}

/// Why a checkpoint was not committed, or (for [`Self::Io`] after the commit
/// point) was committed without the final directory sync. The engine is
/// unchanged either way, and the previously committed checkpoint is intact.
#[derive(Debug)]
pub enum CheckpointError {
    /// The snapshot the checkpoint needs was refused.
    Snapshot(SnapshotError),
    /// Reading the snapshot failed.
    Read(ReadError),
    /// Another process holds the directory's `LOCK`.
    Locked(PathBuf),
    /// The checkpoint would exceed [`CheckpointOptions::max_bytes`].
    DiskLimit {
        /// Bytes the checkpoint would have reached.
        bytes: u64,
        /// The limit.
        max_bytes: u64,
    },
    /// A filesystem operation failed.
    Io {
        /// The file or directory.
        path: PathBuf,
        /// The error.
        error: io::Error,
        /// Whether `CURRENT` had already been replaced, so that the new
        /// checkpoint may be committed (if the final sync failed) and both
        /// were kept.
        after_commit_point: bool,
    },
}

impl fmt::Display for CheckpointError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Snapshot(error) => write!(f, "checkpoint snapshot refused: {error}"),
            Self::Read(error) => write!(f, "checkpoint read failed: {error}"),
            Self::Locked(path) => write!(f, "{} is locked by another process", path.display()),
            Self::DiskLimit { bytes, max_bytes } => write!(
                f,
                "checkpoint reached {bytes} bytes, over the limit of {max_bytes}"
            ),
            Self::Io {
                path,
                error,
                after_commit_point,
            } => write!(
                f,
                "checkpoint failed at {}{}: {error}",
                path.display(),
                if *after_commit_point {
                    " after replacing CURRENT"
                } else {
                    ""
                }
            ),
        }
    }
}

impl std::error::Error for CheckpointError {}

/// How stored checkpoint data failed its checks.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Damage {
    /// A file the checkpoint needs does not exist.
    Missing,
    /// The file is shorter or longer than its format or the manifest says.
    Length {
        /// Expected length.
        expected: u64,
        /// Actual length.
        found: u64,
    },
    /// The file does not start with its magic number.
    BadMagic,
    /// A manifest version or flags this build does not read.
    Unsupported {
        /// The version.
        version: u32,
        /// The flags.
        flags: u32,
    },
    /// A checksum does not match the bytes.
    Checksum,
    /// The manifest is for another generation than `CURRENT` names.
    Generation {
        /// Named by `CURRENT`.
        expected: u64,
        /// In the manifest.
        found: u64,
    },
    /// A run is not a valid run.
    Run(CorruptRun),
    /// A run is empty, or its keys do not follow the previous run's.
    Order,
    /// A record has a sequence number above the checkpoint's ceiling.
    Sequence {
        /// The record's.
        seq: u64,
        /// The ceiling.
        ceiling: u64,
    },
    /// The runs hold a different number of records than the manifest says.
    Records {
        /// The manifest's count.
        expected: u64,
        /// The runs'.
        found: u64,
    },
}

/// Why no engine was recovered. Nothing is exposed and nothing on disk is
/// changed; recovery never substitutes an empty database or an older
/// checkpoint.
#[derive(Debug)]
pub enum RecoveryError {
    /// The directory or its `CURRENT` file does not exist.
    NoCheckpoint(PathBuf),
    /// Another process holds the directory's `LOCK`.
    Locked(PathBuf),
    /// A file could not be read.
    Io {
        /// The file.
        path: PathBuf,
        /// The error.
        error: io::Error,
    },
    /// A file failed its checks.
    Corrupt {
        /// The file.
        path: PathBuf,
        /// How.
        damage: Damage,
    },
    /// The checkpoint holds more runs or bytes than the options allow.
    RunLimit(FlushError),
}

impl fmt::Display for RecoveryError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NoCheckpoint(path) => write!(f, "no checkpoint in {}", path.display()),
            Self::Locked(path) => write!(f, "{} is locked by another process", path.display()),
            Self::Io { path, error } => write!(f, "cannot read {}: {error}", path.display()),
            Self::Corrupt { path, damage } => {
                write!(
                    f,
                    "checkpoint file {} is damaged: {damage:?}",
                    path.display()
                )
            }
            Self::RunLimit(error) => write!(f, "recovered runs exceed the limits: {error}"),
        }
    }
}

impl std::error::Error for RecoveryError {}

fn checkpoint_dir(dir: &Path, generation: u64) -> PathBuf {
    dir.join(format!("{PREFIX}{generation:020}"))
}

fn run_name(index: usize) -> String {
    format!("run-{index:06}")
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

/// Opens and locks `LOCK`, which is released when the file is dropped.
fn lock(dir: &Path) -> io::Result<Result<File, ()>> {
    let file = OpenOptions::new()
        .create(true)
        .truncate(false)
        .write(true)
        .open(dir.join(LOCK))?;
    match file.try_lock() {
        Ok(()) => Ok(Ok(file)),
        Err(TryLockError::WouldBlock) => Ok(Err(())),
        Err(TryLockError::Error(error)) => Err(error),
    }
}

fn encode_current(generation: u64) -> [u8; CURRENT_LEN] {
    let mut bytes = [0; CURRENT_LEN];
    bytes[..8].copy_from_slice(&CURRENT_MAGIC);
    bytes[8..16].copy_from_slice(&generation.to_le_bytes());
    let sum = crc(&[&bytes[..16]]);
    bytes[16..].copy_from_slice(&sum.to_le_bytes());
    bytes
}

fn decode_current(bytes: &[u8]) -> Result<u64, Damage> {
    if bytes.len() != CURRENT_LEN {
        return Err(Damage::Length {
            expected: CURRENT_LEN as u64,
            found: bytes.len() as u64,
        });
    }
    if bytes[..8] != CURRENT_MAGIC {
        return Err(Damage::BadMagic);
    }
    if crc(&[&bytes[..16]]).to_le_bytes() != bytes[16..] {
        return Err(Damage::Checksum);
    }
    Ok(u64::from_le_bytes(bytes[8..16].try_into().unwrap()))
}

/// What a manifest records.
#[derive(Debug, PartialEq, Eq)]
struct Manifest {
    generation: u64,
    last_seq: u64,
    records: u64,
    /// Length and CRC-32 of each run file.
    runs: Vec<(u64, u32)>,
}

impl Manifest {
    fn encode(&self) -> Vec<u8> {
        let mut bytes =
            Vec::with_capacity(MANIFEST_HEADER_LEN + self.runs.len() * MANIFEST_ENTRY_LEN + 4);
        bytes.extend_from_slice(&MANIFEST_MAGIC);
        bytes.extend_from_slice(&MANIFEST_VERSION.to_le_bytes());
        bytes.extend_from_slice(&0u32.to_le_bytes());
        bytes.extend_from_slice(&self.generation.to_le_bytes());
        bytes.extend_from_slice(&self.last_seq.to_le_bytes());
        bytes.extend_from_slice(&self.records.to_le_bytes());
        // Runs are bounded by the engine's run limits, far below u32.
        bytes.extend_from_slice(&(self.runs.len() as u32).to_le_bytes());
        for &(len, sum) in &self.runs {
            bytes.extend_from_slice(&len.to_le_bytes());
            bytes.extend_from_slice(&sum.to_le_bytes());
        }
        let sum = crc(&[&bytes]);
        bytes.extend_from_slice(&sum.to_le_bytes());
        bytes
    }

    fn decode(bytes: &[u8]) -> Result<Self, Damage> {
        let u32_at = |at: usize| u32::from_le_bytes(bytes[at..at + 4].try_into().unwrap());
        let u64_at = |at: usize| u64::from_le_bytes(bytes[at..at + 8].try_into().unwrap());
        if bytes.len() < MANIFEST_HEADER_LEN + 4 {
            return Err(Damage::Length {
                expected: (MANIFEST_HEADER_LEN + 4) as u64,
                found: bytes.len() as u64,
            });
        }
        if bytes[..8] != MANIFEST_MAGIC {
            return Err(Damage::BadMagic);
        }
        let count = u32_at(MANIFEST_HEADER_LEN - 4) as usize;
        let expected = MANIFEST_HEADER_LEN + count * MANIFEST_ENTRY_LEN + 4;
        if bytes.len() != expected {
            return Err(Damage::Length {
                expected: expected as u64,
                found: bytes.len() as u64,
            });
        }
        let body = expected - 4;
        if crc(&[&bytes[..body]]).to_le_bytes() != bytes[body..] {
            return Err(Damage::Checksum);
        }
        let (version, flags) = (u32_at(8), u32_at(12));
        if version != MANIFEST_VERSION || flags != 0 {
            return Err(Damage::Unsupported { version, flags });
        }
        let runs = (0..count)
            .map(|index| {
                let at = MANIFEST_HEADER_LEN + index * MANIFEST_ENTRY_LEN;
                (u64_at(at), u32_at(at + 8))
            })
            .collect();
        Ok(Self {
            generation: u64_at(16),
            last_seq: u64_at(24),
            records: u64_at(32),
            runs,
        })
    }
}

/// Places where a test can stop a checkpoint as a crash would: returning at
/// once, removing nothing.
#[cfg(test)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Crash {
    AfterFirstRun,
    BeforeManifestSync,
    BeforeRename,
    AfterRename,
    BeforeReclaim,
}

#[cfg(test)]
thread_local! {
    pub(super) static CRASH_AT: std::cell::Cell<Option<Crash>> = const { std::cell::Cell::new(None) };
}

/// Whether a test asked to crash here, which also makes the caller skip all
/// cleanup.
#[cfg(test)]
fn crash(at: Crash) -> bool {
    CRASH_AT.with(|crash| crash.get() == Some(at))
}

/// Stands for the crash a test asked for.
#[cfg(test)]
fn crashed(path: &Path) -> CheckpointError {
    CheckpointError::Io {
        path: path.to_owned(),
        error: io::Error::other("crash injected by a test"),
        after_commit_point: false,
    }
}

#[cfg(test)]
macro_rules! crash_point {
    ($at:expr, $path:expr) => {
        if crash($at) {
            return Err(Failure::Crashed(crashed($path)));
        }
    };
}

#[cfg(not(test))]
macro_rules! crash_point {
    ($at:expr, $path:expr) => {};
}

/// A failure while building the new checkpoint directory.
enum Failure {
    /// Clean up and report.
    Error(CheckpointError),
    /// A test's crash: report without cleaning up.
    #[cfg(test)]
    Crashed(CheckpointError),
}

impl From<CheckpointError> for Failure {
    fn from(error: CheckpointError) -> Self {
        Self::Error(error)
    }
}

fn io_error(path: &Path) -> impl FnOnce(io::Error) -> CheckpointError + '_ {
    move |error| CheckpointError::Io {
        path: path.to_owned(),
        error,
        after_commit_point: false,
    }
}

/// Writes `bytes` to a new file at `path` and syncs it.
fn write_synced(path: &Path, bytes: &[u8]) -> Result<(), CheckpointError> {
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
        .map_err(io_error(path))?;
    file.write_all(bytes).map_err(io_error(path))?;
    file.sync_all().map_err(io_error(path))
}

/// Generations of the `ckpt-*` directories present.
fn generations(dir: &Path) -> Result<Vec<u64>, CheckpointError> {
    let mut found = Vec::new();
    for entry in fs::read_dir(dir).map_err(io_error(dir))? {
        let entry = entry.map_err(io_error(dir))?;
        let name = entry.file_name();
        let Some(digits) = name.to_str().and_then(|name| name.strip_prefix(PREFIX)) else {
            continue;
        };
        if digits.len() == 20
            && digits.bytes().all(|b| b.is_ascii_digit())
            && let Ok(generation) = digits.parse()
        {
            found.push(generation);
        }
    }
    Ok(found)
}

/// Removes every checkpoint directory but `keep`, returning how many were
/// removed and how many could not be.
fn reclaim(dir: &Path, found: &[u64], keep: u64) -> (usize, usize) {
    let (mut removed, mut failed) = (0, 0);
    for &generation in found.iter().filter(|&&generation| generation != keep) {
        match fs::remove_dir_all(checkpoint_dir(dir, generation)) {
            Ok(()) => removed += 1,
            Err(_) => failed += 1,
        }
    }
    (removed, failed)
}

struct Written {
    manifest: Manifest,
    bytes: u64,
    largest_run_bytes: usize,
}

/// Streams the snapshot's live keys into run files in `path`, then writes
/// the manifest, syncing each file and the directory.
fn write_runs(
    core: &Core,
    snapshot: &super::Snapshot,
    path: &Path,
    generation: u64,
    options: CheckpointOptions,
) -> Result<Written, Failure> {
    let target = core.target_run_len.min(MAX_TARGET_RUN_LEN);
    let mut written = Written {
        manifest: Manifest {
            generation,
            last_seq: snapshot.last_seq(),
            records: 0,
            runs: Vec::new(),
        },
        bytes: 0,
        largest_run_bytes: 0,
    };
    let emit = |run: Run<Vec<u8>>, written: &mut Written| -> Result<(), Failure> {
        let bytes = run.as_bytes();
        let total = written.bytes + bytes.len() as u64;
        if total > options.max_bytes {
            return Err(CheckpointError::DiskLimit {
                bytes: total,
                max_bytes: options.max_bytes,
            }
            .into());
        }
        let file = path.join(run_name(written.manifest.runs.len()));
        write_synced(&file, bytes)?;
        written.bytes = total;
        written.largest_run_bytes = written.largest_run_bytes.max(bytes.len());
        written
            .manifest
            .runs
            .push((bytes.len() as u64, crc(&[bytes])));
        crash_point!(Crash::AfterFirstRun, &file);
        Ok(())
    };

    let mut workspace = EngineWorkspace::new();
    let mut scan = snapshot.scan(&mut workspace);
    let mut builder: Option<RunBuilder> = None;
    let mut input = 0;
    while let Some(record) = scan.next_record().map_err(CheckpointError::Read)? {
        let open = builder.get_or_insert_with(RunBuilder::default);
        // Keys and values came from runs and memtables that held them.
        open.add(record.key, record.seq, Value::Bytes(record.value))
            .expect("live records fit a run");
        written.manifest.records += 1;
        input += record.key.len() + record.value.len() + RECORD_INPUT_OVERHEAD;
        if input >= target {
            emit(builder.take().unwrap().finish(), &mut written)?;
            input = 0;
        }
    }
    drop(scan);
    if let Some(open) = builder.take() {
        emit(open.finish(), &mut written)?;
    }

    let manifest = written.manifest.encode();
    let total = written.bytes + manifest.len() as u64;
    if total > options.max_bytes {
        return Err(CheckpointError::DiskLimit {
            bytes: total,
            max_bytes: options.max_bytes,
        }
        .into());
    }
    let file = path.join(MANIFEST);
    write_synced(&file, &manifest)?;
    written.bytes = total;
    crash_point!(Crash::BeforeManifestSync, &file);
    sync_dir(path).map_err(io_error(path))?;
    let parent = path.parent().expect("a checkpoint directory has a parent");
    sync_dir(parent).map_err(io_error(parent))?;
    Ok(written)
}

impl Engine {
    /// Writes the current state to `dir` as a new checkpoint and commits it,
    /// then removes older ones. The contract, layout and filesystem
    /// assumptions are documented at the top of `src/engine/checkpoint.rs`.
    ///
    /// Writes continue throughout: the checkpoint holds the state at the
    /// snapshot it takes, and holds the state lock only to take it. Flushes
    /// and compactions also continue; sources they replace stay allocated
    /// until the runs are written. Checkpoints of one engine run one at a
    /// time. On any error before the commit point the new files are removed
    /// and the previous checkpoint stays committed.
    pub fn checkpoint(
        &self,
        dir: &Path,
        options: CheckpointOptions,
    ) -> Result<CheckpointReport, CheckpointError> {
        // This mutex guards no data.
        let _serial = self
            .core
            .checkpointing
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        fs::create_dir_all(dir).map_err(io_error(dir))?;
        let _lock = match lock(dir).map_err(io_error(dir))? {
            Ok(file) => file,
            Err(()) => return Err(CheckpointError::Locked(dir.to_owned())),
        };
        // None if there is no CURRENT, Some(None) if it cannot be read.
        let committed = match fs::read(dir.join(CURRENT)) {
            Ok(bytes) => Some(decode_current(&bytes).ok()),
            Err(error) if error.kind() == io::ErrorKind::NotFound => None,
            Err(_) => Some(None),
        };
        let found = generations(dir)?;
        // Leftovers of interrupted attempts, removed first so that disk holds
        // at most the committed checkpoint and the new one. While CURRENT
        // exists but cannot be read, nothing is removed before the commit.
        let (mut reclaimed, mut unreclaimed) = match committed {
            None => reclaim(dir, &found, u64::MAX),
            Some(Some(generation)) => reclaim(dir, &found, generation),
            Some(None) => (0, 0),
        };
        let generation = found
            .iter()
            .copied()
            .chain(committed.flatten())
            .max()
            .map_or(1, |latest| latest + 1);
        let _ = fs::remove_file(dir.join(CURRENT_TMP));

        let path = checkpoint_dir(dir, generation);
        fs::create_dir(&path).map_err(io_error(&path))?;
        let snapshot = self.core.snapshot().map_err(CheckpointError::Snapshot);
        let built = snapshot
            .map_err(Failure::Error)
            .and_then(|snapshot| write_runs(&self.core, &snapshot, &path, generation, options));
        let written = match built {
            Ok(written) => written,
            Err(Failure::Error(error)) => {
                let _ = fs::remove_dir_all(&path);
                return Err(error);
            }
            #[cfg(test)]
            Err(Failure::Crashed(error)) => return Err(error),
        };

        // Publication: CURRENT.tmp, then the rename that commits.
        let tmp = dir.join(CURRENT_TMP);
        let current = encode_current(generation);
        #[cfg(test)]
        if crash(Crash::BeforeRename) {
            write_synced(&tmp, &current)?;
            return Err(crashed(&tmp));
        }
        let published = write_synced(&tmp, &current)
            .and_then(|()| fs::rename(&tmp, dir.join(CURRENT)).map_err(io_error(dir)));
        if let Err(error) = published {
            let _ = fs::remove_file(&tmp);
            let _ = fs::remove_dir_all(&path);
            return Err(error);
        }
        #[cfg(test)]
        if crash(Crash::AfterRename) {
            return Err(crashed(dir));
        }
        if let Err(error) = sync_dir(dir) {
            return Err(CheckpointError::Io {
                path: dir.to_owned(),
                error,
                after_commit_point: true,
            });
        }
        #[cfg(test)]
        if crash(Crash::BeforeReclaim) {
            return Err(crashed(dir));
        }
        let (removed, failed) = reclaim(dir, &found, generation);
        reclaimed += removed;
        unreclaimed += failed;
        // Removal need not be durable: a directory that reappears after a
        // crash is a leftover the next checkpoint removes.
        let _ = sync_dir(dir);
        let Written {
            manifest,
            bytes,
            largest_run_bytes,
        } = written;
        Ok(CheckpointReport {
            generation,
            last_seq: manifest.last_seq,
            records: manifest.records,
            runs: manifest.runs.len(),
            bytes,
            largest_run_bytes,
            reclaimed,
            unreclaimed,
        })
    }

    /// Recovers the checkpoint that `dir`'s `CURRENT` names into a new engine
    /// with `options`, whose sequence numbers continue after the larger of the
    /// checkpoint's ceiling and [`EngineOptions::last_seq`].
    ///
    /// Every file is read and checked (lengths, checksums, the full run
    /// check, key order across runs and sequence numbers against the
    /// ceiling) and each run's key filter rebuilt before the engine exists.
    /// Nothing on disk is changed. Any failure is an error: recovery never
    /// returns an empty engine for a missing or damaged checkpoint, nor falls
    /// back to an older one.
    pub fn recover(dir: &Path, options: EngineOptions) -> Result<Engine, RecoveryError> {
        let read_error = |path: &Path| {
            let path = path.to_owned();
            move |error: io::Error| match error.kind() {
                io::ErrorKind::NotFound => RecoveryError::Corrupt {
                    path,
                    damage: Damage::Missing,
                },
                _ => RecoveryError::Io { path, error },
            }
        };
        let current = dir.join(CURRENT);
        // Checked before locking, which would create LOCK.
        match fs::metadata(&current) {
            Ok(_) => {}
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                return Err(RecoveryError::NoCheckpoint(dir.to_owned()));
            }
            Err(error) => {
                return Err(RecoveryError::Io {
                    path: current,
                    error,
                });
            }
        }
        let _lock = match lock(dir).map_err(read_error(dir))? {
            Ok(file) => file,
            Err(()) => return Err(RecoveryError::Locked(dir.to_owned())),
        };
        let bytes = fs::read(&current).map_err(read_error(&current))?;
        let corrupt = |path: &Path, damage| RecoveryError::Corrupt {
            path: path.to_owned(),
            damage,
        };
        let generation = decode_current(&bytes).map_err(|damage| corrupt(&current, damage))?;
        let path = checkpoint_dir(dir, generation);
        let manifest_path = path.join(MANIFEST);
        let bytes = fs::read(&manifest_path).map_err(read_error(&manifest_path))?;
        let manifest =
            Manifest::decode(&bytes).map_err(|damage| corrupt(&manifest_path, damage))?;
        if manifest.generation != generation {
            return Err(corrupt(
                &manifest_path,
                Damage::Generation {
                    expected: generation,
                    found: manifest.generation,
                },
            ));
        }

        // The manifest's lengths bound what reading the runs allocates; refuse
        // before reading any. Filters are added to the bytes once rebuilt.
        let listed: u64 = manifest.runs.iter().map(|&(len, _)| len).sum();
        if manifest.runs.len() > options.max_runs || listed > options.max_run_bytes as u64 {
            return Err(RecoveryError::RunLimit(FlushError {
                runs: manifest.runs.len(),
                max_runs: options.max_runs,
                run_bytes: usize::try_from(listed).unwrap_or(usize::MAX),
                max_run_bytes: options.max_run_bytes,
            }));
        }
        let mut runs: Vec<Run<Vec<u8>>> = Vec::with_capacity(manifest.runs.len());
        let mut records = 0u64;
        for (index, &(len, sum)) in manifest.runs.iter().enumerate() {
            let file = path.join(run_name(index));
            let data = fs::read(&file).map_err(read_error(&file))?;
            if data.len() as u64 != len {
                return Err(corrupt(
                    &file,
                    Damage::Length {
                        expected: len,
                        found: data.len() as u64,
                    },
                ));
            }
            if crc(&[&data]) != sum {
                return Err(corrupt(&file, Damage::Checksum));
            }
            let (run, max_seq) =
                Run::recover(data).map_err(|error| corrupt(&file, Damage::Run(error)))?;
            if max_seq > manifest.last_seq {
                return Err(corrupt(
                    &file,
                    Damage::Sequence {
                        seq: max_seq,
                        ceiling: manifest.last_seq,
                    },
                ));
            }
            let range = run
                .key_range()
                .map_err(|error| corrupt(&file, Damage::Run(error)))?;
            let follows = match (range, runs.last()) {
                (None, _) => false,
                (Some(_), None) => true,
                (Some(range), Some(previous)) => previous
                    .key_range()
                    .ok()
                    .flatten()
                    .is_some_and(|previous| previous.max < range.min),
            };
            if !follows {
                return Err(corrupt(&file, Damage::Order));
            }
            records += run.len() as u64;
            runs.push(run);
        }
        if records != manifest.records {
            return Err(corrupt(
                &manifest_path,
                Damage::Records {
                    expected: manifest.records,
                    found: records,
                },
            ));
        }
        let run_bytes: usize = runs.iter().map(|run| run.as_bytes().len()).sum();
        let filter_bytes: usize = runs.iter().map(Run::filter_bytes).sum();
        if runs.len() > options.max_runs || run_bytes + filter_bytes > options.max_run_bytes {
            return Err(RecoveryError::RunLimit(FlushError {
                runs: runs.len(),
                max_runs: options.max_runs,
                run_bytes: run_bytes + filter_bytes,
                max_run_bytes: options.max_run_bytes,
            }));
        }

        let options = EngineOptions {
            last_seq: options.last_seq.max(manifest.last_seq),
            ..options
        };
        let core = Arc::new(Core::new(&options));
        {
            let mut state = core.write_state();
            state.runs.extend(published(runs));
            state.run_bytes = run_bytes;
            state.filter_bytes = filter_bytes;
        }
        Ok(Engine::start(core, &options))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn manifest_and_current_round_trip_and_reject_damage() {
        let manifest = Manifest {
            generation: 7,
            last_seq: 1 << 40,
            records: 12,
            runs: vec![(100, 0xdead_beef), (5, 1)],
        };
        let bytes = manifest.encode();
        assert_eq!(Manifest::decode(&bytes).unwrap(), manifest);
        for at in [0, 9, 13, 20, bytes.len() - 1] {
            let mut damaged = bytes.clone();
            damaged[at] ^= 1;
            assert!(Manifest::decode(&damaged).is_err(), "byte {at}");
        }
        assert!(matches!(
            Manifest::decode(&bytes[..bytes.len() - 1]),
            Err(Damage::Length { .. })
        ));
        let current = encode_current(9);
        assert_eq!(decode_current(&current), Ok(9));
        let mut damaged = current;
        damaged[10] ^= 1;
        assert_eq!(decode_current(&damaged), Err(Damage::Checksum));
    }

    fn scratch(name: &str) -> PathBuf {
        let path =
            std::env::temp_dir().join(format!("tessera-crash-{name}-{}", std::process::id()));
        let _ = fs::remove_dir_all(&path);
        path
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

    fn entries(dir: &Path) -> Vec<String> {
        let mut names: Vec<String> = fs::read_dir(dir)
            .unwrap()
            .map(|entry| entry.unwrap().file_name().into_string().unwrap())
            .filter(|name| name != LOCK)
            .collect();
        names.sort();
        names
    }

    /// A crash at each boundary leaves CURRENT naming a complete checkpoint:
    /// the previous one before the rename, the new one after. The next
    /// checkpoint removes what the crash left and commits normally.
    #[test]
    fn a_crash_at_each_boundary_leaves_one_recoverable_checkpoint() {
        for (crash, committed) in [
            (Crash::AfterFirstRun, false),
            (Crash::BeforeManifestSync, false),
            (Crash::BeforeRename, false),
            (Crash::AfterRename, true),
            (Crash::BeforeReclaim, true),
        ] {
            let dir = scratch(&format!("{crash:?}"));
            let engine = Engine::new(EngineOptions {
                target_run_len: 1024,
                ..EngineOptions::default()
            });
            for i in 0..500u32 {
                engine.put(&i.to_be_bytes(), b"old").unwrap();
            }
            engine.flush().unwrap();
            engine
                .checkpoint(&dir, CheckpointOptions::default())
                .unwrap();
            let old = state(&engine);
            for i in 0..500u32 {
                engine.put(&i.to_be_bytes(), b"new").unwrap();
            }
            engine.delete(&7u32.to_be_bytes()).unwrap();
            let new = state(&engine);

            CRASH_AT.with(|at| at.set(Some(crash)));
            let error = engine.checkpoint(&dir, CheckpointOptions::default());
            CRASH_AT.with(|at| at.set(None));
            assert!(error.is_err(), "{crash:?}");
            let recovered = Engine::recover(&dir, EngineOptions::default()).unwrap();
            let expected = if committed { &new } else { &old };
            assert_eq!(&state(&recovered), expected, "{crash:?}");
            drop(recovered);

            let report = engine
                .checkpoint(&dir, CheckpointOptions::default())
                .unwrap();
            assert_eq!(report.generation, 3, "{crash:?}");
            assert_eq!(report.reclaimed, 2, "{crash:?}");
            assert_eq!(
                entries(&dir),
                [CURRENT.to_owned(), format!("{PREFIX}{:020}", 3)],
                "{crash:?}"
            );
            let recovered = Engine::recover(&dir, EngineOptions::default()).unwrap();
            assert_eq!(state(&recovered), new, "{crash:?}");
            fs::remove_dir_all(&dir).unwrap();
        }
    }

    /// A second process's lock refuses both a checkpoint and a recovery.
    #[test]
    fn the_lock_keeps_a_second_user_out() {
        let dir = scratch("lock");
        let engine = Engine::new(EngineOptions::default());
        engine.put(b"a", b"1").unwrap();
        engine
            .checkpoint(&dir, CheckpointOptions::default())
            .unwrap();
        // A lock taken through another open file description conflicts like
        // another process's.
        let held = lock(&dir).unwrap().unwrap();
        assert!(matches!(
            engine.checkpoint(&dir, CheckpointOptions::default()),
            Err(CheckpointError::Locked(_))
        ));
        assert!(matches!(
            Engine::recover(&dir, EngineOptions::default()),
            Err(RecoveryError::Locked(_))
        ));
        drop(held);
        Engine::recover(&dir, EngineOptions::default()).unwrap();
        fs::remove_dir_all(&dir).unwrap();
    }
}
