//! Incremental checkpoints of immutable runs, with no re-encoding or recompression.
//!
//! At flush rotation a checkpoint pins the existing runs and the tables being
//! flushed. The normal flush builder publishes those tables, completing the
//! structural snapshot at that rotation's sequence ceiling. Later writes and
//! compactions proceed while files are written, outside the state lock.
//!
//! Layout: `runs/<32-hex-digit RunId>` holds the exact v6 run chunks, followed
//! by little-endian Bloom words and their CRC-32. The manifest records both
//! lengths and a CRC-32 of the entire file. Filters use the fixed hash defined
//! in `run/filter.rs`; recovery validates runs and filter membership, then
//! checks copies across runs, without rebuilding filters.
//! `ckpt-<20-digit generation>/MANIFEST` records levels, level-0 flush groups,
//! effective sequence overrides, ceiling, next ID counter and application data.
//!
//! Publication uses the LOCK/CURRENT protocol: sync every new run file,
//! its directory, the manifest, its directory and the parent; write and sync
//! CURRENT.tmp, rename over CURRENT (the commit point), then sync the parent.
//! Only after that sync does GC remove files outside the union of the new and
//! previous manifests. Before rename an error leaves the old commit intact;
//! after rename a sync error retains both generations. Atomic temporary run
//! files prevent an interrupted write from becoming a reusable final file.
//!
//! LOCK is an advisory exclusive file lock, released on close or process exit.
//! Recovery reads exactly CURRENT, with no fallback, and leaves checkpoint data
//! unchanged. It may create LOCK when the lock file is absent.
//! Durability assumes local POSIX rename and working file/directory fsync
//! (Linux ext4/XFS); network filesystems and external writers are unsupported.
//! Checksums detect damage rather than repairing it. CURRENT visibility after
//! process death is tested; power-loss durability depends on the filesystem.
//!
//! Run IDs combine a random process namespace and a monotonic counter. The
//! counter is persisted and advanced on recovery; a fresh namespace after a
//! restart also prevents collisions with abandoned or forked recovery branches.
//! File names remain unchanged for recovered runs. A checkpoint retains one
//! structural snapshot, but builds no additional copy of an existing run.
//! Recovery temporarily holds one contiguous file while packing its frames into
//! pool chunks. Application metadata is bounded at 16 MiB.

use crate::chunk::{ChunkPool, RESERVE_BYTES};

use std::fmt;
use std::fs::{self, File, OpenOptions, TryLockError};
use std::io::{self, BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, PoisonError};

use crc32fast::Hasher;

use crate::run::{CorruptRun, Run};
use std::collections::HashSet;
use std::hash::{BuildHasher, RandomState};
use std::sync::OnceLock;

use super::levels::Levels;
use super::{Core, Engine, EngineOptions, FlushJob, Published, SharedRun};

pub(super) const LOCK: &str = "LOCK";
const CURRENT: &str = "CURRENT";
const CURRENT_TMP: &str = "CURRENT.tmp";
const MANIFEST: &str = "MANIFEST";
const PREFIX: &str = "ckpt-";

const CURRENT_MAGIC: [u8; 8] = *b"TSRCUR\0\x01";
const CURRENT_LEN: usize = 20;
const MANIFEST_MAGIC: [u8; 8] = *b"TSRCKPT\x01";
// Manifest version 4 specifies aligned 512-bit Bloom blocks in run files.
// Run payloads use format version 6.
const MANIFEST_VERSION: u32 = 4;
// Magic/version/flags, generation/ceiling/records/next-id/previous,
// depth, metadata length, run count. Entries include both file and run lengths.
const MANIFEST_HEADER_LEN: usize = 72;
const MANIFEST_ENTRY_LEN: usize = 60;
const MAX_METADATA_BYTES: usize = 16 << 20;
const RUNS: &str = "runs";
// Keep namespace and counter together, including when recovery advances the
// counter to the end of its range. Rollover starts a fresh namespace.
static RUN_IDS: Mutex<(Option<u64>, u64)> = Mutex::new((None, 1));

pub(super) fn new_run_id() -> u128 {
    let mut ids = RUN_IDS.lock().unwrap_or_else(PoisonError::into_inner);
    if ids.1 == u64::MAX {
        ids.0 = None;
        ids.1 = 1;
    }
    let namespace = *ids
        .0
        .get_or_insert_with(|| RandomState::new().hash_one(std::process::id()));
    let number = ids.1;
    ids.1 += 1;
    (u128::from(namespace) << 64) | u128::from(number)
}

/// Bounds on one checkpoint.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CheckpointOptions {
    /// Maximum new run-file and manifest bytes written in this attempt.
    /// Reused files, CURRENT and filesystem overhead are excluded. Older
    /// generations remain until the new commit is durable.
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

/// What a committed checkpoint holds and the I/O performed for it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CheckpointReport {
    /// Its generation, named by CURRENT.
    pub generation: u64,
    /// Every update through this ceiling is included; later updates are excluded.
    pub last_seq: u64,
    /// Physical records, including retained versions and tombstones.
    pub records: u64,
    /// Total referenced run files.
    pub runs: usize,
    /// New run-file and manifest bytes, excluding CURRENT.
    pub bytes_written: u64,
    /// Existing run-file bytes skipped, including filters.
    pub bytes_reused: u64,
    /// New run files written.
    pub runs_written: usize,
    /// Existing run files skipped.
    pub runs_reused: usize,
    /// Obsolete run files (including interrupted temporary files) deleted.
    pub files_deleted: usize,
    /// Alias of bytes_written.
    pub bytes: u64,
    /// Largest referenced run file, including its filter.
    pub largest_run_bytes: usize,
    /// Obsolete generation directories removed.
    pub reclaimed: usize,
    /// File or directory removals that failed; retried after the next commit.
    pub unreclaimed: usize,
}

/// Why a checkpoint was not committed, or (for [`Self::Io`] after the commit
/// point) was committed without the final directory sync. The engine is
/// still readable either way; its memtables may have flushed. The preceding
/// committed generation is retained.
#[derive(Debug)]
pub enum CheckpointError {
    /// Application metadata exceeds the 16 MiB format bound.
    MetadataLimit,
    /// Another operation or process holds the directory's `LOCK`.
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
            Self::MetadataLimit => f.write_str("checkpoint metadata exceeds 16 MiB"),
            Self::Locked(path) => write!(
                f,
                "{} is locked by another engine or process",
                path.display()
            ),
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
    /// A persisted Bloom filter failed its independent checksum.
    FilterChecksum,
    /// The manifest is for another generation than `CURRENT` names.
    Generation {
        /// Named by `CURRENT`.
        expected: u64,
        /// In the manifest.
        found: u64,
    },
    /// A run is not a valid run.
    Run(CorruptRun),
    /// Run IDs, levels, flush groups, level ordering or copies of one update disagree.
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

/// Why [`Engine::new`] or [`Engine::recover`] returned no engine. Nothing is
/// exposed, no worker is left running and any directory lock is released.
/// Recovery leaves checkpoint data unchanged and may create `LOCK`. It never
/// substitutes an empty database or a different checkpoint. [`Engine::new`] fails only with [`Self::Config`]
/// or [`Self::Worker`].
#[derive(Debug)]
pub enum OpenError {
    /// The directory or its `CURRENT` file does not exist.
    NoCheckpoint(PathBuf),
    /// Another operation or process holds the directory's `LOCK`.
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
    /// The engine options were refused before anything was started or
    /// the directory touched.
    Config(super::ConfigError),
    /// The operating system could not create the background compaction
    /// worker's thread. Recovery changed nothing on disk.
    Worker(io::Error),
}

impl fmt::Display for OpenError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NoCheckpoint(path) => write!(f, "no checkpoint in {}", path.display()),
            Self::Locked(path) => write!(
                f,
                "{} is locked by another engine or process",
                path.display()
            ),
            Self::Io { path, error } => write!(f, "cannot read {}: {error}", path.display()),
            Self::Corrupt { path, damage } => {
                write!(
                    f,
                    "checkpoint file {} is damaged: {damage:?}",
                    path.display()
                )
            }
            Self::Config(error) => error.fmt(f),
            Self::Worker(error) => {
                write!(f, "cannot start the background compaction worker: {error}")
            }
        }
    }
}

impl std::error::Error for OpenError {}

fn checkpoint_dir(dir: &Path, generation: u64) -> PathBuf {
    dir.join(format!("{PREFIX}{generation:020}"))
}

fn run_name(id: u128) -> String {
    format!("{id:032x}")
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
pub(super) fn lock(dir: &Path) -> io::Result<Result<File, ()>> {
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

#[derive(Clone, Debug, PartialEq, Eq)]
struct RunEntry {
    id: u128,
    level: usize,
    flush: u64,
    seq_override: Option<u64>,
    len: u64,
    run_len: u64,
    sum: u32,
}

#[derive(Debug, PartialEq, Eq)]
struct Manifest {
    generation: u64,
    last_seq: u64,
    records: u64,
    next_run_id: u64,
    previous: u64,
    depth: usize,
    metadata: Vec<u8>,
    runs: Vec<RunEntry>,
}

impl Manifest {
    fn encode(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(
            MANIFEST_HEADER_LEN + self.runs.len() * MANIFEST_ENTRY_LEN + self.metadata.len() + 4,
        );
        bytes.extend_from_slice(&MANIFEST_MAGIC);
        bytes.extend_from_slice(&MANIFEST_VERSION.to_le_bytes());
        bytes.extend_from_slice(&0u32.to_le_bytes());
        for n in [
            self.generation,
            self.last_seq,
            self.records,
            self.next_run_id,
            self.previous,
        ] {
            bytes.extend_from_slice(&n.to_le_bytes());
        }
        bytes.extend_from_slice(&(self.depth as u32).to_le_bytes());
        bytes.extend_from_slice(&(self.metadata.len() as u64).to_le_bytes());
        bytes.extend_from_slice(&(self.runs.len() as u32).to_le_bytes());
        for run in &self.runs {
            bytes.extend_from_slice(&run.id.to_le_bytes());
            bytes.extend_from_slice(&(run.level as u32).to_le_bytes());
            bytes.extend_from_slice(&u32::from(run.seq_override.is_some()).to_le_bytes());
            bytes.extend_from_slice(&run.seq_override.unwrap_or(0).to_le_bytes());
            bytes.extend_from_slice(&run.flush.to_le_bytes());
            bytes.extend_from_slice(&run.len.to_le_bytes());
            bytes.extend_from_slice(&run.run_len.to_le_bytes());
            bytes.extend_from_slice(&run.sum.to_le_bytes());
        }
        bytes.extend_from_slice(&self.metadata);
        let sum = crc(&[&bytes]);
        bytes.extend_from_slice(&sum.to_le_bytes());
        bytes
    }

    fn decode(bytes: &[u8]) -> Result<Self, Damage> {
        let u32_at = |at: usize| u32::from_le_bytes(bytes[at..at + 4].try_into().unwrap());
        let u64_at = |at: usize| u64::from_le_bytes(bytes[at..at + 8].try_into().unwrap());
        let length = |expected| Damage::Length {
            expected,
            found: bytes.len() as u64,
        };
        if bytes.len() < MANIFEST_HEADER_LEN + 4 {
            return Err(length((MANIFEST_HEADER_LEN + 4) as u64));
        }
        if bytes[..8] != MANIFEST_MAGIC {
            return Err(Damage::BadMagic);
        }
        let (version, flags) = (u32_at(8), u32_at(12));
        if version != MANIFEST_VERSION || flags != 0 {
            return Err(Damage::Unsupported { version, flags });
        }
        let count = u32_at(68) as usize;
        let metadata_len = u64_at(60);
        if metadata_len > MAX_METADATA_BYTES as u64 {
            return Err(length(
                (MANIFEST_HEADER_LEN + MAX_METADATA_BYTES + 4) as u64,
            ));
        }
        let expected = MANIFEST_HEADER_LEN as u64
            + count as u64 * MANIFEST_ENTRY_LEN as u64
            + metadata_len
            + 4;
        if bytes.len() as u64 != expected {
            return Err(length(expected));
        }
        let body = bytes.len() - 4;
        if crc(&[&bytes[..body]]).to_le_bytes() != bytes[body..] {
            return Err(Damage::Checksum);
        }
        let depth = u32_at(56) as usize;
        let next_run_id = u64_at(40);
        if depth == 0
            || depth > count.saturating_add(64)
            || next_run_id == 0
            || u64_at(48) >= u64_at(16)
        {
            return Err(Damage::Order);
        }
        let mut runs = Vec::with_capacity(count);
        let mut ids = HashSet::with_capacity(count);
        let mut previous = (0, 0);
        for index in 0..count {
            let at = MANIFEST_HEADER_LEN + index * MANIFEST_ENTRY_LEN;
            let id = u128::from_le_bytes(bytes[at..at + 16].try_into().unwrap());
            let level = u32_at(at + 16) as usize;
            let flags = u32_at(at + 20);
            let seq = u64_at(at + 24);
            let flush = u64_at(at + 32);
            let len = u64_at(at + 40);
            let run_len = u64_at(at + 48);
            if run_len > u32::MAX as u64 {
                return Err(Damage::Length {
                    expected: u32::MAX as u64,
                    found: run_len,
                });
            }
            let max_len = run_len + crate::run::MAX_FILTER_BYTES as u64 + 4;
            if len > max_len {
                return Err(Damage::Length {
                    expected: max_len,
                    found: len,
                });
            }
            if !ids.insert(id)
                || id as u64 == 0
                || id as u64 >= next_run_id
                || level > depth
                || flags > 1
                || (flags == 0 && seq != 0)
                || (level > 0 && flush != 0)
                || (level, flush) < previous
                || flush == u64::MAX
                || run_len > len.saturating_sub(4)
                || len < 4
            {
                return Err(Damage::Order);
            }
            previous = (level, flush);
            runs.push(RunEntry {
                id,
                level,
                flush,
                seq_override: (flags == 1).then_some(seq),
                len,
                run_len,
                sum: u32_at(at + 56),
            });
        }
        let metadata_at = MANIFEST_HEADER_LEN + count * MANIFEST_ENTRY_LEN;
        Ok(Self {
            generation: u64_at(16),
            last_seq: u64_at(24),
            records: u64_at(32),
            next_run_id,
            previous: u64_at(48),
            depth,
            metadata: bytes[metadata_at..body].to_vec(),
            runs,
        })
    }
}

/// Places where a test can stop a checkpoint as a crash would: returning at
/// once, removing nothing.
#[cfg(test)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Crash {
    PartialRun,
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
pub(super) fn crash(at: Crash) -> bool {
    CRASH_AT.with(|crash| crash.get() == Some(at))
}

/// Stands for the crash a test asked for.
#[cfg(test)]
pub(super) fn crashed(path: &Path) -> CheckpointError {
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

/// GC runs only after the new CURRENT and its parent are durable. The previous
/// committed manifest is retained even if abandoned attempts lie between it
/// and the new generation.
fn reclaim(
    dir: &Path,
    found: &[u64],
    manifest: &Manifest,
    previous: Option<&Manifest>,
) -> (usize, usize, usize) {
    let keep: HashSet<_> = manifest
        .runs
        .iter()
        .chain(previous.into_iter().flat_map(|m| &m.runs))
        .map(|r| run_name(r.id))
        .collect();
    let (mut files, mut directories, mut failed) = (0, 0, 0);
    if let Ok(entries) = fs::read_dir(dir.join(RUNS)) {
        for entry in entries {
            let Ok(entry) = entry else {
                failed += 1;
                continue;
            };
            let name = entry.file_name();
            let Some(name) = name.to_str() else { continue };
            let digits = name.strip_suffix(".tmp").unwrap_or(name);
            if digits.len() == 32
                && digits.bytes().all(|b| b.is_ascii_hexdigit())
                && !keep.contains(name)
            {
                match fs::remove_file(entry.path()) {
                    Ok(()) => files += 1,
                    Err(_) => failed += 1,
                }
            }
        }
    } else {
        failed += 1;
    }
    for &generation in found {
        if generation != manifest.generation && generation != manifest.previous {
            match fs::remove_dir_all(checkpoint_dir(dir, generation)) {
                Ok(()) => directories += 1,
                Err(_) => failed += 1,
            }
        }
    }
    (files, directories, failed)
}

struct Image {
    runs: Vec<(usize, u64, SharedRun)>,
    ceiling: u64,
    depth: usize,
    _live: Arc<()>,
}

impl Core {
    /// Use the normal flush job, keeping precisely the run image at rotation.
    /// Taking a later snapshot of the state would include runs published by
    /// concurrent ingestion, or miss pre-ceiling runs replaced by compaction.
    fn checkpoint_image(&self) -> Result<Image, CheckpointError> {
        loop {
            let serial = self.serialize_flush();
            let mut state = self.write_state();
            // Respect the ordinary frozen-table bound before rotating active.
            // This can occur while the background flush is building.
            if !state.active.is_empty() && state.frozen.len() >= self.max_frozen_memtables {
                let job = FlushJob {
                    engine: self,
                    _serial: serial,
                    tables: state.frozen.clone(),
                    cuts: state.flush_cuts(self.target_run_len),
                };
                drop(state);
                job.build().publish();
                continue;
            }
            let mut image = Image {
                runs: state.levels.checkpoint_runs(),
                ceiling: state.last_seq,
                depth: state.levels.depth(),
                _live: Arc::clone(&self.snapshots),
            };
            let flush = state.levels.next_flush();
            if !state.active.is_empty() {
                self.freeze_active(&mut state);
            }
            if state.frozen.is_empty() {
                return Ok(image);
            }
            let job = FlushJob {
                engine: self,
                _serial: serial,
                tables: state.frozen.clone(),
                cuts: state.flush_cuts(self.target_run_len),
            };
            drop(state);
            #[cfg(test)]
            AFTER_ROTATION.with(|hook| {
                if let Some(hook) = hook.borrow_mut().take() {
                    hook();
                }
            });
            let (_, runs) = job.build().publish_kept();
            image
                .runs
                .extend(runs.into_iter().map(|run| (0, flush, run)));
            image.runs.sort_by_key(|(level, _, _)| *level);
            return Ok(image);
        }
    }
}

/// Visit the exact file bytes without allocating a contiguous serialization.
fn run_parts(run: &Run<Vec<u8>>, mut part: impl FnMut(&[u8]) -> io::Result<()>) -> io::Result<()> {
    for bytes in run.chunks() {
        part(bytes)?;
    }
    let mut filter_sum = Hasher::new();
    for word in run.filter_words() {
        filter_sum.update(&word);
        part(&word)?;
    }
    part(&filter_sum.finalize().to_le_bytes())
}

struct Written {
    manifest: Manifest,
    bytes: u64,
    bytes_reused: u64,
    runs_written: usize,
    runs_reused: usize,
    files_deleted: usize,
    largest_run_bytes: usize,
}

fn write_runs(
    image: &Image,
    dir: &Path,
    generation: u64,
    previous: u64,
    options: CheckpointOptions,
    metadata: &[u8],
) -> Result<Written, Failure> {
    let runs_dir = dir.join(RUNS);
    fs::create_dir_all(&runs_dir).map_err(io_error(&runs_dir))?;
    let mut written = Written {
        manifest: Manifest {
            generation,
            last_seq: image.ceiling,
            records: image.runs.iter().map(|(_, _, r)| r.run.len() as u64).sum(),
            next_run_id: image
                .runs
                .iter()
                .map(|(_, _, run)| (run.id as u64) + 1)
                .chain(std::iter::once(
                    RUN_IDS.lock().unwrap_or_else(PoisonError::into_inner).1,
                ))
                .max()
                .unwrap(),
            previous,
            depth: image.depth,
            metadata: metadata.to_vec(),
            runs: Vec::with_capacity(image.runs.len()),
        },
        bytes: 0,
        bytes_reused: 0,
        runs_written: 0,
        runs_reused: 0,
        files_deleted: 0,
        largest_run_bytes: 0,
    };
    for &(level, flush, ref published) in &image.runs {
        let run = &published.run;
        let len = (run.stored_len() + run.filter_bytes() + 4) as u64;
        let file = runs_dir.join(run_name(published.id));
        let exists = match fs::metadata(&file) {
            Ok(found) if found.len() == len => true,
            Ok(_) => {
                return Err(io_error(&file)(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "existing run file has the wrong length",
                ))
                .into());
            }
            Err(e) if e.kind() == io::ErrorKind::NotFound => false,
            Err(e) => return Err(io_error(&file)(e).into()),
        };
        if !exists && written.bytes.saturating_add(len) > options.max_bytes {
            return Err(CheckpointError::DiskLimit {
                bytes: written.bytes.saturating_add(len),
                max_bytes: options.max_bytes,
            }
            .into());
        }
        let sum = if exists {
            written.bytes_reused += len;
            written.runs_reused += 1;
            *published.checkpoint_crc.get_or_init(|| {
                let mut sum = Hasher::new();
                run_parts(run, |bytes| {
                    sum.update(bytes);
                    Ok(())
                })
                .expect("hashing cannot fail");
                sum.finalize()
            })
        } else {
            let tmp = runs_dir.join(format!("{}.tmp", run_name(published.id)));
            // An interrupted temporary file is never a reusable final file.
            match fs::remove_file(&tmp) {
                Ok(()) => written.files_deleted += 1,
                Err(e) if e.kind() == io::ErrorKind::NotFound => {}
                Err(e) => return Err(io_error(&tmp)(e).into()),
            }
            let mut output = BufWriter::new(File::create_new(&tmp).map_err(io_error(&tmp))?);
            #[cfg(test)]
            if crash(Crash::PartialRun) {
                output
                    .write_all(&run.chunks().next().unwrap()[..1])
                    .map_err(io_error(&tmp))?;
                return Err(Failure::Crashed(crashed(&tmp)));
            }
            let mut sum = Hasher::new();
            run_parts(run, |bytes| {
                output.write_all(bytes)?;
                sum.update(bytes);
                Ok(())
            })
            .map_err(io_error(&tmp))?;
            output.flush().map_err(io_error(&tmp))?;
            output.get_ref().sync_all().map_err(io_error(&tmp))?;
            fs::rename(&tmp, &file).map_err(io_error(&file))?;
            written.bytes += len;
            written.runs_written += 1;
            let sum = sum.finalize();
            let _ = published.checkpoint_crc.set(sum);
            crash_point!(Crash::AfterFirstRun, &file);
            sum
        };
        written.largest_run_bytes = written.largest_run_bytes.max(len as usize);
        written.manifest.runs.push(RunEntry {
            id: published.id,
            level,
            flush,
            seq_override: run.seq_override(),
            len,
            run_len: run.stored_len() as u64,
            sum,
        });
    }
    sync_dir(&runs_dir).map_err(io_error(&runs_dir))?;
    let manifest = written.manifest.encode();
    let total = written.bytes.saturating_add(manifest.len() as u64);
    if total > options.max_bytes {
        return Err(CheckpointError::DiskLimit {
            bytes: total,
            max_bytes: options.max_bytes,
        }
        .into());
    }
    let path = checkpoint_dir(dir, generation);
    let file = path.join(MANIFEST);
    write_synced(&file, &manifest)?;
    written.bytes = total;
    crash_point!(Crash::BeforeManifestSync, &file);
    sync_dir(&path).map_err(io_error(&path))?;
    sync_dir(dir).map_err(io_error(dir))?;
    Ok(written)
}

impl Engine {
    /// Flushes and commits the state at one sequence ceiling. Existing run
    /// files are reused; file I/O holds no state lock. Keeps one previous
    /// generation. See [Checkpoints](crate::engine#checkpoints).
    ///
    /// # Errors
    /// Returns [`CheckpointError`] for locking, disk-budget or I/O failures.
    /// Its I/O variant identifies errors after the commit point.
    ///
    /// # Panics
    /// Panics if the state lock is poisoned or a synchronous flush build panics.
    pub fn checkpoint(
        &self,
        dir: &Path,
        options: CheckpointOptions,
    ) -> Result<CheckpointReport, CheckpointError> {
        self.checkpoint_with_metadata(dir, options, &[])
    }

    /// Like checkpoint, with an opaque application blob (at most 16 MiB).
    /// Recover it with [`Self::recover_with_metadata`].
    ///
    /// # Errors
    /// Returns [`CheckpointError::MetadataLimit`] for oversized metadata and
    /// the same locking, disk-budget and I/O errors as [`Self::checkpoint`].
    ///
    /// # Panics
    /// Panics under the same conditions as [`Self::checkpoint`].
    pub fn checkpoint_with_metadata(
        &self,
        dir: &Path,
        options: CheckpointOptions,
        metadata: &[u8],
    ) -> Result<CheckpointReport, CheckpointError> {
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
        self.publish_metadata(dir, options, metadata)
    }

    fn publish_metadata(
        &self,
        dir: &Path,
        options: CheckpointOptions,
        metadata: &[u8],
    ) -> Result<CheckpointReport, CheckpointError> {
        if metadata.len() > MAX_METADATA_BYTES {
            return Err(CheckpointError::MetadataLimit);
        }
        let previous = match committed_manifest(dir) {
            Ok((_, _, manifest)) => Some(manifest),
            Err(OpenError::Corrupt {
                path,
                damage: Damage::Missing,
            }) if path == dir.join(CURRENT) => None,
            Err(error) => return Err(io_error(dir)(io::Error::other(error))),
        };
        let found = generations(dir)?;
        let generation = found
            .iter()
            .copied()
            .chain(previous.iter().map(|m| m.generation))
            .max()
            .unwrap_or(0)
            .checked_add(1)
            .ok_or_else(|| io_error(dir)(io::Error::other("checkpoint generations exhausted")))?;
        let _ = fs::remove_file(dir.join(CURRENT_TMP));
        let path = checkpoint_dir(dir, generation);
        fs::create_dir(&path).map_err(io_error(&path))?;
        let built = self
            .core
            .checkpoint_image()
            .map_err(Failure::Error)
            .and_then(|image| {
                #[cfg(test)]
                AFTER_IMAGE.with(|hook| {
                    if let Some(hook) = hook.borrow_mut().take() {
                        hook();
                    }
                });
                write_runs(
                    &image,
                    dir,
                    generation,
                    previous.as_ref().map_or(0, |m| m.generation),
                    options,
                    metadata,
                )
            });
        let written = match built {
            Ok(written) => written,
            Err(Failure::Error(error)) => {
                let _ = fs::remove_dir_all(&path);
                return Err(error);
            }
            #[cfg(test)]
            Err(Failure::Crashed(error)) => return Err(error),
        };
        let tmp = dir.join(CURRENT_TMP);
        let current = encode_current(generation);
        #[cfg(test)]
        if crash(Crash::BeforeRename) {
            write_synced(&tmp, &current)?;
            return Err(crashed(&tmp));
        }
        let committed = write_synced(&tmp, &current)
            .and_then(|()| fs::rename(&tmp, dir.join(CURRENT)).map_err(io_error(dir)));
        if let Err(error) = committed {
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
        let (files_deleted, reclaimed, unreclaimed) =
            reclaim(dir, &found, &written.manifest, previous.as_ref());
        let _ = sync_dir(&dir.join(RUNS));
        let _ = sync_dir(dir);
        Ok(CheckpointReport {
            generation,
            last_seq: written.manifest.last_seq,
            records: written.manifest.records,
            runs: written.manifest.runs.len(),
            bytes: written.bytes,
            bytes_written: written.bytes,
            bytes_reused: written.bytes_reused,
            runs_written: written.runs_written,
            runs_reused: written.runs_reused,
            files_deleted: files_deleted + written.files_deleted,
            largest_run_bytes: written.largest_run_bytes,
            reclaimed,
            unreclaimed,
        })
    }

    /// Recovers CURRENT without rewriting checkpoint data. Sequences continue after the
    /// checkpoint ceiling. No fallback.
    ///
    /// # Errors
    /// Returns [`OpenError`] for invalid options, missing or locked checkpoints,
    /// I/O, validation failures or worker thread creation failures. A failed
    /// recovery releases its directory lock and leaves no worker running.
    pub fn recover(dir: &Path, options: EngineOptions) -> Result<Engine, OpenError> {
        Self::recover_with_metadata(dir, options).map(|(engine, _)| engine)
    }

    /// Recovers the engine and its opaque application metadata.
    ///
    /// # Errors
    /// Returns the same [`OpenError`] variants as [`Self::recover`].
    pub fn recover_with_metadata(
        dir: &Path,
        options: EngineOptions,
    ) -> Result<(Engine, Vec<u8>), OpenError> {
        options.validate().map_err(OpenError::Config)?;
        let _lock = lock_existing(dir)?;
        let mut loaded = load(dir)?;
        let metadata = std::mem::take(&mut loaded.metadata);
        let engine = Engine::start(Arc::new(loaded.into_core(&options)), &options)?;
        Ok((engine, metadata))
    }
}

#[cfg(test)]
thread_local! {
    static AFTER_ROTATION: std::cell::RefCell<Option<Box<dyn FnOnce()>>> = const { std::cell::RefCell::new(None) };
    static AFTER_IMAGE: std::cell::RefCell<Option<Box<dyn FnOnce()>>> = const { std::cell::RefCell::new(None) };
}

fn read_error(path: &Path) -> impl FnOnce(io::Error) -> OpenError + '_ {
    move |error: io::Error| match error.kind() {
        io::ErrorKind::NotFound => OpenError::Corrupt {
            path: path.to_owned(),
            damage: Damage::Missing,
        },
        _ => OpenError::Io {
            path: path.to_owned(),
            error,
        },
    }
}

/// Locks `dir` for a recovery, which needs `CURRENT`: checked first, since
/// locking would create `LOCK`.
pub(super) fn lock_existing(dir: &Path) -> Result<File, OpenError> {
    let current = dir.join(CURRENT);
    match fs::metadata(&current) {
        Ok(_) => {}
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            return Err(OpenError::NoCheckpoint(dir.to_owned()));
        }
        Err(error) => {
            return Err(OpenError::Io {
                path: current,
                error,
            });
        }
    }
    match lock(dir).map_err(read_error(dir))? {
        Ok(file) => Ok(file),
        Err(()) => Err(OpenError::Locked(dir.to_owned())),
    }
}

/// A committed checkpoint, checked in full before starting any worker.
pub(super) struct Loaded {
    pub(super) ceiling: u64,
    metadata: Vec<u8>,
    next_run_id: u64,
    depth: usize,
    runs: Vec<(usize, u64, SharedRun)>,
    pool: ChunkPool,
    run_bytes: usize,
    held_bytes: usize,
}

impl Loaded {
    pub(super) fn into_core(self, options: &EngineOptions) -> Core {
        {
            let mut ids = RUN_IDS.lock().unwrap_or_else(PoisonError::into_inner);
            ids.1 = ids.1.max(self.next_run_id);
        }
        let core = Core::with_pool(options, self.pool);
        {
            let mut state = core.write_state();
            state.last_seq = self.ceiling;
            state.active = crate::memtable::Memtable::in_pool(
                options.memtable_budget,
                Some(self.ceiling),
                &core.chunks,
            );
            state.runs = self
                .runs
                .iter()
                .map(|(_, _, run)| Arc::clone(run))
                .collect();
            state.levels = Levels::recovered(&self.runs, self.depth, options.level_ratio);
            state.order_runs();
            state.run_bytes = self.run_bytes;
            state.held_bytes = self.held_bytes;
        }
        core
    }
}

fn corrupt(path: &Path, damage: Damage) -> OpenError {
    OpenError::Corrupt {
        path: path.to_owned(),
        damage,
    }
}

/// Reads `path` if it holds at most `max` bytes. A longer file is damage,
/// found before anything is allocated for it.
fn read_bounded(path: &Path, max: u64) -> Result<Vec<u8>, OpenError> {
    let file = File::open(path).map_err(read_error(path))?;
    let len = file.metadata().map_err(read_error(path))?.len();
    let too_long = |found| {
        corrupt(
            path,
            Damage::Length {
                expected: max,
                found,
            },
        )
    };
    if len > max {
        return Err(too_long(len));
    }
    let mut bytes = Vec::with_capacity(len as usize);
    file.take(max.saturating_add(1))
        .read_to_end(&mut bytes)
        .map_err(read_error(path))?;
    if bytes.len() as u64 > max {
        return Err(too_long(bytes.len() as u64));
    }
    Ok(bytes)
}

/// CURRENT's generation and checked manifest, bounded before allocation.
fn committed_manifest(dir: &Path) -> Result<(PathBuf, PathBuf, Manifest), OpenError> {
    let current = dir.join(CURRENT);
    let bytes = read_bounded(&current, CURRENT_LEN as u64)?;
    let generation = decode_current(&bytes).map_err(|damage| corrupt(&current, damage))?;
    let path = checkpoint_dir(dir, generation);
    let manifest_path = path.join(MANIFEST);
    let mut file = File::open(&manifest_path).map_err(read_error(&manifest_path))?;
    let found = file.metadata().map_err(read_error(&manifest_path))?.len();
    let mut header = [0u8; MANIFEST_HEADER_LEN];
    if found < (MANIFEST_HEADER_LEN + 4) as u64 {
        return Err(corrupt(
            &manifest_path,
            Damage::Length {
                expected: (MANIFEST_HEADER_LEN + 4) as u64,
                found,
            },
        ));
    }
    file.read_exact(&mut header)
        .map_err(read_error(&manifest_path))?;
    let count = u32::from_le_bytes(header[68..72].try_into().unwrap()) as u64;
    let metadata = u64::from_le_bytes(header[60..68].try_into().unwrap());
    let expected = MANIFEST_HEADER_LEN as u64
        + count * MANIFEST_ENTRY_LEN as u64
        + metadata.min(MAX_METADATA_BYTES as u64)
        + 4;
    if found != expected || metadata > MAX_METADATA_BYTES as u64 {
        return Err(corrupt(&manifest_path, Damage::Length { expected, found }));
    }
    let mut bytes = Vec::with_capacity(usize::try_from(expected).map_err(|_| {
        corrupt(
            &manifest_path,
            Damage::Length {
                expected: usize::MAX as u64,
                found,
            },
        )
    })?);
    bytes.extend_from_slice(&header);
    file.take(expected - MANIFEST_HEADER_LEN as u64 + 1)
        .read_to_end(&mut bytes)
        .map_err(read_error(&manifest_path))?;
    let manifest = Manifest::decode(&bytes).map_err(|damage| corrupt(&manifest_path, damage))?;
    if manifest.generation != generation {
        return Err(corrupt(
            &manifest_path,
            Damage::Generation {
                expected: generation,
                found: manifest.generation,
            },
        ));
    }
    Ok((path, manifest_path, manifest))
}

/// Reads and checks exactly CURRENT's generation under LOCK.
pub(super) fn load(dir: &Path) -> Result<Loaded, OpenError> {
    let (_, manifest_path, manifest) = committed_manifest(dir)?;
    let pool = ChunkPool::new(RESERVE_BYTES);
    let mut runs: Vec<(usize, u64, SharedRun)> = Vec::with_capacity(manifest.runs.len());
    let mut records = 0u64;
    for entry in &manifest.runs {
        let file = dir.join(RUNS).join(run_name(entry.id));
        let mut data = read_bounded(&file, entry.len)?;
        if data.len() as u64 != entry.len {
            return Err(corrupt(
                &file,
                Damage::Length {
                    expected: entry.len,
                    found: data.len() as u64,
                },
            ));
        }
        let filter_at = entry.run_len as usize;
        let filter_end = data.len() - 4;
        let filter = &data[filter_at..filter_end];
        if crc(&[filter]).to_le_bytes() != data[filter_end..] {
            return Err(corrupt(&file, Damage::FilterChecksum));
        }
        if crc(&[&data]) != entry.sum {
            return Err(corrupt(&file, Damage::Checksum));
        }
        let filter = filter.to_vec();
        data.truncate(filter_at);
        let (mut run, max_seq) = Run::recover_checkpoint_in(data, &filter, &pool)
            .map_err(|error| corrupt(&file, Damage::Run(error)))?;
        if max_seq > manifest.last_seq
            || entry
                .seq_override
                .is_some_and(|seq| seq > manifest.last_seq)
        {
            return Err(corrupt(
                &file,
                Damage::Sequence {
                    seq: max_seq.max(entry.seq_override.unwrap_or(0)),
                    ceiling: manifest.last_seq,
                },
            ));
        }
        if let Some(seq) = entry.seq_override {
            if max_seq != 0 {
                return Err(corrupt(&file, Damage::Order));
            }
            run.set_seq_override(seq);
        }
        let Some(range) = run.key_range() else {
            return Err(corrupt(&file, Damage::Order));
        };
        if entry.level > 0
            && runs.last().is_some_and(|(level, _, previous)| {
                *level == entry.level && previous.run.key_range().unwrap().max >= range.min
            })
        {
            return Err(corrupt(&file, Damage::Order));
        }
        records += run.len() as u64;
        let largest_block = run
            .view()
            .largest_block_len()
            .map_err(|error| corrupt(&file, Damage::Run(error)))?;
        runs.push((
            entry.level,
            entry.flush,
            Arc::new(Published {
                id: entry.id,
                checkpoint_crc: OnceLock::from(entry.sum),
                run,
                largest_block,
            }),
        ));
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
    // Per-run checks cannot detect disagreeing copies in different runs. Check
    // the grouped merge before exposing an engine, including older versions
    // that compaction will eventually visit.
    // This holds one decoded block per overlapping group, with no value copies.
    {
        let published: Vec<_> = runs.iter().map(|(_, _, run)| Arc::clone(run)).collect();
        let directory = super::directory::RunDirectory::build(&published);
        let mut workspace = crate::run::MergeWorkspace::new();
        let mut cursor =
            crate::run::MergeCursor::grouped(&directory, directory.groups(), &mut workspace);
        while cursor
            .next_record()
            .map_err(|error| corrupt(&manifest_path, Damage::Run(error)))?
            .is_some()
        {
            cursor.collapse_copies().map_err(|error| {
                let damage = match error {
                    crate::run::ReadError::Corrupt(error) => Damage::Run(error),
                    crate::run::ReadError::ConflictingCopies(_) => Damage::Order,
                };
                corrupt(&manifest_path, damage)
            })?;
        }
    }
    let run_bytes: usize = runs.iter().map(|(_, _, r)| r.run.stored_len()).sum();
    let held_bytes: usize = runs.iter().map(|(_, _, r)| r.run.held_bytes()).sum();
    Ok(Loaded {
        ceiling: manifest.last_seq,
        metadata: manifest.metadata,
        next_run_id: manifest.next_run_id,
        depth: manifest.depth,
        pool,
        runs,
        run_bytes,
        held_bytes,
    })
}

#[cfg(test)]
#[path = "checkpoint_robustness.rs"]
mod robustness;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::EngineWorkspace;

    #[test]
    fn an_unsupported_manifest_version_is_rejected_with_a_valid_checksum() {
        let dir = scratch("unsupported-manifest-version");
        let engine = Engine::new(manual()).unwrap();
        engine.put(b"key", b"value").unwrap();
        engine
            .checkpoint(&dir, CheckpointOptions::default())
            .unwrap();
        engine.close().unwrap();
        let (_, _, manifest) = committed_manifest(&dir).unwrap();
        let mut bytes = manifest.encode();
        bytes[8..12].copy_from_slice(&u32::MAX.to_le_bytes());
        let end = bytes.len() - 4;
        let sum = crc(&[&bytes[..end]]);
        bytes[end..].copy_from_slice(&sum.to_le_bytes());
        assert!(matches!(
            Manifest::decode(&bytes),
            Err(Damage::Unsupported {
                version: u32::MAX,
                ..
            })
        ));
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn manifest_lengths_are_bounded_by_the_run_and_filter_formats() {
        let dir = scratch("format-allocation-bounds");
        let engine = Engine::new(manual()).unwrap();
        engine.put(b"key", b"value").unwrap();
        engine
            .checkpoint(&dir, CheckpointOptions::default())
            .unwrap();
        engine.close().unwrap();
        let (_, _, mut manifest) = committed_manifest(&dir).unwrap();
        for (run_len, len) in [
            (u32::MAX as u64 + 1, u32::MAX as u64 + 5),
            (48, 48 + crate::run::MAX_FILTER_BYTES as u64 + 5),
            (48, u64::MAX),
        ] {
            manifest.runs[0].run_len = run_len;
            manifest.runs[0].len = len;
            assert!(matches!(
                Manifest::decode(&manifest.encode()),
                Err(Damage::Length { .. })
            ));
        }
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn recovery_rejects_conflicting_copies_hidden_by_a_newer_version() {
        let dir = scratch("hidden-conflicting-copies");
        let engine = Engine::new(manual()).unwrap();
        for value in [b"old", b"bad", b"new"] {
            let mut batch = crate::engine::WriteBatch::new();
            batch.put(b"key", value);
            engine.ingest_sorted(&batch).unwrap();
        }
        engine
            .checkpoint(&dir, CheckpointOptions::default())
            .unwrap();
        engine.close().unwrap();
        let (_, path, mut manifest) = committed_manifest(&dir).unwrap();
        assert_eq!(manifest.runs.len(), 3);
        manifest.runs[1].seq_override = manifest.runs[0].seq_override;
        fs::write(path, manifest.encode()).unwrap();
        assert!(matches!(
            Engine::recover(&dir, manual()),
            Err(OpenError::Corrupt {
                damage: Damage::Order,
                ..
            })
        ));
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn recovery_with_an_exhausted_run_counter_can_flush_and_checkpoint() {
        let dir = scratch("exhausted-run-counter");
        let engine = Engine::new(manual()).unwrap();
        engine.put(b"a", b"old").unwrap();
        engine
            .checkpoint(&dir, CheckpointOptions::default())
            .unwrap();
        engine.close().unwrap();
        let (_, path, mut manifest) = committed_manifest(&dir).unwrap();
        manifest.next_run_id = u64::MAX;
        fs::write(path, manifest.encode()).unwrap();
        let recovered = Engine::recover(&dir, manual()).unwrap();
        for key in [b"b", b"c"] {
            recovered.put(key, b"new").unwrap();
            recovered.flush().unwrap();
        }
        let expected = state(&recovered);
        recovered
            .checkpoint(&dir, CheckpointOptions::default())
            .unwrap();
        recovered.close().unwrap();
        let again = Engine::recover(&dir, manual()).unwrap();
        assert_eq!(state(&again), expected);
        again.close().unwrap();
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn recovery_rejects_conflicting_copies_across_valid_runs() {
        for identical in [false, true] {
            let dir = scratch(if identical {
                "identical-copies"
            } else {
                "conflicting-copies"
            });
            let engine = Engine::new(manual()).unwrap();
            for value in [b"old".as_slice(), if identical { b"old" } else { b"new" }] {
                let mut batch = crate::engine::WriteBatch::new();
                batch.put(b"key", value);
                engine.ingest_sorted(&batch).unwrap();
            }
            engine
                .checkpoint(&dir, CheckpointOptions::default())
                .unwrap();
            engine.close().unwrap();
            let (_, path, mut manifest) = committed_manifest(&dir).unwrap();
            assert_eq!(manifest.runs.len(), 2);
            manifest.runs[1].seq_override = manifest.runs[0].seq_override;
            fs::write(path, manifest.encode()).unwrap();
            let result = Engine::recover(&dir, manual());
            if identical {
                let recovered = result.unwrap();
                assert_eq!(state(&recovered), vec![(b"key".to_vec(), b"old".to_vec())]);
                recovered.close().unwrap();
            } else {
                assert!(matches!(
                    result,
                    Err(OpenError::Corrupt {
                        damage: Damage::Order,
                        ..
                    })
                ));
            }
            fs::remove_dir_all(dir).unwrap();
        }
    }

    #[test]
    fn an_unlimited_file_bound_does_not_overflow() {
        let dir = scratch("unlimited-read-bound");
        fs::create_dir_all(&dir).unwrap();
        let path = dir.join("bytes");
        fs::write(&path, b"small").unwrap();
        assert_eq!(read_bounded(&path, u64::MAX).unwrap(), b"small");
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn a_manifest_with_an_unlimited_run_length_is_typed_damage() {
        let dir = scratch("unlimited-run-length");
        let engine = Engine::new(manual()).unwrap();
        engine.put(b"key", b"value").unwrap();
        engine
            .checkpoint(&dir, CheckpointOptions::default())
            .unwrap();
        engine.close().unwrap();
        let (_, path, mut manifest) = committed_manifest(&dir).unwrap();
        manifest.runs[0].len = u64::MAX;
        fs::write(&path, manifest.encode()).unwrap();
        assert!(matches!(
            Engine::recover(&dir, manual()),
            Err(OpenError::Corrupt { .. })
        ));
        fs::remove_dir_all(dir).unwrap();
    }

    /// The defaults without the background worker: these tests flush and
    /// compact by hand and count what each step leaves.
    fn manual() -> EngineOptions {
        EngineOptions {
            background_compaction: None,
            ..EngineOptions::default()
        }
    }

    /// Every file under `dir` with its length and contents, to show that a
    /// failed open changed nothing.
    fn files(dir: &Path) -> Vec<(PathBuf, Vec<u8>)> {
        let mut out = Vec::new();
        let mut pending = vec![dir.to_owned()];
        while let Some(at) = pending.pop() {
            for entry in fs::read_dir(&at).unwrap() {
                let path = entry.unwrap().path();
                if path.is_dir() {
                    pending.push(path);
                } else {
                    out.push((path.clone(), fs::read(&path).unwrap()));
                }
            }
        }
        out.sort();
        out
    }

    #[test]
    fn a_worker_that_cannot_start_leaves_the_directory_as_it_was() {
        let fail_next = || crate::engine::worker::tests::FAIL_SPAWN.with(|f| f.set(true));
        let options = EngineOptions::default();
        let checkpoint = scratch("spawn-failure-checkpoint");
        let engine = Engine::new(options).unwrap();
        engine.put(b"k", b"checkpointed").unwrap();
        engine
            .checkpoint(&checkpoint, CheckpointOptions::default())
            .unwrap();
        engine.close().unwrap();
        let before = files(&checkpoint);
        fail_next();
        let refused = Engine::recover(&checkpoint, options).map(drop);
        assert!(matches!(refused, Err(OpenError::Worker(_))), "{refused:?}");
        assert_eq!(files(&checkpoint), before);
        let engine = Engine::recover(&checkpoint, options).unwrap();
        let mut workspace = EngineWorkspace::new();
        assert_eq!(
            engine.get(b"k", &mut workspace).unwrap().unwrap().value,
            b"checkpointed"
        );
        engine.close().unwrap();
        fs::remove_dir_all(&checkpoint).unwrap();
    }

    #[test]
    fn manifest_and_current_round_trip_and_reject_damage() {
        let manifest = Manifest {
            generation: 7,
            last_seq: 1 << 40,
            records: 12,
            next_run_id: 3,
            previous: 4,
            depth: 2,
            metadata: b"offsets\0\xff".to_vec(),
            runs: vec![
                RunEntry {
                    id: 1,
                    level: 0,
                    flush: 0,
                    seq_override: Some(7),
                    len: 100,
                    run_len: 88,
                    sum: 0xdead_beef,
                },
                RunEntry {
                    id: 2,
                    level: 2,
                    flush: 0,
                    seq_override: None,
                    len: 100,
                    run_len: 88,
                    sum: 1,
                },
            ],
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

    #[test]
    fn malformed_manifest_fields_are_rejected_without_panicking() {
        let manifest = Manifest {
            generation: 2,
            last_seq: 42,
            records: 2,
            next_run_id: 3,
            previous: 1,
            depth: 1,
            metadata: Vec::new(),
            runs: vec![
                RunEntry {
                    id: 1,
                    level: 0,
                    flush: 0,
                    seq_override: None,
                    len: 100,
                    run_len: 88,
                    sum: 0,
                },
                RunEntry {
                    id: 2,
                    level: 0,
                    flush: 1,
                    seq_override: None,
                    len: 100,
                    run_len: 88,
                    sum: 0,
                },
            ],
        };
        let bytes = manifest.encode();
        for end in 0..bytes.len() {
            assert!(Manifest::decode(&bytes[..end]).is_err());
        }
        let seal = |bytes: &mut [u8]| {
            let end = bytes.len() - 4;
            let sum = crc(&[&bytes[..end]]);
            bytes[end..].copy_from_slice(&sum.to_le_bytes());
        };
        for (at, replacement) in [
            (40, 0u64),                           // exhausted/invalid next counter
            (48, 2),                              // previous is not older
            (60, u64::MAX),                       // unbounded metadata length
            (MANIFEST_HEADER_LEN + 40, 3),        // file cannot hold its checksum
            (MANIFEST_HEADER_LEN + 48, u64::MAX), // run payload exceeds file
            (MANIFEST_HEADER_LEN + 32, u64::MAX), // flush counter cannot advance
        ] {
            let mut damaged = bytes.clone();
            damaged[at..at + 8].copy_from_slice(&replacement.to_le_bytes());
            seal(&mut damaged);
            assert!(Manifest::decode(&damaged).is_err(), "offset {at}");
        }
        let mut duplicate = bytes.clone();
        duplicate[MANIFEST_HEADER_LEN + MANIFEST_ENTRY_LEN
            ..MANIFEST_HEADER_LEN + MANIFEST_ENTRY_LEN + 16]
            .copy_from_slice(&1u128.to_le_bytes());
        seal(&mut duplicate);
        assert_eq!(Manifest::decode(&duplicate), Err(Damage::Order));
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
            (Crash::PartialRun, false),
            (Crash::AfterFirstRun, false),
            (Crash::BeforeManifestSync, false),
            (Crash::BeforeRename, false),
            (Crash::AfterRename, true),
            (Crash::BeforeReclaim, true),
        ] {
            let dir = scratch(&format!("{crash:?}"));
            let engine = Engine::new(EngineOptions {
                target_run_len: 1024,
                ..manual()
            })
            .unwrap();
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
            let recovered = Engine::recover(&dir, manual()).unwrap();
            let expected = if committed { &new } else { &old };
            assert_eq!(&state(&recovered), expected, "{crash:?}");
            drop(recovered);

            let report = engine
                .checkpoint(&dir, CheckpointOptions::default())
                .unwrap();
            assert_eq!(report.generation, 3, "{crash:?}");
            assert_eq!(report.reclaimed, 1, "{crash:?}");
            assert_eq!(
                entries(&dir),
                [
                    CURRENT.to_owned(),
                    format!("{PREFIX}{:020}", if committed { 2 } else { 1 }),
                    format!("{PREFIX}{:020}", 3),
                    RUNS.to_owned()
                ],
                "{crash:?}"
            );
            let recovered = Engine::recover(&dir, manual()).unwrap();
            assert_eq!(state(&recovered), new, "{crash:?}");
            fs::remove_dir_all(&dir).unwrap();
        }
    }

    /// A second process's lock refuses both a checkpoint and a recovery.
    #[test]
    fn the_lock_keeps_a_second_user_out() {
        let dir = scratch("lock");
        let engine = Engine::new(manual()).unwrap();
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
            Engine::recover(&dir, manual()),
            Err(OpenError::Locked(_))
        ));
        drop(held);
        Engine::recover(&dir, manual()).unwrap();
        fs::remove_dir_all(&dir).unwrap();
    }
    fn manifest(dir: &Path) -> Manifest {
        committed_manifest(dir).unwrap().2
    }

    fn compact_all(engine: &Engine) {
        let options = super::super::CompactionPolicy {
            max_overlap: 1,
            ..super::super::CompactionPolicy::default()
        };
        let mut workspace = super::super::MergeWorkspace::new();
        while engine
            .core
            .compact(options, &mut workspace)
            .unwrap()
            .is_some()
        {}
    }

    #[test]
    fn incremental_checkpoints_write_only_new_runs_and_gc_keeps_two_commits() {
        let dir = scratch("incremental");
        let options = EngineOptions {
            compression: super::super::Compression::Uncompressed,
            target_run_len: 1 << 20,
            ..manual()
        };
        let engine = Engine::new(options).unwrap();
        for i in 0..1_000u32 {
            engine.put(&i.to_be_bytes(), b"original").unwrap();
        }
        engine.flush().unwrap();
        let expected: Vec<_> = engine
            .core
            .read_state()
            .runs
            .iter()
            .map(|r| {
                let mut bytes = Vec::new();
                run_parts(&r.run, |part| {
                    bytes.extend_from_slice(part);
                    Ok(())
                })
                .unwrap();
                (r.id, bytes)
            })
            .collect();
        let first = engine
            .checkpoint(&dir, CheckpointOptions::default())
            .unwrap();
        assert_eq!((first.runs_written, first.runs_reused), (1, 0));
        assert_eq!(first.bytes_reused, 0);
        for (id, bytes) in &expected {
            assert_eq!(
                &fs::read(dir.join(RUNS).join(run_name(*id))).unwrap(),
                bytes
            );
        }
        let original = manifest(&dir);
        for i in 0..10u32 {
            engine.put(&i.to_be_bytes(), b"changed").unwrap();
        }
        engine.flush().unwrap();
        let second = engine
            .checkpoint(&dir, CheckpointOptions::default())
            .unwrap();
        let second_manifest = manifest(&dir);
        assert_eq!(
            (
                second.runs_written,
                second.runs_reused,
                second.files_deleted
            ),
            (1, 1, 0)
        );
        assert_eq!(second.bytes_reused, original.runs[0].len);
        assert_eq!(
            second.bytes_written,
            second_manifest.runs[1].len + second_manifest.encode().len() as u64
        );
        assert!(second.bytes_written < first.bytes_written / 10);
        for (id, bytes) in &expected {
            assert_eq!(
                &fs::read(dir.join(RUNS).join(run_name(*id))).unwrap(),
                bytes
            );
        }

        compact_all(&engine);
        let third = engine
            .checkpoint(&dir, CheckpointOptions::default())
            .unwrap();
        let third_manifest = manifest(&dir);
        assert_eq!(
            (third.runs_written, third.runs_reused, third.files_deleted),
            (1, 0, 0)
        );
        assert_eq!(
            third.bytes_written,
            third_manifest.runs[0].len + third_manifest.encode().len() as u64
        );
        // The preceding committed generation still recovers after GC.
        fs::write(dir.join(CURRENT), encode_current(second.generation)).unwrap();
        let previous = Engine::recover(&dir, options).unwrap();
        assert_eq!(state(&previous), state(&engine));
        previous.close().unwrap();
        fs::write(dir.join(CURRENT), encode_current(third.generation)).unwrap();

        let fourth = engine
            .checkpoint(&dir, CheckpointOptions::default())
            .unwrap();
        assert_eq!(
            (
                fourth.runs_written,
                fourth.runs_reused,
                fourth.files_deleted
            ),
            (0, 1, 2)
        );
        assert_eq!(fourth.bytes_written, manifest(&dir).encode().len() as u64);
        assert_eq!(fourth.bytes_reused, third_manifest.runs[0].len);
        for entry in &second_manifest.runs {
            assert!(!dir.join(RUNS).join(run_name(entry.id)).exists());
        }
        // Recovery preserves IDs and the persisted filter exactly, allowing a
        // metadata-only checkpoint without rehashing any keys or rewriting files.
        let recovered = Engine::recover(&dir, options).unwrap();
        assert!(
            recovered
                .core
                .read_state()
                .runs
                .iter()
                .all(|r| r.run.filter_build_bytes() == 0)
        );
        let fifth = recovered
            .checkpoint(&dir, CheckpointOptions::default())
            .unwrap();
        assert_eq!(
            (fifth.runs_written, fifth.runs_reused, fifth.files_deleted),
            (0, 1, 0)
        );
        assert_eq!(fifth.bytes_reused, fourth.bytes_reused);
        assert_eq!(manifest(&dir).runs, third_manifest.runs);
        assert_eq!(state(&recovered), state(&engine));
        for (name, report) in [
            ("first", first),
            ("second", second),
            ("third", third),
            ("fourth", fourth),
            ("recovered", fifth),
        ] {
            eprintln!(
                "{name}: written={} reused={} runs_written={} runs_reused={} deleted={}",
                report.bytes_written,
                report.bytes_reused,
                report.runs_written,
                report.runs_reused,
                report.files_deleted
            );
        }
        recovered.close().unwrap();
        engine.close().unwrap();
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn metadata_commits_with_its_ceiling_and_empty_metadata_remains_supported() {
        let dir = scratch("metadata");
        let engine = Engine::new(manual()).unwrap();
        engine.put(b"offset", b"value").unwrap();
        let metadata = b"kafka/partition-7=12345\0\xff";
        let first = engine
            .checkpoint_with_metadata(&dir, CheckpointOptions::default(), metadata)
            .unwrap();
        let (recovered, found) = Engine::recover_with_metadata(&dir, manual()).unwrap();
        assert_eq!(found, metadata);
        assert_eq!(recovered.stats().last_seq, first.last_seq);
        assert_eq!(state(&recovered), state(&engine));
        recovered.close().unwrap();
        let second = engine
            .checkpoint(&dir, CheckpointOptions::default())
            .unwrap();
        assert_eq!((second.runs_written, second.runs_reused), (0, 1));
        let (recovered, found) = Engine::recover_with_metadata(&dir, manual()).unwrap();
        assert!(found.is_empty());
        recovered.close().unwrap();
        assert!(matches!(
            engine.checkpoint_with_metadata(
                &dir,
                CheckpointOptions::default(),
                &vec![0; MAX_METADATA_BYTES + 1]
            ),
            Err(CheckpointError::MetadataLimit)
        ));
        assert_eq!(manifest(&dir).generation, second.generation);
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn a_paused_checkpoint_retains_its_ceiling_while_writes_ingestion_and_compaction_continue() {
        let dir = scratch("paused-image");
        let engine = Arc::new(Engine::new(manual()).unwrap());
        for i in 0..1_000u32 {
            engine.put(&i.to_be_bytes(), b"original").unwrap();
        }
        engine.flush().unwrap();
        for i in 0..10u32 {
            engine.delete(&i.to_be_bytes()).unwrap();
        }
        let pin = engine.snapshot();
        let expected = state(&engine);
        let ceiling = engine.stats().last_seq;
        let (ready_tx, ready_rx) = std::sync::mpsc::channel();
        let (resume_tx, resume_rx) = std::sync::mpsc::channel();
        let checkpoint_engine = Arc::clone(&engine);
        let checkpoint_dir = dir.clone();
        let checkpoint = std::thread::spawn(move || {
            AFTER_IMAGE.with(|hook| {
                *hook.borrow_mut() = Some(Box::new(move || {
                    ready_tx.send(()).unwrap();
                    resume_rx.recv().unwrap();
                }))
            });
            checkpoint_engine.checkpoint_with_metadata(
                &checkpoint_dir,
                CheckpointOptions::default(),
                b"offset=1010",
            )
        });
        ready_rx
            .recv_timeout(std::time::Duration::from_secs(10))
            .unwrap();
        engine.put(b"after-ceiling", b"excluded").unwrap();
        engine.flush().unwrap();
        let mut batch = super::super::WriteBatch::new();
        batch.put(b"ingested-later", b"excluded");
        engine.ingest_sorted(&batch).unwrap();
        let before = engine
            .core
            .read_state()
            .runs
            .iter()
            .map(|r| r.id)
            .collect::<HashSet<_>>();
        compact_all(&engine);
        let after = engine
            .core
            .read_state()
            .runs
            .iter()
            .map(|r| r.id)
            .collect::<HashSet<_>>();
        assert_ne!(
            before, after,
            "compaction published while the checkpoint was paused"
        );
        assert!(engine.stats().retained_run_bytes > 0);
        resume_tx.send(()).unwrap();
        let report = checkpoint.join().unwrap().unwrap();
        assert_eq!(report.last_seq, ceiling);
        let (recovered, metadata) = Engine::recover_with_metadata(&dir, manual()).unwrap();
        assert_eq!(state(&recovered), expected);
        assert_eq!(metadata, b"offset=1010");
        let mut ws = EngineWorkspace::new();
        let mut scan = pin.scan(&mut ws);
        let mut pinned = Vec::new();
        while let Some(record) = scan.next_record().unwrap() {
            pinned.push((record.key.to_vec(), record.value.to_vec()));
        }
        assert_eq!(pinned, expected);
        recovered.close().unwrap();
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn independently_recovered_branches_never_reuse_ids_for_different_outputs() {
        let dir = scratch("recovery-branches");
        let seed = Engine::new(manual()).unwrap();
        seed.put(b"shared", b"base").unwrap();
        seed.checkpoint(&dir, CheckpointOptions::default()).unwrap();
        let left = Engine::recover(&dir, manual()).unwrap();
        let right = Engine::recover(&dir, manual()).unwrap();
        left.put(b"branch", b"left").unwrap();
        right.put(b"branch", b"right").unwrap();
        left.checkpoint(&dir, CheckpointOptions::default()).unwrap();
        let left_manifest = manifest(&dir);
        right
            .checkpoint(&dir, CheckpointOptions::default())
            .unwrap();
        let right_manifest = manifest(&dir);
        assert_eq!(left_manifest.runs[0].id, right_manifest.runs[0].id);
        assert_ne!(left_manifest.runs[1].id, right_manifest.runs[1].id);
        let recovered = Engine::recover(&dir, manual()).unwrap();
        assert_eq!(state(&recovered), state(&right));
        fs::remove_dir_all(dir).unwrap();
    }
    #[test]
    fn writes_during_the_checkpoint_flush_are_excluded_even_if_they_publish_first() {
        let dir = scratch("flush-ceiling");
        let engine = Arc::new(
            Engine::new(EngineOptions {
                max_frozen_memtables: 1,
                ..manual()
            })
            .unwrap(),
        );
        engine.put(b"a", b"base").unwrap();
        // Leave a full frozen queue and a nonempty successor, as after an
        // abandoned flush. Checkpoint must drain the queue before rotation.
        drop(engine.core.begin_flush().unwrap());
        engine.put(b"b", b"checkpointed").unwrap();
        let expected = state(&engine);
        let ceiling = engine.stats().last_seq;
        let (ready_tx, ready_rx) = std::sync::mpsc::channel();
        let (resume_tx, resume_rx) = std::sync::mpsc::channel();
        let checkpoint_engine = Arc::clone(&engine);
        let checkpoint_dir = dir.clone();
        let checkpoint = std::thread::spawn(move || {
            AFTER_ROTATION.with(|hook| {
                *hook.borrow_mut() = Some(Box::new(move || {
                    ready_tx.send(()).unwrap();
                    resume_rx.recv().unwrap();
                }))
            });
            checkpoint_engine.checkpoint(&checkpoint_dir, CheckpointOptions::default())
        });
        ready_rx
            .recv_timeout(std::time::Duration::from_secs(10))
            .unwrap();
        assert_eq!(
            engine.stats().frozen_tables,
            1,
            "checkpoint respects the frozen-table limit"
        );
        // Both operations finish while checkpoint compression is paused. The
        // ingestion publishes first, yet its sequence is above the ceiling.
        assert!(engine.put(b"z", b"later").unwrap() > ceiling);
        let mut batch = super::super::WriteBatch::new();
        batch.put(b"zz", b"published-before-checkpoint-flush");
        assert!(engine.ingest_sorted(&batch).unwrap() > ceiling);
        resume_tx.send(()).unwrap();
        let report = checkpoint.join().unwrap().unwrap();
        assert_eq!(report.last_seq, ceiling);
        let recovered = Engine::recover(&dir, manual()).unwrap();
        assert_eq!(state(&recovered), expected);
        recovered.close().unwrap();
        fs::remove_dir_all(dir).unwrap();
    }
}
