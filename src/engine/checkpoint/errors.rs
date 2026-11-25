//! Checkpoint, damage and open errors.

use super::*;

/// Why a checkpoint was not committed, or (for [`Self::Io`] after the commit
/// point) was committed without the final directory sync. The engine is
/// still readable either way; its memtables may have flushed. The preceding
/// committed generation is retained.
#[derive(Debug)]
#[non_exhaustive]
pub(crate) enum CheckpointError {
    /// Application metadata exceeds [`MAX_METADATA_LEN`].
    MetadataLimit,
    /// A filesystem operation failed.
    #[non_exhaustive]
    Io {
        /// The file or directory.
        path: PathBuf,
        /// The error.
        error: io::Error,
        /// The committed checkpoint's report if `CURRENT` had already been
        /// replaced, so that the new checkpoint may be committed (if the final
        /// sync failed) and both were kept.
        committed: Option<Box<CheckpointReport>>,
    },
}

impl fmt::Display for CheckpointError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MetadataLimit => write!(
                f,
                "checkpoint metadata exceeds the limit of {MAX_METADATA_LEN} bytes"
            ),
            Self::Io {
                path,
                error,
                committed,
            } => write!(
                f,
                "checkpoint failed at {}{}: {error}",
                path.display(),
                if committed.is_some() {
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
#[non_exhaustive]
pub(crate) enum Damage {
    /// A file the checkpoint needs does not exist.
    Missing,
    /// The file is shorter or longer than its format or the manifest says.
    #[non_exhaustive]
    Length {
        /// Expected length.
        expected: u64,
        /// Actual length.
        found: u64,
    },
    /// The file does not start with its magic number.
    BadMagic,
    /// A manifest version or flags this build does not read.
    #[non_exhaustive]
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
    #[non_exhaustive]
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
    /// The manifest's keyspace table is malformed: a bad name, depth or
    /// length, IDs out of order, or more than the live keyspace limit.
    Keyspaces,
    /// Two keyspaces of the manifest share a name or an ID.
    DuplicateKeyspace,
    /// A keyspace ID is not below the manifest's next keyspace ID.
    #[non_exhaustive]
    KeyspaceId {
        /// The keyspace's ID.
        id: u32,
        /// The next ID.
        next: u32,
    },
    /// A run belongs to a keyspace the manifest does not list.
    #[non_exhaustive]
    UnknownKeyspace {
        /// The run's keyspace ID.
        id: u32,
    },
    /// A record has a sequence number above the checkpoint's ceiling.
    #[non_exhaustive]
    Sequence {
        /// The record's.
        seq: u64,
        /// The ceiling.
        ceiling: u64,
    },
    /// The runs hold a different number of records than the manifest says.
    #[non_exhaustive]
    Records {
        /// The manifest's count.
        expected: u64,
        /// The runs'.
        found: u64,
    },
}

impl fmt::Display for Damage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Missing => f.write_str("the file is missing"),
            Self::Length { expected, found } => {
                write!(f, "length {found} bytes where {expected} are expected")
            }
            Self::BadMagic => f.write_str("the file does not start with its magic number"),
            Self::Unsupported { version, .. } if *version != MANIFEST_VERSION => {
                write!(f, "unsupported manifest version {version}")
            }
            Self::Unsupported { flags, .. } => {
                write!(f, "unsupported manifest flags {flags:#x}")
            }
            Self::Checksum => f.write_str("checksum mismatch"),
            Self::FilterChecksum => f.write_str("key filter checksum mismatch"),
            Self::Generation { expected, found } => write!(
                f,
                "the manifest is of generation {found}, CURRENT names {expected}"
            ),
            Self::Run(error) => write!(f, "invalid run: {error}"),
            Self::Order => f.write_str(
                "run IDs, levels, flush groups, level order or copies of one update disagree",
            ),
            Self::Keyspaces => f.write_str("malformed keyspace table"),
            Self::DuplicateKeyspace => f.write_str("two keyspaces share a name or an ID"),
            Self::KeyspaceId { id, next } => write!(
                f,
                "keyspace ID {id} is not below the next keyspace ID {next}"
            ),
            Self::UnknownKeyspace { id } => {
                write!(f, "a run belongs to keyspace ID {id}, which is not listed")
            }
            Self::Sequence { seq, ceiling } => write!(
                f,
                "sequence number {seq} is above the checkpoint's ceiling {ceiling}"
            ),
            Self::Records { expected, found } => write!(
                f,
                "the runs hold {found} records where the manifest says {expected}"
            ),
        }
    }
}

/// Why an engine did not open. Nothing is exposed, no worker is left
/// running and the directory lock is released. Recovery leaves checkpoint
/// data unchanged. It never substitutes an empty database or a different
/// checkpoint.
#[derive(Debug)]
#[non_exhaustive]
pub(crate) enum OpenError {
    /// Another operation or process holds the directory's `LOCK`.
    Locked(PathBuf),
    /// A file could not be read.
    #[non_exhaustive]
    Io {
        /// The file.
        path: PathBuf,
        /// The error.
        error: io::Error,
    },
    /// A file failed its checks.
    #[non_exhaustive]
    Corrupt {
        /// The file.
        path: PathBuf,
        /// How.
        damage: Damage,
    },
    /// The operating system could not create a background worker thread.
    /// Recovery changed nothing on disk.
    Worker(io::Error),
}

impl fmt::Display for OpenError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Locked(path) => write!(
                f,
                "{} is locked by another engine or process",
                path.display()
            ),
            Self::Io { path, error } => write!(f, "cannot read {}: {error}", path.display()),
            Self::Corrupt { path, damage } => {
                write!(f, "checkpoint file {} is damaged: {damage}", path.display())
            }
            Self::Worker(error) => {
                write!(f, "cannot start a background worker: {error}")
            }
        }
    }
}

impl std::error::Error for OpenError {}
