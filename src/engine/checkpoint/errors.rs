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
        /// Whether `CURRENT` had already been replaced, so that the new
        /// checkpoint may be committed (if the final sync failed) and both
        /// were kept.
        after_commit_point: bool,
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
    /// The keyspace and application metadata in the manifest are malformed.
    Catalog,
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
    /// The operating system could not create the background compaction
    /// worker's thread. Recovery changed nothing on disk.
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
                write!(
                    f,
                    "checkpoint file {} is damaged: {damage:?}",
                    path.display()
                )
            }
            Self::Worker(error) => {
                write!(f, "cannot start the background compaction worker: {error}")
            }
        }
    }
}

impl std::error::Error for OpenError {}
