//! The one public error type and its kinds.

use std::fmt;
use std::io;
use std::path::{Path, PathBuf};

use super::{
    BackgroundError, CheckpointError, CompactionError, ConfigError, OpenError, ReadError,
    UpdateError,
};

/// `Result` with Tessera's [`Error`].
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// What kind of failure an [`Error`] is. Match on it; new kinds may be added.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum ErrorKind {
    /// An option outside its range, or an operation this engine was not
    /// configured for, such as a checkpoint of an in-memory engine.
    Config,
    /// A key, value, record, metadata blob or keyspace name outside its
    /// limits, or a keyspace this engine cannot create.
    InvalidInput,
    /// Writer admission found no memory it can reclaim for this write.
    MemoryLimit,
    /// The keyspace was dropped.
    KeyspaceDropped,
    /// The keyspace belongs to another engine.
    UnknownKeyspace,
    /// Another engine or process holds the directory's `LOCK`.
    Locked,
    /// A filesystem operation failed, or a thread could not be started.
    Io,
    /// Stored data failed a check, or two copies of one update disagree.
    Corruption,
    /// The engine was closed.
    Stopped,
    /// A background worker panicked.
    Background,
}

/// Tessera's one error type: a kind, a message and, for filesystem
/// failures, the path and the underlying [`io::Error`] as its
/// [`source`](std::error::Error::source).
///
/// It is one pointer wide, so `Result<()>` is too. `Send + Sync + 'static`.
pub struct Error {
    inner: Box<Inner>,
}

struct Inner {
    kind: ErrorKind,
    message: String,
    path: Option<PathBuf>,
    source: Option<Box<dyn std::error::Error + Send + Sync>>,
    committed: bool,
}

impl Error {
    pub(crate) fn new(kind: ErrorKind, message: impl Into<String>) -> Self {
        Self {
            inner: Box::new(Inner {
                kind,
                message: message.into(),
                path: None,
                source: None,
                committed: false,
            }),
        }
    }

    fn at(mut self, path: impl Into<PathBuf>) -> Self {
        self.inner.path = Some(path.into());
        self
    }

    fn caused_by(mut self, source: impl std::error::Error + Send + Sync + 'static) -> Self {
        self.inner.source = Some(Box::new(source));
        self
    }

    pub(crate) fn io(path: &Path, error: io::Error, message: impl Into<String>) -> Self {
        Self::new(ErrorKind::Io, message).at(path).caused_by(error)
    }

    pub(crate) fn stopped() -> Self {
        Self::new(ErrorKind::Stopped, "the engine was closed")
    }

    pub(crate) fn keyspace_dropped(name: &str) -> Self {
        Self::new(
            ErrorKind::KeyspaceDropped,
            format!("keyspace {name:?} was dropped"),
        )
    }

    pub(crate) fn unknown_keyspace(name: &str) -> Self {
        Self::new(
            ErrorKind::UnknownKeyspace,
            format!("keyspace {name:?} belongs to another engine"),
        )
    }

    /// The kind of failure.
    pub fn kind(&self) -> ErrorKind {
        self.inner.kind
    }

    /// The file or directory involved, if any.
    pub fn path(&self) -> Option<&Path> {
        self.inner.path.as_deref()
    }

    /// For a failed checkpoint, whether it was committed all the same:
    /// `CURRENT` was replaced before a later directory sync failed, so the
    /// new checkpoint is the one a later [`Engine::open`](crate::Engine::open)
    /// recovers if the rename reached the disk, and both generations are kept.
    pub fn checkpoint_committed(&self) -> bool {
        self.inner.committed
    }
}

impl fmt::Debug for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Error")
            .field("kind", &self.inner.kind)
            .field("message", &self.inner.message)
            .field("path", &self.inner.path)
            .field("source", &self.inner.source)
            .finish_non_exhaustive()
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.inner.message)
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.inner
            .source
            .as_deref()
            .map(|source| source as &(dyn std::error::Error + 'static))
    }
}

impl From<Error> for io::Error {
    /// Keeps the kind of an underlying [`io::Error`]; maps the other kinds to
    /// the closest [`io::ErrorKind`]. The [`Error`] is the new error's source.
    fn from(error: Error) -> Self {
        let kind = match error.kind() {
            ErrorKind::Io => error
                .inner
                .source
                .as_deref()
                .and_then(|source| source.downcast_ref::<io::Error>())
                .map_or(io::ErrorKind::Other, io::Error::kind),
            ErrorKind::Config | ErrorKind::InvalidInput | ErrorKind::UnknownKeyspace => {
                io::ErrorKind::InvalidInput
            }
            ErrorKind::MemoryLimit => io::ErrorKind::OutOfMemory,
            ErrorKind::KeyspaceDropped => io::ErrorKind::NotFound,
            ErrorKind::Locked => io::ErrorKind::ResourceBusy,
            ErrorKind::Corruption => io::ErrorKind::InvalidData,
            ErrorKind::Stopped | ErrorKind::Background => io::ErrorKind::Other,
        };
        io::Error::new(kind, error)
    }
}

impl From<ConfigError> for Error {
    fn from(error: ConfigError) -> Self {
        Self::new(ErrorKind::Config, error.to_string())
    }
}

impl From<UpdateError> for Error {
    fn from(error: UpdateError) -> Self {
        let kind = match error {
            UpdateError::Rejected(_) => ErrorKind::InvalidInput,
            UpdateError::MemoryLimit { .. } => ErrorKind::MemoryLimit,
            UpdateError::KeyspaceDropped => ErrorKind::KeyspaceDropped,
            UpdateError::Keyspaces => ErrorKind::InvalidInput,
            UpdateError::Stopped => ErrorKind::Stopped,
            UpdateError::LeaderPanicked => ErrorKind::Background,
        };
        Self::new(kind, error.to_string())
    }
}

impl From<ReadError> for Error {
    fn from(error: ReadError) -> Self {
        Self::new(ErrorKind::Corruption, error.to_string())
    }
}

impl From<CompactionError> for Error {
    fn from(error: CompactionError) -> Self {
        let kind = match error {
            CompactionError::Read(_) | CompactionError::Stale => ErrorKind::Corruption,
            CompactionError::OutputLimit { .. } => ErrorKind::MemoryLimit,
        };
        Self::new(kind, error.to_string())
    }
}

impl From<BackgroundError> for Error {
    fn from(error: BackgroundError) -> Self {
        match error {
            BackgroundError::Panicked(_) => Self::new(ErrorKind::Background, error.to_string()),
            BackgroundError::Failed(failure) => Self::from(failure),
        }
    }
}

impl From<OpenError> for Error {
    fn from(error: OpenError) -> Self {
        let message = error.to_string();
        match error {
            OpenError::Locked(path) => Self::new(ErrorKind::Locked, message).at(path),
            OpenError::Io { path, error } => {
                Self::new(ErrorKind::Io, message).at(path).caused_by(error)
            }
            OpenError::Corrupt { path, .. } => Self::new(ErrorKind::Corruption, message).at(path),
            OpenError::Worker(error) => Self::new(ErrorKind::Io, message).caused_by(error),
        }
    }
}

impl From<CheckpointError> for Error {
    fn from(error: CheckpointError) -> Self {
        let message = error.to_string();
        match error {
            CheckpointError::MetadataLimit => Self::new(ErrorKind::InvalidInput, message),
            CheckpointError::Io {
                path,
                error,
                after_commit_point,
            } => {
                let mut error = Self::new(ErrorKind::Io, message).at(path).caused_by(error);
                error.inner.committed = after_commit_point;
                error
            }
        }
    }
}

const _: () = {
    const fn bounds<T: Send + Sync + 'static>() {}
    bounds::<Error>();
    assert!(size_of::<Result<()>>() == size_of::<usize>());
};
