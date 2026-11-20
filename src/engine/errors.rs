//! Write and update errors.

use super::*;

/// Why a write can never be applied with the engine's current options. A
/// write that only needs room flushes instead; see
/// [Flushing](super#flushing).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub(crate) enum WriteError {
    /// The key exceeds [`MAX_KEY_LEN`].
    #[non_exhaustive]
    KeyTooLong {
        /// Length of the rejected key.
        len: usize,
    },
    /// The value exceeds [`MAX_VALUE_LEN`].
    #[non_exhaustive]
    ValueTooLong {
        /// Length of the rejected value.
        len: usize,
    },
    /// The record alone would be charged more than a whole memtable budget
    /// ([`Options::memtable_size`]), so no memtable can hold it.
    #[non_exhaustive]
    RecordExceedsBudget {
        /// What the record would be charged: its key and value and
        /// [`ENTRY_OVERHEAD`].
        charge: usize,
        /// The budget.
        budget: usize,
    },
}

impl fmt::Display for WriteError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match *self {
            Self::KeyTooLong { len } => {
                write!(f, "key of {len} bytes exceeds the limit of {MAX_KEY_LEN}")
            }
            Self::ValueTooLong { len } => {
                write!(
                    f,
                    "value of {len} bytes exceeds the limit of {MAX_VALUE_LEN}"
                )
            }
            Self::RecordExceedsBudget { charge, budget } => write!(
                f,
                "record charged {charge} bytes exceeds the memtable size of {budget}"
            ),
        }
    }
}

impl std::error::Error for WriteError {}

/// A write used no sequence number and changed no reads.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub(crate) enum UpdateError {
    /// The write violates key, value or record-budget limits.
    Rejected(WriteError),
    /// No further memory can currently be reclaimed. The write used no sequence.
    #[non_exhaustive]
    MemoryLimit {
        /// Accounted memory at admission.
        used: usize,
        /// Configured budget.
        limit: usize,
    },
    /// A keyspace the write names is not live.
    KeyspaceDropped,
}

impl fmt::Display for UpdateError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Rejected(error) => error.fmt(f),
            Self::MemoryLimit { used, limit } => {
                write!(f, "memory use {used} bytes reached the limit of {limit}")
            }
            Self::KeyspaceDropped => f.write_str("the keyspace was dropped"),
        }
    }
}

impl std::error::Error for UpdateError {}
