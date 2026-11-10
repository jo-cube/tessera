//! Bounded mutable storage and its conversion into immutable runs.
//!
//! A [`Memtable`] accepts updates. [`Memtable::freeze`] consumes it and returns
//! a [`FrozenMemtable`], which has no way to change, and
//! [`FrozenMemtable::flush`] builds compressed runs from it:
//!
//! ```
//! use tessera::block::Value;
//! use tessera::memtable::{Memtable, TARGET_RUN_LEN};
//!
//! let mut table = Memtable::new(1 << 20, None);
//! table.put(b"apple", 1, b"red")?;
//! table.put(b"apple", 2, b"green")?;
//! table.delete(b"pear", 3)?;
//! assert_eq!(table.get(b"apple").unwrap().value, Value::Bytes(b"green"));
//!
//! let frozen = table.freeze();
//! let runs: Vec<_> = frozen.flush(TARGET_RUN_LEN).collect();
//! assert_eq!(runs[0].len(), 2);
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! ```
//!
//! A frozen memtable cannot be written to:
//!
//! ```compile_fail,E0599
//! # use tessera::memtable::Memtable;
//! let mut table = Memtable::new(1 << 20, None);
//! let frozen = table.freeze();
//! frozen.put(b"apple", 1, b"red");
//! ```
//!
//! # Versions
//!
//! A memtable keeps only the latest update of each key. An update replaces the
//! previous one and frees its value at once. A delete is stored as a tombstone,
//! even when the table holds nothing for that key, because older runs may. A
//! snapshot could not read a replaced value, so snapshots will require this
//! table to keep the versions that some snapshot can still see.
//!
//! # Sequence numbers
//!
//! The caller assigns sequence numbers, and a memtable only checks them. Every
//! accepted update must have a sequence number above all earlier ones in the
//! table, and above the `after` given to [`Memtable::new`], so that a table can
//! continue from its frozen predecessor. Updates are never reordered.
//!
//! Every value of `u64` is usable. After `u64::MAX` no number is larger, so
//! every further update is rejected as
//! [`SequenceNotIncreasing`](WriteError::SequenceNotIncreasing). Nothing is
//! ever incremented, so nothing can wrap.
//!
//! A rejected update changes nothing, and its sequence number stays unused, so
//! it may be retried with the same number.
//!
//! # Memory
//!
//! A table admits an update only if its charged bytes stay within the budget.
//! An entry is charged the length of its key, the length of its value and
//! [`ENTRY_OVERHEAD`]. Replacing a value charges only the difference in value
//! length, so an update that does not grow its entry is always admitted.
//!
//! The charge is an estimate of what the table allocates, not a limit on
//! resident memory: allocator rounding and headers are not charged, and
//! replacing a value briefly holds both values. `docs/benchmarks.md` compares
//! the charge with measured memory.

use std::collections::{BTreeMap, btree_map};
use std::fmt;
use std::ops::Bound;

use crate::block::{MAX_KEY_LEN, MAX_VALUE_LEN, Record, Value, Version};
use crate::run::{RECORD_INPUT_OVERHEAD, Run, RunBuilder};

/// Bytes charged per entry beyond its key and value.
///
/// This covers `BTreeMap` nodes when leaves hold their minimum of five of
/// eleven entries, with internal nodes above them, or about 119 bytes on 64-bit
/// targets. The standard library does not promise that layout, so this is an
/// estimate.
pub const ENTRY_OVERHEAD: usize = 128;

/// Input per output run suggested for [`FrozenMemtable::flush`].
pub const TARGET_RUN_LEN: usize = 64 << 20;

pub use crate::run::MAX_TARGET_RUN_LEN;

const FLUSHABLE: &str = "records are ordered, within the limits, and fit in one run";

/// The latest update of one key.
struct Slot {
    seq: u64,
    /// `None` is a tombstone. An empty value allocates nothing.
    value: Option<Box<[u8]>>,
}

impl Slot {
    fn value(&self) -> Value<'_> {
        self.value.as_deref().map_or(Value::Tombstone, Value::Bytes)
    }

    fn value_len(&self) -> usize {
        self.value.as_deref().map_or(0, <[u8]>::len)
    }
}

/// How much memory a table accounts for.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct MemoryUsage {
    /// Key and value bytes retained.
    pub payload: usize,
    /// Bytes charged against the budget, which never exceed it.
    pub charged: usize,
    /// The table's budget.
    pub budget: usize,
}

/// Why an update was rejected. The table is unchanged.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WriteError {
    /// The key exceeds [`MAX_KEY_LEN`].
    KeyTooLong {
        /// Length of the rejected key.
        len: usize,
    },
    /// The value exceeds [`MAX_VALUE_LEN`].
    ValueTooLong {
        /// Length of the rejected value.
        len: usize,
    },
    /// The sequence number is not above the last one accepted.
    SequenceNotIncreasing {
        /// The rejected sequence number.
        seq: u64,
        /// The sequence number it must exceed.
        last: u64,
    },
    /// The record alone exceeds the budget, so no empty table of this budget
    /// could admit it.
    RecordExceedsBudget {
        /// What the record would be charged in an empty table.
        charge: usize,
        /// The table's budget.
        budget: usize,
    },
    /// The update does not fit now. A new table may admit it.
    BudgetExceeded {
        /// Additional bytes the update needs.
        needed: usize,
        /// Bytes available to it, including any it would release.
        available: usize,
    },
}

impl fmt::Display for WriteError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match *self {
            Self::KeyTooLong { len } => {
                write!(f, "key of {len} bytes exceeds limit of {MAX_KEY_LEN}")
            }
            Self::ValueTooLong { len } => {
                write!(f, "value of {len} bytes exceeds limit of {MAX_VALUE_LEN}")
            }
            Self::SequenceNotIncreasing { seq, last } => {
                write!(f, "sequence number {seq} is not above {last}")
            }
            Self::RecordExceedsBudget { charge, budget } => {
                write!(
                    f,
                    "record charged {charge} bytes exceeds budget of {budget}"
                )
            }
            Self::BudgetExceeded { needed, available } => {
                write!(
                    f,
                    "update needs {needed} bytes and {available} are available"
                )
            }
        }
    }
}

impl std::error::Error for WriteError {}

/// The state a table has both while mutable and once frozen.
struct Table {
    entries: BTreeMap<Box<[u8]>, Slot>,
    charged: usize,
    budget: usize,
    last_seq: Option<u64>,
}

impl Table {
    fn write(&mut self, key: &[u8], seq: u64, value: Value<'_>) -> Result<(), WriteError> {
        let bytes = match value {
            Value::Bytes(bytes) => Some(bytes),
            Value::Tombstone => None,
        };
        let value_len = bytes.map_or(0, <[u8]>::len);
        if key.len() > MAX_KEY_LEN {
            return Err(WriteError::KeyTooLong { len: key.len() });
        }
        if value_len > MAX_VALUE_LEN {
            return Err(WriteError::ValueTooLong { len: value_len });
        }
        if let Some(last) = self.last_seq.filter(|&last| seq <= last) {
            return Err(WriteError::SequenceNotIncreasing { seq, last });
        }
        // Both lengths are within the limits above, so this cannot overflow.
        let charge = key.len() + value_len + ENTRY_OVERHEAD;
        if charge > self.budget {
            return Err(WriteError::RecordExceedsBudget {
                charge,
                budget: self.budget,
            });
        }

        let slot = self.entries.get_mut(key);
        let (needed, released) = match &slot {
            Some(slot) => (value_len, slot.value_len()),
            None => (charge, 0),
        };
        let available = self.budget - self.charged + released;
        if needed > available {
            return Err(WriteError::BudgetExceeded { needed, available });
        }

        // Nothing can be rejected from here on.
        let replacement = Slot {
            seq,
            value: bytes.map(Box::from),
        };
        match slot {
            Some(slot) => *slot = replacement,
            None => {
                self.entries.insert(Box::from(key), replacement);
            }
        }
        self.charged = self.charged - released + needed;
        self.last_seq = Some(seq);
        Ok(())
    }

    fn get(&self, key: &[u8]) -> Option<Version<'_>> {
        self.entries.get(key).map(|slot| Version {
            seq: slot.seq,
            value: slot.value(),
        })
    }

    fn iter_from(&self, start: &[u8]) -> Records<'_> {
        Records(
            self.entries
                .range::<[u8], _>((Bound::Included(start), Bound::Unbounded)),
        )
    }

    fn memory(&self) -> MemoryUsage {
        MemoryUsage {
            payload: self.charged - self.entries.len() * ENTRY_OVERHEAD,
            charged: self.charged,
            budget: self.budget,
        }
    }
}

impl fmt::Debug for Table {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Table")
            .field("len", &self.entries.len())
            .field("memory", &self.memory())
            .field("last_seq", &self.last_seq)
            .finish_non_exhaustive()
    }
}

/// An ordered, bounded table of the latest update to each key.
///
/// See the [module documentation](self) for its version, sequence number and
/// memory rules. Writes take `&mut self`: this type is single-writer.
#[derive(Debug)]
pub struct Memtable(Table);

impl Memtable {
    /// Creates an empty table limited to `budget` charged bytes.
    ///
    /// Updates must have sequence numbers above `after`. A table that follows
    /// a frozen one should pass [`FrozenMemtable::last_seq`] here.
    pub fn new(budget: usize, after: Option<u64>) -> Self {
        Self(Table {
            entries: BTreeMap::new(),
            charged: 0,
            budget,
            last_seq: after,
        })
    }

    /// Sets `key` to `value` at sequence number `seq`.
    ///
    /// Key and value limits are checked first, then the sequence number, then
    /// the budget. A rejected update changes nothing.
    pub fn put(&mut self, key: &[u8], seq: u64, value: &[u8]) -> Result<(), WriteError> {
        self.0.write(key, seq, Value::Bytes(value))
    }

    /// Stores a tombstone for `key` at sequence number `seq`, under the same
    /// rules as [`put`](Self::put).
    pub fn delete(&mut self, key: &[u8], seq: u64) -> Result<(), WriteError> {
        self.0.write(key, seq, Value::Tombstone)
    }

    /// Returns the latest update of `key`, which may be a tombstone, or `None`
    /// if the table holds nothing for it.
    pub fn get(&self, key: &[u8]) -> Option<Version<'_>> {
        self.0.get(key)
    }

    /// Returns each key's latest update, in ascending key order.
    pub fn iter(&self) -> Records<'_> {
        self.0.iter_from(&[])
    }

    /// Returns each update whose key is at or above `start`, in ascending key
    /// order.
    pub fn iter_from(&self, start: &[u8]) -> Records<'_> {
        self.0.iter_from(start)
    }

    /// Returns the number of keys held, including tombstones.
    pub fn len(&self) -> usize {
        self.0.entries.len()
    }

    /// Returns whether the table holds no keys.
    pub fn is_empty(&self) -> bool {
        self.0.entries.is_empty()
    }

    /// Returns the sequence number every update must exceed, if any.
    pub fn last_seq(&self) -> Option<u64> {
        self.0.last_seq
    }

    /// Returns the table's memory accounting.
    pub fn memory(&self) -> MemoryUsage {
        self.0.memory()
    }

    /// Ends the table's mutable life, in constant time and without copying.
    pub fn freeze(self) -> FrozenMemtable {
        FrozenMemtable(self.0)
    }
}

/// A memtable that can no longer change.
#[derive(Debug)]
pub struct FrozenMemtable(Table);

impl FrozenMemtable {
    /// Returns the latest update of `key`, which may be a tombstone, or `None`
    /// if the table holds nothing for it.
    pub fn get(&self, key: &[u8]) -> Option<Version<'_>> {
        self.0.get(key)
    }

    /// Returns each key's latest update, in ascending key order.
    pub fn iter(&self) -> Records<'_> {
        self.0.iter_from(&[])
    }

    /// Returns each update whose key is at or above `start`, in ascending key
    /// order.
    pub fn iter_from(&self, start: &[u8]) -> Records<'_> {
        self.0.iter_from(start)
    }

    /// Returns the number of keys held, including tombstones.
    pub fn len(&self) -> usize {
        self.0.entries.len()
    }

    /// Returns whether the table holds no keys.
    pub fn is_empty(&self) -> bool {
        self.0.entries.is_empty()
    }

    /// Returns the sequence number a successor's updates must exceed, if any.
    pub fn last_seq(&self) -> Option<u64> {
        self.0.last_seq
    }

    /// Returns the table's memory accounting.
    pub fn memory(&self) -> MemoryUsage {
        self.0.memory()
    }

    /// Returns an iterator that builds this table's runs one at a time, with
    /// the default codec and block size.
    ///
    /// A run ends once its records reach `target_run_len` bytes of input,
    /// reduced to [`MAX_TARGET_RUN_LEN`] if larger, so runs hold disjoint key
    /// ranges. An empty table produces no runs. Every tombstone is written.
    ///
    /// The table is only borrowed. A caller that cannot use a run can drop the
    /// iterator and flush again, which produces identical runs. The table and
    /// the runs built so far coexist until the caller drops the table.
    pub fn flush(&self, target_run_len: usize) -> Flush<'_> {
        Flush {
            records: self.0.entries.iter(),
            target: target_run_len.min(MAX_TARGET_RUN_LEN),
        }
    }
}

/// Iterates a table's latest updates in ascending key order.
pub struct Records<'a>(btree_map::Range<'a, Box<[u8]>, Slot>);

impl<'a> Iterator for Records<'a> {
    type Item = Record<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        self.0.next().map(|(key, slot)| Record {
            key,
            seq: slot.seq,
            value: slot.value(),
        })
    }
}

impl fmt::Debug for Records<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Records").finish_non_exhaustive()
    }
}

/// The runs of a [`FrozenMemtable`], built as they are requested.
pub struct Flush<'a> {
    records: btree_map::Iter<'a, Box<[u8]>, Slot>,
    target: usize,
}

impl Iterator for Flush<'_> {
    type Item = Run<Vec<u8>>;

    fn next(&mut self) -> Option<Self::Item> {
        let (mut key, mut slot) = self.records.next()?;
        let mut builder = RunBuilder::default();
        let mut input = 0;
        loop {
            builder.add(key, slot.seq, slot.value()).expect(FLUSHABLE);
            input += key.len() + slot.value_len() + RECORD_INPUT_OVERHEAD;
            if input >= self.target {
                break;
            }
            let Some(next) = self.records.next() else {
                break;
            };
            (key, slot) = next;
        }
        Some(builder.finish())
    }
}

impl fmt::Debug for Flush<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Flush")
            .field("target", &self.target)
            .finish_non_exhaustive()
    }
}
