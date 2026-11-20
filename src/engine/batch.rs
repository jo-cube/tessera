//! Owned, reusable atomic write batches. Operations retain insertion order
//! and share a byte arena; complete preflight precedes any publication so
//! rejected batches consume no sequences and expose no partial writes.

use super::*;
use std::ops::Range;

#[derive(Clone, Debug)]
struct Operation {
    keyspace: u32,
    key: Range<usize>,
    value: Option<Range<usize>>,
}

/// A reusable list of puts and deletes in the keyspaces of one engine,
/// applied atomically and in insertion order by [`Engine::write`]: later
/// operations on a key win. Key and value bytes are copied into one
/// reusable buffer when appended; limits are checked when it is written.
///
/// ```
/// use tessera::{Engine, Options, WriteBatch};
///
/// let engine = Engine::in_memory(Options::default())?;
/// let users = engine.keyspace("users")?;
/// let mut batch = WriteBatch::new();
/// batch.put(&users, b"user/1", b"ada");
/// batch.put(&users, b"user/2", b"grace");
/// batch.delete(&users, b"user/1");
/// engine.write(&batch)?;
/// assert_eq!(users.get(b"user/1")?, None);
/// assert_eq!(users.get(b"user/2")?.as_deref(), Some(&b"grace"[..]));
/// # Ok::<(), tessera::Error>(())
/// ```
#[derive(Clone, Debug, Default)]
pub struct WriteBatch {
    bytes: Vec<u8>,
    operations: Vec<Operation>,
    /// The engine of the first keyspace appended.
    engine: Option<u64>,
    /// The name of a keyspace of another engine, if one was appended.
    foreign: Option<Arc<str>>,
}

impl WriteBatch {
    /// Creates an empty batch without allocating.
    pub fn new() -> Self {
        Self::default()
    }

    /// Appends a put of `key` in `keyspace`, copying the key and value.
    pub fn put(&mut self, keyspace: &Keyspace, key: &[u8], value: &[u8]) {
        self.note(keyspace);
        let key = self.append(key);
        let value = Some(self.append(value));
        self.operations.push(Operation {
            keyspace: keyspace.id(),
            key,
            value,
        });
    }

    /// Appends a delete of `key` in `keyspace`, copying the key. Deleting a
    /// missing key succeeds.
    pub fn delete(&mut self, keyspace: &Keyspace, key: &[u8]) {
        self.note(keyspace);
        let key = self.append(key);
        self.operations.push(Operation {
            keyspace: keyspace.id(),
            key,
            value: None,
        });
    }

    /// Returns the number of operations, including repeated keys.
    pub fn len(&self) -> usize {
        self.operations.len()
    }

    /// Returns whether the batch has no operations.
    pub fn is_empty(&self) -> bool {
        self.operations.is_empty()
    }

    /// Removes all operations while keeping both buffers' capacity.
    pub fn clear(&mut self) {
        self.bytes.clear();
        self.operations.clear();
        self.engine = None;
        self.foreign = None;
    }

    fn note(&mut self, keyspace: &Keyspace) {
        let engine = *self.engine.get_or_insert(keyspace.engine_id());
        if engine != keyspace.engine_id() && self.foreign.is_none() {
            self.foreign = Some(keyspace.shared_name());
        }
    }

    fn append(&mut self, bytes: &[u8]) -> Range<usize> {
        let start = self.bytes.len();
        self.bytes.extend_from_slice(bytes);
        start..self.bytes.len()
    }

    pub(super) fn records(&self) -> impl ExactSizeIterator<Item = (&[u8], Value<'_>)> + Clone {
        self.operations.iter().map(|op| {
            (
                &self.bytes[op.key.clone()],
                op.value.as_ref().map_or(Value::Tombstone, |range| {
                    Value::Bytes(&self.bytes[range.clone()])
                }),
            )
        })
    }

    /// The engine its keyspaces belong to, or the name of one that belongs
    /// to another engine than the first.
    pub(super) fn engine(&self) -> Result<Option<u64>, &str> {
        match &self.foreign {
            Some(name) => Err(name),
            None => Ok(self.engine),
        }
    }

    /// The one keyspace every operation names, or `None` for an empty
    /// batch. Operations naming several keyspaces are refused: an engine
    /// holds one live keyspace, so one of them was dropped.
    pub(super) fn keyspace(&self) -> Result<Option<u32>, UpdateError> {
        let Some(first) = self.operations.first() else {
            return Ok(None);
        };
        if self
            .operations
            .iter()
            .all(|op| op.keyspace == first.keyspace)
        {
            Ok(Some(first.keyspace))
        } else {
            Err(UpdateError::KeyspaceDropped)
        }
    }
}

impl WriteBatch {
    pub(super) fn is_sorted(&self) -> bool {
        self.operations
            .windows(2)
            .all(|pair| self.bytes[pair[0].key.clone()] < self.bytes[pair[1].key.clone()])
    }

    pub(super) fn validate(&self, budget: usize) -> Result<usize, UpdateError> {
        let mut charge = 0usize;
        for (key, value) in self.records() {
            let len = match value {
                Value::Bytes(bytes) => bytes.len(),
                Value::Tombstone => 0,
            };
            if key.len() > MAX_KEY_LEN {
                return Err(UpdateError::Rejected(WriteError::KeyTooLong {
                    len: key.len(),
                }));
            }
            if len > MAX_VALUE_LEN {
                return Err(UpdateError::Rejected(WriteError::ValueTooLong { len }));
            }
            let record = key.len() + len + ENTRY_OVERHEAD;
            if record > budget {
                return Err(UpdateError::Rejected(WriteError::RecordExceedsBudget {
                    charge: record,
                    budget,
                }));
            }
            charge = charge.saturating_add(record);
        }
        Ok(charge)
    }
}

impl Core {
    /// Applies `batch` atomically and returns its last sequence number, or
    /// the current one for an empty batch.
    pub(super) fn write_batch(&self, batch: &WriteBatch) -> Result<u64, UpdateError> {
        let Some(keyspace) = batch.keyspace()? else {
            return Ok(self.read_state().last_seq);
        };
        let charge = batch.validate(self.memtable_budget)?;
        let sorted = batch.is_sorted();
        if !batch.is_empty()
            && (sorted && charge >= self.bulk_ingest_min || charge > self.memtable_budget)
        {
            return self.ingest_batch(keyspace, batch, sorted);
        }
        if batch.len() == 1 {
            let (key, value) = batch.records().next().unwrap();
            return self.update(keyspace, key, value);
        }
        let mut state = self.admit(self.write_state())?;
        loop {
            state.check_keyspace(keyspace)?;
            let end = state
                .last_seq
                .checked_add(batch.len() as u64)
                .expect(SEQUENCES);
            let needed = batch.records().fold(0usize, |sum, (key, value)| {
                sum.saturating_add(state.active.write_charge(key, value))
            });
            let memory = state.active.memory();
            if needed <= memory.budget.saturating_sub(memory.charged) {
                for (key, value) in batch.records() {
                    self.write(&mut state, key, value)
                        .expect("preflight admitted every batch operation");
                }
                return Ok(end);
            }
            state = self.admit(self.make_room(state))?;
        }
    }
}
