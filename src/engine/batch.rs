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
    /// The engine and name of the first keyspace appended.
    engine: Option<(u64, Arc<str>)>,
    /// The name of a keyspace of another engine, if one was appended.
    foreign: Option<Arc<str>>,
    /// Whether operations name more than one keyspace.
    mixed: bool,
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
        self.mixed = false;
    }

    fn note(&mut self, keyspace: &Keyspace) {
        let (engine, _) = self
            .engine
            .get_or_insert_with(|| (keyspace.engine_id(), keyspace.shared_name()));
        if *engine != keyspace.engine_id() && self.foreign.is_none() {
            self.foreign = Some(keyspace.shared_name());
        }
        self.mixed |= self
            .operations
            .first()
            .is_some_and(|first| first.keyspace != keyspace.id());
    }

    fn append(&mut self, bytes: &[u8]) -> Range<usize> {
        let start = self.bytes.len();
        self.bytes.extend_from_slice(bytes);
        start..self.bytes.len()
    }

    /// Total key and value payload bytes held by the batch.
    pub(super) fn payload_bytes(&self) -> usize {
        self.bytes.len()
    }

    /// Each operation's keyspace, key and value, in insertion order.
    pub(super) fn records(&self) -> impl ExactSizeIterator<Item = (u32, &[u8], Value<'_>)> + Clone {
        self.operations.iter().map(|op| self.operation_record(op))
    }

    /// The operation at this position, borrowing directly from the batch.
    pub(super) fn record(&self, index: usize) -> (u32, &[u8], Value<'_>) {
        self.operation_record(&self.operations[index])
    }

    fn operation_record(&self, op: &Operation) -> (u32, &[u8], Value<'_>) {
        (
            op.keyspace,
            &self.bytes[op.key.clone()],
            op.value.as_ref().map_or(Value::Tombstone, |range| {
                Value::Bytes(&self.bytes[range.clone()])
            }),
        )
    }

    /// Checks that every keyspace in the batch belongs to `engine`.
    pub(super) fn check_engine(&self, engine: u64) -> Result<(), Error> {
        let Some((first_engine, first_name)) = &self.engine else {
            return Ok(());
        };
        if *first_engine != engine {
            return Err(if self.foreign.is_some() {
                Error::unknown_keyspace(first_name)
            } else {
                Error::new(
                    ErrorKind::UnknownKeyspace,
                    "the batch's keyspaces belong to another engine",
                )
            });
        }
        if let Some(name) = &self.foreign {
            return Err(Error::unknown_keyspace(name));
        }
        Ok(())
    }

    /// The keyspaces the operations name, ascending and each once.
    pub(super) fn keyspaces(&self) -> Vec<u32> {
        let mut ids: Vec<u32> = match self.operations.first() {
            Some(first) if !self.mixed => vec![first.keyspace],
            _ => self.operations.iter().map(|op| op.keyspace).collect(),
        };
        ids.sort_unstable();
        ids.dedup();
        ids
    }

    /// Refuses the whole batch unless every keyspace it names is live.
    pub(super) fn check_keyspaces(&self, state: &State) -> Result<(), UpdateError> {
        match self.operations.first() {
            None => Ok(()),
            Some(first) if !self.mixed => state.check_keyspace(first.keyspace),
            Some(_) => self
                .keyspaces()
                .into_iter()
                .try_for_each(|id| state.check_keyspace(id)),
        }
    }
}

/// The batch's memtable charge and the bytes needed to stage its prefixed keys.
pub(super) struct BatchSize {
    pub(super) charge: usize,
    pub(super) key_bytes: usize,
}

impl WriteBatch {
    /// Whether the operations are strictly ascending in the memtables'
    /// order: by keyspace prefix, then key.
    pub(super) fn is_sorted(&self) -> bool {
        self.operations.windows(2).all(|pair| {
            let [a, b] = [&pair[0], &pair[1]]
                .map(|op| (KeyPrefix::new(op.keyspace), &self.bytes[op.key.clone()]));
            (a.0.as_bytes(), a.1) < (b.0.as_bytes(), b.1)
        })
    }

    pub(super) fn validate(&self, budget: usize) -> Result<BatchSize, UpdateError> {
        let mut charge = 0usize;
        let mut key_bytes = 0;
        for (keyspace, key, value) in self.records() {
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
            // As the memtable charges it: the key behind its keyspace prefix.
            let key_len = KeyPrefix::new(keyspace).as_bytes().len() + key.len();
            let record = key_len + len + ENTRY_OVERHEAD;
            if record > budget {
                return Err(UpdateError::Rejected(WriteError::RecordExceedsBudget {
                    charge: record,
                    budget,
                }));
            }
            charge = charge.saturating_add(record);
            key_bytes += key_len;
        }
        Ok(BatchSize { charge, key_bytes })
    }
}

impl Core {
    /// Applies `batch` atomically and returns its last sequence number, or
    /// the current one for an empty batch.
    pub(super) fn write_batch(&self, batch: &WriteBatch) -> Result<u64, UpdateError> {
        if batch.is_empty() {
            return Ok(self.read_state().last_seq);
        }
        let size = batch.validate(self.memtable_budget)?;
        let charge = size.charge;
        if charge >= self.bulk_ingest_min || charge > self.memtable_budget {
            let sorted = batch.is_sorted();
            if sorted || charge > self.memtable_budget {
                return self.ingest_batch(batch, sorted, size);
            }
        }
        if batch.len() == 1 {
            let (keyspace, key, value) = batch.records().next().unwrap();
            return self.combine_single(keyspace, key, value);
        }
        self.combine_batch(batch, charge)
    }

    pub(super) fn write_batch_direct(
        &self,
        batch: &WriteBatch,
        charge: usize,
    ) -> Result<u64, UpdateError> {
        let state = self.admit(self.write_state())?;
        self.apply_batch(state, batch, charge).0
    }

    pub(super) fn apply_batch<'s>(
        &'s self,
        mut state: RwLockWriteGuard<'s, State>,
        batch: &WriteBatch,
        charge: usize,
    ) -> (
        Result<u64, UpdateError>,
        Option<RwLockWriteGuard<'s, State>>,
    ) {
        loop {
            if let Err(error) = batch.check_keyspaces(&state) {
                return (Err(error), Some(state));
            }
            let end = state
                .last_seq
                .checked_add(batch.len() as u64)
                .expect(SEQUENCES);
            // Without shared storage, the validated charge already bounds the
            // append and a new slot for every operation. Only snapshots can
            // add copy-on-write costs, and none can start under the state lock.
            let needed = if state.active.has_shared_storage() {
                let (ops, copy) = batch.records().fold(
                    (0usize, 0usize),
                    |(ops, copy), (keyspace, key, value)| {
                        let prefix = KeyPrefix::new(keyspace);
                        let (op, chunk) = state.active.write_charge(prefix.key(key), value);
                        (ops.saturating_add(op), copy.max(chunk))
                    },
                );
                ops.saturating_add(copy)
            } else {
                charge
            };
            let memory = state.active.memory();
            if needed <= memory.budget.saturating_sub(memory.charged) {
                let was_below = memory.charged < self.quiet_tail_min();
                let single_prefix = (!batch.mixed)
                    .then(|| {
                        batch
                            .operations
                            .first()
                            .map(|op| KeyPrefix::new(op.keyspace))
                    })
                    .flatten();
                for (keyspace, key, value) in batch.records() {
                    let prefix = single_prefix.unwrap_or_else(|| KeyPrefix::new(keyspace));
                    let seq = state.last_seq.checked_add(1).expect(SEQUENCES);
                    state
                        .active
                        .write(prefix.key(key), seq, value)
                        .expect("preflight admitted every batch operation");
                    state.last_seq = seq;
                }
                self.chunks.ingested(batch.bytes.len());
                if was_below && self.quiet_tail_ready(&state) {
                    self.arm_quiet_tail(&state);
                }
                return (Ok(end), Some(state));
            }
            match self.admit(self.make_room(state)) {
                Ok(next) => state = next,
                Err(error) => return (Err(error), None),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mixed_engine_batch_names_the_foreign_keyspace() {
        let local_engine = Engine::in_memory(Options::default()).unwrap();
        let other_engine = Engine::in_memory(Options::default()).unwrap();
        let local = local_engine.keyspace("local").unwrap();
        let foreign = other_engine.keyspace("foreign").unwrap();

        let mut batch = WriteBatch::new();
        batch.put(&foreign, b"a", b"1");
        batch.put(&local, b"b", b"2");

        let error = local_engine.write(&batch).unwrap_err();
        assert_eq!(error.kind(), ErrorKind::UnknownKeyspace);
        assert!(
            error.to_string().contains("\"foreign\""),
            "expected foreign keyspace in error, got: {error}"
        );

        let mut reversed = WriteBatch::new();
        reversed.put(&local, b"a", b"1");
        reversed.put(&foreign, b"b", b"2");
        let error = local_engine.write(&reversed).unwrap_err();
        assert_eq!(error.kind(), ErrorKind::UnknownKeyspace);
        assert!(
            error.to_string().contains("\"foreign\""),
            "expected foreign keyspace in error, got: {error}"
        );
    }

    #[test]
    fn small_batch_under_snapshot_does_not_flush_partly_filled_memtable() {
        let engine = Engine::in_memory(Options {
            memtable_size: 1 << 20,
            background_compaction: None,
            ..Options::default()
        })
        .unwrap();
        let ks = engine.keyspace("data").unwrap();
        ks.put(b"seed", &[1u8; 64 << 10]).unwrap();
        let snapshot = engine.snapshot();
        let before = engine.engine_stats();

        let mut batch = WriteBatch::new();
        for i in 0..100u32 {
            batch.put(&ks, &i.to_be_bytes(), b"val");
        }
        engine.write(&batch).unwrap();

        let after = engine.engine_stats();
        assert_eq!(after.flushes, before.flushes);
        assert!(after.frozen.is_none());
        assert_eq!(after.runs, 0);
        assert_eq!(
            snapshot.reader().get(&ks, b"seed").unwrap(),
            Some(&[1u8; 64 << 10][..])
        );
        assert_eq!(
            ks.get(&99u32.to_be_bytes()).unwrap().as_deref(),
            Some(&b"val"[..])
        );
    }
}
