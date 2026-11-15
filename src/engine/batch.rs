//! Owned, reusable atomic write batches.
use super::*;
use std::ops::Range;

#[derive(Clone, Debug)]
struct Operation {
    key: Range<usize>,
    value: Option<Range<usize>>,
}

/// A reusable list of puts and deletes, applied in insertion order.
/// Key and value bytes are copied into one reusable buffer.
#[derive(Clone, Debug, Default)]
pub struct WriteBatch {
    bytes: Vec<u8>,
    operations: Vec<Operation>,
}

impl WriteBatch {
    /// Creates an empty batch without allocating.
    pub fn new() -> Self {
        Self::default()
    }

    /// Appends a put, copying the key and value.
    pub fn put(&mut self, key: &[u8], value: &[u8]) {
        let key = self.append(key);
        let value = Some(self.append(value));
        self.operations.push(Operation { key, value });
    }

    /// Appends a delete, copying the key.
    pub fn delete(&mut self, key: &[u8]) {
        let key = self.append(key);
        self.operations.push(Operation { key, value: None });
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
    }

    fn append(&mut self, bytes: &[u8]) -> Range<usize> {
        let start = self.bytes.len();
        self.bytes.extend_from_slice(bytes);
        start..self.bytes.len()
    }

    fn records(&self) -> impl ExactSizeIterator<Item = (&[u8], Value<'_>)> + Clone {
        self.operations.iter().map(|op| {
            (
                &self.bytes[op.key.clone()],
                op.value.as_ref().map_or(Value::Tombstone, |range| {
                    Value::Bytes(&self.bytes[range.clone()])
                }),
            )
        })
    }
}

impl Core {
    pub(super) fn write_batch(&self, batch: &WriteBatch) -> Result<u64, UpdateError> {
        if batch.len() == 1 {
            let (key, value) = batch.records().next().unwrap();
            return self.update(key, value);
        }
        let mut charge = 0usize;
        for (key, value) in batch.records() {
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
            if record > self.memtable_budget {
                return Err(UpdateError::Rejected(WriteError::RecordExceedsBudget {
                    charge: record,
                    budget: self.memtable_budget,
                }));
            }
            charge = charge.saturating_add(record);
        }
        let mut state = self.write_state();
        loop {
            let end = state
                .last_seq
                .checked_add(batch.len() as u64)
                .ok_or(UpdateError::SequencesExhausted)?;
            if batch.is_empty() {
                return Ok(end);
            }
            let needed = batch.records().fold(0usize, |sum, (key, value)| {
                sum.saturating_add(state.active.write_charge(key, value))
            });
            let memory = state.active.memory();
            if needed <= memory.budget.saturating_sub(memory.charged)
                || state.active.is_empty() && charge > self.memtable_budget
            {
                if charge > memory.budget {
                    state.active.raise_budget(charge);
                }
                for (key, value) in batch.records() {
                    self.write(&mut state, key, value)
                        .expect("preflight admitted every batch operation");
                }
                return Ok(end);
            }
            state = self.make_room(state)?;
        }
    }
}
