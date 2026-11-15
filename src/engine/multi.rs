//! Batched lookups: one index sort, table passes, then one ordered pass per
//! run group. Values and metadata use reusable arenas; no per-key allocation.
use super::*;
use crate::run::{ReadWorkspace, RunList};
use std::ops::Range;

#[derive(Debug, Default, Clone)]
struct Hit {
    memory: bool,
    seq: Option<u64>,
    value: Option<Range<usize>>,
    input: usize,
    conflict: Option<(usize, usize)>,
    hash: Option<u64>,
}

#[derive(Debug, Default)]
pub(super) struct Workspace {
    order: Vec<usize>,
    probes: Vec<usize>,
    hits: Vec<Hit>,
    values: Vec<u8>,
    read: ReadWorkspace,
}

impl Workspace {
    pub(super) fn capacity(&self) -> usize {
        self.read.capacity()
    }
    pub(super) fn context_bytes(&self) -> usize {
        self.read.context_bytes()
    }
    pub(super) fn blocks_loaded(&self) -> u64 {
        self.read.blocks_loaded()
    }

    fn prepare<K: AsRef<[u8]>>(&mut self, keys: &[K]) {
        self.order.clear();
        self.order.extend(0..keys.len());
        if !keys
            .windows(2)
            .all(|pair| pair[0].as_ref() <= pair[1].as_ref())
        {
            self.order
                .sort_unstable_by(|&a, &b| keys[a].as_ref().cmp(keys[b].as_ref()));
        }
        self.hits.clear();
        self.hits.resize_with(keys.len(), Hit::default);
        self.values.clear();
    }

    fn table<'a, K: AsRef<[u8]>>(
        &mut self,
        keys: &[K],
        get: impl Fn(&[u8]) -> Option<Version<'a>>,
    ) {
        for &index in &self.order {
            if self.hits[index].memory {
                continue;
            }
            if let Some(version) = get(keys[index].as_ref()) {
                self.hits[index].set(version, 0, &mut self.values);
                self.hits[index].memory = true;
            }
        }
    }

    fn runs<K: AsRef<[u8]>>(
        &mut self,
        keys: &[K],
        directory: &RunDirectory,
    ) -> Result<(), ReadError> {
        for group in directory.groups() {
            let mut next = 0;
            for input in group {
                let run = directory.run(input);
                let Some(range) = run.key_range() else {
                    continue;
                };
                while next < self.order.len() && keys[self.order[next]].as_ref() < range.min {
                    next += 1;
                }
                self.probes.clear();
                while next < self.order.len() && keys[self.order[next]].as_ref() <= range.max {
                    let index = self.order[next];
                    next += 1;
                    let hit = &mut self.hits[index];
                    if hit.memory || hit.seq.is_some_and(|seq| seq > run.max_seq()) {
                        continue;
                    }
                    if !run.excludes(keys[index].as_ref(), &mut hit.hash) {
                        self.probes.push(index);
                    }
                }
                if self.probes.is_empty() {
                    continue;
                }
                let Self {
                    read,
                    probes,
                    hits,
                    values,
                    ..
                } = self;
                run.view().get_many(keys, probes, read, |index, version| {
                    if let Some(version) = version {
                        hits[index].consider(version, input, values);
                    }
                })?;
            }
        }
        for (index, hit) in self.hits.iter().enumerate() {
            if let Some(inputs) = hit.conflict {
                return Err(ReadError::ConflictingCopies(ConflictingCopies {
                    key: keys[index].as_ref().to_vec(),
                    seq: hit.seq.unwrap(),
                    inputs,
                }));
            }
        }
        Ok(())
    }

    fn deliver(&self, mut found: impl FnMut(usize, Option<&[u8]>)) {
        for (index, hit) in self.hits.iter().enumerate() {
            found(
                index,
                hit.value.as_ref().map(|range| &self.values[range.clone()]),
            );
        }
    }
}

impl Hit {
    fn set(&mut self, version: Version<'_>, input: usize, values: &mut Vec<u8>) {
        self.seq = Some(version.seq);
        self.input = input;
        self.conflict = None;
        self.value = match version.value {
            Value::Tombstone => None,
            Value::Bytes(bytes) => {
                let start = values.len();
                values.extend_from_slice(bytes);
                Some(start..values.len())
            }
        };
    }
    fn consider(&mut self, version: Version<'_>, input: usize, values: &mut Vec<u8>) {
        match self.seq {
            Some(seq) if seq > version.seq => (),
            Some(seq) if seq == version.seq => {
                let equal = match (&self.value, version.value) {
                    (None, Value::Tombstone) => true,
                    (Some(range), Value::Bytes(bytes)) => &values[range.clone()] == bytes,
                    _ => false,
                };
                if !equal && self.conflict.is_none() {
                    self.conflict = Some((self.input, input));
                }
            }
            _ => self.set(version, input, values),
        }
    }
}

impl Engine {
    /// Looks up keys in any order, invoking `found(index, value)` in input
    /// order, including duplicates and misses. The entire call sees one state.
    /// Values borrow reusable workspace storage for the callback's duration.
    /// No callbacks run if reading fails. Run decoding holds no state lock.
    pub fn multi_get<K: AsRef<[u8]>>(
        &self,
        keys: &[K],
        workspace: &mut EngineWorkspace,
        found: impl FnMut(usize, Option<&[u8]>),
    ) -> Result<(), ReadError> {
        let multi = &mut workspace.multi;
        multi.prepare(keys);
        let state = self.core.read_state();
        multi.table(keys, |key| state.active.get(key));
        for table in state.frozen.iter().rev() {
            multi.table(keys, |key| table.get(key));
        }
        let directory = Arc::clone(&state.directory);
        drop(state);
        multi.runs(keys, &directory)?;
        multi.deliver(found);
        Ok(())
    }
}

impl ReadGuard<'_> {
    /// Looks up keys in input order from this guard's stable state. See
    /// [`Engine::multi_get`] for the callback and workspace contract.
    pub fn multi_get<K: AsRef<[u8]>>(
        &self,
        keys: &[K],
        workspace: &mut EngineWorkspace,
        found: impl FnMut(usize, Option<&[u8]>),
    ) -> Result<(), ReadError> {
        let multi = &mut workspace.multi;
        multi.prepare(keys);
        multi.table(keys, |key| self.0.active.get(key));
        for table in self.0.frozen.iter().rev() {
            multi.table(keys, |key| table.get(key));
        }
        multi.runs(keys, &self.0.directory)?;
        multi.deliver(found);
        Ok(())
    }
}

impl Snapshot {
    /// Looks up keys in input order from this snapshot's state. See
    /// [`Engine::multi_get`] for the callback and workspace contract.
    pub fn multi_get<K: AsRef<[u8]>>(
        &self,
        keys: &[K],
        workspace: &mut EngineWorkspace,
        found: impl FnMut(usize, Option<&[u8]>),
    ) -> Result<(), ReadError> {
        let multi = &mut workspace.multi;
        multi.prepare(keys);
        for table in &self.tables {
            multi.table(keys, |key| table.get(key));
        }
        multi.runs(keys, &self.runs)?;
        multi.deliver(found);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::run::RunBuilder;

    fn run(records: &[(&[u8], u64, Value<'_>)]) -> SharedRun {
        let mut builder = RunBuilder::default();
        for &(key, seq, value) in records {
            builder.add(key, seq, value).unwrap();
        }
        published(vec![builder.finish()]).next().unwrap()
    }

    #[test]
    fn conflicts_and_corruption_match_point_lookups() {
        let a = run(&[(b"a", 10, Value::Bytes(b"one"))]);
        let b = run(&[(b"a", 10, Value::Bytes(b"two"))]);
        let c = run(&[(b"a", 11, Value::Tombstone)]);
        let damaged_source = run(&[(b"z", 1, Value::Bytes(b"old"))]);
        let damaged = Arc::new(Published {
            run: damaged_source.run.damaged_at(16),
            largest_block: 0,
        });
        for runs in [
            vec![a.clone(), b.clone()],
            vec![a.clone(), b, c],
            vec![a, damaged],
        ] {
            let directory = RunDirectory::build(&runs);
            for key in [b"a", b"z", b"missing".as_slice()] {
                let mut logical = LogicalWorkspace::default();
                let expected = latest_from(directory.lookup(), key, &mut logical)
                    .map(|value| value.map(|v| v.value.to_vec()));
                let mut workspace = Workspace::default();
                workspace.prepare(&[key]);
                let mut actual = None;
                let result = workspace.runs(&[key], &directory);
                if result.is_ok() {
                    workspace.deliver(|_, value| actual = value.map(<[u8]>::to_vec));
                }
                assert_eq!(result.map(|()| actual), expected);
            }
        }
    }
}
