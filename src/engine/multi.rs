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
    #[cfg_attr(not(any(test, feature = "diagnostics")), allow(dead_code))]
    pub(super) fn capacity(&self) -> usize {
        self.read.capacity()
    }
    #[cfg_attr(not(any(test, feature = "diagnostics")), allow(dead_code))]
    pub(super) fn context_bytes(&self) -> usize {
        self.read.context_bytes()
    }
    #[cfg_attr(not(any(test, feature = "diagnostics")), allow(dead_code))]
    pub(super) fn blocks_loaded(&self) -> u64 {
        self.read.blocks_loaded()
    }

    /// Raw bytes in blocks loaded through this workspace.
    #[cfg_attr(not(any(test, feature = "diagnostics")), allow(dead_code))]
    pub(super) fn decoded_bytes(&self) -> u64 {
        self.read.decoded_bytes()
    }

    pub(super) fn prepare<K: AsRef<[u8]>>(&mut self, keys: &[K]) {
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

impl Workspace {
    /// The value of each key prepared, in input order.
    pub(super) fn found(&self) -> impl Iterator<Item = Option<&[u8]>> {
        self.hits
            .iter()
            .map(|hit| hit.value.as_ref().map(|range| &self.values[range.clone()]))
    }

    /// Looks up `keys` in `view`.
    pub(super) fn lookup_view<K: AsRef<[u8]>>(
        &mut self,
        keys: &[K],
        view: &View,
    ) -> Result<(), ReadError> {
        self.prepare(keys);
        for table in &view.tables {
            self.table(keys, |key| table.get(key));
        }
        self.runs(keys, &view.runs)
    }

    /// Releases buffers whose capacity exceeds `bytes`.
    pub(super) fn trim(&mut self, bytes: usize) {
        if self.values.capacity() > bytes {
            self.values = Vec::new();
        }
        if self.hits.capacity() * size_of::<Hit>() > bytes {
            self.hits = Vec::new();
        }
        if self.order.capacity() * size_of::<usize>() > bytes {
            self.order = Vec::new();
            self.probes = Vec::new();
        }
    }
}

impl Core {
    /// Looks up `keys` in keyspace `keyspace` from one state, leaving the
    /// values in `multi`. Run decoding holds no state lock.
    pub(super) fn multi_get<K: AsRef<[u8]>>(
        &self,
        keyspace: u32,
        keys: &[K],
        multi: &mut Workspace,
    ) -> Result<()> {
        multi.prepare(keys);
        let state = self.read_state();
        state.check_keyspace(keyspace)?;
        multi.table(keys, |key| state.active.get(key));
        for table in state.frozen.iter().rev() {
            multi.table(keys, |key| table.get(key));
        }
        let directory = Arc::clone(&state.directory);
        drop(state);
        multi.runs(keys, &directory)?;
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
            id: crate::engine::checkpoint::new_run_id(),
            checkpoint_crc: std::sync::OnceLock::new(),
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
                let result = workspace.runs(&[key], &directory);
                let actual = result.map(|()| workspace.found().next().unwrap().map(<[u8]>::to_vec));
                assert_eq!(actual, expected);
            }
        }
    }
    #[test]
    fn adjacent_keys_and_duplicates_decode_each_group_block_once() {
        let mut runs = Vec::new();
        let keys: Vec<_> = (0..120u64).map(u64::to_be_bytes).collect();
        for part in keys.chunks(40) {
            let mut builder = RunBuilder::with_raw_block_target(Codec::Zstd, 180);
            for (index, key) in part.iter().enumerate() {
                builder
                    .add(
                        key,
                        index as u64 + 1,
                        Value::Bytes(b"a moderately sized value"),
                    )
                    .unwrap();
            }
            runs.extend(published(vec![builder.finish()]));
        }
        let blocks: usize = runs.iter().map(|run| run.run.block_count()).sum();
        let directory = RunDirectory::build(&runs);
        assert_eq!(directory.group_count(), 1);
        let probes: Vec<_> = keys.iter().rev().chain(keys.iter()).collect();
        let mut workspace = Workspace::default();
        workspace.prepare(&probes);
        workspace.runs(&probes, &directory).unwrap();
        assert_eq!(workspace.blocks_loaded(), blocks as u64);
        for value in workspace.found() {
            assert_eq!(value, Some(&b"a moderately sized value"[..]));
        }
    }
}
