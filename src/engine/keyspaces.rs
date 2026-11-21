//! Keyspaces inside the engine: each one's runs, levels and directory, and
//! the prefix that keeps its records apart in the shared memtables.
//!
//! Memtable keys are the keyspace ID as an LEB128 varint followed by the
//! caller's key. The varint is prefix-free, so each keyspace's records are
//! contiguous and keep their own order. A flush cuts at every keyspace
//! boundary and strips the prefix, so runs hold one keyspace's keys as the
//! caller wrote them, and the keyspace of a run is known only from the
//! keyspace that holds it, in memory and in the manifest.

use super::*;

/// The most live keyspaces an engine holds.
pub(super) const MAX_KEYSPACES: usize = 4096;

/// The longest prefix: a `u32` in LEB128.
pub(super) const MAX_PREFIX_LEN: usize = 5;

/// A keyspace ID as the prefix of its memtable keys.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct KeyPrefix {
    bytes: [u8; MAX_PREFIX_LEN],
    len: u8,
}

impl KeyPrefix {
    pub(super) fn new(id: u32) -> Self {
        let (mut bytes, mut len, mut rest) = ([0; MAX_PREFIX_LEN], 0, id);
        loop {
            let low = (rest & 0x7f) as u8;
            rest >>= 7;
            if rest == 0 {
                bytes[len] = low;
                len += 1;
                return Self {
                    bytes,
                    len: len as u8,
                };
            }
            bytes[len] = low | 0x80;
            len += 1;
        }
    }

    pub(super) fn as_bytes(&self) -> &[u8] {
        &self.bytes[..usize::from(self.len)]
    }

    /// `key` in this keyspace, as the memtables store it.
    pub(super) fn key<'a>(&'a self, key: &'a [u8]) -> Key<'a> {
        Key {
            prefix: self.as_bytes(),
            key,
        }
    }
}

/// The keyspace ID and the caller's key of a memtable key.
pub(super) fn split(stored: &[u8]) -> (u32, &[u8]) {
    let mut id = 0u32;
    for (at, &byte) in stored.iter().enumerate().take(MAX_PREFIX_LEN) {
        id |= u32::from(byte & 0x7f) << (7 * at);
        if byte & 0x80 == 0 {
            return (id, &stored[at + 1..]);
        }
    }
    unreachable!("memtable keys start with a keyspace prefix")
}

/// One keyspace's published runs and what is kept beside them.
pub(super) struct KeyspaceState {
    pub(super) name: Arc<str>,
    pub(super) prefix: KeyPrefix,
    /// Shared so that a compaction can read its inputs without the lock while
    /// flushes publish beside them.
    pub(super) runs: Vec<SharedRun>,
    /// The same runs, indexed for reading: grouped into key-disjoint runs,
    /// newest group first. Rebuilt by [`State::published`] in the same write
    /// section as every change to `runs`, so readers only ever see the two
    /// agree; `runs` keeps publication order, which compaction selection uses
    /// to break ties. Immutable once built, so a lookup, a scan or a snapshot
    /// shares it with one reference count.
    pub(super) directory: Arc<RunDirectory>,
    pub(super) run_bytes: usize,
    pub(super) held_bytes: usize,
    /// The same runs as compaction sees them, in levels. Changed in the same
    /// write section as `runs`, and never read by lookups or scans.
    pub(super) levels: Levels,
}

impl KeyspaceState {
    pub(super) fn new(id: u32, name: Arc<str>, level_ratio: usize) -> Self {
        Self {
            name,
            prefix: KeyPrefix::new(id),
            runs: Vec::new(),
            directory: Arc::new(RunDirectory::empty()),
            run_bytes: 0,
            held_bytes: 0,
            levels: Levels::new(level_ratio),
        }
    }

    /// Where a flush beginning now splits this keyspace's runs.
    pub(super) fn flush_cuts(&self, split: bool, target_run_len: usize) -> FlushCuts {
        if split {
            self.levels.flush_cuts(target_run_len)
        } else {
            FlushCuts::default()
        }
    }
}

/// Every live keyspace's directory, by ID: what a view reads its runs from.
pub(super) type Directories = Arc<[(u32, Arc<RunDirectory>)]>;

/// The directory of keyspace `id` in `directories`, if it is there.
pub(super) fn directory_of(directories: &Directories, id: u32) -> Option<&Arc<RunDirectory>> {
    directories
        .binary_search_by_key(&id, |(id, _)| *id)
        .ok()
        .map(|at| &directories[at].1)
}

impl State {
    /// The live keyspace `id`, or [`UpdateError::KeyspaceDropped`].
    pub(super) fn keyspace(&self, id: u32) -> Result<&KeyspaceState, UpdateError> {
        self.keyspaces.get(&id).ok_or(UpdateError::KeyspaceDropped)
    }

    /// Refuses an update to keyspace `id` unless it is live.
    pub(super) fn check_keyspace(&self, id: u32) -> Result<(), UpdateError> {
        self.keyspace(id).map(|_| ())
    }

    /// The live keyspace `id`, which the caller knows is live.
    pub(super) fn space_mut(&mut self, id: u32) -> &mut KeyspaceState {
        self.keyspaces.get_mut(&id).expect("a live keyspace")
    }

    /// Creates keyspace `name` with the next ID.
    pub(super) fn create_keyspace(
        &mut self,
        name: &str,
        level_ratio: usize,
    ) -> Result<u32, UpdateError> {
        if self.keyspaces.len() >= MAX_KEYSPACES {
            return Err(UpdateError::Keyspaces);
        }
        let id = self.next_keyspace;
        self.next_keyspace = id.checked_add(1).ok_or(UpdateError::Keyspaces)?;
        let name: Arc<str> = Arc::from(name);
        self.names.insert(Arc::clone(&name), id);
        self.keyspaces
            .insert(id, KeyspaceState::new(id, name, level_ratio));
        self.refresh_directories();
        Ok(id)
    }

    /// Rebuilds keyspace `id`'s directory and byte counts after its runs
    /// or levels changed, and what the engine keeps over all keyspaces.
    /// Publication is rare, so this builds a fresh directory rather than
    /// maintaining one, in O(n log n) for n runs, and readers holding the
    /// previous directory keep it.
    pub(super) fn published(&mut self, id: u32) {
        let space = self.keyspaces.get_mut(&id).expect("a live keyspace");
        space.directory = Arc::new(RunDirectory::build(&space.runs));
        if cfg!(any(test, debug_assertions)) {
            space.levels.check(&space.runs);
        }
        let (run_bytes, held_bytes) = (space.run_bytes, space.held_bytes);
        space.run_bytes = physical_runs(&space.runs).map(|r| r.run.stored_len()).sum();
        space.held_bytes = physical_runs(&space.runs).map(|r| r.run.held_bytes()).sum();
        self.run_bytes = self.run_bytes - run_bytes + space.run_bytes;
        self.held_bytes = self.held_bytes - held_bytes + space.held_bytes;
        self.refresh_directories();
        self.levels_changed();
    }

    /// Recomputes the deepest level 0 after levels changed.
    pub(super) fn levels_changed(&mut self) {
        self.level0_flushes = self
            .keyspaces
            .values()
            .map(|space| space.levels.level0_flushes())
            .max()
            .unwrap_or(0);
    }

    pub(super) fn refresh_directories(&mut self) {
        self.directories = self
            .keyspaces
            .iter()
            .map(|(&id, space)| (id, Arc::clone(&space.directory)))
            .collect();
    }

    /// Retires those of `inputs` that keyspace `id` does not hold.
    pub(super) fn retire_runs(&mut self, id: u32, inputs: &[SharedRun]) {
        let live: HashSet<_> = self
            .keyspaces
            .get(&id)
            .into_iter()
            .flat_map(|space| &space.runs)
            .map(|r| r.run.storage_id())
            .collect();
        for input in physical_runs(inputs).filter(|r| !live.contains(&r.run.storage_id())) {
            self.retire(
                input.run.retirement(),
                input.run.stored_len(),
                Some(input.run.held_bytes()),
            );
        }
    }

    /// Removes keyspace `name` and retires its runs; returns whether it
    /// was live. Its memtable records stay until a flush leaves them out.
    pub(super) fn remove_keyspace(&mut self, name: &str) -> bool {
        let Some(id) = self.names.remove(name) else {
            return false;
        };
        let space = self
            .keyspaces
            .remove(&id)
            .expect("named keyspaces are live");
        self.run_bytes -= space.run_bytes;
        self.held_bytes -= space.held_bytes;
        self.retire_runs(id, &space.runs);
        self.refresh_directories();
        self.levels_changed();
        true
    }

    /// The live keyspaces' prefixes and flush cuts, sorted by prefix, as a
    /// flush beginning now uses them.
    pub(super) fn flush_spaces(&self, target_run_len: usize) -> Vec<(u32, KeyPrefix, FlushCuts)> {
        #[cfg(test)]
        let split = self.split_flushes;
        #[cfg(not(test))]
        let split = true;
        let mut spaces: Vec<_> = self
            .keyspaces
            .iter()
            .map(|(&id, space)| (id, space.prefix, space.flush_cuts(split, target_run_len)))
            .collect();
        spaces.sort_by(|a, b| a.1.as_bytes().cmp(b.1.as_bytes()));
        spaces
    }

    /// The single live keyspace, for tests of one keyspace.
    #[cfg(test)]
    pub(super) fn only(&self) -> &KeyspaceState {
        assert_eq!(self.keyspaces.len(), 1, "one live keyspace");
        self.keyspaces.values().next().unwrap()
    }

    #[cfg(test)]
    pub(super) fn only_mut(&mut self) -> &mut KeyspaceState {
        assert_eq!(self.keyspaces.len(), 1, "one live keyspace");
        self.keyspaces.values_mut().next().unwrap()
    }

    /// The single live keyspace's ID.
    #[cfg(test)]
    pub(super) fn only_id(&self) -> u32 {
        assert_eq!(self.keyspaces.len(), 1, "one live keyspace");
        *self.keyspaces.keys().next().unwrap()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn prefixes_are_leb128_and_prefix_free() {
        assert_eq!(KeyPrefix::new(0).as_bytes(), [0]);
        assert_eq!(KeyPrefix::new(127).as_bytes(), [0x7f]);
        assert_eq!(KeyPrefix::new(128).as_bytes(), [0x80, 0x01]);
        assert_eq!(KeyPrefix::new(16_383).as_bytes(), [0xff, 0x7f]);
        assert_eq!(KeyPrefix::new(16_384).as_bytes(), [0x80, 0x80, 0x01]);
        assert_eq!(
            KeyPrefix::new(u32::MAX).as_bytes(),
            [0xff, 0xff, 0xff, 0xff, 0x0f]
        );
        let ids = (0..300u32).chain([16_383, 16_384, 1 << 21, 1 << 28, u32::MAX]);
        let prefixes: Vec<KeyPrefix> = ids.clone().map(KeyPrefix::new).collect();
        for id in ids {
            let stored = [KeyPrefix::new(id).as_bytes(), b"key"].concat();
            assert_eq!(split(&stored), (id, &b"key"[..]));
        }
        for a in &prefixes {
            for b in &prefixes {
                if a != b {
                    assert!(!b.as_bytes().starts_with(a.as_bytes()), "{a:?} {b:?}");
                }
            }
        }
    }
}
