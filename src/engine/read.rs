//! Read workspaces, read guards, snapshots and scans.

use super::*;

/// Reusable buffers for reading an engine, created once per reader.
/// Each concurrent operation needs an exclusive mutable borrow; returned
/// values borrow this storage. Workspace allocations are outside admission
/// accounting.
#[derive(Debug, Default)]
pub struct EngineWorkspace {
    pub(super) logical: LogicalWorkspace,
    pub(super) value: Vec<u8>,
    prefix: Vec<u8>,
    pub(super) multi: multi::Workspace,
    /// Always empty between scans; keeps only the cursor allocation.
    pub(super) scan_tables: Vec<TableCursor<'static>>,
}

impl EngineWorkspace {
    /// Creates an empty workspace, which allocates nothing until first used.
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns the decoded-block bytes retained across all runs.
    ///
    /// Point lookups keep two blocks whatever the number of runs. Scans keep
    /// one per group of key-disjoint runs they have merged, at most the
    /// largest [`EngineStats::max_overlap`] they have read.
    pub fn capacity(&self) -> usize {
        self.logical.capacity() + self.multi.capacity()
    }

    /// Returns the bytes held by Zstandard decompression contexts, as
    /// Zstandard reports them: one context for point lookups, and one per
    /// group of runs a scan has decoded Zstandard blocks from. A context is
    /// created on first use and kept for the workspace's lifetime.
    ///
    /// These bytes are allocated outside the Rust heap, are not part of
    /// [`capacity`](Self::capacity), and are charged against no limit.
    pub fn context_bytes(&self) -> usize {
        self.logical.context_bytes() + self.multi.context_bytes()
    }

    /// Returns how many run blocks reads through this workspace have loaded.
    pub fn blocks_loaded(&self) -> u64 {
        self.logical.blocks_loaded() + self.multi.blocks_loaded()
    }

    /// Bloom-filter probes by point reads through this workspace, including
    /// probes that rule a run out. Scans and multi-get are excluded.
    #[cfg(feature = "diagnostics")]
    pub fn filter_probes(&self) -> u64 {
        self.logical.filter_probes()
    }

    /// Raw bytes in run blocks loaded through this workspace, including misses.
    pub fn decoded_bytes(&self) -> u64 {
        self.logical.decoded_bytes() + self.multi.decoded_bytes()
    }
}

/// A stable view of an [`Engine`], holding its state lock shared until it
/// is dropped, from [`Engine::read`].
///
/// Every read through it sees one state, and it copies and retains nothing.
/// In exchange, **writers, flushes and compaction publication wait while it
/// is held**, and a waiting writer holds back new readers. Keep it to a few
/// reads that must agree, and drop it promptly. For a long scan, or a view
/// kept while writes continue, take a [`Snapshot`]: it holds no lock, at the
/// cost of retaining what the engine releases while it lives.
///
/// Do not call [`Engine`] methods on a thread that holds a guard: a write
/// would deadlock. The guard borrows the engine, so the engine cannot be
/// closed while it is held. A guard is not `Send`; keep it on its acquiring
/// thread.
///
/// ```
/// use tessera::engine::{Engine, EngineOptions, EngineWorkspace};
///
/// let engine = Engine::new(EngineOptions::default())?;
/// engine.put(b"from", b"10")?;
/// engine.put(b"to", b"5")?;
/// let mut workspace = EngineWorkspace::new();
/// let (from, to) = {
///     let guard = engine.read();
///     let from = guard.get(b"from", &mut workspace)?.map(|v| v.value.to_vec());
///     let to = guard.get(b"to", &mut workspace)?.map(|v| v.value.to_vec());
///     (from, to)
/// }; // Writers wait until the guard is dropped here.
/// assert_eq!((from.as_deref(), to.as_deref()), (Some(&b"10"[..]), Some(&b"5"[..])));
/// engine.close()?;
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
pub struct ReadGuard<'e>(pub(super) RwLockReadGuard<'e, State>);

impl ReadGuard<'_> {
    /// Returns the latest value of `key`, or `None` if it is absent or
    /// deleted, without copying it. Returns [`ReadError`] for touched corruption
    /// or conflicting copies.
    pub fn get<'r>(
        &'r self,
        key: &[u8],
        workspace: &'r mut EngineWorkspace,
    ) -> Result<Option<LiveValue<'r>>, ReadError> {
        lookup_state(&self.0, key, &mut workspace.logical)
    }

    /// Returns a scan of every live key in bytewise ascending order.
    /// Decoding failures are returned by [`Scan::next_record`].
    pub fn scan<'r>(&'r self, workspace: &'r mut EngineWorkspace) -> Scan<'r> {
        self.scan_prefix(&[], workspace)
    }

    /// Returns a scan of every live key starting with the raw bytes `prefix`.
    /// An empty prefix selects all keys. Decoding failures are returned by
    /// [`Scan::next_record`].
    pub fn scan_prefix<'r>(
        &'r self,
        prefix: &[u8],
        workspace: &'r mut EngineWorkspace,
    ) -> Scan<'r> {
        let state = &*self.0;
        let tables = std::iter::once(state.active.iter_from(prefix)).chain(
            state
                .frozen
                .iter()
                .rev()
                .map(|table| table.iter_from(prefix)),
        );
        Scan::new(tables, &state.directory, prefix, workspace)
    }
}

impl fmt::Debug for ReadGuard<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ReadGuard").finish_non_exhaustive()
    }
}

/// The engine's state when [`Engine::snapshot`] returned, holding no lock.
///
/// It owns shared references to the frozen memtables and runs that made up
/// that state, which never change, so reads through it agree with each other
/// and are unaffected by later writes, flushes and compactions. Each source
/// is freed when neither the engine nor any snapshot holds it.
///
/// A snapshot is `Send` and `Sync`, may be read from several threads with a
/// workspace each, and may outlive its engine: it stays readable after
/// [`Engine::close`]. It cannot be refreshed: take a
/// new one to see later writes. While it lives it keeps whatever the engine
/// releases from its view, and the memtable history later writes copy away
/// from it, reported in [`EngineStats::retained_bytes`], so drop it once read.
/// Retained bytes count towards writer admission; see [Memory](super#memory).
///
/// Share one by reference or in an `Arc`:
///
/// ```compile_fail,E0599
/// # let engine = tessera::engine::Engine::new(Default::default()).unwrap();
/// let snapshot = engine.snapshot();
/// let second = snapshot.clone();
/// ```
pub struct Snapshot {
    /// Newest first.
    pub(super) tables: Vec<Arc<FrozenMemtable>>,
    pub(super) runs: Arc<RunDirectory>,
    pub(super) last_seq: u64,
    pub(super) _live: Arc<()>,
}

impl Snapshot {
    /// The last sequence number of the state this sees. Every update up to
    /// it is visible and no later one is.
    pub fn last_seq(&self) -> u64 {
        self.last_seq
    }

    /// Returns the value `key` had, or `None` if it was absent or deleted,
    /// without copying it. Returns [`ReadError`] for touched corruption or
    /// conflicting copies.
    pub fn get<'r>(
        &'r self,
        key: &[u8],
        workspace: &'r mut EngineWorkspace,
    ) -> Result<Option<LiveValue<'r>>, ReadError> {
        let in_memory = self.tables.iter().find_map(|table| table.get(key));
        resolve(in_memory, &self.runs, key, &mut workspace.logical)
    }

    /// Returns a scan of every live key in bytewise ascending order.
    /// Decoding failures are returned by [`Scan::next_record`].
    pub fn scan<'r>(&'r self, workspace: &'r mut EngineWorkspace) -> Scan<'r> {
        self.scan_prefix(&[], workspace)
    }

    /// Returns a scan of every live key starting with the raw bytes `prefix`.
    /// An empty prefix selects all keys. Decoding failures are returned by
    /// [`Scan::next_record`].
    pub fn scan_prefix<'r>(
        &'r self,
        prefix: &[u8],
        workspace: &'r mut EngineWorkspace,
    ) -> Scan<'r> {
        let tables = self.tables.iter().map(|table| table.iter_from(prefix));
        Scan::new(tables, &self.runs, prefix, workspace)
    }
}

impl fmt::Debug for Snapshot {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Snapshot")
            .field("last_seq", &self.last_seq)
            .field("tables", &self.tables.len())
            .field("runs", &self.runs.len())
            .finish_non_exhaustive()
    }
}

/// Newest first: every key's updates are in `active`, then the frozen tables
/// from newest to oldest, then the runs, so the first source holding a key
/// decides it.
fn lookup_state<'r>(
    state: &'r State,
    key: &[u8],
    logical: &'r mut LogicalWorkspace,
) -> Result<Option<LiveValue<'r>>, ReadError> {
    let in_memory = state
        .active
        .get(key)
        .or_else(|| state.frozen.iter().rev().find_map(|table| table.get(key)));
    resolve(in_memory, &state.directory, key, logical)
}

/// Decides `key` from the newest table update, or else from the runs.
fn resolve<'r>(
    in_memory: Option<Version<'r>>,
    runs: &'r RunDirectory,
    key: &[u8],
    logical: &'r mut LogicalWorkspace,
) -> Result<Option<LiveValue<'r>>, ReadError> {
    match in_memory {
        Some(Version {
            seq,
            value: Value::Bytes(value),
        }) => Ok(Some(LiveValue { seq, value })),
        Some(_) => Ok(None),
        None => latest_from(runs.lookup(), key, logical),
    }
}

fn next_within<'a>(records: &mut Records<'a>, prefix: &[u8]) -> Option<Record<'a>> {
    records
        .next()
        .filter(|record| record.key.starts_with(prefix))
}

type TableCursor<'a> = (Records<'a>, Option<Record<'a>>);

/// Iterates live keys in ascending order, each once with its latest value.
///
/// This merges the memtables and the live keys of the runs. A table's update
/// beats anything in older tables or the runs, so a tombstone in a table hides
/// the key whatever they hold, and values are not copied.
/// Records borrow the scan, so consume or copy a record before advancing.
/// The scan holds its view and workspace borrowed for its lifetime.
pub struct Scan<'r> {
    /// Each table's records and the next of them, newest table first.
    pub(super) tables: Vec<TableCursor<'r>>,
    spare_tables: &'r mut Vec<TableCursor<'static>>,
    runs: LogicalCursor<'r>,
    /// Whether the record the runs stand on has been used.
    runs_stale: bool,
    prefix: &'r [u8],
}

impl<'r> Scan<'r> {
    /// Starts a scan of `tables`, newest first and each positioned at
    /// `prefix`, over `runs`.
    fn new(
        tables: impl Iterator<Item = Records<'r>>,
        runs: &'r RunDirectory,
        prefix: &[u8],
        workspace: &'r mut EngineWorkspace,
    ) -> Self {
        let EngineWorkspace {
            logical,
            prefix: bound,
            scan_tables,
            ..
        } = workspace;
        bound.clear();
        bound.extend_from_slice(prefix);
        let prefix: &'r [u8] = bound;

        let mut cursor = LogicalCursor::grouped(runs, runs.groups(), logical);
        // A failed seek is repeated by the first read of the cursor.
        let _ = cursor.seek_prefix(prefix);
        let mut cursors: Vec<TableCursor<'r>> = std::mem::take(scan_tables);
        cursors.extend(tables.map(|mut records| {
            let head = next_within(&mut records, prefix);
            (records, head)
        }));
        Self {
            tables: cursors,
            spare_tables: scan_tables,
            runs: cursor,
            runs_stale: true,
            prefix,
        }
    }
}

impl Drop for Scan<'_> {
    fn drop(&mut self) {
        self.tables.clear();
        // All borrowed cursors are gone. Vec's in-place collection reuses the
        // empty allocation while changing its element lifetime, in safe code.
        *self.spare_tables = std::mem::take(&mut self.tables)
            .into_iter()
            .map(|_| unreachable!("scan cursors were cleared"))
            .collect();
    }
}

impl Scan<'_> {
    /// Advances to the next live key, or returns `None` when none is left.
    /// After the end, further calls return `None`.
    ///
    /// # Errors
    /// Returns [`ReadError`] for touched corrupt data or conflicting copies.
    /// Discard the scan after an error.
    pub fn next_record(&mut self) -> Result<Option<LiveRecord<'_>>, ReadError> {
        loop {
            if self.runs_stale {
                self.runs.next_record()?;
                self.runs_stale = false;
            }
            let run_key = self.runs.live().map(|record| record.key);
            let smallest = self
                .tables
                .iter()
                .filter_map(|(_, head)| head.map(|record| record.key))
                .chain(run_key)
                .min();
            let Some(key) = smallest else {
                return Ok(None);
            };

            self.runs_stale = run_key == Some(key);
            // The newest table holding the key decides it; every table
            // holding it moves past it.
            let mut decided = None;
            for (records, head) in &mut self.tables {
                if let Some(record) = head.filter(|record| record.key == key) {
                    decided.get_or_insert(record);
                    *head = next_within(records, self.prefix);
                }
            }

            match decided {
                Some(Record {
                    key,
                    seq,
                    value: Value::Bytes(value),
                }) => return Ok(Some(LiveRecord { key, seq, value })),
                Some(_) => {}
                None => return Ok(self.runs.live()),
            }
        }
    }
}

impl fmt::Debug for Scan<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Scan").finish_non_exhaustive()
    }
}
