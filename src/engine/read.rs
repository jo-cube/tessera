//! Snapshots, pooled readers, multi-gets and scans.

use super::*;

use std::cell::Cell;
use std::marker::PhantomData;
use std::ops::{self, Bound, Deref};
use std::sync::LazyLock;

/// The engine's state at one moment: what a [`Snapshot`] holds, and what a
/// latest-state scan pins for its life.
#[cfg_attr(not(any(test, feature = "diagnostics")), allow(dead_code))]
pub(super) struct View {
    /// Newest first.
    pub(super) tables: Vec<Arc<FrozenMemtable>>,
    /// The directory of every keyspace live at that moment.
    pub(super) directories: Directories,
    #[cfg_attr(not(test), allow(dead_code))]
    pub(super) last_seq: u64,
    /// The ID the next keyspace created after that moment gets.
    pub(super) next_keyspace: u32,
    pub(super) _live: Arc<()>,
}

/// How a view sees a keyspace.
enum Visible<'v> {
    Live(&'v RunDirectory),
    /// Created after the view: empty.
    Later,
    Dropped,
}

impl View {
    fn visible(&self, keyspace: u32) -> Visible<'_> {
        match keyspaces::directory_of(&self.directories, keyspace) {
            Some(directory) => Visible::Live(directory),
            None if keyspace >= self.next_keyspace => Visible::Later,
            None => Visible::Dropped,
        }
    }

    /// The value `key` had in live keyspace `keyspace`, or `None` if it was
    /// absent or deleted, without copying it.
    #[cfg(test)]
    pub(super) fn get<'r>(
        &'r self,
        keyspace: u32,
        key: &[u8],
        logical: &'r mut LogicalWorkspace,
    ) -> Result<Option<LiveValue<'r>>, ReadError> {
        let Visible::Live(runs) = self.visible(keyspace) else {
            return Ok(None);
        };
        self.get_in(KeyPrefix::new(keyspace), runs, key, logical)
    }

    fn get_in<'r>(
        &'r self,
        prefix: KeyPrefix,
        runs: &'r RunDirectory,
        key: &[u8],
        logical: &'r mut LogicalWorkspace,
    ) -> Result<Option<LiveValue<'r>>, ReadError> {
        let stored = prefix.key(key);
        let in_memory = self.tables.iter().find_map(|table| table.get_in(stored));
        resolve(in_memory, runs, key, logical)
    }

    /// The one keyspace live in the view, for tests of one keyspace, or
    /// `None` if the view was taken before it was created.
    #[cfg(test)]
    pub(super) fn only(&self) -> Option<u32> {
        assert!(self.directories.len() <= 1, "at most one live keyspace");
        self.directories.first().map(|(id, _)| *id)
    }
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

/// A consistent view of every keyspace of an engine at one moment, from
/// [`Engine::snapshot`]. It holds no lock: writes, flushes and compactions
/// continue, and reads through it are unaffected by them. It keeps the
/// memtables and runs it sees, so drop it once read; see
/// [Snapshots](crate#snapshots).
///
/// A cheap clone shares the same view. `Send + Sync`, and it outlives its
/// engine: it stays readable after [`Engine::close`]. A keyspace created
/// after the snapshot reads as empty through it.
#[derive(Clone)]
pub struct Snapshot {
    pub(super) view: Arc<View>,
    engine: u64,
    pool: Arc<Pool>,
}

impl Snapshot {
    pub(super) fn new(view: View, shared: &handle::Shared) -> Self {
        Self {
            view: Arc::new(view),
            engine: shared.id,
            pool: Arc::clone(&shared.pool),
        }
    }

    /// A reader of this snapshot, with buffers from the engine's pool. Take
    /// one per thread.
    pub fn reader(&self) -> Reader<'_> {
        Reader::new(Source::Snapshot(self), &self.pool)
    }

    /// The last sequence number this snapshot sees.
    #[cfg(test)]
    pub(crate) fn last_seq(&self) -> u64 {
        self.view.last_seq
    }

    /// The live records with `prefix`, with their sequence numbers, in
    /// the one keyspace live in the snapshot.
    #[cfg(test)]
    pub(crate) fn records(&self, prefix: &[u8]) -> Vec<(Vec<u8>, u64, Vec<u8>)> {
        let (mut lower, mut upper) = (Vec::new(), Vec::new());
        let (mut logical, mut spare) = (LogicalWorkspace::default(), Vec::new());
        let Some(id) = self.view.only() else {
            return Vec::new();
        };
        let limit = Bounds::Prefix(prefix).copy_into(KeyPrefix::new(id), &mut lower, &mut upper);
        let Visible::Live(runs) = self.view.visible(id) else {
            unreachable!("the keyspace is live in the view")
        };
        let mut scan = Scan::new(&self.view.tables, runs, limit, &mut logical, &mut spare);
        let mut out = Vec::new();
        while let Some(record) = scan.next_record().unwrap() {
            out.push((record.key.to_vec(), record.seq, record.value.to_vec()));
        }
        out
    }

    /// The value of `key` in the one keyspace live in the snapshot.
    #[cfg(test)]
    pub(crate) fn value(&self, key: &[u8]) -> Option<Vec<u8>> {
        self.get_seq(key).map(|(_, value)| value)
    }

    /// The value of `key` and its sequence number, in the one keyspace live
    /// in the snapshot.
    #[cfg(test)]
    pub(crate) fn get_seq(&self, key: &[u8]) -> Option<(u64, Vec<u8>)> {
        let mut logical = LogicalWorkspace::default();
        self.view
            .get(self.view.only()?, key, &mut logical)
            .unwrap()
            .map(|found| (found.seq, found.value.to_vec()))
    }
}

impl fmt::Debug for Snapshot {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Snapshot")
            .field("tables", &self.view.tables.len())
            .field(
                "runs",
                &self
                    .view
                    .directories
                    .iter()
                    .map(|(_, directory)| directory.len())
                    .sum::<usize>(),
            )
            .finish_non_exhaustive()
    }
}

/// Buffers a reader decodes and copies into, pooled per engine.
#[derive(Default)]
pub(super) struct Buffers {
    pub(super) logical: LogicalWorkspace,
    value: Vec<u8>,
    pub(super) multi: multi::Workspace,
    /// Always empty outside a multi-get; keeps only the allocation.
    keys: Vec<&'static [u8]>,
    /// Always empty outside a multi-get; keeps only the allocation.
    results: Vec<Option<&'static [u8]>>,
    /// Always empty outside a scan; keeps only the allocation.
    pub(super) scan_tables: Vec<TableCursor<'static>>,
    lower: Vec<u8>,
    upper: Vec<u8>,
}

/// Retained capacity above this is released when buffers return to the
/// pool, so one large value does not stay allocated in every reader.
const RETAINED_BYTES: usize = 1 << 20;

impl Buffers {
    fn trim(&mut self) {
        for buffer in [&mut self.value, &mut self.lower, &mut self.upper] {
            if buffer.capacity() > RETAINED_BYTES {
                *buffer = Vec::new();
            }
        }
        if self.keys.capacity() * size_of::<&'static [u8]>() > RETAINED_BYTES {
            self.keys = Vec::new();
        }
        if self.results.capacity() * size_of::<Option<&'static [u8]>>() > RETAINED_BYTES {
            self.results = Vec::new();
        }
        self.logical.trim(RETAINED_BYTES);
        self.multi.trim(RETAINED_BYTES);
    }
}

/// Temporarily borrows `Buffers::keys` with the caller's key lifetime and
/// returns its allocation on drop, even on early return or panic.
struct ResolvedKeys<'k, 'b> {
    keys: Vec<&'k [u8]>,
    spare: &'b mut Vec<&'static [u8]>,
}

impl<'k, 'b> ResolvedKeys<'k, 'b> {
    fn new<K: AsRef<[u8]>>(keys: &'k [K], spare: &'b mut Vec<&'static [u8]>) -> Self {
        let resolved: Vec<&'k [u8]> = std::mem::take(spare)
            .into_iter()
            .map(|_| unreachable!("resolved keys are cleared"))
            .collect();
        let mut out = Self {
            keys: resolved,
            spare,
        };
        out.keys.reserve(keys.len());
        out.keys.extend(keys.iter().map(AsRef::as_ref));
        out
    }
}

impl Drop for ResolvedKeys<'_, '_> {
    fn drop(&mut self) {
        self.keys.clear();
        *self.spare = std::mem::take(&mut self.keys)
            .into_iter()
            .map(|_| unreachable!("resolved keys are cleared"))
            .collect();
    }
}

/// Readers' buffers returned by dropped readers, reused by new ones.
pub(super) struct Pool {
    free: Mutex<Vec<Buffers>>,
    capacity: usize,
}

impl Pool {
    pub(super) fn new() -> Self {
        let cpus = std::thread::available_parallelism().map_or(1, std::num::NonZero::get);
        Self {
            free: Mutex::new(Vec::new()),
            capacity: 2 * cpus,
        }
    }

    fn take(&self) -> Buffers {
        self.free
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .pop()
            .unwrap_or_default()
    }

    fn give(&self, mut buffers: Buffers) {
        buffers.trim();
        let mut free = self.free.lock().unwrap_or_else(PoisonError::into_inner);
        if free.len() < self.capacity {
            free.push(buffers);
        }
    }

    /// Buffers waiting in the pool.
    #[cfg(test)]
    pub(super) fn len(&self) -> usize {
        self.free.lock().unwrap().len()
    }
}

enum Source<'a> {
    Latest(&'a handle::Shared),
    Snapshot(&'a Snapshot),
}

/// Zero-copy reads of the latest state, from [`Engine::reader`], or of a
/// snapshot, from [`Snapshot::reader`].
///
/// A reader owns buffers taken from its engine's pool and returns them when
/// dropped, so decoded blocks, decompression contexts and result vectors
/// are reused without allocation. Values it returns borrow it until its
/// next call. `Send`, not `Sync`: use one per thread.
///
/// ```compile_fail,E0277
/// fn shared<T: Sync>(_: &T) {}
/// # fn f(engine: &tessera::Engine) {
/// shared(&engine.reader());
/// # }
/// ```
///
/// On a snapshot every call reads the same state. On the latest state each
/// call reads the state of its moment; a scan pins that state, as a
/// snapshot does, from the call that starts it until the reader's next call
/// or drop.
///
/// A [`Scan`] borrows its reader for its whole life, so to look keys up
/// while scanning, take a second reader of the same snapshot: both read the
/// same state.
///
/// A borrowed value lives until the reader's next call:
///
/// ```compile_fail,E0499
/// # fn f(engine: &tessera::Engine, users: &tessera::Keyspace) -> tessera::Result<()> {
/// let mut reader = engine.reader();
/// let a = reader.get(users, b"a")?;
/// let b = reader.get(users, b"b")?;
/// assert_ne!(a, b);
/// # Ok(()) }
/// ```
///
/// A reader cannot outlive the snapshot it reads:
///
/// ```compile_fail,E0505
/// # fn f(engine: &tessera::Engine) {
/// let snapshot = engine.snapshot();
/// let reader = snapshot.reader();
/// drop(snapshot);
/// drop(reader);
/// # }
/// ```
pub struct Reader<'a> {
    source: Source<'a>,
    pool: &'a Pool,
    pub(super) buffers: Buffers,
    /// The state the current latest-state scan reads.
    pinned: Option<View>,
    /// The pooled buffers' cumulative counters when this reader took them.
    #[cfg(any(test, feature = "diagnostics"))]
    initial: ReaderCounters,
    _not_sync: PhantomData<Cell<()>>,
}

impl<'a> Reader<'a> {
    pub(super) fn latest(shared: &'a handle::Shared) -> Self {
        Self::new(Source::Latest(shared), &shared.pool)
    }

    fn new(source: Source<'a>, pool: &'a Pool) -> Self {
        let reader = Self {
            source,
            pool,
            buffers: pool.take(),
            pinned: None,
            #[cfg(any(test, feature = "diagnostics"))]
            initial: ReaderCounters::default(),
            _not_sync: PhantomData,
        };
        #[cfg(any(test, feature = "diagnostics"))]
        let reader = {
            let mut reader = reader;
            reader.initial = reader.cumulative();
            reader
        };
        reader
    }

    /// The value of `key` in `keyspace`, or `None` if it is absent or
    /// deleted. It borrows the reader until its next call; a snapshot
    /// reader returns it in place, a latest-state reader copies it once.
    ///
    /// # Errors
    /// [`ErrorKind::Corruption`] for damaged data the lookup touches;
    /// [`ErrorKind::KeyspaceDropped`]; [`ErrorKind::UnknownKeyspace`] for a
    /// keyspace of another engine; [`ErrorKind::Stopped`] for a latest-state
    /// reader after [`Engine::close`].
    ///
    /// # Panics
    /// A latest-state reader panics if the state lock is poisoned.
    pub fn get(&mut self, keyspace: &Keyspace, key: &[u8]) -> Result<Option<&[u8]>> {
        self.pinned = None;
        let Buffers { logical, value, .. } = &mut self.buffers;
        match self.source {
            Source::Latest(shared) => {
                let _open = shared.enter()?;
                own(shared.id, keyspace)?;
                shared
                    .core
                    .get(keyspace.id(), key, logical, value)
                    .map_err(|error| keyspace.named(error))
            }
            Source::Snapshot(snapshot) => {
                own(snapshot.engine, keyspace)?;
                match snapshot.view.visible(keyspace.id()) {
                    Visible::Live(runs) => Ok(snapshot
                        .view
                        .get_in(KeyPrefix::new(keyspace.id()), runs, key, logical)?
                        .map(|found| found.value)),
                    Visible::Later => Ok(None),
                    Visible::Dropped => Err(Error::keyspace_dropped(keyspace.name())),
                }
            }
        }
    }

    /// The values of `keys` in `keyspace`, one per key in input order,
    /// duplicates and misses included, all from one state. The result
    /// derefs to a slice of `Option<&[u8]>` and borrows the reader until its
    /// next call; its vector is reused across calls.
    ///
    /// Keys are looked up in sorted order, so sorted input saves a sort.
    ///
    /// # Errors
    /// As [`Reader::get`].
    ///
    /// # Panics
    /// As [`Reader::get`].
    pub fn multi_get<K: AsRef<[u8]>>(
        &mut self,
        keyspace: &Keyspace,
        keys: &[K],
    ) -> Result<MultiGet<'_>> {
        self.pinned = None;
        let Buffers {
            multi,
            keys: spare_keys,
            results,
            ..
        } = &mut self.buffers;
        // Validate engine/keyspace ownership before resolving keys so rejected
        // calls neither allocate scratch nor invoke caller `AsRef` code, while
        // still resolving keys outside the admission stripe.
        match self.source {
            Source::Latest(shared) => {
                if shared.is_closed() {
                    return Err(Error::stopped());
                }
                own(shared.id, keyspace)?;
                let resolved = ResolvedKeys::new(keys, spare_keys);
                let _open = shared.enter()?;
                shared
                    .core
                    .multi_get(keyspace.id(), &resolved.keys, multi)
                    .map_err(|error| keyspace.named(error))?;
            }
            Source::Snapshot(snapshot) => {
                own(snapshot.engine, keyspace)?;
                match snapshot.view.visible(keyspace.id()) {
                    Visible::Live(runs) => {
                        let resolved = ResolvedKeys::new(keys, spare_keys);
                        multi.lookup_view(
                            &resolved.keys,
                            &snapshot.view.tables,
                            KeyPrefix::new(keyspace.id()),
                            runs,
                        )?;
                    }
                    Visible::Later => multi.prepare_empty(keys.len()),
                    Visible::Dropped => return Err(Error::keyspace_dropped(keyspace.name())),
                }
            }
        }
        let multi: &multi::Workspace = multi;
        // The vector is empty; collecting in place reuses its allocation
        // while changing its element lifetime, in safe code.
        let mut values: Vec<Option<&[u8]>> = std::mem::take(results)
            .into_iter()
            .map(|_| unreachable!("multi-get results are cleared"))
            .collect();
        values.extend(multi.found());
        Ok(MultiGet {
            values,
            spare: results,
        })
    }

    /// A scan of the live keys of `keyspace` that start with `prefix`, in
    /// ascending bytewise order. An empty prefix selects every key.
    pub fn prefix(&mut self, keyspace: &Keyspace, prefix: &[u8]) -> Scan<'_> {
        self.scan(keyspace, Bounds::Prefix(prefix))
    }

    /// A scan of the live keys of `keyspace` within `range`, in ascending
    /// bytewise order: any std range of byte-string keys, `..`, or a pair of
    /// [`Bound`]s. An empty or inverted range yields nothing.
    ///
    /// ```
    /// # let engine = tessera::Engine::in_memory(Default::default())?;
    /// # let orders = engine.keyspace("orders")?;
    /// # for i in 0..10 { orders.put(format!("order/{i}").as_bytes(), b"")?; }
    /// use std::ops::Bound::{Excluded, Unbounded};
    /// let mut reader = engine.reader();
    /// let mut scan = reader.range(&orders, b"order/2"..b"order/5");
    /// let mut keys = Vec::new();
    /// while let Some((key, _value)) = scan.next()? {
    ///     keys.push(key.to_vec());
    /// }
    /// assert_eq!(keys, [b"order/2", b"order/3", b"order/4"]);
    /// drop(scan);
    /// let mut scan = reader.range(&orders, (Excluded(b"order/8"), Unbounded));
    /// assert_eq!(scan.next()?.map(|(key, _)| key.to_vec()), Some(b"order/9".to_vec()));
    /// # Ok::<(), tessera::Error>(())
    /// ```
    pub fn range(&mut self, keyspace: &Keyspace, range: impl KeyRange) -> Scan<'_> {
        self.scan(keyspace, Bounds::Range(range.start(), range.end()))
    }

    fn scan(&mut self, keyspace: &Keyspace, bounds: Bounds<'_>) -> Scan<'_> {
        let Self {
            source,
            buffers,
            pinned,
            ..
        } = self;
        *pinned = None;
        let Buffers {
            logical,
            scan_tables,
            lower,
            upper,
            ..
        } = buffers;
        let limit = bounds.copy_into(KeyPrefix::new(keyspace.id()), lower, upper);
        let view = match *source {
            Source::Latest(shared) => shared.enter().and_then(|_open| {
                own(shared.id, keyspace)?;
                let view = pinned.insert(shared.core.snapshot());
                match view.visible(keyspace.id()) {
                    Visible::Live(runs) => Ok(Some((&*view, runs))),
                    _ => Err(Error::keyspace_dropped(keyspace.name())),
                }
            }),
            Source::Snapshot(snapshot) => own(snapshot.engine, keyspace).and_then(|()| {
                match snapshot.view.visible(keyspace.id()) {
                    Visible::Live(runs) => Ok(Some((&*snapshot.view, runs))),
                    Visible::Later => Ok(None),
                    Visible::Dropped => Err(Error::keyspace_dropped(keyspace.name())),
                }
            }),
        };
        match view {
            Ok(Some((view, runs))) => Scan::new(&view.tables, runs, limit, logical, scan_tables),
            Ok(None) => Scan::new(&[], &EMPTY, limit, logical, scan_tables),
            Err(error) => {
                let mut scan = Scan::new(&[], &EMPTY, limit, logical, scan_tables);
                scan.error = Some(error);
                scan
            }
        }
    }

    /// Work done by this reader, and memory held by its buffers.
    #[cfg(any(test, feature = "diagnostics"))]
    pub(super) fn reader_counters(&self) -> ReaderCounters {
        let total = self.cumulative();
        ReaderCounters {
            blocks_loaded: total.blocks_loaded - self.initial.blocks_loaded,
            decoded_bytes: total.decoded_bytes - self.initial.decoded_bytes,
            filter_probes: total.filter_probes - self.initial.filter_probes,
            ..total
        }
    }

    /// The buffers' counters since they were created.
    #[cfg(any(test, feature = "diagnostics"))]
    fn cumulative(&self) -> ReaderCounters {
        let Buffers { logical, multi, .. } = &self.buffers;
        ReaderCounters {
            blocks_loaded: logical.blocks_loaded() + multi.blocks_loaded(),
            decoded_bytes: logical.decoded_bytes() + multi.decoded_bytes(),
            #[cfg(feature = "diagnostics")]
            filter_probes: logical.filter_probes(),
            #[cfg(not(feature = "diagnostics"))]
            filter_probes: 0,
            capacity: logical.capacity() + multi.capacity(),
            context_bytes: logical.context_bytes() + multi.context_bytes(),
        }
    }

    /// As [`ReaderDiagnostics::counters`](super::diagnostics::ReaderDiagnostics::counters).
    #[cfg(test)]
    pub(crate) fn counters(&self) -> ReaderCounters {
        self.reader_counters()
    }

    /// This reader with fresh buffers instead of pooled ones, so that its
    /// capacity reflects only its own reads.
    #[cfg(test)]
    pub(crate) fn unpooled(mut self) -> Self {
        self.buffers = Buffers::default();
        self.initial = ReaderCounters::default();
        self
    }

    /// The capacity of the vector multi-get results are collected into.
    #[cfg(test)]
    pub(crate) fn result_capacity(&self) -> usize {
        self.buffers.results.capacity()
    }

    /// The address of the value buffer, which pooling keeps.
    #[cfg(test)]
    pub(crate) fn value_buffer(&self) -> (usize, usize) {
        (
            self.buffers.value.as_ptr().addr(),
            self.buffers.value.capacity(),
        )
    }
}

impl Drop for Reader<'_> {
    fn drop(&mut self) {
        self.pinned = None;
        self.pool.give(std::mem::take(&mut self.buffers));
    }
}

impl fmt::Debug for Reader<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Reader")
            .field(
                "source",
                &match self.source {
                    Source::Latest(_) => "latest",
                    Source::Snapshot(_) => "snapshot",
                },
            )
            .finish_non_exhaustive()
    }
}

/// Refuses a keyspace of another engine than `engine`.
fn own(engine: u64, keyspace: &Keyspace) -> Result<()> {
    if keyspace.engine_id() == engine {
        Ok(())
    } else {
        Err(Error::unknown_keyspace(keyspace.name()))
    }
}

/// Work a reader has done and memory its buffers hold, for tools and tests.
#[cfg(any(test, feature = "diagnostics"))]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct ReaderCounters {
    /// Run blocks loaded, each a decompression for a compressed run.
    pub blocks_loaded: u64,
    /// Raw bytes in the blocks loaded, including misses.
    pub decoded_bytes: u64,
    /// Bloom-filter probes by point reads, including probes that rule a run
    /// out. Scans and multi-gets are excluded.
    pub filter_probes: u64,
    /// Decoded-block bytes retained. Point lookups keep two blocks whatever
    /// the number of runs; scans keep one per group of key-disjoint runs.
    pub capacity: usize,
    /// Bytes held by Zstandard decompression contexts, outside the Rust heap.
    pub context_bytes: usize,
}

/// The values of one [`Reader::multi_get`], one per key in input order.
/// Derefs to `[Option<&[u8]>]`: index it, iterate it, take its `len`. It
/// borrows the reader, which takes no other call while it lives. It hands
/// its vector back to the reader when dropped, so the borrow lasts until it
/// goes out of scope, not just until its last use: drop it before the
/// reader's next call in the same scope.
///
/// ```compile_fail,E0499
/// # fn f(engine: &tessera::Engine, users: &tessera::Keyspace) -> tessera::Result<()> {
/// let mut reader = engine.reader();
/// let values = reader.multi_get(users, &[b"a", b"b"])?;
/// reader.get(users, b"c")?;
/// assert!(values[0].is_none());
/// # Ok(()) }
/// ```
pub struct MultiGet<'r> {
    values: Vec<Option<&'r [u8]>>,
    spare: &'r mut Vec<Option<&'static [u8]>>,
}

impl<'r> Deref for MultiGet<'r> {
    type Target = [Option<&'r [u8]>];

    fn deref(&self) -> &Self::Target {
        &self.values
    }
}

impl<'a, 'r> IntoIterator for &'a MultiGet<'r> {
    type Item = &'a Option<&'r [u8]>;
    type IntoIter = std::slice::Iter<'a, Option<&'r [u8]>>;

    fn into_iter(self) -> Self::IntoIter {
        self.values.iter()
    }
}

impl Drop for MultiGet<'_> {
    fn drop(&mut self) {
        self.values.clear();
        *self.spare = std::mem::take(&mut self.values)
            .into_iter()
            .map(|_| unreachable!("multi-get results were cleared"))
            .collect();
    }
}

impl fmt::Debug for MultiGet<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_list().entries(self.values.iter()).finish()
    }
}

/// A key range for [`Reader::range`]: `a..b`, `a..=b`, `a..`, `..b`,
/// `..=b`, `..`, or a `(Bound, Bound)` pair, over any `K: AsRef<[u8]>`
/// such as byte-string literals, slices and vectors. Both ends of one range
/// have the same type, so mixed lengths of literals need slices:
/// `&b"a"[..]..&b"bc"[..]`. Sealed: no other type implements it.
///
/// ```compile_fail,E0277
/// struct Mine;
/// impl tessera::KeyRange for Mine {
///     fn start(&self) -> std::ops::Bound<&[u8]> { std::ops::Bound::Unbounded }
///     fn end(&self) -> std::ops::Bound<&[u8]> { std::ops::Bound::Unbounded }
/// }
/// ```
pub trait KeyRange: sealed::Sealed {
    /// The lower bound.
    fn start(&self) -> Bound<&[u8]>;
    /// The upper bound.
    fn end(&self) -> Bound<&[u8]>;
}

mod sealed {
    #[allow(unnameable_types)]
    pub trait Sealed {}
}

macro_rules! key_range {
    ($($ty:ty => |$r:ident| $start:expr, $end:expr;)*) => {$(
        impl<K: AsRef<[u8]>> sealed::Sealed for $ty {}
        impl<K: AsRef<[u8]>> KeyRange for $ty {
            #[allow(unused_variables)]
            fn start(&self) -> Bound<&[u8]> {
                let $r = self;
                $start
            }
            #[allow(unused_variables)]
            fn end(&self) -> Bound<&[u8]> {
                let $r = self;
                $end
            }
        }
    )*};
}

key_range! {
    ops::Range<K> => |r| Bound::Included(r.start.as_ref()), Bound::Excluded(r.end.as_ref());
    ops::RangeInclusive<K> => |r| Bound::Included(r.start().as_ref()), Bound::Included(r.end().as_ref());
    ops::RangeFrom<K> => |r| Bound::Included(r.start.as_ref()), Bound::Unbounded;
    ops::RangeTo<K> => |r| Bound::Unbounded, Bound::Excluded(r.end.as_ref());
    ops::RangeToInclusive<K> => |r| Bound::Unbounded, Bound::Included(r.end.as_ref());
    (Bound<K>, Bound<K>) => |r| r.0.as_ref().map(AsRef::as_ref), r.1.as_ref().map(AsRef::as_ref);
}

impl sealed::Sealed for ops::RangeFull {}
impl KeyRange for ops::RangeFull {
    fn start(&self) -> Bound<&[u8]> {
        Bound::Unbounded
    }
    fn end(&self) -> Bound<&[u8]> {
        Bound::Unbounded
    }
}

/// What a scan was asked for.
enum Bounds<'a> {
    Prefix(&'a [u8]),
    Range(Bound<&'a [u8]>, Bound<&'a [u8]>),
}

impl Bounds<'_> {
    /// Copies the bounds into the reader's buffers, for the scan to borrow.
    /// An upper bound longer than any key is cut to [`MAX_KEY_LEN`] bytes,
    /// inclusive, which admits the same keys.
    fn copy_into<'r>(
        &self,
        keyspace: KeyPrefix,
        lower: &'r mut Vec<u8>,
        upper: &'r mut Vec<u8>,
    ) -> Limit<'r> {
        lower.clear();
        upper.clear();
        match *self {
            Bounds::Prefix(prefix) => {
                lower.extend_from_slice(prefix);
                Limit {
                    prefix: keyspace,
                    start: lower,
                    after: false,
                    end: End::Prefix,
                    upper,
                }
            }
            Bounds::Range(start, end) => {
                let after = matches!(start, Bound::Excluded(_));
                if let Bound::Included(key) | Bound::Excluded(key) = start {
                    lower.extend_from_slice(key);
                }
                let end = match end {
                    Bound::Unbounded => End::Unbounded,
                    Bound::Included(key) | Bound::Excluded(key) if key.len() > MAX_KEY_LEN => {
                        upper.extend_from_slice(&key[..MAX_KEY_LEN]);
                        End::Through
                    }
                    Bound::Included(key) => {
                        upper.extend_from_slice(key);
                        End::Through
                    }
                    Bound::Excluded(key) => {
                        upper.extend_from_slice(key);
                        End::Below
                    }
                };
                Limit {
                    prefix: keyspace,
                    start: lower,
                    after,
                    end,
                    upper,
                }
            }
        }
    }
}

/// Where a scan ends, beside its start.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum End {
    /// Keys starting with the start key.
    Prefix,
    Unbounded,
    /// Keys below the upper key.
    Below,
    /// Keys up to and including the upper key.
    Through,
}

/// A scan's bounds, borrowed from the reader's buffers.
#[derive(Clone, Copy)]
struct Limit<'r> {
    /// The keyspace's prefix of the memtables' keys.
    prefix: KeyPrefix,
    /// The first key, or the prefix.
    start: &'r [u8],
    /// Whether `start` itself is excluded.
    after: bool,
    end: End,
    upper: &'r [u8],
}

impl Limit<'_> {
    fn admits(&self, key: &[u8]) -> bool {
        match self.end {
            End::Prefix => key.starts_with(self.start),
            End::Unbounded => true,
            End::Below => key < self.upper,
            End::Through => key <= self.upper,
        }
    }
}

/// A directory with no runs, for scans that read nothing.
static EMPTY: LazyLock<RunDirectory> = LazyLock::new(RunDirectory::empty);

/// The next memtable record in `limit`, with the keyspace prefix removed.
fn next_within<'a>(records: &mut Records<'a>, limit: &Limit<'_>) -> Option<Record<'a>> {
    let record = records.next()?;
    let key = record.key.strip_prefix(limit.prefix.as_bytes())?;
    limit.admits(key).then_some(Record { key, ..record })
}

type TableCursor<'a> = (Records<'a>, Option<Record<'a>>);

/// A lending scan of live keys in ascending order, each once with its
/// latest value, from [`Reader::prefix`] or [`Reader::range`]. Each record
/// borrows the scan until the next step:
///
/// ```compile_fail,E0499
/// # fn f(engine: &tessera::Engine, users: &tessera::Keyspace) -> tessera::Result<()> {
/// let mut reader = engine.reader();
/// let mut scan = reader.prefix(users, b"");
/// let first = scan.next()?;
/// let second = scan.next()?;
/// assert_ne!(first, second);
/// # Ok(()) }
/// ```
///
/// It merges the memtables with the runs: a memtable's update beats older
/// tables and the runs, so a deletion there hides the key, and no value is
/// copied.
pub struct Scan<'r> {
    /// Each table's records and the next of them, newest table first.
    pub(super) tables: Vec<TableCursor<'r>>,
    spare_tables: &'r mut Vec<TableCursor<'static>>,
    runs: LogicalCursor<'r>,
    /// Whether the record the runs stand on has been used.
    runs_stale: bool,
    limit: Limit<'r>,
    /// An excluded start the runs have not passed yet.
    skip: Option<&'r [u8]>,
    /// A failure found before the first step, returned by it.
    error: Option<Error>,
}

impl<'r> Scan<'r> {
    /// Starts a scan of `tables`, newest first, and `runs` within `limit`.
    fn new(
        tables: &'r [Arc<FrozenMemtable>],
        runs: &'r RunDirectory,
        limit: Limit<'r>,
        logical: &'r mut LogicalWorkspace,
        spare_tables: &'r mut Vec<TableCursor<'static>>,
    ) -> Self {
        let mut cursor = LogicalCursor::grouped(runs, runs.groups(), logical);
        // A failed seek is repeated by the first read of the cursor.
        let _ = match limit.end {
            End::Prefix => cursor.seek_prefix(limit.start),
            End::Unbounded => cursor.seek_range(limit.start, Bound::Unbounded),
            End::Below => cursor.seek_range(limit.start, Bound::Excluded(limit.upper)),
            End::Through => cursor.seek_range(limit.start, Bound::Included(limit.upper)),
        };
        let skip = limit.after.then_some(limit.start);
        let mut cursors: Vec<TableCursor<'r>> = std::mem::take(spare_tables);
        cursors.extend(tables.iter().map(|table| {
            let mut records = table.iter_from_in(limit.prefix.key(limit.start));
            let mut head = next_within(&mut records, &limit);
            if head.is_some_and(|record| Some(record.key) == skip) {
                head = next_within(&mut records, &limit);
            }
            (records, head)
        }));
        Self {
            tables: cursors,
            spare_tables,
            runs: cursor,
            runs_stale: true,
            limit,
            skip,
            error: None,
        }
    }

    /// The next live key and its value, or `None` at the end; after the end
    /// it keeps returning `None`.
    ///
    /// # Errors
    /// [`ErrorKind::Corruption`] for damaged data the scan touches;
    /// [`ErrorKind::KeyspaceDropped`], [`ErrorKind::UnknownKeyspace`] or
    /// [`ErrorKind::Stopped`] as for [`Reader::get`], from the first step.
    /// Discard the scan after an error.
    #[allow(clippy::should_implement_trait)]
    pub fn next(&mut self) -> Result<Option<(&[u8], &[u8])>> {
        if let Some(error) = self.error.take() {
            return Err(error);
        }
        Ok(self.next_record()?.map(|record| (record.key, record.value)))
    }

    /// The next live record, with its sequence number.
    pub(super) fn next_record(&mut self) -> Result<Option<LiveRecord<'_>>, ReadError> {
        loop {
            if self.runs_stale {
                self.runs.next_record()?;
                if let Some(skip) = self.skip.take()
                    && self.runs.live().is_some_and(|record| record.key == skip)
                {
                    self.runs.next_record()?;
                }
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
                    *head = next_within(records, &self.limit);
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

impl fmt::Debug for Scan<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Scan").finish_non_exhaustive()
    }
}

const _: () = {
    const fn send_sync<T: Send + Sync>() {}
    const fn send<T: Send>() {}
    send_sync::<Snapshot>();
    send::<Reader<'static>>();
    send::<MultiGet<'static>>();
};
