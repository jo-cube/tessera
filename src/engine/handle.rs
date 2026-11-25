//! Engine and keyspace handles: lifecycle, the directory lock, keyspaces,
//! writes, checkpoints and statistics.

use super::*;

use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicU64;
use std::thread::JoinHandle;

/// The largest checkpoint metadata, 16 MiB.
pub const MAX_METADATA_LEN: usize = 16 << 20;

/// The longest keyspace name, in bytes.
pub const MAX_KEYSPACE_NAME_LEN: usize = 64;

/// Engines in this process, numbered so that a handle can tell its own.
static ENGINE_IDS: AtomicU64 = AtomicU64::new(1);

/// Number of cache-line-isolated open-state stripes so concurrent operations do
/// not bounce a single `RwLock<bool>` cache line across CPU clusters.
const OPEN_STRIPES: usize = 16;

static NEXT_OPEN_STRIPE: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);

thread_local! {
    static THREAD_OPEN_STRIPE: usize =
        NEXT_OPEN_STRIPE.fetch_add(1, std::sync::atomic::Ordering::Relaxed) % OPEN_STRIPES;
}

fn open_stripe() -> usize {
    THREAD_OPEN_STRIPE.with(|&stripe| stripe)
}

/// 128-byte aligned to avoid false sharing on both 64-byte and 128-byte cache
/// line architectures (such as Apple Silicon).
#[repr(align(128))]
struct OpenStripe(RwLock<bool>);

/// Everything the handles of one engine share. Dropping it, when the last
/// [`Engine`] and [`Keyspace`] handle is dropped, stops the workers and
/// releases the directory lock.
pub(super) struct Shared {
    pub(super) core: Arc<Core>,
    pub(super) id: u64,
    dir: Option<PathBuf>,
    /// Holds `dir/LOCK` until the engine closes.
    lock: Mutex<Option<File>>,
    workers: Mutex<Workers>,
    /// Whether the engine is open. Operations hold their thread's stripe
    /// shared for their duration; closing locks every stripe exclusively to
    /// wait for in-flight operations and mark the engine closed.
    open: Box<[OpenStripe; OPEN_STRIPES]>,
    /// Set once every stripe is locked exclusively during [`Engine::close`],
    /// allowing cheap lock-free pre-checks before running caller callbacks.
    closed: std::sync::atomic::AtomicBool,
    pub(super) pool: Arc<read::Pool>,
    /// The metadata of the latest committed checkpoint.
    metadata: Mutex<Option<Vec<u8>>>,
    #[cfg(feature = "diagnostics")]
    pub(super) last_checkpoint: Mutex<Option<CheckpointReport>>,
}

#[derive(Default)]
struct Workers {
    compaction: Option<JoinHandle<()>>,
    flush: Option<JoinHandle<()>>,
}

impl Shared {
    /// Reports whether [`Engine::close`] has already committed the closed state.
    pub(super) fn is_closed(&self) -> bool {
        self.closed.load(std::sync::atomic::Ordering::Acquire)
    }

    /// Admits an operation while the engine is open, until the guard drops.
    pub(super) fn enter(&self) -> Result<RwLockReadGuard<'_, bool>> {
        let open = self.open[open_stripe()]
            .0
            .read()
            .unwrap_or_else(PoisonError::into_inner);
        if *open {
            Ok(open)
        } else {
            Err(Error::stopped())
        }
    }

    /// Joins both workers, reporting a panic of either.
    fn stop_workers(&self) -> Result<(), BackgroundError> {
        let mut workers = self.workers.lock().unwrap_or_else(PoisonError::into_inner);
        self.core.flush_signal.stop();
        if let Some(handle) = workers.flush.take() {
            let _ = handle.join();
        }
        let flush_error = self.core.flush_signal.failure();
        let Some(handle) = workers.compaction.take() else {
            return flush_error.map_or(Ok(()), Err);
        };
        let signal = self.core.background.as_ref().expect(WORKER);
        signal.stop();
        // The thread catches its own panics, so joining cannot fail.
        let _ = handle.join();
        match signal.status().panic {
            Some(message) => Err(BackgroundError::Panicked(message)),
            None => flush_error.map_or(Ok(()), Err),
        }
    }
}

impl Drop for Shared {
    fn drop(&mut self) {
        self.core.combiner.close();
        let _ = self.stop_workers();
    }
}

/// An engine: a cheap, cloneable handle to one RAM-first store of
/// keyspaces. All clones share it; it runs until [`Engine::close`] or until
/// the last `Engine` and [`Keyspace`] handle is dropped. `Send + Sync`.
///
/// Nothing is written to disk except by [`Engine::checkpoint`] and
/// [`Engine::checkpoint_with`]: closing or dropping the engine loses every
/// write since the last checkpoint.
///
/// Methods that take the engine's state lock panic if a panic poisoned it;
/// see [Panics](crate#panics).
#[derive(Clone)]
pub struct Engine {
    pub(super) core: Arc<Core>,
    pub(super) shared: Arc<Shared>,
}

impl Engine {
    /// Opens the engine stored in `dir`, creating the directory if it is
    /// missing: recovers the checkpoint its `CURRENT` file names, or starts
    /// empty if there is none. The engine holds `dir/LOCK` until it closes.
    ///
    /// Recovery reads and checks every file of exactly that checkpoint
    /// before it returns, and never falls back to an older one. Run files
    /// left by an interrupted first checkpoint are removed by the next one.
    ///
    /// # Errors
    /// [`ErrorKind::Config`] for invalid options, before `dir` is touched;
    /// [`ErrorKind::Locked`] if another engine or process holds the lock;
    /// [`ErrorKind::Corruption`] if a checkpoint file fails its checks;
    /// [`ErrorKind::Io`] if the directory or a file cannot be read or a
    /// worker thread cannot start. A failed open leaves no worker running
    /// and releases the lock.
    pub fn open(dir: impl AsRef<Path>, options: Options) -> Result<Engine> {
        let dir = dir.as_ref();
        options.validate()?;
        fs::create_dir_all(dir)
            .map_err(|error| Error::io(dir, error, format!("cannot create {}", dir.display())))?;
        let lock = match checkpoint::lock(dir) {
            Ok(Ok(file)) => file,
            Ok(Err(())) => return Err(OpenError::Locked(dir.to_owned()).into()),
            Err(error) => {
                return Err(Error::io(
                    dir,
                    error,
                    format!("cannot lock {}", dir.display()),
                ));
            }
        };
        let (core, metadata) = match checkpoint::recover(dir, &options)? {
            Some((core, metadata)) => (core, metadata),
            None => (Core::new(&options), None),
        };
        Self::start(core, &options, Some((dir.to_owned(), lock)), metadata)
    }

    /// Starts an empty engine with no directory. [`Engine::checkpoint`]
    /// fails with [`ErrorKind::Config`].
    ///
    /// # Errors
    /// [`ErrorKind::Config`] for invalid options; [`ErrorKind::Io`] if a
    /// worker thread cannot start.
    pub fn in_memory(options: Options) -> Result<Engine> {
        options.validate()?;
        Self::start(Core::new(&options), &options, None, None)
    }

    /// Starts the workers of `core`. On failure nothing is left running.
    pub(super) fn start(
        core: Core,
        options: &Options,
        dir: Option<(PathBuf, File)>,
        metadata: Option<Vec<u8>>,
    ) -> Result<Engine> {
        #[cfg(test)]
        let enabled = options.background_compaction.is_some();
        #[cfg(not(test))]
        let enabled = {
            let _ = options;
            true
        };
        let core = Arc::new(core);
        let compaction = enabled
            .then(|| worker::start(Arc::clone(&core)))
            .transpose()
            .map_err(OpenError::Worker)?;
        let (dir, lock) = dir.unzip();
        let shared = Arc::new(Shared {
            core: Arc::clone(&core),
            id: ENGINE_IDS.fetch_add(1, std::sync::atomic::Ordering::Relaxed),
            dir,
            lock: Mutex::new(lock),
            workers: Mutex::new(Workers {
                compaction,
                flush: None,
            }),
            open: Box::new(std::array::from_fn(|_| OpenStripe(RwLock::new(true)))),
            closed: std::sync::atomic::AtomicBool::new(false),
            pool: Arc::new(read::Pool::new()),
            metadata: Mutex::new(metadata),
            #[cfg(feature = "diagnostics")]
            last_checkpoint: Mutex::new(None),
        });
        // On failure, dropping `shared` stops the compaction worker.
        let flush = flusher::start(Arc::clone(&core)).map_err(OpenError::Worker)?;
        shared.workers.lock().unwrap().flush = Some(flush);
        Ok(Engine { core, shared })
    }

    /// Returns the keyspace `name`, creating it if it does not exist. Names
    /// are 1 to [`MAX_KEYSPACE_NAME_LEN`] bytes of ASCII letters, digits,
    /// `_`, `.` and `-`, compared exactly. An engine holds at most 4096 live
    /// keyspaces. A created keyspace is durable from the next checkpoint.
    ///
    /// # Errors
    /// [`ErrorKind::InvalidInput`] for a name outside the rules, or when
    /// creating the keyspace would exceed the limit; [`ErrorKind::Stopped`] after
    /// [`Engine::close`].
    pub fn keyspace(&self, name: &str) -> Result<Keyspace> {
        let _open = self.shared.enter()?;
        if !valid_name(name) {
            return Err(invalid_name(name));
        }
        let found = |state: &State| {
            state
                .names
                .get_key_value(name)
                .map(|(name, &id)| self.handle(id, name))
        };
        if let Some(found) = found(&self.core.read_state()) {
            return Ok(found);
        }
        let mut state = self.core.write_state();
        if let Some(found) = found(&state) {
            return Ok(found);
        }
        let id = state.create_keyspace(name, self.core.level_ratio)?;
        Ok(self.handle(id, &state.keyspaces[&id].name))
    }

    /// Returns the keyspace `name` if it exists, and `None` otherwise; unlike
    /// [`Engine::keyspace`], it never creates one. A keyspace exists from its
    /// creation until it is dropped, and after a reopen if the recovered
    /// checkpoint holds it. Names follow the rules of [`Engine::keyspace`].
    ///
    /// ```
    /// # use tessera::{Engine, Options};
    /// let engine = Engine::in_memory(Options::default())?;
    /// assert!(engine.existing_keyspace("users")?.is_none());
    /// engine.keyspace("users")?;
    /// assert!(engine.existing_keyspace("users")?.is_some());
    /// # Ok::<(), tessera::Error>(())
    /// ```
    ///
    /// # Errors
    /// [`ErrorKind::InvalidInput`] for a name outside the rules;
    /// [`ErrorKind::Stopped`] after [`Engine::close`].
    pub fn existing_keyspace(&self, name: &str) -> Result<Option<Keyspace>> {
        let _open = self.shared.enter()?;
        if !valid_name(name) {
            return Err(invalid_name(name));
        }
        let state = self.core.read_state();
        Ok(state
            .names
            .get_key_value(name)
            .map(|(name, &id)| self.handle(id, name)))
    }

    fn handle(&self, id: u32, name: &Arc<str>) -> Keyspace {
        Keyspace {
            shared: Arc::clone(&self.shared),
            id,
            name: Arc::clone(name),
        }
    }

    /// The names of the live keyspaces, in bytewise order.
    pub fn keyspaces(&self) -> Vec<String> {
        let state = self.core.read_state();
        state.names.keys().map(|name| name.to_string()).collect()
    }

    /// Drops keyspace `name` and its data, and returns whether it existed.
    ///
    /// Its handles, and batches naming it, then fail with
    /// [`ErrorKind::KeyspaceDropped`]; snapshots taken before keep reading
    /// it. The name can be created again, as a distinct keyspace that
    /// handles of the dropped one do not reach. Dropping is durable from the next checkpoint.
    ///
    /// # Errors
    /// [`ErrorKind::InvalidInput`] for a name outside the rules;
    /// [`ErrorKind::Stopped`] after [`Engine::close`].
    pub fn drop_keyspace(&self, name: &str) -> Result<bool> {
        let _open = self.shared.enter()?;
        if !valid_name(name) {
            return Err(invalid_name(name));
        }
        Ok(self.core.drop_keyspace(name))
    }

    /// Applies every operation of `batch` atomically: a read sees all of it
    /// or none, and a failed write applies none. Later operations on one key
    /// win. An empty batch changes nothing.
    ///
    /// A large batch of strictly ascending keys is built into runs outside
    /// the state lock, so other writes and reads proceed meanwhile.
    ///
    /// # Errors
    /// [`ErrorKind::InvalidInput`] for a key over [`MAX_KEY_LEN`], a value
    /// over [`MAX_VALUE_LEN`] or a record charged more than
    /// [`Options::memtable_size`]; [`ErrorKind::MemoryLimit`] when admission
    /// cannot reclaim memory; [`ErrorKind::KeyspaceDropped`] or
    /// [`ErrorKind::UnknownKeyspace`] for a keyspace of the batch;
    /// [`ErrorKind::Stopped`] after [`Engine::close`].
    ///
    /// # Panics
    /// Panics if the state lock is poisoned or a synchronous build panics.
    pub fn write(&self, batch: &WriteBatch) -> Result<()> {
        let _open = self.shared.enter()?;
        match batch.engine() {
            Err(name) => return Err(Error::unknown_keyspace(name)),
            Ok(Some(engine)) if engine != self.shared.id => {
                return Err(Error::new(
                    ErrorKind::UnknownKeyspace,
                    "the batch's keyspaces belong to another engine",
                ));
            }
            Ok(_) => {}
        }
        self.core.write_batch(batch)?;
        Ok(())
    }

    /// A reader of the latest state: each call sees the state of its moment.
    /// Use a [`Snapshot`] when several calls must agree. Take one per thread.
    pub fn reader(&self) -> Reader<'_> {
        Reader::latest(&self.shared)
    }

    /// A consistent view of every keyspace now, holding no lock. Writes,
    /// flushes and compactions continue; the snapshot keeps the sources it
    /// sees until it is dropped. See [Snapshots](crate#snapshots).
    ///
    /// # Panics
    /// Panics if the state lock is poisoned.
    pub fn snapshot(&self) -> Snapshot {
        Snapshot::new(self.core.snapshot(), &self.shared)
    }

    /// Commits a checkpoint of every keyspace into [`Engine::path`], keeping
    /// the metadata of the last checkpoint, if it had any. See
    /// [Checkpoints](crate#checkpoints).
    ///
    /// # Errors
    /// [`ErrorKind::Config`] for an in-memory engine; [`ErrorKind::Io`] if
    /// a file cannot be written, with [`Error::checkpoint_committed`] telling
    /// whether the commit happened before the failure;
    /// [`ErrorKind::Stopped`] after [`Engine::close`]. A failed checkpoint
    /// leaves the previous one intact.
    ///
    /// # Panics
    /// Panics if the state lock is poisoned or a synchronous flush build
    /// panics.
    pub fn checkpoint(&self) -> Result<()> {
        self.commit(None)
    }

    /// Commits a checkpoint as [`Engine::checkpoint`] does, with
    /// `metadata`, at most [`MAX_METADATA_LEN`] bytes, in the same atomic
    /// commit. [`Engine::metadata`] returns it, then and after the engine
    /// is opened again.
    ///
    /// The metadata describes the checkpoint's state only if no write
    /// happened between choosing it and this call. To record a position in
    /// an input stream exactly under concurrent writes, write it into a
    /// keyspace in the same [`WriteBatch`] as the data.
    ///
    /// # Errors
    /// [`ErrorKind::InvalidInput`] for metadata over the limit; otherwise
    /// as [`Engine::checkpoint`].
    ///
    /// # Panics
    /// As [`Engine::checkpoint`].
    pub fn checkpoint_with(&self, metadata: &[u8]) -> Result<()> {
        self.commit(Some(metadata))
    }

    fn commit(&self, metadata: Option<&[u8]>) -> Result<()> {
        let _open = self.shared.enter()?;
        let Some(dir) = &self.shared.dir else {
            return Err(Error::new(
                ErrorKind::Config,
                "an in-memory engine has no directory to checkpoint into",
            ));
        };
        let _serial = self
            .core
            .checkpointing
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        let kept;
        let metadata = match metadata {
            Some(metadata) => Some(metadata),
            None => {
                kept = self.metadata();
                kept.as_deref()
            }
        };
        let result = self.core.checkpoint_into(dir, metadata);
        let committed = match &result {
            Ok(_) => true,
            Err(CheckpointError::Io {
                after_commit_point, ..
            }) => *after_commit_point,
            Err(_) => false,
        };
        if committed {
            *self
                .shared
                .metadata
                .lock()
                .unwrap_or_else(PoisonError::into_inner) = metadata.map(<[u8]>::to_vec);
        }
        let _report = result?;
        #[cfg(feature = "diagnostics")]
        {
            *self
                .shared
                .last_checkpoint
                .lock()
                .unwrap_or_else(PoisonError::into_inner) = Some(_report);
        }
        Ok(())
    }

    /// The metadata of the latest committed checkpoint, or `None` if it has
    /// none or there is no checkpoint.
    pub fn metadata(&self) -> Option<Vec<u8>> {
        self.shared
            .metadata
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    /// Memory and storage figures an application acts on.
    ///
    /// # Panics
    /// Panics if the state lock is poisoned.
    pub fn stats(&self) -> Stats {
        let state = self.core.read_state();
        let usage = self.core.usage(&state);
        Stats {
            memory_used: usage.used,
            memory_limit: self.core.memory_limit,
            stored_bytes: state.run_bytes,
            snapshot_bytes: usage.snapshots,
            keyspaces: state
                .names
                .iter()
                .map(|(name, id)| KeyspaceStats {
                    name: name.to_string(),
                    stored_bytes: state.keyspaces[id].run_bytes,
                })
                .collect(),
        }
    }

    /// The engine's directory, or `None` for an in-memory engine.
    pub fn path(&self) -> Option<&Path> {
        self.shared.dir.as_deref()
    }

    /// Closes the engine: waits for operations in progress, stops both
    /// workers and releases the directory lock. Other handles then fail with
    /// [`ErrorKind::Stopped`]; snapshots, and readers of them, keep working.
    /// It writes nothing, so writes since the last checkpoint are lost.
    /// Closing a closed engine does nothing.
    ///
    /// # Errors
    /// [`ErrorKind::Background`] if a worker panicked at any time; the
    /// engine is closed all the same.
    pub fn close(self) -> Result<()> {
        self.close_with(|_| {})
    }

    pub(super) fn close_with(self, mut on_stripe: impl FnMut(usize)) -> Result<()> {
        {
            let mut guards = std::array::from_fn::<_, OPEN_STRIPES, _>(|i| {
                on_stripe(i);
                self.shared.open[i]
                    .0
                    .write()
                    .unwrap_or_else(PoisonError::into_inner)
            });
            if !*guards[0] {
                return Ok(());
            }
            self.shared
                .closed
                .store(true, std::sync::atomic::Ordering::Release);
            for guard in &mut guards {
                **guard = false;
            }
        }
        self.core.combiner.close();
        let stopped = self.shared.stop_workers();
        drop(
            self.shared
                .lock
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .take(),
        );
        Ok(stopped?)
    }
}

impl fmt::Debug for Engine {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut out = f.debug_struct("Engine");
        out.field("path", &self.shared.dir);
        match self.core.state.try_read() {
            Ok(state) => {
                let names: Vec<&str> = state
                    .keyspaces
                    .values()
                    .map(|space| space.name.as_ref())
                    .collect();
                out.field("keyspaces", &names);
            }
            Err(std::sync::TryLockError::WouldBlock) => {
                out.field("keyspaces", &"<locked>");
            }
            Err(std::sync::TryLockError::Poisoned(_)) => {
                out.field("keyspaces", &"<poisoned>");
            }
        }
        out.finish_non_exhaustive()
    }
}

/// Whether `name` follows the keyspace name rules.
pub(super) fn valid_name(name: &str) -> bool {
    (1..=MAX_KEYSPACE_NAME_LEN).contains(&name.len())
        && name
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'.' | b'-'))
}

fn invalid_name(name: &str) -> Error {
    Error::new(
        ErrorKind::InvalidInput,
        format!(
            "keyspace name {name:?} must be 1 to {MAX_KEYSPACE_NAME_LEN} bytes of ASCII \
             letters, digits, '_', '.' and '-'"
        ),
    )
}

/// A named keyspace of an [`Engine`]: a cheap, cloneable handle. Keys are
/// independent across keyspaces. A handle keeps its engine running, as an
/// [`Engine`] handle does. `Send + Sync`.
#[derive(Clone)]
pub struct Keyspace {
    shared: Arc<Shared>,
    id: u32,
    name: Arc<str>,
}

impl Keyspace {
    /// The keyspace's name.
    pub fn name(&self) -> &str {
        &self.name
    }

    pub(super) fn id(&self) -> u32 {
        self.id
    }

    pub(super) fn engine_id(&self) -> u64 {
        self.shared.id
    }

    pub(super) fn shared_name(&self) -> Arc<str> {
        Arc::clone(&self.name)
    }

    /// Names this keyspace in a dropped-keyspace error.
    pub(super) fn named(&self, error: Error) -> Error {
        if error.kind() == ErrorKind::KeyspaceDropped {
            Error::keyspace_dropped(&self.name)
        } else {
            error
        }
    }

    /// Sets `key` to `value`.
    ///
    /// # Errors
    /// As [`Engine::write`].
    ///
    /// # Panics
    /// As [`Engine::write`].
    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        self.update(key, Value::Bytes(value))
    }

    /// Deletes `key`. Deleting a missing key succeeds.
    ///
    /// # Errors
    /// As [`Engine::write`].
    ///
    /// # Panics
    /// As [`Engine::write`].
    pub fn delete(&self, key: &[u8]) -> Result<()> {
        self.update(key, Value::Tombstone)
    }

    fn update(&self, key: &[u8], value: Value<'_>) -> Result<()> {
        let _open = self.shared.enter()?;
        self.shared
            .core
            .update(self.id, key, value)
            .map_err(|error| self.named(error.into()))?;
        Ok(())
    }

    /// The latest value of `key`, copied, or `None` if it is absent or
    /// deleted. Uses a pooled [`Reader`]; take one to read without copying.
    ///
    /// # Errors
    /// [`ErrorKind::Corruption`] for damaged data the lookup touches;
    /// [`ErrorKind::KeyspaceDropped`]; [`ErrorKind::Stopped`] after
    /// [`Engine::close`].
    ///
    /// # Panics
    /// Panics if the state lock is poisoned.
    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let mut reader = Reader::latest(&self.shared);
        Ok(reader.get(self, key)?.map(<[u8]>::to_vec))
    }

    /// This keyspace's figures.
    ///
    /// # Errors
    /// [`ErrorKind::KeyspaceDropped`]; [`ErrorKind::Stopped`] after
    /// [`Engine::close`].
    pub fn stats(&self) -> Result<KeyspaceStats> {
        let _open = self.shared.enter()?;
        let state = self.shared.core.read_state();
        let space = state
            .keyspace(self.id)
            .map_err(|error| self.named(error.into()))?;
        Ok(KeyspaceStats {
            name: self.name.to_string(),
            stored_bytes: space.run_bytes,
        })
    }
}

impl fmt::Debug for Keyspace {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Keyspace")
            .field("name", &self.name)
            .finish_non_exhaustive()
    }
}

/// Memory and storage figures an application acts on, from
/// [`Engine::stats`]. Fields may be added.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct Stats {
    /// Bytes charged to writer admission: memtables, runs and their filters
    /// and indexes, retained snapshot sources, build estimates and pool
    /// reserve. An admission budget, not a process memory figure.
    pub memory_used: usize,
    /// [`Options::memory_limit`].
    pub memory_limit: Option<usize>,
    /// Stored bytes of every keyspace's runs, after compression.
    pub stored_bytes: usize,
    /// The part of `memory_used` that only live snapshots and scans hold.
    pub snapshot_bytes: usize,
    /// One entry per live keyspace, by name.
    pub keyspaces: Vec<KeyspaceStats>,
}

/// One keyspace's figures. Fields may be added.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct KeyspaceStats {
    /// Its name.
    pub name: String,
    /// Stored bytes of its runs, after compression; the keyspaces' figures
    /// sum to [`Stats::stored_bytes`].
    pub stored_bytes: usize,
}

const _: () = {
    const fn bounds<T: Send + Sync>() {}
    bounds::<Engine>();
    bounds::<Keyspace>();
    bounds::<WriteBatch>();
    bounds::<Stats>();
};

/// Inherent forms of the diagnostics for the crate's own tests.
#[cfg(test)]
impl Engine {
    pub(crate) fn engine_stats(&self) -> EngineStats {
        self.core.stats()
    }

    pub(crate) fn flush(&self) -> Result<FlushReport, BackgroundError> {
        self.core.flush()
    }

    pub(crate) fn wait_for_compaction(&self) -> Result<(), BackgroundError> {
        match &self.core.background {
            Some(signal) => signal.wait(),
            None => Ok(()),
        }
    }

    pub(crate) fn background_status(&self) -> Option<BackgroundStatus> {
        self.core.background.as_ref().map(worker::Signal::status)
    }

    /// Writes `batch`, strictly sorted, directly into runs, returning its
    /// one publication sequence.
    pub(crate) fn ingest_sorted(&self, batch: &WriteBatch) -> Result<u64, UpdateError> {
        assert!(batch.is_sorted(), "ingestion needs strictly sorted keys");
        if batch.is_empty() {
            return Ok(self.core.read_state().last_seq);
        }
        let size = batch.validate(self.core.memtable_budget)?;
        self.core.ingest_batch(batch, true, size)
    }

    /// Opens or creates the keyspace the crate's tests write to.
    pub(crate) fn test_keyspace(&self) -> Keyspace {
        self.keyspace("test").unwrap()
    }

    /// Commits a checkpoint of this engine into `dir`, which nothing else
    /// holds, with no application metadata. Tests use it to move an
    /// in-memory engine onto disk; [`Engine::open`] then recovers it.
    pub(crate) fn checkpoint_to(&self, dir: &Path) -> Result<checkpoint::CheckpointReport> {
        self.checkpoint_to_with(dir, None)
    }

    /// [`Engine::checkpoint_to`] with application metadata.
    pub(crate) fn checkpoint_to_with(
        &self,
        dir: &Path,
        metadata: Option<&[u8]>,
    ) -> Result<checkpoint::CheckpointReport> {
        let _open = self.shared.enter()?;
        fs::create_dir_all(dir).map_err(|error| Error::io(dir, error, "creating the directory"))?;
        let _serial = self
            .core
            .checkpointing
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        Ok(self.core.checkpoint_into(dir, metadata)?)
    }

    /// [`Engine::write`], returning the batch's last sequence number.
    pub(crate) fn write_seq(&self, batch: &WriteBatch) -> Result<u64, UpdateError> {
        self.core.write_batch(batch)
    }

    /// [`Engine::test_keyspace`], for short.
    pub(crate) fn ks(&self) -> Keyspace {
        self.test_keyspace()
    }

    /// The test keyspace's latest value of `key` and its sequence number.
    pub(crate) fn get_seq(&self, key: &[u8]) -> Option<(u64, Vec<u8>)> {
        let mut logical = LogicalWorkspace::default();
        let id = self.ks().id();
        self.core
            .snapshot()
            .get(id, key, &mut logical)
            .unwrap()
            .map(|found| (found.seq, found.value.to_vec()))
    }
}

/// Writes that return their sequence numbers, for the crate's own tests.
#[cfg(test)]
impl Keyspace {
    pub(crate) fn put_seq(&self, key: &[u8], value: &[u8]) -> Result<u64, UpdateError> {
        self.shared.core.update(self.id, key, Value::Bytes(value))
    }

    pub(crate) fn delete_seq(&self, key: &[u8]) -> Result<u64, UpdateError> {
        self.shared.core.update(self.id, key, Value::Tombstone)
    }
}

#[cfg(test)]
mod close_tests {
    use super::*;
    use std::sync::mpsc;
    use std::time::Duration;

    #[test]
    fn close_holds_every_stripe_before_exposing_stopped() {
        let engine = Engine::in_memory(Options::default()).unwrap();
        let in_flight = engine.shared.open[2].0.read().unwrap();
        let (at_stripe_2_tx, at_stripe_2_rx) = mpsc::channel();
        let closer_engine = engine.clone();
        let closer = std::thread::spawn(move || {
            closer_engine.close_with(|stripe| {
                if stripe == 2 {
                    at_stripe_2_tx.send(()).unwrap();
                }
            })
        });

        at_stripe_2_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("close should reach stripe 2");
        // Stripes 0 and 1 must still be held exclusively (blocking callers
        // instead of exposing Stopped early), while stripe 3 is still open.
        assert!(matches!(
            engine.shared.open[0].0.try_read(),
            Err(std::sync::TryLockError::WouldBlock)
        ));
        assert!(matches!(
            engine.shared.open[1].0.try_read(),
            Err(std::sync::TryLockError::WouldBlock)
        ));
        assert!(*engine.shared.open[3].0.try_read().unwrap());
        assert!(!engine.shared.is_closed());

        drop(in_flight);
        closer.join().unwrap().unwrap();
        assert!(engine.shared.is_closed());
        for stripe in engine.shared.open.iter() {
            assert!(!*stripe.0.read().unwrap());
        }
        assert_eq!(
            engine.shared.enter().unwrap_err().kind(),
            ErrorKind::Stopped
        );
    }

    #[test]
    fn multi_get_resolves_keys_before_taking_open_stripe() {
        struct ClosingKey<'a> {
            bytes: &'a [u8],
            entered: mpsc::Sender<()>,
            closed: mpsc::Receiver<()>,
        }
        impl AsRef<[u8]> for ClosingKey<'_> {
            fn as_ref(&self) -> &[u8] {
                self.entered.send(()).unwrap();
                self.closed
                    .recv_timeout(Duration::from_secs(5))
                    .expect("close must not deadlock on multi_get key resolution");
                self.bytes
            }
        }

        let engine = Engine::in_memory(Options::default()).unwrap();
        let ks = engine.keyspace("test").unwrap();
        ks.put(b"k", b"v").unwrap();

        let (entered_tx, entered_rx) = mpsc::channel();
        let (closed_tx, closed_rx) = mpsc::channel();
        let closer_engine = engine.clone();
        let closer = std::thread::spawn(move || {
            entered_rx
                .recv_timeout(Duration::from_secs(5))
                .expect("multi_get should invoke AsRef before taking an open stripe");
            closer_engine.close().unwrap();
            closed_tx.send(()).unwrap();
        });

        let keys = [ClosingKey {
            bytes: b"k",
            entered: entered_tx,
            closed: closed_rx,
        }];
        let mut reader = engine.reader();
        let err = reader.multi_get(&ks, &keys).unwrap_err();
        drop(keys);
        assert_eq!(err.kind(), ErrorKind::Stopped);
        closer.join().unwrap();
    }

    #[test]
    fn rejected_multi_get_skips_key_conversion() {
        struct PanicKey;
        impl AsRef<[u8]> for PanicKey {
            fn as_ref(&self) -> &[u8] {
                panic!("key conversion should not run for a rejected multi_get");
            }
        }

        let engine = Engine::in_memory(Options::default()).unwrap();
        let other = Engine::in_memory(Options::default()).unwrap();
        let ks = engine.keyspace("test").unwrap();
        let foreign = other.keyspace("test").unwrap();
        let snap = engine.snapshot();
        engine.drop_keyspace("test").unwrap();
        let after_drop = engine.snapshot();
        let later_ks = engine.keyspace("later").unwrap();

        let mut reader = engine.reader();
        assert_eq!(
            reader.multi_get(&foreign, &[PanicKey]).unwrap_err().kind(),
            ErrorKind::UnknownKeyspace
        );

        let mut snap_reader = snap.reader();
        assert_eq!(
            snap_reader
                .multi_get(&foreign, &[PanicKey])
                .unwrap_err()
                .kind(),
            ErrorKind::UnknownKeyspace
        );
        assert_eq!(
            snap_reader.multi_get(&later_ks, &[PanicKey]).unwrap().len(),
            1
        );

        let mut dropped_reader = after_drop.reader();
        assert_eq!(
            dropped_reader
                .multi_get(&ks, &[PanicKey])
                .unwrap_err()
                .kind(),
            ErrorKind::KeyspaceDropped
        );

        drop(reader);
        engine.clone().close().unwrap();
        let mut closed_reader = engine.reader();
        assert_eq!(
            closed_reader
                .multi_get(&foreign, &[PanicKey])
                .unwrap_err()
                .kind(),
            ErrorKind::Stopped
        );
    }
}
