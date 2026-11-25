//! Committing checkpoints: run files, manifests, CURRENT and reclamation.

use super::*;

/// Places where a test can stop a checkpoint as a crash would: returning at
/// once, removing nothing.
#[cfg(test)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Crash {
    PartialRun,
    AfterFirstRun,
    BeforeManifestSync,
    BeforeRename,
    AfterRename,
    BeforeReclaim,
}

#[cfg(test)]
thread_local! {
    pub(super) static CRASH_AT: std::cell::Cell<Option<Crash>> = const { std::cell::Cell::new(None) };
}

/// Whether a test asked to crash here, which also makes the caller skip all
/// cleanup.
#[cfg(test)]
pub(super) fn crash(at: Crash) -> bool {
    CRASH_AT.with(|crash| crash.get() == Some(at))
}

/// Stands for the crash a test asked for.
#[cfg(test)]
pub(super) fn crashed(path: &Path, committed: Option<Box<CheckpointReport>>) -> CheckpointError {
    CheckpointError::Io {
        path: path.to_owned(),
        error: io::Error::other("crash injected by a test"),
        committed,
    }
}

/// An error a test makes the rename of CURRENT report.
#[cfg(test)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum RenameFault {
    /// The rename fails and nothing changes.
    Fails,
    /// The rename takes effect, then reports an error.
    FailsAfterEffect,
    /// The rename fails and nothing changes, and CURRENT cannot be read back.
    FailsUnread,
}

#[cfg(test)]
thread_local! {
    pub(super) static RENAME_FAULT: std::cell::Cell<Option<RenameFault>> = const { std::cell::Cell::new(None) };
}

/// Renames `tmp` over CURRENT in `dir`.
fn rename_current(tmp: &Path, dir: &Path) -> io::Result<()> {
    #[cfg(test)]
    if let Some(fault) = RENAME_FAULT.with(std::cell::Cell::get) {
        if fault == RenameFault::FailsAfterEffect {
            fs::rename(tmp, dir.join(CURRENT))?;
        }
        return Err(io::Error::other("rename error injected by a test"));
    }
    fs::rename(tmp, dir.join(CURRENT))
}

/// The generation CURRENT in `dir` names, or `None` if it cannot be read.
fn current_generation(dir: &Path) -> Option<u64> {
    #[cfg(test)]
    if RENAME_FAULT.with(std::cell::Cell::get) == Some(RenameFault::FailsUnread) {
        return None;
    }
    let bytes = read_bounded(&dir.join(CURRENT), CURRENT_LEN as u64).ok()?;
    decode_current(&bytes).ok()
}

#[cfg(test)]
macro_rules! crash_point {
    ($at:expr, $path:expr) => {
        if crash($at) {
            return Err(Failure::Crashed(crashed($path, None)));
        }
    };
}

#[cfg(not(test))]
macro_rules! crash_point {
    ($at:expr, $path:expr) => {};
}

/// A failure while building the new checkpoint directory.
enum Failure {
    /// Clean up and report.
    Error(CheckpointError),
    /// A test's crash: report without cleaning up.
    #[cfg(test)]
    Crashed(CheckpointError),
}

impl From<CheckpointError> for Failure {
    fn from(error: CheckpointError) -> Self {
        Self::Error(error)
    }
}

fn io_error(path: &Path) -> impl FnOnce(io::Error) -> CheckpointError + '_ {
    move |error| CheckpointError::Io {
        path: path.to_owned(),
        error,
        committed: None,
    }
}

/// Writes `bytes` to a new file at `path` and syncs it.
fn write_synced(path: &Path, bytes: &[u8]) -> Result<(), CheckpointError> {
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
        .map_err(io_error(path))?;
    file.write_all(bytes).map_err(io_error(path))?;
    file.sync_all().map_err(io_error(path))
}

/// Generations of the `ckpt-*` directories present.
fn generations(dir: &Path) -> Result<Vec<u64>, CheckpointError> {
    let mut found = Vec::new();
    for entry in fs::read_dir(dir).map_err(io_error(dir))? {
        let entry = entry.map_err(io_error(dir))?;
        let name = entry.file_name();
        let Some(digits) = name.to_str().and_then(|name| name.strip_prefix(PREFIX)) else {
            continue;
        };
        if digits.len() == 20
            && digits.bytes().all(|b| b.is_ascii_digit())
            && let Ok(generation) = digits.parse()
        {
            found.push(generation);
        }
    }
    Ok(found)
}

/// GC runs only after the new CURRENT and its parent are durable. The previous
/// committed manifest is retained even if abandoned attempts lie between it
/// and the new generation.
fn reclaim(
    dir: &Path,
    found: &[u64],
    manifest: &Manifest,
    previous: Option<&Manifest>,
) -> (usize, usize, usize) {
    let keep: HashSet<_> = manifest
        .runs
        .iter()
        .chain(previous.into_iter().flat_map(|m| &m.runs))
        .map(|r| run_name(r.id))
        .collect();
    let (mut files, mut directories, mut failed) = (0, 0, 0);
    if let Ok(entries) = fs::read_dir(dir.join(RUNS)) {
        for entry in entries {
            let Ok(entry) = entry else {
                failed += 1;
                continue;
            };
            let name = entry.file_name();
            let Some(name) = name.to_str() else { continue };
            let digits = name.strip_suffix(".tmp").unwrap_or(name);
            if digits.len() == 32
                && digits.bytes().all(|b| b.is_ascii_hexdigit())
                && !keep.contains(name)
            {
                match fs::remove_file(entry.path()) {
                    Ok(()) => files += 1,
                    Err(_) => failed += 1,
                }
            }
        }
    } else {
        failed += 1;
    }
    for &generation in found {
        if generation != manifest.generation && generation != manifest.previous {
            match fs::remove_dir_all(checkpoint_dir(dir, generation)) {
                Ok(()) => directories += 1,
                Err(_) => failed += 1,
            }
        }
    }
    (files, directories, failed)
}

/// One keyspace of a checkpoint image.
struct ImageKeyspace {
    id: u32,
    name: Arc<str>,
    depth: usize,
    runs: Vec<(usize, u64, SharedRun)>,
}

struct Image {
    /// The keyspaces live at the ceiling, by ID.
    keyspaces: Vec<ImageKeyspace>,
    next_keyspace: u32,
    ceiling: u64,
    _live: Arc<()>,
}

impl Core {
    /// Use the normal flush job, keeping precisely the run image at rotation.
    /// Taking a later snapshot of the state would include runs published by
    /// concurrent ingestion, or miss pre-ceiling runs replaced by compaction.
    fn checkpoint_image(&self) -> Result<Image, CheckpointError> {
        loop {
            let serial = self.serialize_flush();
            let mut state = self.write_state();
            // Respect the ordinary frozen-table bound before rotating active.
            // This can occur while the background flush is building.
            if !state.active.is_empty() && state.frozen.len() >= self.max_frozen_memtables {
                let job = FlushJob::new(self, serial, &state);
                drop(state);
                job.build().publish();
                continue;
            }
            let mut image = Image {
                keyspaces: state
                    .keyspaces
                    .iter()
                    .map(|(&id, space)| ImageKeyspace {
                        id,
                        name: Arc::clone(&space.name),
                        depth: space.levels.depth(),
                        runs: space.levels.checkpoint_runs(),
                    })
                    .collect(),
                next_keyspace: state.next_keyspace,
                ceiling: state.last_seq,
                _live: Arc::clone(&self.snapshots),
            };
            if !state.active.is_empty() {
                self.freeze_active(&mut state);
            }
            if state.frozen.is_empty() {
                return Ok(image);
            }
            let job = FlushJob::new(self, serial, &state);
            drop(state);
            #[cfg(test)]
            AFTER_ROTATION.with(|hook| {
                if let Some(hook) = hook.borrow_mut().take() {
                    hook();
                }
            });
            let (_, kept) = job.build().publish_kept();
            for kept in kept {
                // The flush began in the section the image was taken in, so
                // its keyspaces are the image's.
                let at = image
                    .keyspaces
                    .binary_search_by_key(&kept.keyspace, |space| space.id)
                    .expect("a flushed keyspace is in the image");
                let space = &mut image.keyspaces[at];
                space
                    .runs
                    .extend(kept.runs.into_iter().map(|run| (0, kept.flush, run)));
                space.runs.sort_by_key(|(level, _, _)| *level);
            }
            return Ok(image);
        }
    }
}

/// Visit the exact file bytes without allocating a contiguous serialization.
pub(super) fn run_parts(
    run: &Run<Vec<u8>>,
    mut part: impl FnMut(&[u8]) -> io::Result<()>,
) -> io::Result<()> {
    for bytes in run.chunks() {
        part(bytes)?;
    }
    let mut filter_sum = Hasher::new();
    for word in run.filter_words() {
        filter_sum.update(&word);
        part(&word)?;
    }
    part(&filter_sum.finalize().to_le_bytes())
}

struct Written {
    manifest: Manifest,
    bytes: u64,
    bytes_reused: u64,
    runs_written: usize,
    runs_reused: usize,
    files_deleted: usize,
    largest_run_bytes: usize,
}

impl Written {
    fn report(
        &self,
        generation: u64,
        files_deleted: usize,
        reclaimed: usize,
        unreclaimed: usize,
    ) -> CheckpointReport {
        CheckpointReport {
            generation,
            last_seq: self.manifest.last_seq,
            records: self.manifest.records,
            runs: self.manifest.runs.len(),
            bytes_written: self.bytes,
            bytes_reused: self.bytes_reused,
            runs_written: self.runs_written,
            runs_reused: self.runs_reused,
            files_deleted: files_deleted + self.files_deleted,
            largest_run_bytes: self.largest_run_bytes,
            reclaimed,
            unreclaimed,
        }
    }
}

fn write_runs(
    image: &Image,
    dir: &Path,
    generation: u64,
    previous: u64,
    metadata: Option<&[u8]>,
) -> Result<Written, Failure> {
    let runs_dir = dir.join(RUNS);
    fs::create_dir_all(&runs_dir).map_err(io_error(&runs_dir))?;
    let image_runs = || {
        image.keyspaces.iter().flat_map(|space| {
            space
                .runs
                .iter()
                .map(move |(level, flush, run)| (space.id, *level, *flush, run))
        })
    };
    let mut written = Written {
        manifest: Manifest {
            generation,
            last_seq: image.ceiling,
            records: {
                let mut seen = HashSet::new();
                image_runs()
                    .filter(|(_, _, _, r)| seen.insert(r.id))
                    .map(|(_, _, _, r)| r.run.len() as u64)
                    .sum()
            },
            next_run_id: image_runs()
                .map(|(_, _, _, run)| (run.id as u64) + 1)
                .chain(std::iter::once(
                    RUN_IDS.lock().unwrap_or_else(PoisonError::into_inner).1,
                ))
                .max()
                .unwrap(),
            previous,
            next_keyspace: image.next_keyspace,
            keyspaces: image
                .keyspaces
                .iter()
                .map(|space| KeyspaceEntry {
                    id: space.id,
                    name: space.name.to_string(),
                    depth: space.depth,
                })
                .collect(),
            metadata: metadata.map(<[u8]>::to_vec),
            runs: Vec::with_capacity(image_runs().count()),
        },
        bytes: 0,
        bytes_reused: 0,
        runs_written: 0,
        runs_reused: 0,
        files_deleted: 0,
        largest_run_bytes: 0,
    };
    let mut physical_sums = HashMap::new();
    for (keyspace, level, flush, published) in image_runs() {
        let run = &published.run;
        let len = (run.stored_len() + run.filter_bytes() + 4) as u64;
        if let Some(&sum) = physical_sums.get(&published.id) {
            written.manifest.runs.push(RunEntry {
                id: published.id,
                keyspace,
                level,
                flush,
                seq_override: run.seq_override(),
                len,
                run_len: run.stored_len() as u64,
                sum,
                bounds: run.slice().map(|(a, b)| (a.to_vec(), b.to_vec())),
            });
            continue;
        }
        let file = runs_dir.join(run_name(published.id));
        let exists = match fs::metadata(&file) {
            Ok(found) if found.len() == len => true,
            // Memory holds the exact bytes, so a damaged file is replaced
            // through a temporary file and a rename, like a missing one.
            Ok(_) => false,
            Err(e) if e.kind() == io::ErrorKind::NotFound => false,
            Err(e) => return Err(io_error(&file)(e).into()),
        };
        let sum = if exists {
            written.bytes_reused += len;
            written.runs_reused += 1;
            *published.checkpoint_crc.get_or_init(|| {
                let mut sum = Hasher::new();
                run_parts(run, |bytes| {
                    sum.update(bytes);
                    Ok(())
                })
                .expect("hashing cannot fail");
                sum.finalize()
            })
        } else {
            let tmp = runs_dir.join(format!("{}.tmp", run_name(published.id)));
            // An interrupted temporary file is never a reusable final file.
            match fs::remove_file(&tmp) {
                Ok(()) => written.files_deleted += 1,
                Err(e) if e.kind() == io::ErrorKind::NotFound => {}
                Err(e) => return Err(io_error(&tmp)(e).into()),
            }
            let mut output = BufWriter::new(File::create_new(&tmp).map_err(io_error(&tmp))?);
            #[cfg(test)]
            if crash(Crash::PartialRun) {
                output
                    .write_all(&run.chunks().next().unwrap()[..1])
                    .map_err(io_error(&tmp))?;
                return Err(Failure::Crashed(crashed(&tmp, None)));
            }
            let mut sum = Hasher::new();
            run_parts(run, |bytes| {
                output.write_all(bytes)?;
                sum.update(bytes);
                Ok(())
            })
            .map_err(io_error(&tmp))?;
            output.flush().map_err(io_error(&tmp))?;
            output.get_ref().sync_all().map_err(io_error(&tmp))?;
            fs::rename(&tmp, &file).map_err(io_error(&file))?;
            written.bytes += len;
            written.runs_written += 1;
            let sum = sum.finalize();
            let _ = published.checkpoint_crc.set(sum);
            crash_point!(Crash::AfterFirstRun, &file);
            sum
        };
        physical_sums.insert(published.id, sum);
        written.largest_run_bytes = written.largest_run_bytes.max(len as usize);
        written.manifest.runs.push(RunEntry {
            id: published.id,
            keyspace,
            level,
            flush,
            seq_override: run.seq_override(),
            len,
            run_len: run.stored_len() as u64,
            sum,
            bounds: run.slice().map(|(a, b)| (a.to_vec(), b.to_vec())),
        });
    }
    sync_dir(&runs_dir).map_err(io_error(&runs_dir))?;
    let manifest = written.manifest.encode();
    let total = written.bytes.saturating_add(manifest.len() as u64);
    let path = checkpoint_dir(dir, generation);
    let file = path.join(MANIFEST);
    write_synced(&file, &manifest)?;
    written.bytes = total;
    crash_point!(Crash::BeforeManifestSync, &file);
    sync_dir(&path).map_err(io_error(&path))?;
    sync_dir(dir).map_err(io_error(dir))?;
    Ok(written)
}

impl Core {
    /// Flushes and commits the state at one sequence ceiling into `dir`,
    /// with `metadata` if any. Existing run files are reused; file I/O
    /// holds no state lock. Keeps one previous generation. The caller holds
    /// `dir`'s lock and the checkpoint serializer.
    pub(in crate::engine) fn checkpoint_into(
        &self,
        dir: &Path,
        metadata: Option<&[u8]>,
    ) -> Result<CheckpointReport, CheckpointError> {
        if metadata.is_some_and(|metadata| metadata.len() > MAX_METADATA_LEN) {
            return Err(CheckpointError::MetadataLimit);
        }
        let previous = match committed_manifest(dir) {
            Ok((_, _, manifest)) => Some(manifest),
            Err(OpenError::Corrupt {
                path,
                damage: Damage::Missing,
            }) if path == dir.join(CURRENT) => None,
            Err(error) => return Err(io_error(dir)(io::Error::other(error))),
        };
        let found = generations(dir)?;
        let generation = found
            .iter()
            .copied()
            .chain(previous.iter().map(|m| m.generation))
            .max()
            .unwrap_or(0)
            .checked_add(1)
            .ok_or_else(|| io_error(dir)(io::Error::other("checkpoint generations exhausted")))?;
        let _ = fs::remove_file(dir.join(CURRENT_TMP));
        let path = checkpoint_dir(dir, generation);
        fs::create_dir(&path).map_err(io_error(&path))?;
        let built = self
            .checkpoint_image()
            .map_err(Failure::Error)
            .and_then(|image| {
                #[cfg(test)]
                AFTER_IMAGE.with(|hook| {
                    if let Some(hook) = hook.borrow_mut().take() {
                        hook();
                    }
                });
                write_runs(
                    &image,
                    dir,
                    generation,
                    previous.as_ref().map_or(0, |m| m.generation),
                    metadata,
                )
            });
        let written = match built {
            Ok(written) => written,
            Err(Failure::Error(error)) => {
                let _ = fs::remove_dir_all(&path);
                return Err(error);
            }
            #[cfg(test)]
            Err(Failure::Crashed(error)) => return Err(error),
        };
        let tmp = dir.join(CURRENT_TMP);
        let current = encode_current(generation);
        #[cfg(test)]
        if crash(Crash::BeforeRename) {
            write_synced(&tmp, &current)?;
            return Err(crashed(&tmp, None));
        }
        write_synced(&tmp, &current).inspect_err(|_| {
            let _ = fs::remove_file(&tmp);
            let _ = fs::remove_dir_all(&path);
        })?;
        if let Err(error) = rename_current(&tmp, dir) {
            // A rename can report an error after it took effect. If CURRENT
            // names the new generation, the commit is done: removing that
            // generation would lose the checkpoint, and reclamation waits
            // for the next one. If CURRENT cannot be read back, the outcome
            // is unknown, so the new generation is kept and reported as not
            // committed; the next commit reclaims it unless CURRENT names it.
            let named = current_generation(dir);
            let after_commit_point = named == Some(generation);
            if after_commit_point {
                let _ = sync_dir(dir);
            } else {
                let _ = fs::remove_file(&tmp);
                if named.is_some() {
                    let _ = fs::remove_dir_all(&path);
                }
            }
            return Err(CheckpointError::Io {
                path: dir.to_owned(),
                error,
                committed: after_commit_point
                    .then(|| Box::new(written.report(generation, 0, 0, 0))),
            });
        }
        #[cfg(test)]
        if crash(Crash::AfterRename) {
            return Err(crashed(
                dir,
                Some(Box::new(written.report(generation, 0, 0, 0))),
            ));
        }
        if let Err(error) = sync_dir(dir) {
            return Err(CheckpointError::Io {
                path: dir.to_owned(),
                error,
                committed: Some(Box::new(written.report(generation, 0, 0, 0))),
            });
        }
        #[cfg(test)]
        if crash(Crash::BeforeReclaim) {
            return Err(crashed(
                dir,
                Some(Box::new(written.report(generation, 0, 0, 0))),
            ));
        }
        let (files_deleted, reclaimed, unreclaimed) =
            reclaim(dir, &found, &written.manifest, previous.as_ref());
        let _ = sync_dir(&dir.join(RUNS));
        let _ = sync_dir(dir);
        Ok(written.report(generation, files_deleted, reclaimed, unreclaimed))
    }
}

#[cfg(test)]
thread_local! {
    pub(super) static AFTER_ROTATION: std::cell::RefCell<Option<Box<dyn FnOnce()>>> = const { std::cell::RefCell::new(None) };
    pub(super) static AFTER_IMAGE: std::cell::RefCell<Option<Box<dyn FnOnce()>>> = const { std::cell::RefCell::new(None) };
}
