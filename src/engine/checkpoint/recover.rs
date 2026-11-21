//! Opening and validating a committed checkpoint.

use super::*;
use crate::engine::directory::RunDirectory;
use crate::run::{MergeCursor, MergeWorkspace, ReadError};

/// A recovered engine core and the checkpoint's metadata.
pub(in crate::engine) type Recovered = (Core, Option<Vec<u8>>);

/// Recovers the checkpoint `CURRENT` names in `dir`, which the caller has
/// locked, without rewriting checkpoint data and with no fallback; sequences
/// continue after its ceiling. Returns `None` if `dir` has no `CURRENT`.
pub(in crate::engine) fn recover(
    dir: &Path,
    options: &Options,
) -> Result<Option<Recovered>, OpenError> {
    let current = dir.join(CURRENT);
    match fs::metadata(&current) {
        Ok(_) => {}
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(error) => {
            return Err(OpenError::Io {
                path: current,
                error,
            });
        }
    }
    let mut loaded = load(dir)?;
    let metadata = loaded.metadata.take();
    Ok(Some((loaded.into_core(options), metadata)))
}

fn read_error(path: &Path) -> impl FnOnce(io::Error) -> OpenError + '_ {
    move |error: io::Error| match error.kind() {
        io::ErrorKind::NotFound => OpenError::Corrupt {
            path: path.to_owned(),
            damage: Damage::Missing,
        },
        _ => OpenError::Io {
            path: path.to_owned(),
            error,
        },
    }
}

/// One recovered keyspace.
struct LoadedKeyspace {
    id: u32,
    name: String,
    depth: usize,
    runs: Vec<(usize, u64, SharedRun)>,
    directory: Arc<RunDirectory>,
}

/// A committed checkpoint, checked in full before starting any worker.
pub(super) struct Loaded {
    pub(super) ceiling: u64,
    metadata: Option<Vec<u8>>,
    next_run_id: u64,
    next_keyspace: u32,
    keyspaces: Vec<LoadedKeyspace>,
    pool: ChunkPool,
}

impl Loaded {
    pub(super) fn into_core(self, options: &Options) -> Core {
        {
            let mut ids = RUN_IDS.lock().unwrap_or_else(PoisonError::into_inner);
            ids.1 = ids.1.max(self.next_run_id);
        }
        let core = Core::with_pool(options, self.pool);
        {
            let mut state = core.write_state();
            state.last_seq = self.ceiling;
            state.next_keyspace = self.next_keyspace;
            state.active = crate::memtable::Memtable::in_pool(
                options.memtable_size,
                Some(self.ceiling),
                &core.chunks,
            );
            for keyspace in self.keyspaces {
                let name: Arc<str> = Arc::from(keyspace.name);
                let mut space =
                    KeyspaceState::new(keyspace.id, Arc::clone(&name), options.level_ratio);
                space.runs = keyspace
                    .runs
                    .iter()
                    .map(|(_, _, run)| Arc::clone(run))
                    .collect();
                space.levels =
                    Levels::recovered(&keyspace.runs, keyspace.depth, options.level_ratio);
                space.directory = keyspace.directory;
                state.names.insert(name, keyspace.id);
                state.keyspaces.insert(keyspace.id, space);
                state.account_runs(keyspace.id);
            }
            state.refresh_directories();
            state.levels_changed();
        }
        core
    }
}

fn corrupt(path: &Path, damage: Damage) -> OpenError {
    OpenError::Corrupt {
        path: path.to_owned(),
        damage,
    }
}

/// Reads `path` if it holds at most `max` bytes. A longer file is damage,
/// found before anything is allocated for it.
pub(super) fn read_bounded(path: &Path, max: u64) -> Result<Vec<u8>, OpenError> {
    let file = File::open(path).map_err(read_error(path))?;
    let len = file.metadata().map_err(read_error(path))?.len();
    let too_long = |found| {
        corrupt(
            path,
            Damage::Length {
                expected: max,
                found,
            },
        )
    };
    if len > max {
        return Err(too_long(len));
    }
    let mut bytes = Vec::with_capacity(len as usize);
    file.take(max.saturating_add(1))
        .read_to_end(&mut bytes)
        .map_err(read_error(path))?;
    if bytes.len() as u64 > max {
        return Err(too_long(bytes.len() as u64));
    }
    Ok(bytes)
}

/// CURRENT's generation and checked manifest, bounded before allocation.
pub(super) fn committed_manifest(dir: &Path) -> Result<(PathBuf, PathBuf, Manifest), OpenError> {
    let current = dir.join(CURRENT);
    let bytes = read_bounded(&current, CURRENT_LEN as u64)?;
    let generation = decode_current(&bytes).map_err(|damage| corrupt(&current, damage))?;
    let path = checkpoint_dir(dir, generation);
    let manifest_path = path.join(MANIFEST);
    let mut file = File::open(&manifest_path).map_err(read_error(&manifest_path))?;
    let found = file.metadata().map_err(read_error(&manifest_path))?.len();
    // The version is checked first, so that a manifest of another version
    // reports its version rather than its length.
    let mut header = [0u8; MANIFEST_HEADER_LEN];
    let head = found.min(MANIFEST_HEADER_LEN as u64) as usize;
    file.read_exact(&mut header[..head])
        .map_err(read_error(&manifest_path))?;
    if head >= 16 {
        check_version(&header).map_err(|damage| corrupt(&manifest_path, damage))?;
    }
    if found < (MANIFEST_HEADER_LEN + 4) as u64 {
        return Err(corrupt(
            &manifest_path,
            Damage::Length {
                expected: (MANIFEST_HEADER_LEN + 4) as u64,
                found,
            },
        ));
    }
    let expected = Lengths::of(&header)
        .file_len()
        .map_err(|damage| corrupt(&manifest_path, damage))?;
    if found != expected {
        return Err(corrupt(&manifest_path, Damage::Length { expected, found }));
    }
    let mut bytes = Vec::with_capacity(usize::try_from(expected).map_err(|_| {
        corrupt(
            &manifest_path,
            Damage::Length {
                expected: usize::MAX as u64,
                found,
            },
        )
    })?);
    bytes.extend_from_slice(&header);
    file.take(expected - MANIFEST_HEADER_LEN as u64 + 1)
        .read_to_end(&mut bytes)
        .map_err(read_error(&manifest_path))?;
    let manifest = Manifest::decode(&bytes).map_err(|damage| corrupt(&manifest_path, damage))?;
    if manifest.generation != generation {
        return Err(corrupt(
            &manifest_path,
            Damage::Generation {
                expected: generation,
                found: manifest.generation,
            },
        ));
    }
    Ok((path, manifest_path, manifest))
}

/// Reads and checks exactly CURRENT's generation under LOCK.
pub(super) fn load(dir: &Path) -> Result<Loaded, OpenError> {
    let (_, manifest_path, manifest) = committed_manifest(dir)?;
    let pool = ChunkPool::new(RESERVE_BYTES);
    let mut runs_by_keyspace =
        vec![Vec::<(usize, u64, SharedRun)>::new(); manifest.keyspaces.len()];
    let mut records = 0u64;
    let mut backings: HashMap<u128, Run<Vec<u8>>> = HashMap::new();
    for entry in &manifest.runs {
        let file = dir.join(RUNS).join(run_name(entry.id));
        // Decoding checked that the keyspace is listed.
        let at = manifest
            .keyspaces
            .binary_search_by_key(&entry.keyspace, |keyspace| keyspace.id)
            .map_err(|_| {
                corrupt(
                    &manifest_path,
                    Damage::UnknownKeyspace { id: entry.keyspace },
                )
            })?;
        let runs = &mut runs_by_keyspace[at];
        let (mut run, max_seq) = if let Some(backing) = backings.get(&entry.id) {
            (backing.clone(), backing.max_seq())
        } else {
            let mut data = read_bounded(&file, entry.len)?;
            if data.len() as u64 != entry.len {
                return Err(corrupt(
                    &file,
                    Damage::Length {
                        expected: entry.len,
                        found: data.len() as u64,
                    },
                ));
            }
            let filter_at = entry.run_len as usize;
            let filter_end = data.len() - 4;
            let filter = &data[filter_at..filter_end];
            if crc(&[filter]).to_le_bytes() != data[filter_end..] {
                return Err(corrupt(&file, Damage::FilterChecksum));
            }
            if crc(&[&data]) != entry.sum {
                return Err(corrupt(&file, Damage::Checksum));
            }
            let filter = filter.to_vec();
            data.truncate(filter_at);
            let (run, max_seq) = Run::recover_checkpoint_in(data, &filter, &pool)
                .map_err(|error| corrupt(&file, Damage::Run(error)))?;
            records += run.len() as u64;
            backings.insert(entry.id, run.clone());
            (run, max_seq)
        };
        if max_seq > manifest.last_seq
            || entry
                .seq_override
                .is_some_and(|seq| seq > manifest.last_seq)
        {
            return Err(corrupt(
                &file,
                Damage::Sequence {
                    seq: max_seq.max(entry.seq_override.unwrap_or(0)),
                    ceiling: manifest.last_seq,
                },
            ));
        }
        if let Some(seq) = entry.seq_override {
            if max_seq != 0 {
                return Err(corrupt(&file, Damage::Order));
            }
            run.set_seq_override(seq);
        }
        if let Some((min, max)) = &entry.bounds {
            run.restore_slice(min.clone(), max.clone())
                .map_err(|error| corrupt(&file, Damage::Run(error)))?;
        }
        let Some(range) = run.key_range() else {
            return Err(corrupt(&file, Damage::Order));
        };
        if entry.level > 0
            && runs.last().is_some_and(|(level, _, previous)| {
                *level == entry.level && previous.run.key_range().unwrap().max >= range.min
            })
        {
            return Err(corrupt(&file, Damage::Order));
        }
        let largest_block = run.largest_block();
        runs.push((
            entry.level,
            entry.flush,
            Arc::new(Published {
                id: entry.id,
                checkpoint_crc: OnceLock::from(entry.sum),
                run,
                largest_block,
            }),
        ));
    }
    if records != manifest.records {
        return Err(corrupt(
            &manifest_path,
            Damage::Records {
                expected: manifest.records,
                found: records,
            },
        ));
    }
    let mut keyspaces = Vec::with_capacity(manifest.keyspaces.len());
    let mut workspace = MergeWorkspace::new();
    for (keyspace, runs) in manifest.keyspaces.into_iter().zip(runs_by_keyspace) {
        let published: Vec<_> = runs.iter().map(|(_, _, run)| Arc::clone(run)).collect();
        let directory = Arc::new(RunDirectory::build(&published));
        validate_copies(&directory, &mut workspace, &manifest_path)?;
        keyspaces.push(LoadedKeyspace {
            id: keyspace.id,
            name: keyspace.name,
            depth: keyspace.depth,
            runs,
            directory,
        });
    }
    Ok(Loaded {
        ceiling: manifest.last_seq,
        metadata: manifest.metadata,
        next_run_id: manifest.next_run_id,
        next_keyspace: manifest.next_keyspace,
        keyspaces,
        pool,
    })
}

/// Each run has already been validated. A single directory group has
/// disjoint key ranges, so only overlapping groups can disagree. Check
/// all versions in the merge, including older ones that compaction may visit.
fn validate_copies(
    directory: &RunDirectory,
    workspace: &mut MergeWorkspace,
    path: &Path,
) -> Result<(), OpenError> {
    if directory.group_count() <= 1 {
        return Ok(());
    }
    let mut cursor = MergeCursor::grouped(directory, directory.groups(), workspace);
    while cursor
        .next_record()
        .map_err(|error| corrupt(path, Damage::Run(error)))?
        .is_some()
    {
        cursor.collapse_copies().map_err(|error| {
            let damage = match error {
                ReadError::Corrupt(error) => Damage::Run(error),
                ReadError::ConflictingCopies(_) => Damage::Order,
            };
            corrupt(path, damage)
        })?;
    }
    Ok(())
}
