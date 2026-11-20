//! Opening and validating a committed checkpoint.

use super::*;

impl Engine {
    /// Recovers CURRENT without rewriting checkpoint data. Sequences continue after the
    /// checkpoint ceiling. No fallback.
    ///
    /// # Errors
    /// Returns [`OpenError`] for invalid options, missing or locked checkpoints,
    /// I/O, validation failures or worker thread creation failures. A failed
    /// recovery releases its directory lock and leaves no worker running.
    pub fn recover(dir: &Path, options: EngineOptions) -> Result<Engine, OpenError> {
        Self::recover_with_metadata(dir, options).map(|(engine, _)| engine)
    }

    /// Recovers the engine and its opaque application metadata.
    ///
    /// # Errors
    /// Returns the same [`OpenError`] variants as [`Self::recover`].
    pub fn recover_with_metadata(
        dir: &Path,
        options: EngineOptions,
    ) -> Result<(Engine, Vec<u8>), OpenError> {
        options.validate().map_err(OpenError::Config)?;
        let _lock = lock_existing(dir)?;
        let mut loaded = load(dir)?;
        let metadata = std::mem::take(&mut loaded.metadata);
        let engine = Engine::start(Arc::new(loaded.into_core(&options)), &options)?;
        Ok((engine, metadata))
    }
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

/// Locks `dir` for a recovery, which needs `CURRENT`: checked first, since
/// locking would create `LOCK`.
pub(super) fn lock_existing(dir: &Path) -> Result<File, OpenError> {
    let current = dir.join(CURRENT);
    match fs::metadata(&current) {
        Ok(_) => {}
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            return Err(OpenError::NoCheckpoint(dir.to_owned()));
        }
        Err(error) => {
            return Err(OpenError::Io {
                path: current,
                error,
            });
        }
    }
    match lock(dir).map_err(read_error(dir))? {
        Ok(file) => Ok(file),
        Err(()) => Err(OpenError::Locked(dir.to_owned())),
    }
}

/// A committed checkpoint, checked in full before starting any worker.
pub(super) struct Loaded {
    pub(super) ceiling: u64,
    metadata: Vec<u8>,
    next_run_id: u64,
    depth: usize,
    runs: Vec<(usize, u64, SharedRun)>,
    pool: ChunkPool,
    run_bytes: usize,
    held_bytes: usize,
}

impl Loaded {
    pub(super) fn into_core(self, options: &EngineOptions) -> Core {
        {
            let mut ids = RUN_IDS.lock().unwrap_or_else(PoisonError::into_inner);
            ids.1 = ids.1.max(self.next_run_id);
        }
        let core = Core::with_pool(options, self.pool);
        {
            let mut state = core.write_state();
            state.last_seq = self.ceiling;
            state.active = crate::memtable::Memtable::in_pool(
                options.memtable_budget,
                Some(self.ceiling),
                &core.chunks,
            );
            state.runs = self
                .runs
                .iter()
                .map(|(_, _, run)| Arc::clone(run))
                .collect();
            state.levels = Levels::recovered(&self.runs, self.depth, options.level_ratio);
            state.order_runs();
            state.run_bytes = self.run_bytes;
            state.held_bytes = self.held_bytes;
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
    let mut header = [0u8; MANIFEST_HEADER_LEN];
    if found < (MANIFEST_HEADER_LEN + 4) as u64 {
        return Err(corrupt(
            &manifest_path,
            Damage::Length {
                expected: (MANIFEST_HEADER_LEN + 4) as u64,
                found,
            },
        ));
    }
    file.read_exact(&mut header)
        .map_err(read_error(&manifest_path))?;
    let count = u32::from_le_bytes(header[68..72].try_into().unwrap()) as u64;
    let metadata = u64::from_le_bytes(header[60..68].try_into().unwrap());
    let bounds = u64::from_le_bytes(header[72..80].try_into().unwrap());
    let max_bounds = count * 2 * crate::block::MAX_KEY_LEN as u64;
    let expected = MANIFEST_HEADER_LEN as u64
        + count * MANIFEST_ENTRY_LEN as u64
        + metadata.min(MAX_METADATA_BYTES as u64)
        + bounds.min(max_bounds)
        + 4;
    if found != expected || metadata > MAX_METADATA_BYTES as u64 || bounds > max_bounds {
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
    let mut runs: Vec<(usize, u64, SharedRun)> = Vec::with_capacity(manifest.runs.len());
    let mut records = 0u64;
    let mut backings: HashMap<u128, Run<Vec<u8>>> = HashMap::new();
    for entry in &manifest.runs {
        let file = dir.join(RUNS).join(run_name(entry.id));
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
    // Per-run checks cannot detect disagreeing copies in different runs. Check
    // the grouped merge before exposing an engine, including older versions
    // that compaction will eventually visit.
    // This holds one decoded block per overlapping group, with no value copies.
    {
        let published: Vec<_> = runs.iter().map(|(_, _, run)| Arc::clone(run)).collect();
        let directory = super::directory::RunDirectory::build(&published);
        let mut workspace = crate::run::MergeWorkspace::new();
        let mut cursor =
            crate::run::MergeCursor::grouped(&directory, directory.groups(), &mut workspace);
        while cursor
            .next_record()
            .map_err(|error| corrupt(&manifest_path, Damage::Run(error)))?
            .is_some()
        {
            cursor.collapse_copies().map_err(|error| {
                let damage = match error {
                    crate::run::ReadError::Corrupt(error) => Damage::Run(error),
                    crate::run::ReadError::ConflictingCopies(_) => Damage::Order,
                };
                corrupt(&manifest_path, damage)
            })?;
        }
    }
    let published: Vec<_> = runs.iter().map(|(_, _, r)| r.clone()).collect();
    let run_bytes: usize = super::physical_runs(&published)
        .map(|r| r.run.stored_len())
        .sum();
    let held_bytes: usize = super::physical_runs(&published)
        .map(|r| r.run.held_bytes())
        .sum();
    Ok(Loaded {
        ceiling: manifest.last_seq,
        metadata: manifest.metadata,
        next_run_id: manifest.next_run_id,
        depth: manifest.depth,
        pool,
        runs,
        run_bytes,
        held_bytes,
    })
}
