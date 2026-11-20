//! Incremental checkpoints of immutable runs, with no re-encoding or recompression.
//!
//! At flush rotation a checkpoint pins the existing runs and the tables being
//! flushed. The normal flush builder publishes those tables, completing the
//! structural snapshot at that rotation's sequence ceiling. Later writes and
//! compactions proceed while files are written, outside the state lock.
//!
//! Layout: `runs/<32-hex-digit RunId>` holds the exact v6 run chunks, followed
//! by little-endian Bloom words and their CRC-32. The manifest records both
//! lengths and a CRC-32 of the entire file. Filters use the fixed hash defined
//! in `run/filter.rs`; recovery validates runs and filter membership, then
//! checks copies across runs, without rebuilding filters.
//! `ckpt-<20-digit generation>/MANIFEST` records levels, level-0 flush groups,
//! effective sequence overrides, ceiling, next ID counter and application data.
//!
//! Publication uses the LOCK/CURRENT protocol: sync every new run file,
//! its directory, the manifest, its directory and the parent; write and sync
//! CURRENT.tmp, rename over CURRENT (the commit point), then sync the parent.
//! Only after that sync does GC remove files outside the union of the new and
//! previous manifests. Before rename an error leaves the old commit intact;
//! after rename a sync error retains both generations. Atomic temporary run
//! files prevent an interrupted write from becoming a reusable final file.
//!
//! LOCK is an advisory exclusive file lock, released on close or process exit.
//! Recovery reads exactly CURRENT, with no fallback, and leaves checkpoint data
//! unchanged. It may create LOCK when the lock file is absent.
//! Durability assumes local POSIX rename and working file/directory fsync
//! (Linux ext4/XFS); network filesystems and external writers are unsupported.
//! Checksums detect damage rather than repairing it. CURRENT visibility after
//! process death is tested; power-loss durability depends on the filesystem.
//!
//! Run IDs combine a random process namespace and a monotonic counter. The
//! counter is persisted and advanced on recovery; a fresh namespace after a
//! restart also prevents collisions with abandoned or forked recovery branches.
//! File names remain unchanged for recovered runs. A checkpoint retains one
//! structural snapshot, but builds no additional copy of an existing run.
//! Recovery temporarily holds one contiguous file while packing its frames into
//! pool chunks. Application metadata is bounded at 16 MiB.

use crate::chunk::{ChunkPool, RESERVE_BYTES};

use std::fmt;
use std::fs::{self, File, OpenOptions, TryLockError};
use std::io::{self, BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, PoisonError};

use crc32fast::Hasher;

use crate::run::{CorruptRun, Run};
use std::collections::{HashMap, HashSet};
use std::hash::{BuildHasher, RandomState};
use std::sync::OnceLock;

use super::levels::Levels;
use super::{
    ConfigError, Core, Engine, EngineOptions, FlushJob, Published, SharedRun, directory,
    physical_runs,
};

mod errors;
mod manifest;
mod recover;
mod write;

pub use errors::{CheckpointError, Damage, OpenError};
use manifest::*;
use recover::*;
#[cfg(test)]
use write::*;

pub(super) const LOCK: &str = "LOCK";
const CURRENT: &str = "CURRENT";
const CURRENT_TMP: &str = "CURRENT.tmp";
const MANIFEST: &str = "MANIFEST";
const PREFIX: &str = "ckpt-";

const CURRENT_MAGIC: [u8; 8] = *b"TSRCUR\0\x01";
const CURRENT_LEN: usize = 20;
const MANIFEST_MAGIC: [u8; 8] = *b"TSRCKPT\x01";
// Manifest version 5 persists exact inclusive bounds of shared run views.
// Run payloads use format version 6.
const MANIFEST_VERSION: u32 = 5;
// Magic/version/flags, generation/ceiling/records/next-id/previous,
// depth, metadata length, run count. Entries include both file and run lengths.
const MANIFEST_HEADER_LEN: usize = 80;
const MANIFEST_ENTRY_LEN: usize = 68;
const MAX_METADATA_BYTES: usize = 16 << 20;
const RUNS: &str = "runs";
// Keep namespace and counter together, including when recovery advances the
// counter to the end of its range. Rollover starts a fresh namespace.
static RUN_IDS: Mutex<(Option<u64>, u64)> = Mutex::new((None, 1));

pub(super) fn new_run_id() -> u128 {
    let mut ids = RUN_IDS.lock().unwrap_or_else(PoisonError::into_inner);
    if ids.1 == u64::MAX {
        ids.0 = None;
        ids.1 = 1;
    }
    let namespace = *ids
        .0
        .get_or_insert_with(|| RandomState::new().hash_one(std::process::id()));
    let number = ids.1;
    ids.1 += 1;
    (u128::from(namespace) << 64) | u128::from(number)
}

/// Bounds on one checkpoint.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct CheckpointOptions {
    /// Maximum new run-file and manifest bytes written in this attempt.
    /// Reused files, CURRENT and filesystem overhead are excluded. Older
    /// generations remain until the new commit is durable.
    pub max_bytes: u64,
}

impl Default for CheckpointOptions {
    /// No limit on disk.
    fn default() -> Self {
        Self {
            max_bytes: u64::MAX,
        }
    }
}

/// What a committed checkpoint holds and the I/O performed for it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct CheckpointReport {
    /// Its generation, named by CURRENT.
    pub generation: u64,
    /// Every update through this ceiling is included; later updates are excluded.
    pub last_seq: u64,
    /// Physical records, including retained versions and tombstones.
    pub records: u64,
    /// Total referenced run files.
    pub runs: usize,
    /// New run-file and manifest bytes, excluding CURRENT.
    pub bytes_written: u64,
    /// Existing run-file bytes skipped, including filters.
    pub bytes_reused: u64,
    /// New run files written.
    pub runs_written: usize,
    /// Existing run files skipped.
    pub runs_reused: usize,
    /// Obsolete run files (including interrupted temporary files) deleted.
    pub files_deleted: usize,
    /// Largest referenced run file, including its filter.
    pub largest_run_bytes: usize,
    /// Obsolete generation directories removed.
    pub reclaimed: usize,
    /// File or directory removals that failed; retried after the next commit.
    pub unreclaimed: usize,
}

fn checkpoint_dir(dir: &Path, generation: u64) -> PathBuf {
    dir.join(format!("{PREFIX}{generation:020}"))
}

fn run_name(id: u128) -> String {
    format!("{id:032x}")
}

fn crc(parts: &[&[u8]]) -> u32 {
    let mut hasher = Hasher::new();
    for part in parts {
        hasher.update(part);
    }
    hasher.finalize()
}

fn sync_dir(path: &Path) -> io::Result<()> {
    File::open(path)?.sync_all()
}

/// Opens and locks `LOCK`, which is released when the file is dropped.
pub(super) fn lock(dir: &Path) -> io::Result<Result<File, ()>> {
    let file = OpenOptions::new()
        .create(true)
        .truncate(false)
        .write(true)
        .open(dir.join(LOCK))?;
    match file.try_lock() {
        Ok(()) => Ok(Ok(file)),
        Err(TryLockError::WouldBlock) => Ok(Err(())),
        Err(TryLockError::Error(error)) => Err(error),
    }
}

#[cfg(test)]
#[path = "checkpoint_robustness.rs"]
mod robustness;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::EngineWorkspace;

    #[test]
    fn an_unsupported_manifest_version_is_rejected_with_a_valid_checksum() {
        let dir = scratch("unsupported-manifest-version");
        let engine = Engine::new(manual()).unwrap();
        engine.put(b"key", b"value").unwrap();
        engine
            .checkpoint(&dir, CheckpointOptions::default())
            .unwrap();
        engine.close().unwrap();
        let (_, _, manifest) = committed_manifest(&dir).unwrap();
        let mut bytes = manifest.encode();
        bytes[8..12].copy_from_slice(&u32::MAX.to_le_bytes());
        let end = bytes.len() - 4;
        let sum = crc(&[&bytes[..end]]);
        bytes[end..].copy_from_slice(&sum.to_le_bytes());
        assert!(matches!(
            Manifest::decode(&bytes),
            Err(Damage::Unsupported {
                version: u32::MAX,
                ..
            })
        ));
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn manifest_lengths_are_bounded_by_the_run_and_filter_formats() {
        let dir = scratch("format-allocation-bounds");
        let engine = Engine::new(manual()).unwrap();
        engine.put(b"key", b"value").unwrap();
        engine
            .checkpoint(&dir, CheckpointOptions::default())
            .unwrap();
        engine.close().unwrap();
        let (_, _, mut manifest) = committed_manifest(&dir).unwrap();
        for (run_len, len) in [
            (u32::MAX as u64 + 1, u32::MAX as u64 + 5),
            (48, 48 + crate::run::MAX_FILTER_BYTES as u64 + 5),
            (48, u64::MAX),
        ] {
            manifest.runs[0].run_len = run_len;
            manifest.runs[0].len = len;
            assert!(matches!(
                Manifest::decode(&manifest.encode()),
                Err(Damage::Length { .. })
            ));
        }
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn recovery_rejects_conflicting_copies_hidden_by_a_newer_version() {
        let dir = scratch("hidden-conflicting-copies");
        let engine = Engine::new(manual()).unwrap();
        for value in [b"old", b"bad", b"new"] {
            let mut batch = crate::engine::WriteBatch::new();
            batch.put(b"key", value);
            engine.ingest_sorted(&batch).unwrap();
        }
        engine
            .checkpoint(&dir, CheckpointOptions::default())
            .unwrap();
        engine.close().unwrap();
        let (_, path, mut manifest) = committed_manifest(&dir).unwrap();
        assert_eq!(manifest.runs.len(), 3);
        manifest.runs[1].seq_override = manifest.runs[0].seq_override;
        fs::write(path, manifest.encode()).unwrap();
        assert!(matches!(
            Engine::recover(&dir, manual()),
            Err(OpenError::Corrupt {
                damage: Damage::Order,
                ..
            })
        ));
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn recovery_with_an_exhausted_run_counter_can_flush_and_checkpoint() {
        let dir = scratch("exhausted-run-counter");
        let engine = Engine::new(manual()).unwrap();
        engine.put(b"a", b"old").unwrap();
        engine
            .checkpoint(&dir, CheckpointOptions::default())
            .unwrap();
        engine.close().unwrap();
        let (_, path, mut manifest) = committed_manifest(&dir).unwrap();
        manifest.next_run_id = u64::MAX;
        fs::write(path, manifest.encode()).unwrap();
        let recovered = Engine::recover(&dir, manual()).unwrap();
        for key in [b"b", b"c"] {
            recovered.put(key, b"new").unwrap();
            recovered.flush().unwrap();
        }
        let expected = state(&recovered);
        recovered
            .checkpoint(&dir, CheckpointOptions::default())
            .unwrap();
        recovered.close().unwrap();
        let again = Engine::recover(&dir, manual()).unwrap();
        assert_eq!(state(&again), expected);
        again.close().unwrap();
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn recovery_rejects_conflicting_copies_across_valid_runs() {
        for identical in [false, true] {
            let dir = scratch(if identical {
                "identical-copies"
            } else {
                "conflicting-copies"
            });
            let engine = Engine::new(manual()).unwrap();
            for value in [b"old".as_slice(), if identical { b"old" } else { b"new" }] {
                let mut batch = crate::engine::WriteBatch::new();
                batch.put(b"key", value);
                engine.ingest_sorted(&batch).unwrap();
            }
            engine
                .checkpoint(&dir, CheckpointOptions::default())
                .unwrap();
            engine.close().unwrap();
            let (_, path, mut manifest) = committed_manifest(&dir).unwrap();
            assert_eq!(manifest.runs.len(), 2);
            manifest.runs[1].seq_override = manifest.runs[0].seq_override;
            fs::write(path, manifest.encode()).unwrap();
            let result = Engine::recover(&dir, manual());
            if identical {
                let recovered = result.unwrap();
                assert_eq!(state(&recovered), vec![(b"key".to_vec(), b"old".to_vec())]);
                recovered.close().unwrap();
            } else {
                assert!(matches!(
                    result,
                    Err(OpenError::Corrupt {
                        damage: Damage::Order,
                        ..
                    })
                ));
            }
            fs::remove_dir_all(dir).unwrap();
        }
    }

    #[test]
    fn an_unlimited_file_bound_does_not_overflow() {
        let dir = scratch("unlimited-read-bound");
        fs::create_dir_all(&dir).unwrap();
        let path = dir.join("bytes");
        fs::write(&path, b"small").unwrap();
        assert_eq!(read_bounded(&path, u64::MAX).unwrap(), b"small");
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn a_manifest_with_an_unlimited_run_length_is_typed_damage() {
        let dir = scratch("unlimited-run-length");
        let engine = Engine::new(manual()).unwrap();
        engine.put(b"key", b"value").unwrap();
        engine
            .checkpoint(&dir, CheckpointOptions::default())
            .unwrap();
        engine.close().unwrap();
        let (_, path, mut manifest) = committed_manifest(&dir).unwrap();
        manifest.runs[0].len = u64::MAX;
        fs::write(&path, manifest.encode()).unwrap();
        assert!(matches!(
            Engine::recover(&dir, manual()),
            Err(OpenError::Corrupt { .. })
        ));
        fs::remove_dir_all(dir).unwrap();
    }

    /// The defaults without the background worker: these tests flush and
    /// compact by hand and count what each step leaves.
    fn manual() -> EngineOptions {
        EngineOptions {
            background_compaction: None,
            ..EngineOptions::default()
        }
    }

    /// Every file under `dir` with its length and contents, to show that a
    /// failed open changed nothing.
    fn files(dir: &Path) -> Vec<(PathBuf, Vec<u8>)> {
        let mut out = Vec::new();
        let mut pending = vec![dir.to_owned()];
        while let Some(at) = pending.pop() {
            for entry in fs::read_dir(&at).unwrap() {
                let path = entry.unwrap().path();
                if path.is_dir() {
                    pending.push(path);
                } else {
                    out.push((path.clone(), fs::read(&path).unwrap()));
                }
            }
        }
        out.sort();
        out
    }

    #[test]
    fn a_worker_that_cannot_start_leaves_the_directory_as_it_was() {
        let fail_next = || crate::engine::worker::tests::FAIL_SPAWN.with(|f| f.set(true));
        let options = EngineOptions::default();
        let checkpoint = scratch("spawn-failure-checkpoint");
        let engine = Engine::new(options).unwrap();
        engine.put(b"k", b"checkpointed").unwrap();
        engine
            .checkpoint(&checkpoint, CheckpointOptions::default())
            .unwrap();
        engine.close().unwrap();
        let before = files(&checkpoint);
        fail_next();
        let refused = Engine::recover(&checkpoint, options).map(drop);
        assert!(matches!(refused, Err(OpenError::Worker(_))), "{refused:?}");
        assert_eq!(files(&checkpoint), before);
        let engine = Engine::recover(&checkpoint, options).unwrap();
        let mut workspace = EngineWorkspace::new();
        assert_eq!(
            engine.get(b"k", &mut workspace).unwrap().unwrap().value,
            b"checkpointed"
        );
        engine.close().unwrap();
        fs::remove_dir_all(&checkpoint).unwrap();
    }

    #[test]
    fn manifest_and_current_round_trip_and_reject_damage() {
        let manifest = Manifest {
            generation: 7,
            last_seq: 1 << 40,
            records: 12,
            next_run_id: 3,
            previous: 4,
            depth: 2,
            metadata: b"offsets\0\xff".to_vec(),
            runs: vec![
                RunEntry {
                    id: 1,
                    level: 0,
                    flush: 0,
                    seq_override: Some(7),
                    len: 100,
                    run_len: 88,
                    sum: 0xdead_beef,
                    bounds: None,
                },
                RunEntry {
                    id: 2,
                    level: 2,
                    flush: 0,
                    seq_override: None,
                    len: 100,
                    run_len: 88,
                    sum: 1,
                    bounds: None,
                },
            ],
        };
        let bytes = manifest.encode();
        assert_eq!(Manifest::decode(&bytes).unwrap(), manifest);
        for at in [0, 9, 13, 20, bytes.len() - 1] {
            let mut damaged = bytes.clone();
            damaged[at] ^= 1;
            assert!(Manifest::decode(&damaged).is_err(), "byte {at}");
        }
        assert!(matches!(
            Manifest::decode(&bytes[..bytes.len() - 1]),
            Err(Damage::Length { .. })
        ));
        let current = encode_current(9);
        assert_eq!(decode_current(&current), Ok(9));
        let mut damaged = current;
        damaged[10] ^= 1;
        assert_eq!(decode_current(&damaged), Err(Damage::Checksum));
    }

    #[test]
    fn malformed_manifest_fields_are_rejected_without_panicking() {
        let manifest = Manifest {
            generation: 2,
            last_seq: 42,
            records: 2,
            next_run_id: 3,
            previous: 1,
            depth: 1,
            metadata: Vec::new(),
            runs: vec![
                RunEntry {
                    id: 1,
                    level: 0,
                    flush: 0,
                    seq_override: None,
                    len: 100,
                    run_len: 88,
                    sum: 0,
                    bounds: None,
                },
                RunEntry {
                    id: 2,
                    level: 0,
                    flush: 1,
                    seq_override: None,
                    len: 100,
                    run_len: 88,
                    sum: 0,
                    bounds: None,
                },
            ],
        };
        let bytes = manifest.encode();
        for end in 0..bytes.len() {
            assert!(Manifest::decode(&bytes[..end]).is_err());
        }
        let seal = |bytes: &mut [u8]| {
            let end = bytes.len() - 4;
            let sum = crc(&[&bytes[..end]]);
            bytes[end..].copy_from_slice(&sum.to_le_bytes());
        };
        for (at, replacement) in [
            (40, 0u64),                           // exhausted/invalid next counter
            (48, 2),                              // previous is not older
            (60, u64::MAX),                       // unbounded metadata length
            (MANIFEST_HEADER_LEN + 40, 3),        // file cannot hold its checksum
            (MANIFEST_HEADER_LEN + 48, u64::MAX), // run payload exceeds file
            (MANIFEST_HEADER_LEN + 32, u64::MAX), // flush counter cannot advance
        ] {
            let mut damaged = bytes.clone();
            damaged[at..at + 8].copy_from_slice(&replacement.to_le_bytes());
            seal(&mut damaged);
            assert!(Manifest::decode(&damaged).is_err(), "offset {at}");
        }
        let mut duplicate = bytes.clone();
        duplicate[MANIFEST_HEADER_LEN + MANIFEST_ENTRY_LEN
            ..MANIFEST_HEADER_LEN + MANIFEST_ENTRY_LEN + 16]
            .copy_from_slice(&1u128.to_le_bytes());
        seal(&mut duplicate);
        assert_eq!(Manifest::decode(&duplicate), Err(Damage::Order));
    }

    fn scratch(name: &str) -> PathBuf {
        let path =
            std::env::temp_dir().join(format!("tessera-crash-{name}-{}", std::process::id()));
        let _ = fs::remove_dir_all(&path);
        path
    }

    fn state(engine: &Engine) -> Vec<(Vec<u8>, Vec<u8>)> {
        let mut workspace = EngineWorkspace::new();
        let guard = engine.read();
        let mut scan = guard.scan(&mut workspace);
        let mut out = Vec::new();
        while let Some(record) = scan.next_record().unwrap() {
            out.push((record.key.to_vec(), record.value.to_vec()));
        }
        out
    }

    fn entries(dir: &Path) -> Vec<String> {
        let mut names: Vec<String> = fs::read_dir(dir)
            .unwrap()
            .map(|entry| entry.unwrap().file_name().into_string().unwrap())
            .filter(|name| name != LOCK)
            .collect();
        names.sort();
        names
    }

    /// A crash at each boundary leaves CURRENT naming a complete checkpoint:
    /// the previous one before the rename, the new one after. The next
    /// checkpoint removes what the crash left and commits normally.
    #[test]
    fn a_crash_at_each_boundary_leaves_one_recoverable_checkpoint() {
        for (crash, committed) in [
            (Crash::PartialRun, false),
            (Crash::AfterFirstRun, false),
            (Crash::BeforeManifestSync, false),
            (Crash::BeforeRename, false),
            (Crash::AfterRename, true),
            (Crash::BeforeReclaim, true),
        ] {
            let dir = scratch(&format!("{crash:?}"));
            let engine = Engine::new(EngineOptions {
                target_run_len: 1024,
                ..manual()
            })
            .unwrap();
            for i in 0..500u32 {
                engine.put(&i.to_be_bytes(), b"old").unwrap();
            }
            engine.flush().unwrap();
            engine
                .checkpoint(&dir, CheckpointOptions::default())
                .unwrap();
            let old = state(&engine);
            for i in 0..500u32 {
                engine.put(&i.to_be_bytes(), b"new").unwrap();
            }
            engine.delete(&7u32.to_be_bytes()).unwrap();
            let new = state(&engine);

            CRASH_AT.with(|at| at.set(Some(crash)));
            let error = engine.checkpoint(&dir, CheckpointOptions::default());
            CRASH_AT.with(|at| at.set(None));
            assert!(error.is_err(), "{crash:?}");
            let recovered = Engine::recover(&dir, manual()).unwrap();
            let expected = if committed { &new } else { &old };
            assert_eq!(&state(&recovered), expected, "{crash:?}");
            drop(recovered);

            let report = engine
                .checkpoint(&dir, CheckpointOptions::default())
                .unwrap();
            assert_eq!(report.generation, 3, "{crash:?}");
            assert_eq!(report.reclaimed, 1, "{crash:?}");
            assert_eq!(
                entries(&dir),
                [
                    CURRENT.to_owned(),
                    format!("{PREFIX}{:020}", if committed { 2 } else { 1 }),
                    format!("{PREFIX}{:020}", 3),
                    RUNS.to_owned()
                ],
                "{crash:?}"
            );
            let recovered = Engine::recover(&dir, manual()).unwrap();
            assert_eq!(state(&recovered), new, "{crash:?}");
            fs::remove_dir_all(&dir).unwrap();
        }
    }

    /// A second process's lock refuses both a checkpoint and a recovery.
    #[test]
    fn the_lock_keeps_a_second_user_out() {
        let dir = scratch("lock");
        let engine = Engine::new(manual()).unwrap();
        engine.put(b"a", b"1").unwrap();
        engine
            .checkpoint(&dir, CheckpointOptions::default())
            .unwrap();
        // A lock taken through another open file description conflicts like
        // another process's.
        let held = lock(&dir).unwrap().unwrap();
        assert!(matches!(
            engine.checkpoint(&dir, CheckpointOptions::default()),
            Err(CheckpointError::Locked(_))
        ));
        assert!(matches!(
            Engine::recover(&dir, manual()),
            Err(OpenError::Locked(_))
        ));
        drop(held);
        Engine::recover(&dir, manual()).unwrap();
        fs::remove_dir_all(&dir).unwrap();
    }
    fn manifest(dir: &Path) -> Manifest {
        committed_manifest(dir).unwrap().2
    }

    fn compact_all(engine: &Engine) {
        let options = super::super::CompactionPolicy {
            max_overlap: 1,
            ..super::super::CompactionPolicy::default()
        };
        let mut workspace = super::super::MergeWorkspace::new();
        while engine
            .core
            .compact(options, &mut workspace)
            .unwrap()
            .is_some()
        {}
    }

    #[test]
    fn incremental_checkpoints_write_only_new_runs_and_gc_keeps_two_commits() {
        let dir = scratch("incremental");
        let options = EngineOptions {
            compression: super::super::Compression::Uncompressed,
            target_run_len: 1 << 20,
            ..manual()
        };
        let engine = Engine::new(options).unwrap();
        for i in 0..1_000u32 {
            engine.put(&i.to_be_bytes(), b"original").unwrap();
        }
        engine.flush().unwrap();
        let expected: Vec<_> = engine
            .core
            .read_state()
            .runs
            .iter()
            .map(|r| {
                let mut bytes = Vec::new();
                run_parts(&r.run, |part| {
                    bytes.extend_from_slice(part);
                    Ok(())
                })
                .unwrap();
                (r.id, bytes)
            })
            .collect();
        let first = engine
            .checkpoint(&dir, CheckpointOptions::default())
            .unwrap();
        assert_eq!((first.runs_written, first.runs_reused), (1, 0));
        assert_eq!(first.bytes_reused, 0);
        for (id, bytes) in &expected {
            assert_eq!(
                &fs::read(dir.join(RUNS).join(run_name(*id))).unwrap(),
                bytes
            );
        }
        let original = manifest(&dir);
        for i in 0..10u32 {
            engine.put(&i.to_be_bytes(), b"changed").unwrap();
        }
        engine.flush().unwrap();
        let second = engine
            .checkpoint(&dir, CheckpointOptions::default())
            .unwrap();
        let second_manifest = manifest(&dir);
        assert_eq!(
            (
                second.runs_written,
                second.runs_reused,
                second.files_deleted
            ),
            (1, 1, 0)
        );
        assert_eq!(second.bytes_reused, original.runs[0].len);
        assert_eq!(
            second.bytes_written,
            second_manifest.runs[1].len + second_manifest.encode().len() as u64
        );
        assert!(second.bytes_written < first.bytes_written / 10);
        for (id, bytes) in &expected {
            assert_eq!(
                &fs::read(dir.join(RUNS).join(run_name(*id))).unwrap(),
                bytes
            );
        }

        compact_all(&engine);
        let third = engine
            .checkpoint(&dir, CheckpointOptions::default())
            .unwrap();
        let third_manifest = manifest(&dir);
        assert_eq!(
            (third.runs_written, third.runs_reused, third.files_deleted),
            (1, 0, 0)
        );
        assert_eq!(
            third.bytes_written,
            third_manifest.runs[0].len + third_manifest.encode().len() as u64
        );
        // The preceding committed generation still recovers after GC.
        fs::write(dir.join(CURRENT), encode_current(second.generation)).unwrap();
        let previous = Engine::recover(&dir, options).unwrap();
        assert_eq!(state(&previous), state(&engine));
        previous.close().unwrap();
        fs::write(dir.join(CURRENT), encode_current(third.generation)).unwrap();

        let fourth = engine
            .checkpoint(&dir, CheckpointOptions::default())
            .unwrap();
        assert_eq!(
            (
                fourth.runs_written,
                fourth.runs_reused,
                fourth.files_deleted
            ),
            (0, 1, 2)
        );
        assert_eq!(fourth.bytes_written, manifest(&dir).encode().len() as u64);
        assert_eq!(fourth.bytes_reused, third_manifest.runs[0].len);
        for entry in &second_manifest.runs {
            assert!(!dir.join(RUNS).join(run_name(entry.id)).exists());
        }
        // Recovery preserves IDs and the persisted filter exactly, allowing a
        // metadata-only checkpoint without rehashing any keys or rewriting files.
        let recovered = Engine::recover(&dir, options).unwrap();
        assert!(
            recovered
                .core
                .read_state()
                .runs
                .iter()
                .all(|r| r.run.filter_build_bytes() == 0)
        );
        let fifth = recovered
            .checkpoint(&dir, CheckpointOptions::default())
            .unwrap();
        assert_eq!(
            (fifth.runs_written, fifth.runs_reused, fifth.files_deleted),
            (0, 1, 0)
        );
        assert_eq!(fifth.bytes_reused, fourth.bytes_reused);
        assert_eq!(manifest(&dir).runs, third_manifest.runs);
        assert_eq!(state(&recovered), state(&engine));
        for (name, report) in [
            ("first", first),
            ("second", second),
            ("third", third),
            ("fourth", fourth),
            ("recovered", fifth),
        ] {
            eprintln!(
                "{name}: written={} reused={} runs_written={} runs_reused={} deleted={}",
                report.bytes_written,
                report.bytes_reused,
                report.runs_written,
                report.runs_reused,
                report.files_deleted
            );
        }
        recovered.close().unwrap();
        engine.close().unwrap();
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn metadata_commits_with_its_ceiling_and_empty_metadata_remains_supported() {
        let dir = scratch("metadata");
        let engine = Engine::new(manual()).unwrap();
        engine.put(b"offset", b"value").unwrap();
        let metadata = b"kafka/partition-7=12345\0\xff";
        let first = engine
            .checkpoint_with_metadata(&dir, CheckpointOptions::default(), metadata)
            .unwrap();
        let (recovered, found) = Engine::recover_with_metadata(&dir, manual()).unwrap();
        assert_eq!(found, metadata);
        assert_eq!(recovered.stats().last_seq, first.last_seq);
        assert_eq!(state(&recovered), state(&engine));
        recovered.close().unwrap();
        let second = engine
            .checkpoint(&dir, CheckpointOptions::default())
            .unwrap();
        assert_eq!((second.runs_written, second.runs_reused), (0, 1));
        let (recovered, found) = Engine::recover_with_metadata(&dir, manual()).unwrap();
        assert!(found.is_empty());
        recovered.close().unwrap();
        assert!(matches!(
            engine.checkpoint_with_metadata(
                &dir,
                CheckpointOptions::default(),
                &vec![0; MAX_METADATA_BYTES + 1]
            ),
            Err(CheckpointError::MetadataLimit)
        ));
        assert_eq!(manifest(&dir).generation, second.generation);
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn a_paused_checkpoint_retains_its_ceiling_while_writes_ingestion_and_compaction_continue() {
        let dir = scratch("paused-image");
        let engine = Arc::new(Engine::new(manual()).unwrap());
        for i in 0..1_000u32 {
            engine.put(&i.to_be_bytes(), b"original").unwrap();
        }
        engine.flush().unwrap();
        for i in 0..10u32 {
            engine.delete(&i.to_be_bytes()).unwrap();
        }
        let pin = engine.snapshot();
        let expected = state(&engine);
        let ceiling = engine.stats().last_seq;
        let (ready_tx, ready_rx) = std::sync::mpsc::channel();
        let (resume_tx, resume_rx) = std::sync::mpsc::channel();
        let checkpoint_engine = Arc::clone(&engine);
        let checkpoint_dir = dir.clone();
        let checkpoint = std::thread::spawn(move || {
            AFTER_IMAGE.with(|hook| {
                *hook.borrow_mut() = Some(Box::new(move || {
                    ready_tx.send(()).unwrap();
                    resume_rx.recv().unwrap();
                }))
            });
            checkpoint_engine.checkpoint_with_metadata(
                &checkpoint_dir,
                CheckpointOptions::default(),
                b"offset=1010",
            )
        });
        ready_rx
            .recv_timeout(std::time::Duration::from_secs(10))
            .unwrap();
        engine.put(b"after-ceiling", b"excluded").unwrap();
        engine.flush().unwrap();
        let mut batch = super::super::WriteBatch::new();
        batch.put(b"ingested-later", b"excluded");
        engine.ingest_sorted(&batch).unwrap();
        let before = engine
            .core
            .read_state()
            .runs
            .iter()
            .map(|r| r.id)
            .collect::<HashSet<_>>();
        compact_all(&engine);
        let after = engine
            .core
            .read_state()
            .runs
            .iter()
            .map(|r| r.id)
            .collect::<HashSet<_>>();
        assert_ne!(
            before, after,
            "compaction published while the checkpoint was paused"
        );
        assert!(engine.stats().retained_run_bytes > 0);
        resume_tx.send(()).unwrap();
        let report = checkpoint.join().unwrap().unwrap();
        assert_eq!(report.last_seq, ceiling);
        let (recovered, metadata) = Engine::recover_with_metadata(&dir, manual()).unwrap();
        assert_eq!(state(&recovered), expected);
        assert_eq!(metadata, b"offset=1010");
        let mut ws = EngineWorkspace::new();
        let mut scan = pin.scan(&mut ws);
        let mut pinned = Vec::new();
        while let Some(record) = scan.next_record().unwrap() {
            pinned.push((record.key.to_vec(), record.value.to_vec()));
        }
        assert_eq!(pinned, expected);
        recovered.close().unwrap();
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn independently_recovered_branches_never_reuse_ids_for_different_outputs() {
        let dir = scratch("recovery-branches");
        let seed = Engine::new(manual()).unwrap();
        seed.put(b"shared", b"base").unwrap();
        seed.checkpoint(&dir, CheckpointOptions::default()).unwrap();
        let left = Engine::recover(&dir, manual()).unwrap();
        let right = Engine::recover(&dir, manual()).unwrap();
        left.put(b"branch", b"left").unwrap();
        right.put(b"branch", b"right").unwrap();
        left.checkpoint(&dir, CheckpointOptions::default()).unwrap();
        let left_manifest = manifest(&dir);
        right
            .checkpoint(&dir, CheckpointOptions::default())
            .unwrap();
        let right_manifest = manifest(&dir);
        assert_eq!(left_manifest.runs[0].id, right_manifest.runs[0].id);
        assert_ne!(left_manifest.runs[1].id, right_manifest.runs[1].id);
        let recovered = Engine::recover(&dir, manual()).unwrap();
        assert_eq!(state(&recovered), state(&right));
        fs::remove_dir_all(dir).unwrap();
    }
    #[test]
    fn writes_during_the_checkpoint_flush_are_excluded_even_if_they_publish_first() {
        let dir = scratch("flush-ceiling");
        let engine = Arc::new(
            Engine::new(EngineOptions {
                max_frozen_memtables: 1,
                ..manual()
            })
            .unwrap(),
        );
        engine.put(b"a", b"base").unwrap();
        // Leave a full frozen queue and a nonempty successor, as after an
        // abandoned flush. Checkpoint must drain the queue before rotation.
        drop(engine.core.begin_flush().unwrap());
        engine.put(b"b", b"checkpointed").unwrap();
        let expected = state(&engine);
        let ceiling = engine.stats().last_seq;
        let (ready_tx, ready_rx) = std::sync::mpsc::channel();
        let (resume_tx, resume_rx) = std::sync::mpsc::channel();
        let checkpoint_engine = Arc::clone(&engine);
        let checkpoint_dir = dir.clone();
        let checkpoint = std::thread::spawn(move || {
            AFTER_ROTATION.with(|hook| {
                *hook.borrow_mut() = Some(Box::new(move || {
                    ready_tx.send(()).unwrap();
                    resume_rx.recv().unwrap();
                }))
            });
            checkpoint_engine.checkpoint(&checkpoint_dir, CheckpointOptions::default())
        });
        ready_rx
            .recv_timeout(std::time::Duration::from_secs(10))
            .unwrap();
        assert_eq!(
            engine.stats().frozen_tables,
            1,
            "checkpoint respects the frozen-table limit"
        );
        // Both operations finish while checkpoint compression is paused. The
        // ingestion publishes first, yet its sequence is above the ceiling.
        assert!(engine.put(b"z", b"later").unwrap() > ceiling);
        let mut batch = super::super::WriteBatch::new();
        batch.put(b"zz", b"published-before-checkpoint-flush");
        assert!(engine.ingest_sorted(&batch).unwrap() > ceiling);
        resume_tx.send(()).unwrap();
        let report = checkpoint.join().unwrap().unwrap();
        assert_eq!(report.last_seq, ceiling);
        let recovered = Engine::recover(&dir, manual()).unwrap();
        assert_eq!(state(&recovered), expected);
        recovered.close().unwrap();
        fs::remove_dir_all(dir).unwrap();
    }
}
