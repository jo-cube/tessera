//! Tessera is a RAM-first, compressed LSM key–value engine in safe Rust,
//! with keyspaces, atomic batches, snapshots and explicit checkpoints.
//!
//! An [`Engine`] keeps its data in memory as compressed sorted runs and
//! writes to disk only when asked to [checkpoint](Engine::checkpoint). There
//! is no write-ahead log: writes since the latest checkpoint are lost when
//! the engine closes or its process ends.
//!
//! ```
//! use tessera::{Engine, Options, WriteBatch};
//!
//! # let dir = std::env::temp_dir().join(format!("tessera-doc-{}", std::process::id()));
//! # let _ = std::fs::remove_dir_all(&dir);
//! // Recovers the latest checkpoint in `dir`, or starts empty.
//! let engine = Engine::open(&dir, Options::default())?;
//! let users = engine.keyspace("users")?; // opened or created by name
//! users.put(b"user/1", b"ada")?;
//! assert_eq!(users.get(b"user/1")?.as_deref(), Some(&b"ada"[..])); // owned
//!
//! let mut batch = WriteBatch::new(); // applied atomically
//! batch.put(&users, b"user/2", b"grace");
//! batch.delete(&users, b"user/1");
//! engine.write(&batch)?;
//!
//! let snapshot = engine.snapshot(); // consistent, holds no lock
//! let mut reader = snapshot.reader(); // pooled buffers; one per thread
//! let mut scan = reader.prefix(&users, b"user/");
//! while let Some((key, value)) = scan.next()? {
//!     assert_eq!((key, value), (&b"user/2"[..], &b"grace"[..])); // borrowed
//! }
//! drop(scan);
//! let values = reader.multi_get(&users, &[&b"user/1"[..], b"user/2"])?;
//! assert_eq!(values[..], [None, Some(&b"grace"[..])]);
//! drop(values);
//! drop(reader);
//!
//! engine.checkpoint_with(b"offset=42")?; // into `dir`, atomically
//! engine.close()?;
//!
//! let engine = Engine::open(&dir, Options::default())?;
//! assert_eq!(engine.metadata().as_deref(), Some(&b"offset=42"[..]));
//! assert_eq!(engine.keyspaces(), ["users"]);
//! # engine.close()?;
//! # std::fs::remove_dir_all(&dir).unwrap();
//! # Ok::<(), tessera::Error>(())
//! ```
//!
//! # Engines and keyspaces
//!
//! [`Engine::open`] recovers a directory and holds its `LOCK` file until the
//! engine closes; [`Engine::in_memory`] needs no directory. `Engine`,
//! [`Keyspace`] and [`Snapshot`] are cheap `Clone + Send + Sync` handles.
//! The engine runs until [`Engine::close`] or until its last `Engine` and
//! `Keyspace` handle is dropped; either joins its background workers and
//! releases the lock, and writes nothing.
//!
//! A [`Keyspace`] is a named, independent key space: the same key in two
//! keyspaces is two records, and scans never cross from one to another. Keys
//! and values are byte strings of at most [`MAX_KEY_LEN`] and
//! [`MAX_VALUE_LEN`] bytes. An engine holds up to 4096 live keyspaces, and a
//! [`WriteBatch`] or [`Snapshot`] spans all of them.
//! [`Engine::drop_keyspace`] removes one and its data, and the name can be
//! created again as an empty keyspace with another ID.
//!
//! # Reads
//!
//! [`Keyspace::get`] returns an owned copy. A [`Reader`] reads without
//! copying: [`Reader::get`] and [`Reader::multi_get`] borrow the reader until
//! its next call, and [`Reader::prefix`] and [`Reader::range`] return a
//! lending [`Scan`] whose records borrow it until the next step. A reader of
//! [`Engine::reader`] sees, in each call, the latest state; a scan pins that
//! state until the reader's next call. Readers take their buffers from a
//! pool in the engine and return them when dropped.
//!
//! # Snapshots
//!
//! [`Engine::snapshot`] is a consistent view of every keyspace that holds no
//! lock: writes, flushes and compactions continue. It shares the
//! copy-on-write memtables and the immutable runs, and keeps whatever the
//! engine releases from its view, which counts towards writer admission as
//! [`Stats::snapshot_bytes`], so drop it once read. It stays readable after
//! the engine closes.
//!
//! # Memory
//!
//! [`Options::memory_limit`] is a writer admission budget, unlimited by
//! default. [`Stats::memory_used`] sums charged memtables, stored runs and
//! their filters and indexes, pool reserve, retained snapshot sources and
//! estimates of builds in progress. Estimates and allocator overhead make it
//! an admission budget, not a cap on the process's memory. Below 90% of the
//! limit writers proceed; above it they wait briefly for publication; at
//! the limit they flush and ask compaction to reclaim memory, and fail with
//! [`ErrorKind::MemoryLimit`] when nothing can be reclaimed. Reads,
//! snapshots, checkpoints and maintenance never wait on the budget.
//!
//! # Checkpoints
//!
//! [`Engine::checkpoint`] writes the state at one moment into the engine's
//! directory while writes continue. A checkpoint holds every key live at
//! that moment and nothing written later. It reuses the run files of the
//! previous checkpoint and writes only new runs, then commits by renaming
//! `CURRENT` once its files are synced; the previous generation stays
//! until the next commit. A failed or interrupted checkpoint leaves the
//! previous one intact. [`Engine::checkpoint_with`] stores application
//! metadata in the same commit, and [`Engine::metadata`] returns it.
//!
//! Recovery checks file lengths, checksums, every record, filter
//! membership and copies across runs before it returns an engine. CRC-32
//! detects damage but does not authenticate files. Durability assumes a
//! local POSIX filesystem with working file and directory sync.
//!
//! # Errors
//!
//! Every fallible call returns [`Error`]; match on [`Error::kind`].
//!
//! # Panics
//!
//! A panic while the engine's state lock is held poisons the engine, and
//! later operations that take the lock panic. A panic in a background
//! worker stops it; the engine keeps working, building synchronously where
//! needed, and [`Engine::close`] reports [`ErrorKind::Background`].

#![forbid(unsafe_code)]
#![warn(missing_docs)]
#![warn(unnameable_types)]

mod block;
mod chunk;
#[cfg(test)]
extern crate self as tessera;

mod engine;
mod memtable;
mod run;

pub use engine::{
    Compression, Engine, Error, ErrorKind, KeyRange, Keyspace, KeyspaceStats, MAX_KEY_LEN,
    MAX_KEYSPACE_NAME_LEN, MAX_METADATA_LEN, MAX_VALUE_LEN, MultiGet, Options, Reader, Result,
    Scan, Snapshot, Stats, WriteBatch,
};

#[cfg(feature = "diagnostics")]
#[doc(hidden)]
pub use engine::diagnostics;

#[cfg(test)]
mod limits_tests;

#[cfg(test)]
mod test_support;

/// The README's Rust examples, compiled and run as documentation tests so
/// that they keep matching the API.
#[cfg(doctest)]
#[doc = include_str!("../README.md")]
pub struct ReadmeDoctests;
