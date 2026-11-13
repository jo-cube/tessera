//! Tessera is a RAM-first, compressed LSM key-value database.
//!
//! Everything public lives in [`engine`]: the [`Engine`](engine::Engine),
//! its options, statistics and errors, and the reads, snapshots and
//! maintenance it offers. An engine is RAM-only by default, with opt-in
//! checkpoints and an opt-in write-ahead log:
//!
//! ```
//! use tessera::engine::{
//!     CheckpointOptions, DurableOptions, Engine, EngineOptions, EngineWorkspace,
//! };
//!
//! let dir = std::env::temp_dir().join(format!("tessera-doc-{}", std::process::id()));
//! let engine = Engine::create(&dir, EngineOptions::default(), DurableOptions::default())?;
//! engine.put(b"user/1", b"ada")?;
//! engine.put(b"user/2", b"grace")?;
//! engine.delete(b"user/1")?;
//!
//! // One workspace per reading thread, reused across reads.
//! let mut workspace = EngineWorkspace::new();
//! assert!(engine.get(b"user/1", &mut workspace)?.is_none());
//!
//! // A snapshot holds no lock; writes, flushes and compactions continue.
//! let snapshot = engine.snapshot()?;
//! let mut scan = snapshot.scan_prefix(b"user/", &mut workspace);
//! assert_eq!(scan.next_record()?.unwrap().value, b"grace");
//! assert_eq!(scan.next_record()?, None);
//! drop(scan);
//!
//! // Bound the log: checkpoint, then drop the segments it covers.
//! engine.durable_checkpoint(CheckpointOptions::default())?;
//! // Stop the worker and release the directory; snapshots stay readable.
//! engine.close()?;
//! drop(snapshot);
//!
//! let (engine, _report) = Engine::open(&dir, EngineOptions::default(), DurableOptions::default())?;
//! assert_eq!(engine.get(b"user/2", &mut workspace)?.unwrap().value, b"grace");
//! # engine.close()?;
//! # std::fs::remove_dir_all(&dir)?;
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! ```
//!
//! Writes flush the memtable themselves when it fills, so callers never
//! flush or retry for room. See the [`engine`] module for the consistency,
//! snapshot, maintenance, memory and durability contracts.
//!
//! Internally, writes go to a bounded mutable memtable, which the engine
//! flushes into immutable sorted runs of compressed blocks when it fills;
//! compaction merges runs. Those components are private: their contracts are
//! the engine's to keep.

#![forbid(unsafe_code)]
#![warn(missing_docs)]
#![warn(unnameable_types)]

mod block;
pub mod engine;
mod memtable;
mod run;

#[cfg(test)]
mod limits_tests;

/// The README's Rust examples, compiled and run as documentation tests so
/// that they keep matching the API.
#[cfg(doctest)]
#[doc = include_str!("../README.md")]
pub struct ReadmeDoctests;
