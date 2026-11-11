//! Tessera is a RAM-first, compressed LSM key-value database.
//!
//! The storage primitives are the immutable, sorted data [`block`], the
//! immutable sorted [`run`] of blocks, and the bounded mutable [`memtable`]
//! that flushes into runs. The [`engine`] combines them into a key-value
//! store that is RAM-only by default, with opt-in checkpoints and an opt-in
//! write-ahead log.

#![forbid(unsafe_code)]
#![warn(missing_docs)]

pub mod block;
pub mod engine;
pub mod memtable;
pub mod run;
