//! Tessera is a RAM-first, compressed LSM key-value database.
//!
//! Only its storage primitives exist so far: the immutable, sorted data
//! [`block`], the immutable sorted [`run`] of blocks, and the bounded mutable
//! [`memtable`] that flushes into runs.

#![forbid(unsafe_code)]
#![warn(missing_docs)]

pub mod block;
pub mod memtable;
pub mod run;
