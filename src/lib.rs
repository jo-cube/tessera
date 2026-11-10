//! Tessera is a RAM-first, compressed LSM key-value database.
//!
//! Only its physical storage primitives exist so far: the immutable, sorted
//! data [`block`] and the immutable sorted [`run`] of blocks.

#![forbid(unsafe_code)]
#![warn(missing_docs)]

pub mod block;
pub mod run;
