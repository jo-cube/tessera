//! Tessera is a RAM-first, compressed LSM key-value database.
//!
//! Only its first physical storage primitive exists so far: the immutable,
//! sorted data [`block`].

#![forbid(unsafe_code)]
#![warn(missing_docs)]

pub mod block;
