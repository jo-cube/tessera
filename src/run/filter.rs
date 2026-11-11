//! Run-local Bloom filters over user keys.
//!
//! A filter is built beside each run its builder finishes and lives only in
//! memory: it is not part of the run format, and a run made from bytes with
//! [`Run::new`](super::Run::new) has none. It answers whether a user key may
//! be in the run; "no" is exact, "maybe" is wrong for about 1% of absent
//! keys. Every version and tombstone of a key counts as the key.
//!
//! Keys are hashed with SipHash-1-3 under fixed keys (the standard library's
//! [`DefaultHasher`]), and the probes are `h + i * step` for one 64-bit hash
//! `h` and an odd `step` derived from it (Kirsch and Mitzenmacher), each
//! reduced to a bit by multiplying and shifting. The hash may differ between
//! builds of the standard library, which is why filters are never stored.

use std::fmt;
use std::hash::{DefaultHasher, Hasher};

/// Ten bits per distinct key with seven probes: about a 0.8% false positive
/// rate, near the optimum for that size.
const BITS_PER_KEY: usize = 10;
const PROBES: u64 = 7;

/// Distinct keys beyond which a run gets no filter, so that the hashes held
/// while building (8 bytes each) and the filter stay bounded. A flush or
/// compaction run of the default length holds far fewer.
pub(crate) const MAX_FILTER_KEYS: usize = 1 << 24;

/// Hashes first reserved, after which the buffer doubles up to its limit.
const FIRST_RESERVE: usize = 1024;

pub(crate) fn hash(key: &[u8]) -> u64 {
    let mut hasher = DefaultHasher::new();
    hasher.write(key);
    hasher.finish()
}

/// Which bits a hash sets, in a filter of `bits` bits.
fn probes(hash: u64, bits: u64) -> impl Iterator<Item = usize> {
    let step = hash.rotate_left(32) | 1;
    (0..PROBES).map(move |i| {
        let mixed = hash.wrapping_add(i.wrapping_mul(step));
        // Below `bits`, which came from a slice length.
        ((u128::from(mixed) * u128::from(bits)) >> 64) as usize
    })
}

/// A run's Bloom filter.
#[derive(Clone)]
pub(crate) struct KeyFilter {
    words: Box<[u64]>,
    /// The bytes the builder held for hashes while building it.
    build_bytes: usize,
}

impl KeyFilter {
    /// Whether a key with this hash may be in the run. Never false for a key
    /// that is.
    pub(crate) fn may_contain(&self, hash: u64) -> bool {
        let bits = self.words.len() as u64 * 64;
        probes(hash, bits).all(|bit| self.words[bit / 64] & (1 << (bit % 64)) != 0)
    }

    pub(crate) fn bytes(&self) -> usize {
        self.words.len() * size_of::<u64>()
    }

    pub(crate) fn build_bytes(&self) -> usize {
        self.build_bytes
    }
}

impl fmt::Debug for KeyFilter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("KeyFilter")
            .field("bytes", &self.bytes())
            .finish_non_exhaustive()
    }
}

/// The hashes of a run's distinct user keys, collected while it is built.
///
/// The buffer grows by explicit doubling and never past `limit` hashes, so
/// its capacity stays below twice the keys seen and at most `limit`. A run
/// with more distinct keys than `limit` gets no filter.
#[derive(Debug)]
pub(crate) struct KeyHashes {
    hashes: Vec<u64>,
    limit: usize,
    /// Set once more than `limit` distinct keys were seen.
    over: bool,
}

impl KeyHashes {
    pub(crate) fn new() -> Self {
        Self::with_limit(MAX_FILTER_KEYS)
    }

    pub(crate) fn with_limit(limit: usize) -> Self {
        Self {
            hashes: Vec::new(),
            limit: limit.min(MAX_FILTER_KEYS),
            over: false,
        }
    }

    /// Lowers the limit; a run already past it gets no filter.
    pub(crate) fn limit_to(&mut self, limit: usize) {
        self.limit = self.limit.min(limit);
        if self.hashes.len() > self.limit {
            self.give_up();
        }
    }

    /// The bytes held for hashes now.
    pub(crate) fn held_bytes(&self) -> usize {
        self.hashes.capacity() * size_of::<u64>()
    }

    fn give_up(&mut self) {
        self.over = true;
        self.hashes = Vec::new();
    }

    /// Notes a record's key. Keys arrive in order, so versions of one key are
    /// adjacent and hash alike. Two adjacent distinct keys with one hash are
    /// also kept once, which sets the same bits.
    pub(crate) fn add(&mut self, key: &[u8]) {
        if self.over {
            return;
        }
        let hash = hash(key);
        if self.hashes.last() == Some(&hash) {
            return;
        }
        let len = self.hashes.len();
        if len == self.limit {
            self.give_up();
            return;
        }
        if len == self.hashes.capacity() {
            // Checked before growing; the allocator may round up.
            let grown = (len * 2).max(FIRST_RESERVE).min(self.limit);
            self.hashes.reserve_exact(grown - len);
        }
        self.hashes.push(hash);
    }

    /// The filter, or `None` for no keys or too many.
    pub(crate) fn finish(self) -> Option<KeyFilter> {
        if self.over || self.hashes.is_empty() {
            return None;
        }
        // The limit keeps this far from overflow; checked regardless.
        let bits = self.hashes.len().checked_mul(BITS_PER_KEY)?;
        let mut words = vec![0u64; bits.div_ceil(64)].into_boxed_slice();
        let total = words.len() as u64 * 64;
        for &hash in &self.hashes {
            for bit in probes(hash, total) {
                words[bit / 64] |= 1 << (bit % 64);
            }
        }
        Some(KeyFilter {
            words,
            build_bytes: self.held_bytes(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn filter(keys: &[&[u8]]) -> KeyFilter {
        let mut hashes = KeyHashes::new();
        for key in keys {
            hashes.add(key);
        }
        hashes.finish().unwrap()
    }

    #[test]
    fn every_key_added_is_reported_present() {
        let large = vec![0xFF; 4096];
        let mut keys: Vec<Vec<u8>> = vec![Vec::new(), vec![0], vec![0xFF], large];
        keys.extend((0..20_000u32).map(|i| i.to_be_bytes().to_vec()));
        keys.sort();
        let refs: Vec<&[u8]> = keys.iter().map(Vec::as_slice).collect();
        let filter = filter(&refs);
        assert!(keys.iter().all(|key| filter.may_contain(hash(key))));
        // About 1% of other keys pass; the hash is fixed, so this is exact.
        let passed = (20_000..40_000u32)
            .filter(|i| filter.may_contain(hash(&i.to_be_bytes())))
            .count();
        assert!(passed < 400, "{passed} of 20000 absent keys passed");
    }

    #[test]
    fn versions_of_a_key_count_once() {
        let mut hashes = KeyHashes::new();
        for key in [&b"a"[..], b"a", b"a", b"b", b"b"] {
            hashes.add(key);
        }
        assert_eq!(hashes.hashes.len(), 2);
        // Ten bits per key, in whole words.
        assert_eq!(hashes.finish().unwrap().bytes(), 8);
    }

    #[test]
    fn no_filter_without_keys_or_beyond_the_limit() {
        assert!(KeyHashes::new().finish().is_none());
        let mut hashes = KeyHashes::with_limit(2);
        for key in [&b"a"[..], b"b", b"c"] {
            hashes.add(key);
        }
        assert!(hashes.hashes.is_empty());
        assert!(hashes.finish().is_none());
    }

    /// The buffer never holds more than its limit, nor much more than twice
    /// the keys seen.
    #[test]
    fn the_hash_buffer_grows_within_its_limit() {
        let mut hashes = KeyHashes::with_limit(5_000);
        for i in 0..3_000u32 {
            hashes.add(&i.to_be_bytes());
        }
        assert!(hashes.held_bytes() <= 5_000 * 8, "{}", hashes.held_bytes());
        assert!(hashes.held_bytes() < 2 * 3_000 * 8 + 64);
        hashes.limit_to(2_000);
        assert_eq!((hashes.over, hashes.held_bytes()), (true, 0));
    }
}
