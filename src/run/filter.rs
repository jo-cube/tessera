//! Run-local Bloom filters over user keys.
//!
//! Filters accompany checkpoint run bytes with their own checksum. Hashing
//! is fixed: FNV-1a followed by the MurmurHash3 finalizer. The checkpoint
//! format fixes the blocked layout and probe sequence below. Every version
//! and tombstone contributes its key; a negative is exact.

use std::fmt;

/// Ten bits per distinct key in 512-bit blocks. With Poisson block occupancy,
/// six probes give about 0.96% false positives, almost identical to seven.
const BITS_PER_KEY: usize = 10;
const PROBES: usize = 6;
const BLOCK_BITS: usize = 512;

/// Distinct keys beyond which a run gets no filter, so that the hashes held
/// while building (8 bytes each) and the filter stay bounded. A flush or
/// compaction run of the default length holds far fewer.
pub(crate) const MAX_FILTER_KEYS: usize = 1 << 24;

pub(crate) const MAX_FILTER_BYTES: usize =
    (MAX_FILTER_KEYS * BITS_PER_KEY).div_ceil(BLOCK_BITS) * 64;

/// Hashes first reserved, after which the buffer doubles up to its limit.
const FIRST_RESERVE: usize = 1024;

pub(crate) fn hash(key: &[u8]) -> u64 {
    let mut h = 0xcbf2_9ce4_8422_2325u64;
    for &byte in key {
        h = (h ^ u64::from(byte)).wrapping_mul(0x100_0000_01b3);
    }
    h ^= h >> 33;
    h = h.wrapping_mul(0xff51_afd7_ed55_8ccd);
    h ^= h >> 33;
    h = h.wrapping_mul(0xc4ce_b9fe_1a85_ec53);
    h ^ (h >> 33)
}

/// Block selection and local probes use separate halves of the mixed hash.
fn block(hash: u64, count: usize) -> usize {
    ((u64::from(hash as u32) * count as u64) >> 32) as usize
}

fn probes(hash: u64) -> impl Iterator<Item = usize> {
    let mut local = (hash >> 32) as u32;
    (0..PROBES).map(move |_| {
        let bit = (local >> 23) as usize;
        local = local.wrapping_mul(0x9e37_79b9);
        bit
    })
}

/// Safe allocation alignment ensures that a block never straddles cache lines.
#[repr(align(64))]
#[derive(Clone, Debug, PartialEq, Eq)]
struct FilterBlock([u64; 8]);

/// A run's Bloom filter.
#[derive(Clone)]
pub(crate) struct KeyFilter {
    blocks: Box<[FilterBlock]>,
    /// The bytes the builder held for hashes while building it.
    build_bytes: usize,
}

impl KeyFilter {
    /// Whether a key with this hash may be in the run. Never false for a key
    /// that is.
    pub(crate) fn may_contain(&self, hash: u64) -> bool {
        let words = &self.blocks[block(hash, self.blocks.len())].0;
        probes(hash).all(|bit| words[bit / 64] & (1 << (bit % 64)) != 0)
    }

    pub(crate) fn bytes(&self) -> usize {
        size_of_val(&*self.blocks)
    }

    pub(crate) fn encoded(&self) -> impl Iterator<Item = [u8; 8]> + '_ {
        self.blocks
            .iter()
            .flat_map(|block| block.0.iter().map(|word| word.to_le_bytes()))
    }

    pub(crate) fn decode(bytes: &[u8]) -> Option<Self> {
        if bytes.is_empty() || !bytes.len().is_multiple_of(64) || bytes.len() > MAX_FILTER_BYTES {
            return None;
        }
        Some(Self {
            blocks: bytes
                .as_chunks::<64>()
                .0
                .iter()
                .map(|block| {
                    FilterBlock(std::array::from_fn(|i| {
                        u64::from_le_bytes(block[i * 8..i * 8 + 8].try_into().unwrap())
                    }))
                })
                .collect(),
            build_bytes: 0,
        })
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

    /// Whether no filter can be built, so keys need not be noted.
    pub(crate) fn disabled(&self) -> bool {
        self.limit == 0
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
        let mut blocks = vec![FilterBlock([0; 8]); bits.div_ceil(BLOCK_BITS)].into_boxed_slice();
        for &hash in &self.hashes {
            let words = &mut blocks[block(hash, blocks.len())].0;
            for bit in probes(hash) {
                words[bit / 64] |= 1 << (bit % 64);
            }
        }
        Some(KeyFilter {
            blocks,
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
    fn persisted_filters_keep_their_hash_and_bits() {
        // Golden hashes are independent of std's
        // DefaultHasher implementation and the machine's byte order.
        assert_eq!(hash(b""), 0xefd0_1f60_ba99_2926);
        assert_eq!(hash(b"a"), 0x82a2_a958_a9be_ce5b);
        assert_eq!(hash(b"abc"), 0x33eb_af99_27cb_c5bd);
        let original = filter(&[b"a", b"b", b"c"]);
        let bytes: Vec<_> = original.encoded().flatten().collect();
        let restored = KeyFilter::decode(&bytes).unwrap();
        assert_eq!(restored.blocks, original.blocks);
        assert_eq!(restored.blocks.as_ptr().addr() % 64, 0);
        assert_eq!(restored.build_bytes(), 0);
        assert!(
            [b"a", b"b", b"c"]
                .iter()
                .all(|key| restored.may_contain(hash(*key)))
        );
        assert!(KeyFilter::decode(&[]).is_none());
        assert!(KeyFilter::decode(&[0; 7]).is_none());
        assert!(KeyFilter::decode(&[0; 63]).is_none());
        assert!(KeyFilter::decode(&[0; 65]).is_none());
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
        assert_eq!(filter.blocks.as_ptr().addr() % 64, 0);
        assert!(filter.bytes() >= keys.len() * BITS_PER_KEY / 8);
        assert!(filter.bytes() < keys.len() * BITS_PER_KEY / 8 + 64);
        // One million absent keys; fixed hashes make this reproducible.
        let passed = (20_000..1_020_000u32)
            .filter(|i| filter.may_contain(hash(&i.to_be_bytes())))
            .count();
        println!("blocked Bloom: {passed}/1000000 absent keys passed");
        assert!((8_000..13_000).contains(&passed), "{passed}/1000000");
    }

    #[test]
    fn versions_of_a_key_count_once() {
        let mut hashes = KeyHashes::new();
        for key in [&b"a"[..], b"a", b"a", b"b", b"b"] {
            hashes.add(key);
        }
        assert_eq!(hashes.hashes.len(), 2);
        // Ten bits per key, rounded to a whole cache line.
        assert_eq!(hashes.finish().unwrap().bytes(), 64);
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
