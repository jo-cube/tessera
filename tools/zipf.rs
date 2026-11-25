//! Scrambled Zipfian key generator (Gray et al., SIGMOD 1994; YCSB) and
//! offline block-reuse / S3-FIFO cache-curve evaluators (Yang et al., SOSP 2023).
use std::collections::{HashMap, HashSet, VecDeque};

/// Deterministic 64-bit mixer for scrambling Zipfian ranks uniformly across
/// the key space so hot keys do not cluster artificially into adjacent blocks.
fn mix64(mut z: u64) -> u64 {
    z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    z ^ (z >> 31)
}

/// Scrambled Zipfian generator over `0..n` with skew parameter `0 < theta < 1`.
pub struct ScrambledZipfian {
    n: usize,
    theta: f64,
    alpha: f64,
    zetan: f64,
    eta: f64,
    seed: u64,
}

impl ScrambledZipfian {
    pub fn new(n: usize, theta: f64, seed: u64) -> Self {
        assert!(n >= 2, "Zipfian requires at least 2 items");
        assert!(theta > 0.0 && theta < 1.0, "theta must lie in (0, 1)");
        let zetan = zeta(n, theta);
        let zeta2 = 1.0 + 2.0_f64.powf(-theta);
        let alpha = 1.0 / (1.0 - theta);
        let eta = (1.0 - (2.0 / n as f64).powf(1.0 - theta)) / (1.0 - zeta2 / zetan);
        Self {
            n,
            theta,
            alpha,
            zetan,
            eta,
            seed,
        }
    }

    /// Draws the next scrambled index in `0..n`, given a uniform `u` in `[0, 1)`.
    pub fn sample(&self, u: f64) -> usize {
        let uz = u * self.zetan;
        let rank = if uz < 1.0 {
            0
        } else if uz < 1.0 + 0.5_f64.powf(self.theta) {
            1
        } else {
            let v = (self.n as f64) * (self.eta * u - self.eta + 1.0).powf(self.alpha);
            (v as usize).min(self.n - 1)
        };
        (mix64((rank as u64).wrapping_add(self.seed)) as usize) % self.n
    }
}

/// Generalized harmonic number H(n, theta) = sum_{i=1..=n} i^{-theta}.
/// Exact sum for the head and Euler-Maclaurin midpoint integral for the tail.
fn zeta(n: usize, theta: f64) -> f64 {
    let head = n.min(10_000);
    let mut sum = 0.0;
    for i in 1..=head {
        sum += (i as f64).powf(-theta);
    }
    if n > head {
        let a = head as f64 + 0.5;
        let b = n as f64 + 0.5;
        let p = 1.0 - theta;
        sum += (b.powf(p) - a.powf(p)) / p;
    }
    sum
}

/// Block capacities evaluated on the reuse curve.
pub const CURVE_SIZES: [usize; 5] = [16, 64, 256, 1024, 4096];

/// Fenwick tree for exact O(log Q) LRU stack-distance calculation.
struct Bit {
    tree: Vec<i64>,
}

impl Bit {
    fn new(max_steps: usize) -> Self {
        Self {
            tree: vec![0; max_steps + 2],
        }
    }

    fn add(&mut self, mut i: usize, delta: i64) {
        while i < self.tree.len() {
            self.tree[i] += delta;
            i += i & i.wrapping_neg();
        }
    }

    fn sum(&self, mut i: usize) -> i64 {
        let mut s = 0;
        while i > 0 {
            s += self.tree[i];
            i -= i & i.wrapping_neg();
        }
        s
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Queue {
    Small,
    Main,
}

/// S3-FIFO cache simulator (Yang et al., SOSP 2023) over block identities.
struct S3Fifo {
    small_cap: usize,
    main_cap: usize,
    ghost_cap: usize,
    small: VecDeque<(usize, usize)>,
    main: VecDeque<(usize, usize)>,
    ghost: VecDeque<(usize, usize)>,
    ghost_set: HashSet<(usize, usize)>,
    entries: HashMap<(usize, usize), (Queue, u8)>,
    hits: u64,
    accesses: u64,
}

impl S3Fifo {
    fn new(capacity: usize) -> Self {
        let small_cap = (capacity / 10).max(1);
        let main_cap = capacity.saturating_sub(small_cap).max(1);
        Self {
            small_cap,
            main_cap,
            ghost_cap: capacity,
            small: VecDeque::with_capacity(small_cap + 1),
            main: VecDeque::with_capacity(main_cap + 1),
            ghost: VecDeque::with_capacity(capacity + 1),
            ghost_set: HashSet::with_capacity(capacity + 1),
            entries: HashMap::with_capacity(capacity + 1),
            hits: 0,
            accesses: 0,
        }
    }

    fn access(&mut self, key: (usize, usize)) -> bool {
        self.accesses += 1;
        if let Some((_, freq)) = self.entries.get_mut(&key) {
            *freq = (*freq + 1).min(3);
            self.hits += 1;
            return true;
        }
        if self.ghost_set.remove(&key) {
            self.insert_main(key);
        } else {
            self.insert_small(key);
        }
        false
    }

    fn insert_small(&mut self, key: (usize, usize)) {
        while self.small.len() >= self.small_cap {
            if let Some(evict) = self.small.pop_front() {
                let (_, freq) = self.entries.remove(&evict).unwrap_or((Queue::Small, 0));
                if freq > 0 {
                    self.insert_main(evict);
                } else {
                    self.insert_ghost(evict);
                }
            }
        }
        self.small.push_back(key);
        self.entries.insert(key, (Queue::Small, 0));
    }

    fn insert_main(&mut self, key: (usize, usize)) {
        while self.main.len() >= self.main_cap {
            if let Some(evict) = self.main.pop_front() {
                if let Some((_, freq)) = self.entries.get_mut(&evict)
                    && *freq > 0
                {
                    *freq -= 1;
                    self.main.push_back(evict);
                    continue;
                }
                self.entries.remove(&evict);
            }
        }
        self.main.push_back(key);
        self.entries.insert(key, (Queue::Main, 0));
    }

    fn insert_ghost(&mut self, key: (usize, usize)) {
        if self.ghost_set.insert(key) {
            self.ghost.push_back(key);
            while self.ghost_set.len() > self.ghost_cap {
                if let Some(old) = self.ghost.pop_front() {
                    self.ghost_set.remove(&old);
                }
            }
        }
    }

    fn hit_rate(&self) -> f64 {
        if self.accesses == 0 {
            0.0
        } else {
            self.hits as f64 / self.accesses as f64
        }
    }
}

/// Summary point on the block-reuse curve for one block budget `n_blocks`.
pub struct ReusePoint {
    pub n_blocks: usize,
    pub get_hit_rate: f64,
    pub block_hit_rate: f64,
    pub s3fifo_hit_rate: f64,
    pub lru_bytes: u64,
    pub mean_n_bytes: u64,
}

/// Tracks block reuse distances and S3-FIFO hit rates across a stream of gets.
pub struct ReuseTracker {
    step: usize,
    bit: Bit,
    last_seen: HashMap<(usize, usize), usize>,
    block_sizes: HashMap<(usize, usize), usize>,
    total_gets: u64,
    total_blocks: u64,
    get_hits: [u64; CURVE_SIZES.len()],
    block_hits: [u64; CURVE_SIZES.len()],
    current_get_all_hit: [bool; CURVE_SIZES.len()],
    s3fifo: Vec<S3Fifo>,
}

impl ReuseTracker {
    pub fn new(max_block_decodes: usize) -> Self {
        Self {
            step: 0,
            bit: Bit::new(max_block_decodes),
            last_seen: HashMap::new(),
            block_sizes: HashMap::new(),
            total_gets: 0,
            total_blocks: 0,
            get_hits: [0; CURVE_SIZES.len()],
            block_hits: [0; CURVE_SIZES.len()],
            current_get_all_hit: [true; CURVE_SIZES.len()],
            s3fifo: CURVE_SIZES.iter().map(|&n| S3Fifo::new(n)).collect(),
        }
    }

    pub fn begin_get(&mut self) {
        self.current_get_all_hit = [true; CURVE_SIZES.len()];
    }

    pub fn record_block(&mut self, run_id: usize, block: usize, decoded_bytes: usize) {
        self.step += 1;
        if self.step + 1 >= self.bit.tree.len() {
            // Grow lazily if a workload decodes more blocks than estimated.
            let prev_seen = self.last_seen.clone();
            let mut next = Bit::new(self.bit.tree.len() * 2);
            for &pos in prev_seen.values() {
                next.add(pos, 1);
            }
            self.bit = next;
        }
        self.total_blocks += 1;
        let key = (run_id, block);
        self.block_sizes.insert(key, decoded_bytes);
        let dist = match self.last_seen.insert(key, self.step) {
            Some(prev) => {
                let d = (self.bit.sum(self.step - 1) - self.bit.sum(prev)) as usize;
                self.bit.add(prev, -1);
                d
            }
            None => usize::MAX,
        };
        self.bit.add(self.step, 1);
        for (idx, &n) in CURVE_SIZES.iter().enumerate() {
            if dist < n {
                self.block_hits[idx] += 1;
            } else {
                self.current_get_all_hit[idx] = false;
            }
            self.s3fifo[idx].access(key);
        }
    }

    pub fn end_get(&mut self) {
        self.total_gets += 1;
        for (idx, &hit) in self.current_get_all_hit.iter().enumerate() {
            if hit {
                self.get_hits[idx] += 1;
            }
        }
    }

    pub fn distinct_blocks(&self) -> usize {
        self.last_seen.len()
    }

    pub fn curve(&self) -> Vec<ReusePoint> {
        let mut recency: Vec<(usize, usize)> = self
            .last_seen
            .iter()
            .map(|(k, &step)| (step, self.block_sizes[k]))
            .collect();
        recency.sort_unstable_by_key(|a| std::cmp::Reverse(a.0));
        let mean_block = if self.block_sizes.is_empty() {
            0.0
        } else {
            self.block_sizes.values().sum::<usize>() as f64 / self.block_sizes.len() as f64
        };
        CURVE_SIZES
            .iter()
            .enumerate()
            .map(|(idx, &n)| {
                let lru_bytes: u64 = recency.iter().take(n).map(|&(_, sz)| sz as u64).sum();
                let mean_n_bytes = (mean_block * n.min(recency.len()) as f64).round() as u64;
                ReusePoint {
                    n_blocks: n,
                    get_hit_rate: if self.total_gets == 0 {
                        0.0
                    } else {
                        self.get_hits[idx] as f64 / self.total_gets as f64
                    },
                    block_hit_rate: if self.total_blocks == 0 {
                        0.0
                    } else {
                        self.block_hits[idx] as f64 / self.total_blocks as f64
                    },
                    s3fifo_hit_rate: self.s3fifo[idx].hit_rate(),
                    lru_bytes,
                    mean_n_bytes,
                }
            })
            .collect()
    }
}

/// Cache capacities of the byte-weighted curves, in MiB.
pub const CAPACITY_MIB: [f64; 5] = [0.25, 1.0, 4.0, 16.0, 64.0];
const CAPS: usize = CAPACITY_MIB.len();

fn capacity_bytes(at: usize) -> u64 {
    (CAPACITY_MIB[at] * f64::from(1u32 << 20)) as u64
}

/// LRU at every capacity of [`CAPACITY_MIB`] from one byte-weighted stack
/// distance: an access hits a cache of `C` bytes when the bytes of the
/// distinct entries used since its previous use, plus its own, fit in `C`.
/// This is exact when entries have equal sizes and none is invalidated; an
/// invalidation frees its bytes for entries a real LRU already evicted, so
/// read/update mixes are slightly overstated.
pub struct ByteLru {
    bit: Bit,
    step: usize,
    last: HashMap<u64, (usize, u32)>,
}

impl ByteLru {
    pub fn new(accesses: usize) -> Self {
        Self {
            bit: Bit::new(accesses),
            step: 0,
            last: HashMap::new(),
        }
    }

    /// Records an access and returns, per capacity, whether it hit.
    pub fn access(&mut self, key: u64, size: u32) -> [bool; CAPS] {
        self.step += 1;
        if self.step + 1 >= self.bit.tree.len() {
            let mut next = Bit::new(self.bit.tree.len() * 2);
            for &(at, bytes) in self.last.values() {
                next.add(at, i64::from(bytes));
            }
            self.bit = next;
        }
        let distance = self.last.insert(key, (self.step, size)).map(|(prev, old)| {
            let above = self.bit.sum(self.step - 1) - self.bit.sum(prev);
            self.bit.add(prev, -i64::from(old));
            above as u64 + u64::from(size)
        });
        self.bit.add(self.step, i64::from(size));
        std::array::from_fn(|at| distance.is_some_and(|d| d <= capacity_bytes(at)))
    }

    /// Removes `key`, as an update does to a cached record.
    pub fn invalidate(&mut self, key: u64) {
        if let Some((at, bytes)) = self.last.remove(&key) {
            self.bit.add(at, -i64::from(bytes));
        }
    }
}

struct Entry {
    generation: u32,
    size: u32,
    freq: u8,
    main: bool,
}

/// Byte-weighted S3-FIFO: a small FIFO of a tenth of the bytes, a main FIFO
/// that reinserts entries read again (frequency capped at 3), and a ghost
/// FIFO of evicted keys as large in bytes as the main queue.
pub struct ByteS3Fifo {
    capacity: [u64; 2],
    bytes: [u64; 3],
    queues: [VecDeque<(u64, u32, u32)>; 3],
    live: HashMap<u64, Entry>,
    ghosts: HashMap<u64, (u32, u32)>,
    generation: u32,
}

impl ByteS3Fifo {
    pub fn new(capacity: u64) -> Self {
        Self {
            capacity: [capacity / 10, capacity - capacity / 10],
            bytes: [0; 3],
            queues: Default::default(),
            live: HashMap::new(),
            ghosts: HashMap::new(),
            generation: 0,
        }
    }

    /// Records an access, inserting on a miss, and returns whether it hit.
    pub fn access(&mut self, key: u64, size: u32) -> bool {
        if let Some(entry) = self.live.get_mut(&key) {
            entry.freq = (entry.freq + 1).min(3);
            return true;
        }
        self.generation += 1;
        let main = self.ghosts.remove(&key).is_some_and(|(_, bytes)| {
            self.bytes[2] -= u64::from(bytes);
            true
        });
        self.insert(key, size, self.generation, main);
        false
    }

    pub fn invalidate(&mut self, key: u64) {
        if let Some(entry) = self.live.remove(&key) {
            self.bytes[usize::from(entry.main)] -= u64::from(entry.size);
        }
    }

    fn insert(&mut self, key: u64, size: u32, generation: u32, main: bool) {
        let q = usize::from(main);
        self.queues[q].push_back((key, size, generation));
        self.bytes[q] += u64::from(size);
        let entry = Entry {
            generation,
            size,
            freq: 0,
            main,
        };
        self.live.insert(key, entry);
        while self.bytes[q] > self.capacity[q] {
            let Some((key, size, generation)) = self.queues[q].pop_front() else {
                break;
            };
            let Some(entry) = self.live.get_mut(&key) else {
                continue;
            };
            if entry.generation != generation || entry.main != main {
                continue;
            }
            if main && entry.freq > 0 {
                entry.freq -= 1;
                self.queues[1].push_back((key, size, generation));
                continue;
            }
            let freq = entry.freq;
            self.live.remove(&key);
            self.bytes[q] -= u64::from(size);
            if main {
                continue;
            }
            if freq > 0 {
                self.insert(key, size, generation, true);
            } else {
                self.ghost(key, size, generation);
            }
        }
    }

    fn ghost(&mut self, key: u64, size: u32, generation: u32) {
        self.queues[2].push_back((key, size, generation));
        self.ghosts.insert(key, (generation, size));
        self.bytes[2] += u64::from(size);
        while self.bytes[2] > self.capacity[1] {
            let Some((key, size, generation)) = self.queues[2].pop_front() else {
                break;
            };
            if self.ghosts.get(&key).is_some_and(|g| g.0 == generation) {
                self.ghosts.remove(&key);
                self.bytes[2] -= u64::from(size);
            }
        }
    }
}

/// Byte-weighted LRU and S3-FIFO hit counts at every capacity of
/// [`CAPACITY_MIB`]. Hits count only while `counting` is set.
pub struct Curves {
    lru: ByteLru,
    s3: [ByteS3Fifo; CAPS],
    /// LRU, then S3-FIFO hits per capacity.
    pub hits: [[u64; CAPS]; 2],
    pub counted: usize,
}

impl Curves {
    pub fn new(accesses: usize) -> Self {
        Self {
            lru: ByteLru::new(accesses),
            s3: std::array::from_fn(|at| ByteS3Fifo::new(capacity_bytes(at))),
            hits: [[0; CAPS]; 2],
            counted: 0,
        }
    }

    /// Accesses `key`, returning per policy and capacity whether it hit.
    pub fn access(&mut self, key: u64, size: u32) -> [[bool; CAPS]; 2] {
        let lru = self.lru.access(key, size);
        [lru, std::array::from_fn(|at| self.s3[at].access(key, size))]
    }

    /// Counts one lookup made of accesses whose hits are `all`.
    pub fn count(&mut self, all: [[bool; CAPS]; 2], counting: bool) {
        if counting {
            self.counted += 1;
            for (hits, all) in self.hits.iter_mut().zip(all) {
                hits.iter_mut()
                    .zip(all)
                    .for_each(|(h, a)| *h += u64::from(a));
            }
        }
    }

    pub fn invalidate(&mut self, key: u64) {
        self.lru.invalidate(key);
        self.s3.iter_mut().for_each(|c| c.invalidate(key));
    }
}
