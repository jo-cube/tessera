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
