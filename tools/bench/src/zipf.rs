//! Scrambled Zipfian key generator (Gray et al., SIGMOD 1994; YCSB) for skewed
//! read and mixed benchmarks.
use crate::data::mix;

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
        (mix((rank as u64).wrapping_add(self.seed)) as usize) % self.n
    }
}

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
