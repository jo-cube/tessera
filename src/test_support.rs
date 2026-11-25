//! Deterministic helpers shared by bounded robustness tests.

#[derive(Clone, Copy)]
pub(crate) struct Rng(pub(crate) u64);

impl Rng {
    pub(crate) fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^ (z >> 31)
    }

    pub(crate) fn below(&mut self, bound: usize) -> usize {
        (self.next() % bound as u64) as usize
    }
}

/// The seed a long campaign uses in place of its fixed `seed`: `seed` itself
/// unless `TESSERA_TEST_SEED` holds a `u64`, and otherwise one derived from
/// both, so that each of a campaign's streams still differs.
///
/// # Panics
/// If `TESSERA_TEST_SEED` is set but is not a decimal `u64`.
pub(crate) fn campaign_seed(seed: u64) -> u64 {
    let Some(value) = std::env::var_os("TESSERA_TEST_SEED") else {
        return seed;
    };
    let base: u64 = value
        .to_str()
        .and_then(|value| value.parse().ok())
        .unwrap_or_else(|| panic!("TESSERA_TEST_SEED is not a u64: {value:?}"));
    Rng(base ^ seed).next()
}

/// Every mutation has bounded output, including when replacing length fields.
pub(crate) fn mutate(
    original: &[u8],
    fields: &[(usize, usize)],
    iteration: usize,
    rng: &mut Rng,
) -> Vec<u8> {
    let mut bytes = original.to_vec();
    match iteration % 5 {
        0 => {
            let at = rng.below(bytes.len());
            bytes[at] ^= 1 << rng.below(8);
        }
        1 => bytes.truncate(rng.below(bytes.len())),
        2 => {
            let extra = 1 + rng.below(32);
            bytes.extend((0..extra).map(|_| rng.next() as u8));
        }
        3 => {
            let &(at, width) = &fields[rng.below(fields.len())];
            let value = [0, 1, u32::MAX as u64, u64::MAX, rng.next()][rng.below(5)];
            bytes[at..at + width].copy_from_slice(&value.to_le_bytes()[..width]);
        }
        4 => {
            let width = 1 + rng.below(bytes.len().min(32));
            let left = rng.below(bytes.len() - width + 1);
            let right = rng.below(bytes.len() - width + 1);
            for i in 0..width {
                bytes.swap(left + i, right + i);
            }
        }
        _ => unreachable!(),
    }
    bytes
}

pub(crate) struct Scratch(pub(crate) std::path::PathBuf);

/// Publication makes records visible before the worker drops its temporary
/// source references. Retention assertions wait for that asynchronous cleanup.
pub(crate) fn wait_until(mut ready: impl FnMut() -> bool) {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    while !ready() {
        assert!(
            std::time::Instant::now() < deadline,
            "publication cleanup timed out"
        );
        std::thread::yield_now();
    }
}

impl Scratch {
    pub(crate) fn new(name: &str) -> Self {
        use std::sync::atomic::{AtomicU64, Ordering};
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let path = std::env::temp_dir().join(format!(
            "tessera-hardening-{name}-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir(&path).unwrap();
        Self(path)
    }
}

impl Drop for Scratch {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}
