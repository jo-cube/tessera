//! Engine-owned bulk allocations. Only the bounded free reserve is pooled;
//! checked-out chunks belong to their run or memtable, including snapshots.
//! Shared ownership returns an allocation only after its last user releases it,
//! bounding idle capacity without constraining live data.
use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, Weak};

pub(crate) const CHUNK_BYTES: usize = 1 << 20;
pub(crate) const RESERVE_BYTES: usize = 32 << 20;

#[derive(Default, Debug)]
struct State {
    free: Vec<Box<[u8]>>,
    in_use: usize,
    used: usize,
    limit: usize,
}

#[derive(Debug)]
struct Pool {
    state: Mutex<State>,
    counters: Counters,
}

/// Cumulative work performed by this engine, including abandoned builds.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct BuildCounters {
    /// Always zero; compaction re-encodes every block.
    pub blocks_reused: u64,
    /// Always zero; compaction re-encodes every block.
    pub bytes_reused: u64,
    /// Newly encoded output blocks (including uncompressed output).
    pub blocks_compressed: u64,
    /// Newly encoded frame bytes, including headers.
    pub bytes_compressed: u64,
    /// Encoded raw block bytes passed to Zstandard, excluding ratio probes.
    pub raw_bytes_compressed: u64,
    /// Admitted key and value bytes; deletes count their keys.
    pub raw_bytes_ingested: u64,
    /// Always zero; compaction re-encodes every block.
    pub forced_undersized_blocks: u64,
    /// Always zero; compaction re-encodes every block.
    pub filter_decoded_bytes: u64,
}
#[derive(Default, Debug)]
struct Counters {
    blocks_compressed: AtomicU64,
    bytes_compressed: AtomicU64,
    raw_bytes_compressed: AtomicU64,
    raw_bytes_ingested: AtomicU64,
}
impl Counters {
    fn snapshot(&self) -> BuildCounters {
        BuildCounters {
            blocks_reused: 0,
            bytes_reused: 0,
            blocks_compressed: self.blocks_compressed.load(Ordering::Relaxed),
            bytes_compressed: self.bytes_compressed.load(Ordering::Relaxed),
            raw_bytes_compressed: self.raw_bytes_compressed.load(Ordering::Relaxed),
            raw_bytes_ingested: self.raw_bytes_ingested.load(Ordering::Relaxed),
            forced_undersized_blocks: 0,
            filter_decoded_bytes: 0,
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) struct ChunkPool(Arc<Pool>);

impl ChunkPool {
    pub(crate) fn new(reserve_bytes: usize) -> Self {
        Self(Arc::new(Pool {
            state: Mutex::new(State {
                limit: reserve_bytes / CHUNK_BYTES,
                ..State::default()
            }),
            counters: Counters::default(),
        }))
    }

    pub(crate) fn counters(&self) -> BuildCounters {
        self.0.counters.snapshot()
    }
    pub(crate) fn ingested(&self, bytes: usize) {
        self.0
            .counters
            .raw_bytes_ingested
            .fetch_add(bytes as u64, Ordering::Relaxed);
    }
    pub(crate) fn compressed(&self, raw: usize, stored: usize, compressed: bool) {
        self.0
            .counters
            .blocks_compressed
            .fetch_add(1, Ordering::Relaxed);
        self.0
            .counters
            .bytes_compressed
            .fetch_add(stored as u64, Ordering::Relaxed);
        if compressed {
            self.0
                .counters
                .raw_bytes_compressed
                .fetch_add(raw as u64, Ordering::Relaxed);
        }
    }
    pub(crate) fn take(&self) -> Chunk {
        let bytes = {
            let mut state = self
                .0
                .state
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            state.in_use += 1;
            state.free.pop()
        }
        .unwrap_or_else(|| vec![0; CHUNK_BYTES].into_boxed_slice());
        Chunk {
            bytes,
            used: 0,
            pool: Arc::downgrade(&self.0),
        }
    }

    /// (free reserve bytes, checked-out bytes, reserve cap bytes).
    #[cfg(test)]
    pub(crate) fn stats(&self) -> (usize, usize, usize) {
        let (reserve, in_use, cap, _) = self.accounting();
        (reserve, in_use, cap)
    }
    /// A coherent snapshot, including capacity beyond logically charged bytes.
    pub(crate) fn accounting(&self) -> (usize, usize, usize, usize) {
        let state = self
            .0
            .state
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        (
            state.free.len() * CHUNK_BYTES,
            state.in_use * CHUNK_BYTES,
            state.limit * CHUNK_BYTES,
            (state.in_use * CHUNK_BYTES).saturating_sub(state.used),
        )
    }
    pub(crate) fn trim(&self) {
        self.0
            .state
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .free
            .clear();
    }
}

pub(crate) struct Chunk {
    bytes: Box<[u8]>,
    pool: Weak<Pool>,
    used: usize,
}

impl Chunk {
    pub(crate) fn exact(bytes: &[u8]) -> Self {
        Self {
            bytes: bytes.into(),
            pool: Weak::new(),
            used: 0,
        }
    }

    pub(crate) fn exact_parts(first: &[u8], second: &[u8]) -> Self {
        let mut bytes = vec![0; first.len() + second.len()].into_boxed_slice();
        bytes[..first.len()].copy_from_slice(first);
        bytes[first.len()..].copy_from_slice(second);
        Self {
            bytes,
            pool: Weak::new(),
            used: 0,
        }
    }

    pub(crate) fn set_used(&mut self, used: usize) {
        if let Some(pool) = self.pool.upgrade() {
            let mut state = pool.state.lock().unwrap_or_else(|error| error.into_inner());
            state.used = state.used - self.used + used;
        }
        self.used = used;
    }

    pub(crate) fn bytes(&self) -> &[u8] {
        &self.bytes
    }
    pub(crate) fn bytes_mut(&mut self) -> &mut [u8] {
        &mut self.bytes
    }
}

impl Clone for Chunk {
    fn clone(&self) -> Self {
        if let Some(pool) = self.pool.upgrade() {
            let mut copy = ChunkPool(pool).take();
            copy.bytes.copy_from_slice(&self.bytes);
            copy.set_used(self.used);
            copy
        } else {
            Self::exact(&self.bytes)
        }
    }
}

impl Drop for Chunk {
    fn drop(&mut self) {
        if let Some(pool) = self.pool.upgrade() {
            let mut state = pool.state.lock().unwrap_or_else(|error| error.into_inner());
            state.in_use -= 1;
            state.used -= self.used;
            if state.free.len() < state.limit {
                state.free.push(std::mem::take(&mut self.bytes));
            }
        }
    }
}

impl fmt::Debug for Chunk {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Chunk")
            .field("bytes", &self.bytes.len())
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reuse_returns_the_same_allocation_and_only_keeps_the_reserve() {
        let pool = ChunkPool::new(CHUNK_BYTES);
        let mut first = pool.take();
        first.bytes_mut()[7] = 91;
        let address = first.bytes().as_ptr();
        let second = pool.take();
        assert_eq!(pool.stats(), (0, 2 * CHUNK_BYTES, CHUNK_BYTES));
        drop(first);
        drop(second);
        assert_eq!(pool.stats(), (CHUNK_BYTES, 0, CHUNK_BYTES));
        let reused = pool.take();
        assert_eq!(reused.bytes().as_ptr(), address);
        assert_eq!(reused.bytes()[7], 91);
        assert_eq!(pool.stats(), (0, CHUNK_BYTES, CHUNK_BYTES));
    }

    #[test]
    fn exact_allocations_do_not_raise_or_fill_the_reserve() {
        let pool = ChunkPool::new(CHUNK_BYTES);
        let chunks = [pool.take(), pool.take(), pool.take()];
        let oversized = Chunk::exact(&vec![4; CHUNK_BYTES + 1]);
        drop(chunks);
        assert_eq!(pool.stats(), (CHUNK_BYTES, 0, CHUNK_BYTES));
        drop(oversized);
        assert_eq!(pool.stats(), (CHUNK_BYTES, 0, CHUNK_BYTES));
    }

    #[test]
    fn the_last_owner_can_outlive_the_engine_pool() {
        let pool = ChunkPool::new(CHUNK_BYTES);
        let mut chunk = pool.take();
        chunk.bytes_mut()[0] = 42;
        drop(pool);
        assert_eq!(chunk.bytes()[0], 42);
        drop(chunk);
    }
}
