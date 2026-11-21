//! Engine-owned bulk allocations. Only the bounded free reserve is pooled;
//! checked-out chunks belong to their run or memtable, including snapshots.
//! Shared ownership returns an allocation only after its last user releases it,
//! bounding idle capacity without constraining live data.
use std::fmt;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, Weak};

pub(crate) const CHUNK_BYTES: usize = 1 << 20;
pub(crate) const RESERVE_BYTES: usize = 32 << 20;

#[derive(Default, Debug)]
struct State {
    free: Vec<Box<[u8]>>,
    in_use: usize,
    limit: usize,
}

#[derive(Debug)]
struct Pool {
    state: Mutex<State>,
    /// Logically charged bytes across checked-out chunks. Updated atomically
    /// on writes without taking `state`.
    used: AtomicUsize,
    counters: Counters,
}

/// Cumulative work performed by this engine, including abandoned builds.
#[cfg_attr(not(any(test, feature = "diagnostics")), allow(dead_code))]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct BuildCounters {
    /// Settled blocks a merge into the bottom level copied forward without
    /// decoding them.
    pub blocks_reused: u64,
    /// Their frame bytes, including headers.
    pub bytes_reused: u64,
    /// Newly encoded output blocks (including uncompressed output).
    pub blocks_compressed: u64,
    /// Newly encoded frame bytes, including headers.
    pub bytes_compressed: u64,
    /// Encoded raw block bytes passed to Zstandard, excluding ratio probes.
    pub raw_bytes_compressed: u64,
    /// Admitted key and value bytes; deletes count their keys.
    pub raw_bytes_ingested: u64,
}
#[derive(Default, Debug)]
struct Counters {
    blocks_reused: AtomicU64,
    bytes_reused: AtomicU64,
    blocks_compressed: AtomicU64,
    bytes_compressed: AtomicU64,
    raw_bytes_compressed: AtomicU64,
    raw_bytes_ingested: AtomicU64,
    /// Raw input, raw output and stored output bytes by [`work_slot`].
    #[cfg(feature = "diagnostics")]
    work: [[AtomicU64; 3]; WORK_SLOTS],
    /// Records written into the bottom level, by [`Erasure`] field.
    #[cfg(feature = "diagnostics")]
    erasure: [AtomicU64; 5],
    /// Copying forward, by [`CopyForward`] field.
    #[cfg(feature = "diagnostics")]
    copied: [AtomicU64; 8],
}

/// What merges into the bottom level copied forward.
#[cfg(feature = "diagnostics")]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct CopyForward {
    /// Input blocks copied.
    pub blocks: u64,
    /// Their raw bytes.
    pub raw: u64,
    /// Their stored frame bytes.
    pub stored: u64,
    /// Input segments reached and judged.
    pub segments: u64,
    /// Judged segments given an output segment with their dictionary.
    pub inherited: u64,
    /// Blocks of the bottom-level inputs of merges from level 0.
    pub input_blocks: u64,
    /// Their raw bytes.
    pub input_raw: u64,
    /// Their segments.
    pub input_segments: u64,
}

/// What sequence erasure did to the records of jobs into the bottom level.
#[cfg(feature = "diagnostics")]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Erasure {
    /// Records written.
    pub records: u64,
    /// Records written with sequence 0.
    pub erased: u64,
    /// Filter probes of unselected runs that cover a record's key.
    pub probes: u64,
    /// Records kept because a covering run without a filter may hold the key.
    pub kept_unfiltered: u64,
    /// Records kept because a covering run's filter may hold the key.
    pub kept_filtered: u64,
}

#[cfg(feature = "diagnostics")]
pub(crate) const WORK_SLOTS: usize = 16;

/// Slot 0 is flush and ingestion; compaction into level `n` is slot `n`,
/// or `n + 8` if it is the bottom level.
#[cfg(feature = "diagnostics")]
pub(crate) fn work_slot(level: Option<usize>, bottom: bool) -> usize {
    level.map_or(0, |n| n.min(7) + if bottom { 8 } else { 0 })
}
impl Counters {
    #[cfg_attr(not(any(test, feature = "diagnostics")), allow(dead_code))]
    fn snapshot(&self) -> BuildCounters {
        BuildCounters {
            blocks_reused: self.blocks_reused.load(Ordering::Relaxed),
            bytes_reused: self.bytes_reused.load(Ordering::Relaxed),
            blocks_compressed: self.blocks_compressed.load(Ordering::Relaxed),
            bytes_compressed: self.bytes_compressed.load(Ordering::Relaxed),
            raw_bytes_compressed: self.raw_bytes_compressed.load(Ordering::Relaxed),
            raw_bytes_ingested: self.raw_bytes_ingested.load(Ordering::Relaxed),
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
            used: AtomicUsize::new(0),
            counters: Counters::default(),
        }))
    }

    #[cfg_attr(not(any(test, feature = "diagnostics")), allow(dead_code))]
    pub(crate) fn counters(&self) -> BuildCounters {
        self.0.counters.snapshot()
    }
    pub(crate) fn ingested(&self, bytes: usize) {
        self.0
            .counters
            .raw_bytes_ingested
            .fetch_add(bytes as u64, Ordering::Relaxed);
    }
    pub(crate) fn reused(&self, stored: usize) {
        self.0
            .counters
            .blocks_reused
            .fetch_add(1, Ordering::Relaxed);
        self.0
            .counters
            .bytes_reused
            .fetch_add(stored as u64, Ordering::Relaxed);
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
    #[cfg(feature = "diagnostics")]
    pub(crate) fn work(&self, slot: usize, raw_in: usize, raw_out: usize, stored: usize) {
        for (counter, bytes) in self.0.counters.work[slot]
            .iter()
            .zip([raw_in, raw_out, stored])
        {
            counter.fetch_add(bytes as u64, Ordering::Relaxed);
        }
    }
    #[cfg(feature = "diagnostics")]
    pub(crate) fn erasure(&self, add: Erasure) {
        let fields = [
            add.records,
            add.erased,
            add.probes,
            add.kept_unfiltered,
            add.kept_filtered,
        ];
        for (counter, n) in self.0.counters.erasure.iter().zip(fields) {
            counter.fetch_add(n, Ordering::Relaxed);
        }
    }
    #[cfg(feature = "diagnostics")]
    pub(crate) fn copied(&self, add: crate::run::CopyStats) {
        let fields = [add.blocks, add.raw, add.stored, add.segments, add.inherited];
        for (counter, n) in self.0.counters.copied.iter().zip(fields) {
            counter.fetch_add(n, Ordering::Relaxed);
        }
    }
    #[cfg(feature = "diagnostics")]
    pub(crate) fn bottom_inputs(&self, blocks: u64, raw: u64, segments: u64) {
        for (counter, n) in self.0.counters.copied[5..]
            .iter()
            .zip([blocks, raw, segments])
        {
            counter.fetch_add(n, Ordering::Relaxed);
        }
    }
    #[cfg(feature = "diagnostics")]
    pub(crate) fn copy_counters(&self) -> CopyForward {
        let [
            blocks,
            raw,
            stored,
            segments,
            inherited,
            input_blocks,
            input_raw,
            input_segments,
        ] = self
            .0
            .counters
            .copied
            .each_ref()
            .map(|n| n.load(Ordering::Relaxed));
        CopyForward {
            blocks,
            raw,
            stored,
            segments,
            inherited,
            input_blocks,
            input_raw,
            input_segments,
        }
    }
    #[cfg(feature = "diagnostics")]
    pub(crate) fn erasure_counters(&self) -> Erasure {
        let [records, erased, probes, kept_unfiltered, kept_filtered] = self
            .0
            .counters
            .erasure
            .each_ref()
            .map(|n| n.load(Ordering::Relaxed));
        Erasure {
            records,
            erased,
            probes,
            kept_unfiltered,
            kept_filtered,
        }
    }
    #[cfg(feature = "diagnostics")]
    pub(crate) fn work_counters(&self) -> [[u64; 3]; WORK_SLOTS] {
        self.0
            .counters
            .work
            .each_ref()
            .map(|slot| slot.each_ref().map(|n| n.load(Ordering::Relaxed)))
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
        let used = self.0.used.load(Ordering::Relaxed);
        (
            state.free.len() * CHUNK_BYTES,
            state.in_use * CHUNK_BYTES,
            state.limit * CHUNK_BYTES,
            (state.in_use * CHUNK_BYTES).saturating_sub(used),
        )
    }
    pub(crate) fn trim(&self) {
        let dropped = {
            let mut state = self
                .0
                .state
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            std::mem::take(&mut state.free)
        };
        drop(dropped);
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

    pub(crate) fn exact_parts(parts: &[&[u8]]) -> Self {
        let bytes = parts.concat().into_boxed_slice();
        Self {
            bytes,
            pool: Weak::new(),
            used: 0,
        }
    }

    #[cfg(test)]
    pub(crate) fn set_used(&mut self, used: usize) {
        if used == self.used {
            return;
        }
        if let Some(pool) = self.pool.upgrade() {
            Self::update_pool_used(&pool, self.used, used);
        }
        self.used = used;
    }

    /// Updates used bytes using the caller's strong handle to this chunk's
    /// pool, avoiding a `Weak::upgrade` and `Arc::drop` on every write.
    pub(crate) fn set_used_in(&mut self, pool: &ChunkPool, used: usize) {
        if used == self.used {
            return;
        }
        debug_assert!(self.pool.ptr_eq(&Arc::downgrade(&pool.0)));
        Self::update_pool_used(&pool.0, self.used, used);
        self.used = used;
    }

    fn update_pool_used(pool: &Pool, old: usize, new: usize) {
        if new > old {
            pool.used.fetch_add(new - old, Ordering::Relaxed);
        } else {
            pool.used.fetch_sub(old - new, Ordering::Relaxed);
        }
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
            let pool = ChunkPool(pool);
            let mut copy = pool.take();
            copy.bytes[..self.used].copy_from_slice(&self.bytes[..self.used]);
            copy.set_used_in(&pool, self.used);
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
            pool.used.fetch_sub(self.used, Ordering::Relaxed);
            state.in_use -= 1;
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

    #[test]
    fn accounting_tracks_used_bytes_across_updates_and_drops() {
        let pool = ChunkPool::new(2 * CHUNK_BYTES);
        let mut first = pool.take();
        let mut second = pool.take();
        assert_eq!(
            pool.accounting(),
            (0, 2 * CHUNK_BYTES, 2 * CHUNK_BYTES, 2 * CHUNK_BYTES)
        );
        first.set_used(1000);
        second.set_used(3000);
        assert_eq!(pool.accounting().3, 2 * CHUNK_BYTES - 4000);
        first.set_used(400);
        assert_eq!(pool.accounting().3, 2 * CHUNK_BYTES - 3400);
        drop(first);
        assert_eq!(pool.accounting().3, CHUNK_BYTES - 3000);
        drop(second);
        assert_eq!(pool.accounting(), (2 * CHUNK_BYTES, 0, 2 * CHUNK_BYTES, 0));
        pool.trim();
        assert_eq!(pool.accounting(), (0, 0, 2 * CHUNK_BYTES, 0));
    }
}
