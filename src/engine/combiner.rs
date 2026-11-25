//! Write-group combining for concurrent memtable updates and small batches.
//!
//! An uncontended writer takes the direct path with one atomic compare-and-swap
//! and copies nothing. When writers overlap, one leader claims combining
//! ownership atomically, holds the exclusive state lock once, drains up to
//! [`GROUP_MAX_REQUESTS`] requests (capped at [`GROUP_MAX_BYTES`] of payload)
//! across cache-line-isolated thread shards, applies them in order, releases
//! the state lock, wakes its followers and hands leadership to a waiting writer
//! if one remains.

use super::*;
use std::collections::VecDeque;
use std::sync::Condvar;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

/// Maximum requests applied in one combined group (leader plus followers).
/// Bounds exclusive state-lock hold time for waiting readers and tail latency
/// for queued followers while combining typical concurrent writer counts in
/// one lock acquisition.
pub(super) const GROUP_MAX_REQUESTS: usize = 64;

/// Maximum key and value payload bytes in one combined group, and the largest
/// single request eligible for combining. Keeps transient queued copies far
/// below one memtable budget.
pub(super) const GROUP_MAX_BYTES: usize = 1 << 20;

/// Longest time a follower spins in user space before parking on its condvar.
/// About one kernel park-and-wake round trip, so spinning never burns much
/// more than parking would cost. A small in-memory insert takes well under a
/// microsecond, so a group of a few dozen small requests finishes within the
/// bound and its followers skip the kernel. Longer waits (large batches,
/// flushes, level-0 backpressure) park after spinning at most this long.
/// Bounding by elapsed time keeps the bound independent of the cost of one
/// spin hint, and a follower descheduled while spinning parks as soon as it
/// runs again instead of competing with the leader for a core.
const FOLLOWER_SPIN_TIME: Duration = Duration::from_micros(20);

/// Spin hints between clock reads while a follower spins.
const FOLLOWER_SPIN_CHECK_EVERY: u32 = 32;

/// Number of cache-line-isolated submission shards so concurrent writers do
/// not contend on a single queue mutex across CPU clusters.
const SHARDS: usize = 16;

static NEXT_SHARD: AtomicUsize = AtomicUsize::new(0);

thread_local! {
    static THREAD_SHARD: usize = NEXT_SHARD.fetch_add(1, Ordering::Relaxed) % SHARDS;
}

fn thread_shard() -> usize {
    THREAD_SHARD.with(|&shard| shard)
}

/// Number of group-size histogram buckets: exact counts for sizes 1..=8,
/// then 9..=16 and 17+.
#[cfg(any(test, feature = "diagnostics"))]
pub(super) const GROUP_SIZE_BUCKETS: usize = 10;

/// Bit 0 of `WriteCombiner::mode`: a leader owns combining.
const COMBINING: usize = 1 << 0;
/// Bit 1 of `WriteCombiner::mode`: the combiner is closed.
const CLOSED: usize = 1 << 1;
/// Higher bits of `WriteCombiner::mode`: active direct-path writers (0 or 1).
const DIRECT_STEP: usize = 1 << 2;

#[cfg(any(test, feature = "diagnostics"))]
pub(super) fn group_size_bucket(size: usize) -> usize {
    match size {
        0 | 1 => 0,
        2..=8 => size - 1,
        9..=16 => 8,
        _ => 9,
    }
}

/// 128-byte aligned to avoid false sharing on 64-byte and 128-byte cache lines.
#[repr(align(128))]
#[derive(Default)]
struct Shard {
    entries: Mutex<VecDeque<Entry>>,
}

/// Coordinates fast-path direct writes and contended leader/follower groups.
pub(super) struct WriteCombiner {
    mode: AtomicUsize,
    pending_mask: AtomicU32,
    drain_cursor: AtomicUsize,
    shards: Box<[Shard; SHARDS]>,
}

impl Default for WriteCombiner {
    fn default() -> Self {
        Self {
            mode: AtomicUsize::new(0),
            pending_mask: AtomicU32::new(0),
            drain_cursor: AtomicUsize::new(0),
            shards: Box::new(std::array::from_fn(|_| Shard::default())),
        }
    }
}

struct Entry {
    request: Request,
    slot: Arc<Slot>,
}

struct Slot {
    done: AtomicBool,
    state: Mutex<SlotInner>,
    ready: Condvar,
}

struct SlotInner {
    outcome: SlotState,
    parked: bool,
}

enum SlotState {
    Waiting,
    Done(Result<u64, UpdateError>),
    Leader(Request),
}

enum SlotOutcome {
    Done(Result<u64, UpdateError>),
    Leader(Request),
}

impl Slot {
    fn new() -> Self {
        Self {
            done: AtomicBool::new(false),
            state: Mutex::new(SlotInner {
                outcome: SlotState::Waiting,
                parked: false,
            }),
            ready: Condvar::new(),
        }
    }

    fn wait(&self) -> SlotOutcome {
        let start = Instant::now();
        loop {
            for _ in 0..FOLLOWER_SPIN_CHECK_EVERY {
                if self.done.load(Ordering::Acquire) {
                    return self.take_finished();
                }
                std::hint::spin_loop();
            }
            if start.elapsed() >= FOLLOWER_SPIN_TIME {
                break;
            }
        }
        let mut inner = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        loop {
            match std::mem::replace(&mut inner.outcome, SlotState::Waiting) {
                SlotState::Waiting => {
                    inner.parked = true;
                    inner = self
                        .ready
                        .wait(inner)
                        .unwrap_or_else(PoisonError::into_inner);
                }
                SlotState::Done(result) => return SlotOutcome::Done(result),
                SlotState::Leader(request) => return SlotOutcome::Leader(request),
            }
        }
    }

    fn take_finished(&self) -> SlotOutcome {
        let mut inner = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        match std::mem::replace(&mut inner.outcome, SlotState::Waiting) {
            SlotState::Done(result) => SlotOutcome::Done(result),
            SlotState::Leader(request) => SlotOutcome::Leader(request),
            SlotState::Waiting => unreachable!("done is set only after writing outcome"),
        }
    }

    fn finish(&self, outcome: SlotState) {
        let parked = {
            let mut inner = self.state.lock().unwrap_or_else(PoisonError::into_inner);
            inner.outcome = outcome;
            self.done.store(true, Ordering::Release);
            inner.parked
        };
        if parked {
            self.ready.notify_one();
        }
    }

    fn complete(&self, result: Result<u64, UpdateError>) {
        self.finish(SlotState::Done(result));
    }

    fn promote(&self, request: Request) {
        self.finish(SlotState::Leader(request));
    }
}

enum Request {
    Single {
        keyspace: u32,
        bytes: Vec<u8>,
        key_len: usize,
        tombstone: bool,
    },
    Batch {
        batch: WriteBatch,
        charge: usize,
    },
}

impl Request {
    fn single(keyspace: u32, key: &[u8], value: Value<'_>) -> Self {
        let (value_bytes, tombstone) = match value {
            Value::Bytes(bytes) => (bytes, false),
            Value::Tombstone => (&[][..], true),
        };
        let mut bytes = Vec::with_capacity(key.len() + value_bytes.len());
        bytes.extend_from_slice(key);
        bytes.extend_from_slice(value_bytes);
        Self::Single {
            keyspace,
            bytes,
            key_len: key.len(),
            tombstone,
        }
    }

    fn payload_bytes(&self) -> usize {
        match self {
            Self::Single { bytes, .. } => bytes.len(),
            Self::Batch { batch, .. } => batch.payload_bytes(),
        }
    }
}

enum LeaderRequest<'a> {
    Single {
        keyspace: u32,
        key: &'a [u8],
        value: Value<'a>,
    },
    Batch {
        batch: &'a WriteBatch,
        charge: usize,
    },
    Owned(Request),
}

impl LeaderRequest<'_> {
    fn payload_bytes(&self) -> usize {
        match self {
            Self::Single { key, value, .. } => {
                key.len()
                    + match value {
                        Value::Bytes(bytes) => bytes.len(),
                        Value::Tombstone => 0,
                    }
            }
            Self::Batch { batch, .. } => batch.payload_bytes(),
            Self::Owned(request) => request.payload_bytes(),
        }
    }
}

struct DirectGuard<'a> {
    combiner: &'a WriteCombiner,
}

impl Drop for DirectGuard<'_> {
    fn drop(&mut self) {
        self.combiner.mode.fetch_sub(DIRECT_STEP, Ordering::Release);
    }
}

/// Ensures every follower taken by a leader and every follower waiting in the
/// shards wakes with an error if the leader unwinds.
struct LeaderGuard<'a> {
    combiner: &'a WriteCombiner,
    pending: VecDeque<Entry>,
    current: Option<Arc<Slot>>,
    completed: Vec<(Arc<Slot>, Result<u64, UpdateError>)>,
    active: bool,
}

impl<'a> LeaderGuard<'a> {
    fn new(combiner: &'a WriteCombiner) -> Self {
        Self {
            combiner,
            pending: VecDeque::new(),
            current: None,
            completed: Vec::new(),
            active: true,
        }
    }

    fn finish(&mut self) {
        for (slot, result) in self.completed.drain(..) {
            slot.complete(result);
        }
        self.combiner.handoff_or_release();
        self.active = false;
    }
}

impl Drop for LeaderGuard<'_> {
    fn drop(&mut self) {
        if !self.active {
            return;
        }
        for (slot, result) in self.completed.drain(..) {
            slot.complete(result);
        }
        if let Some(slot) = self.current.take() {
            slot.complete(Err(UpdateError::LeaderPanicked));
        }
        for entry in self.pending.drain(..) {
            entry.slot.complete(Err(UpdateError::LeaderPanicked));
        }
        self.combiner.abort_leader();
    }
}

impl WriteCombiner {
    fn try_enter_direct(&self) -> Option<DirectGuard<'_>> {
        if self
            .mode
            .compare_exchange(0, DIRECT_STEP, Ordering::Acquire, Ordering::Relaxed)
            .is_ok()
        {
            Some(DirectGuard { combiner: self })
        } else {
            None
        }
    }

    fn try_claim_leader(&self) -> Result<bool, UpdateError> {
        let mut current = self.mode.load(Ordering::Acquire);
        loop {
            if (current & CLOSED) != 0 {
                return Err(UpdateError::Stopped);
            }
            if (current & COMBINING) != 0 {
                return Ok(false);
            }
            match self.mode.compare_exchange_weak(
                current,
                current | COMBINING,
                Ordering::SeqCst,
                Ordering::Acquire,
            ) {
                Ok(_) => return Ok(true),
                Err(observed) => current = observed,
            }
        }
    }

    #[cfg(test)]
    fn is_combining(&self) -> bool {
        (self.mode.load(Ordering::Acquire) & COMBINING) != 0
    }

    fn is_closed(&self) -> bool {
        (self.mode.load(Ordering::Acquire) & CLOSED) != 0
    }

    fn lock_shard(&self, index: usize) -> MutexGuard<'_, VecDeque<Entry>> {
        self.shards[index]
            .entries
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }

    fn drain_shards(
        &self,
        requests: &mut usize,
        bytes: &mut usize,
        out: &mut VecDeque<Entry>,
    ) -> usize {
        let mask = self.pending_mask.load(Ordering::Acquire);
        if mask == 0 || *requests >= GROUP_MAX_REQUESTS {
            return 0;
        }
        let start = self.drain_cursor.fetch_add(1, Ordering::Relaxed) % SHARDS;
        let mut drained = 0usize;
        for offset in 0..SHARDS {
            if *requests >= GROUP_MAX_REQUESTS {
                break;
            }
            let idx = (start + offset) % SHARDS;
            let bit = 1u32 << idx;
            if (mask & bit) == 0 {
                continue;
            }
            let mut entries = self.lock_shard(idx);
            while *requests < GROUP_MAX_REQUESTS {
                let Some(front) = entries.front() else {
                    break;
                };
                let next_bytes = front.request.payload_bytes();
                if bytes.saturating_add(next_bytes) > GROUP_MAX_BYTES {
                    break;
                }
                *bytes += next_bytes;
                *requests += 1;
                drained += 1;
                out.push_back(entries.pop_front().unwrap());
            }
            if entries.is_empty() {
                self.pending_mask.fetch_and(!bit, Ordering::Release);
            }
        }
        drained
    }

    fn pop_one(&self) -> Option<Entry> {
        let mut mask = self.pending_mask.load(Ordering::Acquire);
        while mask != 0 {
            let start = self.drain_cursor.fetch_add(1, Ordering::Relaxed) % SHARDS;
            for offset in 0..SHARDS {
                let idx = (start + offset) % SHARDS;
                let bit = 1u32 << idx;
                if (mask & bit) == 0 {
                    continue;
                }
                let mut entries = self.lock_shard(idx);
                if let Some(entry) = entries.pop_front() {
                    if entries.is_empty() {
                        self.pending_mask.fetch_and(!bit, Ordering::Release);
                    }
                    return Some(entry);
                }
                self.pending_mask.fetch_and(!bit, Ordering::Release);
            }
            mask = self.pending_mask.load(Ordering::Acquire);
        }
        None
    }

    fn handoff_or_release(&self) {
        loop {
            if let Some(entry) = self.pop_one() {
                entry.slot.promote(entry.request);
                return;
            }
            self.mode.fetch_and(!COMBINING, Ordering::SeqCst);
            if self.pending_mask.load(Ordering::SeqCst) == 0 {
                return;
            }
            match self.try_claim_leader() {
                Ok(true) => continue,
                Ok(false) | Err(_) => return,
            }
        }
    }

    fn abort_leader(&self) {
        self.mode.fetch_and(!COMBINING, Ordering::SeqCst);
        let mut waiting = Vec::new();
        for idx in 0..SHARDS {
            let mut entries = self.lock_shard(idx);
            waiting.extend(entries.drain(..));
            self.pending_mask
                .fetch_and(!(1u32 << idx), Ordering::Release);
        }
        for entry in waiting {
            entry.slot.complete(Err(UpdateError::LeaderPanicked));
        }
    }

    /// Marks the combiner closed and wakes waiting followers with `Stopped`.
    pub(super) fn close(&self) {
        self.mode.fetch_or(CLOSED, Ordering::SeqCst);
        let mut waiting = Vec::new();
        for idx in 0..SHARDS {
            let mut entries = self.lock_shard(idx);
            waiting.extend(entries.drain(..));
            self.pending_mask
                .fetch_and(!(1u32 << idx), Ordering::Release);
        }
        for entry in waiting {
            entry.slot.complete(Err(UpdateError::Stopped));
        }
    }

    #[cfg(test)]
    pub(super) fn queued_len(&self) -> usize {
        (0..SHARDS).map(|idx| self.lock_shard(idx).len()).sum()
    }

    #[cfg(test)]
    pub(super) fn all_queued_published(&self, expected: usize) -> bool {
        let mut total = 0usize;
        let mut non_empty_mask = 0u32;
        for idx in 0..SHARDS {
            let len = self.lock_shard(idx).len();
            if len > 0 {
                total += len;
                non_empty_mask |= 1u32 << idx;
            }
        }
        total >= expected
            && (self.pending_mask.load(Ordering::SeqCst) & non_empty_mask) == non_empty_mask
    }
}

impl Core {
    pub(super) fn combine_single(
        &self,
        keyspace: u32,
        key: &[u8],
        value: Value<'_>,
    ) -> Result<u64, UpdateError> {
        let payload = key.len()
            + match value {
                Value::Bytes(bytes) => bytes.len(),
                Value::Tombstone => 0,
            };
        if payload > GROUP_MAX_BYTES {
            return self.update_direct(keyspace, key, value);
        }
        if let Some(_direct) = self.combiner.try_enter_direct() {
            return self.update_direct(keyspace, key, value);
        }
        self.submit_contended(
            LeaderRequest::Single {
                keyspace,
                key,
                value,
            },
            || Request::single(keyspace, key, value),
        )
    }

    pub(super) fn combine_batch(
        &self,
        batch: &WriteBatch,
        charge: usize,
    ) -> Result<u64, UpdateError> {
        if charge >= self.bulk_ingest_min || batch.payload_bytes() > GROUP_MAX_BYTES {
            return self.write_batch_direct(batch, charge);
        }
        if let Some(_direct) = self.combiner.try_enter_direct() {
            return self.write_batch_direct(batch, charge);
        }
        self.submit_contended(LeaderRequest::Batch { batch, charge }, || Request::Batch {
            batch: batch.clone(),
            charge,
        })
    }

    fn submit_contended(
        &self,
        borrowed: LeaderRequest<'_>,
        to_owned: impl FnOnce() -> Request,
    ) -> Result<u64, UpdateError> {
        if self.combiner.try_claim_leader()? {
            return self.run_leader(borrowed);
        }
        let request = to_owned();
        let slot = Arc::new(Slot::new());
        let idx = thread_shard();
        let bit = 1u32 << idx;
        {
            let mut entries = self.combiner.lock_shard(idx);
            if self.combiner.is_closed() {
                return Err(UpdateError::Stopped);
            }
            entries.push_back(Entry {
                request,
                slot: Arc::clone(&slot),
            });
        }
        // Pairs with `handoff_or_release`'s SeqCst clear of COMBINING and load
        // of `pending_mask` so no enqueued entry is stranded without a leader.
        self.combiner.pending_mask.fetch_or(bit, Ordering::SeqCst);
        if (self.combiner.mode.load(Ordering::SeqCst) & COMBINING) == 0
            && matches!(self.combiner.try_claim_leader(), Ok(true))
        {
            self.combiner.handoff_or_release();
        }
        match slot.wait() {
            SlotOutcome::Done(result) => result,
            SlotOutcome::Leader(request) => self.run_leader(LeaderRequest::Owned(request)),
        }
    }

    fn run_leader(&self, first: LeaderRequest<'_>) -> Result<u64, UpdateError> {
        let mut guard = LeaderGuard::new(&self.combiner);
        if self.combiner.is_closed() {
            guard.finish();
            return Err(UpdateError::Stopped);
        }
        #[cfg(test)]
        if let Some(hook) = self.before_combine.lock().unwrap().clone() {
            hook();
        }
        let state = match self.admit(self.write_state()) {
            Ok(state) => state,
            Err(error) => {
                guard.finish();
                return Err(error);
            }
        };
        let mut group_requests = 1usize;
        let mut group_bytes = first.payload_bytes();
        self.combiner
            .drain_shards(&mut group_requests, &mut group_bytes, &mut guard.pending);

        let (leader_result, mut state_opt) = self.apply_leader_request(state, first);
        let mut abort_error = match (&leader_result, &state_opt) {
            (Err(error), None) => Some(error.clone()),
            _ => None,
        };

        loop {
            while let Some(entry) = guard.pending.pop_front() {
                let Some(mut state) = state_opt.take() else {
                    let error = abort_error
                        .clone()
                        .expect("missing state only after an admission failure");
                    guard.completed.push((entry.slot, Err(error)));
                    continue;
                };
                guard.current = Some(entry.slot);
                if self.memory_limit.is_some() {
                    match self.admit(state) {
                        Ok(admitted) => state = admitted,
                        Err(error) => {
                            abort_error = Some(error.clone());
                            let slot = guard.current.take().unwrap();
                            guard.completed.push((slot, Err(error)));
                            continue;
                        }
                    }
                }
                let (result, next_state) = self.apply_owned_request(state, entry.request);
                if next_state.is_none()
                    && let Err(error) = &result
                {
                    abort_error = Some(error.clone());
                }
                state_opt = next_state;
                let slot = guard.current.take().unwrap();
                guard.completed.push((slot, result));
            }
            if state_opt.is_none()
                || self.combiner.drain_shards(
                    &mut group_requests,
                    &mut group_bytes,
                    &mut guard.pending,
                ) == 0
            {
                break;
            }
        }
        drop(state_opt);
        #[cfg(any(test, feature = "diagnostics"))]
        self.resources.record_write_group(group_requests);
        guard.finish();
        leader_result
    }

    fn apply_leader_request<'s>(
        &'s self,
        state: RwLockWriteGuard<'s, State>,
        request: LeaderRequest<'_>,
    ) -> (
        Result<u64, UpdateError>,
        Option<RwLockWriteGuard<'s, State>>,
    ) {
        match request {
            LeaderRequest::Single {
                keyspace,
                key,
                value,
            } => self.apply_single(state, keyspace, key, value),
            LeaderRequest::Batch { batch, charge } => self.apply_batch(state, batch, charge),
            LeaderRequest::Owned(request) => self.apply_owned_request(state, request),
        }
    }

    fn apply_owned_request<'s>(
        &'s self,
        state: RwLockWriteGuard<'s, State>,
        request: Request,
    ) -> (
        Result<u64, UpdateError>,
        Option<RwLockWriteGuard<'s, State>>,
    ) {
        match request {
            Request::Single {
                keyspace,
                bytes,
                key_len,
                tombstone,
            } => {
                let key = &bytes[..key_len];
                let value = if tombstone {
                    Value::Tombstone
                } else {
                    Value::Bytes(&bytes[key_len..])
                };
                self.apply_single(state, keyspace, key, value)
            }
            Request::Batch { batch, charge } => self.apply_batch(state, &batch, charge),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicBool;
    use std::sync::mpsc;
    use std::time::Duration;

    fn manual() -> Options {
        Options {
            background_compaction: None,
            ..Options::default()
        }
    }

    fn wait_for_queued(core: &Core, expected: usize) {
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        while !core.combiner.all_queued_published(expected) {
            assert!(
                std::time::Instant::now() < deadline,
                "timed out waiting for {expected} queued followers (saw {})",
                core.combiner.queued_len()
            );
            std::thread::yield_now();
        }
    }

    fn wait_for_combining(core: &Core) {
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        while !core.combiner.is_combining() {
            assert!(
                std::time::Instant::now() < deadline,
                "timed out waiting for leader to enter combiner"
            );
            std::thread::yield_now();
        }
    }

    fn pause_leader(core: &Core) -> (mpsc::Receiver<()>, mpsc::Sender<()>) {
        let (entered_tx, entered_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let release_rx = Mutex::new(release_rx);
        let fired = AtomicBool::new(false);
        *core.before_combine.lock().unwrap() = Some(Arc::new(move || {
            if fired.swap(true, Ordering::SeqCst) {
                return;
            }
            entered_tx.send(()).unwrap();
            release_rx
                .lock()
                .unwrap()
                .recv_timeout(Duration::from_secs(5))
                .expect("leader release");
        }));
        (entered_rx, release_tx)
    }

    #[test]
    fn uncontended_writes_take_no_queue() {
        let engine = Engine::in_memory(manual()).unwrap();
        let ks = engine.ks();
        for i in 0..200u32 {
            ks.put(&i.to_be_bytes(), b"value").unwrap();
        }
        for i in 0..50u32 {
            ks.delete(&i.to_be_bytes()).unwrap();
        }
        let mut batch = WriteBatch::new();
        for i in 0..50u32 {
            batch.clear();
            batch.put(&ks, &i.to_be_bytes(), b"a");
            batch.put(&ks, &(i + 1000).to_be_bytes(), b"b");
            engine.write(&batch).unwrap();
        }
        assert_eq!(
            engine.core.resources.write_groups.load(Ordering::Relaxed),
            0
        );
        assert_eq!(
            engine.core.resources.grouped_writes.load(Ordering::Relaxed),
            0
        );
        for bucket in &engine.core.resources.write_group_sizes {
            assert_eq!(bucket.load(Ordering::Relaxed), 0);
        }
    }

    #[test]
    fn concurrent_batches_are_atomic_and_contiguous_under_snapshot_oracle() {
        // Each 4-op batch charges ~860 bytes (below bulk_ingest_min = 1024), so
        // two batches fit in `active` and the 6-writer group rotates mid-group,
        // pausing on `wait_for_flush` while the flush worker holds `frozen[0]`.
        let engine = Engine::in_memory(Options {
            memtable_size: 2048,
            max_frozen_memtables: 1,
            ..manual()
        })
        .unwrap();
        let ks = engine.ks();
        let writers = 6usize;
        let rounds = 4u8;
        let ops_per_batch = 4usize;

        let check_oracle = |snap: &Snapshot,
                            expected_new: &[u8],
                            expected_prev: Option<&[u8]>,
                            require_partial: bool| {
            let mut reader = snap.reader();
            let mut at_new = 0usize;
            let mut at_prev = 0usize;
            for w in 0..writers {
                let mut seen = None;
                for slot in 0..ops_per_batch {
                    let key = format!("w{w:02}/k{slot}");
                    let val = reader.get(&ks, key.as_bytes()).unwrap().map(<[u8]>::to_vec);
                    if let Some(expected) = &seen {
                        assert_eq!(
                            &val, expected,
                            "partial batch observed for writer {w} slot {slot}"
                        );
                    } else {
                        seen = Some(val);
                    }
                }
                match seen.unwrap().as_deref() {
                    Some(v) if v == expected_new => at_new += 1,
                    v if v == expected_prev => at_prev += 1,
                    other => panic!("unexpected value for writer {w}: {other:?}"),
                }
            }
            if require_partial {
                assert!(
                    at_new > 0 && at_prev > 0,
                    "expected mid-group snapshot to split round (new={at_new}, prev={at_prev})"
                );
            } else {
                assert_eq!(at_new, writers);
            }
        };

        let mut ends = Vec::with_capacity(writers * usize::from(rounds));
        for round in 1..=rounds {
            let payload = [round; 128];
            let prev_payload = (round > 1).then_some([round - 1; 128]);
            let (flush_entered_tx, flush_entered_rx) = mpsc::channel();
            let (flush_release_tx, flush_release_rx) = mpsc::channel();
            let flush_release_rx = Mutex::new(flush_release_rx);
            *engine.core.before_flush_build.lock().unwrap() = Some(Arc::new(move || {
                flush_entered_tx.send(()).unwrap();
                flush_release_rx
                    .lock()
                    .unwrap()
                    .recv_timeout(Duration::from_secs(5))
                    .expect("flush release");
            }));

            let waits_before = engine.core.flush_waits.load(Ordering::SeqCst);
            let groups_before = engine.core.resources.write_groups.load(Ordering::Relaxed);
            let grouped_before = engine.core.resources.grouped_writes.load(Ordering::Relaxed);
            let (entered_rx, release_tx) = pause_leader(&engine.core);
            let direct = engine.core.combiner.try_enter_direct().unwrap();

            let round_ends: Vec<u64> = std::thread::scope(|scope| {
                let mut handles = Vec::with_capacity(writers);
                for w in 0..writers {
                    let (engine, ks) = (&engine, &ks);
                    handles.push(scope.spawn(move || {
                        let mut batch = WriteBatch::new();
                        for slot in 0..ops_per_batch {
                            let key = format!("w{w:02}/k{slot}");
                            batch.put(ks, key.as_bytes(), &payload);
                        }
                        engine.write_seq(&batch).unwrap()
                    }));
                    if w == 0 {
                        entered_rx.recv_timeout(Duration::from_secs(5)).unwrap();
                        wait_for_combining(&engine.core);
                    } else {
                        wait_for_queued(&engine.core, w);
                    }
                }

                drop(direct);
                release_tx.send(()).unwrap();

                flush_entered_rx
                    .recv_timeout(Duration::from_secs(5))
                    .unwrap();
                crate::test_support::wait_until(|| {
                    engine.core.flush_waits.load(Ordering::SeqCst) > waits_before
                });
                let mid = engine.snapshot();
                check_oracle(&mid, &payload, prev_payload.as_ref().map(|p| &p[..]), true);
                drop(mid);

                flush_release_tx.send(()).unwrap();
                handles.into_iter().map(|h| h.join().unwrap()).collect()
            });

            assert_eq!(
                engine.core.resources.write_groups.load(Ordering::Relaxed),
                groups_before + 1
            );
            assert_eq!(
                engine.core.resources.grouped_writes.load(Ordering::Relaxed),
                grouped_before + writers as u64
            );
            ends.extend(round_ends);
            let post = engine.snapshot();
            check_oracle(&post, &payload, prev_payload.as_ref().map(|p| &p[..]), false);
            drop(post);
            engine.flush().unwrap();
        }

        ends.sort_unstable();
        let total_batches = writers * usize::from(rounds);
        assert_eq!(ends.len(), total_batches);
        for (i, &end) in ends.iter().enumerate() {
            assert_eq!(
                end,
                ((i + 1) * ops_per_batch) as u64,
                "batch sequences must be contiguous with no gaps or overlap"
            );
        }
        assert_eq!(
            engine.engine_stats().last_seq,
            (total_batches * ops_per_batch) as u64
        );
        let final_payload = [rounds; 128];
        for w in 0..writers {
            for slot in 0..ops_per_batch {
                let key = format!("w{w:02}/k{slot}");
                assert_eq!(
                    ks.get(key.as_bytes()).unwrap().as_deref(),
                    Some(&final_payload[..])
                );
            }
        }
        engine.close().unwrap();
    }

    #[test]
    fn dropped_keyspace_error_inside_group_affects_only_that_writer() {
        let engine = Engine::in_memory(manual()).unwrap();
        let live = engine.keyspace("live").unwrap();
        let doomed = engine.keyspace("doomed").unwrap();
        assert!(engine.drop_keyspace("doomed").unwrap());

        let (entered_rx, release_tx) = pause_leader(&engine.core);
        let direct = engine.core.combiner.try_enter_direct().unwrap();

        let (r1, r2, r3) = std::thread::scope(|scope| {
            let t1 = scope.spawn(|| live.put(b"k1", b"v1"));
            entered_rx.recv_timeout(Duration::from_secs(5)).unwrap();
            wait_for_combining(&engine.core);

            let t2 = scope.spawn(|| doomed.put(b"k2", b"v2"));
            wait_for_queued(&engine.core, 1);
            let t3 = scope.spawn(|| live.put(b"k3", b"v3"));
            wait_for_queued(&engine.core, 2);

            drop(direct);
            release_tx.send(()).unwrap();
            (t1.join().unwrap(), t2.join().unwrap(), t3.join().unwrap())
        });

        assert!(r1.is_ok(), "{r1:?}");
        let err = r2.unwrap_err();
        assert_eq!(err.kind(), ErrorKind::KeyspaceDropped);
        assert!(err.to_string().contains("doomed"), "{err}");
        assert!(r3.is_ok(), "{r3:?}");

        assert_eq!(live.get(b"k1").unwrap().as_deref(), Some(&b"v1"[..]));
        assert_eq!(live.get(b"k3").unwrap().as_deref(), Some(&b"v3"[..]));
        assert_eq!(engine.engine_stats().last_seq, 2);
        assert_eq!(
            engine.core.resources.write_groups.load(Ordering::Relaxed),
            1
        );
        assert_eq!(
            engine.core.resources.grouped_writes.load(Ordering::Relaxed),
            3
        );
    }

    #[test]
    fn group_hitting_memtable_full_mid_group_rotates_and_loses_nothing() {
        let engine = Engine::in_memory(Options {
            memtable_size: 16 << 10,
            ..Options::default()
        })
        .unwrap();
        let ks = engine.ks();
        let writers = 12usize;
        let payload = vec![b'x'; 2048];

        let (entered_rx, release_tx) = pause_leader(&engine.core);
        let direct = engine.core.combiner.try_enter_direct().unwrap();

        let results: Vec<Result<u64, UpdateError>> = std::thread::scope(|scope| {
            let mut handles = Vec::with_capacity(writers);
            let (core, ks, payload) = (&engine.core, &ks, &payload);
            handles.push(
                scope.spawn(move || core.update(ks.id(), b"k00", Value::Bytes(payload.as_slice()))),
            );
            entered_rx.recv_timeout(Duration::from_secs(5)).unwrap();
            wait_for_combining(core);

            for i in 1..writers {
                let key = format!("k{i:02}");
                handles.push(scope.spawn(move || {
                    core.update(ks.id(), key.as_bytes(), Value::Bytes(payload.as_slice()))
                }));
                wait_for_queued(core, i);
            }

            drop(direct);
            release_tx.send(()).unwrap();
            handles.into_iter().map(|h| h.join().unwrap()).collect()
        });

        let mut seqs: Vec<u64> = results.into_iter().map(Result::unwrap).collect();
        seqs.sort_unstable();
        assert_eq!(seqs, (1..=writers as u64).collect::<Vec<_>>());
        assert_eq!(
            engine.core.resources.write_groups.load(Ordering::Relaxed),
            1
        );
        assert_eq!(
            engine.core.resources.grouped_writes.load(Ordering::Relaxed),
            writers as u64
        );
        let stats = engine.engine_stats();
        assert!(
            stats.frozen_tables > 0 || stats.flushes > 0,
            "mid-group Refusal::Full must rotate the active memtable"
        );
        for i in 0..writers {
            let key = format!("k{i:02}");
            assert_eq!(
                ks.get(key.as_bytes()).unwrap().as_deref(),
                Some(payload.as_slice())
            );
        }
        engine.close().unwrap();
    }

    #[test]
    fn memory_limit_refusal_reaches_the_right_writers() {
        // Start with one settled uncompressed run. Set `memory_limit` just above
        // its accounted size so Writer 1 is admitted and writes live data that
        // stays above the limit even after flush and compaction, causing Writers
        // 2 and 3 in the same combined group to receive MemoryLimit.
        let mut core = Core::new(&Options {
            memtable_size: 64 << 10,
            target_run_len: 32 << 10,
            compression: Compression::Uncompressed,
            ..manual()
        });
        core.put(b"seed", &[1u8; 4096]).unwrap();
        core.begin_flush().unwrap().build().publish();
        let policy = CompactionPolicy {
            max_overlap: 1,
            ..core.policy
        };
        while core
            .compact(policy, &mut MergeWorkspace::new())
            .unwrap()
            .is_some()
        {}
        core.chunks.trim();
        let base_used = core.stats().used;
        let limit = base_used + 4096;
        core.memory_limit = Some(limit);
        core.policy = CompactionPolicy::for_memory(Some(limit));
        let engine = Engine::start(core, &manual(), None, None).unwrap();
        let ks = engine.ks();
        let live_payload = vec![2u8; 8192];

        let (entered_rx, release_tx) = pause_leader(&engine.core);
        let direct = engine.core.combiner.try_enter_direct().unwrap();

        let (r1, r2, r3) = std::thread::scope(|scope| {
            let (ks, live_payload) = (&ks, &live_payload);
            let t1 = scope.spawn(move || ks.put(b"admitted", live_payload));
            entered_rx.recv_timeout(Duration::from_secs(5)).unwrap();
            wait_for_combining(&engine.core);

            let t2 = scope.spawn(move || ks.put(b"refused-1", b"v"));
            wait_for_queued(&engine.core, 1);
            let t3 = scope.spawn(move || ks.put(b"refused-2", b"v"));
            wait_for_queued(&engine.core, 2);

            drop(direct);
            release_tx.send(()).unwrap();
            (t1.join().unwrap(), t2.join().unwrap(), t3.join().unwrap())
        });

        assert!(r1.is_ok(), "{r1:?}");
        assert_eq!(r2.unwrap_err().kind(), ErrorKind::MemoryLimit);
        assert_eq!(r3.unwrap_err().kind(), ErrorKind::MemoryLimit);
        assert_eq!(
            ks.get(b"admitted").unwrap().as_deref(),
            Some(live_payload.as_slice())
        );
        assert_eq!(ks.get(b"refused-1").unwrap(), None);
        assert_eq!(ks.get(b"refused-2").unwrap(), None);
        assert_eq!(
            engine.core.resources.write_groups.load(Ordering::Relaxed),
            1
        );
        assert_eq!(
            engine.core.resources.grouped_writes.load(Ordering::Relaxed),
            3
        );
        engine.close().unwrap();
    }

    #[test]
    fn close_wakes_waiting_followers_with_stopped() {
        // Deterministic combiner-level check: parked followers wake with Stopped.
        let engine = Engine::in_memory(manual()).unwrap();
        let ks = engine.ks();
        let (entered_rx, release_tx) = pause_leader(&engine.core);
        let direct = engine.core.combiner.try_enter_direct().unwrap();

        let (leader_res, follower_res) = std::thread::scope(|scope| {
            let (core, ks) = (&engine.core, &ks);
            let leader = scope.spawn(move || core.update(ks.id(), b"l", Value::Bytes(b"v")));
            entered_rx.recv_timeout(Duration::from_secs(5)).unwrap();
            wait_for_combining(core);

            let mut followers = Vec::new();
            for i in 0..4usize {
                let key = format!("f{i}");
                followers.push(
                    scope.spawn(move || core.update(ks.id(), key.as_bytes(), Value::Bytes(b"v"))),
                );
                wait_for_queued(core, i + 1);
            }

            core.combiner.close();
            let follower_res: Vec<_> = followers.into_iter().map(|h| h.join().unwrap()).collect();

            drop(direct);
            release_tx.send(()).unwrap();
            (leader.join().unwrap(), follower_res)
        });

        for result in follower_res {
            assert_eq!(result, Err(UpdateError::Stopped));
        }
        assert!(leader_res.is_ok() || leader_res == Err(UpdateError::Stopped));
        engine.close().unwrap();

        // Concurrent Engine::close during an active combined group waits for
        // the in-flight group to finish, closes cleanly and rejects new writes.
        let engine = Engine::in_memory(Options::default()).unwrap();
        let ks = engine.ks();
        let (entered_rx, release_tx) = pause_leader(&engine.core);
        let direct = engine.core.combiner.try_enter_direct().unwrap();
        let (closing_tx, closing_rx) = mpsc::channel();
        std::thread::scope(|scope| {
            let leader = scope.spawn(|| ks.put(b"l", b"payload"));
            entered_rx.recv_timeout(Duration::from_secs(5)).unwrap();
            wait_for_combining(&engine.core);

            let mut followers = Vec::new();
            for i in 0..4usize {
                let key = format!("f{i}");
                let ks = &ks;
                followers.push(scope.spawn(move || ks.put(key.as_bytes(), b"payload")));
                wait_for_queued(&engine.core, i + 1);
            }

            let closer = engine.clone();
            let closer_handle = scope.spawn(move || {
                closer.close_with(|stripe| {
                    if stripe == 0 {
                        closing_tx.send(()).unwrap();
                    }
                })
            });
            closing_rx.recv_timeout(Duration::from_secs(5)).unwrap();

            drop(direct);
            release_tx.send(()).unwrap();
            leader.join().unwrap().unwrap();
            for follower in followers {
                follower.join().unwrap().unwrap();
            }
            closer_handle.join().unwrap().unwrap();
        });
        assert_eq!(
            engine.core.resources.write_groups.load(Ordering::Relaxed),
            1
        );
        assert_eq!(
            engine.core.resources.grouped_writes.load(Ordering::Relaxed),
            5
        );
        assert_eq!(
            ks.put(b"after", b"payload").unwrap_err().kind(),
            ErrorKind::Stopped
        );
    }

    #[test]
    fn leader_panic_wakes_followers_with_errors_and_does_not_hang() {
        // Sequence exhaustion panics the leader inside the state lock while
        // applying the second request in a three-writer group.
        let engine = Engine::in_memory(Options {
            last_seq: u64::MAX - 1,
            ..manual()
        })
        .unwrap();
        let ks = engine.ks();
        let (entered_rx, release_tx) = pause_leader(&engine.core);
        let direct = engine.core.combiner.try_enter_direct().unwrap();

        let (r1, r2, r3) = std::thread::scope(|scope| {
            let t1 = scope.spawn(|| ks.put(b"k1", b"v1"));
            entered_rx.recv_timeout(Duration::from_secs(5)).unwrap();
            wait_for_combining(&engine.core);

            let t2 = scope.spawn(|| ks.put(b"k2", b"v2"));
            wait_for_queued(&engine.core, 1);
            let t3 = scope.spawn(|| ks.put(b"k3", b"v3"));
            wait_for_queued(&engine.core, 2);

            drop(direct);
            release_tx.send(()).unwrap();
            (t1.join(), t2.join().unwrap(), t3.join().unwrap())
        });

        assert!(r1.is_err(), "leader must panic on sequence exhaustion");
        assert_eq!(r2.unwrap_err().kind(), ErrorKind::Background);
        assert_eq!(r3.unwrap_err().kind(), ErrorKind::Background);
        engine.close().unwrap();

        // A panic before acquiring the state lock wakes queued followers without
        // poisoning state or wedging the combiner for subsequent writes.
        let engine = Engine::in_memory(manual()).unwrap();
        let ks = engine.ks();
        let (entered_tx, entered_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let release_rx = Mutex::new(release_rx);
        let fired = AtomicBool::new(false);
        *engine.core.before_combine.lock().unwrap() = Some(Arc::new(move || {
            if fired.swap(true, Ordering::SeqCst) {
                return;
            }
            entered_tx.send(()).unwrap();
            release_rx
                .lock()
                .unwrap()
                .recv_timeout(Duration::from_secs(5))
                .unwrap();
            panic!("injected leader failure");
        }));
        let direct = engine.core.combiner.try_enter_direct().unwrap();

        let (r1, r2, r3) = std::thread::scope(|scope| {
            let t1 = scope.spawn(|| ks.put(b"p1", b"v1"));
            entered_rx.recv_timeout(Duration::from_secs(5)).unwrap();
            wait_for_combining(&engine.core);

            let t2 = scope.spawn(|| ks.put(b"p2", b"v2"));
            wait_for_queued(&engine.core, 1);
            let t3 = scope.spawn(|| ks.put(b"p3", b"v3"));
            wait_for_queued(&engine.core, 2);

            drop(direct);
            release_tx.send(()).unwrap();
            (t1.join(), t2.join().unwrap(), t3.join().unwrap())
        });

        assert!(r1.is_err());
        assert_eq!(r2.unwrap_err().kind(), ErrorKind::Background);
        assert_eq!(r3.unwrap_err().kind(), ErrorKind::Background);
        ks.put(b"after", b"ok").unwrap();
        assert_eq!(ks.get(b"after").unwrap().as_deref(), Some(&b"ok"[..]));
        engine.close().unwrap();
    }

    #[test]
    fn byte_cap_break_in_drain_shards_hands_off_without_stranding() {
        let engine = Engine::in_memory(Options {
            memtable_size: 4 << 20,
            ..manual()
        })
        .unwrap();
        let ks = engine.ks();
        let big_a = vec![b'a'; 600 << 10];
        let big_b = vec![b'b'; 600 << 10];

        let (entered_rx, release_tx) = pause_leader(&engine.core);
        let direct = engine.core.combiner.try_enter_direct().unwrap();

        let (r1, r2, r3) = std::thread::scope(|scope| {
            let (core, ks, big_a, big_b) = (&engine.core, &ks, &big_a, &big_b);
            let t1 = scope.spawn(move || core.update(ks.id(), b"l0", Value::Bytes(big_a)));
            entered_rx.recv_timeout(Duration::from_secs(5)).unwrap();
            wait_for_combining(core);

            let t2 = scope.spawn(move || core.update(ks.id(), b"f1", Value::Bytes(big_b)));
            wait_for_queued(core, 1);
            let t3 = scope.spawn(move || core.update(ks.id(), b"f2", Value::Bytes(b"tiny")));
            wait_for_queued(core, 2);

            drop(direct);
            release_tx.send(()).unwrap();
            (
                t1.join().unwrap().unwrap(),
                t2.join().unwrap().unwrap(),
                t3.join().unwrap().unwrap(),
            )
        });

        let mut seqs = [r1, r2, r3];
        seqs.sort_unstable();
        assert_eq!(seqs, [1, 2, 3]);
        assert_eq!(engine.core.combiner.queued_len(), 0);
        assert_eq!(
            engine.core.resources.write_groups.load(Ordering::Relaxed),
            2
        );
        assert_eq!(
            engine.core.resources.grouped_writes.load(Ordering::Relaxed),
            3
        );
        assert_eq!(ks.get(b"l0").unwrap().as_deref(), Some(big_a.as_slice()));
        assert_eq!(ks.get(b"f1").unwrap().as_deref(), Some(big_b.as_slice()));
        assert_eq!(ks.get(b"f2").unwrap().as_deref(), Some(&b"tiny"[..]));
        engine.close().unwrap();
    }
}
