//! Group commit: concurrent durable writes share one append and one sync.
//!
//! # Ownership
//!
//! Each durable write encodes its record on its own thread (copying its key
//! and value once, and checksumming everything after the sequence number),
//! is admitted in arrival order, and joins a FIFO queue. The writer at the front of the queue, when no
//! batch is in progress, becomes the **leader**: it takes a batch from the
//! front (at most [`DurableOptions::max_batch_records`] writes and
//! [`DurableOptions::max_batch_bytes`], but always its own), commits it, and
//! hands every other member its outcome. Others wait on one condition
//! variable; the next front writer leads the next batch. There is no thread,
//! timer or delay of its own: a write that arrives while a batch syncs waits
//! for the next one, so batches form only under concurrency, and a lone
//! writer commits at once, alone.
//!
//! # Commit
//!
//! The leader holds the log for the whole batch, so batches are serialized
//! and so are pruning and log status. For the batch, in queue order:
//!
//! 1. **Validate and reserve**, under the state lock: each write is given
//!    the next sequence number only if the active memtable admits it beside
//!    the earlier writes of the batch (their full charges are reserved), and
//!    if the log has room for it. A refused write gets its error and no
//!    number, so the accepted ones still have consecutive numbers. The
//!    first write the memtables have no room for ends the batch's
//!    validation: it and the writes after it are deferred, unlogged.
//! 2. **Log**, without the state lock: the accepted records get their
//!    numbers and checksums and are appended with vector writes and synced
//!    once (twice if the batch starts a segment: the old segment is synced
//!    before the new one is created).
//! 3. **Publish**, under the state lock again: the writes are applied in
//!    order and the last sequence number published, in one hold of the lock,
//!    so no reader or snapshot sees a later write without an earlier one.
//! 4. **Acknowledge**: each member gets its sequence number, after the
//!    publication.
//!
//! If writes were deferred, the leader, holding the log no longer, makes room
//! (see "Automatic flushing" in `engine.rs`): it waits for any flush in
//! progress, freezes the active table, and commits the deferred
//! writes again, in order, as the rest of its batch. It builds and publishes
//! the flush only after finishing the batch, so the next batch commits into
//! the new table meanwhile, and its own write returns after the flush, which
//! is the backpressure. If the new table still has no room (tables were
//! already frozen, or the batch outgrows a table), it publishes the flush
//! first and retries. A published flush leaves an empty table of a whole
//! budget, so each round after one commits at least one write, and the
//! rounds are bounded by the batch. If a round after a flush accepts
//! nothing, the deferred writes fail, having logged nothing: with
//! [`UpdateError::FlushRefused`] if the run limits refused it. While the
//! leader holds the flush lock it takes the log and state locks; nothing
//! takes the flush lock while holding either, so the order is flush, log,
//! state.
//!
//! So a write is visible only after its record is synced, acknowledged only
//! after it is visible, a refused write logs nothing and uses no number, and
//! a logged write is never left unapplied. Writes in a batch are separate
//! single-key writes, not a transaction: they become visible together only
//! because one lock hold publishes them.
//!
//! A flush may freeze the table checked, and snapshots may pin more of it,
//! between steps 1 and 3; the batch is then applied regardless of the budget,
//! and the active table may exceed it by that batch. The next check admits a
//! write only if it brings the table back within the budget, so the excess
//! does not accumulate.
//!
//! # Failure
//!
//! If the append, a rotation or the sync fails, every accepted write of the
//! batch gets [`UpdateError::LogFailed`] and none is applied, though any of
//! their records may survive (a prefix of the batch, possibly with a torn
//! last record). The log closes: queued and later writes get
//! [`UpdateError::LogClosed`], and all waiters are woken. Nothing is retried.
//! Recovery then returns every acknowledged write and possibly a prefix of
//! that final batch, which was never acknowledged. If a leader panics, a
//! guard closes the log the same way: the other members of its batch get
//! `LogFailed` too (their records may have been logged), and the queue gets
//! `LogClosed`. An engine cannot be dropped while a write is queued, since
//! writes borrow it; a process that dies leaves the outcome of its last
//! batch to recovery.
//!
//! # Resources
//!
//! Admission is strictly in arrival order. A write is charged its encoded
//! length plus [`REQUEST_OVERHEAD`] before it encodes, and admitted once the
//! queued and admitted writes leave room for it under
//! [`DurableOptions::max_queue_bytes`]; a single larger write is admitted
//! alone, once the queue is empty, so it always makes progress and never
//! shares the bound. No later write is admitted ahead of a waiting one, so
//! a large write is not starved by a stream of small ones. Lengths are
//! checked before anything is allocated. A writer encodes its record before
//! admission, which is usually immediate; one that must wait drops it first
//! and encodes again once admitted, so a waiting writer holds nothing beyond
//! its caller's key and value.
//!
//! A batch the leader has taken leaves the queue's count, so the memory of
//! encoded records is at most `max_queue_bytes` queued plus one batch in
//! flight (`max_batch_bytes`), with one larger write in place of either. The
//! overhead covers a request's slot in the queue (about 100 bytes) or its
//! outcome until taken; the queue's and the outcome table's capacity stays
//! at its peak, at most about twice the slots. These are logical counts and
//! a conservative estimate of what the queue owns, not process memory.

use std::collections::{HashMap, VecDeque};
use std::ops::Range;
use std::sync::{MutexGuard, PoisonError};

use crate::memtable::ENTRY_OVERHEAD;

use super::wal::{Durable, Unsealed, encode, encoded_len};
use super::{Core, FlushJob, RunLimitError, UpdateError};
use super::{Refusal, WriteError};
use crate::block::{MAX_KEY_LEN, MAX_VALUE_LEN, Value};

/// Bytes charged to the queue per write beside its record.
pub(super) const REQUEST_OVERHEAD: usize = 128;

const _: () = assert!(std::mem::size_of::<Request>() <= REQUEST_OVERHEAD);

type Outcome = Result<u64, UpdateError>;

/// One write waiting to be committed.
struct Request {
    ticket: u64,
    /// What it holds against [`DurableOptions::max_queue_bytes`].
    cost: usize,
    write: Unsealed,
}

impl Request {
    /// What it reserves of the memtable's budget, before it is known whether
    /// it replaces a value.
    fn charge(&self) -> usize {
        self.write.key.len() + self.write.value.as_ref().map_or(0, Range::len) + ENTRY_OVERHEAD
    }
}

/// The queue of durable writes, behind [`Durable::group`].
#[derive(Default)]
pub(super) struct Group {
    queue: VecDeque<Request>,
    /// The cost of `queue` and of writes admitted but still encoding.
    queued: usize,
    next_ticket: u64,
    /// Admission order: the next writer to arrive takes `arrivals`, and the
    /// one holding `admitted` is admitted next.
    arrivals: u64,
    admitted: u64,
    /// Whether a leader is committing a batch.
    leading: bool,
    /// Set when the log closes; nothing more is queued.
    closed: bool,
    /// Outcomes of finished batches, until their writers take them.
    outcomes: HashMap<u64, Outcome>,
    /// Writers waiting to be admitted.
    waiting: usize,
    /// Writers asleep on [`Durable::turn`]; nobody is woken while there are
    /// none, which spares a lone writer the system calls.
    sleepers: usize,
}

impl Group {
    /// The batch at the front of the queue.
    fn take(&mut self, options: &super::DurableOptions) -> Vec<Request> {
        let records = options.max_batch_records;
        let mut batch = Vec::new();
        let mut bytes = 0;
        while let Some(front) = self.queue.front() {
            let len = front.write.record.len();
            if batch.len() == records
                || (!batch.is_empty() && bytes + len > options.max_batch_bytes)
            {
                break;
            }
            bytes += len;
            let request = self.queue.pop_front().unwrap();
            self.queued -= request.cost;
            batch.push(request);
        }
        batch
    }

    /// Closes the log for writers: every queued write fails, and so does
    /// every write admitted or waiting.
    fn close(&mut self) {
        self.closed = true;
        for request in self.queue.drain(..) {
            self.outcomes
                .insert(request.ticket, Err(UpdateError::LogClosed));
        }
        self.queued = 0;
    }
}

impl Durable {
    fn lock_group(&self) -> MutexGuard<'_, Group> {
        // The group's invariants hold between its short critical sections;
        // a leader's panic is handled by `Leading`.
        self.group.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn wait<'g>(&self, mut group: MutexGuard<'g, Group>) -> MutexGuard<'g, Group> {
        group.sleepers += 1;
        let mut group = self
            .turn
            .wait(group)
            .unwrap_or_else(PoisonError::into_inner);
        group.sleepers -= 1;
        group
    }

    /// Releases the group and wakes its sleepers, if any. Anyone not yet
    /// asleep checks the group under its lock before sleeping.
    fn wake(&self, group: MutexGuard<'_, Group>) {
        let sleepers = group.sleepers;
        drop(group);
        if sleepers > 0 {
            self.turn.notify_all();
        }
    }

    /// Queued writes and their cost.
    pub(super) fn queue_load(&self) -> (usize, usize) {
        let group = self.lock_group();
        (group.queue.len(), group.queued)
    }

    /// Queued writes and writers waiting to be admitted, for tests.
    #[cfg(test)]
    pub(super) fn queued(&self) -> (usize, usize) {
        let group = self.lock_group();
        (group.queue.len(), group.waiting)
    }
}

/// A leader's batch. Dropped without [`Self::finish`], which happens only
/// when the leader panics, it closes the log and wakes everyone.
struct Leading<'d> {
    durable: &'d Durable,
    tickets: Range<u64>,
    mine: u64,
    finished: bool,
}

impl Leading<'_> {
    fn finish(mut self, outcomes: Vec<(u64, Outcome)>, closed: bool) -> Outcome {
        self.finished = true;
        let mut mine = Err(UpdateError::LogClosed);
        let mut group = self.durable.lock_group();
        group.leading = false;
        for (ticket, outcome) in outcomes {
            if ticket == self.mine {
                mine = outcome;
            } else {
                group.outcomes.insert(ticket, outcome);
            }
        }
        if closed {
            group.close();
        }
        self.durable.wake(group);
        mine
    }
}

impl Drop for Leading<'_> {
    fn drop(&mut self) {
        if self.finished {
            return;
        }
        let mut group = self.durable.lock_group();
        group.leading = false;
        // Their records may have been logged before the panic: uncertain,
        // like a failed sync's.
        let uncertain = UpdateError::LogFailed(std::io::ErrorKind::Other);
        for ticket in self.tickets.clone().filter(|&ticket| ticket != self.mine) {
            group.outcomes.insert(ticket, Err(uncertain));
        }
        group.close();
        self.durable.wake(group);
    }
}

impl Core {
    /// A durable write: queue, then wait for its batch's outcome or lead it.
    // Kept out of the RAM-only write path, which only tests for it.
    #[inline(never)]
    pub(super) fn update_durable(
        &self,
        durable: &Durable,
        key: &[u8],
        value: Value<'_>,
    ) -> Result<u64, UpdateError> {
        let options = &durable.options;
        // Refused before anything is allocated for it; the memtable checks
        // again at reservation.
        let value_len = match value {
            Value::Bytes(bytes) => bytes.len(),
            Value::Tombstone => 0,
        };
        if key.len() > MAX_KEY_LEN {
            return Err(UpdateError::Rejected(WriteError::KeyTooLong {
                len: key.len(),
            }));
        }
        if value_len > MAX_VALUE_LEN {
            return Err(UpdateError::Rejected(WriteError::ValueTooLong {
                len: value_len,
            }));
        }
        let cost = encoded_len(key, value) + REQUEST_OVERHEAD;
        // Encoded before admission, which is usually immediate. A writer that
        // must wait drops it, outside the lock, and encodes again once
        // admitted.
        let mut write = Some(encode(key, value));
        let mut group = durable.lock_group();
        let arrival = group.arrivals;
        group.arrivals += 1;
        loop {
            if group.closed {
                return Err(UpdateError::LogClosed);
            }
            if group.admitted == arrival
                && (group.queued == 0 || group.queued + cost <= options.max_queue_bytes)
            {
                break;
            }
            if let Some(unadmitted) = write.take() {
                drop(group);
                drop(unadmitted);
                group = durable.lock_group();
                continue;
            }
            group.waiting += 1;
            group = durable.wait(group);
            group.waiting -= 1;
        }
        group.admitted += 1;
        group.queued += cost;
        // The next arrival may fit too.
        if group.waiting > 0 {
            durable.turn.notify_all();
        }
        let write = match write {
            Some(write) => write,
            None => {
                drop(group);
                let write = encode(key, value);
                group = durable.lock_group();
                if group.closed {
                    return Err(UpdateError::LogClosed);
                }
                write
            }
        };
        let ticket = group.next_ticket;
        group.next_ticket += 1;
        group.queue.push_back(Request {
            ticket,
            cost,
            write,
        });
        loop {
            if let Some(outcome) = group.outcomes.remove(&ticket) {
                return outcome;
            }
            if !group.leading
                && group
                    .queue
                    .front()
                    .is_some_and(|front| front.ticket == ticket)
            {
                break;
            }
            group = durable.wait(group);
        }
        group.leading = true;
        let batch = group.take(options);
        // Room in the queue for writers waiting to join it.
        durable.wake(group);
        let leading = Leading {
            durable,
            tickets: ticket..batch.last().map_or(ticket, |last| last.ticket) + 1,
            mine: ticket,
            finished: false,
        };
        let (outcomes, closed, flush) = self.commit_making_room(durable, batch);
        let outcome = leading.finish(outcomes, closed);
        // Built after the next batch may start; a refusal is remembered.
        if let Some(job) = flush {
            let _ = job.build().publish();
        }
        outcome
    }

    /// Commits a batch, making room whenever the memtables defer the rest of
    /// it. Returns each member's outcome, whether the log is now closed, and
    /// a flush of tables frozen for room, for the leader to publish.
    fn commit_making_room(
        &self,
        durable: &Durable,
        mut batch: Vec<Request>,
    ) -> (Vec<(u64, Outcome)>, bool, Option<FlushJob<'_>>) {
        let mut outcomes = Vec::with_capacity(batch.len());
        // Tables frozen for room and not yet flushed: freezing is what makes
        // room, so the flush is left until the batch is committed.
        let mut frozen: Option<FlushJob<'_>> = None;
        // The refusal of the flush published since a write was last accepted.
        let mut refused: Option<RunLimitError> = None;
        loop {
            let round = self.commit(durable, batch, &mut outcomes);
            if round.closed || round.deferred.is_empty() {
                return (outcomes, round.closed, frozen);
            }
            batch = round.deferred;
            if round.accepted {
                refused = None;
            }
            // Freezing made too little room: earlier tables were frozen.
            if let Some(job) = frozen.take() {
                refused = job.build().publish().err();
                continue;
            }
            // Writes accepted since the last flush may have filled the new
            // table, so a successful flush is followed by another.
            let refused = match refused {
                Some(refused) => refused,
                None => match self.freeze_for_room() {
                    Ok(Some(job)) => {
                        frozen = Some(job);
                        continue;
                    }
                    Ok(None) => {
                        unreachable!("an empty memtable has room for any record within its budget")
                    }
                    Err(refused) => refused,
                },
            };
            let error = UpdateError::FlushRefused(refused);
            outcomes.extend(batch.iter().map(|request| (request.ticket, Err(error))));
            return (outcomes, false, frozen);
        }
    }

    /// Validates, logs and publishes the batch up to the first write the
    /// memtables have no room for, holding the log throughout, and adds the
    /// outcomes of the others to `outcomes`.
    fn commit(
        &self,
        durable: &Durable,
        mut batch: Vec<Request>,
        outcomes: &mut Vec<(u64, Outcome)>,
    ) -> Round {
        let closed = |batch: Vec<Request>, outcomes: &mut Vec<(u64, Outcome)>| {
            outcomes.extend(
                batch
                    .iter()
                    .map(|request| (request.ticket, Err(UpdateError::LogClosed))),
            );
            Round::closed()
        };
        let Ok(mut wal) = durable.wal.lock() else {
            return closed(batch, outcomes);
        };
        if wal.closed.is_some() {
            return closed(batch, outcomes);
        }

        // 1. Validate and reserve.
        let mut plan = wal.plan();
        // The member, its sequence number and the base of a segment to start
        // before it.
        let mut accepted: Vec<(usize, u64, Option<u64>)> = Vec::with_capacity(batch.len());
        let mut full = None;
        {
            let mut state = self.write_state();
            let (mut last, mut reserved) = (state.last_seq, 0usize);
            for (index, request) in batch.iter().enumerate() {
                let write = &request.write;
                let admitted = (|| -> Result<_, Refusal> {
                    let seq = last.checked_add(1).ok_or(UpdateError::SequencesExhausted)?;
                    state
                        .active
                        .check(write.key(), seq, write.value(), reserved)
                        .map_err(|error| self.refusal(error))?;
                    let rotate = wal.admit(&mut plan, write.record.len() as u64)?;
                    Ok((seq, rotate))
                })();
                match admitted {
                    Ok((seq, rotate)) => {
                        accepted.push((index, seq, rotate.then(|| seq - 1)));
                        last = seq;
                        reserved += request.charge();
                    }
                    Err(Refusal::Full) => {
                        full = Some(index);
                        break;
                    }
                    Err(Refusal::Error(error)) => outcomes.push((request.ticket, Err(error))),
                }
            }
        }
        // Unlogged, for the next round.
        let deferred = full.map_or_else(Vec::new, |index| batch.split_off(index));
        let round = |any_accepted: bool| Round {
            closed: false,
            accepted: any_accepted,
            deferred,
        };
        if accepted.is_empty() {
            return round(false);
        }

        // 2. Log.
        for &(index, seq, _) in &accepted {
            batch[index].write.seal(seq);
        }
        let records: Vec<(&[u8], Option<u64>)> = accepted
            .iter()
            .map(|&(index, _, rotate)| (batch[index].write.record.as_slice(), rotate))
            .collect();
        let fault = durable
            .fault
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take();
        if let Err(error) = wal.write_batch(&records, fault) {
            let kind = error.kind();
            wal.closed = Some(kind);
            outcomes.extend(
                accepted
                    .iter()
                    .map(|&(index, ..)| (batch[index].ticket, Err(UpdateError::LogFailed(kind)))),
            );
            // Nothing deferred was logged, but the log is closed now.
            let mut round = round(true);
            outcomes.extend(
                std::mem::take(&mut round.deferred)
                    .iter()
                    .map(|request| (request.ticket, Err(UpdateError::LogClosed))),
            );
            round.closed = true;
            return round;
        }
        drop(records);
        #[cfg(test)]
        if let Some(hook) = super::wal::BEFORE_PUBLISH.with(|hook| hook.take()) {
            hook(self);
        }

        // 3. Publish. A flush may have frozen the table checked, whose
        // successor continues after the last published number; snapshots may
        // have pinned more. The batch is logged, so it goes in regardless of
        // the budget.
        {
            let mut state = self.write_state();
            for &(index, seq, _) in &accepted {
                let write = &batch[index].write;
                state
                    .active
                    .write_admitted(write.key(), seq, write.value())
                    .expect("lengths and the reserved sequence numbers were checked");
            }
            state.last_seq = accepted.last().unwrap().1;
        }
        drop(wal);
        outcomes.extend(
            accepted
                .iter()
                .map(|&(index, seq, _)| (batch[index].ticket, Ok(seq))),
        );
        round(true)
    }
}

/// What one [`Core::commit`] left to do.
struct Round {
    /// Whether the log is closed.
    closed: bool,
    /// Whether any write was accepted.
    accepted: bool,
    /// Writes from the first the memtables had no room for, unlogged.
    deferred: Vec<Request>,
}

impl Round {
    fn closed() -> Self {
        Self {
            closed: true,
            accepted: false,
            deferred: Vec::new(),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::PathBuf;
    use std::sync::mpsc;
    use std::thread;
    use std::time::Duration;

    use super::*;
    use crate::block::MAX_KEY_LEN;
    use crate::engine::wal::{BEFORE_PUBLISH, Fault};
    use crate::engine::{DurableOptions, Engine, EngineOptions, EngineWorkspace};

    /// The defaults without the background worker: these tests flush and
    /// compact by hand and count what each step leaves.
    fn manual() -> EngineOptions {
        EngineOptions {
            background_compaction: None,
            ..EngineOptions::default()
        }
    }

    fn scratch(name: &str) -> PathBuf {
        let path =
            std::env::temp_dir().join(format!("tessera-group-{name}-{}", std::process::id()));
        let _ = fs::remove_dir_all(&path);
        path
    }

    fn get(engine: &Engine, key: &[u8]) -> Option<Vec<u8>> {
        let mut workspace = EngineWorkspace::new();
        engine
            .get(key, &mut workspace)
            .unwrap()
            .map(|found| found.value.to_vec())
    }

    fn durable(engine: &Engine) -> &Durable {
        engine.core.durable.get().unwrap()
    }

    /// Waits until `engine`'s queue holds `queued` writes and `waiting`
    /// writers wait to join it.
    fn until_queued(engine: &Engine, queued: usize, waiting: usize) {
        for _ in 0..10_000 {
            if durable(engine).queued() == (queued, waiting) {
                return;
            }
            thread::sleep(Duration::from_millis(1));
        }
        panic!("queue is {:?}", durable(engine).queued());
    }

    /// Runs `body` while a first write (`b"leader"`) is logged but held before
    /// publication, so that writes started in `body` queue behind it; `body`
    /// returns once they have, and the leader is released.
    fn behind_a_held_leader<'e, T>(
        engine: &'e Engine,
        scope: &'e thread::Scope<'e, '_>,
        hook: impl FnOnce(&Core) + Send + 'static,
        body: impl FnOnce() -> T,
    ) -> (thread::ScopedJoinHandle<'e, Result<u64, UpdateError>>, T) {
        let (logged, wait_logged) = mpsc::channel();
        let (go, wait_go) = mpsc::channel::<()>();
        let leader = scope.spawn(move || {
            BEFORE_PUBLISH.with(|cell| {
                cell.set(Some(Box::new(move |core: &Core| {
                    logged.send(()).unwrap();
                    wait_go.recv().unwrap();
                    hook(core);
                })))
            });
            engine.put(b"leader", b"first")
        });
        wait_logged.recv().unwrap();
        // Logged, not yet visible.
        assert_eq!(get(engine, b"leader"), None);
        assert_eq!(engine.stats().last_seq, 0);
        let out = body();
        go.send(()).unwrap();
        (leader, out)
    }

    /// Writers queued while a batch syncs form the next batch, which takes
    /// one sync (two when it starts a segment), consecutive sequence numbers
    /// in queue order, and becomes visible at once.
    #[test]
    fn queued_writers_share_one_sync_in_queue_order() {
        for (segment_bytes, syncs) in [(64 << 20, 2), (1024, 3)] {
            let dir = scratch(&format!("share-{segment_bytes}"));
            let options = DurableOptions {
                segment_bytes,
                ..DurableOptions::default()
            };
            let engine = Engine::create(&dir, manual(), options).unwrap();
            let seqs = thread::scope(|scope| {
                let (leader, writers) = behind_a_held_leader(
                    &engine,
                    scope,
                    |_| {},
                    || {
                        (0..4u8)
                            .map(|at| {
                                let engine = &engine;
                                let writer =
                                    scope.spawn(move || engine.put(&[at], &[at; 300]).unwrap());
                                until_queued(engine, at as usize + 1, 0);
                                writer
                            })
                            .collect::<Vec<_>>()
                    },
                );
                let first = leader.join().unwrap().unwrap();
                let rest: Vec<u64> = writers.into_iter().map(|w| w.join().unwrap()).collect();
                (first, rest)
            });
            assert_eq!(seqs, (1, vec![2, 3, 4, 5]));
            let status = engine.log_status().unwrap();
            assert_eq!(
                (status.batches, status.records, status.syncs),
                (2, 5, syncs),
                "{status:?}"
            );
            assert_eq!(status.batch_sizes[..3], [1, 0, 1]);
            for at in 0..4u8 {
                assert_eq!(get(&engine, &[at]), Some(vec![at; 300]));
            }
            drop(engine);
            let (engine, report) = Engine::open(&dir, manual(), options).unwrap();
            assert_eq!((report.last_seq, report.replayed), (5, 5));
            assert_eq!(get(&engine, &[3]), Some(vec![3; 300]));
            drop(engine);
            fs::remove_dir_all(&dir).unwrap();
        }
    }

    /// In a batch, a write the memtable refuses for good gets its error and
    /// no sequence number. One that has no room beside the earlier writes the
    /// batch reserved room for is deferred, unlogged, with the writes after
    /// it: the leader commits the earlier ones, flushes, and commits the
    /// deferred ones in order. Every accepted write is numbered without a gap.
    #[test]
    fn refused_writes_in_a_batch_use_no_sequence_number() {
        let dir = scratch("refused");
        let options = EngineOptions {
            memtable_budget: 16 << 10,
            ..manual()
        };
        let engine = Engine::create(&dir, options, DurableOptions::default()).unwrap();
        let writes: Vec<(Vec<u8>, usize)> = vec![
            (b"a".to_vec(), 6 << 10),
            // Larger than any table of this budget.
            (b"z".to_vec(), 20 << 10),
            (b"b".to_vec(), 6 << 10),
            // Fits the table alone, not beside the two reserved before it.
            (b"c".to_vec(), 6 << 10),
            (b"d".to_vec(), 10),
        ];
        let outcomes = thread::scope(|scope| {
            let (leader, writers) = behind_a_held_leader(
                &engine,
                scope,
                |_| {},
                || {
                    writes
                        .iter()
                        .enumerate()
                        .map(|(at, (key, len))| {
                            let engine = &engine;
                            let writer = scope.spawn(move || engine.put(key, &vec![7; *len]));
                            until_queued(engine, at + 1, 0);
                            writer
                        })
                        .collect::<Vec<_>>()
                },
            );
            assert_eq!(leader.join().unwrap(), Ok(1));
            writers
                .into_iter()
                .map(|w| w.join().unwrap())
                .collect::<Vec<_>>()
        });
        assert_eq!(outcomes[0], Ok(2));
        assert!(matches!(
            outcomes[1],
            Err(UpdateError::Rejected(
                WriteError::RecordExceedsBudget { .. }
            ))
        ));
        assert_eq!(outcomes[2..], [Ok(3), Ok(4), Ok(5)]);
        let stats = engine.stats();
        assert_eq!((stats.runs, stats.frozen_tables), (1, 0));
        assert!(stats.active.charged <= stats.active.budget, "{stats:?}");
        let status = engine.log_status().unwrap();
        // The batch of five took two appends, one before the flush.
        assert_eq!((status.records, status.batches), (5, 3), "{status:?}");
        for (key, len) in &writes {
            let expected = (*len < 16 << 10).then(|| vec![7; *len]);
            assert_eq!(
                get(&engine, key).map(|v| v.len()),
                expected.map(|v| v.len()),
                "{key:?}"
            );
        }
        // Lengths are checked before a write is queued or encoded.
        assert!(matches!(
            engine.put(&vec![0; MAX_KEY_LEN + 1], b""),
            Err(UpdateError::Rejected(WriteError::KeyTooLong { .. }))
        ));
        assert_eq!(engine.stats().last_seq, 5);
        drop(engine);
        let (engine, report) = Engine::open(&dir, options, DurableOptions::default()).unwrap();
        assert_eq!(report.last_seq, 5);
        assert_eq!(get(&engine, b"c"), Some(vec![7; 6 << 10]));
        assert_eq!(get(&engine, b"z"), None);
        drop(engine);
        fs::remove_dir_all(&dir).unwrap();
    }

    /// A batch larger than two tables fills the table that replaced the
    /// frozen one before that flush publishes, so the leader flushes again
    /// rather than giving up.
    #[test]
    fn a_batch_larger_than_two_tables_flushes_again() {
        let dir = scratch("twice");
        let options = EngineOptions {
            memtable_budget: 16 << 10,
            ..manual()
        };
        let engine = Engine::create(&dir, options, DurableOptions::default()).unwrap();
        let outcomes = thread::scope(|scope| {
            let (leader, writers) = behind_a_held_leader(
                &engine,
                scope,
                |_| {},
                || {
                    (0..8u8)
                        .map(|at| {
                            let engine = &engine;
                            let writer = scope.spawn(move || engine.put(&[at], &[at; 6 << 10]));
                            until_queued(engine, at as usize + 1, 0);
                            writer
                        })
                        .collect::<Vec<_>>()
                },
            );
            assert_eq!(leader.join().unwrap(), Ok(1));
            writers
                .into_iter()
                .map(|w| w.join().unwrap())
                .collect::<Vec<_>>()
        });
        assert_eq!(outcomes, (2..10).map(Ok).collect::<Vec<_>>());
        assert!(engine.stats().flushes >= 2, "{:?}", engine.stats());
        for at in 0..8u8 {
            assert_eq!(get(&engine, &[at]), Some(vec![at; 6 << 10]));
        }
        drop(engine);
        fs::remove_dir_all(&dir).unwrap();
    }

    /// A batch whose append or sync fails fails every member, closes the log
    /// and wakes everyone, writers waiting for room in the queue included;
    /// recovery has the acknowledged writes and the failed batch's records
    /// only as far as they reached the file.
    #[test]
    fn a_failed_batch_fails_its_members_and_wakes_every_waiter() {
        for (fault, survives) in [(Fault::TornAppend, false), (Fault::SyncFails, true)] {
            let dir = scratch(&format!("failed-{fault:?}"));
            // Room for two queued writes.
            let options = DurableOptions {
                max_queue_bytes: 2 * (300 + 200),
                ..DurableOptions::default()
            };
            let engine = Engine::create(&dir, manual(), options).unwrap();
            let outcomes = thread::scope(|scope| {
                let (leader, writers) = behind_a_held_leader(
                    &engine,
                    scope,
                    |_| {},
                    || {
                        let writers: Vec<_> = (0..4u8)
                            .map(|at| {
                                let engine = &engine;
                                let writer = scope.spawn(move || engine.put(&[at], &[at; 300]));
                                let (queued, waiting) =
                                    if at < 2 { (at + 1, 0) } else { (2, at - 1) };
                                until_queued(engine, queued as usize, waiting as usize);
                                writer
                            })
                            .collect();
                        *durable(&engine).fault.lock().unwrap() = Some(fault);
                        writers
                    },
                );
                assert_eq!(leader.join().unwrap(), Ok(1));
                writers
                    .into_iter()
                    .map(|w| w.join().unwrap())
                    .collect::<Vec<_>>()
            });
            // The first two formed the failed batch; the others queued after
            // it was taken and were refused when the log closed.
            assert!(matches!(outcomes[0], Err(UpdateError::LogFailed(_))));
            assert!(matches!(outcomes[1], Err(UpdateError::LogFailed(_))));
            assert!(
                outcomes[2..].iter().all(|outcome| matches!(
                    outcome,
                    Err(UpdateError::LogFailed(_)) | Err(UpdateError::LogClosed)
                )),
                "{outcomes:?}"
            );
            assert_eq!(engine.put(b"later", b""), Err(UpdateError::LogClosed));
            assert_eq!(get(&engine, &[0]), None);
            assert_eq!(engine.stats().last_seq, 1);
            drop(engine);
            let (engine, report) = Engine::open(&dir, manual(), options).unwrap();
            let failed = outcomes
                .iter()
                .filter(|outcome| matches!(outcome, Err(UpdateError::LogFailed(_))))
                .count() as u64;
            assert_eq!(
                report.last_seq,
                if survives { 1 + failed } else { 1 },
                "{fault:?}"
            );
            assert_eq!(get(&engine, b"leader"), Some(b"first".to_vec()));
            drop(engine);
            fs::remove_dir_all(&dir).unwrap();
        }
    }

    /// Admission is in arrival order: a write larger than the queue bound
    /// waits for the queue to empty and is then admitted alone, and a later
    /// small write that would fit waits behind it instead of passing it.
    #[test]
    fn a_large_write_is_admitted_in_turn_and_not_passed() {
        let dir = scratch("large");
        // Room for two small writes (330 encoded bytes and the overhead each).
        let options = DurableOptions {
            max_queue_bytes: 1000,
            ..DurableOptions::default()
        };
        let engine = Engine::create(&dir, manual(), options).unwrap();
        let outcomes = thread::scope(|scope| {
            let (leader, writers) = behind_a_held_leader(
                &engine,
                scope,
                |_| {},
                || {
                    let engine = &engine;
                    let small = scope.spawn(move || engine.put(b"a", &[1; 300]));
                    until_queued(engine, 1, 0);
                    let large = scope.spawn(move || engine.put(b"b", &[2; 4000]));
                    until_queued(engine, 1, 1);
                    let later = scope.spawn(move || engine.put(b"c", &[3; 300]));
                    until_queued(engine, 1, 2);
                    [small, large, later]
                },
            );
            assert_eq!(leader.join().unwrap(), Ok(1));
            writers.map(|writer| writer.join().unwrap())
        });
        assert_eq!(outcomes, [Ok(2), Ok(3), Ok(4)]);
        assert_eq!(get(&engine, b"b"), Some(vec![2; 4000]));
        assert_eq!(engine.log_status().unwrap().queued_bytes, 0);
        drop(engine);
        fs::remove_dir_all(&dir).unwrap();
    }

    /// A leader that panics after logging closes the log and wakes everyone:
    /// the other members of its batch get `LogFailed`, since their records
    /// were logged and are recovered, and the writes queued behind it get
    /// `LogClosed` and are not. The engine stays readable.
    #[test]
    fn a_panicking_leader_closes_the_log_and_wakes_everyone() {
        let dir = scratch("panic");
        let options = DurableOptions {
            max_batch_records: 2,
            ..DurableOptions::default()
        };
        let engine = Engine::create(&dir, manual(), options).unwrap();
        thread::scope(|scope| {
            let (leader, (panicking, writers)) = behind_a_held_leader(
                &engine,
                scope,
                |_| {},
                || {
                    let engine = &engine;
                    // Leads the next batch, with the write after it.
                    let panicking = scope.spawn(move || {
                        BEFORE_PUBLISH.with(|cell| {
                            cell.set(Some(Box::new(|_: &Core| {
                                panic!("a leader panics before publishing")
                            })))
                        });
                        engine.put(&[0], b"x")
                    });
                    until_queued(engine, 1, 0);
                    let writers: Vec<_> = (1..3u8)
                        .map(|at| {
                            let writer = scope.spawn(move || engine.put(&[at], b"x"));
                            until_queued(engine, at as usize + 1, 0);
                            writer
                        })
                        .collect();
                    (panicking, writers)
                },
            );
            assert_eq!(leader.join().unwrap(), Ok(1));
            assert!(panicking.join().is_err());
            let outcomes: Vec<_> = writers.into_iter().map(|w| w.join().unwrap()).collect();
            assert_eq!(
                outcomes,
                [
                    Err(UpdateError::LogFailed(std::io::ErrorKind::Other)),
                    Err(UpdateError::LogClosed)
                ]
            );
        });
        assert_eq!(engine.put(b"later", b""), Err(UpdateError::LogClosed));
        assert_eq!(get(&engine, b"leader"), Some(b"first".to_vec()));
        assert_eq!(get(&engine, &[1]), None);
        drop(engine);
        let (engine, report) = Engine::open(&dir, manual(), DurableOptions::default()).unwrap();
        assert_eq!(report.last_seq, 3);
        assert_eq!(get(&engine, &[0]), Some(b"x".to_vec()));
        assert_eq!(get(&engine, &[1]), Some(b"x".to_vec()));
        assert_eq!(get(&engine, &[2]), None);
        drop(engine);
        fs::remove_dir_all(&dir).unwrap();
    }
}
