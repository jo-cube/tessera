# Tessera

Tessera aims to become a RAM-first, compressed LSM key-value database written
in Rust.

## Status: Milestone 5E

Two physical storage layers exist, with mutable storage above them.

A **block** stores versioned records, ordered by user key ascending then
sequence number descending, with prefix-compressed keys and a restart point
every 16 records. `BlockBuilder` encodes records and `Block` validates bytes
once, then provides allocation-free newest-version lookup and an ordered cursor
over every stored version. `BlockView` opens bytes already known to be valid in
constant time, and `BlockPosition` walks a block without borrowing it.

A **run** is a self-contained sequence of independently compressed blocks in
contiguous bytes with a sparse index of each block's last user key.
`RunBuilder` consumes records in internal-key order, cuts blocks near 16 KiB of
uncompressed data by default, keeps ordinary version chains whole and stores them
uncompressed or with LZ4 or Zstandard (level 3 is the provisional default). Reads take a reusable `ReadWorkspace`:
`Run::get` finds the newest version of a key, including tombstones, by decoding
at most one block. `Run::cursor` iterates across blocks; `RunCursor::seek` moves
to the first key at or above a target and `RunCursor::seek_prefix` iterates only
while keys share a prefix, both yielding every stored version. `Run::new`
validates untrusted bytes completely. `RunView` offers the same reads on bytes
already known to be valid. Each run stores its smallest key once, beside the index,
and reuses the last block's index key as its largest, so `key_range`
reports both bounds without decoding anything. `Run::new` checks them against
the records. A run its builder made also carries an in-memory Bloom filter
over its distinct user keys (10 bits each, about 1% false positives), so a
lookup of a key it lacks usually decodes nothing; the filter is not part of the
bytes, and a run from `Run::new` has none and decodes as before. The engine's
point reads use the filters; scans do not. `EngineOptions::max_run_bytes`
limits stored and filter bytes together, as does a compaction's
`max_output_bytes`, and `EngineStats` reports them apart.

`MergeCursor` merges several runs, which may overlap and use different codecs
and block sizes, into one stream in internal-key order, with the same `seek` and
`seek_prefix`. It yields every physical record: all versions, tombstones, and
every copy of an internal key found in more than one run, in input order. Only
`MergeWorkspace` holds its state, one decoded block per input, and returned
records borrow from it, so iteration copies no values and allocates nothing.
An input is not decoded until the merge reaches its smallest key.

`RunSet` reads a fixed collection of runs as the latest state of each key:
`get` for one key, and a cursor with `seek` and `seek_prefix` for live keys in
order. The newest sequence number decides a key, and a tombstone hides it.
Identical copies of a record count once. Copies that differ fail the read
with `ReadError::ConflictingCopies`, because run order must not choose a winner.
A run set has no snapshots of its own: it reads the newest version only.
Key ranges exclude runs that cannot hold a key or prefix, without decoding them.

The byte formats and their invariants are documented in
[`src/block/mod.rs`](src/block/mod.rs) and [`src/run/mod.rs`](src/run/mod.rs).

A **memtable** holds the latest update of each key in memory, in key order,
under a memory budget. It is a copy-on-write B+ tree whose nodes and updates
are shared by reference count, so `Memtable::snapshot` returns the current
tree as a `FrozenMemtable` in constant time and later writes copy only the
nodes on their path that a snapshot still shares. What they copy or replace is
charged to the table's budget as `pinned` until the snapshot is dropped. The
caller supplies strictly increasing sequence numbers and a rejected update
changes nothing. Freezing consumes the table, and a `FrozenMemtable` builds
compressed runs on demand, one at a time, keeping every tombstone. A frozen
table is only borrowed by a flush, so a flush can be repeated.

An **`Engine`** combines them: `put`, `delete`, `get`, ordered and prefix scans,
and an explicit synchronous `flush`. It assigns sequence numbers and holds one
active memtable, frozen memtables awaiting flush and the published runs behind
a single reader-writer lock, with a gate that keeps readers from starving
writers. Single-key reads and writes are linearizable; `get` holds the lock
only to search the memtables and take the run list, and decodes blocks after
releasing it. A `ReadGuard` gives a
stable view for a few reads and **blocks writers while held**. A flush freezes
the active table, compresses outside the lock, and publishes its runs while
removing the frozen tables in one step; an abandoned or rejected flush leaves
the tables readable for a retry. Memtable budgets and limits on run count and
bytes bound what is kept.

**Snapshots** give long scans a stable view without the lock.
`Engine::snapshot` takes, under the exclusive lock and in constant time per
source, a memtable snapshot of the active table and shared references to the
frozen tables and published runs. Nothing is copied, frozen or flushed for it.
It sees exactly the writes up to its `last_seq` while writes, flushes and
compactions continue, can be sent between threads and may outlive the engine.
Flushes and compactions still keep only the latest version of each key: a
snapshot keeps the sources it saw instead of asking the engine for older
versions, and sources the engine releases stay alive until the last snapshot
holding them is dropped. The history a snapshot pins in a memtable counts
against that memtable's budget, so a long-lived snapshot brings flushes
forward instead of growing memory. `SnapshotLimits` bound live snapshots and
retained bytes; over the retained limit, new snapshots and compactions are
refused, which is the backpressure. `EngineStats` reports what snapshots
retain, as an upper estimate, and how much of it is runs.

**Compaction** is explicit and synchronous. `compact` merges runs into new
disjoint runs that keep only the newest version of each key, collapse identical
copies, fail on copies that differ at any version, and drop a tombstone only
when the caller says no older value can lie outside the merge. Workspace and
output are limited and checked, the workspace before anything is decoded.
`Engine::compact` selects inputs under the shared lock, merges without the lock,
and replaces exactly its inputs in one step, after checking they are still
published; runs flushed meanwhile and the memtables are newer and stay. The
policy merges the smallest runs covering the most deeply overlapped key once
more than `max_overlap` runs share a key, taking a larger run only once the
smaller ones taken add up to it, and merges neighbouring short runs
once there are many disjoint runs. It is not leveled.

With `EngineOptions::background_compaction` set, the engine owns one worker
thread that runs the same compaction whenever a flush publishes runs. Requests
made while it is busy coalesce into one pass, it sleeps when nothing is
eligible, and a failed compaction is reported in `background_status` and not
retried until the next flush, or the next flush refused by the run limits.
`wait_for_compaction` asks for a pass and waits
for its outcome; dropping the engine finishes the compaction in progress and
joins the thread.

**Checkpoints** are explicit and opt-in; the engine never touches a file
otherwise. `Engine::checkpoint(dir, options)` takes a snapshot, streams its
live keys into runs of the ordinary format one at a time, syncs every file,
then commits by atomically replacing a small `CURRENT` file and syncing the
directory, and removes the previous checkpoint. Writes, flushes and
compaction continue meanwhile. `Engine::recover(dir, options)` validates the
committed checkpoint completely, rebuilds the key filters and continues the
sequence after its ceiling; a missing or damaged checkpoint is an error,
never an empty engine. The format, publication protocol and filesystem
assumptions (local Linux filesystems) are in
[`src/engine/checkpoint.rs`](src/engine/checkpoint.rs).

**Durable mode** is opt-in too. `Engine::create(dir, ..)` starts a database
and `Engine::open(dir, ..)` reopens one; such an engine holds the directory's
lock and appends every write to a write-ahead log, synced before the write is
applied, visible or acknowledged. Concurrent writers share syncs through
bounded group commit: the first writer in the queue leads a batch of those
behind it (at most `max_batch_records` and `max_batch_bytes`), appends it,
syncs once, publishes it in sequence order and wakes its members; a lone
writer never waits for company. Queued writes are bounded by
`max_queue_bytes`. The sync happens outside the state lock, so reads
continue meanwhile. A failed append or sync is reported to every write
of its batch, none of which is applied, and closes the log until the
database is reopened. `durable_checkpoint`
commits a checkpoint while writes continue and then removes the log segments
it covers. `open` loads the committed checkpoint and replays the log after
it, checking every record; only an interrupted final append is tolerated
(and truncated), and any other damage is an error. Engines from
`Engine::new` and `recover` stay RAM-only and write no file. The record
format, write protocol and recovery rules are in
[`src/engine/wal.rs`](src/engine/wal.rs).

There is no transaction, sequence-filtered read of the runs, persisted filter,
snapshot of a state older than the call, weaker durability mode or
parallel log writer, and flushing never happens in the background.

## Validate

Validation runs offline in a Linux container with hard limits, using a Colima
VM (2 CPUs, 4 GiB) and an image built once from a pinned Rust base:

```sh
colima start
scripts/validate.sh image    # once, adds rustfmt and Clippy (uses the network)
scripts/validate.sh deps     # once, fetches pinned crates (uses the network)
scripts/validate.sh verify   # prove the limits are enforced
scripts/validate.sh          # fmt, Clippy, tests and docs
scripts/validate.sh full     # also the large-allocation boundary tests
scripts/validate.sh bench    # block and run size, timing and allocations
scripts/validate.sh geometry # run block size and codec comparison
scripts/validate.sh prefix   # prefix scan comparison
scripts/validate.sh merge    # multi-run merge measurements
scripts/validate.sh logical  # latest-state read measurements
scripts/validate.sh ranges   # key-range pruning measurements
scripts/validate.sh memtable # memtable and flush measurements
scripts/validate.sh engine   # integrated engine measurements
scripts/validate.sh compaction # sustained writes: none, sync and background compaction
scripts/validate.sh snapshot # snapshots compared with read guards
scripts/validate.sh frequency # frequent and long-lived snapshots
scripts/validate.sh retention # sustained snapshot retention and backpressure
scripts/validate.sh persistent # memtable tree costs under held snapshots
scripts/validate.sh pacing   # background compaction under frequent snapshots
scripts/validate.sh profile  # memtable and engine write cost by component
scripts/validate.sh policy   # compaction selection cost across arrival rates
scripts/validate.sh reads    # read amplification under sustained writes
scripts/validate.sh bloom    # filtered against unfiltered point lookups
scripts/validate.sh filtercost # key filter construction cost
scripts/validate.sh operate  # sustained, concurrent and limited operation
scripts/validate.sh checkpoint # checkpoint and recovery cost
scripts/validate.sh durable  # durable writes and log recovery
colima stop
```

Benchmark method, reproduction steps and findings are in
[`docs/benchmarks.md`](docs/benchmarks.md).

Each step gets a 3 GiB cgroup with no swap, 2 CPUs, 256 processes, 2 build jobs,
2 test threads, a 512 MiB tmpfs at `/tmp` for checkpoint tests, and a
timeout. Use `scripts/validate.sh exec SECONDS CMD...` for
anything else that could loop or allocate without bound, including tests of
deliberately broken code. A bare `cargo test` on macOS has no memory limit.

## License

MIT
