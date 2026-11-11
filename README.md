# Tessera

Tessera is a RAM-first, compressed LSM key-value store, written in Rust as an
embedded library.

## Status

Prerelease and unpublished; not production-ready. The engine, its
checkpoints and its write-ahead log are implemented and tested, and their
durability rests on filesystem assumptions that have not been verified under
real power loss (see [Durability](#durability)). The API may still change.

## Modes

Every engine keeps its whole dataset in memory: memtables and compressed
runs. Persistence is opt-in and explicit.

- **RAM-only**, the default: `Engine::new(options)`. No file is ever read or
  written.
- **Checkpoints**: `Engine::checkpoint(dir, options)` writes one consistent
  snapshot of the state to a directory while writes continue, and
  `Engine::recover(dir, options)` restores it into a new RAM-only engine.
  Writes after the checkpoint are not kept.
- **Durable**: `Engine::create(dir, ..)` starts a database and
  `Engine::open(dir, ..)` reopens one. Every write is appended to a
  write-ahead log and synced before it becomes visible or is acknowledged,
  and `open` recovers every acknowledged write from the latest checkpoint and
  the log. `durable_checkpoint` commits a checkpoint into the database and
  removes the log segments it covers.

A directory is used by one engine at a time, through an advisory lock.
`recover` refuses a durable database and `open` a checkpoint-only directory,
so the guarantees of the two never mix.

## Engine

`put` and `delete` return the write's sequence number. `get` returns a key's
latest value. Ordered and prefix scans read through a `ReadGuard`, which
holds the state lock and **blocks writers while held**, or through a
`Snapshot`, which holds no lock. Single-key reads and writes are
linearizable; there are no multi-key transactions.

`get` holds the lock only to search the memtables and take the run list, and
decodes blocks after releasing it. A writer waiting for the lock holds back
new readers, so readers cannot starve it.

**Snapshots.** `Engine::snapshot` takes, in constant time per source, a
copy-on-write snapshot of the active memtable and shared references to the
frozen tables and runs. It sees exactly the writes up to its `last_seq`
while writes, flushes and compactions continue, can be sent between threads
and may outlive the engine. Flushes and compactions keep only the latest
version of each key; sources the engine releases stay alive until the last
snapshot holding them is dropped. History a snapshot pins in a memtable
counts against that memtable's budget, and `SnapshotLimits` bound live
snapshots and retained bytes.

**Flushing** is explicit. When the active memtable is full a write is
refused with `WriteError::BudgetExceeded`, and the caller flushes and
retries. `flush` freezes the active table, compresses it into runs outside
the lock, and publishes them in one step; a refused or abandoned flush leaves
the tables readable for a retry.

**Compaction** merges runs into disjoint runs that keep only the newest
version of each key. `Engine::compact` selects inputs under the shared lock,
merges without it and replaces exactly its inputs; the policy merges the
smallest runs covering the most deeply overlapped key, and neighbouring
short runs once there are many disjoint ones. It is not leveled. With
`EngineOptions::background_compaction` set, one worker thread compacts after
each flush; `wait_for_compaction` waits for a pass and `background_status`
reports failures.

**Durable writes** are committed by bounded group commit: the first writer
in the queue leads a batch of those behind it, appends it, syncs once and
publishes it in sequence order; a lone writer never waits for company. The
sync happens outside the state lock, so reads continue meanwhile. A failed
append or sync fails every write of its batch, whose outcome is then
uncertain until the database is reopened, and closes the log.
`Engine::inspect` reads a database as `open` would, changing nothing, and
reports how far the log is valid, where it is damaged and what `open` would
do. Nothing is ever repaired automatically.

## Storage layers

A **block** stores versioned records, ordered by user key ascending then
sequence number descending, with prefix-compressed keys and a restart point
every 16 records. A **run** is a self-contained sequence of independently
compressed blocks (uncompressed, LZ4 or Zstandard) with a sparse index of each
block's last key and its key range; a point lookup decodes at most one
block. Runs the engine builds carry an in-memory Bloom filter (10 bits a
key), so lookups of absent keys usually decode nothing; filters are rebuilt
on recovery, not stored. `MergeCursor` merges overlapping runs and `RunSet`
reads a set of runs as the latest state of each key.

A **memtable** holds the latest update of each key in a copy-on-write B+
tree under a memory budget, so snapshots are constant-time.

The byte formats are documented in [`src/block/mod.rs`](src/block/mod.rs)
and [`src/run/mod.rs`](src/run/mod.rs), the checkpoint format and protocol
in [`src/engine/checkpoint.rs`](src/engine/checkpoint.rs), and the log format,
write protocol and recovery rules in [`src/engine/wal.rs`](src/engine/wal.rs).

## Resource limits

All limits are admission checks on logical byte counts and estimates, not a
bound on process memory:

- memtables: two budgets in all (`memtable_budget`), including history that
  snapshots pin;
- runs: a count and a byte total of stored bytes and filters
  (`max_runs`, `max_run_bytes`); a flush beyond them is refused;
- snapshots: live count and retained bytes (`SnapshotLimits`), beyond which
  snapshots and compactions are refused;
- compaction: input, output and workspace (`CompactionOptions`);
- checkpoints: bytes on disk (`CheckpointOptions::max_bytes`);
- the log: bytes on disk (`max_log_bytes`), beyond which writes are refused
  with `LogFull` until a durable checkpoint, and queued and batched writes
  (`max_queue_bytes`, `max_batch_records`, `max_batch_bytes`).

Recovery checks each file's length against what it may hold before reading
it.

## Operating requirements and limitations

- Callers flush: nothing flushes on its own, and writes are refused once the
  memtables are full.
- A durable log grows until `durable_checkpoint` is called.
- Do not call engine methods while holding a `ReadGuard`; use the guard's own
  reads.
- The dataset must fit in memory: there is no disk tiering.
- There are no transactions, no reads at an older sequence number than a
  snapshot's, no persisted filters and no parallel log writers.

## Durability

The supported environment is a local Linux filesystem such as ext4 or XFS,
where `fsync` makes data durable, a directory sync makes entries durable and
`rename` is atomic. Network filesystems and devices that acknowledge `fsync`
without persisting are unsupported; macOS is not part of the tested
contract. Tests cover killed processes, failed writes and syncs, interrupted
checkpoints and rotations, damage and a file-level model of lost unsynced
data. Real power loss, and how a device orders the pages of one write, have
not been tested. After a failed sync, check the device before reopening.

## Performance

Measured in a 2-CPU, 4 GiB Linux VM, not on characterised hardware. RAM-only
writes run at about 1.2–1.7 million a second from 1–4 writers. One durable
writer on the VM's virtual disk manages about 6,000–8,000 writes a second
(one sync each, p50 about 125 µs); group commit reaches about 30,000–36,000
with 8 writers, with about 0.2 syncs a write. Recovery replays the log at
about 270–290 MiB a second. Methods, workloads and full results are in
[`docs/benchmarks.md`](docs/benchmarks.md).

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
scripts/validate.sh durable  # durable writes and log recovery, on tmpfs
```

The script's header lists every benchmark. The `checkpoint-disk` and
`durable-disk` modes write to the VM's disk, and so to the host's SSD; the
others use tmpfs.

Each step gets a 3 GiB cgroup with no swap, 2 CPUs, 256 processes, 2 build jobs,
2 test threads, a 512 MiB tmpfs at `/tmp` for test databases, and a timeout,
and reports its peak memory and block-device writes. Use
`scripts/validate.sh exec SECONDS CMD...` for anything else that could loop or
allocate without bound, including tests of deliberately broken code. A bare
`cargo test` on macOS has no memory limit.

## License

MIT
