# Tessera

Tessera is a RAM-first, compressed LSM key-value store, written in Rust as an
embedded library.

It suits an application that keeps a byte-keyed dataset in its own process
and wants it compressed in memory: point lookups and ordered or prefix scans
that never touch disk, single-key writes, consistent snapshots, and
optionally checkpoints and a write-ahead log for restarts. The dataset must
fit in memory once compressed.

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

## Using it

Everything public lives in `tessera::engine`:

```rust
use tessera::engine::{DurableOptions, Engine, EngineOptions, EngineWorkspace};

let dir = std::env::temp_dir().join("tessera-readme");
let engine = Engine::create(&dir, EngineOptions::default(), DurableOptions::default())?;
engine.put(b"user/1", b"ada")?;
engine.put(b"user/2", b"grace")?;
engine.delete(b"user/1")?;

// One workspace per reading thread, reused across reads.
let mut workspace = EngineWorkspace::new();
assert!(engine.get(b"user/1", &mut workspace)?.is_none());

// A snapshot holds no lock; writes, flushes and compactions continue.
let snapshot = engine.snapshot()?;
let mut scan = snapshot.scan_prefix(b"user/", &mut workspace);
while let Some(record) = scan.next_record()? {
    println!("{:?} = {:?}", record.key, record.value);
}

// Bound the log: checkpoint, then drop the segments it covers.
engine.durable_checkpoint(Default::default())?;
```

`Engine::new(options)` makes a RAM-only engine with the same reads and
writes. Writes never need the caller to flush or retry for room; see
**Flushing** under [Engine](#engine).

`EngineOptions::compression` chooses how runs store their blocks:
`Compression::Zstd` (the default, densest), `Lz4`, or `Uncompressed`. A
lookup that reaches a run costs about 8 µs, 2 µs or 0.5 µs respectively,
and JSON-like data takes about 1.7× (LZ4) or 3.8× (uncompressed) the
Zstandard memory. Blocks record their codec, so the setting can change
between restarts: existing runs and checkpoints stay readable, and
compaction rewrites runs with the current setting.

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

**Flushing** is automatic. A write that finds the active memtable full
waits for any flush in progress, freezes the table if it is still full,
lands in the new one and flushes the frozen table before returning, so
writers that run out of room together share one flush. A flush compresses
outside the lock and publishes its runs in one step; a refused flush leaves
its tables frozen and readable, and the next flush takes them with the
active table. `Engine::flush` flushes everything written before the call,
which is only needed to bound what the memtables hold or to measure a
flush; it is not needed before a checkpoint, which includes the memtables.
If the run limits refuse the flush, writes that need room fail with
`UpdateError::FlushRefused` until compaction or `set_run_limits` makes room;
the refusal is kept, not rebuilt for every write.

**Compaction** merges runs into disjoint runs that keep only the newest
version of each key. `Engine::compact` performs one compaction, or returns
`None` when its policy finds nothing to do: it selects inputs under the
shared lock, merges without it and replaces exactly its inputs, so reads,
writes and flushes continue. The policy merges the smallest runs covering
the most deeply overlapped key, and neighbouring short runs once there are
many disjoint ones. It is not leveled. Calling `compact` until it returns
`None` with `max_overlap: 1` consolidates the runs so that a lookup reads at
most one. With `EngineOptions::background_compaction` set, one worker thread
compacts after each flush; `wait_for_compaction` waits for a pass and
`background_status` reports failures.

**Durable writes** are committed by bounded group commit: the first writer
in the queue leads a batch of those behind it, appends it, syncs once and
publishes it in sequence order; a lone writer never waits for company. The
sync happens outside the state lock, so reads continue meanwhile. If the
memtable has no room for a write, the leader logs the writes before it,
freezes the table, commits the rest, and builds the flush after handing on
leadership, so nothing is logged for a write that is then refused. A failed
append or sync fails every write of its batch, whose outcome is then
uncertain until the database is reopened, and closes the log.
`Engine::inspect` reads a database as `open` would, changing nothing, and
reports how far the log is valid, where it is damaged and what `open` would
do. Nothing is ever repaired automatically.

## Storage layers

These are internal: the engine's contracts above are the supported surface.
A **block** stores versioned records, ordered by user key ascending then
sequence number descending, with prefix-compressed keys and a restart point
every 16 records. A **run** is a self-contained sequence of independently
compressed blocks (uncompressed, LZ4 or Zstandard) with a sparse index of each
block's last key and its key range; a point lookup decodes at most one
block. Runs the engine builds carry an in-memory Bloom filter (10 bits a
key), so lookups of absent keys usually decode nothing; filters are rebuilt
on recovery, not stored. Reads merge the memtables and overlapping runs into
the latest state of each key.

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

- A refused write (`UpdateError`) was not applied and used no sequence
  number. `Rejected` means the options can never admit it (key, value or
  record too large); `FlushRefused` and `LogFull` are backpressure, cleared by
  compaction or a durable checkpoint, after which the write can be retried;
  `LogFailed`, `LogClosed` and `SequencesExhausted` are terminal for the open
  engine.
- Writes wait for flushes when the memtables are full, and are refused with
  `FlushRefused` when the run limits are reached, until compaction makes
  room; with no background compaction, call `compact`. A caller that wants
  to wait can call `wait_for_compaction` or `compact` and retry.
- A durable log grows until `durable_checkpoint` is called; writes past
  `max_log_bytes` are refused with `LogFull`. Checkpoints are never taken
  automatically, and cannot prune the active segment, so `max_log_bytes`
  must exceed `segment_bytes` (creating or opening checks this) and leave
  room beside a full segment for the largest record.
- A failed log append or sync is terminal for the open engine: that batch's
  writes return `LogFailed` (their outcome is uncertain until reopening) and
  every later durable write `LogClosed`. Reopen the database to continue.
  Reads keep working.
- While snapshots retain more than `SnapshotLimits::max_retained_bytes`, new
  snapshots and compactions are refused, so runs accumulate until flushes,
  and then writes, are refused. Dropping snapshots lifts this.
- A `ReadGuard` blocks writers, flushes and compaction publication while
  held; keep it short. Do not call engine methods while holding one; use the
  guard's own reads. A `Snapshot` holds no lock.
- A panic in the background worker stops it; `background_status` reports it
  and the engine keeps working without it.
- Dropping the engine is shutdown: it waits for the worker's current
  compaction, writes nothing and releases the directory lock. A RAM-only
  engine's data goes with it unless checkpointed; a durable engine's
  acknowledged writes are already in the log, and a `durable_checkpoint`
  before dropping only shortens the next `open`.
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

## Platform

- Built and tested only with Rust 1.98.1 (edition 2024) on Linux aarch64, in
  the container described under [Validate](#validate). No minimum supported
  Rust version is declared yet. Other targets, including x86-64, macOS and
  Windows, are untested.
- RAM-only engines touch no files and need no platform support beyond
  threads. Checkpoints and the log need a filesystem with the guarantees under
  [Durability](#durability), and use advisory file locks (`File::try_lock`).
- Zstandard is C code: `zstd-safe` builds the bundled libzstd 1.5.7
  (`vendored`), so building needs a C compiler but no system library. LZ4
  and CRC-32 are pure Rust. `Cargo.lock` pins every dependency; build with
  `--locked`.
- `unsafe` is forbidden in the crate itself.

## Performance

Measured in a 2-CPU, 4 GiB Linux VM, not on characterised hardware, with the
benchmarks in `examples/bench.rs`. RAM-only
writes run at about 1.2–1.7 million a second from 1–4 writers. One durable
writer on the VM's virtual disk manages about 6,000–8,000 writes a second
(one sync each, p50 about 125 µs); group commit reaches about 30,000–36,000
with 8 writers, with about 0.2 syncs a write. Recovery replays the log at
about 270–290 MiB a second. A point lookup that reaches a run is dominated
by decoding one 16 KiB block (about 8 µs under Zstandard; see
`EngineOptions::compression`). Writes that flush for themselves measured level
with caller-driven flushing, and several writers share one flush per full
table. Methods, workloads and full results are in
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
scripts/validate.sh bench    # the benchmark suite, or e.g. `bench durable`
```

The benchmarks, `examples/bench.rs`, use only the public API and keep their
files on tmpfs.

Each step gets a 3 GiB cgroup with no swap, 2 CPUs, 256 processes, 2 build jobs,
2 test threads, a 512 MiB tmpfs at `/tmp` for test databases, and a timeout,
and reports its peak memory and block-device writes. Use
`scripts/validate.sh exec SECONDS CMD...` for anything else that could loop or
allocate without bound, including tests of deliberately broken code. A bare
`cargo test` on macOS has no memory limit.

## License

MIT
