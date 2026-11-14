# Tessera

Tessera is a RAM-first, compressed LSM key-value store, written in Rust as an
embedded library.

It suits an application that keeps a byte-keyed dataset in its own process
and wants it compressed in memory: point lookups and ordered or prefix scans
that never touch disk, single-key writes, consistent snapshots, and
optionally checkpoints and a write-ahead log for restarts. The dataset must
fit in memory once compressed.

## Status

Private alpha (`0.1.0-alpha.1`), unpublished and not production-ready. The
engine, its checkpoints and its write-ahead log are implemented and tested
on Linux aarch64 only (see [Platform](#platform)), and their durability
rests on filesystem assumptions that have not been verified under real power
loss (see [Durability](#durability)). The API may still change before a
stable release. Release steps are in [`docs/release.md`](docs/release.md).

## Modes

Every engine keeps its whole dataset in memory: memtables and compressed
runs. Persistence is opt-in and explicit.

- **RAM-only**, the default: `Engine::new(options)`. No file is ever read or
  written, and every write is lost when the engine is closed or dropped or
  the process ends.
- **Checkpoints**: `Engine::checkpoint(dir, options)` writes one consistent
  snapshot of the state to a directory while writes continue, and
  `Engine::recover(dir, options)` restores it into a new RAM-only engine.
  Writes after the latest checkpoint are lost. Checkpoints are never taken
  automatically.
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

Tessera is not on crates.io. Depend on it by path or by Git revision:

```toml
[dependencies]
tessera = { git = "https://github.com/jo-cube/tessera", rev = "<commit>" }
```

Everything public lives in `tessera::engine`:

```rust
use tessera::engine::{CheckpointOptions, DurableOptions, Engine, EngineOptions, EngineWorkspace};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let dir = std::env::temp_dir().join(format!("tessera-readme-{}", std::process::id()));
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
    drop(scan);

    // Bound the log: checkpoint, then drop the segments it covers.
    engine.durable_checkpoint(CheckpointOptions::default())?;
    // Stop the background worker and release the directory lock.
    engine.close()?;

    std::fs::remove_dir_all(&dir)?;
    Ok(())
}
```

`Engine::new(options)` makes a RAM-only engine with the same reads and
writes. Writes never need the caller to flush or retry for room; see
**Flushing** under [Engine](#engine).

## Configuration

Every constructor (`new`, `create`, `open`, `recover`) checks all options
before it allocates, starts a thread or touches a file, and returns a
`ConfigError` naming the setting, its value and the accepted range (inside
`OpenError::Config` for the persistent ones). No value is clamped or
adjusted. `compact` and `set_run_limits` check their arguments the same
way.

The defaults suit general use:

```rust
use tessera::engine::{Engine, EngineOptions};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let engine = Engine::new(EngineOptions::default())?;
    engine.close()?;
    Ok(())
}
```

That is 64 MiB memtables, 4 GiB of runs (with a safety valve of 65,536
runs), leveled background compaction with `CompactionOptions::default()`
(32 inputs a job, level 0 compacted at 4 flushes),
16 snapshots and 1 GiB retained by them, and plain Zstandard in 16 KiB
blocks, which favours memory density and longer scans.

For structured values such as JSON, where point reads set the latency,
4 KiB blocks with dictionary compression made lookups that reach a run
2.4–3.5 times faster, with about 10% less run memory, in the measurements
in `docs/benchmarks.md`:

```rust
use tessera::engine::{Compression, Engine, EngineOptions};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let engine = Engine::new(EngineOptions {
        compression: Compression::ZstdDictionary,
        target_block_len: 4 << 10,
        ..EngineOptions::default()
    })?;
    engine.close()?;
    Ok(())
}
```

Ranges, all inclusive:

| setting | accepted |
|---|---|
| `memtable_budget` | `ENTRY_OVERHEAD` (an empty record's charge) to `usize::MAX / 2` |
| `target_run_len` | 1 byte to 1 GiB |
| `max_runs`, `max_run_bytes` | at least 1 |
| `target_block_len` | 1 byte (a block per record) to 64 KiB |
| `CompactionOptions::max_inputs` | at least 2 |
| `CompactionOptions::max_input_bytes`, `max_output_bytes` | at least twice `target_run_len` |
| `CompactionOptions::max_workspace_bytes` | at least the estimate for two inputs of `target_block_len` blocks (about 3.1 MiB at 16 KiB) |
| `CompactionOptions::max_overlap` | at least 1 |
| `SnapshotLimits::max_live`, `max_retained_bytes` | any; 0 refuses snapshots |
| `DurableOptions::segment_bytes`, `max_batch_bytes`, `max_queue_bytes` | at least 1 |
| `DurableOptions::max_log_bytes` | more than `segment_bytes` plus a segment header |
| `DurableOptions::max_batch_records` | 1 to 256 |

These are the values the engine accepts and handles correctly, not
recommendations: a 1-byte block length is safe but stores each record as
its own block, at about 20 bytes of overhead each. The measured choices
for block length are 4 KiB and 16 KiB.

Valid options can still meet limits at run time, and the operation they
stop reports it: run limits and the level-0 stop refuse flushes
(`UpdateError::FlushRefused`), the log limit refuses writes
(`UpdateError::LogFull`), snapshot limits refuse snapshots and
compactions, and compaction limits refuse a job
(`CompactionError::JobLimit`, only for runs longer than `target_run_len`,
such as a single record longer than it, or blocks longer than
`target_block_len`). None of these is a configuration error.

**Settings that changed meaning with leveled compaction.** `max_runs` was
the capacity ceiling (64 by default) and is now a safety valve on run
metadata (65,536 by default, one per 64 KiB of the default
`max_run_bytes`; scale it with `max_run_bytes`). `max_run_bytes` is the
physical limit. `CompactionOptions::max_overlap` bounded every key's
overlap and now is the number of flushes at which level 0 is compacted
(same default, 4; 1 compacts every flush). `disjoint_runs_above` was
removed: two small adjacent runs are merged whatever their number.
`max_inputs` defaults to 32 instead of 8. `max_input_bytes` and
`max_output_bytes` count held bytes (filters and dictionaries) as well as
stored bytes and must admit two full runs.

**Compression and blocks.** `compression` (`Zstd`, `Lz4`, `Uncompressed` or
`ZstdDictionary`) and `target_block_len` decide only how new runs are
built. Every run records its own codec, block lengths and dictionary, so
runs and checkpoints written under other settings are read and recovered
as they are, and take the current settings only when a compaction or
checkpoint rewrites them. Settings may change freely between restarts.
Smaller blocks make a read of a run cheaper and cost memory and long-scan
speed; LZ4 and `Uncompressed` trade memory for cheaper reads.

**Dictionaries.** With `ZstdDictionary`, flushes stay plain Zstandard;
each compaction and checkpoint that rewrites at least about 6.4 MiB trains
a 16 KiB dictionary from its input and uses it only if it saves at least
10% on held-out blocks, and only if the compaction workspace leaves room
for training (the default does). It is accepted with any block length and
the requested length is kept. It was measured only at 4 KiB; at any length
the 10% check decides, per rewrite, whether the output uses the dictionary.
Small binary records gain nothing and stay plain. Dictionary runs use a newer run format, which older builds reject.

**Background compaction** is on by default: one worker thread compacts
after each flush, keeping the levels within their sizes without any call
from the application. Setting `background_compaction: None` makes
compaction explicit; then the application must call `Engine::compact`, or
runs accumulate until flushes, and the writes that need them, are refused
with `FlushRefused`.

**Persistence** depends only on the constructor: `new` and `recover` make
RAM-only engines, and `create` and `open` durable ones, which additionally
take `DurableOptions`. The engine options mean the same in every mode.
`last_seq` applies to `new` and `create`; `open` takes the sequence from the
checkpoint and log, and `recover` continues after the larger.

## Lifecycle

```rust
use tessera::engine::{DurableOptions, Engine, EngineOptions, OpenError};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let dir = std::env::temp_dir().join(format!("tessera-lifecycle-{}", std::process::id()));
    let (options, durable) = (EngineOptions::default(), DurableOptions::default());
    let engine = match Engine::open(&dir, options, durable) {
        Ok((engine, _report)) => engine,
        // Never an empty database in place of a missing one: create it explicitly.
        Err(OpenError::NoCheckpoint(_)) => Engine::create(&dir, options, durable)?,
        Err(OpenError::Config(error)) => panic!("fix the options: {error}"),
        Err(error) => return Err(error.into()),
    };
    engine.put(b"k", b"v")?;
    // Before exiting: stop the worker and release the directory lock.
    engine.close()?;

    std::fs::remove_dir_all(&dir)?;
    Ok(())
}
```

Constructors fail with `OpenError` before anything is exposed: `Config` for
refused options, `Worker` if the worker thread cannot be created, and for
`create`, `open` and `recover` the directory, lock, log and checkpoint
errors. A failed `open` or `recover` changes nothing on disk and releases
the lock.

`close(self)` shuts the engine down: the worker finishes and publishes the
compaction it is running, if any, starts no other, and is joined; then the
engine's memory and directory lock are freed. It consumes the engine, so no
operation can race it or follow it; an engine shared through an `Arc` is
closed by its last owner (`Arc::into_inner`). Snapshots stay readable
afterwards. `close` fails only if the worker had panicked, with the panic's
message, and releases everything regardless. Dropping the engine does the
same and discards that result.

Closing is not a durability step. It writes, syncs and checkpoints
nothing: a RAM-only or recovered engine's writes since its last checkpoint
are gone, and a durable engine's acknowledged writes are in its log, as
after a crash. Call `checkpoint` or `durable_checkpoint` first if that is
wanted.

## Engine

`put` and `delete` return the write's sequence number. `get` returns a key's
latest value. Ordered and prefix scans read through a `ReadGuard`, which
holds the state lock and **blocks writers while held**, or through a
`Snapshot`, which holds no lock. Single-key reads and writes are
linearizable; there are no multi-key transactions.

`get` holds the lock only to search the memtables and take the run list, and
decodes blocks after releasing it. A writer waiting for the lock holds back
new readers, so readers cannot starve it.

**Read guards and snapshots.** `Engine::read` returns a `ReadGuard` for a
few reads that must see one state: it copies nothing, but writers, flushes
and compaction publication wait until it is dropped, and calling the engine
from the thread holding it can deadlock. A `Snapshot` is for anything
longer, such as a scan while writes continue: it holds no lock, but keeps
whatever the engine releases from its view alive until it is dropped. Prefer
a snapshot unless the guard's few reads are all there is.

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
A flush also ends its runs at level 1's run boundaries, in pieces of at
least an eighth of a run length and at most eight per run length, so that
later level-0 compactions stay range-local. If the run limits refuse the
flush, writes that need room fail with `UpdateError::FlushRefused` until
compaction or `set_run_limits` makes room; the refusal is kept, not rebuilt
for every write.

**Compaction** is leveled, with a size ratio of 4. Level 0 holds recent
flushes, which may overlap; every deeper level is sorted and its runs are
disjoint, and each is a quarter the size of the next, sized from the last.
Levels exist only in memory as compaction's bookkeeping: reads never
consult them, nothing about them is persisted, and recovery puts a
checkpoint's runs in one level. `Engine::compact` takes one step, or
returns `None` when nothing is due: it merges the next run of the level
most over its size into the level below, or, once no deeper level is over
its size, level 0's oldest run (with the other level-0 runs it overlaps)
into level 1; it relabels a run that
overlaps nothing below (a move, writing nothing), splits a run whose job
exceeds the limits into pieces whose jobs fit, or merges two small
adjacent runs. It chooses from level sizes and key ranges only, merges
without the lock and replaces exactly its inputs, so reads, writes and
flushes continue. Afterwards a lookup visits at most one run per level and
one per level-0 flush. The background worker (on by default) compacts after
each flush; `wait_for_compaction` waits for a pass and `background_status`
reports failures. A job is formed only if its inputs leave a thirty-second
of `max_output_bytes` free for what its output runs add. Output can still
outgrow that (after reopening with another codec, say): a job that fails
at the output limit is not chosen again as it was; jobs from its source
take half as much each time, and are refused with `OutputLimit` before
reading anything once none fits.
`docs/capacity-design.md` describes the design, and its
section 16 what was built and measured: at 200,000 writes and 64 KiB runs
(a reduced scale), write amplification fell 1.4–3.7× against the old
selector without its run limit, at up to 1.37× space amplification on
overwrites.

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
block. A run that compaction or a checkpoint built under
`Compression::ZstdDictionary` also stores one 16 KiB Zstandard dictionary
that all its blocks share (format version 4; other runs stay version 3),
and holds it prepared for decoding beside its bytes. Runs the engine builds
carry an in-memory Bloom filter (10 bits a key), so lookups of absent keys usually decode nothing; filters are rebuilt
on recovery, not stored. Reads merge the memtables and overlapping runs into
the latest state of each key. Each publication of runs also publishes an
immutable **run directory**: the runs partitioned into the fewest groups of
strictly key-disjoint runs, as many as the deepest overlap. A point lookup
visits the groups newest first, binary-searches each for the one run that
could hold the key, and stops once no group left can hold a newer version
than one found: an updated or deleted key usually decodes one block however
many older runs hold it. A scan merges one input per group, each reading
its runs in key order through one decoded block, so its memory grows with
the overlap rather than the run count. Snapshots keep the directory they
were taken with.

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
- runs: a byte total of stored bytes and filters (`max_run_bytes`), the
  physical limit, and a count (`max_runs`), a safety valve; a flush beyond
  either is refused; with background compaction, one that would leave
  level 0 with more than 12 flushes first waits for one compaction pass,
  then is refused if level 0 still has no room; a compaction may pass the
  limits only by shrinking what is published;
- snapshots: live count and retained bytes (`SnapshotLimits`), beyond which
  snapshots and compactions are refused;
- compaction: inputs, input and output bytes, and workspace
  (`CompactionOptions`), each checked against a job before it reads
  anything;
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
- Writes wait for flushes when the memtables are full: a flush runs on the
  writer that needs room, so that write stalls for it. When level 0 is at
  its stop (compaction behind), that flush also waits for the background
  worker, without holding locks that readers, fitting writes or compaction
  need, until a compaction step leaves room or one pass ends. Only a pass
  that was refused ends without room, so the flush and the write are then
  refused with `FlushRefused`, whose `RunLimitError::background` says why
  that pass made no room: snapshots retaining too much (recovers once they
  are dropped, on the next write needing room), a job over the
  `CompactionOptions` limits (raise them), or a panicked worker (call
  `compact`). `background_status` reports the latest pass, which may be a
  later one. Writes at the run limits are refused
  without waiting. With background compaction turned off nothing waits:
  call `compact`.
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
  snapshots and compactions (other than moves, which write nothing) are
  refused, so flushes accumulate in level 0 until its stop refuses flushes,
  and then writes. Dropping snapshots lifts this: the next write that needs
  room waits for a compaction pass that drains level 0.
- A `ReadGuard` blocks writers, flushes and compaction publication while
  held; keep it short. Do not call engine methods while holding one; use the
  guard's own reads. A `Snapshot` holds no lock.
- A panic in the background worker stops it for good; `background_status`
  and `wait_for_compaction` report it with its message, `close` returns it,
  and the engine keeps working without the worker.
- How much data fits is decided by `max_run_bytes`, which is memory, and
  by how much of the data is overwritten or deleted versions not yet
  compacted away (space amplification, about 1.3 under uniform
  overwrites in reduced-scale measurements). Once `max_run_bytes` holds
  only live data, flushes and then writes, deletes included, stay refused
  until `set_run_limits` raises it. The run count is no longer a capacity
  limit. Refusals that recover by themselves: the level-0 stop (as
  compaction drains level 0), `Retained` (once snapshots are dropped), and
  `max_run_bytes` while compaction can still reclaim versions. Refusals that
  need action: `max_run_bytes` full of live data, `max_runs` if reached,
  and `CompactionError::JobLimit` (raise the compaction limits).
- Every checkpoint rewrites all live data, so at large sizes checkpoints,
  not compaction, dominate the bytes written. Reduced-scale measurements do
  not establish behaviour at hundreds of millions of records.
- The dataset must fit in memory: there is no disk tiering.
- There are no transactions, no reads at an older sequence number than a
  snapshot's, no persisted filters and no parallel log writers.

## Durability

What survives, by mode:

| mode | survives a crash or exit | lost |
|---|---|---|
| RAM-only (`new`) | nothing | every write |
| checkpoints (`checkpoint`, `recover`) | the latest committed checkpoint | every write after it |
| durable (`create`, `open`) | every acknowledged write | at most the writes whose call had not returned or failed with `LogFailed` |

A durable write is acknowledged, its sequence number returned, only after
its record is synced, and `open` recovers it from the checkpoint and the
log. A write whose call failed with `LogFailed`, or had not returned when
the process died, may or may not be recovered: read the key back after
reopening. Recovery never applies a write in part or without the writes
before it. `close` adds nothing to any of this: it does not flush, sync or
checkpoint.

The supported environment is a local Linux filesystem such as ext4 or XFS,
where `fsync` makes data durable, a directory sync makes entries durable and
`rename` is atomic. Network filesystems and devices that acknowledge `fsync`
without persisting are unsupported; macOS is untested. Tests cover killed
processes, failed writes and syncs, interrupted checkpoints and rotations,
damage and a file-level model of lost unsynced data. **Real power loss, and
how a device orders the pages of one write, have not been tested**: the
guarantees above assume that a completed sync reached stable media. After a
failed sync, check the device before reopening.

## Platform

| target | status |
|---|---|
| Linux aarch64 (`aarch64-unknown-linux-gnu`) | tested: the full validation below, Rust 1.98.1 |
| Linux x86-64 (`x86_64-unknown-linux-gnu`) | expected to work, untested; in the CI workflow, not yet run |
| macOS arm64 (`aarch64-apple-darwin`) | expected to build, untested; in the CI workflow, not yet run; durability not part of the contract |
| Windows and others | untested; not supported in this alpha |

"Expected" means the code has nothing specific to the target, not that it
has been run there.

**Build requirements:**

- Rust 1.89 or later (`rust-version` in `Cargo.toml`): 1.89 stabilised the
  file locks the engine uses, and edition 2024 and the locked dependencies
  need 1.85. Every target passes `cargo check` and strict Clippy on 1.89.0;
  tests run on 1.98.1.
- A C compiler and archiver for the target, found by the `cc` crate (`cc`
  and `ar`, or `CC` and `AR`): on Debian and Ubuntu `build-essential`, on
  macOS the Xcode command-line tools. Zstandard is C: `zstd-safe` builds the
  bundled libzstd 1.5.7 (`vendored`, with its dictionary trainer), so no
  system library, `pkg-config` or CMake is needed. LZ4 and CRC-32 are pure
  Rust.
- `Cargo.lock` is committed and pins every dependency; build with
  `--locked`. The crate has no Cargo features.

RAM-only engines touch no files and need no platform support beyond
threads. Checkpoints and the log need a filesystem with the guarantees under
[Durability](#durability), and use advisory file locks (`File::try_lock`).
`unsafe` is forbidden in the crate itself.

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

Against RocksDB 7.8.3 (a 2022 distribution build, its files in memory) on
the same data in the same VM, with matched 4 KiB blocks and dictionaries at
equal density, Tessera's `ZstdDictionary` runs gave 1.7–2.1× faster
uniform point lookups and 1.3–2.2× faster short scans, with cheaper puts
and lower peak memory. RocksDB was faster for skewed lookups served from
its block cache (6–7×) and filtered misses, and after sustained writes
Tessera held 14–19% more resident memory, the versions that the flat
compaction policy of that time left with its default overlap allowance
(`max_overlap` 4) in exchange for less rewriting; `max_overlap` 2 cut run
memory and sped scans at about 2.75 times the bytes rewritten. That
comparison predates leveled compaction and was not repeated. With plain
Zstandard both engines cost the same where decoding a block dominates. These
are results for that version and configuration only, not a general ranking.

## Validate

Validation runs offline in a Linux container with hard limits, using a Colima
VM (2 CPUs, 4 GiB) and an image built once from a pinned Rust base:

```sh
colima start
scripts/validate.sh image       # once, adds rustfmt and Clippy (uses the network)
scripts/validate.sh deps        # once, fetches pinned crates (uses the network)
scripts/validate.sh msrv-image  # once, adds the minimum Rust toolchain (uses the network)
scripts/validate.sh verify      # prove the limits are enforced
scripts/validate.sh             # fmt, Clippy, tests, doctests, docs and a release build
scripts/validate.sh full        # also the large-allocation boundary tests
scripts/validate.sh msrv        # check and Clippy on the minimum Rust version
scripts/validate.sh bench       # the benchmark suite, or e.g. `bench durable`
```

`check` covers formatting, Clippy with warnings denied on every target,
unit, integration and documentation tests (including this README's
examples), example compilation, rustdoc with warnings denied and a release
build of the library and examples. The benchmarks, `examples/bench.rs`, use
only the public API, keep their files on tmpfs and are never part of
`check`; neither is the RocksDB comparison in `tools/compare`.

`.github/workflows/ci.yml` runs the same checks without the container on
Linux x86-64, Linux arm64 and macOS arm64, and the minimum-version check on
Linux; it has not yet run.

Each step gets a 3 GiB cgroup with no swap, 2 CPUs, 256 processes, 2 build jobs,
2 test threads, a 512 MiB tmpfs at `/tmp` for test databases, and a timeout,
and reports its peak memory and block-device writes. Use
`scripts/validate.sh exec SECONDS CMD...` for anything else that could loop or
allocate without bound, including tests of deliberately broken code. A bare
`cargo test` on macOS has no memory limit.

## License

MIT
