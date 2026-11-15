# Tessera

Tessera is a general-purpose, RAM-only compressed LSM key–value engine written
in safe Rust and embedded in the application's process. The dataset must fit
in memory. Data can be rebuilt from upstream sources or restored from an
explicit checkpoint.

This is a private alpha (`publish = false`). The API and on-disk formats may
change without migration. Requires Rust 1.89 or later and a C compiler for the
bundled Zstandard library.

## Supported operations

- Byte-string keys and values, atomic write batches, sorted bulk ingestion,
  point lookups and batched multi-get.
- Ordered scans and prefix scans in bytewise key order.
- Consistent in-memory snapshots that remain readable after the engine closes.
- Automatic memtable flushing and background leveled compaction; manual
  maintenance is also available.
- Zstandard level 1 with raw segment dictionaries and adaptive blocks, or
  uncompressed blocks.
- Explicit full checkpoints and recovery of the latest committed checkpoint,
  including format, checksum and corruption checks.
- Configurable admission limits for memtables, runs, snapshots and compaction.
  These account for estimated bytes; they do not cap process resident memory.

There is no write-ahead log, per-write durability, automatic checkpointing,
transactions beyond atomic batches, replication or disk tier.
Closing or dropping an engine writes nothing. Every update after the last
checkpoint is lost when the process ends unless rebuilt upstream.

## Usage

```rust
use tessera::engine::{
    CheckpointOptions, Engine, EngineOptions, EngineWorkspace, WriteBatch,
};

let engine = Engine::new(EngineOptions::default())?;
engine.put(b"user/1", b"ada")?;
engine.put(b"user/2", b"grace")?;
engine.delete(b"user/1")?;

let mut batch = WriteBatch::new();
batch.put(b"user/3", b"katherine");
batch.put(b"user/4", b"dorothy");
engine.write(&batch)?; // One atomic visibility change; later duplicate keys win.
batch.clear(); // Keeps its capacity.

// Reuse one workspace per reading thread.
let mut workspace = EngineWorkspace::new();
assert!(engine.get(b"user/1", &mut workspace)?.is_none());
assert_eq!(engine.get(b"user/2", &mut workspace)?.unwrap().value, b"grace");

engine.multi_get(&[b"user/2", b"user/1"], &mut workspace, |index, value| {
    // Called in input order, including misses and duplicate keys.
    assert_eq!(value.is_some(), index == 0);
})?;

// Snapshots hold no lock while readers scan them.
let snapshot = engine.snapshot()?;
let mut scan = snapshot.scan_prefix(b"user/", &mut workspace);
assert_eq!(scan.next_record()?.unwrap().value, b"grace");
assert_eq!(scan.next_record()?.unwrap().value, b"katherine");
assert_eq!(scan.next_record()?.unwrap().value, b"dorothy");
assert_eq!(scan.next_record()?, None);
drop(scan);

// Choose an application-owned checkpoint directory.
let dir = std::env::temp_dir().join(format!("tessera-readme-{}", std::process::id()));
engine.checkpoint(&dir, CheckpointOptions::default())?;
engine.close()?;
let recovered = Engine::recover(&dir, EngineOptions::default())?;
assert_eq!(recovered.get(b"user/2", &mut workspace)?.unwrap().value, b"grace");
# recovered.close()?;
# std::fs::remove_dir_all(&dir)?;
# Ok::<(), Box<dyn std::error::Error>>(())
```

`Engine::write(&WriteBatch) -> Result<u64, UpdateError>` returns the batch's
last sequence number. Memtable batches take a contiguous sequence range;
batches built directly into runs take one publication sequence. In either case
all operations become visible together. An empty batch returns the current ceiling.
`put` and `delete` share the memtable update path. A dedicated flush thread
builds runs while reads and writes continue; writers wait when the frozen-table
limit or level-0 stop is reached. `max_frozen_memtables` defaults to 2, in
addition to one active table, each with `memtable_budget` charged bytes.
`flush()` waits until all earlier writes are in runs. Refused or failed builds
leave frozen tables readable and surface through the existing flush errors.

Strictly key-sorted, unique batches use the direct run path when their charged
input reaches `bulk_ingest_min: Option<usize>` (`None` means half the memtable
budget). `ingest_sorted(&WriteBatch)` forces that path and rejects unsorted or
duplicate keys. Tables sharing an actual batch key are flushed before atomic
publication into level 0; range overlap alone causes no wait. Builds release the
state lock, so concurrent writes proceed. Each ingestion builds once with stored
placeholders, then assigns one shared sequence at publication, after all
previous writes. A run-level override supplies this effective sequence to readers
and merges; compaction and checkpoints write it into ordinary run records.
An oversized unsorted batch stages its records in a private memtable and uses
the same single-sequence run path, preserving the last duplicate operation.
Snapshots and checkpoints taken during a build exclude its unpublished runs;
a failed or panicked build consumes no sequence.

`multi_get(keys, workspace, |index, value| ...) -> Result<(), ReadError>` is
available on `Engine`, `ReadGuard` and `Snapshot`. It sees one consistent state
and reports borrowed values in input order. Indices are sorted once when needed;
run groups probe Bloom filters and decode each needed block once. Workspaces
reuse the index, result, value and decode buffers; warm calls allocate nothing
on the Rust heap. Callbacks run only after a successful read.

`EngineOptions::compression` is `Compression::Zstd` by default; the other
choice is `Compression::Uncompressed`. Zstandard buffers up to 4 MiB of raw
encoded records per segment and stores one sampled raw dictionary of up to
16 KiB. `target_block_compressed` defaults to 1024 bytes: a 64 KiB sample
estimates the ratio used to cut blocks, with a 256 KiB raw cap. One larger
record gets its own block. Segments use level 1; block sizing retains its
level-3 ratio probe.
Uncompressed runs use a 16 KiB raw target before applying the raw floor.
Frames and dictionaries share engine-owned 1 MiB chunks, including across
segments of the same run. A frame larger than a chunk has an exact allocation.
A finished run copies its partial final chunk into an exact allocation.
Retired chunks return to a shared pool, whose free reserve is capped at the
fixed 32 MiB, independent of job size.
`EngineStats` reports free reserve, its cap and checked-out chunk capacity,
including padding and snapshots. Stored run limits exclude chunk padding;
index, footer and filter buffers remain exact allocations.
Compaction builds disjoint key ranges of one selected job in parallel, using up
to `background_threads` builders (default half the available CPUs, capped at 4,
at least 1). Segment buffers shrink to share the existing workspace limit;
small jobs and tight limits use fewer builders. Flush builds remain serial.
Bottom-level compaction can erase settled sequence numbers to zero when no
snapshot or overlapping unselected run needs them. Snapshot views keep their
records and visibility, and checkpoint recovery preserves the write ceiling.
Readers reuse one context and borrow dictionary bytes from the run; there is
no prepared dictionary or decoded-block cache. Run format 5 and checkpoint
manifest version 2 replace earlier formats without backward compatibility.

## Tuning

- `level_ratio` defaults to 10. Higher ratios generally use less live heap and
  give faster reads; lower ratios favor faster writes and lower write
  amplification. Choose 4 when write cost matters more than memory and reads.
- `target_block_compressed` defaults to 1024 bytes. Larger blocks reduce index
  and frame overhead, using less memory, but point reads decode more data and
  become slower. Smaller blocks trade memory for faster point reads.
- `min_block_raw` defaults to 4 KiB (0 disables it): a floor on the ratio-based raw block target, capped at 256 KiB.
- `background_threads` limits builders within a selected compaction job. The
  default is half the available CPUs, capped at 4 and at least 1. More builders
  can accelerate large jobs at greater CPU cost; small jobs and tight workspace
  limits use fewer, and 1 builds serially.
- `memtable_budget` defaults to 64 MiB for each active or frozen table. A larger
  budget reduces flush frequency and can improve writes while using more
  transient memory. A smaller budget lowers that memory cost but flushes more
  often; the budget charges arena bytes used, including overwritten garbage,
  plus node overhead and snapshot history. It does not reserve memory in advance.

A `ReadGuard` provides a stable view while holding a shared lock and blocks
writers for its lifetime. Use snapshots for longer reads. See the
[`engine` module](src/engine.rs) for consistency, memory, maintenance and
checkpoint contracts. Build API documentation with `cargo doc --no-deps`.

## Tests

```sh
cargo fmt --check
cargo clippy --locked --all-targets --all-features -- -D warnings
cargo test --locked
RUSTDOCFLAGS="-D warnings" cargo doc --no-deps
```

The suite includes model comparisons, corruption and format checks, snapshot
and checkpoint correctness, compaction, allocation and retention checks.
Large-allocation boundary tests stay ignored by default.

For resource-limited Linux validation, use `scripts/validate.sh check`;
`scripts/validate.sh limits` runs the large-allocation boundary tests.
See [the release checklist](docs/release.md) when cutting a private alpha.
