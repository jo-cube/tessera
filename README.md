# Tessera

Tessera is a general-purpose, RAM-only compressed LSM key–value engine written
in safe Rust and embedded in the application's process. The dataset must fit
in memory. Data can be rebuilt from upstream sources or restored from an
explicit checkpoint.

This is a private alpha (`publish = false`). The API and on-disk formats may
change without migration. Requires Rust 1.89 or later and a C compiler for the
bundled Zstandard library.

## Supported operations

- Byte-string keys and values, puts, deletes and point lookups.
- Ordered scans and prefix scans in bytewise key order.
- Consistent in-memory snapshots that remain readable after the engine closes.
- Automatic memtable flushing and background leveled compaction; manual
  maintenance is also available.
- Zstandard level 3 with raw segment dictionaries and adaptive blocks, or
  uncompressed blocks.
- Explicit full checkpoints and recovery of the latest committed checkpoint,
  including format, checksum and corruption checks.
- Configurable admission limits for memtables, runs, snapshots and compaction.
  These account for estimated bytes; they do not cap process resident memory.

There is no write-ahead log, per-write durability, automatic checkpointing,
transaction support, multi-key atomic write, replication or disk tier.
Closing or dropping an engine writes nothing. Every update after the last
checkpoint is lost when the process ends unless rebuilt upstream.

## Usage

```rust
use tessera::engine::{
    CheckpointOptions, Engine, EngineOptions, EngineWorkspace,
};

let engine = Engine::new(EngineOptions::default())?;
engine.put(b"user/1", b"ada")?;
engine.put(b"user/2", b"grace")?;
engine.delete(b"user/1")?;

// Reuse one workspace per reading thread.
let mut workspace = EngineWorkspace::new();
assert!(engine.get(b"user/1", &mut workspace)?.is_none());
assert_eq!(engine.get(b"user/2", &mut workspace)?.unwrap().value, b"grace");

// Snapshots hold no lock while readers scan them.
let snapshot = engine.snapshot()?;
let mut scan = snapshot.scan_prefix(b"user/", &mut workspace);
assert_eq!(scan.next_record()?.unwrap().value, b"grace");
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

`EngineOptions::compression` is `Compression::Zstd` by default; the other
choice is `Compression::Uncompressed`. Zstandard buffers roughly 4 MiB of raw
encoded records per segment and stores one sampled raw dictionary of up to
16 KiB. `target_block_compressed` defaults to 1024 bytes: a 64 KiB sample
estimates the ratio used to cut blocks, with a 256 KiB raw cap. One larger
record gets its own block. Uncompressed runs use a fixed 16 KiB raw target.
Bottom-level compaction can erase settled sequence numbers to zero when no
snapshot or overlapping unselected run needs them. Snapshot views keep their
records and visibility, and checkpoint recovery preserves the write ceiling.
Readers reuse one context and borrow dictionary bytes from the run; there is
no prepared dictionary or decoded-block cache. Run format 5 and checkpoint
manifest version 2 replace earlier formats without backward compatibility.

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
