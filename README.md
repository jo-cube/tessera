# Tessera

Tessera is a RAM-first compressed LSM key–value engine embedded in the
application process, written in safe Rust. The dataset must fit in memory.
Explicit checkpoints restore data after a restart; there is no write-ahead
log or per-write durability. Closing or dropping an engine writes nothing.

This private alpha has `publish = false`. Its API and formats may change
without migration. Rust 1.89 and a C compiler for bundled Zstandard are required.

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
let snapshot = engine.snapshot();
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

## Operations and consistency

Keys and values are byte strings. Keys compare bytewise; empty keys and values
are valid. Keys are limited to 64 KiB and values to 64 MiB. A record's key,
value and 128-byte entry charge must also fit the memtable budget.

- `put`, `delete` and `write` publish atomically and return a sequence ceiling.
  A memtable batch takes consecutive sequences, and later duplicate keys win.
  A batch built into runs takes one shared publication sequence. An empty batch
  returns the current ceiling. Snapshots never see a partial batch.
- `ingest_sorted` requires strictly increasing, unique keys and builds runs
  outside the state lock. Sorted batches reaching half the memtable budget
  automatically use that path; oversized unsorted batches use private staging.
  Publication follows concurrent writes and drains tables sharing batch keys.
- `get` and `multi_get` each see one consistent state. Multi-get callbacks run
  after successful decoding, in input order, including duplicates and misses.
- Snapshots preserve the state at `last_seq()` through writes, maintenance and
  engine closure. They share immutable sources and copy-on-write memtables.
  Scans yield each live key once in ascending order; prefix scans use raw bytes.
  A value's sequence may be zero after settled bottom-level compaction, so it
  is not a stable update identifier.

## Options and memory

`EngineOptions` is fixed when the engine starts:

| Option | Default | Meaning |
| --- | --- | --- |
| `memory_limit` | `None` | Optional writer admission budget in bytes |
| `memtable_budget` | 64 MiB | Charged bytes per active or frozen table |
| `level_ratio` | 10 | Level size multiplier, at least 2 |
| `background_threads` | Half the CPUs, clamped to 1–4 | Builders per compaction |
| `compression` | `Compression::Zstd` | Zstandard or `Uncompressed` |
| `target_block_compressed` | 1024 | Estimated compressed block bytes, 1–256 KiB |

At 90% of the memory budget writers wait briefly for publication. At 100% they
request maintenance while reclamation can progress, then return `MemoryLimit`
if live data or held snapshots prevent admission. Reads and maintenance remain
available. Writers also wait at two frozen memtables or twelve level-0 flushes.
`flush()` waits until all preceding writes are in runs.

`EngineStats::used` includes charged memtables, stored runs and metadata,
retained snapshot sources, pool reserve and unused checked-out capacity, plus
build estimates. This is an admission budget with transient maintenance
headroom. Caller-owned workspaces and allocator overhead are outside it.
Memtables charge arena bytes used, including overwritten garbage, entry
charges and pinned history; the budget does not reserve that memory in advance.

Frames and dictionaries share 1 MiB chunks. Large frames use exact allocations;
finished runs trim their partial final chunk. The free reserve is capped at
32 MiB. Readers borrow dictionaries and reuse one decode context per workspace;
there is no decoded-block cache. Zstandard level 1 uses raw dictionaries up to
16 KiB per 4 MiB segment, with a level-3 ratio probe for adaptive block cuts.
Ordinary raw blocks are capped at 256 KiB; a larger record gets its own block.
Uncompressed runs target 16 KiB raw blocks. Bottom-level compactions omit Bloom
filters; other builders may omit them to bound temporary memory.

Higher `level_ratio` usually favors memory and reads; lower ratios favor writes.
Larger blocks reduce metadata while increasing point-read decoding. Larger
memtables reduce flush frequency at greater transient memory cost.

## Threading and errors

An engine can be shared between threads. State changes use an exclusive lock;
run decoding and builds happen outside it. A dedicated flush worker and a
compaction worker perform maintenance. Compaction can use several builders;
flush builds are serial. Use one reusable `EngineWorkspace` per reading thread.
A `ReadGuard` holds a shared lock and blocks writers and publication: drop it
before calling engine methods that need the exclusive lock. Use snapshots for
long reads. `close()` joins workers; snapshots can outlive the engine.

Invalid configuration returns `OpenError::Config(ConfigError)`. Invalid writes
return `UpdateError::Rejected(WriteError)`; admission pressure and sequence
exhaustion return `MemoryLimit` and `SequencesExhausted`. Reads report corruption
or conflicting copies through `ReadError`. Maintenance reports read failures,
stale jobs or worker panics. A build panic leaves published sources readable;
a panic under the exclusive state lock poisons the engine.

## Checkpoints

`checkpoint` flushes and pins one structural snapshot at its sequence ceiling,
then writes files while updates and compactions continue. Immutable runs are
written once and reused by stable ID, without recompression. Reports count new,
reused and deleted files and bytes. GC retains the latest two generations.
`checkpoint_with_metadata` and `recover_with_metadata` include an opaque blob
of at most 16 MiB in the same commit.

The layout is `CURRENT`, `LOCK`, `runs/<32-hex-digit ID>` and
`ckpt-<20-digit generation>/MANIFEST`. Run format 6 uses magicless Zstandard
frames and a prefix-compressed sparse index. Each run file appends Bloom words
and their CRC-32. Manifest version 3 records file checksums and lengths, levels,
flush groups, sequence overrides, the ceiling and application metadata.
Unsupported versions are rejected.

Runs and the manifest are synced before renaming `CURRENT`, the commit point,
then the parent directory is synced before GC. An error before the rename keeps
the old commit. An I/O error after it reports `after_commit_point = true` and
retains both generations. Durability assumes local POSIX rename and working
file and directory fsync. An advisory lock excludes cooperating directory users.

Recovery validates exactly the generation named by `CURRENT`, with no fallback.
It checks lengths, checksums, run structure, Bloom membership, sequence bounds
and cross-run consistency before starting workers. Allocation is bounded by
actual file sizes and format limits; a declared decoded length is checked
against compressed block headers before sizing its buffer. Recovery temporarily
holds a contiguous run file while packing it into chunks. Cross-run validation
keeps one decoded block per overlapping group. Recovery leaves checkpoint
data unchanged and may create `LOCK`. Rejections return typed `OpenError`s.
CRC-32 detects accidental damage; it does not authenticate files.

## Validation

```sh
cargo fmt --check
cargo clippy --locked --all-targets --all-features -- -D warnings
cargo test --locked
RUSTDOCFLAGS="-D warnings" cargo doc --locked --no-deps
cargo +1.89.0 check --locked --all-targets
cargo +1.89.0 clippy --locked --all-targets -- -D warnings
cargo test --locked --lib long_ -- --ignored
```

The default suite includes bounded mutation tests and a concurrent sequence
model with a 20-second process deadline. Longer campaigns and large-allocation
boundary tests are ignored by default. `scripts/validate.sh check` runs the full
checks in a resource-limited Linux container; `limits` runs boundary tests.
See the [engine contracts](src/engine.rs) and [release checklist](docs/release.md).
