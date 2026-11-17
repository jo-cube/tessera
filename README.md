# Tessera

Tessera 0.1.0 is a general-purpose, RAM-first compressed LSM key–value engine
embedded in the application process, written in safe Rust. Use it when the
working dataset fits in RAM, ordered scans and atomic batches are useful, and
explicit checkpoints meet the application's persistence needs. It has no
write-ahead log, per-write durability, disk tier, replication or multi-operation
read/write transactions. Closing or dropping an engine writes nothing.

The package is consumed by path or Git revision (`publish = false`). Rust 1.89
and a C compiler for bundled Zstandard are required. The license is [MIT](LICENSE).

## Usage

```rust
use tessera::engine::{
    CheckpointOptions, Engine, EngineOptions, EngineWorkspace, WriteBatch,
};

let engine = Engine::new(EngineOptions::default())?;
engine.put(b"user/1", b"ada")?;
let mut workspace = EngineWorkspace::new();
assert_eq!(engine.get(b"user/1", &mut workspace)?.unwrap().value, b"ada");

let mut batch = WriteBatch::new();
batch.put(b"user/2", b"grace");
batch.put(b"user/3", b"katherine");
engine.write(&batch)?; // All operations become visible together.
engine.multi_get(&[b"user/1", b"user/0"], &mut workspace, |index, value| {
    assert_eq!(value, if index == 0 { Some(&b"ada"[..]) } else { None });
})?;

let snapshot = engine.snapshot(); // Holds no lock after acquisition.
let mut scan = snapshot.scan_prefix(b"user/", &mut workspace);
for expected in [b"ada".as_slice(), b"grace", b"katherine"] {
    assert_eq!(scan.next_record()?.unwrap().value, expected);
}
assert_eq!(scan.next_record()?, None);
drop(scan);
drop(snapshot);

batch.clear(); // Retains capacity; ingestion requires sorted, unique keys.
batch.put(b"user/4", b"dorothy");
batch.put(b"user/5", b"mary");
engine.ingest_sorted(&batch)?;

// Use an application-owned directory for persistent checkpoints.
let dir = std::env::temp_dir().join(format!("tessera-basic-{}", std::process::id()));
engine.checkpoint_with_metadata(&dir, CheckpointOptions::default(), b"schema=1")?;
engine.close()?;
let (recovered, metadata) = Engine::recover_with_metadata(&dir, EngineOptions::default())?;
assert_eq!(metadata, b"schema=1");
assert_eq!(recovered.get(b"user/5", &mut workspace)?.unwrap().value, b"mary");
recovered.close()?;

let recovered = Engine::recover(&dir, EngineOptions::default())?;
assert_eq!(recovered.get(b"user/1", &mut workspace)?.unwrap().value, b"ada");
recovered.close()?;
std::fs::remove_dir_all(&dir)?;
# Ok::<(), Box<dyn std::error::Error>>(())
```

Run the same code with `cargo run --example basic`. API contracts are in the
[engine documentation](src/engine.rs); build local rustdoc with `cargo doc --open`.

## Operations and consistency

Keys and values are byte strings. Keys compare bytewise; empty keys and values
are valid. Keys are limited to 64 KiB and values to 64 MiB. A record's key,
value and 128-byte entry charge must also fit the memtable budget.

- `put`, `delete` and `write` publish atomically and return a sequence ceiling.
  Memtable batches take consecutive sequences; later duplicate keys win.
  Batches built into runs take one shared publication sequence. Empty batches
  return the current ceiling. Snapshots never see a partial batch.
- `ingest_sorted` requires strictly increasing, unique keys and builds outside
  the state lock. Sorted batches reaching half the memtable budget automatically
  use this path; oversized unsorted batches use private staging. Publication
  follows concurrent writes and drains tables sharing actual batch keys.
- `get` and `multi_get` each see one consistent state. Multi-get callbacks run
  after successful decoding, in input order, including duplicates and misses.
- Snapshots preserve their state through writes, maintenance and engine closure.
  They share immutable runs and copy-on-write memtables. Scans yield each live
  key once in ascending order; prefixes use raw bytes. A value's sequence may be
  zero after bottom-level compaction, so it is not a stable update identifier.

Compaction keeps disjoint levels below level 0. Large level-0 jobs into the
bottom run as disjoint key ranges aligned to bottom-run boundaries, with
about one target run of bottom input per piece. Up to `background_threads`
pieces build at once within a shared unpublished-output allowance. Each piece publishes atomically
and releases its bottom inputs. Bounded views of level-0 sources expose only the
remaining ranges; snapshots and checkpoints preserve the bounds they captured.
A later piece failure leaves earlier publications intact and stops the job.

## Options and memory

`EngineOptions` is fixed when the engine starts:

| Option | Default | Trade-off |
| --- | --- | --- |
| `memory_limit` | `None` | Writer admission budget; held snapshots can prevent admission |
| `memtable_budget` | 64 MiB | Larger tables flush less often and retain more transient memory |
| `level_ratio` | 10 | Larger ratios favor reads and memory; smaller ratios reduce rewriting |
| `background_threads` | Half the CPUs, clamped to 1–4 | Bounds concurrent compaction pieces and builders; trades write throughput for CPU and transient memory |
| `compression` | `Compression::Zstd` | Compression costs CPU; `Uncompressed` reads blocks in place |
| `target_block_compressed` | 1024 bytes | Larger blocks reduce metadata and increase point-read decoding |

The memtable budget must be between 128 and `usize::MAX / 2` bytes, the level
ratio at least 2, the builder count at least 1, and the compressed block target
between 1 byte and 64 KiB. Zstandard level 1 uses raw dictionaries up to 16 KiB
per roughly 4 MiB segment. Ordinary raw blocks are capped at 64 KiB; a larger
record gets its own block. Uncompressed runs target 16 KiB raw blocks.

At 90% of the admission budget writers wait briefly for publication. At 100%
they request maintenance while reclamation can progress, then return
`MemoryLimit` if live data or held snapshots prevent admission. Reads and
maintenance remain available. Writers also wait at two frozen memtables or
twelve level-0 flushes. `flush()` waits until preceding writes are in runs.

`EngineStats::used` includes charged memtables, stored runs and metadata,
retained snapshot sources, pool reserve, unused checked-out capacity and build
estimates. It is an admission budget with transient maintenance headroom;
caller-owned workspaces and allocator overhead are outside it. Memtables charge
arena bytes used, including overwritten garbage, entry charges and pinned
history. The budget reserves no memory in advance. An active table plus two
frozen tables can charge about 192 MiB at the default memtable budget.

Frames and dictionaries share 1 MiB chunks. Large frames use exact allocations;
finished runs trim their partial final chunk. The free reserve is capped at
32 MiB. Concurrent bottom compaction bounds unpublished output by the builder
count times one estimated target stored run.
A growing piece can use the whole allowance; a range exceeding it stops the job
with an output-limit error. With one builder, bottom pieces publish consecutively
using the serial piece budget. A level-0 source is released when its final range
is merged. Snapshots can keep
replaced storage alive. Workspaces retain decode buffers and contexts; there is no decoded-block
cache. Use `Engine::stats()` and workspace counters to inspect memory and work.

On Linux with glibc, freed buffers can remain resident, so RSS may stay above
`EngineStats::used` after maintenance. Applications can evaluate
`GLIBC_TUNABLES=glibc.malloc.mmap_threshold=524288` at process startup. This
[fixes the mmap threshold](https://sourceware.org/glibc/manual/latest/html_node/Memory-Allocation-Tunables.html)
and lets large freed allocations return to the OS; it affects allocations
throughout the process and can change throughput. Measure with the application
workload before adopting it. Allocators that purge freed memory, such as mimalloc
or jemalloc with default settings, also avoid this retention.

## Thread safety and errors

`Engine` and `Snapshot` are `Send + Sync`. Share them across threads and use
one reusable `EngineWorkspace` per concurrent reader. State changes use an
exclusive lock; run decoding and builds happen outside it. Dedicated flush and
compaction workers perform maintenance; compaction can use several builders.
A `ReadGuard` holds a shared lock and is not `Send`. Drop it before calling
engine methods that acquire the state lock. Use snapshots for long reads.
`close()` joins workers; snapshots can outlive the engine.

Invalid configuration returns `OpenError::Config(ConfigError)`. Invalid writes
return `UpdateError::Rejected(WriteError)`; admission pressure and sequence
exhaustion return `MemoryLimit` and `SequencesExhausted`. Reads report corruption
or conflicting copies through `ReadError`. Maintenance reports read failures,
stale selections or worker panics. A build panic leaves published sources
readable; a panic under the exclusive state lock poisons the engine, and later
operations acquiring that lock panic.

## Durability and checkpoints

`checkpoint` flushes and pins one structural snapshot at its sequence ceiling,
then writes files while updates and compactions continue. Immutable runs are
written once and reused by stable ID, without recompression. Reports count new,
reused and deleted files and bytes. GC retains the latest two generations.
`checkpoint_with_metadata` and `recover_with_metadata` include an opaque blob
of at most 16 MiB in the same commit. All writes since the latest successful
checkpoint are lost on process exit.

The layout is `CURRENT`, `LOCK`, `runs/<32-hex-digit ID>` and
`ckpt-<20-digit generation>/MANIFEST`. Run format 6 uses magicless Zstandard
frames and a prefix-compressed sparse index. Run files append Bloom words and
their CRC-32. Manifest version 4 records checksums, lengths, levels, flush groups,
sequence overrides, the ceiling and application metadata. Unsupported versions
are rejected. CRC-32 detects accidental damage; it does not authenticate files.

Runs and the manifest are synced before renaming `CURRENT`, the commit point;
the parent directory is synced before GC. An error before the rename retains
the preceding commit. An I/O error after it reports `after_commit_point = true`
and retains both generations. Durability assumes local POSIX rename and working
file and directory fsync. An advisory lock excludes cooperating directory users.
Network filesystems and external modifications are unsupported.

Recovery validates exactly the generation named by `CURRENT`, with no fallback.
It checks lengths, checksums, run structure, Bloom membership, sequence bounds
and cross-run consistency before starting workers. Allocation is bounded by
actual file sizes and format limits. Recovery temporarily holds a contiguous
run file while packing it into chunks, and one decoded block per overlapping
group for cross-run validation. It leaves checkpoint data unchanged and may
create `LOCK`. Rejections return typed `OpenError`s.

## Validation

```sh
cargo fmt --check
cargo clippy --locked --all-targets --all-features -- -D warnings
cargo test --locked
RUSTDOCFLAGS="-D warnings" cargo doc --locked --no-deps
cargo +1.89.0 check --locked --all-targets --all-features
cargo +1.89.0 clippy --locked --all-targets --all-features -- -D warnings
cargo test --locked --lib long_ -- --ignored
```

The default suite includes bounded mutation tests and a concurrent sequence
model with a 20-second process deadline. Ignored campaigns are longer variants;
large-allocation boundary tests run separately. `scripts/validate.sh check`
runs checks in a resource-limited Linux container; `limits` runs boundaries and
`msrv` checks Rust 1.89. See the [release checklist](docs/release.md),
[diagnostic counters](tools/README.md) and [macOS comparison tool](tools/bench/README.md).
