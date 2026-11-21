# Tessera

[![ci](https://github.com/jo-cube/tessera/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/jo-cube/tessera/actions/workflows/ci.yml)

Tessera is an embedded key–value engine for Rust that keeps the whole dataset
in memory as compressed, sorted runs, and writes it to disk only when the
application asks for a checkpoint. It is a log-structured merge tree written
in safe Rust, with named keyspaces, atomic batches across them, lock-free
snapshots and ordered scans. It has no write-ahead log: writes since the
latest checkpoint are lost when the process ends.

- **Compressed in RAM.** Data lives in memory as Zstandard blocks of about
  1 KiB stored, with a dictionary per segment, so records that share
  structure compress well together and a read decodes one small block.
- **Keyspaces.** Named, independent sets of keys share one memory budget, and
  one `WriteBatch` across them commits atomically.
- **Fast point reads and ordered scans.** Lookups skip runs by key range,
  Bloom filter and sequence before decoding anything, readers reuse their
  buffers and return values without copying, and scans merge one cursor per
  group of disjoint runs.
- **Snapshots.** A snapshot is a consistent view of every keyspace that takes
  no lock; writes and compaction continue beside it.
- **Explicit checkpoints with atomic metadata.** One call writes every
  keyspace at one moment while writes continue, reuses unchanged run files and
  commits with application metadata, such as stream offsets, in one rename.
- **Safe Rust, two dependencies.** `#![forbid(unsafe_code)]`, with only
  `crc32fast` and a bundled Zstandard beneath it.

## Performance

Tessera is strongest where its data shares structure and reads dominate:
redundant data such as grouped JSON takes much less memory than in an engine
that compresses for disk, and point reads and scans are served from memory
without I/O or a block cache. It pays for this on writes, which spend more
CPU per record than disk-oriented LSM engines because compaction
recompresses data at the bottom level, and its compressed working set must
fit in RAM. [docs/performance.md](docs/performance.md) explains where the
memory and CPU go, which options move them and how to measure a workload.

## When to use it, and when not

Use Tessera when the dataset, compressed, fits in RAM; when reads, point or
ordered, dominate; and when periodic checkpoints are enough durability, for
example a cache, a materialized view rebuilt from a log, or a stream
consumer's state that stores its input offsets beside its data.

Look elsewhere for per-write durability, datasets larger than memory,
replication, read/write transactions or Windows.

## Install

Depend on a release tag once it is published:

```toml
[dependencies]
tessera = { git = "https://github.com/jo-cube/tessera.git", tag = "v0.1.0" }
```

Tessera needs Rust 1.89 or newer and a C compiler, which builds the bundled
Zstandard. Linux (x86-64, arm64) and macOS (arm64, x86-64) are supported;
Windows is not. It is pre-1.0: minor versions may break the API and checkpoint
format. [Releases](docs/releases.md) covers versioning, platforms and
limitations. The license is [MIT](LICENSE).

## Quick start

```rust
use tessera::{Engine, Options, WriteBatch};

let dir = std::env::temp_dir().join(format!("tessera-readme-{}", std::process::id()));
let engine = Engine::open(&dir, Options::default())?; // recovers or starts empty
let users = engine.keyspace("users")?; // opened or created
let emails = engine.keyspace("emails")?;
users.put(b"user/1", b"ada")?;
assert_eq!(users.get(b"user/1")?.as_deref(), Some(&b"ada"[..]));

let mut batch = WriteBatch::new(); // atomic across keyspaces
batch.put(&users, b"user/2", b"grace");
batch.put(&emails, b"grace@example.com", b"user/2");
engine.write(&batch)?;

let snapshot = engine.snapshot(); // consistent across keyspaces, lock-free
let mut reader = snapshot.reader(); // one per thread; values borrow it
let mut scan = reader.prefix(&users, b"user/");
while let Some((key, value)) = scan.next()? {
    println!("{} = {}", String::from_utf8_lossy(key), String::from_utf8_lossy(value));
}
drop(scan);

engine.checkpoint_with(b"offset=42")?; // every keyspace, plus metadata
engine.close()?;
let engine = Engine::open(&dir, Options::default())?;
assert_eq!(engine.metadata().as_deref(), Some(&b"offset=42"[..]));
# engine.close()?;
# std::fs::remove_dir_all(&dir).unwrap();
# Ok::<(), tessera::Error>(())
```

[`examples/`](examples) holds longer programs: `basic`, `keyspaces` (related
keyspaces kept in step), `offsets` (a stream consumer that resumes exactly)
and `hot_reads` (a reader per thread). Run one with
`cargo run --example keyspaces`.

## Concepts

**Engine and directory.** `Engine::open(dir, options)` recovers the checkpoint
in `dir`, or starts empty, and holds `dir/LOCK` until the engine closes.
`Engine::in_memory` has no directory. `Engine`, `Keyspace` and `Snapshot`
are cheap, cloneable, thread-safe handles. Closing writes nothing.

**Keyspaces.** A keyspace is a named, independent set of keys:
`engine.keyspace("orders")` opens or creates one,
`engine.existing_keyspace("orders")` opens it only if it exists, and
`drop_keyspace` removes it with its data. Keyspaces share one memory budget and one memtable, so a
batch spanning several is atomic, and a snapshot sees all of them at one
moment. An engine holds up to 4096.

**Reads.** `Keyspace::get` returns an owned copy of the latest value. A
`Reader`, from `engine.reader()` or `snapshot.reader()`, reads without
copying: `get`, `multi_get`, `prefix` and `range` return values that borrow
the reader until its next call, and its buffers are reused across calls.
Keep one reader per thread.

**Snapshots.** `engine.snapshot()` is a consistent view of every keyspace that
holds no lock; writes and compactions continue. It keeps alive whatever it
sees, and that memory counts against the budget, so drop it once read. It
stays readable after the engine closes.

**Checkpoints and metadata.** `checkpoint()` writes every keyspace at one
moment into the directory while writes continue, reusing unchanged run files,
and commits atomically; a failure leaves the previous checkpoint intact.
`checkpoint_with(blob)` adds application metadata to the same commit and
`metadata()` returns it. To record a stream offset exactly, write it into a
keyspace in the same batch as the data it covers.

**Errors.** Every call returns `tessera::Error`; match on `error.kind()`
(`InvalidInput`, `MemoryLimit`, `KeyspaceDropped`, `Locked`, `Io`,
`Corruption` and others).

## Options

Start from `Options::default()` and set the fields to change.

| Option | Default | What it trades |
| --- | --- | --- |
| `memory_limit` | none | A budget for writes: near it writers wait for compaction, at it they fail with `MemoryLimit`. Not a cap on process memory. |
| `memtable_size` | 64 MiB | Bytes buffered before a flush. Larger flushes less often but holds up to three tables' worth in memory. It also caps one record. |
| `level_ratio` | 10 | Size ratio between levels. Larger favours reads and memory; smaller rewrites less on writes. |
| `background_threads` | half the CPUs, 1–4 | Threads building one compaction. More gives faster writes for more CPU and transient memory. |
| `compression` | `Zstd` | `Uncompressed` stores 16 KiB raw blocks: less CPU, much more memory. |
| `block_size` | 1 KiB | Target **stored** bytes of a block after compression, not raw bytes. Smaller blocks decode less per point read and compress worse. |

## Documentation

- API reference: `cargo doc --open`
- [docs/guide.md](docs/guide.md): consistency, memory, threads, durability,
  the on-disk layout, errors and limits
- [docs/performance.md](docs/performance.md): where memory and CPU go, the
  options that move them and how to measure a workload
- [ARCHITECTURE.md](ARCHITECTURE.md): the internals and design decisions
- [docs/releases.md](docs/releases.md): versioning, the format and platforms
- [docs/development.md](docs/development.md): checks, validation and
  benchmarks
