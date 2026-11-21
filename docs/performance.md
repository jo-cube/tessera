# Performance characteristics

Tessera spends CPU to save memory and to make reads cheap. This page explains
where its memory and CPU go and why, which workloads suit it, which options
move the trade-offs and how to measure a workload of your own.
[ARCHITECTURE.md](../ARCHITECTURE.md) describes the same mechanisms in more
detail, with the alternatives that were rejected.

## Where memory goes

- **Compressed runs.** Almost all of a settled engine's memory is its runs:
  Zstandard blocks targeting 1 KiB of stored bytes each. A block's raw length
  adapts to the data, up to 64 KiB raw, so data that compresses well packs
  many records into one block. Memory therefore follows the compressed size
  of the data, not its raw size.
- **Dictionaries.** Each segment samples a dictionary of at most 16 KiB from
  its first 4 MiB of raw records. Small blocks compress poorly on their own;
  the dictionary gives each block the segment's common strings, so redundant
  data such as grouped JSON stays small even in 1 KiB blocks. When a segment's
  4 MiB prefix compresses well, subsequent blocks continue sharing that
  dictionary until stored frames reach 16 times the dictionary length, raw
  bytes reach 32 MiB, or compression drifts enough to eat into the dictionary
  savings, which bounds dictionary overhead on redundant data without growing
  the transient build buffer. Data with little in common across records
  exceeds the stored target within its 4 MiB prefix, closes at once, and is
  stored close to its raw size.
- **Indexes and filters.** A sparse, prefix-compressed index holds each
  block's last key. Bloom filters (10 bits per key) exist only above the
  bottom level, which holds most keys, so they cost a small fraction of the
  stored bytes.
- **Memtables.** Writes are buffered in up to three memtables (one active, two
  frozen) of `memtable_size` each. A memtable is charged its arena bytes,
  including overwritten payloads, plus 80 bytes per live entry.
- **Transient memory.** Compaction holds its inputs until each piece
  publishes, so a rewrite briefly needs its output beside its inputs; pieces
  and a shared output allowance bound that peak. Snapshots keep alive the
  runs and memtables they see, so a long-lived snapshot holds data that
  compaction has already replaced.
- **Across keyspaces.** Keyspaces share one budget, one memtable and one
  chunk pool. Each has its own runs, so a flush touching *k* keyspaces
  writes at least *k* runs, each with an index, filters and dictionaries.
  Many tiny keyspaces cost more stored bytes than one keyspace with a key
  prefix.

`Engine::stats` reports the budget figure (`memory_used`), the stored bytes
in total and per keyspace, and the part held only by snapshots. Reader
buffers and allocator overhead are outside the budget.

## Where CPU goes

- **Writes** insert into a copy-on-write memtable. A background worker
  flushes full memtables into compressed runs, and compaction merges runs
  down a leveled tree with a size ratio of `level_ratio` between levels.
  A record is rewritten, and so recompressed, as it descends the levels, and
  again when newer writes to nearby keys merge into the bottom level. Write
  amplification, and with it write CPU, therefore grows with the number of
  levels and with how widely updates spread over the key space.
- **Bottom-level erasure and copy-forward.** At the bottom level a record's
  sequence number is erased once no other run can hold an older version of
  its key. Blocks whose records are all erased and that hold no deletions are
  *settled*: a later merge that brings no other key into their range copies
  them verbatim after a checksum check, without decoding or recompressing,
  and a segment with at least half its blocks copied keeps its dictionary. Updates that
  cluster in part of the key space therefore leave the rest of the bottom
  level untouched; updates spread uniformly at random touch most blocks and
  recompress them.
- **Large sorted writes.** A batch whose keys ascend and that is at least half
  a memtable, or any batch larger than a memtable, is built directly into
  runs outside the state lock, skipping the memtable and the first flush.
- **Point reads** check the memtables, then at most one candidate run per
  group of disjoint runs, ruled out by key range, Bloom filter and sequence
  bound before anything is decoded. A hit decodes one small block into
  reusable buffers, so a warm lookup allocates nothing. `multi_get` sorts its
  keys and decodes each block once per call.
- **Scans** merge the memtables with one cursor per group of disjoint runs, so
  their cost is mostly decompressing each block once. A level 0 holding more
  flushes means more groups to merge; compaction keeps it short.

## Workloads that favour it

- Data whose records share structure: JSON or other text with repeated field
  names and values, records grouped by key prefix, versions of similar
  objects.
- Read-heavy work: point lookups, multi-gets and ordered or prefix scans, with
  many reader threads.
- A compressed working set that fits in RAM, with durability from periodic
  checkpoints, for example a cache, a materialized view rebuilt from a log,
  or a stream consumer's state stored beside its input offsets.

It suits less well a write-dominated workload that must minimise CPU per
write, random updates over a large keyspace that keep recompressing the
bottom level, data that does not compress, or data larger than memory.

## Options that move the trade-offs

| Option | Raise it to | Lower it to |
| --- | --- | --- |
| `block_size` (stored bytes) | compress better and use less memory, decoding more per point read | decode less per point read, using more memory |
| `level_ratio` | have fewer levels: less memory and fewer runs per read | rewrite less per write, with more levels |
| `memtable_size` | flush and compact less often, holding up to three tables in memory | hold less unflushed data, compressing more often |
| `background_threads` | finish compactions sooner, using more CPU and transient memory at once | leave more CPU to the application |
| `memory_limit` | let writers proceed further ahead of compaction | bound admitted memory sooner, with writers waiting earlier |

`compression: Uncompressed` stores raw 16 KiB blocks: little write CPU and
cheap decoding, at the cost of memory close to the raw data size.

## Linux resident memory

With glibc, freed buffers can stay resident after compaction, so the
process's RSS may stay well above `memory_used`. Fixing the mmap threshold
with `GLIBC_TUNABLES=glibc.malloc.mmap_threshold=524288` at process start makes
large freed allocations return to the operating system and removes most of
that gap, which shows that the gap is allocator retention rather than live
data. The tunable applies to every allocation in the process and may change
throughput. Allocators that return freed memory, such as mimalloc or jemalloc
with default settings, avoid the retention as well.

## Compared with RocksDB

On the repository's synthetic benchmarks, Tessera used much less memory than
RocksDB on redundant, grouped JSON and about the same on data with little
redundancy, served point reads faster, and spent more CPU per write, mostly
in bottom-level recompression. RocksDB is a disk engine and was measured in
its best case, on a RAM disk without its write-ahead log; it also handles
data larger than memory and per-write durability, which Tessera does not.

## Measuring a workload

Measure with your own data and access pattern before relying on any of the
above; compression, and so memory and write CPU, depend on the data.

- **`Engine::stats`** gives the budget figure and stored bytes per keyspace
  at any time; compare stored bytes with the raw bytes written to see the
  compression a dataset gets.
- **The diagnostics counters.** `cargo run --release --features diagnostics
  --example counters` reports, per level, raw and stored bytes written by
  flushes and compactions, write amplification, sequences erased, decoded
  bytes per get and blocks per scan. [tools/README.md](../tools/README.md)
  lists its scenarios and flags; run large scenarios through
  `scripts/validate.sh exec` so they stay bounded.
- **`tools/bench`**, on macOS, runs load, update, read, scan, mixed and bulk
  workloads against Tessera and RocksDB, with correctness gates:
  `just bench`, or `just bench --mode streaming` for the larger datasets. It
  writes `results.csv`, `summary.md`, per-second samples and a local
  `environment.txt` under `target/bench/<date>-<commit>/`.
  [tools/bench/README.md](../tools/bench/README.md) lists its prerequisites,
  cases and accounting.
