# Architecture

Tessera is an embedded, RAM-first LSM key–value engine with a synchronous API.
The whole dataset lives in memory, compressed; disk is touched only by explicit
checkpoints and recovery. Its priorities, in order, are memory efficiency, read
performance (point and scan) and write performance. A small set of options lets
an application trade memory and reads for writes. The module documentation
(`src/engine.rs`, `src/run/mod.rs`, `src/block/mod.rs`, `src/memtable.rs`) states
the contracts; this file explains how the parts fit and why they are shaped so.

## Data flow

```text
put / delete / write ─▶ active memtable ─freeze─▶ frozen memtables (≤ 2)
                                                        │ flush worker
ingest_sorted ─▶ private runs ───────────┐              ▼
                                         ├──────▶ level 0 (overlapping runs)
                                         │              │ compaction worker
                                         │              ▼
                                         └──────▶ levels 1..n (disjoint runs)
```

- **Memtable.** A copy-on-write B+ tree whose payloads live in a chunk arena.
  Each tree holds only the latest update of each key; versions are trees that
  snapshots share, not chains inside one. A table is charged its arena bytes
  (overwritten payloads included), 80 bytes per live entry and the history its
  snapshots pin, against a 64 MiB budget.
- **Flush.** Writers rotate a full table in constant time. A dedicated worker
  merges the frozen tables (newest update wins) into runs outside the state
  lock, cut at the target run length and at level 1's run boundaries so that
  each piece's next merge meets fewer runs.
- **Sorted ingestion.** `ingest_sorted` builds runs outside the lock with
  placeholder sequences, then publishes them at one overridden sequence after
  draining only the tables that share one of its keys.
- **Levels.** Level 0 holds flushes in publication order and may overlap.
  Deeper levels are sorted and strictly disjoint, with targets set by
  `level_ratio` (default 10) and dynamic level sizing. Level 0 is compacted at
  four flush groups; writers wait at twelve.
- **Compaction.** The worker takes the most overfull level, then level 0, then
  small neighbours. Nonoverlapping runs move down without a rewrite. Level-0 →
  bottom jobs run as key-range pieces of about one target run of bottom input:
  each piece publishes atomically and frees its inputs, and level-0 sources
  shrink to bounded views of their remaining range. Up to
  `background_threads` pieces build at once within a shared output allowance;
  a piece that outgrows its reservation retries alone with a doubled request.

## Runs

A run is an immutable, self-contained sorted file image held in pooled 1 MiB
chunks. It is built in one pass: blocks are cut as records arrive, and each
segment of about 4 MiB raw is compressed once complete.

- **Blocks.** Prefix-compressed records with restart keys every sixteen
  records. Block length adapts: each segment compresses its first 64 KiB at
  zstd level 1 without a dictionary, and that ratio sets the raw length
  expected to compress to the target (1 KiB by default), capped at 64 KiB raw.
- **Segments.** Each segment stores a raw-content zstd dictionary of at most
  16 KiB, sampled from evenly spaced records; blocks decode independently with
  it, by reference, through one reusable context.
- **Frames.** Magicless zstd frames with a header binding codec, dictionary
  reference and decoded length, and a CRC-32 checked before every decode.
- **Index.** A sparse index of each block's last key, prefix-compressed, with
  whole-key restarts and `u64` search heads rebuilt in memory.
- **Beside the bytes.** Non-bottom runs carry a blocked Bloom filter (10 bits
  per key, 512-bit blocks). Every run records its sequence bounds and, per
  block, a record count when the block is settled (all sequence 0, no
  tombstones).

## Reads

- **Point lookups** check the active and frozen memtables, then a run directory
  rebuilt at every publication: runs partitioned into the fewest groups with
  strictly disjoint key ranges, newest bound first. Each group yields at most
  one candidate run by binary search; range, filter and sequence bound rule it
  out before anything is decoded, and the search stops once no group can hold a
  newer version. A lookup decodes into two reusable buffers.
- **Multi-get** sorts the keys once and decodes each block once per batch.
- **Scans** merge the memtables with one cursor per group, so a scan holds one
  decoded block and one decompression context per group, not per run.
- **Conflicts.** Identical copies of an update count once; differing copies
  fail the read rather than letting input order choose.

## Memory accounting

`memory_limit` is one writer-admission budget. It sums charged memtables,
stored runs with their filters and metadata, retired sources held by snapshots,
the pool reserve and unused checked-out capacity, and estimates of in-flight
build output and workspace. Shared sources count once. Below 90% writers
proceed; above it they wait briefly on publication; at 100% they request
reclamation and fail with `MemoryLimit` only when live data and held
snapshots cannot shrink. Flushes, compactions, snapshots and checkpoints are
never refused, so maintenance can always finish and free space. Caller-owned
workspaces and allocator overhead are outside the budget.

## Checkpoints

A checkpoint pins one structural snapshot at a flush rotation, flushes the
tables it covers with the normal builder and writes outside the state lock.
Each run is written once under a stable ID as its exact bytes plus its filter;
later checkpoints reuse those files. A manifest records levels, flush groups,
bounded views, sequence overrides, the ceiling and application metadata. The
commit is the rename of `CURRENT` after every file and directory is synced; GC
then keeps the files of the last two generations. Recovery reads exactly the
generation `CURRENT` names and validates every frame, dictionary, record,
filter and cross-run copy before any worker starts.

## Concurrency

One reader–writer lock guards the state; a writer-priority gate keeps new
readers from overtaking a waiting writer. Publication and writes take it
exclusively and briefly. Reads take immutable references under the shared lock
and decode after releasing it. Builds, compression and checkpoint I/O hold no
lock. A flush worker and a compaction worker run per engine; compaction can use
several builders. Snapshots hold no lock and own their sources, so maintenance
keeps only the newest version of each key.

## Design decisions

**Adaptive compressed-size blocks** (1 KiB target, 64 KiB raw cap).
- A 2 KiB target: about 30% fewer point reads per second for about 6% memory.
- zstd `targetCBlockSize` early stop: frames are too small to split, and
  `maxBlockSize` 8 KiB makes grouped JSON 57% larger.
- Key/value-separated blocks with early-stop decoding: zstd decodes the whole
  frame anyway.
- A raw block floor: no measured benefit.

**Per-segment raw-content dictionaries of 16 KiB, sampled.**
- Trained dictionaries: slow to build and to load.
- A 4 KiB dictionary: grouped JSON 16–20% larger. Revisit with real datasets.

**Zstd level 1 only, with uncompressed as the other codec.**
- LZ4: removed to keep one compressing codec, the one that saves the most
  memory, which is the first priority.
- A per-segment adaptive level 1/3 choice: almost never triggered.
- Zstd effort by output level: fast levels store weak-locality blocks raw, and
  level 3 gains almost nothing on dictionary blocks.

**Frame CRCs on every decode; dictionaries trusted after build or validation.**
- Verifying each dictionary's CRC on every decode: about a tenth of write CPU
  on weak-locality data, for bytes already trusted like the run's structure.

**Single-pass building.**
- Buffering a segment's records and cutting blocks after sampling: records
  were encoded twice, and encoding took about 40% of grouped-data write CPU,
  against about 15% in one pass.

**Blocked Bloom filters on non-bottom runs only.**
- Bottom-level filters: the bottom holds most keys, so they would cost the most
  memory where a lookup that reaches the bottom usually hits.
- Prefix Bloom filters: the main scan cost is decompression, which they do not
  touch. Reconsider for short or absent-prefix scans.

**Leveled compaction, ratio 10, level-0 trigger 4 and stop 12.**
- A deeper level 0 or smaller memtables as defaults: they help writes but hurt
  scans and memory, and smaller memtables roughly double compression work.
- Level-0 batching (trigger and stop 8/20 or 12/28): fewer bottom rewrites on
  grouped data, none on weak-locality data, while point-read work rises
  56–525%, worst-point scans up to 83% and live heap up to 58%; no existing
  option carries the trade.
- Range-partitioned shards: no change to write amplification for random writes,
  at the cost of a large rewrite.

**Piecewise, concurrent level-0 → bottom jobs.**
- One atomic job: its transient output peaks at the whole rewrite.
- Pinning level-1 flush cuts during concurrent pieces: did not reduce the
  level-0 read fan-out concurrency leaves.

**Per-record sequence erasure at the bottom.** A record keeps its sequence only
if an unselected overlapping run covers its key, may hold it by its filter and
holds a record at least as old. Snapshots never block erasure: they own their
directories.
- Job-level erasure: one contesting run kept every output sequence.

**Copy-forward of settled bottom blocks.** Untouched settled blocks are copied
verbatim after a CRC check; a segment with at least half its blocks copyable
gives the output a segment that inherits its dictionary.
- Block-reference compaction (outputs sharing input chunks): trades memory for
  write CPU; copy-forward saves the rewrite without shared chunks.

**One memory budget, a chunk pool and a memtable arena.**
- Engine-side 32 MiB pool regions against glibc retention: fragmentation risk;
  the README documents allocator settings instead.
- Charging the typical 55 bytes per entry that random keys measure: 80 bytes
  covers the worst-case leaf occupancy, so the budget stays a bound.

**An embedded library with a synchronous API.**
- A Redis-style single-threaded front end: an extra handoff hop and a
  throughput ceiling, and no fit for an embedded library.
