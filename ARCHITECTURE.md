# Architecture

Tessera is an embedded, RAM-first LSM key–value engine with a synchronous API.
The whole dataset lives in memory, compressed; disk is touched only by explicit
checkpoints and recovery. Its priorities, in order, are memory efficiency, read
performance (point and scan) and write performance. A small set of options lets
an application trade memory and reads for writes. The module documentation
(`src/lib.rs`, `src/engine.rs`, `src/run/mod.rs`, `src/block/mod.rs`,
`src/memtable.rs`) states the contracts, and [docs/guide.md](docs/guide.md)
the behaviour an application sees; this file explains how the parts fit and
why they are shaped so.

## Data flow

```text
put / delete / write ─▶ active memtable ─freeze─▶ frozen memtables (≤ 2)
  (every keyspace)        (shared, keys prefixed      │ flush worker,
                           by keyspace ID)            │ split by keyspace
                                                      ▼
large write ─▶ private runs ────────────▶ level 0 of each keyspace (overlapping)
                                                      │ compaction worker
                                                      ▼
                                          levels 1..n of each keyspace (disjoint)
```

- **Memtable.** A copy-on-write B+ tree whose payloads live in a chunk arena.
  Each tree holds only the latest update of each key; versions are trees that
  snapshots share, not chains inside one. A table is charged its arena bytes
  (overwritten payloads included), 80 bytes per live entry and the history its
  snapshots pin, against a 64 MiB budget.
- **Flush.** Writers rotate a full table in constant time. A dedicated worker
  merges the frozen tables (newest update wins) into runs outside the state
  lock, cut at every keyspace boundary, at the target run length and at
  level 1's run boundaries so that each piece's next merge meets fewer runs.
- **Large writes.** A `write` whose batch is charged at least half a memtable
  and whose keys ascend, or that is larger than a memtable, is built into
  runs outside the lock with placeholder sequences. The runs are published
  at one overridden sequence after draining only the tables that share one
  of the batch's keys, so the batch stays atomic. Oversized unsorted batches
  sort operation positions by key and retain the last operation per key. Run
  builders borrow those operations from the batch.
- **Levels.** Level 0 holds flushes in publication order and may overlap.
  Deeper levels are sorted and strictly disjoint, with targets set by
  `level_ratio` (default 10) and dynamic level sizing. Level 0 is compacted at
  four flush groups; writers wait at twelve. A group counts as long as any
  of its runs remain in level 0, including slices outside the last merge.
- **Compaction.** The worker takes the most overfull level, then eligible
  level 0, then small neighbours. Nonoverlapping runs usually move down by
  relabelling. When ordinary work runs out and level 0 still holds runs, the
  worker waits for five quiet seconds before promoting those runs. It waits
  for frozen tables to publish and checks the committed sequence and flush
  count at selection. If writes resume,
  the selected job can finish and further idle selections wait for quiet.
  An isolated idle promotion into the bottom can rewrite the source to
  remove its filter and elide tombstones under the ordinary erasure rules.
  Level-0 →
  bottom jobs run as key-range pieces of about one target run of bottom input:
  each piece publishes atomically and frees its inputs, and level-0 sources
  shrink to bounded views of their remaining range. Up to
  `background_threads` pieces build at once within a shared output allowance;
  a piece that outgrows its reservation retries alone with a doubled request.

## Keyspaces

A keyspace is a name, an internal `u32` ID that is never reused, and its own
runs, levels, run directory and byte counts. Everything else is shared: the
memtables, the sequence counter, the memory budget, the chunk pool, the flush
and compaction workers, the builders' output allowance and the readers' buffer
pool.

- **Memtables.** A memtable key is `LEB128(id) ++ key`, held as a composite
  `(prefix, key)` compared as if concatenated, so no lookup builds it.
  LEB128 is prefix-free, so keys stay in user order within a keyspace and the
  keyspaces occupy disjoint, ordered ranges. The prefix is 1 byte for the
  first 128 IDs and at most 5; memtables charge it with the entry. One
  `WriteBatch` across keyspaces is one memtable insertion at one sequence
  range, which makes it atomic without a cross-keyspace protocol.
- **Flushes split by keyspace.** The flush merge finds each record's keyspace
  as the last prefix at or below it, cuts the output there, strips the
  prefix and publishes each keyspace's runs into that keyspace's level 0.
  Records of dropped keyspaces are skipped, which frees their memory. No run
  holds two keyspaces.
- **Levels and scheduling.** Each keyspace sizes its own levels from its own
  bottom. The compaction worker scores every keyspace's plan and takes the
  highest; a keyspace whose level 0 is at the writer stop always ranks first,
  and ties go round-robin from a cursor. Under memory pressure it takes the
  keyspace with the most reclaimable bytes. Quiet level-0 runs rank below
  ordinary work in every keyspace. The piece allowance and builder count are
  shared, so one keyspace's job never takes more than they allow.
- **Reads.** A point read probes the memtables with the prefixed key, then
  only its keyspace's directory. A scan merges the memtables, bounded by the
  prefix, with its keyspace's groups. A snapshot holds one shared list of
  every keyspace's directory, taken with one reference count; a keyspace not
  in the list reads as empty. A publication spanning several keyspaces
  rebuilds this shared list once.
- **Drop and recreate.** Dropping takes the flush and compaction locks,
  removes the keyspace from the catalog and retires its runs; snapshots that
  hold them keep reading. Handles and batches carry the ID, so after a drop
  they fail, and batches are checked again at publication. A recreated name
  gets the next ID, so the dropped keyspace's handles and memtable entries
  never reach it.

## Runs

A run is an immutable, self-contained sorted file image held in pooled 1 MiB
chunks. It is built in one pass: blocks are cut as records arrive, each sampled
segment buffers its first 4 MiB raw to build a dictionary, and compressible
segments continue streaming following blocks against that dictionary until a
stopping rule closes the scope.

- **Blocks.** Prefix-compressed records with restart keys every sixteen
  records. Block length adapts: each segment compresses its first 64 KiB at
  zstd level 1 without a dictionary, and that ratio sets the raw length
  expected to compress to the target (1 KiB by default), capped at 64 KiB raw.
- **Segments.** Each segment stores a raw-content zstd dictionary of at most
  16 KiB, sampled from evenly spaced records of its buffered 4 MiB raw prefix;
  blocks decode independently with it, by reference, through one reusable
  context. After compressing the buffered prefix, a Zstandard segment streams
  subsequent blocks directly against the same prepared context until stored
  frames reach 16 times the dictionary length (256 KiB for a full dictionary),
  raw bytes reach 8 times the segment length (32 MiB), or a 64 KiB check
  window's stored-to-raw ratio exceeds the prefix's by more than a quarter of
  the pro-rated dictionary cost. High-entropy segments already exceed the stored
  target in their 4 MiB prefix and close at once.
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
never refused, so maintenance can always finish and free space. Quiet
level-0 compaction uses the same estimates and charges for pieces and
retired sources. Snapshots and checkpoints may retain the inputs after
publication. Readers' buffers and allocator overhead are outside the budget.

## Checkpoints

A checkpoint pins one structural snapshot at a flush rotation, flushes the
tables it covers with the normal builder and writes outside the state lock.
Each run is written once under a stable ID as its exact bytes plus its filter;
later checkpoints reuse those files. A manifest records the keyspaces, levels,
flush groups, bounded views, sequence overrides, the ceiling and application
metadata. The
commit is the rename of `CURRENT` after every file and directory is synced; GC
then keeps the files of the last two generations. Recovery reads exactly the
generation `CURRENT` names and validates every frame, dictionary, record and
filter before any worker starts. A separate merge checks for conflicting
copies in keyspaces whose run ranges overlap.

Manifest version 1, all integers little-endian, closed by a CRC-32:

```text
header (88 bytes)  magic "TSRCKPT\x01", version 1, flags (bit 0: metadata present);
                   generation, sequence ceiling, records, next run ID and
                   previous generation (u64 each); next keyspace ID, keyspace
                   count, keyspace-table bytes and run count (u32 each);
                   bounds bytes and metadata bytes (u64 each)
keyspace table     per keyspace, ascending ID: ID u32, level depth u32,
                   name length u8, name
run entries        72 bytes each: run ID u128, keyspace u32, level, flags,
                   sequence override, flush group, file length, run length,
                   CRC and two bound lengths; grouped by keyspace in table
                   order, then by level and flush
bounds, metadata   the bounded views' keys, then the application blob
```

Recovery checks the version before the length, then rejects, as corruption,
a run of an unknown keyspace, a duplicate name or ID, an ID at or above the
next ID, a malformed table (bad name, zero depth, descending IDs, wrong
length, more than 4096 keyspaces) and runs out of table order. The run
format and `CURRENT` are also version 1.

## Concurrency

One reader–writer lock guards the state; a writer-priority gate keeps new
readers from overtaking a waiting writer. Publication and writes take it
exclusively and briefly. Uncontended small writes take the state lock directly
with one atomic check and no copy; when writers overlap, a combiner gathers
requests across cache-line-isolated thread shards so one leader takes the state
lock once, applies up to 64 requests (capped at 1 MiB of payload) in order,
releases the lock, wakes its followers and hands leadership to a waiting writer.
Bulk ingestion and larger batches keep their direct path. Reads take immutable
references under the shared lock and decode after releasing it. Builds,
compression and checkpoint I/O hold no lock. A flush worker and a compaction
worker run per engine; compaction can use several builders. Snapshots hold no
lock and own their sources, so maintenance keeps only the newest version of
each key.

## Design decisions

**Adaptive compressed-size blocks** (1 KiB target, 64 KiB raw cap).
- A 2 KiB target: about 30% fewer point reads per second for about 6% memory.
- zstd `targetCBlockSize` early stop: frames are too small to split, and
  `maxBlockSize` 8 KiB makes grouped JSON 57% larger.
- Key/value-separated blocks with early-stop decoding: zstd decodes the whole
  frame anyway.
- A raw block floor: no measured benefit.

**Per-segment raw-content dictionaries of 16 KiB, sampled and amortized.**
- Fixed 4 MiB raw scope: on globally redundant grouped data, 4 MiB raw
  compresses to about 38 KiB of frames, making the 16 KiB dictionary nearly a
  third of stored bytes. Continuing a segment against the same prepared
  dictionary up to 16 times the dictionary length in stored frames (capped at
  8 times the segment length raw, with a cost-based drift guard over 64 KiB
  windows) reduces bottom-level stored bytes per record by about 23% on
  globally redundant grouped data and 15% on clustered data while leaving
  weakly compressible workloads at one sampled prefix.
- Scope widening without a cost-based drift guard (fixed multipliers or a
  coarse 25% relative drift guard over 1 MiB windows): on workloads where
  consecutive key groups carry disjoint random payloads, frame growth from
  stale group strings exceeds the saved dictionary bytes and increases stored
  bytes by over 10%. Checking drift against a quarter of the pro-rated
  dictionary cost over 64 KiB windows closes early at group boundaries and
  keeps those workloads smaller as well.
- Trained dictionaries: slow to build and to load.
- Digested entropy tables reduce decode CPU. Their residency and less
  adaptive sequence coding increase memory on grouped data. Loading the
  serialized tables on every decode increases CPU.
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
- Eagerly scheduling every split slice of a partially merged flush group
  would spend a rewrite on a range that can still collect other writes.
  Deferring below-threshold slices until quiet keeps ordinary scoring and
  piece bounds intact, and the worker stops timing its wait when level 0
  empties.
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

**Keyspaces share the memtables; each has its own runs and levels.**
- One memtable per keyspace: each would need its own budget and flushes, and
  a batch across keyspaces would have to commit to several tables at once.
- Runs holding several keyspaces behind their prefixes: a keyspace's scans
  and compactions would read and rewrite the others' data, and a drop could
  not release its runs at once.

**Keyspace IDs only in memtables, not in runs.** Flushes cut at keyspace
boundaries, so a run's keyspace is one manifest field and its keys carry no
prefix: zero bytes per key in memory and on disk, and blocks, filters,
dictionaries, copy-forward and sequence erasure need no change. The cost is
at least one run per keyspace a flush touches, which many tiny keyspaces
feel as extra indexes, filters and dictionaries.
- Prefixed keys in runs as well: a byte or more per key before prefix
  compression, filters and dictionaries trained across keyspaces, and the
  multi-keyspace runs rejected above.

**One level-0 writer stop shared by all keyspaces.** Writers wait when any
keyspace's level 0 reaches twelve flushes. The memtables are shared, so one
keyspace's backlog cannot be held back alone: the stop bounds that
keyspace's read fan-out at the price of stalling writes to the others, and
the scheduler ranks a keyspace at the stop first so the stall is short.
- Per-keyspace stops: a write to a stalled keyspace would still enter the
  shared memtable, so the stop could not hold without per-keyspace tables.
- A snapshot holding one reference per keyspace: taking it would cost one
  reference count per keyspace; the engine keeps one shared list of the
  keyspaces' directories instead.

**Sharded write-group combining with bounded follower spinning.** An uncontended
writer bypasses the queue with a single atomic compare-and-swap and copies
nothing. Under contention, writers publish owned requests into 16
cache-line-isolated shards tracked by an atomic bitmask, and one leader drains
up to 64 requests or 1 MiB of payload per exclusive state-lock acquisition,
re-draining shards that fill while the group applies. Followers spin briefly
before parking on a per-request condition variable, avoiding kernel wake
handoffs for short memtable groups while parking during flushes, large batches
and level-0 stops.
- One global submission mutex with immediate condvar parking: every writer
  still serialized on the queue mutex and paid kernel park/wake latency per
  group, capping multi-writer scaling for small writes.
- A fairer lock alone: still costs one exclusive handoff per operation.
- A concurrent memtable: deferred; requires replacing the copy-on-write
  single-writer B+ tree.
