# Tessera

Tessera aims to become a RAM-first, compressed LSM key-value database written
in Rust.

## Status: Milestone 5C

Two physical storage layers exist, with mutable storage above them.

A **block** stores versioned records, ordered by user key ascending then
sequence number descending, with prefix-compressed keys and a restart point
every 16 records. `BlockBuilder` encodes records and `Block` validates bytes
once, then provides allocation-free newest-version lookup and an ordered cursor
over every stored version. `BlockView` opens bytes already known to be valid in
constant time, and `BlockPosition` walks a block without borrowing it.

A **run** is a self-contained sequence of independently compressed blocks in
contiguous bytes with a sparse index of each block's last user key.
`RunBuilder` consumes records in internal-key order, cuts blocks near 16 KiB of
uncompressed data by default, keeps ordinary version chains whole and stores them
uncompressed or with LZ4 or Zstandard (level 3 is the provisional default). Reads take a reusable `ReadWorkspace`:
`Run::get` finds the newest version of a key, including tombstones, by decoding
at most one block. `Run::cursor` iterates across blocks; `RunCursor::seek` moves
to the first key at or above a target and `RunCursor::seek_prefix` iterates only
while keys share a prefix, both yielding every stored version. `Run::new`
validates untrusted bytes completely. `RunView` offers the same reads on bytes
already known to be valid. Each run stores its smallest key once, beside the index,
and reuses the last block's index key as its largest, so `key_range`
reports both bounds without decoding anything. `Run::new` checks them against
the records.

`MergeCursor` merges several runs, which may overlap and use different codecs
and block sizes, into one stream in internal-key order, with the same `seek` and
`seek_prefix`. It yields every physical record: all versions, tombstones, and
every copy of an internal key found in more than one run, in input order. Only
`MergeWorkspace` holds its state, one decoded block per input, and returned
records borrow from it, so iteration copies no values and allocates nothing.
An input is not decoded until the merge reaches its smallest key.

`RunSet` reads a fixed collection of runs as the latest state of each key:
`get` for one key, and a cursor with `seek` and `seek_prefix` for live keys in
order. The newest sequence number decides a key, and a tombstone hides it.
Identical copies of a record count once. Copies that differ fail the read
with `ReadError::ConflictingCopies`, because run order must not choose a winner.
There are no snapshots: a reader sees the newest version only.
Key ranges exclude runs that cannot hold a key or prefix, without decoding them.

The byte formats and their invariants are documented in
[`src/block/mod.rs`](src/block/mod.rs) and [`src/run/mod.rs`](src/run/mod.rs).

A **memtable** holds the latest update of each key in memory, in key order,
under a memory budget. The caller supplies strictly increasing sequence numbers
and a rejected update changes nothing. Freezing consumes the table, and a
`FrozenMemtable` builds compressed runs on demand, one at a time, keeping every
tombstone. A frozen table is only borrowed by a flush, so a flush can be
repeated.

An **`Engine`** combines them: `put`, `delete`, `get`, ordered and prefix scans,
and an explicit synchronous `flush`. It assigns sequence numbers and holds one
active memtable, at most one frozen memtable and the published runs behind a
single reader-writer lock, with a gate that keeps readers from starving writers.
Single-key reads and writes are linearizable. A `ReadGuard` gives a stable view
for scans and **blocks writers while held**. A flush freezes the active table,
compresses outside the lock, and publishes its runs while removing the frozen
table in one step; an abandoned or rejected flush leaves the table readable for
a retry. Memtable budgets and limits on run count and bytes bound what is kept.

**Compaction** is explicit and synchronous. `compact` merges runs into new
disjoint runs that keep only the newest version of each key, collapse identical
copies, fail on copies that differ at any version, and drop a tombstone only
when the caller says no older value can lie outside the merge. Workspace and
output are limited and checked, the workspace before anything is decoded.
`Engine::compact` selects inputs under the shared lock, merges without the lock,
and replaces exactly its inputs in one step, after checking they are still
published; runs flushed meanwhile and the memtables are newer and stay. The
policy merges the smallest runs covering the most deeply overlapped key once
more than `max_overlap` runs share a key, and merges neighbouring short runs
once there are many disjoint runs. It is not leveled.

There is no Bloom filter, MVCC, snapshot older than the present, write-ahead
log or persistence, and neither flushing nor compaction happens in the
background.

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
scripts/validate.sh bench    # block and run size, timing and allocations
scripts/validate.sh geometry # run block size and codec comparison
scripts/validate.sh prefix   # prefix scan comparison
scripts/validate.sh merge    # multi-run merge measurements
scripts/validate.sh logical  # latest-state read measurements
scripts/validate.sh ranges   # key-range pruning measurements
scripts/validate.sh memtable # memtable and flush measurements
scripts/validate.sh engine   # integrated engine measurements
scripts/validate.sh compaction # sustained writes with and without compaction
colima stop
```

Benchmark method, reproduction steps and findings are in
[`docs/benchmarks.md`](docs/benchmarks.md).

Each step gets a 3 GiB cgroup with no swap, 2 CPUs, 256 processes, 2 build jobs,
2 test threads and a timeout. Use `scripts/validate.sh exec SECONDS CMD...` for
anything else that could loop or allocate without bound, including tests of
deliberately broken code. A bare `cargo test` on macOS has no memory limit.

## License

MIT
