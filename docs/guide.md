# Guide

This guide covers what an application needs to know beyond the
[README](../README.md): how operations behave across keyspaces, how memory is
budgeted, how to use handles from several threads, what a checkpoint
guarantees, and the limits. The rustdoc (`cargo doc --open`) states each
method's contract; [ARCHITECTURE.md](../ARCHITECTURE.md) explains the
internals.

## Operations and consistency

Keys and values are byte strings. Keys compare bytewise, and empty keys and
values are valid. Every key lives in one [keyspace](#keyspaces); the same key
in two keyspaces is two records.

- **Writes.** `Keyspace::put` and `Keyspace::delete` change one key.
  `Engine::write` applies a `WriteBatch` atomically: a read sees all of it or
  none of it, and a failed write applies none. A batch may span any number of
  keyspaces of one engine; later operations on one key win. A batch naming a
  dropped keyspace fails whole with `KeyspaceDropped`.
- **Large batches.** A batch charged at least half of `memtable_size` whose
  keys in one keyspace strictly ascend is built into runs outside the state
  lock, so reads and other writes continue while it builds. So is any batch
  charged more than `memtable_size`. Both are still atomic.
- **Latest-state reads.** `Keyspace::get` copies the value it finds.
  `Engine::reader` returns a `Reader` whose `get` and `multi_get` each read
  the state of their moment: one `multi_get` is consistent across its keys,
  but two calls may see different states. A scan pins the state from the
  call that starts it until the reader's next call or drop, so it sees no
  later write.
- **Snapshots.** `Engine::snapshot` captures every keyspace at one moment.
  Each read through `Snapshot::reader` sees that moment, so several gets and
  scans, across keyspaces, agree. Snapshot reads borrow values in place.
- **Scans** return each live key once, in ascending order. `Reader::prefix`
  matches raw bytes; `Reader::range` accepts any std range over byte strings,
  `..` or a pair of `Bound`s. Deleted keys never appear.

## Keyspaces

`Engine::keyspace(name)` opens a keyspace, creating it if it is missing.
`Engine::existing_keyspace(name)` opens it only if it exists and returns
`None` otherwise, which suits code that reopens a directory and expects its
keyspaces: a misspelt name is reported instead of creating an empty keyspace.
Names are 1 to 64 bytes of ASCII letters, digits, `_`, `.` and `-`, compared
exactly. `Engine::keyspaces` lists the live ones in bytewise order.

`Engine::drop_keyspace(name)` removes a keyspace and its data and returns
whether it existed. Afterwards its handles, and batches that name it, fail
with `KeyspaceDropped`; snapshots taken before the drop still read it. The
name can be created again: the recreated keyspace starts empty, gets another
internal ID, and handles of the dropped one never reach it. Through a
snapshot, a keyspace created after the snapshot reads as empty. Creations and
drops become durable at the next checkpoint.

Keyspaces are cheap but not free. Each has its own levels and runs, so a
flush that touches *k* keyspaces publishes at least *k* runs, each with its
own index, filter and dictionaries. Hundreds of keyspaces are fine; many tiny
ones cost more stored bytes than one keyspace holding the same records
with a key prefix.

## Memory and admission

`Options::memory_limit` is one writer-admission budget for the whole engine,
unlimited by default. `Stats::memory_used` sums:

- charged memtables: their arena bytes, including overwritten values, plus
  80 bytes and the keyspace prefix (1 to 5 bytes) per entry, plus history
  that snapshots pin;
- stored runs with their filters, indexes and block counts;
- sources released by the engine but still held by snapshots and scans
  (`Stats::snapshot_bytes`);
- the chunk pool's free reserve (at most 32 MiB) and unused checked-out
  capacity;
- estimates of builds in progress.

Below 90% of the limit writers proceed. Above it they wait briefly for a
flush or compaction to publish. At the limit they request a flush and
reclamation, and fail with `MemoryLimit` only when live data and held
snapshots leave nothing to reclaim. Reads, snapshots, checkpoints and
maintenance never wait on the budget, so maintenance can always finish and
free memory. Writers also wait, whatever the budget, while two frozen
memtables await a flush or while any keyspace's level 0 holds twelve
flushes.

Across keyspaces the budget is shared. The memtables are shared too, so one
busy keyspace can fill them; runs, filters, retained sources and build
estimates are summed over keyspaces. `Stats::keyspaces` gives each keyspace's
`stored_bytes`, which add up to `Stats::stored_bytes`. Memtable bytes are not
attributed to keyspaces. Dropping a keyspace releases its runs at once
(unless snapshots hold them); its memtable entries stay charged until the next
flush discards them.

The budget is not a cap on the process's memory. Reader buffers (up to 1 MiB
retained per buffer, pooled per engine), queued write-combiner copies (up to
1 MiB per waiting writer), allocator overhead and native Zstandard contexts
are outside it. An active memtable plus two frozen ones can charge about
192 MiB at the default `memtable_size`.

### Resident memory on Linux

With glibc, freed buffers can stay resident, so RSS may remain well above
`memory_used` after compaction. Setting
`GLIBC_TUNABLES=glibc.malloc.mmap_threshold=524288` at process start lets
large freed allocations return to the operating system, which removes most of
that gap. It affects every allocation in the process and can change
throughput, so measure it with the application's workload. Allocators that
return freed memory, such as mimalloc or jemalloc with default settings,
avoid the retention too. See
[performance](performance.md#linux-resident-memory).

## Threads and handles

`Engine`, `Keyspace` and `Snapshot` are cheap `Clone + Send + Sync` handles:
clone them into threads, or share references with `std::thread::scope`.
`WriteBatch`, `Stats` and `Error` are `Send + Sync`.

A `Reader` is `Send` but not `Sync`; take one per thread and keep it. Its
buffers, decompression contexts and result vector are reused across calls and
return to a pool in the engine (two per CPU) when it drops. Values a reader
returns borrow it until its next call, so copy what must outlive that call.
A `MultiGet` returns its vector to the reader when dropped, so it holds the
reader's borrow until it is dropped or goes out of scope; drop it explicitly
before the next call in the same scope. A `Scan` borrows its reader for its
whole life, so to look keys up while scanning, take a second reader of the
same snapshot.

No public call holds a lock when it returns, so there is no lock ordering for
an application to follow. Each engine runs a flush worker and a compaction
worker; compaction builds up to `background_threads` pieces at once. The
engine stops when `Engine::close` is called or when its last `Engine` and
`Keyspace` handle is dropped: both join the workers and release `LOCK`. After
`close`, other handles fail with `Stopped`. Snapshots, and readers of them,
keep working after the engine closes.

## Durability, checkpoints and metadata

Nothing reaches disk except a checkpoint. There is no write-ahead log:
closing, dropping or losing the process discards every write since the last
committed checkpoint.

`Engine::checkpoint` pins the state at one moment, at a memtable rotation,
then writes outside the state lock while writes and compactions continue. It
holds every key live at that moment and nothing later. Run files are written
once under stable IDs and reused by later checkpoints, so a checkpoint writes
only runs the previous one lacks. The commit is the rename of `CURRENT`, after
every file and directory is synced; the previous generation's files are kept
until the next commit. A failure before the commit leaves the previous
checkpoint intact. An `Io` error from a checkpoint says through
`Error::checkpoint_committed` whether the commit had happened; if it had, the
checkpoint is the one recovery opens.

`Engine::checkpoint_with(metadata)` stores an opaque blob of at most 16 MiB
in the same commit, and `Engine::metadata` returns the latest committed blob,
including after reopening. A plain `checkpoint` carries the previous blob
forward, so periodic checkpoints never erase it.

**Recording a position in an input stream.** A consumer of a log such as a
Kafka topic must resume exactly where its durable state ends.

- *Exact under concurrency:* keep offsets in a keyspace and write them in the
  same `WriteBatch` as the records they cover. Any checkpoint then holds
  offsets that match its data, whatever else is written meanwhile. After
  reopening, read the offsets keyspace and resume from there.
  [`examples/offsets.rs`](../examples/offsets.rs) does this.
- *`checkpoint_with`:* the blob describes the checkpoint only if no write
  happened between choosing it and the call, because the checkpoint pins the
  state when the call starts. With a single writer that checkpoints between
  its own batches, that holds.

## Recovery and the on-disk layout

`Engine::open(dir, options)` creates `dir` if needed, takes `dir/LOCK` for the
engine's life, and recovers the checkpoint `CURRENT` names, or starts empty
when there is no `CURRENT`. It never falls back to an older generation.
Recovery checks lengths, checksums, every frame and record, filter membership,
sequence bounds and copies across runs before it returns; any failure is
`Corruption` with a message naming what failed, and no engine starts. Run
files left by an interrupted first checkpoint are removed by the next commit.

```text
dir/
  CURRENT                      names the committed generation
  LOCK                         advisory lock, held while an engine is open
  runs/<32 hex digits>         one immutable run: blocks, index, Bloom words, CRC-32
  ckpt-<20 digits>/MANIFEST    keyspaces, runs by keyspace and level, metadata
```

Format version 1 covers the run files, the manifest and `CURRENT`. The
manifest lists the keyspaces (ID, name, level depth) and the next keyspace ID,
then each run with its keyspace, level, flush group, bounds and checksum,
then the metadata. A directory of another version fails to open with
"unsupported manifest version N". [Releases](releases.md#versioning-and-compatibility)
states the compatibility policy.

Durability assumes a local POSIX filesystem with working file and directory
`fsync` and atomic rename. CRC-32 detects accidental damage; it does not
authenticate files. Network filesystems and changes made by other programs
are unsupported, and `LOCK` excludes only cooperating processes.

## Errors

Every fallible call returns `tessera::Error`. Match on `Error::kind()`;
`Display` gives a message with details, `Error::path` the file of a
filesystem failure, and `source()` the underlying `io::Error`.
`From<Error> for io::Error` lets `?` work in functions returning `io::Result`.

| Kind | Meaning | Typical response |
| --- | --- | --- |
| `Config` | an option out of range, or a checkpoint of an in-memory engine | fix the configuration |
| `InvalidInput` | a key, value, record, metadata or keyspace name over its limit, or a keyspace past the limit | reject the input |
| `MemoryLimit` | admission found nothing to reclaim | drop snapshots, delete data or raise the limit |
| `KeyspaceDropped` | the keyspace was dropped | open the name again for an empty keyspace |
| `UnknownKeyspace` | a handle or batch of another engine | a programming error |
| `Locked` | another engine or process holds `LOCK` | open a different directory, or wait |
| `Io` | a filesystem call failed or a thread could not start | check `checkpoint_committed`; retry later |
| `Corruption` | stored data failed a check, or two copies of one update disagree | restore the directory from a backup |
| `Stopped` | the engine was closed | stop using the handle |
| `Background` | a background worker panicked (reported by `close`) | report a bug |

A panic while the state lock is held poisons the engine; later calls that
take the lock panic. A background worker that panics stops; the engine keeps
working, building synchronously where needed, and `close` reports
`Background`.

## Limits

| Limit | Value |
| --- | --- |
| key length (`MAX_KEY_LEN`) | 64 KiB |
| value length (`MAX_VALUE_LEN`) | 64 MiB |
| one record's charge | key + value + 80 bytes + keyspace prefix, at most `memtable_size` |
| checkpoint metadata (`MAX_METADATA_LEN`) | 16 MiB |
| keyspace name (`MAX_KEYSPACE_NAME_LEN`) | 1–64 bytes of `[A-Za-z0-9_.-]` |
| live keyspaces | 4096 |
| keyspaces created over an engine's life | 2³², IDs never reused |
| `memtable_size` | 85 bytes to `usize::MAX / 2` |
| `level_ratio` | at least 2 |
| `background_threads` | at least 1 |
| `block_size` | 1 byte to 64 KiB, stored |

The dataset must fit in memory. Read/write transactions, replication, a disk
tier and Windows are outside the design; see [releases](releases.md#limitations).
