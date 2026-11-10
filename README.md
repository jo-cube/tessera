# Tessera

Tessera aims to become a RAM-first, compressed LSM key-value database written
in Rust.

## Status: Milestone 1

Only the immutable data block exists. It stores versioned records, ordered by
user key ascending then sequence number descending, with prefix-compressed keys
and a restart point every 16 records. `BlockBuilder` encodes records and rejects
invalid input. `Block` validates untrusted bytes once, then provides allocation
free newest-version lookup and an ordered cursor over every stored version.
`BlockView` opens bytes already known to be valid in constant time, for example
a block decompressed again into a reusable buffer, and `BlockPosition` walks a
block without borrowing it.

The byte format and its invariants are documented in
[`src/block/mod.rs`](src/block/mod.rs).

There is no compression, multi-block run, Bloom filter, memtable, compaction,
MVCC, write-ahead log or persistence yet.

## Validate

Validation runs offline in a Linux container with hard limits, using a Colima
VM (2 CPUs, 4 GiB) and an image built once from a pinned Rust base:

```sh
colima start
scripts/validate.sh image    # once, adds rustfmt and Clippy (uses the network)
scripts/validate.sh verify   # prove the limits are enforced
scripts/validate.sh          # fmt, Clippy, tests and docs
scripts/validate.sh full     # also the large-allocation boundary tests
scripts/validate.sh bench    # encoded size, timing and allocations
colima stop
```

Each step gets a 3 GiB cgroup with no swap, 2 CPUs, 256 processes, 2 build jobs,
2 test threads and a timeout. Use `scripts/validate.sh exec SECONDS CMD...` for
anything else that could loop or allocate without bound, including tests of
deliberately broken code. A bare `cargo test` on macOS has no memory limit.

## License

MIT
