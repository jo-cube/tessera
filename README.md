# Tessera

Tessera aims to become a RAM-first, compressed LSM key-value database written
in Rust.

## Status: Milestone 3A

Two physical storage layers exist.

A **block** stores versioned records, ordered by user key ascending then
sequence number descending, with prefix-compressed keys and a restart point
every 16 records. `BlockBuilder` encodes records and `Block` validates bytes
once, then provides allocation-free newest-version lookup and an ordered cursor
over every stored version. `BlockView` opens bytes already known to be valid in
constant time, and `BlockPosition` walks a block without borrowing it.

A **run** is a self-contained sequence of independently compressed blocks in
contiguous bytes with a sparse index of each block's last user key.
`RunBuilder` consumes records in internal-key order, cuts blocks near 64 KiB of
uncompressed data, keeps ordinary version chains whole and stores them
uncompressed or with LZ4 or Zstandard. Reads take a reusable `ReadWorkspace`:
`Run::get` finds the newest version of a key, including tombstones, by decoding
at most one block, and `Run::cursor` iterates across blocks. `Run::new`
validates untrusted bytes completely. `RunView` offers the same reads on bytes
already known to be valid.

The byte formats and their invariants are documented in
[`src/block/mod.rs`](src/block/mod.rs) and [`src/run/mod.rs`](src/run/mod.rs).

There is no Bloom filter, memtable, compaction, MVCC, write-ahead log or
persistence yet.

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
colima stop
```

Each step gets a 3 GiB cgroup with no swap, 2 CPUs, 256 processes, 2 build jobs,
2 test threads and a timeout. Use `scripts/validate.sh exec SECONDS CMD...` for
anything else that could loop or allocate without bound, including tests of
deliberately broken code. A bare `cargo test` on macOS has no memory limit.

## License

MIT
