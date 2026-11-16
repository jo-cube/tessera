# Diagnostics

`cargo run --release --example counters` prints engine work counters for
fixed-size flushes of grouped JSON and weak-locality records. It checks reads
against an independent value oracle. `cargo run --release --example counters
-- buffers 64` uses a 64 MiB memtable and a larger ingestion workload.

The CSV includes stored frame, index and filter bytes; decoded bytes and blocks;
flush and run counts; sampled memtable charge; and write amplification. It uses
private compaction and Bloom-filter helpers, so it is a repository diagnostic.
The [basic example](../examples/basic.rs) demonstrates the public API.

The macOS [comparison harness](bench/README.md) measures Tessera and RocksDB.
