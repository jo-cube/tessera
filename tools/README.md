# Diagnostics

`cargo run --release --features diagnostics --example counters` prints work
and memory counters by level for three scenarios: grouped JSON (A) and weak
locality (B), each 2,097,152 keys with 600-byte values, and A-large, 4,194,304
keys with 48-byte values. Each loads keys in random order with automatic
flushes, updates half of them at random, waits for background compaction and
checks every read path against an independent value oracle.

Long-format CSV rows report raw bytes decoded and compressed and stored bytes
written by flush and by compaction into each level; stored bytes by component
(frames, dictionaries, segment table, index, filters, footer) and records per
level; and decoded bytes per get, blocks per scan and write amplification.
`--costs` also times Zstandard levels -3, -1, 1 and 3 on 64 MiB of the
scenario's own blocks. `--levels NON_BOTTOM,BOTTOM`, `--dictionary
BYTES|adaptive` and `--scale SHIFT` select an experiment policy or smaller
runs; name scenarios to run only those.

The hidden `diagnostics` feature provides the counters and policy overrides.
It is not part of the stable API. The [basic example](../examples/basic.rs)
demonstrates the public API.

The macOS [comparison harness](bench/README.md) measures Tessera and RocksDB.
