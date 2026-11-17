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
level; records written into the bottom level, how many had their sequence
erased, the filter probes that took and why the rest kept theirs; and decoded
bytes per get, blocks per scan and write amplification.
`--settle` then compacts everything into the bottom level and measures again,
`--variant LABEL` labels the rows and `--scale SHIFT` runs smaller scenarios;
name scenarios to run only those.

The hidden `diagnostics` feature provides the counters. It is not part of the
stable API. The [basic example](../examples/basic.rs) demonstrates the public
API.

The macOS [comparison harness](bench/README.md) measures Tessera and RocksDB.
