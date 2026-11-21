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
name scenarios to run only those. `clustered` (time-ordered load of grouped
JSON, then updates to the newest 10% of entities) runs only when named.

Audit flags: `--copy-audit` prints, per bottom input segment of every level-0
→ bottom merge, how many blocks could be copied verbatim (no other input's key
in range, all sequences 0, no tombstone), and the level-0 block bytes those
merges decoded. `--c2` recompresses evenly spaced bottom segments (at least
64 MiB raw) as the builder does, with Zstandard `targetCBlockSize` and
`maxBlockSize` variants, and prints stored bytes, raw bytes an early-stopping
point read would decode, and relative decompression time. `--write-only`
stops after the drain, for profiling. `--worst` also measures reads and
memory with level 0 at its fullest: new jobs are held off while random updates
fill it to the writer stop. `--memtable VALUE_LEN` (with
`tools-memory`) fills one memtable to its budget with random keys and prints
its live heap per entry beyond key and value. `--keyspaces N` spreads the
same records over N keyspaces, record `i` in keyspace `i % N`, and checks
every read in its keyspace; the probe rows and `--worst` need one keyspace.

The hidden `diagnostics` feature provides the counters. It is not part of the
stable API. The programs in [`examples/`](../examples) demonstrate the public
API: `basic`, `keyspaces`, `offsets` and `hot_reads`.

The macOS [comparison harness](bench/README.md) measures Tessera and RocksDB.

On Linux, add `--features diagnostics,tools-memory` to count current and peak
Rust allocation bytes and sample `/proc/self/status` plus engine memory
components every second and at phase ends. Run one named scenario per process.
Native Zstandard allocations are outside the Rust counter. `--threads COUNT`
sets builders; `--threads default` uses the engine default (otherwise one).
