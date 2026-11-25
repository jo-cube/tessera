# Diagnostics

`cargo run --release --features diagnostics --example counters` prints work
and memory counters by level for four default scenarios: grouped JSON (`A`) and
weak locality (`B`), each 2,097,152 keys with 600-byte values; `A-large`,
4,194,304 keys with 48-byte values; and `small`, 16,777,216 keys (20-byte
structured keys, ~52-byte compact JSON values). Each loads keys in random order
with automatic flushes, updates half of them at random, waits for background
compaction and checks every read path against an independent value oracle.

Long-format CSV rows report raw bytes decoded and compressed and stored bytes
written by flush and by compaction into each level; stored bytes by component
(frames, dictionaries, segment table, index, filters, footer, physical),
blocks and records per level, plus per-record component breakdown (`per_record`); records
written into the bottom level, how many had their sequence erased, the filter
probes that took and why the rest kept theirs; decoded bytes per get, blocks
per scan, write amplification and writer wait / lock counters (`waits`).
`--settle` then compacts everything into the bottom level and measures again,
`--variant LABEL` labels the rows and `--scale SHIFT` runs smaller scenarios;
name scenarios to run only those. `clustered` (time-ordered load of grouped
JSON, then updates to the newest 10% of entities) and `grouped-random`
(1,048,576 keys with the comparison harness's dataset A values, ~1 KiB JSON
whose 360-, 260- and 60-byte random strings are shared by each group of 64
members) run only when named.

Workload-shape flags: `--zipf` runs scrambled Zipfian gets at θ = 0.99 and
θ = 0.80, reporting decoded bytes, CRC bytes, filter probes and a block-reuse
curve (exact LRU stack distance and S3-FIFO hit rates for N ∈ {16, 64, 256,
1024, 4096} blocks alongside the decoded bytes those N blocks occupy), followed
by a scrambled Zipfian update phase (θ = 0.99) and re-measured reads.
`--read-shapes` measures point gets with 0%, 50% and 100% misses (filter
probes, false positives, decoded bytes per miss and per-level block decodes),
absent-prefix scans, and short scans of 1, 5 and 10 records with per-level
block and decoded-byte breakdowns. `--reuse` settles the bottom level, then
prints work per get (blocks, decoded, CRC bytes, filter probes, per-level mean
raw block length) for uniform hits, uniform in-range misses, Zipfian hits
(θ = 0.99, 0.80) and Zipfian misses (absent keys drawn over a miss space as
large as the live set), and byte-capacity cache curves at 0.25, 1, 4, 16 and
64 MiB: LRU by byte-weighted stack distance and byte-weighted S3-FIFO, for a
whole-block cache charged decoded bytes, a record cache charged key + value +
64 B (also under 95/5 and 50/50 read/update mixes, where an update removes the
record) and a negative cache charged key + 64 B. Reuse streams are ten times
the 64 MiB record count long; the first half warms the caches.

Audit flags: `--copy-audit` prints, per bottom input segment of every level-0
→ bottom merge, how many blocks could be copied verbatim (no other input's key
in range, all sequences 0, no tombstone), and the level-0 block bytes those
merges decoded. `--c2` recompresses evenly spaced bottom segments (at least
64 MiB raw) as the builder does, with Zstandard `targetCBlockSize` and
`maxBlockSize` variants, and prints stored bytes, raw bytes an early-stopping
point read would decode, and relative decompression time. `--dict-scope`
settles the bottom level, takes consecutive segments and evaluates reusing one
segment's dictionary across subsequent segments under fixed scope multipliers
and stopping rules, reporting per-offset frame drift, frames plus dictionaries
bytes per record and decode time per frame. `--write-only`
stops after the drain, for profiling. `--worst` also measures reads and
memory with level 0 at its fullest: new jobs are held off while random updates
fill it to the writer stop. `--memtable VALUE_LEN` (with `tools-memory`,
optional `--memtable-key KEY_LEN`, default 16) fills one memtable to its
budget with random keys and prints its live heap per entry beyond key and
value. `--keyspaces N` spreads the same records over N keyspaces, record `i` in
keyspace `i % N`, and checks every read in its keyspace; the probe rows,
`--read-shapes`, `--zipf`, `--reuse` and `--worst` need one keyspace.

The hidden `diagnostics` feature provides the counters. It is not part of the
stable API. The programs in [`examples/`](../examples) demonstrate the public
API: `basic`, `keyspaces`, `offsets` and `hot_reads`.

The macOS [comparison harness](bench/README.md) measures Tessera and RocksDB.

On Linux, add `--features diagnostics,tools-memory` to count current and peak
Rust allocation bytes and sample `/proc/self/status` plus engine memory
components every second and at phase ends. Run one named scenario per process.
Native Zstandard allocations are outside the Rust counter. `--threads COUNT`
sets builders; `--threads default` uses the engine default (otherwise one).
`TESSERA_COUNTERS_MEMTABLE_MIB` sets `Options::memtable_size` in MiB for
every scenario and variant. Use the same value on both sides of a comparison.
`--compression zstd|fast-decode|uncompressed` sets `Options::compression`
(`Zstd`, the default; `ZstdFastDecode`; `Uncompressed`); the `config` line
records it.

## Zstandard entropy measurements

Run the isolated harness under validation limits, with scenario names from
the counters tool. The optional scale shift reduces each input for smoke runs.

```sh
scripts/validate.sh exec 1200 cargo run --locked --release \
  --manifest-path tools/entropy/Cargo.toml -- A B A-large small
```

The harness settles the bottom level and samples evenly spaced segments. It
inspects stored frames, checks that raw-dictionary recompression reproduces
them and verifies every decoded block in each mode. Timed single-thread
replays rotate variant order. Build and dictionary-load rows use thread CPU
time. Build measurements start with encoded raw blocks and sampled dictionary
content.

- `sample` gives sampled segment, frame, raw-byte and record counts.
- `layout` gives frame and Zstandard block counts, then total, header,
  raw-block, RLE-block, literal-section and sequence-section bytes.
- `mode` gives each literal or sequence mode's section count and explicit
  table-description bytes. `reuse` counts frames with fresh Huffman or FSE
  descriptions and frames that reuse each type of table from the dictionary.
- `storage` gives frame bytes, dictionary bytes, resident DDict bytes,
  total and peak local CDict bytes including the content copy, and frames
  with fresh non-RLE descriptions.
- `timing` gives the repeat number, wall nanoseconds to decode the sample,
  thread CPU nanoseconds to build it, and the portions spent finalizing
  dictionaries and digesting DDicts.
- `proof` gives segment counts sampled, spliced and accepted by zstd, followed
  by verified frames and the counts that reused dictionary Huffman or FSE
  tables.
