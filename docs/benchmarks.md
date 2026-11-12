# Benchmarks

The benchmark suite is `examples/bench.rs`. It drives the engine through its
public API only, with data from fixed seeds, so every run sees the same keys
and values. Run it through `scripts/validate.sh`, which applies the memory,
CPU, process and time limits and keeps every fixture on the `/tmp` tmpfs:

```sh
scripts/validate.sh bench                  # every mode, one release build
scripts/validate.sh bench lookups scans    # some modes
```

Timings vary with the machine and between passes; compare configurations
only within one environment, and repeat a pass rather than trusting one:

```sh
mkdir -p target/bench
for pass in 1 2 3; do scripts/validate.sh bench > "target/bench/suite-$pass.log"; done
scripts/summarize_bench.py target/bench/suite-*.log
```

`summarize_bench.py` takes the median of each cell across passes and reports
how far passes disagree.

| mode | workload | what it shows |
|---|---|---|
| `memtable` | 500,000 records of 8 + 100 bytes in a 1 GiB memtable: inserts, overwrites, gets, a scan and deletes | the cost of the write and read paths with nothing flushed |
| `lookups` | 500,000 records of 8 + 100 bytes in six storage shapes, seven lookup classes, two key layouts, beside a `BTreeMap` | point lookup cost, blocks decoded and allocations by shape and class |
| `scans` | 491,520 records in a hierarchical key layout, six shapes, prefixes selecting 0 to every record | seek and iteration cost of prefix scans by shape and result size |
| `readers` | 500,000 records; readers, scanners and a writer in pairs for 1.5 s each | read scaling on 2 CPUs and interference with a writer |
| `density` | three payloads of 35 B to 2.5 KiB, in a memtable and consolidated, beside a `BTreeMap` | heap per record, compression ratio and the read cost of density |
| `maintenance` | a 250,000-record flush; a compaction of 8 overlapping runs | flush and compaction throughput, stored size, and key filter size and construction memory |
| `snapshots` | snapshots taken after a write; 1,000,000 overwrites with 0–16 live snapshots replaced in turn | snapshot cost, and what live snapshots cost writes and memory |
| `ingest` | 1,000,000 writes by 1–4 writers over 400,000 keys, 4 MiB memtable, background compaction | sustained ingestion with automatic flushes, its latency tail and run shape |
| `mixed` | 500,000 writes beside 1–3 readers doing gets and prefix scans | readers and writers sharing the engine |
| `workloads-zstd`, `-lz4`, `-none` | read-heavy, balanced and write-heavy mixes from 2 clients over 491,520 records, background compaction; one codec per process | how the codec choice holds up with reads and writes together |
| `durable` | 200,000 durable writes from 1, 4 and 8 writers | group commit: throughput, latency and writes per sync |
| `recovery` | checkpoints of 500,000 records, recovery, and a reopen replaying 100,000 logged writes | checkpoint, recovery and replay rates |

The `workloads-*` modes are not in the default suite; run them by name
(`scripts/validate.sh bench workloads-zstd workloads-lz4 workloads-none`).

Kinds of result in this file:

- **Complete-engine, isolated:** the 5V read baseline and the 5W codec
  comparison: one operation class at a time through the public API, no
  concurrent writes.
- **Complete-engine, mixed:** the 5X section: reads and writes together,
  with automatic flushes and background compaction.
- **Run layer only:** "Compressed and uncompressed runs" and the block-cache
  simulation, which read runs without an engine.
- **Competitive baseline:** the 5Y section, Tessera against RocksDB on
  identical data and operations, from the separate harness in
  `tools/compare`; read-only phases, plus the cost of building them.
- **Engine change, paired:** the 5Z section, sequence-bound lookups
  measured against the 5Y commit with the same benchmark source, plus the
  harness's memory-cycle mode.
- **Memory accounting:** the 5AA section, stored bytes against RocksDB's,
  allocator state and compaction peaks.
- **Layout research, run layer:** the 5AB section, candidate run layouts
  built and read beside the production run in `src/run/tests/layouts.rs`;
  none is used by the engine.
- **Engine option, paired:** the 5AC section, `target_block_len` at 16, 4
  and 2 KiB through the benchmark suite and the 5Y harness's `tessera`
  engine.
- **Dictionary research, run layer:** the 5AD section, per-run Zstandard
  dictionaries at 4 KiB blocks in the research layout, with a format and
  compaction design; the engine is unchanged.
- **Engine feature, paired:** the 5AE section, `Compression::ZstdDictionary`
  against 16 and 4 KiB plain Zstandard through the 5Y harness, with direct
  run-layer and engine lookup checks.
- **Historical:** everything under "Historical results", from earlier
  harnesses; kept for the decisions it records, not comparable number for
  number.

Key filters are built as each run is written, so flush and compaction times
include their construction; the reports give the filter's size and the
memory its construction held. On tmpfs a sync costs almost nothing, so
`durable` and `recovery` measure the engine's own costs, not a device's.

## Baseline (5S)

One pass in the bounded container (2 CPUs of the 4 GiB Colima VM, release
build). The whole suite took about 66 s, peaked at 237 MiB and wrote 5 MiB
to the VM disk (the build's own outputs; fixtures stayed on tmpfs).

#### Memtable: 500000 records of 8 + 100 bytes

| operation | ns/op | M ops/s |
|---|---|---|
| insert, ascending keys | 99 | 10.14 |
| insert, random keys | 660 | 1.52 |
| overwrite, random keys | 777 | 1.29 |
| get, present | 702 | 1.42 |
| get, absent | 705 | 1.42 |
| scan, per record | 32 | 31.68 |
| delete, random keys | 941 | 1.06 |

charged 112.5 MiB for 51.5 MiB of keys and values

#### Reads: 1000000 records in runs, 200000 probes

From the former `reads` mode, which `lookups` and `scans` replaced in 5V.

| runs | max overlap | filter bits/key | get present ns | blocks/get | get absent ns | blocks/absent get | prefix scan ns (16 records) | full scan ns/record |
|---|---|---|---|---|---|---|---|---|
| 15 (as flushed) | 15 | 10.0 | 9725 | 1.11 | 1426 | 0.122 | 128077 | 152.7 |
| 2 (consolidated) | 1 | 10.0 | 7980 | 1.00 | 136 | 0.008 | 9470 | 85.5 |

#### Flush: 250000 records of 8 + 100 bytes

| ms | MB/s of keys and values | stored / payload | runs | filter bits/key | filter build KiB |
|---|---|---|---|---|---|
| 77.9 | 346 | 0.31 | 1 | 10.0 | 2048 |

#### Compaction: 8 overlapping runs of 250000 random keys each

| ms | MB/s of input runs | input MiB | output MiB | versions dropped | filter build KiB | workspace KiB |
|---|---|---|---|---|---|---|
| 415.6 | 145 | 57.3 | 28.1 | 904749 | 8192 | 9376 |

#### Taking a snapshot after one write

| records in memtable | runs | snapshot ns |
|---|---|---|
| 1000 | 0 | 49 |
| 100000 | 0 | 50 |
| 1000000 | 2 | 57 |

#### Overwrites of 200000 keys with live snapshots, one replaced every 20000 writes, 16 MiB memtable

| live snapshots | writes k/s | p99 µs | flushes | peak retained MiB | peak pinned MiB | retained after drop |
|---|---|---|---|---|---|---|
| 0 | 1958 | 0.5 | 11 | 0.0 | 0.0 | 0 |
| 1 | 1909 | 0.5 | 14 | 14.9 | 2.4 | 0 |
| 4 | 1708 | 0.8 | 16 | 31.7 | 3.4 | 0 |
| 16 | 1899 | 0.7 | 16 | 94.9 | 3.4 | 0 |

#### Sustained ingestion: 1000000 writes over 400000 keys, 4 MiB memtable, background compaction

| writers | writes k/s | p50 µs | p99 µs | max µs | flushes | compactions | runs at end | max overlap | run MiB | peak RSS MiB |
|---|---|---|---|---|---|---|---|---|---|---|
| 1 | 1865 | 0.2 | 0.4 | 6734 | 55 | 13 | 4 | 4 | 19.6 | 61 |
| 2 | 1963 | 0.2 | 0.8 | 13069 | 55 | 11 | 1 | 1 | 11.9 | 96 |
| 4 | 2020 | 0.2 | 0.8 | 46995 | 55 | 9 | 4 | 4 | 21.8 | 111 |

#### Readers beside writers: 500000 writes over 400000 preloaded keys, 4 MiB memtable, background compaction; one read in 16 is a prefix scan

| writers | readers | writes k/s | write p99 µs | gets k/s | get p99 µs | scan p99 µs |
|---|---|---|---|---|---|---|
| 1 | 1 | 1199 | 0.5 | 31 | 39.7 | 2097.1 |
| 1 | 3 | 607 | 0.5 | 64 | 433.4 | 2264.9 |
| 2 | 2 | 1088 | 0.8 | 36 | 1042.5 | 2236.1 |

#### Durable writes: 200000 writes, 16 MiB memtable, log on /tmp

| writers | writes k/s | p50 µs | p99 µs | syncs | writes/sync | flushes | log MiB |
|---|---|---|---|---|---|---|---|
| 1 | 762 | 1.0 | 2.0 | 200000 | 1.0 | 2 | 26.1 |
| 4 | 332 | 3.8 | 39.3 | 137230 | 1.5 | 2 | 26.1 |
| 8 | 324 | 22.6 | 57.2 | 71859 | 2.8 | 2 | 26.1 |

#### Checkpoint and recovery: 500000 records, files on /tmp

| engine | step | ms | MiB | records/s |
|---|---|---|---|---|
| RAM-only | checkpoint | 148.6 | 15.9 | 3364450 |
| RAM-only | recover | 71.4 | 15.9 | 7006743 |
| durable | checkpoint | 152.7 | 15.9 | 3274259 |
| durable | open, replaying 100000 records | 101.0 | 30.3 | 5941366 |

Reading it:

- Point lookups are dominated by decoding one compressed block (no block
  cache): about 8 µs once consolidated. As flushed, 15 overlapping runs add
  filter checks and occasional false-positive decodes. Absent keys decode
  0.12 blocks a lookup across 15 runs and 0.008 in one, which is the
  10-bit filters' false-positive rate at work.
- A prefix scan pays one block decode per run it touches, so consolidation
  matters most there (128 µs against 9 µs for 16 records).
- Snapshots cost about 50 ns whatever the table size. Live snapshots raise
  the flush count, since pinned history counts against the budget, and
  retained memory grows with how many are held across flushes; it returns
  to zero when they are dropped.
- The `durable` rows on tmpfs show the queue's own overhead: with free
  syncs, one writer is fastest, and group commit pays off only when syncs
  cost something (see the historical disk rows).
- Automatic flushes appear as the write-latency maximum in `ingest` (a
  write that flushes a 4 MiB table), not in the p99.

These numbers are a reference point for the suite, not a comparison with
the historical results below, which used other workloads and harnesses.

## Read baseline (5V)

Point lookups, prefix scans, concurrent reads and memory density, measured
by the `lookups`, `scans`, `readers` and `density` modes and by one
codec measurement outside the engine. Two passes in the bounded container
(2 CPUs of the 4 GiB Colima VM, release build), summarized with
`summarize_bench.py`; the tables give the mean of the two. The four modes
took about 130 s a pass, peaked at 304 MiB of container memory and wrote
nothing to the VM disk beyond the build.

### Method

- **Data.** Fixed seeds throughout. `lookups` and `readers` use 500,000
  records of 8-byte keys and 100-byte values that compress about 3:1;
  `scans` uses 491,520 such records in a hierarchical 8-byte key layout
  whose 6-, 4-, 3- and 2-byte prefixes select 32, 512, 2,048 and 16,384
  keys and contain 0x00 and 0xFF bytes. After loading, 1 key in 7 is
  deleted and 1 in 5 of the rest overwritten.
- **Shapes.** The same records, spread six ways: all in the memtable; one
  consolidated run; 9 runs that each span the whole key range (8 loads and
  the updates), with superseded versions; a run and a newer run of the
  updates; a run with the updates in the memtable; and a snapshot of the
  last, read while later writes overwrite a third of the keys.
- **Lookup classes.** Present keys in random order, in ascending order and
  within a hot 4,096-key range; overwritten keys; deleted keys
  (tombstones); absent keys inside the runs' key ranges (decided by the
  10-bit key filters); and absent keys beyond the last key.
- **Procedure.** Each class is 50,000 probes. The first pass after building
  gives the *cold* figure (the first touch after building; there is no
  block cache, so "cold" differs from "warm" only in CPU caches). A
  verification pass checks every result against the model and counts
  blocks decoded through `EngineWorkspace::blocks_loaded`. *Warm* is the
  median of 3 timed passes; p50 and p99 come from a pass timing each
  operation, which adds the clock's overhead (about 40 ns). Allocations
  are counted by a benchmark-only global allocator, enabled only for one
  pass, so it does not slow the other modes.
- **Scans.** Each prefix class is a fixed list of prefixes, scanned once
  to verify every record (order, prefix, value, deleted keys absent)
  against a `BTreeMap` model, then timed in 3 rounds after a warm-up. *Seek*
  is from creating the scan (including taking the read guard) to its first
  record; the rest is iteration. *Versions/record* is derived from how the
  fixture was built, not observed: the engine does not count records
  examined.
- **References.** A `BTreeMap<Vec<u8>, Vec<u8>>` of the live records gives an
  uncompressed in-memory reference. It has no versions, snapshots,
  compression or memory accounting, so it is a floor for the access
  pattern, not a competitor.
- **Noise.** Between the two passes, single-threaded lookup and scan cells
  differ by a median of 1–2% (a few cells up to 16–38%, mostly
  sub-microsecond seeks). Concurrent rows differ by a median of 21% and up
  to 77%, and `density` timings by 8–17%: treat those as indicative.
  Differences smaller than these spreads are not results.

### Point lookups

| shape | runs / overlap | lookup | cold ns | warm ns | p50 µs | p99 µs | blocks/get |
|---|---|---|---|---|---|---|---|
| memtable | 0 / 0 | present, random order | 676 | 602 | 0.67 | 1.2 | 0.00 |
| memtable | 0 / 0 | present, ascending order | 284 | 266 | 0.25 | 0.9 | 0.00 |
| memtable | 0 / 0 | present, hot 4,096-key range | 162 | 162 | 0.17 | 0.3 | 0.00 |
| memtable | 0 / 0 | overwritten | 700 | 651 | 0.73 | 1.2 | 0.00 |
| memtable | 0 / 0 | deleted | 673 | 635 | 0.65 | 1.0 | 0.00 |
| memtable | 0 / 0 | absent, inside run ranges | 630 | 570 | 0.58 | 1.0 | 0.00 |
| memtable | 0 / 0 | absent, beyond the last key | 61 | 58 | 0.08 | 0.1 | 0.00 |
| consolidated | 1 / 1 | present, random order | 7,920 | 7,831 | 7.86 | 10.9 | 1.00 |
| consolidated | 1 / 1 | present, ascending order | 7,407 | 7,352 | 7.40 | 11.2 | 1.00 |
| consolidated | 1 / 1 | present, hot 4,096-key range | 7,406 | 7,412 | 7.48 | 9.9 | 1.00 |
| consolidated | 1 / 1 | overwritten | 7,888 | 7,832 | 7.90 | 10.9 | 1.00 |
| consolidated | 1 / 1 | deleted | 7,828 | 7,866 | 7.94 | 11.2 | 1.00 |
| consolidated | 1 / 1 | absent, inside run ranges | 111 | 108 | 0.04 | 0.1 | 0.01 |
| consolidated | 1 / 1 | absent, beyond the last key | 44 | 42 | 0.04 | 0.1 | 0.00 |
| 9 overlapping runs | 9 / 9 | present, random order | 10,788 | 10,716 | 8.56 | 24.9 | 1.27 |
| 9 overlapping runs | 9 / 9 | present, ascending order | 9,588 | 9,606 | 7.67 | 22.5 | 1.27 |
| 9 overlapping runs | 9 / 9 | present, hot 4,096-key range | 9,551 | 9,511 | 7.54 | 22.4 | 1.27 |
| 9 overlapping runs | 9 / 9 | overwritten | 17,673 | 17,682 | 17.14 | 26.3 | 2.06 |
| 9 overlapping runs | 9 / 9 | deleted | 17,660 | 17,590 | 17.09 | 26.2 | 2.06 |
| 9 overlapping runs | 9 / 9 | absent, inside run ranges | 878 | 860 | 0.25 | 9.1 | 0.07 |
| 9 overlapping runs | 9 / 9 | absent, beyond the last key | 286 | 286 | 0.29 | 0.3 | 0.00 |
| run + newer run | 2 / 2 | present, random order | 9,682 | 9,584 | 7.83 | 17.4 | 1.21 |
| run + newer run | 2 / 2 | present, ascending order | 8,952 | 8,970 | 7.46 | 16.4 | 1.21 |
| run + newer run | 2 / 2 | present, hot 4,096-key range | 8,932 | 8,957 | 7.48 | 15.6 | 1.21 |
| run + newer run | 2 / 2 | overwritten | 16,389 | 16,349 | 16.36 | 20.8 | 2.00 |
| run + newer run | 2 / 2 | deleted | 16,367 | 16,370 | 16.38 | 20.9 | 2.00 |
| run + newer run | 2 / 2 | absent, inside run ranges | 198 | 193 | 0.08 | 7.9 | 0.02 |
| run + newer run | 2 / 2 | absent, beyond the last key | 78 | 74 | 0.08 | 0.1 | 0.00 |
| run + memtable | 1 / 1 | present, random order | 6,828 | 6,764 | 8.14 | 9.4 | 0.80 |
| run + memtable | 1 / 1 | present, ascending order | 6,020 | 6,018 | 7.38 | 9.2 | 0.80 |
| run + memtable | 1 / 1 | present, hot 4,096-key range | 5,972 | 5,966 | 7.29 | 8.5 | 0.80 |
| run + memtable | 1 / 1 | overwritten | 378 | 310 | 0.33 | 0.6 | 0.00 |
| run + memtable | 1 / 1 | deleted | 310 | 312 | 0.33 | 0.5 | 0.00 |
| run + memtable | 1 / 1 | absent, inside run ranges | 455 | 420 | 0.35 | 0.7 | 0.01 |
| run + memtable | 1 / 1 | absent, beyond the last key | 91 | 86 | 0.08 | 0.1 | 0.00 |
| snapshot of run + memtable | 1 / 1 | present, random order | 6,832 | 6,790 | 8.19 | 10.0 | 0.80 |
| snapshot of run + memtable | 1 / 1 | present, ascending order | 6,038 | 6,017 | 7.38 | 9.1 | 0.80 |
| snapshot of run + memtable | 1 / 1 | present, hot 4,096-key range | 5,934 | 5,945 | 7.29 | 8.6 | 0.80 |
| snapshot of run + memtable | 1 / 1 | overwritten | 404 | 315 | 0.33 | 0.5 | 0.00 |
| snapshot of run + memtable | 1 / 1 | deleted | 322 | 303 | 0.33 | 0.6 | 0.00 |
| snapshot of run + memtable | 1 / 1 | absent, inside run ranges | 447 | 413 | 0.35 | 0.7 | 0.01 |
| snapshot of run + memtable | 1 / 1 | absent, beyond the last key | 88 | 82 | 0.08 | 0.1 | 0.00 |
| consolidated, 31-byte shared-prefix keys | 2 / 1 | present, random order | 9,280 | 9,256 | 9.21 | 13.1 | 1.00 |
| consolidated, 31-byte shared-prefix keys | 2 / 1 | absent, inside run ranges | 132 | 128 | 0.08 | 0.1 | 0.01 |
| 9 overlapping runs, 31-byte shared-prefix keys | 9 / 9 | present, random order | 12,084 | 12,062 | 9.48 | 28.3 | 1.26 |
| 9 overlapping runs, 31-byte shared-prefix keys | 9 / 9 | absent, inside run ranges | 958 | 943 | 0.25 | 10.5 | 0.08 |
| BTreeMap (reference) | – | present, random order | 439 | 378 | 0.40 | 0.6 | – |
| BTreeMap (reference) | – | present, ascending order | 96 | 88 | 0.08 | 0.2 | – |
| BTreeMap (reference) | – | present, hot 4,096-key range | 99 | 98 | 0.12 | 0.2 | – |
| BTreeMap (reference) | – | overwritten | 407 | 376 | 0.40 | 0.7 | – |
| BTreeMap (reference) | – | deleted | 414 | 412 | 0.44 | 0.7 | – |
| BTreeMap (reference) | – | absent, inside run ranges | 380 | 356 | 0.40 | 0.7 | – |
| BTreeMap (reference) | – | absent, beyond the last key | 56 | 56 | 0.08 | 0.1 | – |

- **Decompression dominates every run lookup.** A lookup that reaches a run
  decodes one 16 KiB Zstandard block, about 7.4–7.9 µs, against 0.2–0.7
  µs in the memtable and 0.1–0.4 µs in the `BTreeMap`. Allocation is zero
  per lookup in every shape.
- **Locality helps memory, not runs.** Ascending and hot-range lookups are
  2–4× faster in the memtable and the `BTreeMap`, but within 6–11% of
  random order in runs: with no decoded-block cache, every lookup decodes its
  block again, however recently it was read.
- **Absent keys are cheap.** Filters reject 99% of absent keys per run
  (0.01 blocks a lookup in one run, 0.07 across 9), and keys beyond a
  run's range never reach its filter. A false positive costs a full
  decode, which is the 7.9–9.1 µs p99 of absent keys with overlapping runs.
- **Older versions cost a decode each.** A lookup reads every run its
  filter cannot exclude and keeps the highest sequence number, also
  checking that duplicate copies agree. A key overwritten or deleted in a
  newer run therefore decodes the newer run's block *and* the older run's:
  2.0–2.1 blocks and about 16–17 µs, twice a key with one version. Keys
  that live in the memtable never reach the runs (0.3 µs).
- **Snapshots read like the engine.** Snapshot lookups match the engine's
  within noise while writes continue behind them.
- **Key shape.** 31-byte keys with long shared prefixes add about 18% to a
  run lookup (9.3 against 7.8 µs).

### Prefix scans

| shape | prefix | records/scan | seek µs | scan µs | ns/record after seek | blocks/scan | versions/record |
|---|---|---|---|---|---|---|---|
| memtable | 32 keys (6 B) | 27 | 0.6 | 1.3 | 27 | 0.0 | 1.17 |
| memtable | 512 keys (4 B) | 439 | 0.7 | 11.2 | 24 | 0.0 | 1.17 |
| memtable | 2,048 keys (3 B) | 1,755 | 0.7 | 40.5 | 23 | 0.0 | 1.17 |
| memtable | 16,384 keys (2 B, ends FF) | 14,044 | 0.5 | 270.8 | 20 | 0.0 | 1.17 |
| memtable | every key (empty) | 421,303 | 1.3 | 13,520.1 | 32 | 0.0 | 1.17 |
| memtable | no match, inside range | 0 | 0.1 | 0.2 | – | 0.0 | – |
| memtable | no match, beyond last key | 0 | 0.1 | 0.1 | – | 0.0 | – |
| consolidated | 32 keys (6 B) | 27 | 8.3 | 10.9 | 99 | 1.2 | – |
| consolidated | 512 keys (4 B) | 439 | 8.2 | 49.5 | 94 | 3.9 | – |
| consolidated | 2,048 keys (3 B) | 1,755 | 8.2 | 172.2 | 94 | 12.7 | – |
| consolidated | 16,384 keys (2 B, ends FF) | 14,044 | 8.0 | 1,324.5 | 94 | 95.9 | – |
| consolidated | every key (empty) | 421,303 | 8.6 | 39,563.6 | 94 | 2,828.0 | – |
| consolidated | no match, inside range | 0 | 7.6 | 7.7 | – | 1.0 | – |
| consolidated | no match, beyond last key | 0 | 0.1 | 0.1 | – | 0.0 | – |
| 9 overlapping runs | 32 keys (6 B) | 27 | 76.7 | 82.0 | 205 | 9.3 | 1.53 |
| 9 overlapping runs | 512 keys (4 B) | 439 | 75.7 | 166.2 | 207 | 13.1 | 1.53 |
| 9 overlapping runs | 2,048 keys (3 B) | 1,755 | 74.4 | 428.6 | 202 | 24.8 | 1.53 |
| 9 overlapping runs | 16,384 keys (2 B, ends FF) | 14,044 | 74.3 | 2,943.6 | 204 | 138.2 | 1.53 |
| 9 overlapping runs | every key (empty) | 421,303 | 20.2 | 86,693.3 | 206 | 3,875.0 | 1.53 |
| 9 overlapping runs | no match, inside range | 0 | 70.7 | 70.7 | – | 9.0 | – |
| 9 overlapping runs | no match, beyond last key | 0 | 0.4 | 0.5 | – | 0.0 | – |
| run + newer run | 32 keys (6 B) | 27 | 17.2 | 20.9 | 140 | 2.2 | 1.53 |
| run + newer run | 512 keys (4 B) | 439 | 17.0 | 75.7 | 134 | 6.1 | 1.53 |
| run + newer run | 2,048 keys (3 B) | 1,755 | 17.0 | 249.8 | 132 | 18.1 | 1.53 |
| run + newer run | 16,384 keys (2 B, ends FF) | 14,044 | 18.3 | 1,860.8 | 131 | 131.0 | 1.53 |
| run + newer run | every key (empty) | 421,303 | 17.4 | 55,925.4 | 132 | 3,854.0 | 1.53 |
| run + newer run | no match, inside range | 0 | 16.1 | 16.1 | – | 2.0 | – |
| run + newer run | no match, beyond last key | 0 | 0.1 | 0.2 | – | 0.0 | – |
| run + memtable | 32 keys (6 B) | 27 | 8.2 | 11.2 | 116 | 1.2 | 1.53 |
| run + memtable | 512 keys (4 B) | 439 | 8.2 | 56.6 | 110 | 4.4 | 1.53 |
| run + memtable | 2,048 keys (3 B) | 1,755 | 8.2 | 201.0 | 110 | 14.6 | 1.53 |
| run + memtable | 16,384 keys (2 B, ends FF) | 14,044 | 8.1 | 1,564.4 | 110 | 110.0 | 1.53 |
| run + memtable | every key (empty) | 421,303 | 9.3 | 48,825.2 | 116 | 3,256.0 | 1.53 |
| run + memtable | no match, inside range | 0 | 7.4 | 7.4 | – | 1.0 | – |
| run + memtable | no match, beyond last key | 0 | 0.2 | 0.2 | – | 0.0 | – |
| snapshot of run + memtable | 32 keys (6 B) | 27 | 8.6 | 11.9 | 123 | 1.2 | 1.53 |
| snapshot of run + memtable | 512 keys (4 B) | 439 | 8.4 | 56.6 | 110 | 4.4 | 1.53 |
| snapshot of run + memtable | 2,048 keys (3 B) | 1,755 | 8.2 | 201.4 | 110 | 14.6 | 1.53 |
| snapshot of run + memtable | 16,384 keys (2 B, ends FF) | 14,044 | 7.9 | 1,542.8 | 110 | 110.0 | 1.53 |
| snapshot of run + memtable | every key (empty) | 421,303 | 9.1 | 49,286.1 | 117 | 3,256.0 | 1.53 |
| snapshot of run + memtable | no match, inside range | 0 | 7.4 | 7.5 | – | 1.0 | – |
| snapshot of run + memtable | no match, beyond last key | 0 | 0.1 | 0.2 | – | 0.0 | – |
| BTreeMap (reference) | 32 keys (6 B) | 27 | 0.4 | 0.7 | 8 | – | 1.00 |
| BTreeMap (reference) | 512 keys (4 B) | 439 | 0.4 | 2.5 | 4 | – | 1.00 |
| BTreeMap (reference) | 2,048 keys (3 B) | 1,755 | 0.3 | 7.0 | 4 | – | 1.00 |
| BTreeMap (reference) | 16,384 keys (2 B, ends FF) | 14,044 | 0.2 | 37.1 | 3 | – | 1.00 |
| BTreeMap (reference) | every key (empty) | 421,303 | 0.4 | 1,126.2 | 3 | – | 1.00 |
| BTreeMap (reference) | no match, inside range | 0 | 0.1 | 0.1 | – | – | 1.00 |
| BTreeMap (reference) | no match, beyond last key | 0 | 0.1 | 0.1 | – | – | 1.00 |

The 2-byte prefixes ending 0x00 and the prefix `FF FF`, which has no upper
bound, measured within 3% of the 0xFF row and are left out; all returned
exactly the model's records.

- **Seek is one block decode per participating run.** About 8 µs on one
  run, 16–17 µs on two, 52–77 µs across 9 overlapping runs, against
  0.5–0.7 µs in the memtable and 0.2–0.4 µs in the `BTreeMap`. Seek cost
  does not depend on how many records the prefix selects.
- **Short scans are seeks.** For about 27 records the seek is 76% of the
  scan on one run and 93% on 9 runs. From hundreds of records iteration
  dominates.
- **Iteration is merge- and decode-bound.** After the seek a record costs
  about 92 ns from one run, 110 ns from a run beside the memtable, 131 ns
  from two runs and 205 ns from 9 (with 1.5 versions per returned record),
  against 17–32 ns from the memtable and 2–9 ns from the `BTreeMap`.
  Throughput is therefore about 4.9–10.8 million records a second from
  runs, flat from 512 records to the whole dataset.
- **No-match prefixes inside the key range cost a decode per run** (7.6 µs
  on one run, 71 µs on 9): filters cannot exclude a prefix, and the block
  that would hold it must be read. Prefixes beyond every run's range are
  rejected from the key ranges in 0.1–0.4 µs.
- **Workspace.** A scan holds one decoded block (16 KiB) per participating
  run, 144 KiB across 9 runs, and makes 1–2 allocations when created and
  none per record.

### Concurrent reads

`readers`: 500,000 records in one run with the updates in the memtable;
each row runs its threads for 1.5 s. Per-thread rates, as the range of the
two passes:

| threads | role | ops/s per thread | p99 µs |
|---|---|---|---|
| 1 get | get | 167–169 k | 9–12 |
| 2 gets | get | 164–166 k each | 9–13 |
| 2 snapshot gets | snapshot get | 167–177 k each | 9 |
| 1 guard scan, 4,096 keys | scan | 2.4–2.6 k | 430–444 |
| 2 guard scans | scan | 1.8–2.5 k each | 440–777 |
| 2 snapshot scans | scan | 1.4–2.4 k each | 447–797 |
| get + guard scan | get / scan | 118–164 k / 1.7–2.4 k | 10–17 / 448–660 |
| 1 put | put | 1.16–1.25 M | 1.5–1.8 |
| get + put | get / put | 74–131 k / 616–755 k | 32–39 / 11–12 |
| guard scan + put | scan / put | 1.8–2.1 k / 19–23 k | 550–636 / 510–588 |
| snapshot scan + put | scan / put | 1.7–2.4 k / 0.99–1.21 M | 449–654 / 1.4–1.6 |

- Two point readers each keep the single reader's rate in both passes, so
  lookups scale to the 2 CPUs: reads take the state lock only to search
  the memtable and copy the run list, and decode outside it.
- Two scanners each kept the single scanner's rate in one pass and lost
  25–40% in the other; with 2 CPUs and a shared host, this run cannot
  separate contention from host noise.
- **A scan through a `ReadGuard` stalls writers for its duration**, as
  documented: beside one 4,096-key guard scan a writer manages about 20 k
  writes a second with a p99 equal to the scan time. The same scan through
  a `Snapshot` leaves the writer at about 1 M writes a second. Long scans
  beside writes should use snapshots.
- A writer beside a point reader cost the reader 56% of its rate in one
  pass and 22% in the other, with a 32–39 µs p99; nothing beyond
  2 CPUs was measured.

### Memory density

`density` loads each payload in random order into a 1 GiB memtable, then
flushes and consolidates. *Heap* is what the engine holds on the heap by
the counting allocator (allocations it made, less those freed); it includes
runs, their indexes, key filters, the empty memtable and structure
overhead, but not allocator fragmentation or RSS.

| payload | records | mean key + value | logical | memtable heap | memtable charged | runs heap (run + filter) | logical / run bytes | heap per record in runs | `BTreeMap` heap |
|---|---|---|---|---|---|---|---|---|---|
| sensor readings, 14–32 B values | 1,500,000 | 35 B | 50.1 MiB | 154.5 MiB | 233.2 MiB | 35.6 MiB (33.8 + 1.8) | 1.48 | 25 B | 134.7 MiB |
| JSON profiles, ~150 B–1 KiB | 120,000 | 556 B | 63.6 MiB | 72.0 MiB | 78.3 MiB | 16.9 MiB (16.7 + 0.1) | 3.80 | 147 B | 121.3 MiB |
| order documents, ~1–6 KiB | 15,000 | 2,506 B | 35.9 MiB | 36.9 MiB | 37.7 MiB | 8.6 MiB (8.6 + 0.02) | 4.19 | 600 B | 72.3 MiB |

| payload | get ns, memtable | get ns, runs | scan ns/record, runs | get ns, `BTreeMap` |
|---|---|---|---|---|
| sensor readings | 940–1,170 | 15,640–15,700 | 65–69 | 560–630 |
| JSON profiles | 600–650 | 13,500–15,800 | 480–580 | 260–290 |
| order documents | 450–480 | 12,850–15,060 | 1,950–2,250 | 110–135 |

- **Consolidated runs hold the data in 3.8–8.4× less heap than a
  `BTreeMap` of the same records**, and in 24–27% of their logical size
  for the JSON and document payloads (71% for sensor readings). Small
  records gain least: 8-byte random
  parts compress poorly and per-record framing is large beside them.
- **The memtable is no denser than a `BTreeMap`.** Its charge overstates
  its heap for small records (233 against 155 MiB): the accounting is a
  conservative admission estimate, so a budget fills before the memory it
  names is used.
- **Density costs access.** A run lookup costs 13–16 µs on these payloads,
  25–120× the `BTreeMap` lookup, and scanning a large-value run costs
  about 0.8–1 ns per decompressed byte. Key filters stay small (10 bits a
  key), so they cost most, relatively, for small records (5% of the run
  bytes).

### Compressed and uncompressed runs

This comes from the run layer directly (5W below measures the engine): `cargo test --release --lib run::tests::costs -- --ignored
--nocapture` builds one run per payload and codec (smaller, simplified
versions of the `density` payloads, with 10-byte keys) and reads it
without an engine, so it shows block costs, not the engine's read path.

| payload | codec | logical / run | get ns | scan ns/record |
|---|---|---|---|---|
| sensor readings | none | 1.14 | 516 | 16 |
| sensor readings | LZ4 | 1.38 | 5,842 | 25 |
| sensor readings | Zstandard | 1.73 | 15,642 | 42 |
| JSON profiles | none | 1.00 | 402 | 22 |
| JSON profiles | LZ4 | 2.47 | 5,781 | 161 |
| JSON profiles | Zstandard | 5.99 | 11,633 | 327 |
| order documents | none | 1.00 | 205 | 20 |
| order documents | LZ4 | 2.91 | 4,241 | 495 |
| order documents | Zstandard | 5.40 | 11,750 | 1,396 |

One pass. Indexes are 15–30 KiB for 11–22 MiB of data in every codec.
Uncompressed blocks are read in place, so a lookup is 0.2–0.5 µs; LZ4 costs
36–50% of Zstandard's lookup time at 41–80% of its compression ratio (41–54%
on the structured values). The block-level decode cost, not the merge
or the index, is what separates run lookups from memory lookups.

### Reading it

- **Where it does well:** absent keys (filters), keys still in the
  memtable, lookups that scale across both CPUs, snapshot reads that do
  not disturb writers, and dense storage of structured values (3.8–4.2×
  smaller than logical size for JSON-like and document data, 7–8× less heap
  than a `BTreeMap`).
- **Where decompression dominates:** every point lookup that reaches a
  run, every scan seek, and short scans. Repeated access gains nothing,
  since decoded blocks are not kept.
- **Where overlap costs:** seeks (one decode per run), keys with older
  versions in other runs (one decode per version), iteration (2.2× slower
  from 9 runs than from one), and the filter false-positive tail.
  Consolidation (`compact` with `max_overlap: 1`) removes all of it.
- **Prefix scans by size:** below about a hundred records the seek
  decides the cost; from hundreds upwards it is 90–210 ns a record,
  depending on overlap, flat to the whole dataset.
- **Locality:** helps the memtable and the `BTreeMap` 2–4×, runs not at
  all.
- **Limits of the environment:** a 2-CPU VM on a shared laptop host; no
  scaling claim beyond 2 CPUs, concurrent figures within ±40%, datasets of
  36–64 MiB (far beyond CPU caches, small beside RAM), and no comparison
  with another engine.
- **Possible improvements, not made:** a decoded-block cache would turn
  repeated and hot run lookups into memory lookups, at the cost of memory
  the compression saved; per-run sequence bounds would let a lookup stop
  at the newest run holding a key, halving the cost of overwritten keys in
  overlapping runs; and a choice of codec (LZ4, or none for hot data)
  would trade density for 2–3× (LZ4) or 20–60× (none) cheaper decodes. Each is a design decision
  beyond this baseline. 5W, below, measured all three and added the codec
  choice.

## Compression and read latency (5W)

Zstandard level 3, LZ4 and uncompressed runs through the whole engine, on
the identical datasets of the 5V modes. Every run block already records its
codec, so the comparison needed no format change: one pass of `lookups`,
`scans`, `density`, `maintenance`, `snapshots` and `recovery` per codec, in
one session, with the engine's codec switched at build time (the same
setting `EngineOptions::compression` now exposes). One pass each, so read
single-digit percentage differences as noise; the differences below are
2× and more. Lookups took 86.7 / 25.6 / 11.7 s of the `lookups` mode's
wall time (Zstandard / LZ4 / none); container peaks stayed under 360 MiB.

### What each codec costs and saves

Medians of the same cells as the 5V tables. *Measured*, one pass:

| | Zstandard (default) | LZ4 | uncompressed |
|---|---|---|---|
| lookup, consolidated, present key: warm ns / p99 µs | 8,112 / 11.6 | 2,105 / 2.7 | 474 / 0.8 |
| lookup, 9 overlapping runs, present | 10,939 / 25.3 | 3,432 / 7.7 | 886 / 1.8 |
| lookup, 9 overlapping runs, overwritten (2 blocks) | 17,817 / 26.5 | 5,052 / 8.3 | 1,209 / 2.1 |
| lookup, absent key inside run ranges, 9 runs: ns / p99 µs | 865 / 9.3 | 434 / 3.3 | 275 / 0.9 |
| lookup, hot 4,096-key range, consolidated | 7,563 | 1,391 | 314 |
| scan seek, consolidated / 9 runs (µs) | 8.4 / 77.6 | 2.3 / 21.9 | 0.6 / 4.4 |
| 32-key scan, consolidated / 9 runs (µs) | 11.0 / 83.0 | 3.8 / 25.8 | 1.8 / 7.8 |
| ns per record after the seek, consolidated / 9 runs | 94 / 206 | 53 / 148 | 42 / 130 |
| 16,384-key scan, consolidated (µs) | 1,345 | 760 | 589 |
| no match inside the key range, consolidated (µs) | 7.6 | 1.5 | 0.4 |
| heap in runs, sensor / JSON / documents (MiB) | 35.6 / 16.9 / 8.6 | 42.2 / 28.9 / 13.7 | 46.2 / 63.5 / 35.9 |
| logical ÷ run bytes, same payloads | 1.48 / 3.80 / 4.19 | 1.24 / 2.21 / 2.63 | 1.13 / 1.00 / 1.00 |
| flush of 250,000 × 108 B: ms (MB/s), stored ÷ payload | 77.9 (346), 0.31 | 43.0 (627), 0.47 | 37.1 (727), 1.00 |
| compaction of 8 runs: ms, output MiB | 414, 28.1 | 231, 41.7 | 213, 89.2 |
| checkpoint of 500,000: ms, MiB | 150, 15.9 | 70, 24.0 | 59, 51.5 |
| recover / durable open replaying 100,000 (ms) | 70 / 101 | 33 / 63 | 33 / 63 |

For comparison, the memtable answers the same lookups in 0.69 µs and a
`BTreeMap` in 0.38 µs, holding the three payloads in 134.7 / 121.3 / 72.3
MiB of heap.

- **Point lookups:** the run-layer advantage survives the engine almost
  whole. An uncompressed run lookup costs 0.47 µs against 0.2–0.5 µs in the
  run alone, close to the memtable; LZ4 is 3.5–4× and uncompressed 13–17×
  cheaper than Zstandard wherever a block is decoded, including the p99.
  Blocks per lookup, allocations (none) and filter behaviour are unchanged:
  the codec changes the price of a block, not how many are read. Absent
  keys barely change, except the false-positive tail with many runs.
  Locality starts to pay once decoding is cheap: hot and ascending lookups
  are 1.5× faster than random with LZ4 and uncompressed, not at all with
  Zstandard.
- **Scans:** a cheaper codec helps the seek and short scans 3–20×, long
  scans 1.6–2.3× (ns a record falls 94 → 53 → 42), since iteration also
  pays for merging and copying. Overlap costs the same share under every
  codec. Snapshot reads and memtable scans are unchanged.
- **Memory:** the codec matters most for structured values: JSON-like
  runs are 1.7× (LZ4) and 3.8× (uncompressed) the Zstandard size. Small
  binary records save only 23% under Zstandard, at 27× the lookup cost of
  uncompressed. Even uncompressed, runs hold 1.9–2.9× less heap than a
  `BTreeMap`. Indexes (15–30 KiB per 11–22 MiB) and filters (10 bits a key)
  do not depend on the codec. Read workspaces and snapshot retention do
  not change either; a decoded block is still at most one 16 KiB buffer per
  run cursor, and uncompressed blocks need none.
- **Writes and persistence:** Zstandard is the slowest to build: flushes
  and compactions take 1.8–2.1× as long as LZ4, which is within 15% of
  uncompressed. Snapshot-heavy overwrites run 25–45% faster with LZ4 for
  the same reason. Rewrite amplification (versions dropped) is identical.
  Checkpoints re-encode every live record with the configured codec, so
  their size follows the run sizes (1.5× and 3.2× of Zstandard here) and
  so does the disk they write; recovery is 2× faster with either cheaper
  codec.

### Decoded-block cache (simulation only)

`cargo test --release --lib run::tests::costs::block_cache -- --ignored
--nocapture` replays the blocks that 200,000 reads would decode from one
consolidated Zstandard run through an LRU, charging each cached block the
run's largest decoded block plus 64 bytes. Hot keys in the Zipf traces are
scattered across the key space; the hot window in the second trace is
contiguous. *Measured* hit rates, one pass:

| payload | trace | blocks/op | 1 MiB | 4 MiB | 16 MiB | 64 MiB | all blocks decoded |
|---|---|---|---|---|---|---|---|
| sensor readings (600,000) | uniform gets | 1.00 | 6% | 24% | 96% | 99% | 16.7 MiB |
| | 90% of gets in 1% of keys, contiguous | 1.00 | 91% | 92% | 99% | 99% | |
| | Zipf gets, scattered | 1.00 | 25% | 48% | 98% | 99% | |
| | Zipf 32-key scans | 1.05 | 24% | 48% | 98% | 99% | |
| JSON profiles (50,000) | uniform gets | 1.00 | 4% | 18% | 71% | 99% | 22.5 MiB |
| | 90% of gets in 1% of keys, contiguous | 1.00 | 90% | 92% | 97% | 99% | |
| | Zipf gets, scattered | 1.00 | 30% | 51% | 88% | 99% | |
| | Zipf 32-key scans | 1.89 | 25% | 47% | 86% | 100% | |

*Extrapolated:* a hit spares one decode, about 7.5 µs under Zstandard, so
a 4 MiB cache under scattered Zipf traffic would save about 3.6 µs of an
8 µs lookup. The same budget held as uncompressed runs serves every read
at the uncompressed price: a cache pays only where the hot set is small
and contiguous, or where most of the data must stay compressed. It would
also need shared decoded blocks across readers, synchronization on each
read and its own memory accounting. Not implemented; this agrees with the
earlier simulation under "Decoded-block caching".

### Sequence-bound pruning (review only)

A lookup reads every run its filter and key range do not exclude, keeps the
highest sequence number and checks for conflicting copies of it. Recording
each run's largest sequence number (known to the builder, and recomputable
when `Run::new` validates a loaded run, so no format change) and visiting
runs in descending order of it would let the lookup stop as soon as no
remaining run can hold a newer version.

- **Precedence and tombstones:** safe. A tombstone is a version like any
  other; skipping runs whose newest record is older than a found version
  cannot hide a newer one.
- **Overlapping ranges:** unaffected; key ranges and filters still exclude
  first.
- **Conflict detection:** another copy of the decisive sequence number can
  only be in a run whose largest sequence number is at least that one, so
  the check at the deciding version is kept. Conflicting copies of older
  versions would go unnoticed, but today's lookups already ignore them.
- **Snapshots and compaction:** snapshots hold immutable run lists; a
  compaction output carries the largest sequence number of its inputs.
  The run list is not in recency order (compaction outputs are appended
  after newer flushes), so the order would be computed per published list.

It is safe but narrow: it saves one decode only for keys with versions in
more than one run (17.8 → about 11 µs for overwritten keys in 9 runs under
Zstandard, 1.2 → 0.9 µs uncompressed, *extrapolated* from the
present-key cells). Consolidated data, absent keys and memtable hits gain
nothing, and consolidation removes the case entirely. Deferred here;
implemented in 5Z (see "Historical read amplification (5Z)").

### Decision

The codec is now a setting: `EngineOptions::compression`, one of
`Compression::Zstd` (the default), `Lz4` or `Uncompressed`. It applies to
the runs that flushes, compactions and checkpoints build.

- **Why:** a material, reproducible difference (4–17× point lookups,
  3–20× seeks and short scans, 2× flush, compaction, checkpoint and
  recovery throughput) in exchange for memory, which is the trade an
  application should choose. The integration is one value threaded to the
  three run builders.
- **Format and compatibility:** none changed. Each block frame already
  records its codec and checksum, and readers accept all three, so runs of
  different codecs coexist, checkpoints and logs recover under any setting
  (tested in `tests/checkpoint.rs`), and compaction rewrites what it merges
  with the configured codec. No per-run or per-checkpoint setting is
  stored, so there is nothing to keep consistent.
- **Validation:** unchanged for compressed blocks. Uncompressed blocks are
  read in place, so their checksums are verified when a run is loaded
  from a checkpoint, not on each read; runs live only in memory, so
  this leaves no window for on-disk corruption to reach a read.
- **Limits:** `max_run_bytes` and checkpoint `max_bytes` count stored
  bytes, so less compression reaches them sooner; nothing else in memory
  admission depends on the codec.
- **Default:** Zstandard stays. Density is the engine's purpose, and for
  structured values it holds 1.7–3.8× more data than the alternatives.
- **Not done:** per-level or adaptive codecs, recompression, Zstandard
  levels, block caching and sequence-bound pruning.

## Mixed read/write workloads (5X)

*Complete engine, mixed.* Whether the isolated 5W differences hold when
reads, writes, automatic flushes and background compaction share the two
CPUs. Modes `workloads-zstd`, `workloads-lz4` and `workloads-none`, one
process per codec so resident memory is its own; three passes in the
bounded container with the codec order rotated (Z L N, N L Z, L Z N).
Medians of the three; a cell is marked where a pass differed from the
median by 10% or more.

### Method

- **Dataset:** the `scans` layout, 491,520 keys of 8 bytes with 100-byte
  values (50.6 MiB logical). Loaded in random order, flushed and
  consolidated, then a seventh of the keys deleted and a fifth of the
  rest overwritten into newer runs, so the run starts with 9 runs and an
  overlap of 2. Background compaction with default options, 4 MiB
  memtable budget.
- **Clients:** two threads, each with its own seeded list of operations,
  generated before the clock starts; the lists are identical for every
  codec. Writers write disjoint keys (one in ten writes is a delete), so
  the final state is deterministic.
- **Operations:** point gets (one in eight of an absent key between two
  stored keys; the others reported by what the key was before the run:
  present, overwritten or deleted), short scans (a 6-byte prefix, about 28
  records, through a read guard) and medium scans (a 4-byte prefix, about
  440 records, through a `Snapshot`, whose creation is included).
- **Scenarios, per client:** read-heavy, 400,000 operations, 80% gets, 12%
  short and 3% medium scans, 5% writes; balanced, 200,000, 40% gets, 8%
  short, 2% medium, 50% writes, holding a snapshot of the starting state
  throughout; write-heavy, 400,000, 4% gets, 1% short, 0.5% medium, 94.5%
  writes.
- **Measured:** each operation's latency, the time to a scan's first
  record, blocks decoded per operation, the clients' CPU time and the
  rest of the process's (background compaction and a 10 ms stats
  sampler), peaks of the engine's own accounting sampled every 10 ms, and
  resident memory.
- **Checked, after the clock stops:** the final state against a model of
  the plans (every live record, plus 160 random prefixes of 2–8 bytes and
  the 0x00, 0xFF and past-the-end boundaries), the held snapshot against
  the starting state, and that no compaction failed. All passed in every
  pass and codec.

### Reads while writing

p50 / p99 in µs, Zstandard / LZ4 / uncompressed:

| scenario | get, present | get, overwritten | get, absent | 28-record scan | 440-record snapshot scan |
|---|---|---|---|---|---|
| read-heavy | 8.7 / 2.6 / 1.2 — 36 / 24 / 11 | 18.3 / 5.2 / 1.8 — 45 / 27 / 12 | 0.6 / 0.6 / 0.5 — 27 / 21 / 7 | 28.5 / 9.4 / 4.3 — 55 / 32 / 16 | 95 / 51 / 41 — 159 / 86 / 68 |
| balanced | 9.1 / 2.9 / 1.4 — 40 / 29 / 27 | 18.7 / 5.6 / 1.9 — 49 / 32 / 27 | 0.7 / 0.7 / 0.6 — 28 / 26 / 26 | 38.7 / 13.6 / 6.3 — 71 / 41 / 32 | 116 / 65 / 56 — 192 / 110 / 92 |
| write-heavy | 11.7 / 4.1 / 1.8 — 36 / 12 / 9 | 18.7 / 5.9 / 2.3 — 38 / 14 / 10 | 0.6 / 0.6 / 0.5 — 11 / 5 / 8 | 48.6 / 17.4 / 8.6 — 165 (±342%) / 33 / 20 | 156 / 92 / 78 — 2,211 / 1,375 / 1,685 (±40–54%) |

| scenario | gets k/s, Z / L / N | blocks per present get | scan: first record p50 µs, Z / L / N | scan ns/record after the first, short; medium |
|---|---|---|---|---|
| read-heavy | 61 / 163 / 291 | 1.02 | 25.8 / 6.8 / 2.1 | 171 / 107 / 91; 153 / 95 / 84 |
| balanced | 33 / 73 / 121 | 1.17 | 32.9 / 9.5 / 2.9 | 289 / 175 / 138; 193 / 125 / 122 |
| write-heavy | 12 / 21 / 27 | 1.61 | 43.3 / 12.5 / 4.1 | 946 / 281 / 199; 373 / 229 / 225 |

- **The isolated ratios hold.** Present-key gets keep the 5W order and
  close to its ratios (3.3× and 7× faster than Zstandard in read-heavy;
  5W had 3.9× and 17× on a consolidated run). Uncompressed gets cost
  1.2–1.8 µs here, not 0.5, because the memtable and two layers of runs
  are searched and two clients share the CPUs; the cheaper the codec, the
  more of a lookup is this fixed engine cost.
- **Prefix scans gain most at the first record**: 11–12× from Zstandard to
  uncompressed and 3.5–3.8× to LZ4, against 1.6–4.8× per record after it;
  a whole 440-record scan is 2–2.3× (uncompressed) and 1.7–1.9× (LZ4)
  faster.
- **Writes raise read costs through overlap, not the codec.** From
  read-heavy to write-heavy, blocks per present get rise from 1.02 to
  1.61 and latency by 35–55% under every codec. Short scans after the
  first record slow most under Zstandard (171 → 946 ns), since each
  additional overlapping run adds a block decode.
- **Tails:** gets' p99 sits at 21–40 µs in read-heavy and balanced under
  every codec (preemption: two clients, compaction and the sampler on two
  CPUs). In write-heavy, snapshot scans have a p99 of 1.4–2.2 ms under
  every codec, varying by up to 54% between passes. Its cause was not
  investigated; it does not depend on the codec.
- Absent keys cost the same under every codec: the filters answer them.

### Writes, flushes and compaction

| scenario | codec | wall s | writes k/s | write p99 µs | write max ms | flushes | compactions, MiB in → out | client CPU s | other CPU s |
|---|---|---|---|---|---|---|---|---|---|
| read-heavy | Zstandard | 6.28 | 6 | 51 | 5.5 | 2 | 0 | 11.9 | 0 |
| | LZ4 | 2.35 | 17 | 28 | 2.7 (±27%) | 2 | 0 | 4.3 | 0 |
| | uncompressed | 1.32 | 30 | 23 | 2.4 (±46%) | 2 | 0 | 2.4 | 0 |
| balanced | Zstandard | 2.89 | 69 | 62 | 9.3 (±23%) | 10 | 9, 18.9 → 17.8 | 4.5 | 0.2 |
| | LZ4 | 1.31 | 153 | 40 | 4.3 | 10 | 9, 26.8 → 25.1 | 1.9 | 0.1 |
| | uncompressed | 0.79 | 253 | 28 | 3.9 (±19%) | 10 | 9, 56.0 → 52.9 | 1.2 | 0.1 |
| write-heavy | Zstandard | 1.66 | 454 | 6 (±31%) | 11.0 | 41 | 32, 120 → 94 | 2.0 | 1.2 |
| | LZ4 | 0.90 | 844 | 11 | 5.9 | 41 | 31, 124 → 114 | 1.1 | 0.4 |
| | uncompressed | 0.71 | 1,070 | 11 | 5.1 (±50%) | 41 | 33, 381 → 298 | 0.9 | 0.4 |

- **Write throughput** (closed loop, so it also reflects the reads each
  client does, which dominate read-heavy) is 1.9–2.8× higher with LZ4 and
  2.4–5× uncompressed. In
  write-heavy the difference is the flush, which runs inside the write
  that fills the table: the slowest write, a flush, takes 11 ms under
  Zstandard and 5–6 ms otherwise. Median writes cost 0.2–0.7 µs under
  every codec.
- **Compaction** did the same work (31–33 compactions, the same flushes)
  and kept up under every codec: waiting for it after the clients
  finished took at most 0.14 s. Its CPU (the "other" column, mostly the
  background worker) was 1.2 s under Zstandard against 0.4 s, while
  moving 3× the bytes uncompressed. No refusal or backpressure beyond the
  flushes occurred: run limits were unbounded here.
- **Same work, less CPU:** the read-heavy clients used 11.9, 4.3 and 2.4
  CPU seconds for the same 800,000 operations.

### Memory

| scenario | codec | live logical MiB | peak runs + filters MiB | runs at the end, logical ÷ stored | peak memtables MiB | peak retained MiB | peak RSS MiB |
|---|---|---|---|---|---|---|---|
| read-heavy | Zstandard | 43.5 | 21.2 | 2.14 | 4.0 | 0 | 133 |
| | LZ4 | | 31.0 | 1.44 | 4.0 | 0 | 141 |
| | uncompressed | | 64.7 | 0.68 | 4.0 | 0 | 179 |
| balanced | Zstandard | 44.1 | 25.1 | 1.85 | 4.0 | 4.7 (±55%) | 139 |
| | LZ4 | | 36.3 | 1.26 | 4.0 | 5.0 | 154 |
| | uncompressed | | 76.5 | 0.59 | 4.0 | 9.6 (±41%) | 227 |
| write-heavy | Zstandard | 45.1 | 37.2 | 3.0 | 4.4 | 3.9 | 199 |
| | LZ4 | | 52.0 | varies (±124%) | 4.0 | 6.3 (±38%) | 210 (±13%) |
| | uncompressed | | 114.1 | 0.95 | 4.1 | 4.5 (±87%) | 358 |

- Runs hold old versions and tombstones until compaction merges them, so
  stored bytes exceed live logical bytes uncompressed (logical ÷ stored
  below 1) and the end-of-run ratio depends on where compaction stopped;
  the peaks are the steadier measure. The density order of 5W holds:
  peak run memory is 1.4–1.5× (LZ4) and 3.0–3.1× (uncompressed) the
  Zstandard figure on these 100-byte, roughly 3:1 compressible values.
- Memtables stay within their 4 MiB budget under every codec, and the
  snapshot held through the balanced run retained 5–10 MiB: compaction
  replaced only the small update runs it pinned.
- Peak process memory (which also holds the benchmark's model of the
  starting state) follows the run peaks: 199, 210 and 358 MiB in
  write-heavy, where compaction keeps its inputs until its outputs are
  published.

### Reading it

- **Confirmed:** the 5W tradeoff holds with writes and compaction running.
  Every read class and the writes are faster with a cheaper codec, by
  ratios close to the isolated ones, and memory grows by the isolated
  ratios. No measured scenario reverses the order: the fastest isolated
  codec was also the fastest overall here, because the dataset fits and
  compaction keeps up.
- **Where the choice matters least:** absent keys and memtable hits; and
  tails dominated by scheduling and flush stalls, which are the same under
  every codec.
- **Confidence:** medians of read latency, throughput and CPU varied by
  under 10% between passes; differences of 2× and more are established.
  Tails (p99 in write-heavy, max write latency), end-of-run stored size
  and peak retained bytes depend on flush and compaction timing and vary
  by up to ±54% (one cell ±342%); only their order of magnitude is
  reliable. Two CPUs; nothing here measures more parallelism.
- **Not measured:** a dataset near the run limits, where a less compact
  codec would refuse flushes sooner; larger values, where Zstandard's
  density advantage grows (5W: 3.8–4.2× on JSON-like and document data).

## RocksDB baseline (5Y)

*Competitive baseline.* Tessera against RocksDB, an established compressed
LSM engine, on identical keys, values and operations, at three scales up
to 2 GiB of logical data. The question is whether compressed RAM-resident
runs give a better density–latency trade than RocksDB with its SST files
held in memory. Results are from two passes with the engine order
reversed between them; a cell is marked where the passes differed by 5%
or more.

### Setup

- **Harness:** `tools/compare`, a separate package (its own lockfile, a
  path dependency on Tessera) linking the system RocksDB through a few
  hand-declared C API calls, so RocksDB never enters Tessera's dependency
  graph. It is opt-in: `scripts/validate.sh` never builds it or its
  image, and installs nothing; the image is built once, by hand, in the
  Colima VM. One engine per process and per bounded container, so memory
  peaks are that engine's alone:

  ```sh
  docker --context colima build -t tessera-compare:rocksdb-7.8.3 - < tools/compare/Dockerfile
  TESSERA_IMAGE=tessera-compare:rocksdb-7.8.3 scripts/validate.sh exec 600 sh -c \
    'cd tools/compare && CARGO_TARGET_DIR=/cache/compare-target cargo build --locked --release'
  TESSERA_IMAGE=tessera-compare:rocksdb-7.8.3 TESSERA_TMPFS_SIZE=1536m scripts/validate.sh exec 1200 \
    /cache/compare-target/release/tessera-compare rocksdb json:256 /tmp
  ```

- **RocksDB:** 7.8.3 (November 2022), Debian's `librocksdb-dev 7.8.3-2`
  (bookworm; no newer build is packaged for it, and compiling a current
  release on two CPUs was not attempted). It is a historical version
  without later releases' changes, so this is a baseline for the
  architecture rather than for RocksDB's best.
- **Environment:** Rust 1.98.1, the bounded container (2 CPUs, 3 GiB, no
  swap), `/tmp` a tmpfs raised to 1.5 GiB (`TESSERA_TMPFS_SIZE`) and
  charged to the same 3 GiB. Tessera's Zstandard is the vendored 1.5.7,
  RocksDB's the system 1.5.4; LZ4 is `lz4_flex` 0.14 against liblz4
  1.9.4. Both engines use glibc's allocator.
- **Matched configuration (`rocksdb`):** 16 KiB blocks, Zstandard level 3,
  a full Bloom filter of 10 bits per key (format version 5, about 1%
  false positives; Tessera's is 10 bits, about 0.8%), **no block cache**
  (every read decompresses its block, as in Tessera), index and filter
  blocks in the table readers' heap (the default), 64 MiB memtables (two
  at most), 64 MiB target files, two background jobs, level compaction
  with default sizes, writes without the WAL, checksums verified on read.
  Files on the tmpfs, so a block read is a copy from the page cache.
- **Tuned configuration (`rocksdb-tuned`):** the same plus a 64 MiB LRU
  cache of decompressed blocks, mmap reads and dynamic level sizes.
- **Tessera:** defaults (64 MiB memtable, 64 MiB of input per run,
  Zstandard) with background compaction and `max_runs` raised from the
  provisional 64 to 256, since RocksDB has no file limit. Non-durable
  (`Engine::new`), as RocksDB without the WAL.
- **Not equivalent:** Tessera flushes inside the write that fills the
  memtable, RocksDB on a background thread; RocksDB's target file size is
  stored bytes, Tessera's input bytes, so Tessera has about four times
  as many, smaller runs; their compaction policies differ (below), and
  so does the overlapping state they leave.
- **Tessera-only additions (5Z):** a `MissingHeld` row, the same missing
  keys read through one held `ReadGuard`, and a `tessera-cycles` engine
  name that runs the memory cycles of the 5Z section instead of the
  comparison phases.
- **Memory additions (5AA):** a `details` line with RocksDB's aggregated
  table properties after consolidation, `tessera-cycles-16m` (the cycles
  with a 16 MiB memtable) and `tessera-compactions` /
  `tessera-compactions-lz4` (single compactions and their peaks).

### Datasets and states

- **Keys:** the 8-byte `scans` layout, extended to up to 256 groups of
  16,384 records: prefixes of 6, 4, 3 and 2 bytes select 32, 512, 2,048
  and 16,384 consecutive records, with 0x00 and 0xFF bytes throughout.
- **Datasets:** JSON profiles (`json_record`, about 200 B to 1 KiB) at
  8 groups (131,072 records, 69 MiB logical, cache-friendly and comparable
  to 5V–5X), 64 groups (1,048,576 records, 553 MiB, far beyond the CPU
  caches) and 256 groups (4,194,304 records, 2.2 GiB, the largest the key
  layout allows; its peak was 1.33 GiB of the 3 GiB, so larger would have
  been possible but was not needed). Binary sensor readings (14–32 B) at
  256 groups (122 MiB logical).
- **History:** records loaded in a fixed random order with background
  compaction, then consolidated; then one key in 16 overwritten and
  another one in 16 deleted, in another random order, and flushed: the
  *overlapping* state. Then consolidated again: the *consolidated* state,
  with no tombstones and one version per key (RocksDB: a forced
  bottom-level `CompactRange`; Tessera: compaction to overlap 1). Live
  data in both: 15/16 of the records.
- **Reads:** timed only when no flush or compaction is running or due, on
  one thread, warm (every page resident, a warm-up round first). Gets:
  present (uniform), overwritten, deleted, missing (between two keys),
  localized (90% in a window of 1% of the keys); 3 rounds of 30,000 each.
  Scans: an empty in-range prefix, a whole key (trailing 0xFF, so the upper
  bound carries), 6-, 4-, 3- and 2-byte prefixes (the last of an odd group
  `[g, 0xFF]`, or of `[0xFF, 0xFF]` with no upper bound). Tessera scans
  through a read guard; RocksDB through an iterator with the prefix's
  successor as upper bound. Both are consistent views; Tessera's guard
  blocks writers for its duration, RocksDB's iterator does not.
- **Checked, untimed:** every value and every scan's keys, values and order
  in a separate counted pass, and a full ordered scan of every live record
  against the generator, in both states. No copy of the data is held; the
  generator recomputes each expected record. Every check passed in every
  run, so both engines returned identical results.

### Memory

Resident bytes per engine after consolidation. *Total* is what the
engine needs to keep the dataset in memory: for Tessera its process's
anonymous memory, for RocksDB its anonymous memory plus its files on the
tmpfs (not counted twice: without mmap, file pages are not in the process
RSS). Measured after `malloc_trim`, which returns freed heap; the
untrimmed figure is what a process that never trims holds.

| dataset | engine | engine's own accounting MiB | total resident MiB, trimmed (untrimmed) | bytes per live record | logical ÷ total | peak while building MiB (container) |
|---|---|---|---|---|---|---|
| JSON, 131k | Tessera | runs 17.2 + filters 0.1 | 19.8 (44.4) | 169 | 3.26 | 92 (94) |
| | RocksDB | SST 16.9 + readers 0.2 | 21.0 (25.3) | 179 | 3.07 | 108 (115) |
| JSON, 1M | Tessera | runs 138.2 + filters 1.2 | 152.4 (185.4) | 163 | 3.40 | 296 (298) |
| | RocksDB | SST 135.2 + readers 1.8 | 143.2 (151.9) | 153 | 3.62 | 414 (377–431) |
| | RocksDB tuned | + cache 61.5 | 205.2 | 219 | 2.53 | 411 (442) |
| JSON, 4.2M | Tessera | runs 556.2 + filters 4.7 | 587.6 (758.9) | 157 | 3.54 | 965 (925–985) |
| | RocksDB | SST 541.4 + readers 7.2 | 553.8 (565.2) | 148 | 3.75 | 1,300 (1,239–1,329) |
| | RocksDB tuned | + cache 61.4 | 613.7 | 164 | 3.38 | 1,243 (1,300) |
| sensor, 4.2M | Tessera | runs 87.1 + filters 4.7 | 115.2 (167.3) | 31 | 1.01 | 258 (259) |
| | RocksDB | SST 79.2 + readers 4.8 | 88.7 (115.1) | 24 | 1.31 | 244 (288–305) |
| JSON 1M, LZ4 | Tessera | runs 235.8 + filters 1.2 | 258.6 | 276 | 2.00 | 438 (440) |
| | RocksDB | SST 233.2 + readers 1.8 | 240.3 | 256 | 2.16 | 538 (592) |

- **Density: RocksDB is denser, slightly on JSON and clearly on small
  records.** Tessera's runs are 2–3% larger than RocksDB's SST files on
  JSON (which also hold RocksDB's index and filters) and 10% larger on
  sensor records, plus filters (data blocks alone: 17%, all of it from
  sequence numbers; see 5AA); complete resident footprints are 6% (JSON,
  4.2M) and 30% (sensor) larger. On the smallest dataset Tessera is 6%
  smaller, from RocksDB's fixed costs.
- **Allocator retention is Tessera's largest memory cost.** Right after
  consolidation, glibc held 759 MiB for Tessera's 567 MiB of live
  allocations at 4.2M records (22–124% above the trimmed figure across
  datasets, most on the smallest) until `malloc_trim`: freed memtables and compaction inputs
  stay in the heap. RocksDB's data lives in files, so its anonymous
  memory stays at 10–35 MiB. Untrimmed, Tessera's footprint is 34% (JSON)
  and 45% (sensor) above RocksDB's. Even trimmed, 4–19% of Tessera's
  anonymous memory is free heap glibc did not return.
- **Peaks favour Tessera on JSON:** 25–30% lower while building the
  largest dataset, since RocksDB holds compaction inputs and outputs as
  files beside its heap and memtables; on sensor records Tessera's
  sampled peak is 6% higher. Both stay far within 3 GiB.
- **The tuned cache** adds its 64 MiB to the footprint.

### Point lookups

p50 µs (p99), and blocks decoded per lookup; RocksDB's filter checks per
lookup are the files consulted.

| dataset | engine | present | overwritten, overlapping | deleted, overlapping | missing, consolidated | missing, overlapping | localized, consolidated |
|---|---|---|---|---|---|---|---|
| JSON, 131k | Tessera | 13.6 (23), 1.0 | 26.5 (33), 2.0 | 26.5 (33), 2.0 | 0.08 | 0.08 | 12.5 |
| | RocksDB | 13.6 (19), 1.0 | 13.1 (18), 1.0 | 13.0 (18), 1.0 | 0.21 | 0.29 | 12.5 |
| JSON, 1M | Tessera | 13.9 (18), 1.0 | 27.5 (34), 2.0 | 27.5 (34), 2.0 | 0.31 | 0.33 | 13.2 |
| | RocksDB | 13.7 (19), 1.0 | 13.4 (18), 1.0 | 13.3 (18), 1.0 | 0.23 | 0.29 | 12.9 |
| | RocksDB tuned | 13.9 (19), 0.9 | 0.92 (1.7), 0.5 | 0.83 (1.5), 0.2 | 0.23 | 0.29 | 0.79 |
| JSON, 4.2M | Tessera | 14.8 (20), 1.0 | 28.8 (36), 2.0 | 29.3 (46), 2.0 | 1.10 | 1.27 | 14.3 |
| | RocksDB | 13.9 (19), 1.0 | 13.9 (27), 1.0 | 13.8 (27), 1.0 | 0.23 | 0.46 | 13.1 |
| | RocksDB tuned | 14.1 (19), 1.0 | 13.8 (19), 0.7 | 13.7 (19), 0.6 | 0.25 | 0.46 | 0.88 |
| sensor, 4.2M | Tessera | 14.9 (20), 1.0 | 33.0 (45), 2.0 | 33.0 (45), 2.0 | 0.12 | 0.17 | 13.4 |
| | RocksDB | 19.0 (25), 1.0 | 21.2 (27), 1.0 | 21.1 (27), 1.0 | 0.21 | 0.33 | 16.9 |
| JSON 1M, LZ4 | Tessera | 6.0 (7.5), 1.0 | 11.8 (16), 2.0 | 11.8 (16), 2.0 | 0.29 | 0.33 | 5.4 |
| | RocksDB | 6.6 (11), 1.0 | 6.3 (8.4), 1.0 | 6.3 (8.5), 1.0 | 0.21 | 0.29 | 5.9 |

- **A lookup that decodes a block costs the same in both** (13.6–14.8 µs
  on JSON with Zstandard, 6.0–6.6 µs with LZ4): the block decode
  dominates, and the codec, not the engine, sets the price. Tessera is
  22% faster on sensor records and 9% with LZ4; RocksDB 7% faster at
  4.2M JSON records.
- **Overwritten and deleted keys cost Tessera twice as much** in the
  overlapping state: it decodes the newer run and the older one (2.0
  blocks), RocksDB stops at the newest version it finds (1.0). This holds
  at every scale and codec. See the audit below.
- **Tessera's lookup grows with the number of runs.** Missing keys cost
  0.08 µs with 2–3 runs, 0.31 µs with 10 and 1.10 µs with 38 (RocksDB
  0.21–0.25 µs at every scale), and the same ~1 µs is in every 4.2M
  lookup: every published run's key range and filter are checked in turn,
  where RocksDB binary-searches the files of each level.
- **A block cache changes only skewed reads:** the tuned configuration
  serves the 1% hot window at 0.8–0.9 µs (15× faster) for 64 MiB, and
  gives uniform lookups nothing. Tessera has no equivalent.

### Prefix scans and ordered iteration

Consolidated state; first record p50 µs; whole scan p50 µs; ns per
record after the first; blocks per scan.

| dataset | engine | empty prefix | whole key | 6 bytes (30) | 4 bytes (480) | 3 bytes (1,920) | 2 bytes (15,360) | full scan, M records/s |
|---|---|---|---|---|---|---|---|---|
| JSON, 1M | Tessera | 13.8 | 14.0 | 13.9; 27.8; 493; 2.0 | 14.2; 240; 476; 17.3 | 14.4; 921; 474; 66 | 15.4; 7,245; 472 | 2.11 |
| | RocksDB | 13.7 | 14.0 | 14.0; 29.1; 532; 2.0 | 14.4; 264; 520; 17.7 | 14.9; 1,006; 518; 68 | 16.1; 7,991; 518 | 1.93 |
| JSON, 4.2M | Tessera | 15.0 | 15.1 | 15.1; 29.1; 496; 2.0 | 15.5; 243; 480; 17.3 | 15.9; 930; 478; 67 | 17.1; 7,358; 482 | 2.09 |
| | RocksDB | 14.0 | 14.1 | 14.1; 29.1; 527; 2.0 | 14.6; 263; 517; 17.8 | 15.2; 1,006; 517; 68 | 17.0; 7,942; 518 | 1.94 |
| sensor, 4.2M | Tessera | 14.9 | 15.0 | 14.7; 16.3; 74; 1.1 | 14.8; 48; 70; 2.0 | 14.6; 149; 69; 4.9 | 14.4; 1,072; 69 | 14.3 |
| | RocksDB | 19.2 | 19.4 | 19.2; 21.7; 128; 1.1 | 19.4; 73; 115; 2.1 | 19.4; 233; 113; 5.6 | 20.1; 1,772; 114 | 8.8 |
| JSON 1M, LZ4 | Tessera | 5.9 | 6.2 | 6.2; 12.3; 217 | 6.4; 105; 209 | 6.6; 402; 207 | 6.5; 3,159; 206 | 4.84 |
| | RocksDB | 6.8 | 7.1 | 7.0; 14.9; 280 | 7.2; 135; 269 | 7.6; 517; 267 | 8.2; 4,085; 266 | 3.67 |

- **Time to the first record is one block decode in both** (13.7–15 µs
  for JSON), and dominates scans of tens of records: a 30-record scan is
  half seek. An empty prefix costs the same seek.
- **Iteration after the first record is Tessera's clearest advantage:**
  6–8% fewer ns per JSON record, 22–25% with LZ4, and 39–42% on sensor
  records, where per-record cost rather than decompression dominates;
  full ordered scans are 8–14% (JSON), 32% (LZ4) and 62% (sensor) faster.
- **Overlapping state** (not tabled): every scan seeks in every overlapping
  source, so the first record costs one block per layer in both: 27–29 µs
  for Tessera's two layers and RocksDB's two at 1M records, and 56 µs for
  RocksDB's four at 4.2M, where Tessera's background compaction had
  merged its five update flushes into one layer (29 µs) and RocksDB left
  three L0 files below its compaction trigger. Per-record costs keep the
  consolidated order (Tessera 546–594 ns, RocksDB 608–619). Physical
  records per logical record are 36/30 in both by construction: each
  32-record range holds two superseded versions and two tombstones.
- **Scan workspace:** Tessera's reader held 165–181 KiB of decoded-block
  buffers; RocksDB does not report its iterators' memory.

### Building the states

| dataset | engine | load s (CPU s) | settle s | consolidate s (CPU s) | runs or files, consolidated | load peak MiB |
|---|---|---|---|---|---|---|
| JSON, 1M | Tessera | 2.0 (3.3) | 0.5 | 4.7 (4.7) | 10 runs | 292 |
| | RocksDB | 1.8 (3.9) | 1.7 | 3.7 (3.7) | 3 files, L1 | 363 |
| JSON, 4.2M | Tessera | 7.4 (15.9) | 8.2 | 18.9 (18.9) | 38 runs | 935 |
| | RocksDB | 9.9 (21.7) | 9.0 | 15.3 (15.3) | 9 files, L2 | 1,231 |
| sensor, 4.2M | Tessera | 2.3 (3.0) | 0.5 | 2.2 (2.2) | 4 runs | 228 |
| | RocksDB | 6.6 (7.7) | 0.0 | 2.5 (2.5) | 2 files, L1 | 172 |

- Loading takes 25% (JSON, 4.2M) to 65% (sensor) less time in Tessera,
  with less CPU; at 1M records RocksDB is 10% faster. Data generation is
  excluded.
- While loading and settling 2.2 GiB of JSON, RocksDB's leveled compaction
  wrote 1.8–2.0 GiB and Tessera's background compaction 1.25–1.36 GiB;
  both flushed 0.58 GiB of compressed runs or files.
- A full consolidation takes RocksDB 0.8–1.15× Tessera's time, both on
  one thread (CPU time equals wall time).

### Measurement quality

- Within a process, the p50 of each class varied by under 1% between
  rounds; between passes, medians agreed within 5% except the cells
  marked in the raw summary (mostly p99s and missing-key p50s, ±7–45%).
  Differences under 5% (present-key lookups at 1M, first-record times)
  are a tie.
- One thread on two CPUs, warm, no concurrent writes: nothing here
  measures read scaling, interference or cold starts.
- RocksDB 7.8 and glibc's allocator; RocksDB's usual jemalloc and newer
  releases were not measured.

### Read-amplification audit

Why Tessera decodes two blocks for an updated key where RocksDB decodes
one, examined against the code (`lookup_state`, `latest_in`, `select`):

- **The lookup reads every run that may hold the key**, keeps the highest
  sequence number and checks it for a conflicting copy; it cannot stop
  early because the run list is not in recency order (compaction outputs
  are appended after newer flushes) and runs carry no sequence bounds.
  RocksDB visits memtables, L0 newest first and then levels in order, and
  stops at the first version.
- **Runs need not be chronological, and their sequence ranges can
  interleave:** selection takes the *smallest* runs covering the deepest
  key, so it can merge an old run with a new one and skip a middle-aged,
  larger run between them. Pruning does not require chronological
  compaction: visiting runs in descending order of their largest sequence
  number and skipping any run whose largest sequence number is below the
  version already found is correct for interleaved ranges too; it only
  prunes less there.
- **The metadata is cheap:** a run's largest sequence number is not
  stored, but builders see every record's sequence number, a compaction
  output's is its inputs' largest, and recovery already computes it
  (`Run::recover`). It can live beside the run in memory, with no format
  change.
- **Conflict checks:** the contract compares copies only at the newest
  version. A copy of that version has the same sequence number, so it can
  only be in a run whose largest sequence number is at least that one,
  which the rule still reads; the check is preserved exactly. Nothing in
  the engine creates copies today (flushes and compactions replace
  atomically, log replay skips what the checkpoint holds), so the check
  guards against damage; compaction already rejects conflicting copies
  among its inputs, and recovery could do the same across runs, but the
  read-time check costs nothing extra under pruning.
- **Tombstones** are versions like any other and end the search the same
  way, which is why deleted keys gain as much as overwritten ones.
- **Snapshots** pin their own run list and every record in it is visible
  to them, so the same rule applies unchanged.
- **Practical benefit, measured on the overlapping state:** overwritten
  and deleted keys would drop from 2.0 to 1.0 blocks, from 26.5–29.3 µs to
  about the 13–14 µs of a present key (sensor 33 → ~15 µs; LZ4 11.8 →
  ~6 µs), a factor of 2, and by more for keys in deeper histories. It does
  nothing for consolidated data, present keys in one run, or missing keys.
- **The smallest plausible change** (made in 5Z): record each published
  run's largest sequence number, keep the run list ordered by it, and in
  `latest_in` stop once the next run's largest sequence number is below
  the version found. The separate cost that grows with the number of
  runs (1.1 µs at 38 runs) is independent of it: it comes from checking
  every run's key range in turn.

### Reading it

- **Tessera does not beat RocksDB overall.** On the architecture-matched
  comparison the two are equivalent where the block decode dominates
  (lookups and scan seeks of JSON at every scale): the codec sets the
  price in both.
- **Tessera is better at:** iterating records (6–8% per JSON record, 22%
  with LZ4, 40% on small records), lookups of small records (22%), loading
  (up to 65% less time) and peak memory while building JSON (25–30%
  lower).
- **RocksDB is better at:** reads of updated keys before consolidation (2×),
  lookups among many runs (missing keys 4–5× at 38 runs, 7% on present
  keys), density (6% on JSON, 30% on small records, complete footprint)
  and, decisively, memory stability: its data lives in files, so freed
  memory does not accumulate in a heap; Tessera's process held 31–79%
  more than its live allocations (more on the smallest dataset) until
  `malloc_trim`.
- **Tuned RocksDB** buys 15× faster skewed lookups for 64 MiB, a trade
  Tessera cannot offer without a block cache.
- The original premise, a better density–latency trade from compressed
  runs in RAM, is not supported against RocksDB with its files in memory:
  the same compressed blocks in a page cache achieve the same latency at
  equal or better density. Tessera's measurable advantages are in
  iteration, small records and ingestion, not in lookup latency per byte.

## Historical read amplification (5Z)

*Engine change, measured.* A lookup used to read every run whose key range
and filter admitted the key; it now stops once no remaining run can hold a
version newer than the one found. Measured with the `lookups` and `scans`
modes, two passes each, paired against the 5Y commit built from the same
benchmark source, and with the comparison harness at `json:64`.

### Version resolution: the rules

What a lookup must return, established from the code
(`lookup_state`, `latest_in`, `RunSet`, `select`, `publish`, checkpoint
recovery and log replay):

- **Precedence:** the version with the highest sequence number wins; a
  tombstone is a version like any other, so a newer tombstone hides older
  values and a newer value revives a deleted key.
- **Sources:** the active memtable, then frozen memtables newest first,
  then the runs. Every memtable record is newer than every run record (a
  flush publishes a frozen table's records as a run only after they leave
  the memtables, and new writes take higher sequence numbers), so a
  memtable hit ends the lookup before any run is read. This is unchanged.
- **Run order is not chronological.** Flushes append; compaction removes
  its inputs and appends its output, after runs newer than some inputs.
  Selection takes the *smallest* runs covering the deepest key, ties by
  `(bytes, position)`, so it can merge an old run with a newer one and
  leave a middle-aged run outside, and sequence ranges then interleave.
- **Duplicates:** the contract accepts any number of copies of a
  `(key, seq)` pair if they agree and reports corruption if copies of the
  *newest* version differ; older versions are not compared. Nothing in the
  engine creates copies: flushes and compactions replace their sources
  atomically, compaction itself rejects conflicting copies among its
  inputs, log replay applies only records above the checkpoint's sequence
  number, and recovery rejects runs whose largest sequence number exceeds
  the manifest's or that overlap. The read-time check is therefore
  defensive (it detects damage or a future bug), not a correctness
  requirement of normal operation.
- **Snapshots** pin their memtable view and run list at creation; every
  record they hold is at or below their sequence number, so no read path
  filters by a ceiling and the same rules apply unchanged.
- **Why older runs were inspected:** without per-run sequence bounds, and
  with runs not in recency order, a version found in one run could not
  prove that a later run in the list held nothing newer.

### The stopping rule

Each run carries its largest sequence number, `max_seq`. Runs are visited
in descending `max_seq`; a run is skipped when a version at sequence `S`
has been found and the run's `max_seq < S`.

*Proof.* Every record in a run has a sequence number at most the run's
`max_seq`, by definition, since `max_seq` is computed from the records
present (by the builder as it adds them, and by validation when a run is
loaded). A skipped run therefore holds only versions below `S`: none can
outrank the version found, and none is a copy of it. A copy of the
deciding version has sequence `S`, so it lies in a run with
`max_seq >= S`, which the rule still reads, and the conflict check at the
newest version is exactly as before. A newer tombstone is found first and
ends the search the same way. The rule is correct in *any* visiting order,
interleaved ranges included; the descending order only maximizes how much
it skips. With interleaved ranges it prunes less: a run whose maximum is
high but which holds only an old copy of the key is still read.

Cases checked against it: overlapping and interleaved ranges (key range
and filter exclusion still apply first, unchanged), snapshots (they pin
the ordered list they were created with), equal-sequence copies (read and
compared), conflicting older copies (skipped, as they were already
ignored), memtables (resolved before runs), compaction outputs (their
`max_seq` is computed from the records they keep, whatever was dropped), newly flushed runs
(their records are the newest in the run set), recovery (recomputed from
the records), and empty or tombstone-only runs (an empty run has
`max_seq` 0 and holds nothing to find; a tombstone-only run is ordered by
its tombstones like any other).

### Metadata

- **What:** one `u64` per run, held beside the run in memory.
- **Format:** none changed. Every record already persists its sequence
  number, so the value is recoverable from existing runs and checkpoints:
  the builder tracks it as records are added, and `Run::new` and
  `Run::recover` compute it in the validation walk they already perform
  over every record. Checkpoints written before this change load
  unchanged.
- **Cost:** 8 bytes per run; no extra pass at construction or recovery.
- **Order:** the engine keeps a second list of the same runs sorted by
  descending `max_seq`, rebuilt (one clone of the run pointers and a sort)
  each time a flush, compaction or recovery publishes runs. The original
  list keeps publication order, so compaction selection and its
  tie-breaks are untouched. Snapshots and lookups use the sorted list;
  lookups allocate nothing.
- **Corruption:** unchanged. Loading a run (`Run::new`, checkpoint
  recovery) still validates every block and record, recovery still rejects
  runs above the manifest's sequence number and overlapping runs, and
  compaction still reads every input record. A lookup was never a full
  integrity check: it verifies only the blocks it decodes, and a pruned
  run, like one whose range or filter excludes the key, is not read. Normal
  lookup correctness rests on the invariants above; finding historical
  damage is the job of load-time validation and compaction.
- **Not changed:** compaction selection, run storage and its format,
  scans (they merge every run, so the rule does not apply), the
  memtable path, and corruption checks.

### Results

`lookups` mode, 500k records of 8 + 100 bytes under Zstandard, mean of
two passes each, warm ns and p99 µs; the 5Y commit (baseline) and this
change built from the same benchmark source and run back to back.
Allocation is zero per lookup in every row, before and after.

| shape | lookup | before ns | after ns | p99 µs before → after | blocks before → after |
|---|---|---|---|---|---|
| consolidated | present | 8,156 | 8,169 | 11.8 → 11.9 | 1.00 → 1.00 |
| consolidated | absent | 108 | 107 | 0.12 → 0.12 | 0.01 → 0.01 |
| run + newer run | overwritten | 16,759 | 8,370 | 22.1 → 11.4 | 2.00 → 1.00 |
| run + newer run | deleted | 16,734 | 8,360 | 22.2 → 14.4 | 2.00 → 1.00 |
| run + newer run | present | 9,772 | 8,183 | 17.9 → 14.2 | 1.21 → 1.01 |
| 9 overlapping runs | overwritten | 17,900 | 8,424 | 26.6 → 11.5 | 2.06 → 1.00 |
| 9 overlapping runs | deleted | 17,949 | 8,423 | 26.9 → 11.6 | 2.06 → 1.00 |
| 9 overlapping runs | present | 10,983 | 8,905 | 25.5 → 17.4 | 1.27 → 1.03 |
| 9 overlapping runs | absent | 875 | 882 | 9.3 → 9.3 | 0.07 → 0.07 |
| 9-run history | overwritten in 8 runs | 40,726 | 3,956 | 48.7 → 4.6 | 9.00 → 1.00 |
| 9-run history | never updated | 8,346 | 8,267 | 16.5 → 16.0 | 1.07 → 1.07 |
| 9-run history, snapshot after 4 | overwritten in 4 of its runs | 24,309 | 3,887 | 30.6 → 4.2 | 5.00 → 1.00 |
| run + memtable | overwritten (memtable hit) | 366 | 332 | 0.75 → 0.67 | 0 → 0 |

- **Updated and deleted keys** decode one block instead of one per run
  holding a version: half the latency with two versions, a tenth with
  nine. A snapshot gains the same, against its own run list.
- **Present keys among overlapping runs** gain 16–19%: a filter false
  positive in an older run no longer costs a decode once the key is found.
- **The deep-history key costs 4 µs, not 8,** because its newest version
  sits in a small flush run (an eighth of the keys) whose blocks stay in
  the CPU caches; the baseline's 40.7 µs for 9 blocks is that 4 µs eight
  times plus the base run's 8 µs. The per-block cost is unchanged; only
  the number of blocks fell.
- **No regressions:** consolidated, absent, memtable and snapshot rows,
  and every `scans` row (seeks, short to full scans, under 1, 2 and 9
  runs), are within ±5% of the baseline, mostly faster by noise.

**Comparison harness** (one pass, against the two 5Y passes, `json:64`
and `json:256`, overlapping state): overwritten keys 27.5 → 13.3 µs and
28.8 → 14.0 µs, deleted 27.5 → 13.3 and 29.3 → 13.9 µs, all at 1.00
blocks, now matching RocksDB's 13–14 µs and one block. Localized lookups
1.14 → 1.00 blocks. Present, missing and scan rows match 5Y within its
spread; the large dataset's consolidated present and missing keys read
3% and 6% slower in this single pass (15.25 against 14.83 µs, 1.17
against 1.10 µs, where 5Y's own two passes differed by up to 34% on
missing keys), which is not distinguishable from noise here. Memory is
unchanged: 566.6 MiB allocated at 3.9M records before and after, 141.6
MiB at 1M.

**Metadata overhead:** 8 bytes per run for the sequence number plus 8
for its pointer in the second list; about 600 bytes at 38 runs. The
publication-time sort is of the run count, not the data.

### Run-range lookups (investigation)

A lookup that reaches the runs walks the run list: per run it compares
the key with the run's range (re-reading the run's footer), and only a run
whose range holds the key probes its filter. Missing keys cost 0.08 µs
with 2 runs, 0.31 µs with 10 and 1.10–1.27 µs with 38, about 30 ns a run.

- **Pointer copies are a small part:** `Engine::get` copies the run
  pointers under the lock, so it can read without it; the same missing
  lookups through one held read guard, which copies nothing per lookup,
  cost 1.21 against 1.27 µs (consolidated) and 1.36 against 1.49 µs
  (overlapping) at 38 runs. The remaining 1.1 µs is the range checks.
- **Binary search** needs runs sorted by key and disjoint. Consolidated
  runs are disjoint but kept in publication order (and now also in
  sequence order), not key order; overlapping runs would need an
  interval structure, or a sorted list of starts with a scan back over
  the runs that may still cover the key.
- **Range checks against filter probes:** a range check is two key
  comparisons after parsing the footer; a filter probe is a hash and a few
  cache misses, run only where the range holds the key. Ranges exclude 37
  of 38 runs in consolidated state, so filters are probed once.
- **What an index would buy:** at most about 1 µs on a missing key at 38
  runs, nothing measurable on a key that decodes a 14 µs block, and
  nothing at the 2–10 runs that background compaction normally leaves.
  Keeping each run's parsed range beside it (32–64 bytes a run) would
  likely remove most of the per-run cost without any index.

Not implemented: the benefit is confined to missing keys in large run
counts, and the cheaper step (caching parsed ranges) would come first.

### Memory over repeated cycles

`tessera-cycles` in the comparison harness: load 1M JSON records
(`json:64`), consolidate, then six cycles that each overwrite a quarter of
the keys, flush, settle and consolidate. A snapshot is taken in cycle 3
and dropped in cycle 4. Memory is read after each cycle without trimming;
`malloc_trim` runs once at the end. Run once with glibc's defaults and
once with `GLIBC_TUNABLES=glibc.malloc.mmap_threshold=131072`, which fixes
the threshold above which allocations are mapped individually.

| cycle | live runs + filters MiB | allocated MiB | resident, default | heap free, default | resident, fixed threshold | heap free, fixed threshold |
|---|---|---|---|---|---|---|
| 0 (consolidated) | 148.7 | 149.7 | 244 | 111 | 211 | 61 |
| 1 | 148.8 | 149.9 | 247 | 106 | 212 | 62 |
| 2 | 148.9 | 149.9 | 267 | 125 | 211 | 61 |
| 3 (snapshot held) | 148.9 + 186 retained | 336.1 | 458 | 130 | 397 | 61 |
| 4 (released) | 148.8 | 149.9 | 290 | 148 | 214 | 64 |
| 5 | 149.2 | 150.3 | 301 | 158 | 214 | 63 |
| 6 | 149.4 | 150.5 | 265 | 123 | 214 | 63 |
| after `malloc_trim` | | | 170 | | 152 | |

- **Live data is flat:** runs and filters stay at 149 MiB and the
  allocator's live total at 150 MiB through every cycle; nothing leaks.
- **Snapshot-retained memory** is exactly the superseded runs the
  snapshot pins (186 MiB, `retained_bytes`) and is freed when it drops.
- **Transient compaction memory:** the per-cycle peak is 325–360 MiB
  under defaults and 285–291 MiB with the fixed threshold, against 150
  MiB live: compaction holds its inputs and its output together.
- **Allocator-retained memory is reused, not growing.** Under defaults,
  glibc maps the first run buffers individually (147 MiB mapped), then
  raises its threshold as they are freed, so later run buffers come from
  the heap (83–116 MiB mapped). Freed heap memory is kept and reused:
  free heap moves between 106 and 158 MiB and resident memory between 244
  and 301 MiB with no upward trend (cycle 6 is lower than cycles 4 and 5).
  With the fixed threshold every run buffer is mapped and returned on
  free: resident memory is steady at 211–214 MiB and free heap at 61–64
  MiB (memtable and small allocations), with identical cycle times
  (4.4–6.3 s in both).
- **Stable and peak:** resident memory is 1.6–2.0× the live data under
  defaults and 1.4× with the fixed threshold; the peak with a snapshot
  held is 412–485 MiB.

No production change: the engine allocates through the global allocator
and leaves the policy to the application. A deployment that wants stable
resident memory can set the tunable (or a different global allocator)
without code changes; the measurement supports that, not a custom
allocator or trimming in the engine.

## Memory density (5AA)

*Investigation; the engine is unchanged.* Where Tessera's memory goes
beside RocksDB's on the 5Y datasets, separating what each engine stores,
what the allocator holds and what compaction needs transiently. Three
sources, each measured, not estimated:

- **Stored bytes:** the `density_breakdown` measurement in
  `src/run/tests/costs.rs` rebuilds the 5Y harness's consolidated state
  record for record (keys, values, load and update orders, hence sequence
  numbers) and accounts every byte of the run. Its totals equal the
  engine's in the 5Y runs exactly (87.11 and 138.17 MiB). The same records
  are also built uncompressed and with every sequence number zero.
  RocksDB's split comes from `rocksdb.aggregated-table-properties`, which
  the harness now prints after consolidation (a `details` line).
- **Allocations and resident memory:** `mallinfo2` (live bytes, free heap,
  mapped bytes) and `/proc/self/status` (anonymous resident memory), from
  the harness's `tessera-cycles` (64 MiB memtable) and
  `tessera-cycles-16m` modes, under glibc's defaults and with
  `GLIBC_TUNABLES=glibc.malloc.mmap_threshold=131072`.
- **Compaction peaks:** the harness's `tessera-compactions` and
  `tessera-compactions-lz4` modes load without background compaction, then
  compact the two smallest overlapping runs at a time, sampling live
  allocations and anonymous memory every 5 ms; a snapshot is held through
  the fourth compaction.

Engine charges (`run_bytes`, `filter_bytes`, memtable `charged`,
`retained_bytes`) are compared with allocator figures, not substituted for
them.

### Stored bytes against RocksDB

Consolidated state, Zstandard level 3 and 16 KiB blocks in both,
10-bit filters; MiB, and bytes per live record for the data.

| dataset | engine | data blocks | B/record | index | filter |
|---|---|---|---|---|---|
| sensor, 3.93M records of 8 + 23 B | RocksDB | 74.36 | 19.8 | 0.16 | 4.69 |
| | Tessera | 86.97 | 23.2 | 0.15 | 4.69 |
| | Tessera, sequence numbers zero | 72.53 | 19.3 | 0.14 | 4.69 |
| JSON, 983k records of 8 + 545 B | RocksDB | 133.68 | 142.6 | 0.62 | 1.17 |
| | Tessera | 137.53 | 146.7 | 0.64 | 1.17 |
| | Tessera, sequence numbers zero | 134.21 | 143.2 | 0.63 | 1.17 |

Tessera's data blocks include their 12-byte frame headers (0.09 and
0.38 MiB), RocksDB's its 5-byte trailers. Footers are 32 bytes a run.

- **The gap is almost entirely sequence numbers.** Tessera's
  data blocks are 17% larger than RocksDB's on small records (5Y compared
  runs with SST files, which hold RocksDB's filters too, hence its 10%)
  and 2.9% on JSON. With sequence numbers zeroed, Tessera's blocks are
  2.5% *smaller* than RocksDB's on small records and 0.4% larger on JSON:
  sequence numbers account for all of the small-record gap and about 85%
  of the JSON one, and everything else (block encoding, prefix
  compression, framing, the index, the filter) is within 0.4%.
- **Why:** every record keeps its sequence number, a varint of 3–4 bytes
  here, and after a random-order load those bytes are close to random.
  Uncompressed they add 2.5 bytes a record over a zero; compressed they
  cost 3.9 (sensor) and 3.5 (JSON) bytes a record, more than their raw
  size; presumably they also break up matches across neighbouring fields
  (not separated here). RocksDB stores an 8-byte sequence-and-type trailer
  per key; that it compresses to almost nothing here is a hypothesis, not
  inspected: RocksDB documents rewriting bottom-level sequence numbers to
  zero once no snapshot needs them, and its 19.8 bytes a record sit beside
  Tessera's 19.3 with zeros.
- **Everything else is small:** frame headers, index and footer are 0.27%
  of a sensor run and 0.75% of a JSON run; record framing (four varints a
  record, a 4-byte restart offset per 16 records) less prefix compression
  costs a net 1.2 bytes a record uncompressed on 8-byte keys. Filters
  match RocksDB's within 0.1 MiB (10 bits a key in both).
- **Larger values** were not compared against RocksDB; the per-record
  costs above are fixed, so they shrink in proportion as values grow.

### Allocations and resident memory

`json:64`, 150 MiB of live allocations in every cycle (runs 148–149 MiB,
filters 1.3 MiB); resident memory after each cycle without trimming, for
the cycles without a snapshot, and the per-cycle peak:

| memtable | allocator | stable resident MiB | free heap MiB | mapped MiB | peak MiB | after `malloc_trim` MiB |
|---|---|---|---|---|---|---|
| 64 MiB | glibc defaults | 244–301 | 106–158 | 83–147 | 294–360 | 170 |
| 64 MiB | fixed threshold | 211–214 | 61–64 | 147–148 | 263–290 | 152 |
| 16 MiB | glibc defaults | 209–255 | 59–106 | 50–133 | 289–329 | 178 |
| 16 MiB | fixed threshold | 166–168 | 16–17 | 147–148 | 248–255 | 153 |

- **Live allocations are flat and match the charges:** 150 MiB in every
  cycle and configuration; after loading, live allocations equal runs +
  filters + the memtable's charge within 2 MiB (179.2 against 180.9 MiB in
  the 5Z harness run), so the charges are faithful.
- **Free heap has two sources, both reused.** With every run buffer mapped
  separately (fixed threshold), what remains is about one memtable budget
  (61–64 MiB at 64 MiB, 16–17 MiB at 16 MiB): the freed nodes and values of
  the last flushed memtable, which the next memtable reuses. Under glibc's
  defaults, the dynamic threshold rises past the run buffers' size once the
  first mapped buffer is freed, so later run buffers come from the heap and
  leave 40–95 MiB more of free heap as compactions replace them. Neither
  grows over the six cycles measured.
- **Stable resident memory** is therefore live data plus about one memtable
  budget with the fixed threshold (1.1× live at 16 MiB, 1.4× at 64 MiB),
  and 1.4–2.0× live under defaults. `malloc_trim` returns most of it
  (152–178 MiB), which the engine does not do.
- **Throughput** is the same under both allocator settings (cycle times
  within 1%, 5Z).

### Compaction peaks

Per compaction, live allocations above their level before it (sampled):

| data | codec | input MiB | output MiB | filter build MiB | peak above MiB |
|---|---|---|---|---|---|
| JSON | Zstandard | 22.8 | 22.8 | 1.0 | 25.9 |
| | | 27.8 | 27.8 | 1.0 | 34.6 |
| | | 33.2 | 33.2 | 1.0 | 49.9–50.5 |
| JSON | LZ4 | 38.9 | 38.8 | 1.0 | 46.6 |
| | | 56.6 | 56.6 | 1.0 | 64.1–64.9 |
| sensor | Zstandard | 19.5 | 19.1 | 8.0 | 40.8 |
| | | 62.0 | 61.7 | 16.0 | 82.4 |

- **What coexists:** the inputs stay live until publication (they are
  already counted), and beside them the output's stored bytes and filters,
  the filter-construction buffer of 8 bytes per distinct key of the open
  output run (bounded by the workspace limit), the merge workspace (3.1 MiB
  estimated) and the open run's spare capacity while its buffer doubles:
  0–16 MiB here (measured as the residual), and by the buffer's doubling
  growth at most the open run's own size (derived, not measured).
- **Scaling:** the peak follows the output of the largest single
  compaction, not the data set: proportional to output bytes, so to input
  bytes and codec (LZ4 outputs are 1.7× Zstandard's), plus 8 bytes per
  key in the open run for small records. A consolidation is a sequence of
  such compactions, each freeing its inputs at publication (5Y: 41–154 MiB
  above the live level across datasets); background compaction is bounded
  by `max_input_bytes` and `max_output_bytes` (256 MiB each by default).
- **Snapshots:** a snapshot held across a compaction keeps exactly its
  inputs (25.2, 42.9 and 20.5 MiB here, equal to `retained_bytes`) until
  it is dropped, when they are freed.
- **Admission:** the limits behave as documented: `max_output_bytes`
  counts stored and filter bytes, the workspace limit bounds the hash
  buffer. Neither counts the doubling slack of the open run, now stated
  in the option's documentation; it is transient and at most one run's
  stored size.

### Improvements considered

| candidate | memory now | expected saving | cost | decision |
|---|---|---|---|---|
| Zero sequence numbers of records no other run or snapshot can need | 3.5–3.9 B a record | 17% of sensor runs (14.4 MiB), 2.4% of JSON runs (3.3 MiB) | compaction must prove no other run holds the key; interacts with conflict checks, pruning bounds and recovery's sequence ceiling | not here: a design of its own (below) |
| Reserve an output run's buffer from its expected size | 0–16 MiB of slack per compaction | that slack, at peak only | an estimate that can be wrong both ways; nothing at steady state | not done |
| Keep parsed key ranges beside each run | about 30 ns a run per missing key (5Z) | up to 1 µs a missing key at 38 runs, none in memory | 24–64 bytes a run, a second representation of the footer | not done |
| Smaller frame headers or index | 0.27–0.75% of run bytes | under 1% | a format change | not done |
| Allocator policy (threshold, trimming) | 40–95 MiB of free heap under defaults | to 1.1–1.4× live | process-wide settings that belong to the application | documented, not set by the engine |

### Reading it

- **Established:** Tessera's storage format is as dense as RocksDB's in
  everything but sequence numbers, which explain the whole small-record
  gap and 85% of the JSON one; live allocations match the engine's
  charges within 2 MiB and do not grow; the free heap is reused, one memtable's worth plus, under
  glibc's defaults, run buffers in the heap; compaction peaks follow its
  output.
- **Not changed:** no production code, beyond one sentence of
  documentation. No improvement is both substantial and simple: the large
  one changes what a stored sequence number means.
- **Next:** dropping sequence numbers where no reader can need them. The
  condition is local to a compaction: no run outside its inputs holds the
  key, so the version it writes is the key's only one in any run. The
  open questions are
  equal-sequence conflict checks between zeroed copies, sequence-bound
  pruning (a zeroed run's bound falls, which is still correct by the 5Z
  proof, since bounds come from stored numbers), checkpoint recovery's
  ceiling check, and snapshots, which pin runs and so never read a zeroed
  copy of a version they need.

## Immutable-run layouts (5AB)

*Research; the engine and the run format are unchanged.* Whether another
layout for a memory-resident immutable run moves the density–latency
frontier that 5W–5AA measured: Zstandard's 16 KiB blocks are dense but
cost 13–16 µs a point lookup, and smaller blocks or LZ4 buy speed with
memory. Everything here is run-layer only, in
`src/run/tests/layouts.rs` (a test module; the one new dependency
feature, Zstandard's dictionary builder, is a dev-dependency feature, so
only test, example and benchmark builds compile it). The second pass sets
`LAYOUTS_REVERSED=1`, which reverses the order of the layouts:

```sh
scripts/validate.sh exec 2400 cargo test --locked --release --lib \
  run::tests::layouts::layout_tradeoffs -- --ignored --nocapture
scripts/validate.sh exec 2400 env LAYOUTS_REVERSED=1 cargo test --locked --release --lib \
  run::tests::layouts::layout_tradeoffs -- --ignored --nocapture
```

### Candidates

- **Production run** (the baseline), built by `RunBuilder` at 16, 4 and
  2 KiB block targets with Zstandard level 3, and 16 and 4 KiB with LZ4.
  Only 16 KiB is what the engine builds; the other targets are the same
  format, which readers already accept (`with_target_block_len`).
- **Dictionary blocks** (candidates A and B): production-format blocks
  behind the same kind of sparse index, each compressed against a
  Zstandard dictionary of 64 KiB trained on the run's own records
  (samples of about 100 times the dictionary size, spread evenly over the
  run), at 4 and 2 KiB targets. The same layout without a dictionary is a
  control: it must, and does, reproduce the production run at 4 KiB.
  LZ4 blocks were measured through the production run alone (LZ4 has no
  trained dictionaries).
- **Split** (candidate C): each record's key, sequence number and a
  reference to its value in uncompressed production-format key blocks,
  searched without decoding; each key block's values in one compressed
  group (1 or 4 KiB, Zstandard with a dictionary, or LZ4), decoded only
  to read a value.

Every candidate keeps what the production run keeps: every sequence
number, tombstones, version order, the cut rule that keeps a key's
versions together, a 10-bit Bloom filter per run, and a CRC-32 over
every compressed frame, verified on each decode. Blocks and key blocks
are written by the production `BlockBuilder`, so they refuse duplicate
or out-of-order versions as it does.

### Method

- **Data:** the 5Y harness's datasets rebuilt record for record (keys,
  values, sequence numbers from the load and update orders), with the
  deletes kept as tombstones: one key in 16 is a tombstone, one in 16 is
  at a later version. Sensor, 4.19M keys of 8 + 14–32 bytes; JSON, 1.05M
  of 8 + about 545 bytes; documents, 65.5k of 8 + 1–6 KiB. One run of
  each, one dataset in memory at a time.
- **Correctness first:** before timing, each built layout is scanned
  whole and compared with its records, and 600 lookups (present, deleted,
  missing) and 20 prefix scans are compared with a binary search of the
  records. `layouts_agree_with_the_production_run` (in the default suite)
  checks every layout with tiny blocks against 2,000 shared-prefix and
  binary keys (`0x00`, `0xFF` and their combinations), a 300-version
  chain spanning blocks, three-version chains with a tombstone between,
  keys whose newest version is a tombstone, empty values, every absent
  neighbour and shorter prefix of a third of the keys, and empty, single
  byte and boundary prefixes.
- **Point lookups:** 30,000 present, deleted and missing keys each, in
  random order, through a reader workspace held across calls; a warm-up
  quarter, then the median of three rounds, then a timed round per
  operation for p99.
- **Scans:** 2,000 prefixes of 32 keys and 200 of 512, three rounds,
  and two full scans; the seek is the time to the first record.
- **Pairing:** every layout reads the same keys in the same order on the
  same records. The matrix ran twice, the second time with the layouts
  in reverse order. The ranges below are the two passes: 199 of 231
  cells agree within 10%, the rest within 18% for present lookups and
  29% for Split's sub-microsecond deleted lookups and seeks, and every
  comparison the decision rests on goes the same way in both. Within a
  pass the three rounds of a cell differ by at most 8%. A first pass
  that held all three datasets at once read the production rows about 2×
  slower than an isolated probe of the same binary did (28.8 against
  13.75 µs on JSON); with one dataset at a time it matches the isolated
  probe and the 5Y run figures, so that pass is discarded.
- **Accounting:** everything a run holds for as long as it lives:
  compressed blocks or groups, uncompressed key blocks, frame headers,
  footers and checksums, the index of block offsets and last keys, the
  filter, and the dictionary in its decoding form (`DDict`, 89 KiB for a
  64 KiB dictionary). Per reader, the decoding context and buffers. Not
  counted: the encoder's dictionary (`CDict`) and samples, which live
  only while building.

### Results

Two passes; MiB of the whole run, and change against the production 16
KiB run.

| dataset | layout | total MiB | vs run 16 KiB | present µs | deleted µs | missing ns | seek µs | 32-key scan µs | 512-key scan µs | full scan ns/record |
|---|---|---|---|---|---|---|---|---|---|---|
| sensor | production, Zstd 16 KiB blocks | 93.7 | +0% | 15.03–15.57 | 14.94–14.96 | 151 | 15.0 | 16.6 | 39–40 | 48 |
|  | production, Zstd 4 KiB blocks | 97.2 | +4% | 5.21–5.23 | 5.20–5.23 | 72–73 | 5.0–5.1 | 6.9–7.0 | 34 | 56 |
|  | production, Zstd 2 KiB blocks | 101.3 | +8% | 3.79–3.86 | 3.83–3.85 | 61–62 | 3.7 | 6.0 | 40 | 70 |
|  | production, Lz4 16 KiB blocks | 111.2 | +19% | 4.66–4.68 | 4.65–4.66 | 67–70 | 4.6 | 5.6 | 19 | 29 |
|  | production, Lz4 4 KiB blocks | 114.3 | +22% | 1.48 | 1.46–1.47 | 45 | 1.4 | 2.3–2.4 | 16 | 28 |
|  | blocks, Zstd 4 KiB, no dict | 97.1 | +4% | 5.17–5.24 | 5.14–5.15 | 62 | 5.0–5.1 | 6.8 | 32 | 51 |
|  | blocks, Zstd 4 KiB + 64 KiB dict | 104.8 | +12% | 4.95–4.98 | 4.99–5.01 | 61–63 | 4.9–5.0 | 6.6–6.7 | 31 | 50 |
|  | blocks, Zstd 2 KiB + 64 KiB dict | 108.5 | +16% | 3.49–3.55 | 3.52–3.72 | 49–59 | 3.4–4.0 | 5.4–6.1 | 35–39 | 61–63 |
|  | split, Zstd 4 KiB groups + 64 KiB dict | 123.8 | +32% | 5.94–6.32 | 0.55–0.66 | 27–35 | 5.6–5.8 | 7.1–7.3 | 28–31 | 44–46 |
|  | split, Zstd 1 KiB groups + 64 KiB dict | 129.4 | +38% | 3.20–3.92 | 0.50–0.58 | 27–28 | 2.8–3.4 | 5.0–5.7 | 37–39 | 67–72 |
|  | split, Lz4 4 KiB groups | 130.3 | +39% | 2.06–2.19 | 0.52–0.67 | 27–28 | 1.8–1.9 | 2.6–2.7 | 14 | 24–25 |
| JSON | production, Zstd 16 KiB blocks | 140.2 | +0% | 13.56–13.85 | 13.54–13.82 | 155–157 | 13.6–13.8 | 27.3–27.7 | 232–236 | 427–437 |
|  | production, Zstd 4 KiB blocks | 173.2 | +24% | 5.53–6.07 | 5.50–6.05 | 80–89 | 5.5–6.4 | 26.3–29.2 | 337–371 | 647–711 |
|  | production, Zstd 2 KiB blocks | 209.8 | +50% | 4.09–4.46 | 4.02–4.42 | 66–74 | 4.2–4.8 | 33.9–37.2 | 481–524 | 922–996 |
|  | production, Lz4 16 KiB blocks | 237.6 | +70% | 5.60–6.00 | 5.68–6.10 | 80–87 | 5.6–6.2 | 11.2–12.2 | 95–98 | 174–184 |
|  | production, Lz4 4 KiB blocks | 265.9 | +90% | 1.72–1.85 | 1.69–1.83 | 45–50 | 1.8–2.1 | 7.3–8.2 | 90–97 | 171–182 |
|  | blocks, Zstd 4 KiB, no dict | 172.7 | +23% | 5.43–5.70 | 5.41–5.56 | 70 | 5.5–5.7 | 26.0–26.4 | 333–349 | 637–652 |
|  | blocks, Zstd 4 KiB + 64 KiB dict | 130.0 | −7% | 2.98–3.19 | 2.95–3.14 | 49–52 | 3.0–3.3 | 13.6–14.7 | 172–174 | 330–349 |
|  | blocks, Zstd 2 KiB + 64 KiB dict | 139.0 | −1% | 1.58–1.84 | 1.53–1.69 | 36–38 | 1.5–1.9 | 11.6–12.4 | 162–164 | 313–332 |
|  | split, Zstd 4 KiB groups + 64 KiB dict | 130.4 | −7% | 2.97–3.34 | 0.24–0.33 | 26–29 | 2.9–3.4 | 13.0–14.2 | 165–167 | 315–333 |
|  | split, Zstd 1 KiB groups + 64 KiB dict | 160.9 | +15% | 1.24–1.50 | 0.31–0.35 | 26 | 1.2–1.8 | 11.8–13.2 | 170–185 | 326–351 |
|  | split, Lz4 4 KiB groups | 268.4 | +92% | 1.87–1.99 | 0.27–0.29 | 26 | 1.8–2.0 | 7.3–7.6 | 89–90 | 171–176 |
| documents | production, Zstd 16 KiB blocks | 21.4 | +0% | 12.20–12.53 | 12.11–12.81 | 119–125 | 12.1–12.9 | 54.3–57.1 | 685–714 | 1414–1464 |
|  | production, Zstd 4 KiB blocks | 26.3 | +23% | 4.85–5.20 | 4.60–4.98 | 61–65 | 4.9–5.5 | 68.5–74.1 | 1023–1076 | 1987–2097 |
|  | production, Zstd 2 KiB blocks | 31.2 | +46% | 3.72–3.90 | 2.51–2.65 | 52–54 | 3.9–4.6 | 93.0–98.7 | 1427–1432 | 2786–2787 |
|  | production, Lz4 16 KiB blocks | 39.1 | +83% | 4.57 | 4.47 | 58–61 | 4.5–4.6 | 19.5–19.6 | 242–243 | 465–466 |
|  | production, Lz4 4 KiB blocks | 43.4 | +103% | 1.38–1.39 | 1.21–1.22 | 34 | 1.4 | 16.7 | 243 | 474–476 |
|  | blocks, Zstd 4 KiB, no dict | 26.2 | +22% | 4.76–4.78 | 4.51–5.21 | 54–64 | 4.8–4.9 | 67.8–68.0 | 1009 | 1961–1965 |
|  | blocks, Zstd 4 KiB + 64 KiB dict | 21.9 | +2% | 2.88–2.90 | 2.70–2.72 | 40 | 2.9 | 39.5–39.6 | 588–589 | 1144–1149 |
|  | blocks, Zstd 2 KiB + 64 KiB dict | 23.6 | +10% | 1.65 | 1.02–1.03 | 31 | 1.8 | 39.1–39.2 | 596–597 | 1163–1177 |
|  | split, Zstd 4 KiB groups + 64 KiB dict | 22.1 | +3% | 2.82–2.88 | 0.16 | 20–21 | 2.9 | 38.4–38.6 | 572–576 | 1119–1125 |
|  | split, Zstd 1 KiB groups + 64 KiB dict | 24.5 | +15% | 1.45–1.46 | 0.16–0.17 | 20 | 1.6 | 37.8 | 577–579 | 1121–1125 |
|  | split, Lz4 4 KiB groups | 43.8 | +105% | 1.39 | 0.17 | 20–21 | 1.5 | 16.8–16.9 | 243–247 | 475–477 |

p99s are 1.1–2.0× the medians (highest for Split, whose lookups decode
a key block's group of varying size); they are in the test's output, not
repeated here.

### Where the bytes go

MiB by role, for the rows that matter:

| dataset | layout | compressed | key blocks | index | framing | dictionary | filter |
|---|---|---|---|---|---|---|---|
| sensor | production, Zstd 16 KiB | 88.43 | — | 0.15 | 0.09 | — | 5.00 |
| | production, Zstd 4 KiB | 91.27 | — | 0.60 | 0.36 | — | 5.00 |
| | blocks, Zstd 4 KiB + dict | 98.88 | — | 0.60 | 0.24 | 0.089 | 5.00 |
| | split, Zstd 4 KiB + dict | 69.73 | 48.15 | 0.59 | 0.25 | 0.089 | 5.00 |
| JSON | production, Zstd 16 KiB | 137.89 | — | 0.64 | 0.38 | — | 1.25 |
| | production, Zstd 4 KiB | 167.87 | — | 2.55 | 1.53 | — | 1.25 |
| | blocks, Zstd 4 KiB + dict | 125.09 | — | 2.55 | 1.02 | 0.089 | 1.25 |
| | blocks, Zstd 2 KiB + dict | 130.49 | — | 5.10 | 2.04 | 0.089 | 1.25 |
| | split, Zstd 4 KiB + dict | 110.71 | 13.39 | 3.49 | 1.50 | 0.089 | 1.25 |
| documents | production, Zstd 16 KiB | 21.10 | — | 0.14 | 0.08 | — | 0.08 |
| | blocks, Zstd 4 KiB + dict | 20.97 | — | 0.56 | 0.22 | 0.089 | 0.08 |

The candidates' frame headers are 8 bytes against production's 12 (no
codec byte; one codec a run), worth 0.12 MiB on sensor at 4 KiB and
less elsewhere; their index and filter are the production run's.
Readers hold 96–111 KiB each in every Zstandard layout (the decoding
context and a decoded block or group) and 4–18 KiB with production's
LZ4 (the candidates' harness holds a Zstandard context in any case); the
dictionary's decoding form is per run and shared by its readers.

### Dictionaries and block size

- **On structured values a dictionary decouples density from block
  size.** Without one, a 4 KiB block costs JSON 24% and documents 23%
  more memory than a 16 KiB block, because each block compresses only
  what it holds; with a 64 KiB dictionary trained on the run, 4 KiB
  blocks are 7% *smaller* than production's 16 KiB blocks on JSON and
  2.4% larger on documents, and 2 KiB blocks are 1% smaller and 10%
  larger. The dictionary holds what the records share (field names,
  structure, vocabulary), which large blocks otherwise learn afresh in
  every block.
- **On small binary records it does the opposite.** Sensor values are
  mostly numbers and the block's own neighbours are its best context: the
  dictionary makes 4 KiB blocks 7.9% larger than the same blocks without
  it, for 4% faster lookups. Plain smaller blocks are the better point:
  production's own format at 4 KiB costs 3.8% more memory than at 16 KiB
  and looks up 2.9–3.0× faster; LZ4 at 4 KiB, 22% more for 10×.
- **Lookup time follows decoded bytes.** A dictionary does not slow
  decoding measurably (4.95–4.98 against 5.17–5.24 µs at 4 KiB
  on sensor, where the blocks hold the same records), so its speed comes
  entirely from the smaller blocks it makes affordable.
- **Scans:** dictionary blocks scan JSON 1.9–2.0× faster than
  production's 16 KiB blocks for 32-key prefixes, 1.3–1.4× for 512 keys
  and 1.2–1.3× for a full scan, and documents 1.2–1.4×: blocks that
  compress better decode faster per record. LZ4 remains the fastest to scan at
  any block size, at 1.7–2× the memory on structured values.

### Split

- **No density advantage.** Separating keys from values compresses the
  values better (JSON value groups are 110.7 MiB against 125.1 MiB of
  dictionary blocks), but the uncompressed key blocks give it all back:
  13.4 MiB on JSON (a tie, 130.4 against 130.0 MiB) and 48.2 MiB on
  sensor, where keys and sequence numbers are most of the record (+32%
  against production). Compressing the key blocks would remove the
  reason for separating them.
- **Present lookups are no faster** than dictionary blocks of the same
  decoded size (2.97–3.34 against 2.98–3.19 µs on JSON), since the value
  group is still decoded.
- **What it does better is answering without values:** deleted keys
  cost 0.16–0.66 µs against 1.0–5.0 µs, because a tombstone and its
  sequence number are found without decompressing anything; the same
  would hold for existence checks and key-only scans, which the engine
  does not offer.

### Construction

- **CPU:** one build per layout per pass (two in all; they agreed
  within about 0.1 s). Training on samples of about 6.4 MiB took 0.21–0.23 s
  on JSON, 0.14–0.15 s on documents and 0.66–0.68 s on sensor (many
  small samples); compressing against the dictionary cost no more than
  without it. Builds of the whole run: JSON 1.52 s with a dictionary at
  4 KiB against 1.31 s for production's 16 KiB blocks and 1.88 s for its
  4 KiB blocks; documents 0.36 against 0.21 s; sensor 1.55 against
  0.67 s.
- **Transient memory:** not isolated. The high-water mark of resident
  memory above its level before each build (in the test's output)
  reflects reuse of the previous layout's freed heap, so it understates
  every row; it shows nothing above the stored size beyond what the
  production builder's doubling buffer already causes (5AA). By
  construction, a dictionary adds the sample copy (about 100 times the
  dictionary, 6.4 MiB here), the trainer's workspace and the encoder's
  dictionary for the length of the build; their sizes were not measured.

### Correctness and differences from production

- **Checked:** every layout returns exactly the production run's
  records, newest versions, absences and prefix scans, on the tricky
  records above (in the default suite) and on every dataset before it is
  timed. Ordering, missing keys, prefix boundaries, iteration across
  blocks, tombstones, version precedence and long chains are covered;
  duplicate copies of a version are refused by the block builder all
  layouts use; each frame is decoded independently, from its own offset
  and the run's dictionary.
- **Not production:** the candidates are in-memory structures, not a
  serialized format: no footer parsing, no format version, no recovery
  path, and no errors (damage is an assertion). A dictionary block
  depends on the run's dictionary, so it is independent of other blocks
  but not of its run. Split checks its key blocks' CRCs once at build,
  as production checks uncompressed frames once at load. The engine's
  own lookups go through more than one run, a memtable and snapshots;
  none of that is here, and the gaps above are run-layer gaps.

### What adopting dictionary blocks would involve

Not designed, but these are the questions it raises:

- **Format:** a dictionary section in the run (the 64 KiB of raw
  dictionary, which the run's bytes would hold beside its decoding form:
  an estimated 0.15 MiB a run in all), a codec value meaning "Zstandard
  with the run's dictionary", a format version, and the dictionary
  covered by the run's integrity checks.
- **When to train:** a flush of a small memtable gives a small run and
  few samples, and 64 KiB is 1.6% of a 4 MiB run; compactions, which
  write the large runs that hold the data, could train on their own
  output and leave flushes as they are. Every compaction would retrain
  (0.14–0.68 s a run here, roughly fixed by the capped sample size) or
  reuse an input's dictionary.
- **Choosing:** sensor shows a dictionary can hurt; a builder would
  need to compare, an adaptive selection this milestone excluded.
- **Recovery and checkpoints:** a checkpoint stores runs as they are,
  so it would carry each run's dictionary; recovery validates the
  dictionary before any block; old runs stay readable.
- **Snapshots:** unaffected: runs are immutable and own their
  dictionaries, and snapshots pin runs.
- **Memory admission:** the dictionary's two forms belong in
  `run_bytes`; the encoder's dictionary and samples in the compaction
  workspace.

### Sequence numbers

Not changed here. Sequence numbers are most of the gap to RocksDB on
small records (5AA: 3.9 bytes a record compressed on sensor, 3.5 on
JSON). For this milestone:

- **Why they cost memory:** every record stores the sequence number of
  the write that committed it, and after a random-order load
  neighbouring records' numbers are unrelated, so they compress poorly.
  An encoding that keeps the exact numbers cannot go below their entropy:
  for 4.19M records whose numbers are a permutation of about as many
  writes, log2 of the orderings is about 20.6 bits, or 2.6 bytes, a
  record. Against 3.9 bytes now, a better encoding with the same meaning
  could save at most about 1.3 bytes a record, about 6% of a sensor run
  (an estimate, not measured; it is a separate opportunity, not pursued);
  Split's uncompressed key blocks, which store them as plain varints, save
  nothing.
- **What zeroing would change:** a zero no longer says when a version was
  written, so it is only correct where no reader can need that: no other
  run, the memtable or a snapshot holds an older or newer version of the
  key that a reader must order against it. Compaction would have to
  prove that for each key it zeroes (5AA's "Next" lists the open
  questions: equal-sequence conflict checks, sequence-bound pruning,
  recovery's sequence ceiling, and snapshots).
- **Original and compacted representations:** the committed sequence
  number is part of a version's identity: snapshots, write ordering and
  recovery depend on it. A compacted representation that stores less
  (zero, or a rank within the run) is a different value with a narrower
  meaning, valid only under the proof above, and anything that compares
  sequence numbers across runs must then know which kind it holds. None
  of the layouts measured here needs that; all of them keep the
  committed numbers exactly.

### Decision

- **The frontier moves for compressible structured values.** Dictionary
  blocks at 4 KiB are 7% smaller than production's 16 KiB blocks on JSON
  and look up 4.3–4.6× faster; at 2 KiB they are 1% smaller and 7.4–8.8×
  faster. On documents they cost 2.4% more memory for 4.2–4.4×. Both
  exceed the milestone's bar (about 2× at similar density). This is
  measured at the run layer only.
- **It does not move for small binary records** through dictionaries;
  there, production's own format at 4 KiB blocks gives 2.9–3.0× for
  3.8% more memory, with no format change.
- **Split is not worth pursuing** as a whole: no density gain over
  dictionary blocks and none of their lookup gain beyond tombstones.
- **Not implemented:** the run format, the engine's block size and its
  codecs are unchanged. The evidence is compelling enough for a design
  milestone, not for a format change without one: the engine-level gain
  (more than one run, the memtable, mixed workloads), building and
  training on compaction, and the format questions above are untested.

## Block length through the engine (5AC)

*Engine measurements; the run format is unchanged.* 5AB found, at the run
layer, that 4 KiB Zstandard blocks look up about 3× faster than 16 KiB
blocks for 4% more memory on small records. This section measures block
length alone (16, 4 and 2 KiB, Zstandard, with LZ4 at 16 and 4 KiB beside
it) through the complete engine, before any dictionary.

### The option

- **Format:** none changed. Where a block is cut was already a builder
  policy, not part of the format (`src/run/mod.rs`): each frame records
  its decoded length, readers accept any length up to the block limit,
  and compaction sizes its workspace from the largest block of its
  inputs. `RunBuilder::with_target_block_len` already existed.
- **`EngineOptions::target_block_len`** (default 16 KiB, so nothing
  changes unless it is set) reaches every run the engine builds: memtable
  flushes, compaction output and checkpoints, which rebuild their runs
  from a snapshot scan. Existing runs keep their blocks until compaction
  or a checkpoint rewrites their records; nothing is rewritten because
  the setting changed. Values above 64 KiB are treated as 64 KiB: a
  Zstandard compression context grows with its block (0.28 MB at 16 KiB,
  0.66 MB at 64 KiB, 1.3 MB from 256 KiB, measured), and beyond 64 KiB it
  would outgrow the 1 MiB per context that compaction's workspace
  estimate allows. Smaller values down to a block per record are valid;
  the run's index limits hold for any block length, since a run's input
  bounds its record count.
- **Tests:** `runs_of_any_block_length_recover_mix_and_are_rewritten`
  (`tests/checkpoint.rs`) writes runs at 16 KiB, 2 KiB and 256 B and
  recovers each checkpoint under every setting. It reads the recovered
  runs beside newer runs of another length (overwrites, deletes, keys
  only the old run holds), checks that recovered runs keep their blocks,
  that a checkpoint of the mixed engine and a compaction rewrite in the
  current length, and that a snapshot taken before the compaction still
  reads what it saw. The randomized engine test against its model now
  also draws block lengths of 64 B, 2 KiB and 16 KiB.
- **Benchmarks:** `BENCH_BLOCK_LEN` and `BENCH_COMPRESSION` select the
  block length and codec of every run `examples/bench.rs` builds;
  `BENCH_BLOCK_LEN` does the same for the `tools/compare` harness's
  `tessera` engine.

### Method

- **Engine benchmarks:** the 5V `lookups` and `scans` modes (500,000
  records of 8 + 100 bytes in six storage shapes: memtable, consolidated,
  9 overlapping runs, a run under a newer run, run + memtable, its
  snapshot, and a 9-run history), and the `density` and `maintenance`
  modes, for each of the five configurations.
- **Real payloads:** the 5Y harness's `tessera` engine on `sensor:256`
  (4.19M records, 8 + 14–32 bytes) and `json:64` (1.05M records, 8 +
  about 545 bytes), with background compaction, in its overlapping and
  consolidated states. RocksDB was not rerun.
- **Pairing:** each matrix ran twice, the second pass in reverse order;
  ranges below are the two passes. Every 4 and 2 KiB cell agreed within a
  few percent. Some 16 KiB scan cells in the harness differed by up to
  25% (16 KiB ran first in one pass); both values are given. Every scan
  in `scans` and every full scan in the harness was checked against a
  model.
- **Memory:** only from `density` and the harness. The `lookups`, `scans`
  and `maintenance` modes draw their 100-byte values from a pool of 64,
  which large blocks find again: stored bytes are 0.31 of the payload at
  16 KiB and 0.67 at 4 KiB, far more sensitive than any real payload.
  Their latencies are comparable; their memory is not.

### Point lookups

Harness, consolidated state, p50 µs (p99), and throughput per thread:

| dataset | block | present | overwritten | localized | deleted / missing | lookups/s, present |
|---|---|---|---|---|---|---|
| sensor | 16 KiB | 14.75–14.88 (19.2–19.3) | 14.75–14.83 | 13.3 | 0.13 | 67–68k |
| | 4 KiB | 5.25 (5.9–6.4) | 5.25 | 4.63 | 0.13 | 188–189k |
| | 2 KiB | 3.92 (4.3–4.6) | 3.88–3.92 | 3.46 | 0.13 | 255k |
| JSON | 16 KiB | 13.75–14.42 (17.8–20.0) | 13.75–14.42 | 13.0–14.2 | 0.29–0.33 | 67–72k |
| | 4 KiB | 5.79–5.88 (6.6–7.6) | 5.83–5.88 | 5.38–5.42 | 0.33 | 168–171k |
| | 2 KiB | 4.46–4.58 (5.3–6.0) | 4.42–4.58 | 4.00–4.13 | 0.33 | 215–223k |

With overlapping runs (sensor 5 runs, overlap 3; JSON 10–11 runs), present
keys took 15.0–15.1, 5.29 and 3.96 µs on sensor and 13.8, 5.8–5.9 and
4.5–4.6 µs on JSON; overwritten and deleted keys, found in the newer run
alone (5Z's sequence-bound pruning), 17.5, 5.6 and 3.8 µs on sensor.

`lookups` mode, p50 µs (the same in both passes within 0.1 µs unless a
range is given):

| shape, class | Zstd 16 KiB | 4 KiB | 2 KiB | LZ4 16 KiB | LZ4 4 KiB |
|---|---|---|---|---|---|
| consolidated, present random | 8.04 | 4.46–4.50 | 3.29 | 2.04–2.08 | 0.88–0.92 |
| consolidated, hot 4,096-key range | 7.50 | 3.96 | 2.96 | 1.42 | 0.54 |
| consolidated, overwritten | 8.08–8.12 | 4.46–4.50 | 3.25–3.29 | 2.00–2.08 | 0.88 |
| 9 overlapping runs, present | 8.46–8.50 | 5.04–5.08 | 3.67–3.79 | 2.71 | 1.04–1.08 |
| snapshot of run + memtable, present | 8.12–8.83 | 5.46–5.62 | 3.88–3.92 | 2.12–2.54 | 1.33 |
| 9-run history, overwritten in 8 runs | 3.92–4.04 | 2.67–2.75 | 2.42 | 1.21–1.25 | 0.54–0.58 |
| 9-run history, snapshot after 4 | 3.92–4.04 | 2.62–2.75 | 2.42 | 1.21 | 0.50 |

- **Gain:** 4 KiB blocks make a run lookup 1.8× (100-byte values) to
  2.8× (sensor) faster, 2 KiB blocks 2.4–3.8×, p99 alike; LZ4 at 4 KiB
  9×. The engine keeps less of the run layer's gain (5AB: 2.9–3.0× at
  4 KiB on sensor) because a lookup's fixed costs (memtables, run
  selection, the filter, the index search) stay.
- **Blocks decoded are unchanged:** 1.00 a present lookup on a
  consolidated run, 1.01–1.07 with overlap, 1.00 for keys overwritten in 8
  runs, in every configuration: pruning and filters do not depend on
  block length. Bytes decompressed per lookup are therefore about one
  block, roughly the target (derived, not counted). No lookup allocates.
- **Unaffected:** memtable reads, filter-rejected keys (deleted keys
  absent from a consolidated run, missing keys: 0.04–0.33 µs) and keys
  beyond every run.

### Prefix and ordered scans

Harness, consolidated state; the seek is the time to the first record:

| dataset | block | seek µs | 30 records µs | 480 µs | 1,920 µs | 15,360 ms | ns/record after seek | blocks: 30 / 480 / 1,920 | full scan M records/s |
|---|---|---|---|---|---|---|---|---|---|
| sensor | 16 KiB | 14.5–14.7 | 16.3–16.8 | 48–58 | 146–187 | 1.05–1.37 | 68–94 | 1.06 / 1.97 / 4.87 | 11.1–14.3 |
| | 4 KiB | 5.3–5.4 | 6.7–6.8 | 42.4–42.5 | 150 | 1.17 | 76–79 | 1.24 / 4.77 / 16.1 | 12.9–13.0 |
| | 2 KiB | 4.0 | 5.6 | 48.1–48.5 | 177 | 1.39–1.40 | 91–96 | 1.51 / 8.58 / 31.2 | 10.9–11.0 |
| JSON | 16 KiB | 13.8–14.7 | 27.7–29.3 | 246–261 | 914–971 | 7.2–7.6 | 466–524 | 2.0 / 17.3 / 66 | 2.08–2.12 |
| | 4 KiB | 6.0–6.2 | 26.7–27.0 | 344–345 | 1,357–1,368 | 10.7–10.8 | 699–744 | 5.0 / 66 / 262 | 1.41–1.42 |
| | 2 KiB | 4.7–5.0 | 34.3–34.9 | 484–490 | 1,926–1,945 | 15.3–15.5 | 996–1,053 | 9.1 / 131 / 524 | 1.00 |

`scans` mode (100-byte values; µs per scan):

| shape, prefix | Zstd 16 KiB | 4 KiB | 2 KiB | LZ4 16 KiB | LZ4 4 KiB |
|---|---|---|---|---|---|
| consolidated, 32 keys | 11.0 | 8.8–9.3 | 8.5–8.7 | 3.8 | 2.4 |
| consolidated, 512 keys | 48.7–49.2 | 70.1–73.0 | 87.6–87.7 | 25.1–25.5 | 23.2–23.7 |
| consolidated, 2,048 keys | 169 | 269–284 | 337 | 93–96 | 89–91 |
| consolidated, every key (ms) | 39.1–39.7 | 63.7–66.9 | 79.9–80.7 | 22.0–22.2 | 21.7 |
| consolidated, no match inside the range | 7.6 | 4.4–4.6 | 3.0–3.1 | 1.5 | 0.6 |
| 9 overlapping runs, 32 keys | 82.6–84.3 | 50.9–53.1 | 41.4–41.7 | 25.4–25.6 | 12.4–12.7 |
| 9 overlapping runs, 512 keys | 165–167 | 166–177 | 185–186 | 85–86 | 72 |
| 9 overlapping runs, no match inside the range | 71.4–72.2 | 37.2–38.9 | 28.5–28.7 | 15.1–15.3 | 4.9 |
| run + newer run, 32 keys | 21.1–21.4 | 16.6–17.2 | 14.8–14.9 | 6.6–6.7 | 3.9–4.0 |
| run + newer run, 512 keys | 75.6–76.2 | 109–114 | 129–130 | 45.6–46.5 | 34.7–34.8 |

- **Seeks gain what lookups gain:** 2.2–2.8× at 4 KiB and 2.8–3.7× at
  2 KiB on the real payloads (1.7–1.9× and 2.5× on the 100-byte values),
  for each run the scan reads: a no-match prefix inside the key range of
  9 overlapping runs takes 1.9× and 2.5× less.
- **Iteration loses under Zstandard:** each block costs a fixed amount
  to decode besides its bytes, so iteration slows as blocks shrink:
  100-byte values from 93 ns a record at 16 KiB to 150–159 at 4 KiB and
  about 190 at 2 KiB; JSON from about 490 to 700–740 and 1,000–1,050;
  sensor, whose blocks hold many records, from 68–94 to 76–79 and
  91–96. From the full scans, an extra Zstandard block costs about
  2–3 µs beyond its bytes (derived: the difference in scan time over the
  difference in blocks). LZ4 shows almost none: iteration is 48–55 ns a
  record at both lengths.
- **Crossover:** at 4 KiB, a scan gains while the seek dominates. On
  sensor it is ahead up to about 500 records and level at 2,000; on JSON
  level at 30 records and 1.35–1.5× slower beyond; full scans run 1.5×
  slower on JSON and 1.65× on the 100-byte values, about level on sensor.
  2 KiB is slower than 4 KiB on every scan longer than about 30 records.
- **Workspace:** one decoded block per participating run: 16, 4 and
  2 KiB for one run, 144, 36 and 18 KiB for nine; two allocations a scan
  in every configuration. Block transitions follow records over block
  capacity: 4× and 8× as many at 4 and 2 KiB.

### Memory

Consolidated, from `density` (one run, filters included) and the harness
(the engine's charge, and resident memory after trimming):

| payload | block | run MiB | vs 16 KiB | heap B/record in runs | resident MiB |
|---|---|---|---|---|---|
| sensor, 1.5M × 14–32 B (`density`) | 16 KiB | 33.8 | | 25 | |
| | 4 KiB | 34.5 | +2.1% | 25 | |
| | 2 KiB | 35.7 | +5.6% | 26 | |
| | LZ4 4 KiB | 41.2 | +22% | 30 | |
| JSON, 120k × ~150 B–1 KiB (`density`) | 16 KiB | 16.7 | | 147 | |
| | 4 KiB | 20.8 | +25% | 183 | |
| | 2 KiB | 25.3 | +52% | 223 | |
| documents, 15k × 1–6 KiB (`density`) | 16 KiB | 8.6 | | 600 | |
| | 4 KiB | 10.9 | +27% | 762 | |
| | 2 KiB | 12.1 | +41% | 845 | |
| sensor, 4.19M (harness) | 16 KiB | 87.1 + 4.7 filter | | | 115 |
| | 4 KiB | 90.4 + 4.7 | +3.8% | | 118 |
| | 2 KiB | 94.1 + 4.7 | +8.0% | | 126 |
| JSON, 1.05M (harness) | 16 KiB | 138 + 1.2 filter | | | 159 |
| | 4 KiB | 171 + 1.2 | +24% | | 197 |
| | 2 KiB | 208 + 1.2 | +51% | | 238–243 |

- **Where the bytes go:** almost all of the growth is compression lost to
  smaller blocks; index and frame headers grow from 0.27% of a sensor
  run at 16 KiB to about 1% at 4 KiB (5AB's run-layer accounting), and
  filters do not change. Metadata per run (footer, key range) is fixed.
- **Small records pay little; structured values pay a quarter.** Sensor
  blocks hold many short, similar records, so a 4 KiB block still finds
  most of what 16 KiB finds; JSON and documents lose the repetition
  between records, which is what 5AB's dictionaries recover.

### Construction

| | 16 KiB | 4 KiB | 2 KiB |
|---|---|---|---|
| flush, 250k × 108 B (`maintenance`), ms | 78–79 | 145–150 | 170 |
| compaction, 8 runs × 250k, ms | 412–414 | 826–828 | 990 |
| sensor (harness), consolidation CPU s | 2.20–2.22 | 2.41–2.43 | 2.83 |
| sensor, load CPU s | 3.03–3.11 | 3.10–3.14 | 3.31–3.37 |
| JSON (harness), base consolidation CPU s | 3.39–3.40 | 4.31–4.38 | 6.80–8.16 |
| JSON, final consolidation CPU s | 4.70–4.93 | 4.86–4.91 | 8.82–9.23 |
| JSON, load CPU s | 2.95–3.00 | 3.82–3.85 | 4.77–4.84 |
| peak resident MiB, sensor / JSON consolidation | 224–248 / 243–280 | 236–253 / 294–335 | 248–272 / 361–393 |

- **Zstandard pays per block when compressing too:** flushes of the
  100-byte values take 1.9× and 2.2× as long; on real payloads,
  consolidation costs 9% (sensor) to 27% (JSON) more CPU at 4 KiB and 28%
  to 2× more at 2 KiB. LZ4 flushes and compactions cost the same at both
  lengths (45–47 and 227–237 ms).
- **Peaks** grow with the stored size of what is built (5AA: output plus
  slack), so by the memory growth above; workspace estimates shrink with
  the block (9.4 to 9.2 MiB in `maintenance`, dominated by contexts).

### Decision

- **The option stays, the default stays 16 KiB.** Smaller blocks are a
  real, reproducible engine-level gain for point reads and seeks
  (2.4–2.8× at 4 KiB on the real payloads, 1.8× on the 100-byte values;
  3.0–3.8× at 2 KiB), with no correctness or
  maintenance cost: the format already allowed them, the change is one
  option passed to three builders, and compatibility is tested. But the
  memory cost depends on the payload: about 4% on small binary records,
  24–27% on structured values, where long scans and compaction also slow
  by up to 1.5× and 27%. A default has to suit both, and 16 KiB keeps
  density and scans; 4 KiB is an explicit choice for point-read-heavy
  workloads, best on small records.
- **2 KiB is not worth recommending under Zstandard:** a further 1.3×
  on lookups for a second step of memory, slower scans than 4 KiB beyond
  30 records, and about twice the consolidation CPU of 16 KiB on JSON.
- **The per-block cost of Zstandard limits small blocks** (about 2–3 µs a
  block beyond its bytes, here). Where it comes from was not examined
  (frame setup on each decode and encode is the likely candidate); it
  bounds what any small-block layout, dictionaries included, can gain on
  scans and construction.

### Toward dictionaries

From 5AB (run layer) and the measurements above; estimates where marked.

- **Initial target: 4 KiB.** At the run layer dictionary blocks at 4 KiB
  were 7% smaller than today's 16 KiB blocks on JSON and 4.3–4.6×
  faster to look up; at 2 KiB only 1% smaller for 7.4–8.8×. Through the
  engine, plain 2 KiB blocks bought 1.3× over 4 KiB on lookups while
  costing more on scans (1.4× slower iteration on JSON) and up to 1.9×
  the compaction CPU; a dictionary does not remove the per-block decode
  and encode cost. 4 KiB gives most of the lookup gain at the lowest scan
  and construction cost.
- **What a dictionary would buy back:** 4 KiB blocks alone cost JSON 24%
  and documents 27% more memory; dictionaries turned that into −7% and
  +2.4% at the run layer. On sensor they cost memory (5AB: +7.9%), and
  plain 4 KiB blocks already give 2.8× for 3.8%.
- **Minimum run size (estimate):** a 64 KiB dictionary costs about
  150 KiB a run held in memory (raw bytes in the run plus the 89 KiB
  decoding form), 1% of a 15 MiB run; training wanted about 6.4 MiB of
  samples. Runs of a few MiB or less, such as flushes of small memtables,
  are poor candidates; the default run target is 64 MiB of input.
- **Compaction only:** compaction writes the runs that hold the data and
  could train on its own output; flushes would stay plain. Training cost
  0.14–0.68 s a run (5AB), fixed by the sample size rather than the run;
  peak memory adds the samples, the trainer's workspace and the encoding
  dictionary (not measured).
- **Ownership:** the dictionary belongs to its run: stored in it, covered
  by its checksums, its decoding form held as long as the run and charged
  to `run_bytes`; readers keep their own decoding contexts, as now.
- **Which runs:** an engine-wide setting, as for the codec and block
  length, is the simplest policy and needs no adaptive selection; sensor
  shows the setting can lose, so it would be the application's choice.
- **Coexistence:** as with codecs and block lengths, the frame's codec
  byte would say how to decode it, so dictionary and plain runs could sit
  in one engine and one checkpoint, and compaction would rewrite in the
  current setting. This milestone's mixed-length test is the pattern for
  testing it.

## Dictionary-compressed runs (5AD, design and prototype)

Whether a Zstandard dictionary per run lets 4 KiB blocks keep the density
of today's 16 KiB blocks, and what it would take in Tessera's format,
compaction and recovery. **Nothing here changes the engine or the run
format.** The prototype is the research `Blocks` layout from 5AB, which
uses the production block format behind the same kind of sparse index, with
each frame checksummed and verified on decode. It is extended in
`src/run/tests/layouts.rs`:

```text
cargo test --release --lib run::tests::layouts -- --ignored --nocapture
```

`decode_costs` breaks down a single block read, and `dictionary_runs`
builds and reads the candidates. Both ran twice on the bounded container
(2 CPUs, 3 GiB, tmpfs). Sizes matched between the passes. Timings in pass
2 were 3–10% higher overall, and every ordering below held in both;
pass 1 is shown. The datasets are the 5AB ones: sensor (small records,
128 groups), JSON (about 512 B values, 16 groups) and documents (about
2 KiB values, 4 groups).

The labels **M**, **D** and **A** mark figures as measured, derived from
measurements, or assumed but not verified.

### Where a block read goes

Per 4 KiB block, the median of five rounds over the whole run (M):

| cost (ns) | sensor | JSON | documents |
|---|---|---|---|
| CRC-32 | 120 | 44 | 32 |
| header parse | 4 | 5 | 4 |
| Zstd decompress (reused context) | 4,427 | 4,869 | 4,730 |
| the same with a fresh context | 4,456 | 4,913 | 4,697 |
| copy decoded block | 68 | 67 | 63 |
| block open | 2 | 2 | 2 |
| in-block lookup | 161 | 142 | 53 |
| production `Run::get`, end to end | 4,880 | 5,208 | 4,868 |
| LZ4 decompress | 829 | 1,188 | 992 |

- **Decompression is nearly all of it (M).** `Run::get` costs decompression
  plus 0.2–0.5 µs. CRC, header, open and copy add up to 0.1–0.2 µs.
- **The per-frame cost is Zstandard's own (D).** Across 1–16 KiB blocks,
  decompression fits roughly 1.2–2 µs per frame plus 0.63–0.78 µs per
  decoded KB. Starting from a fresh decompression context instead of a
  reused one adds 0–300 ns, and a context is 96 KB. The fixed part is the
  entropy-table setup each frame carries. Tessera does no avoidable setup:
  the workspace already reuses its context and buffer. A decoder cache or
  pool would save at most 0.3 µs, so none is proposed.
- **A prepared dictionary removes most of the fixed cost (M).** With a
  64 KiB dictionary, a 4 KiB JSON block decodes in 2.50 µs and a document
  block in 2.54 µs, against 4.87 and 4.73 µs plain. The frames' tables come
  from the dictionary. On sensor it only reaches 3.87 µs, against 4.43 µs.
- **Only a prepared dictionary is usable (M).** Passing the raw dictionary
  bytes per call costs 6.1–7.3 µs a block, slower than no dictionary,
  because Zstandard prepares it on every frame. Preparing a decoding
  dictionary (`DDict`) takes 4.2–4.4 µs, or 3.6–3.7 µs by reference. It is
  a per-run cost, paid once.
- **Compression (M).** A 4 KiB block compressed with a prepared encoding
  dictionary (`CDict`) takes 8.8 µs on JSON instead of 12.7 µs, and 6.9 µs
  on documents instead of 9.6 µs, but 23.6 µs on sensor instead of 18.0 µs.
  A `CDict` at level 3 is 474 KB.

### Prototype results

Each row encodes the same records. "vs 16 KiB" compares total memory,
which counts payload, keys, index, framing, Bloom filter, the raw
dictionary and the dictionary's decoding form, against production Zstd at
16 KiB. Reads, at 64 MiB of input only:

- p50 and p99 cover present keys;
- seeks;
- 32- and 512-key scans from a seek;
- a full scan.

All of these are M.

| dataset | layout | total MiB | vs 16 KiB | p50 µs | p99 µs | seek µs | 32 keys µs | 512 keys µs | full scan ns/rec |
|---|---|---|---|---|---|---|---|---|---|
| sensor | Zstd 16 KiB | 32.52 | – | 14.96 | 19.21 | 14.90 | 16.4 | 37.9 | 46 |
| sensor | Zstd 4 KiB | 33.68 | +3.6% | 5.17 | 6.04 | 5.08 | 6.8 | 32.6 | 52 |
| sensor | LZ4 4 KiB | 39.71 | +22.1% | 1.33 | 1.75 | 1.31 | 2.1 | 14.2 | 26 |
| sensor | 4 KiB + 64 KiB dict | 34.53 | +6.2% | 4.38 | 5.42 | 4.48 | 6.0 | 28.0 | 47 |
| sensor | 4 KiB + 16 KiB dict | 34.23 | +5.2% | 3.92 | 4.79 | 3.84 | 5.2 | 25.4 | 43 |
| JSON | Zstd 16 KiB | 16.78 | – | 13.29 | 17.17 | 13.31 | 26.9 | 231.4 | 428 |
| JSON | Zstd 4 KiB | 20.72 | +23.5% | 5.38 | 6.38 | 5.45 | 26.5 | 338.1 | 648 |
| JSON | LZ4 4 KiB | 31.77 | +89.4% | 1.58 | 1.96 | 1.58 | 7.1 | 89.0 | 173 |
| JSON | 4 KiB + 64 KiB dict | 15.26 | −9.0% | 2.83 | 3.38 | 2.82 | 13.7 | 174.8 | 342 |
| JSON | 4 KiB + 16 KiB dict | 15.17 | −9.6% | 3.00 | 3.50 | 2.98 | 14.6 | 188.4 | 362 |
| documents | Zstd 16 KiB | 11.99 | – | 12.08 | 15.83 | 12.07 | 54.4 | 686.6 | 1,321 |
| documents | Zstd 4 KiB | 14.73 | +22.8% | 4.83 | 5.88 | 4.94 | 69.9 | 1,038.7 | 2,016 |
| documents | LZ4 4 KiB | 24.29 | +102.6% | 1.33 | 1.83 | 1.33 | 16.4 | 241.2 | 472 |
| documents | 4 KiB + 64 KiB dict | 12.19 | +1.7% | 2.79 | 3.67 | 2.86 | 39.4 | 583.0 | 1,138 |
| documents | 4 KiB + 16 KiB dict | 12.08 | +0.8% | 2.92 | 3.88 | 2.96 | 41.2 | 612.5 | 1,198 |

Size against input (M; "vs 16 KiB" for each dictionary size):

| dataset | input MiB | Zstd 4 KiB | 64 KiB dict | 16 KiB dict |
|---|---|---|---|---|
| sensor | 4 | +3.6% | +11.0% | +6.9% |
| sensor | 16 | +3.6% | +7.2% | +5.6% |
| JSON | 4 | +23.3% | +2.6% | −5.6% |
| JSON | 16 | +23.4% | −6.7% | −8.6% |
| documents | 4 | +23.0% | +19.2% | +7.4% |
| documents | 16 | +22.8% | +4.9% | +1.9% |

- **On structured data, dictionaries beat 16 KiB blocks in both
  dimensions (M).** JSON at 4 KiB with a dictionary is 9–10% smaller than
  today's default, with present-key lookups 4.4–4.7× faster and p99 5×
  lower. It scans faster too: 32 keys 1.8–2.0×, 512 keys 1.2–1.3×, a full
  scan 1.2×. Documents match 16 KiB density within 0.8–1.7%, with lookups
  4.1–4.3× faster and scans 1.1–1.4× faster. Against plain 4 KiB blocks,
  the dictionary saves 26% on JSON and 18% on documents, and halves lookup
  time.
- **Scans gain because frames get cheaper (D).** The frames keep the same
  number of records but decode in about half the time, so the 5AC scan
  regression at 4 KiB disappears on JSON and documents.
- **On sensor, a dictionary is the wrong tool (M).** Records are small and
  repetitive inside a block, so plain 4 KiB blocks already cost only 3.6%.
  A dictionary saves under 2% of payload and adds its own size. Lookups get
  1.2–1.3× faster than plain 4 KiB, but sensor is 1.6–2.6 points larger.
- **Prefer a 16 KiB dictionary (M).** At 64 MiB it costs up to 6% in lookup
  time against 64 KiB (sensor is faster) and is the same size or smaller. Below that it is
  much smaller: JSON −5.6% against +2.6% at 4 MiB, documents +7.4% against
  +19.2%. It trains 3.5–4.7× faster on a quarter of the samples (below).
- **LZ4 at 4 KiB is a different point (M):** 2–3.5× faster than any Zstd
  layout, at 22–103% more memory. Dictionaries do not compete with it on
  latency.

### Training

Sampling, training and building per run, at 64 MiB of input (M):

| dataset | dictionary | samples MiB | sample ms | train s | train peak MiB | build s | build MB/s | estimated saving (actual) |
|---|---|---|---|---|---|---|---|---|
| sensor | Zstd 16 KiB, none | – | – | – | – | 0.23 | 287 | – |
| sensor | 64 KiB, inputs | 7.1 | 8 | 0.28 | 0 | 0.30 | 220 | 1% (2%) |
| sensor | 16 KiB, inputs | 1.6 | 2 | 0.06 | 0 | 0.30 | 221 | 0% (2%) |
| JSON | Zstd 16 KiB, none | – | – | – | – | 0.15 | 440 | – |
| JSON | Zstd 4 KiB, none | – | – | – | – | 0.22 | 312 | – |
| JSON | 64 KiB, inputs | 6.9 | 6 | 0.20 | 13 | 0.16 | 422 | 28% (28%) |
| JSON | 16 KiB, inputs | 1.6 | 2 | 0.05 | 0 | 0.14 | 465 | 28% (28%) |
| JSON | 64 KiB, output blocks | 6.2 | 4 | 0.20 | 4 | 0.15 | 436 | 29% (28%) |
| JSON | 64 KiB, values | 6.8 | 2 | 0.22 | 1 | 0.15 | 440 | 56% (25%) |
| documents | Zstd 16 KiB, none | – | – | – | – | 0.11 | 587 | – |
| documents | Zstd 4 KiB, none | – | – | – | – | 0.16 | 412 | – |
| documents | 64 KiB, inputs | 6.4 | 5 | 0.14 | 0 | 0.12 | 546 | 19% (18%) |
| documents | 16 KiB, inputs | 1.6 | 1 | 0.04 | 0 | 0.11 | 594 | 19% (19%) |

"Estimated saving" is the held-out estimate described below. "Actual" is
the measured payload saving against plain Zstd 4 KiB on the same records.
"Train peak" is resident memory added while training: a lower bound,
because freed heap is reused first.

- **Samples come from the inputs, already in blocks (M).** `input_samples`
  takes whole decoded blocks evenly from the input runs, up to a byte
  budget, which is 100× the dictionary size. It reads each frame's decoded
  length from its header and decodes only the frames it takes, so sampling
  costs 1–8 ms. Nothing beyond the samples is materialized.
- **Input blocks train as well as output blocks (M).** JSON: 15.26 against
  15.15 MiB; documents: 12.19 against 12.10. Values alone train worse
  (JSON 15.76 MiB), and estimate their own saving badly (56% predicted,
  25% actual): a dictionary must be trained on what it will compress.
- **A held-out estimate predicts the saving (M).** One block in eight of
  the samples is held out. Compressing the held-out blocks with and
  without the trained dictionary predicted the actual saving within
  1–3 points for every dataset and size, at every input size measured
  (4, 16 and 64 MiB). It costs one extra compression of the held-out
  eighth, about 0.2–0.9 MiB.
- **Training cost is fixed by the sample budget, not the run (M).** For a
  16 KiB dictionary it is 0.04–0.07 s on 1.6–2.7 MiB of samples. For
  64 KiB it is 0.09–0.45 s. At the default 64 MiB run target, a 16 KiB
  dictionary adds about a third of the build time (D: 0.05 s against
  0.14 s). The build itself is as fast as at 16 KiB, because compressing
  with a dictionary is cheaper per block, except on sensor.
- **Building and training peak memory (M, lower bounds):** 0–20 MiB above
  the build. The samples (1.6 MiB for a 16 KiB dictionary) and a 474 KB
  `CDict` are held during the merge (D).
- **A size threshold is not needed for a 16 KiB dictionary (M).** The
  15 MiB hypothesis came from the 64 KiB dictionary's fixed cost of about
  150 KiB a run. With 16 KiB, both JSON and documents gain over plain 4 KiB
  at every size measured, down to 4 MiB of input. The fixed cost is about
  16 KiB raw plus a decoding form estimated at about 43 KiB (D). A floor,
  such as 1 MiB of input, only avoids training on trivial runs (A).
- **The fallback is a threshold on the held-out saving (D).** If the
  estimated saving against plain Zstd at the same block length is below
  10%, the run is built plain. That separates the datasets with a wide
  margin: sensor shows 0–1%, documents 19%, JSON 28%. There is no
  adaptive policy: one check per output run, with the same result for the
  same inputs.

### Ownership and memory

- **Serialized:** the raw dictionary bytes as Zstandard's trainer emits
  them, including its magic and ID, at most a fixed bound inside the run
  (format below). No other dictionary state is persisted.
- **Prepared:** one `DDict` per run, built when the run is constructed or
  recovered, after validation, and held next to the Bloom filter in the
  `Run`. It is boxed, so a plain run pays one pointer. `DDict` is
  `Send + Sync` and is only read while decoding, so every reader of the
  run shares it through `&Run`. Each reader keeps its own decompression
  context in its `ReadWorkspace`, as now, and needs no pool.
- **By copy, not by reference (M).** A `DDict` that copies its dictionary
  is 90.7 KiB for 64 KiB of dictionary, against 26.7 KiB by reference. A
  by-reference `DDict` would borrow from the run's own bytes, which makes
  the run self-referential. That cannot be written under the crate's
  `#![forbid(unsafe_code)]`, so the copy (about the dictionary's size
  again) is the price. With a 16 KiB dictionary that is about 16 KiB a run
  (D).
- **Charged:** the decoding form is counted in the run's resident bytes,
  with the filter, so `run_bytes`, retained-snapshot accounting and the
  memory limits see it. The prototype's totals already include it.
- **Lifetime:** the dictionary lives exactly as long as its run. Snapshots
  pin runs, so a snapshot keeps the dictionaries of the runs it reads.
  When compaction replaces a run and the last pin drops, its dictionary is
  freed with it. There is no registry, no sharing across runs and no
  reference tracking. A dictionary is never needed beyond its own run's
  frames.
- **Encoding side:** the `CDict` (474 KB at level 3) and the samples exist
  only while one compaction builds one output run, in the merge's
  workspace. They are not kept.

### Compaction

- **Flushes stay plain.** A flush has no input runs to sample, and its
  output is replaced by the next compaction anyway.
- **One pass from the inputs (recommended).** Before merging:
  1. sample decoded blocks from the selected input runs;
  2. train;
  3. evaluate on the held-out eighth;
  4. then merge once, building every output run with that dictionary or
     plain, depending on the threshold.

  The sampling decodes about 1.6 MiB and takes milliseconds.
- **Two passes from the output (not recommended).** Build the output plain,
  sample its blocks and rebuild it with the dictionary. It trains no better
  (M: within 0.7%), doubles the compression work, and holds a full plain
  copy of the output during the rebuild, which the output limit would have
  to count.
- **Where it runs.** The selected runs are already pinned, so training runs
  on the compaction worker outside the state lock, like the merge.
  Publication is the existing atomic replacement of inputs by outputs. A
  training failure, or any error, leaves the inputs and every snapshot
  untouched, exactly as a merge failure does now. A training error can
  instead fall back to building plain (D).
- **Inside the existing limits.** The workspace estimate would add the
  sample budget, the trainer's workspace (about the samples' size; the
  prototype's lower bound is 0–20 MiB), the `CDict` and the held-out
  compression. Exceeding `max_workspace_bytes` would build plain rather
  than fail. The output estimate would add one dictionary and decoding
  form per output run. No new worker, scheduler or selection change is
  needed.
- **One dictionary per compaction, one copy per output run (D).** All
  outputs of one merge come from the same inputs, so they can share the
  trained bytes. Each output run still stores and prepares its own copy,
  which keeps every run self-contained.

### Format sketch (not implemented)

A run with a dictionary would be version 4. Plain runs stay version 3,
byte for byte. A build that knows only version 3 rejects version 4 with
`UnsupportedVersion`, a safe failure.

```text
run       := frame* dictionary table key_area footer
dictionary := dict_bytes                      dict_len bytes, before the table

footer v4 := index_offset:u64 record_count:u64 block_count:u32
             min_key_len:u32 dict_len:u32 dict_checksum:u32
             version:u32 magic:u32            40 bytes

codec 3   := a Zstandard frame that declares its content size and was
             compressed with this run's dictionary
```

- **Version detection:** the version stays the last field but one, so a
  reader learns the footer length before parsing anything else.
- **Location and length:** the dictionary ends at `index_offset` and starts
  at `index_offset - dict_len`. The last frame's stored length is implied
  by the dictionary's start instead of `index_offset`. `dict_len` is at
  most `MAX_DICTIONARY_LEN` (64 KiB; 16 KiB is the recommended size), so
  recovery allocates at most that plus its decoding form before anything
  is trusted. A version 4 run with `dict_len` 0 is invalid: it would be
  version 3.
- **Explicit codec:** codec 3 is its own value. A frame says whether it
  needs the dictionary, and the reader never infers it from the run's
  version or from a dictionary being present. Codecs 0–2 stay valid in a
  version 4 run, so a run's frames may mix codec 2 and 3, as codecs 0–2
  may be mixed now.
- **Per-block reference:** Zstandard writes the dictionary's 32-bit ID into
  every frame. The reader checks that the frame's ID equals the run's
  dictionary ID before decoding, as a second check against frames copied
  between runs.
- **Checksums:** `dict_checksum` is the CRC-32 of the dictionary bytes.
  Frame checksums are unchanged. The footer is not covered now and stays
  uncovered; its fields are cross-checked against the regions, as now.
- **Errors:** new `RunCorruption` kinds:
  - `InvalidDictionary`: bad length, checksum, magic or ID, or Zstandard
    refuses to prepare it;
  - `MissingDictionary`: codec 3 in a version 3 run;
  - `DictionaryMismatch`: the frame names another ID.

  A codec 3 frame that fails to decode is `CorruptPayload`, as now.
- **Compatibility and mixed runs:** version 3 and 4 runs coexist in one
  engine and one checkpoint, as runs of mixed block lengths do since 5AC.
  Compaction rewrites runs in whatever the current setting is. There is
  no migration: old runs remain version 3 until they are compacted.

### Recovery, reads and corruption

- **`Run::new` and recovery:** validation reads the footer, checks
  `dict_len` against its bound, verifies `dict_checksum`, checks the
  dictionary's magic and nonzero ID, and prepares the `DDict`. Only then
  does it validate the frames as now, decoding every one with that
  dictionary and checking each frame's ID. The `Run` is constructed only
  after all of this succeeds, so no reader can see a run whose dictionary
  has not been validated. Recovery memory is one decoded block, as now,
  plus the dictionary's decoding form.
- **Checkpoints and manifests:** a checkpoint stores whole runs, so the
  dictionary travels inside its run. The manifest needs no new field.
  Loading a checkpoint validates every run in full, which includes the
  above.
- **WAL replay:** unchanged. It refills the memtable, and flushes stay
  plain.
- **Pruning:** key ranges, Bloom filters and `max_seq` are derived and
  stored exactly as now. Sequence-bound pruning and sequence semantics do
  not change.
- **Snapshots:** they pin runs, and so their dictionaries. A view never
  changes.
- **Concurrent reads:** readers share the run's `DDict` by shared
  reference; nothing in the read path is mutable beyond each reader's own
  workspace.
- **Corruption while reading:** a frame's CRC is verified before decoding,
  as now. Damage to the dictionary after validation, which takes memory
  corruption, makes frames fail to decode or fail their own checks. That
  is reported as `CorruptPayload`, without a panic, through the same
  checked path.

### Correctness checks added

- `layouts_agree_with_the_production_run` now also checks a dictionary
  trained by `input_samples` from three runs with 1 KiB blocks, read
  through 256 B dictionary blocks, against the production run: gets,
  seeks, scans and versions.
- `dictionary_frames_name_their_dictionary_and_need_it`: dictionary frames
  carry the dictionary's ID. Decoding one without the dictionary, or with
  another, is an error, never wrong bytes. The right `DDict` decodes it.
- The engine is unchanged. The full bounded suite passes (below).

### Decision

- **Integration is justified, for structured values, as an explicit
  option (D).** On JSON and documents, a 16 KiB dictionary at 4 KiB blocks
  matches or beats today's density while making lookups 4–4.5× faster and
  scans faster. Plain 4 KiB blocks cannot do that: 5AC measured +24–27%
  memory. The cost is bounded and per run: a format version with one new
  codec and one region; training of about 0.05 s per compaction output; a
  few MiB of workspace; and about 60 KiB resident per run (D).
- **It is not a general default (M).** On small repetitive records
  (sensor), plain 4 KiB blocks are already dense. The held-out estimate
  (at most 1%) is below the 10% threshold, so a run would be built plain;
  the prototype measured the estimate but did not implement the fallback. Whether a workload gains depends on how much
  structure its values repeat across blocks.
- **Not shown here (A).** The prototype is the run layer. In 5AC, run-layer
  and engine lookups at 4 KiB agreed within a few percent, but these runs
  have not been read through the engine, its run sets or its memory
  accounting. One dictionary per compaction assumes that the inputs'
  value structure is stable within the inputs. Data whose structure
  drifts quickly was not tested. Zstandard version, hardware and other
  datasets may move the 10% threshold.

## Dictionary-compressed runs through the engine (5AE)

*Engine measurements, and a format change.* `Compression::ZstdDictionary`
(opt-in; the default is unchanged) makes compactions and checkpoints
compress the runs they build with a 16 KiB Zstandard dictionary trained
for the rewrite, when a held-out sample shows it saving at least 10%.
Flushes stay plain. The design is the 5AD one, with the changes below.
Labels: **M** measured, **D** derived, **A** assumed.

### What was built

- **Option:** one `Compression` variant. It does not change
  `target_block_len` (set it to 4 KiB, as measured); a dictionary works at
  any block length, and the 10% check is against plain Zstandard at the
  same length.
- **Format:** runs without a dictionary are still version 3, byte for byte.
  A dictionary run is version 4: codec 3 frames, the dictionary between the
  frames and the table, and a 40-byte footer with its length (1 B to
  64 KiB) and CRC-32. The last 8 bytes (version, magic) select the footer.
  Validation checks the length bound from the footer, then the checksum,
  the magic and ID, and prepares the dictionary, before any frame; each
  codec 3 frame must then name its ID. New `RunCorruption` kinds:
  `InvalidDictionary`, `MissingDictionary`, `DictionaryMismatch`.
- **Training** (`src/run/dictionary.rs`): from frame headers, rewrites
  under 6.4 MiB decoded are not sampled. Otherwise whole decoded blocks are
  taken evenly across the inputs up to 1.6 MiB; one in eight is held out,
  the rest train once (`ZDICT_trainFromBuffer`). The held-out blocks, cut
  to the block length, are compressed with and without it. All of this
  happens after the workspace check and before the merge, on the
  compaction's own thread outside the state lock, and is never retried.
- **Fallbacks:** too small, no workspace room, training failure, or under
  10% saving all build plain output; `CompactStats::dictionary` says which.
  A workspace limit without room for training builds plain rather than
  failing. The output limit is unchanged: a compaction that exceeds it
  fails and publishes nothing, as before.
- **Checkpoints also train**, from the snapshot's runs (not its memtables).
  A checkpoint rebuilds every run from a scan, so without this a restart
  would recover plain runs that, being disjoint, compaction might not
  rewrite for a long time.
- **Ownership:** the runs one rewrite builds each store the dictionary
  bytes and share one prepared decoding dictionary (`Arc`), freed with the
  last of them; a recovered run prepares its own. Readers share it; each
  keeps its own decoding context. No registry or pool.
- **Accounting:** the prepared dictionary counts with the key filter
  (`Run::held_bytes`, reported in the `held_bytes` fields of the engine's
  statistics and reports, formerly `filter_bytes`, and limited by
  `max_run_bytes`); runs sharing one each count it in full, an
  overstatement. Training adds `DICTIONARY_WORKSPACE` (21.2 MiB: two
  1.6 MiB sample buffers, a 16 MiB trainer allowance, two codec contexts)
  to the workspace estimate. The trainer's allowance is derived from the
  zstd 1.5.7 source (about 12 MiB of tables); it cannot be measured
  through the safe API (A).

### Method

The 5Y harness's `tessera` engine on `json:64` (1.05M records, about 545 B
values, 518 MiB logical) and `sensor:256` (4.19M records), with background
compaction, consolidated. New harness engines: `tessera-dict`, and
`tessera-compactions-dict` for compaction cost. Two passes, the second in
reverse order; ranges are the passes. Pass 1 ran before the prepared
dictionary was shared between a rewrite's runs; harness lookups were the
same within 1% after the change. Every full scan was verified against the
model. Documents are not in the harness and were not measured through the
engine. Run-layer and direct engine checks: the ignored tests
`lookup_costs` and `engine_lookup_costs` in `src/run/tests/dictionary.rs`
(release, 48–64 groups of the same dataset).

### Results (M)

Consolidated state, harness:

| dataset | layout | runs MiB | beside MiB | resident MiB | present p50/p99 µs | overwritten p50 µs | first record µs | 30 keys µs | 480 keys µs | 1,920 keys µs | full scan M rec/s |
|---|---|---|---|---|---|---|---|---|---|---|---|
| JSON | Zstd 16 KiB | 138.2 | 1.17 | 159.5 | 13.75–13.87 / 17.8 | 13.75–13.91 | 13.7–13.9 | 27.7–28.0 | 237–239 | 912–919 | 2.11–2.13 |
| JSON | Zstd 4 KiB | 171.2 | 1.17 | 197.0 | 5.88–5.92 / 7.0 | 5.92 | 5.79 | 26.9–27.3 | 345 | 1,359–1,365 | 1.41–1.42 |
| JSON | 4 KiB + dictionary | 124.4 | 1.59 | 129.5–137.0 | 5.21–5.25 / 7.1–7.4 | 5.12–5.21 | 4.83–5.29 | 20.7–22.3 | 258–289 | 1,047–1,159 | 1.74–1.93 |
| sensor | Zstd 16 KiB | 87.1 | 4.69 | 115.1 | 14.58–14.80 / 19.1–19.6 | 14.59–14.62 | 14.6–15.0 | 16.2–16.3 | 48 | 148 | 14.2 |
| sensor | Zstd 4 KiB | 90.4 | 4.69 | 118.1 | 5.25 / 6.3–6.5 | 5.25 | 5.3–5.5 | 6.8–6.9 | 43 | 151–152 | 12.6–12.8 |
| sensor | 4 KiB + dictionary | 90.4 | 4.69 | 118.5 | 5.25 / 6.4–9.3 | 5.25 | 5.3–5.4 | 6.8 | 43 | 151–153 | 12.8 |

"Beside" is key filters plus prepared dictionaries; "resident" is trimmed
anonymous memory. Peak anonymous memory over the whole run: JSON 287.6–295.9
MiB (16 KiB), 336.3 (4 KiB), 302.3–314.1 (dictionary); sensor 253–256 for all.

- **Density holds through the engine.** JSON runs with dictionaries are
  10.0% smaller than 16 KiB and 27.3% smaller than 4 KiB plain; resident
  memory 14–19% below 16 KiB. The 10 runs hold 0.42 MiB of prepared
  dictionaries, 43 KiB each, as the docs state.
- **The sensor fallback works.** Every sensor compaction reported
  `NotBeneficial`; sizes are identical to plain 4 KiB and reads equal
  within noise. Nothing was lost by enabling the option.
- **Scans improve over plain 4 KiB** (short 1.2–1.3×, medium 1.2–1.3×,
  full scan 1.2–1.4×) but remain slower than 16 KiB beyond about 30 keys
  (medium 1.1–1.2×, long 1.15–1.26×, full scan 1.1–1.2×).
- **Lookups: a smaller gain than the run layer measured, and not fully
  explained.** In the harness, dictionary lookups took 5.21–5.25 µs against
  5.88–5.92 µs plain at 4 KiB (1.13×) and 13.8 µs at 16 KiB (2.6×). Measured
  directly, on the same dataset and the production paths:

  | path (64 groups unless noted) | plain 4 KiB p50 µs | dictionary p50 µs |
  |---|---|---|
  | one run (48 groups) | 5.83 | 3.58 |
  | 7 runs of one compaction, one or per-run workspaces (48 groups) | 5.88–5.92 | 3.63 |
  | engine, shuffled load, one consolidating compaction (48 groups) | 5.75 | 3.58 |
  | engine, shuffled load with background compaction (48 groups) | 5.83 | 3.92 |
  | engine, the same at 64 groups (10 runs, 5 compactions) | 6.04 | 4.04 |

  So through `Engine::get` the gain is 1.5× over plain 4 KiB (M). Two causes
  were found and one fixed: runs built by one compaction now share one
  prepared dictionary (9 runs with separate copies: 3.92 µs; shared:
  3.33 µs, at 8 groups); runs from different compactions keep separate
  dictionaries, which costs about 0.35–0.45 µs a lookup, consistent with
  each dictionary's decoding tables having to be in cache (D). The
  harness's remaining 1.2 µs is not explained: per-run workspaces, the
  memory sampler thread and a second libzstd (the binary resolves Zstandard
  statically) were ruled out (M); an interaction with the harness's
  preceding phases is untested (A).
- **No lookup allocates (D).** The prepared dictionary is built once per
  run; a lookup uses the reader's existing context and buffer, and the
  dictionary check reads the frame header. Not separately measured.

Compaction cost, `json:64` loaded shuffled into 11 plain runs, then 12
two-input compactions:

| layout | stored input MiB | output MiB | wall s | stored MiB/s | training s | estimate MiB | peak heap above, max MiB | peak resident above, max MiB |
|---|---|---|---|---|---|---|---|---|
| Zstd 16 KiB | 331.4 | 331.3 | 4.13 | 80.2 | – | 3.1 | 50.5 | 47.3 |
| Zstd 4 KiB | 387.6 | 387.2 | 5.62 | 69.0 | – | 3.0 | 54.4 | 55.1 |
| 4 KiB + dictionary | 296.0 | 269.3 | 4.40 | 67.3 | 0.552 | 24.1 | 34.4 | 29.5 |

- **Training is 0.045–0.049 s per compaction** (12% of these compactions'
  time) and saves 28–29% (held-out) every time. Per stored byte the
  compaction runs at the speed of plain 4 KiB (M).
- **No unaccounted transient (M):** peaks above the starting heap, which
  include the growing output, stayed below plain 4 KiB's; the 24.1 MiB
  estimate (3 MiB merge + 21.1 MiB training) covers the training phase
  (A: the trainer's own peak is not isolated).

### Correctness

- `src/run/tests/dictionary.rs`: round trips (gets, tombstones, absent
  keys, prefix scans, full walks) through built, validated and recovered
  runs; independent, ID-carrying frames; deterministic encoding; damaged,
  garbage and unpreparable dictionaries; out-of-bound and incomplete
  dictionary regions refused from the footer; short and unknown-version
  footers; missing and mismatched dictionaries; a detached view; compaction
  using a dictionary (same records as plain, smaller output, dictionary
  inputs re-sampled); and every fallback (sensor, small input, workspace).
- `tests/checkpoint.rs`: dictionary compaction through the engine, a
  refused compaction publishing nothing, a snapshot reading dictionary runs
  a later compaction replaced, a checkpoint writing version 4 runs, and
  recovery under all four settings beside newer plain runs (overwrites,
  deletes, keys only in the older run), then compaction in the current
  setting; and the sensor fallback through the engine.
- The randomized engine model test draws `ZstdDictionary` too (its inputs
  are below the training floor, so it checks the plumbing).
- Format tests: codec 3 in a version 3 run is `MissingDictionary`; codec 4
  is unsupported; version 5 is unsupported.

### Decision

- **Retain, as an opt-in for structured values (D).** Through the engine it
  is the best density measured for JSON-like values (10% below the 16 KiB
  default, 27% below plain 4 KiB) with lookups 2.6–3.4× faster than the
  default and 1.13–1.5× faster than plain 4 KiB, scans better than plain
  4 KiB, and compaction at the same speed per stored byte. It declines on
  sensor data at no cost.
- **Weaker than 5AD predicted on lookups.** The run layer's 1.6–1.9× over
  plain 4 KiB shrinks to 1.5× through the engine and 1.13× in the harness.
  The density case stands on its own; the latency case is modest and its
  harness gap is open.
- **Limitations:** runs no compaction merges stay plain until a checkpoint;
  each compaction's runs have their own dictionary, which costs lookups a
  little as their number grows; documents were not measured through the
  engine.

## Historical results

Everything below was measured with the former suite, `examples/block_bench.rs`
and its modules (removed in 5S; last present at commit `8d3d244`), through
`scripts/validate.sh` modes that no longer exist. Results describe the
engine as it was at each milestone and have not been re-run. Several
measured components directly (block geometry and codecs, merge and logical
cursors, memtable trees), which are now internal.

Each mode was run as `scripts/validate.sh MODE`, usually over three passes
summarized with `scripts/summarize_bench.py`, which then also fitted the
Zstandard decode model in the `geometry` section. The run defaults, 16 KiB
blocks and Zstandard level 3, follow from those findings.

### `geometry`: block size and codec

Compares run block targets of 4 to 128 KiB (encoded, uncompressed) for no
compression, LZ4 and Zstandard level 3, over 32 MiB of logical data in each of
five datasets: JSON-like, small-value, structured keys, version-heavy and
incompressible values. Each metric is the median of 9 rounds, with 2000 seeded
uniform lookups per round, the same for every configuration of a dataset.

What the columns mean:

- `get` is a full lookup. There is no block cache, so every lookup verifies and
  decompresses its candidate block. Percentiles pool per-operation samples,
  which resolve only as finely as the clock (about 42 ns on the reference VM).
- `locate`, `checksum`, `decompress` and `search` time each step in its own
  loop over the same probes. They need not sum to `get`, because each loop pays
  its own cold memory reads and `get` pays them once.
- `validate` is `Run::new`, which decodes every block.
- `build peak` counts live Rust heap bytes and treats a reallocation as old
  plus new. It excludes the input records and the codec contexts' C heap.

Later tables time the codecs on real blocks and on controlled 64 KiB blocks that
vary one property at a time, and show what each Zstandard block contains.

#### Findings

Reference environment: 2-CPU, 4 GiB aarch64 Colima VM, 3 GiB container, three
passes. `get` differs between passes by a median of 3%, p99 by 14% and p99.9 by
25%, so p99.9 is mostly environment noise.

- Zstandard decode time follows block content, not decoded or stored size: about
  1.7 us per frame, 0.5 ns per entropy-coded literal and 6.6 ns per sequence.
  The controlled blocks show the same shape, and the model overpredicts them by
  20 to 30% (about 2.2 times for 4-bit symbols).
- From 64 to 16 KiB, Zstandard point lookups get 2.2 to 3.6 times faster for at
  most 10% more memory. Below 16 KiB, memory rises faster than latency falls.
- These runs are small enough to sit largely in CPU caches, and lookups are
  uniform and single-threaded, so effects at larger scale are untested.

### `prefix`: seeks and prefix scans

Compares 8, 16, 32 and 64 KiB targets for the same three codecs over three
datasets of hierarchical keys (`t0000:g00:s00:d000`), at 32 MiB of logical
data each: JSON values, 8-byte values, and near-duplicate versions with long chains.
Key levels give prefixes of about 0, 1 to 5, 20, 200 and 4000 records. Each round
runs 400 seeded prefixes per scale, the same across configurations, plus
prefixes placed at block edges (those depend on the block target). Run it with
`scripts/validate.sh prefix` and summarize three passes as above.

Every query uses a new cursor, so it decodes its own blocks and reuses none from
an earlier query. Results are checked against the input records.

- `locate`, `first decode` and `block seek` time index navigation, checksum and
  decompression of the first block, and the search inside it, each alone.
  `seek` is the whole `seek_prefix`, and `scan` adds consuming every record.
- `blocks` counts blocks loaded per query. A prefix whose last match ends a block
  loads the next one to end, because the index holds only each block's last key.
- `block end` is that worst case, and `block crossing` a prefix spanning a block
  boundary.

#### Findings

Same reference environment, three passes. Per-query scan time differs between
passes by a median of 3%, and 6 of 264 rows differ by more than 25%, mostly
transient slowdowns in one pass.

- A short prefix costs about one block decode, so smaller blocks win clearly:
  Zstandard takes 6 to 11 us per query at 8 KiB and 30 to 56 us at 64 KiB.
- Blocks larger than 16 KiB start to help only for scans of thousands of
  records, and only slightly: Zstandard at 64 KiB is at most 18% faster than at
  8 KiB and within 8% of 16 KiB on JSON. Small-value data does not benefit.
- LZ4 cuts long-scan time by about 40 to 47% relative to Zstandard, at 24 to
  80% more memory. Zstandard stays much smaller at every size.
- Sparse-index overhead stays below 3% even at 8 KiB and halves with each
  doubling of block size.

### Merged iteration

`block_bench merge` splits 32 MiB of logical data across 1, 2, 4, 8 and 16 runs
of Zstandard at 16 KiB, and at fan-in 8 also compares other codecs, block sizes
and mixtures. Run it with `scripts/validate.sh merge` and summarize three passes
as above.

- `overlap` deals records to inputs in turn, so neighbors come from different
  runs and version chains are split: the worst case for the merge. `disjoint`
  gives each input a contiguous range.
- `bare` iterates each input alone, so `ratio` isolates the merge from the cost
  of decoding the same runs. Each configuration splits the same records, and a
  checked pass requires the merged stream to equal them exactly.
- Cursors are reused and repositioned. `allocs` are totals for a warm workspace.
- `decoded` is block buffers retained, and `state` is everything else the merge
  retains on the Rust heap. Each Zstandard input also keeps one 94 KiB context
  on the C heap, outside both figures.

#### Findings

Same reference environment, three passes. Full-scan time differs between passes
by a median of 0.7% and at most 4.6%. Seek and scan medians agree within about
1%; single rows reach 40%, and p99 latency varies by up to 280%, so tails here
are not reliable.

- The merge adds about 3 to 6 ns per record at fan-in 1 or 2 and, with overlap,
  19, 44 and 62 ns at 4, 8 and 16. Disjoint inputs add at most 9 ns, since the
  same input keeps winning. Uncompressed, the same 42 ns at fan-in 8 triples the
  cost because decoding no longer hides it.
- Warm iteration, repositioning and prefix scans allocate nothing at any fan-in.
  Creating a cursor allocates once.
- Retained buffers are one block per input: 257 KiB decoded and 5 KiB of state
  at fan-in 16, plus 1.5 MiB of Zstandard contexts, which dominate.
- Seeks and short prefixes cost one block decode per input that can hold the
  target: on JSON with overlap, 10 us at one run, 81 us at 8 and 170 us at 16. The merge
  itself is negligible here, and `ratio` is 1.0 to 1.1.
- Disjoint inputs show the limit of the index, which records only each block's
  last key. A run entirely below the target is skipped, but a run entirely above
  it must decode a block to find its first key. So a prefix decodes about half
  the inputs (8.8 of 16) rather than one. Metadata holding each run's first key
  would remove this; the format is unchanged for now.
  Repeated plain seeks on one cursor look cheaper when disjoint (3.7 blocks at
  16) because a run above the target keeps its first block loaded between
  seeks. A prefix that ends releases its block, so it decodes again.
- Long scans are 1.4 to 1.6 times the bare cost at fan-in 8 and 16 under overlap
  and within 6% when disjoint.
- Mixed codecs and block sizes merge correctly, and each input costs what it
  would alone: the merge's own overhead stays near 44 ns at fan-in 8. Zstandard
  at 64 KiB makes a seek about 3 times more expensive than at 16 KiB (254 vs
  81 us).

### Latest-state reads

`block_bench logical` splits 32 MiB of logical data across 1, 4, 8 and 16 runs
of Zstandard at 16 KiB, plus a mix of codecs and block sizes at 8. Run it with
`scripts/validate.sh logical` and summarize three passes as above.

- `unique` has one version per key, `versions` holds version chains (3.28
  physical records per live key), and `tombstones` deletes one key in four
  (1.67).
- `disjoint` gives each input a range, `overlap` deals records in turn, and
  `partial` does half of each.
- `bare`, `merge` and `logical` time the same records alone, physically merged
  and filtered. `filter` is `logical` minus `merge`, per physical record.
- `ranged` repeats a read over only the runs whose key range could hold the
  target, using the benchmark's own knowledge of the data. It predates key
  range metadata; `ranges` below measures the real thing.
- Every result is checked against the input records.

#### Findings

Same reference environment, three passes. Full-scan times differ between passes
by a median of under 1.5% and at most 7.4%. `filter` is a difference of two
medians, so treat it as a range: it differs by a median of 12%. Tails are
unreliable, with p99 varying by up to 154%.

- Latest-state filtering costs 2 to 19 ns per physical record, so a logical
  scan takes 1.03 to 1.20 times the physical merge. Decompression and merging
  dominate it.
- At 16 runs, unique keys scan at 10.7 million live records per second when
  disjoint and 6.3 million with full overlap. Version chains cost more per
  result because older versions must be visited: 3.28 physical records per
  live key gives 2.2 to 4.4 million live keys per second.
- Warm scans, seeks, prefixes and lookups allocate nothing. Creating a cursor
  allocates once.
- A point lookup costs one block decode per run that might hold the key: 10 us
  at one run and 170 to 189 us at 16 overlapping runs. Hits, misses and deleted
  keys cost the same, since every run must be consulted.
- A prefix returning a few keys costs about the same as a lookup. The logical
  scan is within 1% of the physical one until scans are long.
- These figures were taken before runs stored a key range. A simulation of
  pruning at 16 runs predicted a disjoint lookup falling from 77 to 10 us;
  `ranges` below confirms it.
- Retained buffers grow with fan-in: 257 KiB decoded and 5 KiB of state at 16
  runs, plus the 94 KiB Zstandard context per input outside these figures.

### Key-range pruning

`block_bench ranges` splits 32 MiB of logical data across 1, 2, 4, 8 and 16 runs
of Zstandard at 16 KiB, with unique keys and with version chains, in the
`disjoint`, `partial` and `overlap` layouts above. It measures hits, misses,
short and medium prefixes and forward seeks through `RunSet`, using only API that
existed before runs stored a key range, so the same harness runs on older code.
Run it with `scripts/validate.sh ranges`.

#### Findings

Same reference environment, three alternating passes of format 2 and format 3
code. Medians differ between passes by a median of 3.5% and at most 9.8%. Warm
reads allocate nothing in any cell.

At 16 runs, with times in microseconds and blocks decoded per query:

| layout, unique keys | read | before | after | blocks before | blocks after |
| --- | --- | ---: | ---: | ---: | ---: |
| disjoint | hit | 71.9 | 10.3 | 8.43 | 1.00 |
| disjoint | miss | 66.9 | 10.3 | 7.82 | 1.00 |
| disjoint | short prefix | 70.1 | 10.5 | 8.17 | 1.00 |
| disjoint | medium prefix | 89.8 | 28.7 | 9.62 | 2.23 |
| disjoint | forward seek | 30.0 | 10.5 | 3.31 | 1.00 |
| partial | hit | 117.0 | 90.3 | 11.98 | 8.54 |
| partial | miss | 114.4 | 89.2 | 11.63 | 8.45 |
| partial | forward seek | 99.1 | 89.5 | 9.65 | 8.48 |
| overlap | hit | 169.3 | 171.5 | 16.00 | 16.00 |
| overlap | miss | 168.6 | 169.7 | 16.00 | 16.00 |
| overlap | medium prefix | 200.3 | 202.5 | 16.00 | 16.00 |

- A read of disjoint runs no longer grows with the number of runs: 10.2 to 10.3 us
  at 4, 8 and 16 runs, against 22.5, 38.5 and 71.9 us before.
- Version chains behave the same way, with a disjoint hit going from 66.5 to
  9.5 us.
- Full overlap gains nothing and costs about 1% at most. One run costs 0 to 4%.
- A run grows by its smallest key plus 4 bytes: 22 bytes here for unique keys.
  Stored bytes per record are unchanged at three decimal places.
- A full physical merge, over three alternating passes, is 0.97 to 1.02 times
  its earlier time at every fan-in except four overlapping runs, which is 1.08
  to 1.10 times slower (98.1 to 106.0 ns per record for JSON). Passes do not
  explain that, and disabling deferred admission did not remove it. Its cause
  is unknown.
- A seek in a physical merge of 16 disjoint runs falls from 33.0 to 10.3 us.
- Ranges reduce the runs consulted, not the cost of the runs that remain, so
  fully overlapping runs need fan-in control or Bloom filters.

### Memtable

`block_bench memtable` measures three datasets through `Memtable` and
`FrozenMemtable`: `json` (173,536 records, about 100 bytes each), `tiny` (428,865
records with 8-byte values) and `large` (4,084 records with 4 KiB values). Run it
with `scripts/validate.sh memtable`. Each memory row is a fresh process.
`requested` is what Rust asks the allocator for, and RSS is the growth of the
process's resident memory, so it includes allocator rounding and headers. It is
a baseline, not a target. Times include the counting allocator.

#### Findings

Reference environment, one pass of the copy-on-write B+ tree (32 entries per
node) and one of the `BTreeMap` memtable it replaced (5E), in the same session.
Earlier passes of this benchmark differed between runs by a median of 5.5% and
by up to 41%, the largest being `json` traversal, so differences under about
10% are noise. Freezing is at or below the timer's resolution, about 40 ns, and
allocates nothing.

| dataset, random order | payload MiB | charged MiB | requested MiB (5E) | RSS MiB (5E) | RSS after overwrite | RSS/charged |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| json | 17.1 | 38.2 | 29.2 (27.4) | 31.3 (32.0) | 32.7 | 0.82 |
| tiny | 10.6 | 63.0 | 41.6 (36.1) | 48.9 (52.1) | 52.2 | 0.78 |
| large | 16.0 | 16.5 | 16.3 (16.3) | 16.4 (16.4) | 16.4 | 0.99 |

In sorted order the tree requests less than the map did: 26.5 against 30.0 MiB
for `json` and 34.8 against 42.7 MiB for `tiny`, because appending splits leave
the left node full.

| ns per operation (5E) | sequential insert | random insert | random overwrite | random delete | get hit | traversal per record |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| json | 93 (226) | 637 (770) | 768 (589) | 700 (551) | 456 (483) | 4.7 (9.6) |
| tiny | 86 (215) | 740 (931) | 871 (723) | 940 (685) | 574 (599) | 8.0 (14.1) |
| large | 228 (295) | 544 (509) | 573 (428) | 237 (141) | 145 (104) | 2.6 (2.1) |

| flush | target MiB | runs | stored/payload | ms | payload MiB/s | peak collected MiB | peak streamed MiB |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| json | 64 | 1 | 0.202 | 34 | 506 | 7.6 | 7.6 |
| json | 4 | 5 | 0.202 | 33 | 519 | 4.5 | 1.8 |
| tiny | 64 | 1 | 0.547 | 56 | 189 | 14.0 | 14.0 |
| tiny | 4 | 5 | 0.547 | 56 | 191 | 7.5 | 3.4 |
| large | 64 | 1 | 0.507 | 12 | 1330 | 24.4 | 24.4 |
| large | 4 | 5 | 0.507 | 12 | 1373 | 10.1 | 4.1 |

- Measured RSS growth stayed at or below the charge in all six insert cases,
  including after overwriting, and the 128-byte entry charge still covers the
  tree's per-entry overhead with the same margin. That is an observation on
  these datasets and this allocator, not a limit.
- Amplification is a small-record problem. Requested memory is 1.02 times the
  payload at 4 KiB values, 1.6 to 1.7 at 100 bytes and 3.3 to 3.9 at 26 bytes.
- An entry is one allocation holding its key and value together, so an insert
  makes 1.10 to 1.14 allocations (5E: 2.13 to 2.17) and an overwrite or delete
  one. Lookups, traversal and freezing allocate nothing.
- Sequential inserts are 1.3 to 2.5 times faster and traversal about twice as
  fast for small records. Overwrites and deletes are 20 to 40% slower and
  lookups on `large` about 40% slower: the tree compares through a shared
  pointer at every level and replaces the entry's allocation rather than its
  value in place. This is the price of sharing nodes with snapshots.
- Splitting costs nothing measurable in flush time. It does bound what is held:
  for `large`, the 24.4 MiB peak of one run falls to 10.1 MiB when all five runs
  are kept and 4.1 MiB when each is released as it is produced. These peaks are
  on top of the frozen table and leave out Zstandard's own context.
- Dropping a frozen table takes 12 ms for `json` and 16 ms for `tiny` (5E: 11
  and 28 ms), because each entry is freed individually.

### Integrated engine

`block_bench engine` drives `Engine` with the `json` dataset (173,536 records,
17.1 MiB of payload), beside the memtable and run-set operations it is built on,
so the engine's own cost shows as the difference. Run it with
`scripts/validate.sh engine`. Contention rows sample every sixteenth write's
latency, on the reference environment's 2 CPUs.

#### Findings

Same reference environment, three passes. Times differ between passes by a
median of 2.9% and at most 15.9%.

| operation | direct | engine | blocks per operation |
| --- | ---: | ---: | ---: |
| sequential write, ns | 219 | 213 | |
| random write, ns | 786 | 809 | |
| overwrite, ns | 567 | 581 | |
| get from the active table, ns | 508 | 509 | 0 |
| get from 5 disjoint runs, us | 10.0 | 10.1 | 1 |
| get from 8 overlapping runs, us | | 76.9 | 8 |
| scan of runs only, ns per record | 85.9 | 94.9 | |
| scan of the active table only, ns per record | | 42.3 | |
| scan half table and half runs, ns per record | | 74.9 | |
| prefix scan of 20 keys, half table and half runs, us | | 11.2 | 1.05 |

| contention | writes M/s | write p50 ns | write p99 ns | write max |
| --- | ---: | ---: | ---: | ---: |
| writer alone | 1.76 | 542 | 834 | 9 us |
| writer and a point reader | 1.11 | 708 | 2,833 | 15 us |
| writer and back-to-back full scans | | 1,209 | 5.9 ms | 6.6 ms |
| writer and a flush every 20,000 writes | 1.22 | 500 | 2,417 | 458 us |

- The lock and the copy of the value cost nothing measurable on point reads
  and writes. Engine writes are within 3% of direct memtable writes.
- The scan merge adds about 9 ns per record over the logical cursor on runs.
  An active-only scan costs 42 ns per record, against 9 ns for iterating the
  memtable directly, so the merge loop, not the data, dominates it.
- Two point readers reach 3.83 million reads per second together, about twice
  one reader. A point reader and a writer share the lock at 1.11 million
  writes and 0.33 million reads per second.
- A write waits for at most the scan in progress: its p99 equals one full scan
  of 173,536 records. Before writers were given priority, a reader starting a
  scan as soon as one ended kept a write waiting 8 to 21 seconds.
- A flush every 20,000 writes keeps the write p99 near 2.4 us. Its one 458 us
  write has not been traced to a cause.
- Ten flushes from a 4 MiB table take 3.7 ms each, at 464 MiB/s of payload.
  Runs store 0.212 bytes per payload byte, and since nothing is rewritten that
  is the whole write amplification. Rust heap peaked 6.8 MiB above its start
  and kept 3.6 MiB, equal to the stored runs.

### Compaction

`block_bench compaction` writes 1,041,216 puts over the 173,536 keys of the
`json` dataset through a 4 MiB memtable, in two workloads:

- **random**: six passes over every key in random order. Every flush covers
  the whole key space, the worst case for overlap.
- **localized**: 96 phases, each rewriting one sixteenth of the key space,
  contiguous in key order, with the window moving to its neighbour each phase.

The same workload runs in three modes. **none** never compacts. **sync**
compacts once after each flush on the writing thread, with default options.
**background** has the worker, with the same options. When writing ends,
sync and background compact until nothing is eligible; none is left as it is.
A reader thread samples a get, or every eighth time a 200-key group scan,
every 5 ms throughout, so that slow reads cost writers little. Every 16th
put's latency is sampled, including any flush or synchronous compaction it
triggers. Run it with `scripts/validate.sh compaction`.

#### Findings

Reference environment (2 CPUs), three rounds in one pass. Rounds 0 and 1
agree within 2%. Round 2 ran about 20% slower in every mode, including none,
and is left out. Counts and bytes are identical in every round except the
background ones, which depend on when the worker runs.

| workload | mode | puts M/s | write max | compaction CPU s | process CPU s | stored written per byte flushed | mean overlap while writing | overlap when writing ends | catch-up ms | total s | get p50 | get p99 | peak heap MiB |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| random | none | 1.62 | 3.8 ms | 0 | 0.65 | 1.00 | 29.5 | 58 | | 0.64 | 270 us | 560 us | 25.9 |
| random | sync | 0.75 | 61 ms | 0.8 | 1.38 | 3.13 | 2.5 | 2 | 0 | 1.38 | 49 us | 64 us | 12.7 |
| random | background | 1.60 | 3.8 ms | 0.6 | 1.29 | 2.66 | 8.5 | 8 to 13 | 52 to 66 | 0.72 | 89 us | 135 us | 18.9 |
| localized | none | 2.13 | 4.0 ms | 0 | 0.49 | 1.00 | 8.3 | 15 | | 0.49 | 62 us | 155 us | 24.4 |
| localized | sync | 1.02 | 56 ms | 0.5 | 1.03 | 2.76 | 3.4 | 4 | 0 | 1.02 | 30 us | 57 us | 16.2 |
| localized | background | 2.09 | 3.8 ms | 0.5 | 1.01 | 2.59 | 5.5 | 6 to 7 | 143 | 0.64 | 44 us | 80 us | 22.0 |

Write p50 and p99 were the same in every mode, 250 to 334 ns and 459 to
542 ns: almost every put touches only the memtable. Flushes ran at 101 to
106 MiB/s of stored output in every mode.

- Background compaction gives foreground writes the speed of no compaction,
  and its worst write, about 3.8 ms, is a flush. Synchronous compaction halves
  write throughput and puts a compaction of up to 61 ms in front of a write.
- The work is not free. The process used about twice the CPU of none in both
  compacting modes. Background used slightly less than sync, because it
  lagged and so merged more runs at once, rewriting less: 2.66 bytes stored
  per byte flushed against 3.13 on random.
- On 2 CPUs the worker nearly keeps up. Overlap during random writes averaged
  8.5 against the limit of 4 and did not grow over time. When writing ended,
  50 to 150 ms of compaction remained. Counting that, background reached the
  caught-up state in about half the time of sync.
- Reads pay for the lag. Gets during writing were 1.8 times slower than with
  sync, and 3 times faster than with no compaction.
- Heap peaks are higher with background than sync, because more runs wait
  for compaction. The peaks count Rust allocations only.
- In the localized workload, runs left after catch-up (4 to 10) are adjacent
  and do not overlap. There are fewer than 16, so they are not merged.

An earlier version of the localized workload moved the window seven
sixteenths of the key space each phase. A flush spanning two phases then had a
key range covering everything between the two windows, so the overlap
measured from key ranges was as high as for random writes (56 runs without
compaction), and compaction cost as much. Key ranges cannot describe a run
that is dense in two separate places.

### Snapshots

A snapshot takes a copy-on-write view of the active memtable, so acquisition
neither copies nor freezes it; later writes copy the tree nodes the view still
shares and charge them to the memtable's budget as pinned history. Two modes
measure this on the 173,536-key `json` dataset (values of about 90 bytes) and
on 4,096 keys with 4 KiB random values (`large`). Each row starts from every
key put in random order, flushed and compacted with default options.

`scripts/validate.sh snapshot` (about 100 s) measures acquisition against
table size, the cost moved to writes, reads, a writer under concurrent scans,
and reclamation. `scripts/validate.sh frequency` (about 30 s, two rounds)
runs one overwrite pass with a snapshot every N puts. Its configurations are:

- 1, 4 or 16 MiB memtables, and the two value kinds.
- **catch-up**: no compaction during the pass, with `max_runs` raised to
  4096, then compaction until nothing is eligible.
- **background**: the worker with default options and the default `max_runs`
  of 64. A flush refused at that limit waits for compaction.
- **lifetime** rows (json, background, every 1,000 puts): each snapshot is
  held for the next 4,000 or 15,000 puts, or the first is held for the whole
  pass.

Every snapshot scans one 200-key group. Every tenth also scans a 4,000-key
tenant, then gets one key and is dropped. Total time runs from the first put
until flushing and compaction leave nothing eligible, so every configuration
is compared at the same completion point.

#### Findings

Reference environment (2 CPUs), with the uncommitted 5E implementation, which
froze or copied the active table, measured in the same session as the
baseline: **freeze** always froze it, **hybrid** copied a table charged at most
1 MiB and froze a larger one, **copy** always copied. Counts were identical
across rounds, and times varied by at most about 10% unless a range is given.

**Acquisition** (one thread, so the time is the time the exclusive lock is
held; each acquisition follows a put, so it takes a new view):

| values | charged | acquire p50 | acquire max | allocations | 5E copy p50 | 5E freeze |
|---|---|---|---|---|---|---|
| json | 16 KiB | 42 ns | 250 ns | 3 | 1.3 µs | 126 ns |
| json | 1 MiB | 42 ns | 333 ns | 3 | 96 µs | 168 ns |
| json | 4 MiB | 42 ns | 375 ns | 3 | 404 µs | 378 ns |
| json | 16 MiB | 42 ns | 667 ns | 3 | — | — |
| large | 4 MiB | 42 ns | 125 ns | 3 | 113 µs | 209 ns |
| large | 16 MiB | 42 ns | 750 ns | 3 | — | — |

Acquisition is constant in table size. With the base state of 3 runs it is
42 ns p50 and 84 ns p99 after a put, with 4 allocations; sharing an unchanged
view is 22–23 ns, 1 allocation and 88 bytes held, as before. No snapshot in
any mode froze or flushed a table.

**The cost moves to writes** (1,000 overwrites of random keys; pinned and
freed per overwrite under one held snapshot):

| values | charged | put, no snapshot | put after a fresh snapshot | put under a held snapshot | pinned B/overwrite | heap freed B/overwrite | held drop |
|---|---|---|---|---|---|---|---|
| json | 16 KiB | 82 ns | 208 ns | 79 ns | 13 | 13 | 1 µs |
| json | 1 MiB | 180 ns | 374 ns | 251 ns | 338 | 341 | 25 µs |
| json | 4 MiB | 250 ns | 517 ns | 532 ns | 779 | 783 | 51 µs |
| json | 16 MiB | 540 ns | 804 ns | 1,085 ns | 1,225 | 1,229 | 145 µs |
| large | 4 MiB | 326 ns | 525 ns | 883 ns | 2,618 | 2,622 | 17 µs |
| large | 16 MiB | 491 ns | 717 ns | 1,406 ns | 3,844 | 3,849 | 40 µs |

The first put after a snapshot copies its path, 120–270 ns more. Under a
held snapshot, each overwrite of a key the snapshot has not yet diverged on
copies one leaf and its branches; that is the pinned history, and the
estimate charged to the budget matched the heap freed on release within 1%.

**Frequency** (flushes are the same for catch-up and background):

| values | budget | every | flushes (5E freeze / hybrid) | acquire p50 / p99 | catch-up total s (5E hybrid) | background rewrite / flushed (never) |
|---|---|---|---|---|---|---|
| json | 1 MiB | 100 | 39 (695 / 39) | 84–125 / 250–292 ns | 0.96 (1.15–1.18) | 8.31–8.34 (3.97–4.14) |
| json | 1 MiB | 1,000 | 39 (70 / 39) | 125 / 417–625 ns | 0.32 (0.35) | 3.24–3.59 |
| json | 4 MiB | 100 | 10 (695 / 16) | 84 / 250–333 ns | 0.60–0.61 (0.83) | 2.63 (2.63–2.74) |
| json | 4 MiB | 1,000 | 10 (70 / 14) | 125–166 / 0.4–2.5 µs | 0.26–0.27 (0.23) | 2.63 |
| json | 16 MiB | 100 | 3 | 84 / 167–416 ns | 0.47–0.48 | 1.83 (1.83) |
| large | 4 MiB | 100 | 17 (66 / 22) | 84–125 / 375–541 ns | 0.15 | 1.98 (1.98) |
| large | 16 MiB | 100 | 5 | 84 / 625–750 ns | 0.12 | 1.00 (1.00) |

Without snapshots the same passes flushed 39, 10 and 3 times for json and 67,
17 and 5 for large values, and catch-up took 0.22–0.25 s for json.

**Lifetimes** (json, background, a snapshot every 1,000 puts):

| held for | budget | flushes (none held) | rewrite / flushed | peak pinned | peak retained | acquire p99 |
|---|---|---|---|---|---|---|
| 4,000 puts | 4 MiB | 16 (10) | 3.66 | 1.2 MiB | 8.6 MiB | 0.8–2.8 µs |
| 4,000 puts | 16 MiB | 4 (3) | 1.63 | 2.9 MiB | 16.1 MiB | 0.8–12.7 µs |
| 15,000 puts | 4 MiB | 19 (10) | 4.74 | 1.9 MiB | 13.1 MiB | 2.4–2.5 µs |
| 15,000 puts | 16 MiB | 7 (3) | 2.92 | 9.2 MiB | 24.2 MiB | 6.7–6.8 µs |
| the first, whole pass | 4 MiB | 10 (10) | 2.63 | 50 KiB | 7.5 MiB | 0.5–0.6 µs |
| the first, whole pass | 16 MiB | 3 (3) | 1.83 | 50 KiB | 16.0 MiB | 0.5–0.7 µs |

**Concurrent writer** (1 MiB memtables, 21,692 puts with synchronous
compaction after each flush; readers repeat full scans):

| readers | puts M/s | put p99 | put max | flushes | compaction rewrite | reader acquire p50 / max |
|---|---|---|---|---|---|---|
| none | 0.33–0.34 | 0.58–0.75 µs | 7–9 µs | 4 | 3.6 MiB | — |
| 1 guard | 0.00 | 22 ms | 23–36 ms | 4 | 3.6 MiB | — |
| 1 snapshot | 0.33 | 0.67–0.75 µs | 2 µs | 4 | 3.6 MiB | 0.5–0.6 / 1.2–2.2 µs |
| 2 snapshots | 0.33 | 0.63–0.75 µs | 6–8 µs | 4 | 3.6 MiB | 0.3–0.7 / 1.6–2.3 µs |

In 5E, freezing readers reduced throughput to 0.12–0.28 M/s and doubled
compaction rewrite to 7.4 MiB, and the hybrid's reader acquisition reached
8–190 µs at its maximum.

**Other measurements** (three rounds):

- **Reads** (2,000 keys overwritten): group scans 52.0–52.2 µs through a
  snapshot against 52.2–52.5 µs through a guard, and full scans 113–116 ns per
  record either way. Gets were 110–118 ns through a snapshot and 110–123 ns
  through `Engine::get`, against 85–91 and 89–99 ns in 5E, about 25% slower
  on the small active tree; see the memtable findings.
- **Reclamation**: a snapshot of 3 runs and a 1.5 MiB table, held while every
  key was overwritten, flushed and compacted away, pinned 0.6 MiB in the
  memtable and was reported as retaining 7.5 MiB (5E: 5.0 MiB); dropping it
  freed 4.5 MiB of Rust heap in 312–333 µs (5E: 472–504 µs for a frozen
  table, 255–263 µs for a copy).

Measured:

- **Snapshots no longer fragment flushes.** In every frequency row, flushes,
  runs flushed and flushed bytes equal the row without snapshots, at every
  100, 1,000 and 10,000 puts and every budget, where 5E freezing flushed up to
  695 times and the hybrid up to 16 and 22 times above its copy limit.
- **Acquisition is constant.** Median acquisition is 42–125 ns at every table
  size and frequency, against 52–68 µs for the 5E hybrid and up to 404 µs for
  copying a 4 MiB table. The lock is held for that time only.
- **The writer's own time is unchanged.** Catch-up write time minus the
  snapshots' scans equals the row without snapshots (about 0.11 s for json).
  The remaining total, 0.96 s against 0.25 s at every 100 puts with 1 MiB
  tables, is the 1,735 snapshots' own scans and gets.
- **The cost of a held snapshot is retention, not freezing.** Pinned history
  grows with what later writes replace, up to 1.2 KiB per json overwrite and
  3.8 KiB per large one in 16 MiB tables, and counts against the memtable
  budget, so long-lived snapshots bring flushes forward: 16 and 19 against 10
  with 4 MiB tables. A snapshot held over a whole pass pins little in the
  memtable (50 KiB) because the table it saw was flushed early, but keeps the
  runs it saw: 7.5 and 16 MiB retained at completion.
- **Background rewrite still depends on pacing.** At every 100 puts with
  1 MiB tables the worker rewrote 8.31–8.34 times what was flushed against
  3.97–4.14 without snapshots (5E hybrid 9.23), though flushes were equal and
  catch-up rewrite was identical (3.08) at every frequency. With 4 and 16 MiB
  tables it equalled no snapshots.

Hypotheses, not measured:

- The extra background rewrite at 1 MiB comes from compaction pacing: the
  snapshots' scans slow the pass, so the worker compacts smaller increments
  more often. Catch-up rewrite being unchanged supports this; no run of the
  worker with snapshots that do no reads was made.
- The p99 acquisition outliers of 2.4–12.7 µs, seen mostly while snapshots
  are held and once without (2.5 µs), are allocator work after a large free
  or a wait for a worker's publication. The cause was not measured, and the
  same configuration varied from 0.8 to 12.7 µs between rounds.
- The retained figure overestimated after release because a released memtable
  was then charged in full while any snapshot shared it: the heap freed was
  4.5 of 7.5 MiB. A released table that only views share is now charged the
  newest view's charge plus the pinned history; these rows predate that.
- With the default 64 MiB budget, the pinned history a snapshot causes would
  be bounded by the same budget; only 1, 4 and 16 MiB tables were measured.

### Retention under sustained snapshots

Three modes measure what copy-on-write snapshots cost once they are held:

- `scripts/validate.sh retention` (about 15 s): 300,000 puts with the
  background worker and 4 MiB memtables, from a base of every key flushed and
  compacted, for four workloads (json values over 174k keys uniformly, the
  same with 90% of puts to a hot tenth, json over 17k keys, and 8-byte values
  over 214k keys) and five snapshot patterns. Each row is a fresh process.
  Then a backpressure sequence with a small retained limit.
- `scripts/validate.sh persistent` (about 50 s): memtable writes beside no
  snapshot, one held snapshot, or a snapshot every 1,000 writes kept until the
  next, and reads, scans and flushes of a table written that way.
- `scripts/validate.sh pacing` (about 10 s): background rewrite with an event
  every 100 puts that reads through a snapshot, only acquires one, spins or
  sleeps for the reads' time, or does nothing.

Sources of retained memory are reported apart: **pinned** is memtable history
only snapshots need; **tables** and **runs** are released memtables and runs
that snapshots hold (`EngineStats::retained_run_bytes` gives the runs).
**Engine** is what the live engine reaches: memtable charges without pinned
history, plus published run bytes.

#### Findings

Reference environment, one pass each; counts repeated exactly where rerun.

**Sustained workloads** (MiB unless stated; freed is the heap released by
dropping the snapshots still held at the end of the writes):

| workload | snapshots | puts M/s | flushes | rewrite / flushed | peak engine | peak pinned | peak tables | peak runs | peak heap growth | peak RSS growth | retained at end | freed | freed / retained |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| json 174k uniform | none | 1.45 | 16 | 3.32 | 10.5 | 0 | 0 | 0 | 10.2 | 10.2 | 0 | 0 | — |
| | every 100, dropped | 0.67 | 16 | 3.28 | 9.1 | 0 | 0 | 0 | 9.0 | 12.3 | 0 | 0 | — |
| | 8 held | 1.26 | 31 | 4.00 | 8.2 | 1.9 | 15.7 | 4.9 | 13.9 | 12.6 | 7.9 | 4.5 | 0.57 |
| | 8 held + tenant scans | 0.64 | 31 | 5.45 | 6.7 | 1.9 | 15.6 | 4.3 | 15.0 | 15.3 | 12.7 | 7.1 | 0.56 |
| | one, whole pass | 1.43 | 16 | 3.32 | 9.9 | 0 | 0 | 3.5 | 13.2 | 9.4 | 3.5 | 3.5 | 1.00 |
| json 174k, 90% hot | none | 1.73 | 9 | 3.06 | 8.9 | 0 | 0 | 0 | 8.8 | 6.2 | 0 | 0 | — |
| | 8 held | 1.50 | 28 | 3.86 | 7.8 | 2.1 | 11.9 | 4.8 | 14.4 | 9.3 | 8.1 | 4.1 | 0.51 |
| | one, whole pass | 1.90 | 9 | 3.06 | 9.1 | 0 | 0 | 3.5 | 9.1 | 5.4 | 3.5 | 3.5 | 1.00 |
| json 17k uniform | none | 3.11 | 8 | 1.20 | 5.5 | 0 | 0 | 0 | 4.8 | 3.7 | 0 | 0 | — |
| | 8 held | 2.00 | 28 | 1.61 | 3.2 | 2.1 | 11.9 | 1.1 | 7.9 | 7.9 | 8.3 | 3.5 | 0.42 |
| | one, whole pass | 2.93 | 8 | 1.20 | 5.5 | 0 | 0 | 0.4 | 4.8 | 4.2 | 0.4 | 0.4 | 1.00 |
| 8-byte 214k uniform | none | 1.68 | 11 | 3.02 | 9.1 | 0 | 0 | 0 | 8.9 | 10.3 | 0 | 0 | — |
| | 8 held | 1.42 | 27 | 5.02 | 6.2 | 2.4 | 11.9 | 3.9 | 10.4 | 11.5 | 4.4 | 1.7 | 0.39 |
| | 8 held + tenant scans | 0.79 | 27 | 5.64 | 5.5 | 2.4 | 15.9 | 3.6 | 9.6 | 15.0 | 8.1 | 2.1 | 0.26 |
| | one, whole pass | 1.67 | 11 | 3.02 | 9.1 | 0 | 0 | 3.0 | 11.9 | 12.1 | 3.0 | 3.0 | 1.00 |

"8 held" takes a snapshot every 1,000 puts, keeps up to eight and drops a
random one when a ninth arrives. No snapshot was refused and no flush
stalled. Heap growth after flushing and compaction finished was at most
0.5 MiB in every row. RSS at the start of a row was 14–59 MiB, mostly the
dataset.

**Backpressure** (json uniform, 1 MiB memtables, no worker, `max_runs` 16,
`max_retained_bytes` 2 MiB, one snapshot held from the start):

| event | puts | retained | retained runs | memtables | runs | frozen tables | heap growth |
|---|---|---|---|---|---|---|---|
| first compaction refused | 4,610 | 3.6 | 3.6 | 0.0 | 1 | 0 | 3.5 |
| first snapshot refused | 5,000 | 3.6 | 3.6 | 0.1 | 1 | 0 | 3.6 |
| first flush refused | 78,178 | 3.6 | 3.6 | 1.0 | 16 | 1 | 5.7 |
| writes refused | 82,789 | 3.6 | 3.6 | 2.0 | 16 | 3 | 6.5 |
| 50,000 puts after dropping it | 132,789 | 0 | 0 | 0.9 | 1 | 0 | 0.6 |

Dropping the snapshot freed 3.6 MiB in 1 µs.

**Persistent tree** (ns per operation, allocations per operation; 5D is the
latest-only `BTreeMap` memtable, measured in the same session):

| dataset | workload | 5D | none | one held | every 1,000, kept until the next |
|---|---|---|---|---|---|
| json | sequential insert | 215, 2.17 | 90, 1.10 | 90, 1.10 | 84, 1.11 |
| json | random insert, half full | — | 660, 1.14 | 697, 1.23 | 1,256, 3.74 |
| json | random overwrite | 582, 1.00 | 737, 1.00 | 826, 1.10 | 1,443, 3.87 |
| json | random delete | 714, 0.00 | 773, 1.00 | 793, 1.10 | 1,470, 3.87 |
| 8-byte | random insert, half full | — | 810, 1.14 | 895, 1.23 | 1,631, 4.64 |
| 8-byte | random overwrite | 691, 1.00 | 895, 1.00 | 998, 1.10 | 1,960, 4.86 |
| 8-byte | random delete | 686, 0.00 | 939, 1.00 | 952, 1.10 | 1,942, 4.86 |

A held snapshot pins 52 bytes per insert and 94–171 per overwrite or delete.
After a full overwrite pass under the rolling snapshot, all dropped since,
reads of the resulting table against one written with none: gets 558 against
510 ns (json) and 641 against 608 ns (8-byte), traversal 4.5 against 6.6 and
7.8 against 7.5 ns per record, group scans of 200 records 2.2 against 2.1 µs
and 2.8 against 2.8 µs, tenant scans of 4,000 records 40 against 52 µs and
45 against 51 µs, and flushes 207 against 203 and 133 against 134 ns per
record. 5D's gets were 511 (json) and 638 ns (8-byte), traversal 9.7 and 15.2.

**Pacing** (json, 1 MiB memtables, worker, an event every 100 puts):

| event | write s | flushes | passes | compactions | rewrite / flushed | mean input KiB | runs after writes |
|---|---|---|---|---|---|---|---|
| reads through a snapshot | 0.38–0.39 | 39 | 5 | 8 | 8.34 | 3,990 | 7 |
| snapshot acquired only | 0.10 | 39 | 5 | 6 | 3.64 | 2,323 | 14 |
| spin for the reads' time | 0.38–0.39 | 39 | 5 | 8 | 8.37 | 4,006 | 10–11 |
| sleep for it (overshoots) | 0.59 | 39 | 21 | 9 | 9.23 | 3,927 | 7 |
| nothing | 0.10–0.11 | 39 | 5 | 6 | 3.64 | 2,323 | 14 |

Measured:

- **Short-lived snapshots cost the write path nothing.** One dropped before
  the next write leaves no node shared, so flushes and rewrite equal no
  snapshots; the throughput drop is the snapshots' own scans.
- **Held snapshots move flushes forward.** Eight held at once pinned up to
  1.9–2.4 MiB of the 4 MiB budget, so flushes rose from 8–16 to 27–31 and
  rewrite from 1.2–3.3 to 1.6–5.8 times what was flushed.
- **A long-lived snapshot keeps exactly what it saw.** Held over a whole
  pass of 300,000 puts and up to 16 flushes and their compactions, it kept the
  base runs, 0.4–3.5 MiB, from the first compaction on, and the figure did not
  grow; dropping it freed exactly that.
- **Retained bytes are an upper bound.** Dropping the held snapshots freed
  26–57% of what was reported for memtable history and released tables, and
  100% where only runs were held. A tests/retention.rs check finds no case of
  more freed than reported over random histories.
- **Backpressure holds memory flat.** Under a 2 MiB retained limit, each
  refusal began in the documented order, heap growth stayed under 6.5 MiB
  until writes were refused, and dropping the snapshot restored progress.
- **A snapshot that stays shared doubles random-write cost.** A new snapshot
  every 1,000 writes, held until the next, makes each random write copy its
  path: 3.7–4.9 allocations and 1.7–2.2 times the time. At the engine level,
  eight held snapshots cost 13–36% of put throughput, flushes included.
- **The higher background rewrite is pacing, not snapshots.** Spinning or
  sleeping for the reads' time reproduces it without any snapshot, and
  acquiring snapshots without reading matches none. A slower pass gives the
  worker time to compact after fewer new runs, so it compacts more often with
  larger inputs: 8–9 compactions of 3.9–4.0 MiB against 6 of 2.3 MiB.

Hypotheses, not measured:

- The retained overestimate for released tables comes mostly from the
  128-byte entry charge, which exceeds what small entries allocate, and from
  history pinned for snapshots already dropped.
- At larger scale, a snapshot held across a full compaction keeps a second
  copy of every run it saw, up to doubling run memory; `max_retained_bytes`
  bounds that by refusing compactions, not by reclaiming.
- Reads of a table written under frequent snapshots differ within noise from
  one written without; copied nodes did not measurably hurt locality.

### Where write time goes

`scripts/validate.sh profile` (about 40 s) attributes memtable and engine
cost by measuring components separately, since the bounded container has no
sampling profiler or hardware counters. Absolute times on the reference host
varied by more than twofold between runs, so only ratios within one run are
compared.

#### Findings

One run whose times matched the memtable benchmark (json, 174k entries;
8-byte values in parentheses):

| component | ns/op | allocs/op |
|---|---|---|
| get, random order | 460 (556) | 0 |
| get, the same keys ascending | 145 (139) | 0 |
| get, 1,000-key table | 88 (83) | 0 |
| one entry allocated and freed, no tree | 102 (85) | 1 |
| overwrite, no snapshot | 725 (891) | 1 |
| overwrite, snapshot every 1,000 dropped at once | 708 (886) | 1 |
| overwrite, snapshot every 10,000 kept until the next | 1,288 (1,701) | 2.2 (2.8) |
| overwrite, snapshot every 1,000 kept until the next | 1,413 (1,873) | 3.9 (4.9) |
| overwrite, snapshot every 100 kept until the next | 1,535 (1,992) | 6.1 (6.9) |
| engine.put overwrite, never flushing | 746 (891) | 1 |
| freeing a snapshot's history, per entry | 49 (38) | — |
| flush, per record | 287 (279) | — |

Sustained engine writes (300,000 random puts, 4 MiB memtables, worker):

| values | snapshots held | writer s | in puts | flush build | worker CPU s | flushes |
|---|---|---|---|---|---|---|
| json | 0 | 0.18 | 0.12 | 0.05 | 0.15 | 15 |
| json | 1 | 0.20 | 0.13 | 0.05 | 0.17 | 18 |
| json | 8 | 0.21 | 0.13 | 0.06 | 0.19 | 29 |
| 8-byte | 0 | 0.17 | 0.13 | 0.04 | 0.15 | 10 |
| 8-byte | 8 | 0.19 | 0.13 | 0.04 | 0.18 | 25 |

- A random lookup in a table larger than the cache is about 70% memory
  latency: the same lookups in key order take a third of the time. Keys are
  compared through a pointer at every level.
- An overwrite is that search plus about 100 ns of entry allocation; the
  engine's lock and sequence number add about 3%.
- Puts are about two thirds of the writer's time and flush builds most of
  the rest; the worker's compaction CPU is about the writer's total.
- A snapshot dropped before the next write costs nothing. One kept until the
  next doubles a random write.

A temporary comparison with `im::OrdMap`, holding the same single key and
value allocation per entry, measured in the same process twice: random
inserts, overwrites, deletes and gets within 0.90–1.04 times the tree,
sequential inserts 1.40–1.44 times, ordered iteration 3.3–5.0 times,
prefix scans 1.14–1.52 times, writes under kept snapshots 1.14–1.17 times,
and 1.10–1.19 times the heap. It offers no view of which nodes a write
copies, so pinned history could not be charged. `bbtree` was not measured:
its documentation states that a scan is not a snapshot.

### Compaction selection

`scripts/validate.sh policy` (about 40 s) records every compaction the
worker's loop runs: input sizes, the largest input's share and age, input
and output bytes, CPU and wall time, and per-run rewrite counts. A bench
thread runs that loop through the public API so each selection is visible.
The base state is 173,536 json keys put in random order, flushed and
compacted; then 350,000 puts with 1 MiB memtables, unpaced and paced at 400k
and 100k puts per second, for three workloads: uniform overwrites, an
8,000-key window sliding across the key space, and new keys in random order.
Total time runs until nothing is eligible. Two rounds agreed within a few
percent, and paced rows almost exactly.

#### Where the work went

The earlier policy took the smallest runs at the deepest key up to
`max_inputs` (8). A flush of random keys covers the whole key space, so
every flush deepened the same stack. Unpaced, more than eight runs piled up
before the worker ran and the large run was left out. Paced, the worker kept
up, and each time the fifth run arrived it merged all five: the large run
plus one flush's worth. At 100k puts per second, 90–100% of compaction input
went to compactions whose largest input was at least four times the rest,
and the largest input was 89–91% of all input bytes. Rewrite (compaction
input per flushed byte) rose with pacing: uniform 3.6, 8.8 and 9.8; window
8.4, 10.5 and 10.5; new keys 3.6, 6.3 and 9.3.

#### Alternatives (same session, `max_overlap` 4 unless noted)

| alternative | effect |
|---|---|
| size-tiered: the fewest smallest runs that restore the allowance, then each next run no larger than all taken | rewrite 2.4–4.2 at every rate; compactor CPU down 55–80% when paced; settles at up to the allowed overlap |
| minimal: only the fewest that restore the allowance | rewrite up to 15.7: the large run soon becomes one of the fewest |
| earlier policy with `max_overlap` 8 | rewrite 0.8–4.3, but the window workload settles at 47 runs and reads load 4.8 blocks |
| size-tiered with `max_overlap` 3 or 2 | the lopsided merges return (60–95% of input) and rewrite reaches 7–18 |

Size-tiered selection is now the policy. It keeps every invariant of the
merge (inputs share the deepest key, so the overlap there falls and nowhere
rises), and needs no new state.

#### Before and after

The earlier policy from a copy of the previous commit and the current one,
run in the same session, medians of two rounds:

| workload | rate | rewrite before → after | compactor CPU s | total s | runs at end | get after ns | tenant scan after µs |
|---|---|---|---|---|---|---|---|
| uniform | natural | 3.59 → 2.89 | 0.33 → 0.28 | 0.35 → 0.29 | 1 → 3 | 10,200 → 30,100 | 374 → 745 |
| uniform | 400k/s | 8.84 → 4.14 | 0.86 → 0.38 | 0.90 → 0.88 | 4 → 4 | 36,900 → 40,100 | 474 → 909 |
| uniform | 100k/s | 9.80 → 4.19 | 0.94 → 0.38 | 3.55 → 3.51 | 2 → 4 | 18,500 → 38,700 | 401 → 839 |
| window | natural | 8.45 → 2.44 | 0.53 → 0.14 | 0.54 → 0.22 | 6 → 6 | 10,200 → 33,800 | 416 → 843 |
| window | 400k/s | 10.49 → 3.47 | 0.65 → 0.19 | 0.92 → 0.94 | 2 → 3 | 16,700 → 10,300 | 388 → 450 |
| window | 100k/s | 10.49 → 3.71 | 0.66 → 0.20 | 3.53 → 3.55 | 2 → 3 | 16,500 → 16,900 | 386 → 504 |
| new keys | natural | 3.13 → 2.41 | 0.35 → 0.27 | 0.37 → 0.30 | 2–5 → 4 | 10,200 → 19,900 | 383 → 420 |
| new keys | 400k/s | 6.26 → 3.03 | 0.66 → 0.31 | 0.89 → 0.89 | 4 → 6 | 10,100 → 19,800 | 391 → 420 |
| new keys | 100k/s | 9.31 → 3.03 | 0.97 → 0.31 | 3.59 → 3.51 | 3 → 6 | 10,200 → 19,900 | 383 → 425 |

Writer time, writer CPU and put p99 were unchanged: the writer never waited
on compaction at these rates. Gets sampled during ingestion loaded fewer
blocks under uniform overwrites unpaced and at 400k/s (14.5 → 11.4, 7.2 →
4.6) and about as many at 100k/s (3.5 → 3.7), but more under the window
(1.2–2.5 → 2.8) and new keys (1.0 → 2.0). Their p99 moved within 13% under
uniform overwrites, from 41–64 to 50–55 µs for the window, and from 14–16 to
26–31 µs for new keys.

Settled reads are the cost. The earlier policy's frequent merge of the large
run often left one or two runs at the end; the size-tiered policy stops once
the overlap is within the allowance, so it settles at up to four overlapping
runs, as the earlier one also did in some rows. Settled point lookups load
one block per overlapping run and long prefix scans merge them: up to about
three times the get latency and twice the tenant scan time where the earlier
policy happened to finish with one run, and about 10% slower scans for new
keys, whose runs mostly do not overlap. The heap after settling held 2–2.7
MiB more under uniform overwrites, 0.7–2.2 MiB more for the window (superseded
versions in the extra runs) and 0.1–0.2 MiB more for new keys. Peak heap fell
for new keys (21.6–22.4 → 12.2–20.3 MiB), rose up to 3 MiB for uniform
overwrites, and for the window fell unpaced (12.4 → 7.3) and rose 1.9–2.2
MiB paced.

### Read amplification (investigation)

`scripts/validate.sh reads` (about 35 s; it builds with the `diagnostics`
feature, which adds a read-only `ReadGuard::runs` used only here) drives the
real engine and its
background worker: 173,536 json keys put, flushed and compacted, then
1.5 million puts unpaced or 600,000 at 200k/s with 1 MiB memtables and the
default limits (64 runs). It samples every 100,000 puts, then dissects point
lookups and prefix scans on the settled state, run by run, and consolidates
it with `Engine::compact` at `max_overlap` 2 and then 1. The previous policy
ran from a copy of commit `5e9208e` given the same read-only run accessor.
Three sessions; within-session comparisons only. Settled states differ from
round to round, because where ingestion stops decides which merge was last.

#### Sustained ingestion

Unpaced, the worker was busy at every sample under both policies and overlap
rose to 7–27 runs (uniform) and up to 54 (new keys); paced at 200k/s it was
mostly idle and overlap stayed at 3–7. No flush was refused (the closest was
54 of 64 runs), and catch-up after the last put took 0–0.57 s. Writer
throughput did not differ between the policies.

| workload, rate | rewrite previous → size-aware | worker CPU s |
|---|---|---|
| uniform, unpaced | 2.7–4.0 → 2.5–3.6 | 1.06–1.63 → 1.00–1.47 |
| uniform, 200k/s | 9.63 → 5.02 | 1.61 → 0.81 |
| window, unpaced | 3.1–3.9 → 2.9–3.3 | 0.72–0.93 → 0.67–0.76 |
| window, 200k/s | 10.34 → 3.50–3.61 | 1.09 → 0.35 |
| new keys, unpaced | 3.0–5.3 → 2.8–4.1 | 1.50–2.56 → 1.38–2.01 |
| new keys, 200k/s | 11.8–12.6 → 4.26 | 2.17–2.32 → 0.81 |

Over time at 200k/s, rewrite for new keys climbed 2.0 → 11.7 under the
previous policy, rewriting a run that grew with the data, and 1.5 → 3.9
under the size-aware one. Size tiers formed as expected where the data
allowed: new keys settled into runs of under 1 MiB, 1–4 MiB and 4–16 MiB.
With overwrites the key space is 3.4 MiB, so the largest tier is the whole
key space, and runs either hold all of it or are recent flushes.

#### Point lookups

Every run whose key range covers a key decodes one block (about 10 µs for
these runs; a flush run's smaller blocks cost less), whether or not it holds
the key, and nothing caches decoded blocks. Range metadata excluded nothing
under overwrites, whose every run spans the key space, and 1.8–3.5 runs per
lookup for new keys.

| state | candidate runs | holding a present key | absent key in range |
|---|---|---|---|
| uniform settled | 4 | 1.6–3.2 | 4 candidates, 0 hold |
| window settled | 1–3 | 1.0–1.2 | 1–2.3, 0 hold |
| new keys settled | 2.5–3.75 | 1.0 | 2.5–3.75, 0 hold |
| uniform, during unpaced writes | 9–22 | 1.5–2.7 | |
| new keys, during unpaced writes | 4–19 | 1.0 | |

Present and absent keys cost the same (33.4 µs for four runs, 8.2 µs for
one). A run-local Bloom filter could skip the candidates without the key:
almost all of them for absent keys, 80–95% during unpaced ingestion, 60–73%
for settled new keys, but only 20–60% for settled overwrites, where older
versions of the key sit in most runs and must still be read. These savings
are estimated from the counts, not measured. At n·log2(1/p)/ln 2 bits per
record, filters would take 3%, 6% and 9% of the stored bytes at 10%, 1% and
0.1% false positives for these json records (0.64 MiB for 11.1 MiB of runs at
1%); smaller values would make that share larger.

#### Prefix scans

Seeking costs about one block per participating run (41 µs for four runs, 10
µs for one), so 20-record scans are almost all seek. Long scans are paid per
physical record: in settled overwrite states each logical record was stored
1.6–3.2 times, and blocks decoded and time rose with it (tenant scans 803–1,508
µs at 1.64–3.24 physical records per record against 372 µs once consolidated).
With no duplicates (new keys) the cost of 2.6 overlapping runs was 1.2–1.3
times one run, from merging. Merging the runs costs 35–60% over scanning them
one by one. A Bloom filter cannot address any of this: not duplicated
history, not merge work, and not the seek into every run whose range meets
the prefix.

#### Consolidation

| state | rewritten | CPU s | peak heap | runs/overlap | get p50 | tenant scan |
|---|---|---|---|---|---|---|
| uniform | 7.5–11.1 MiB (1.0–1.4 times stored) | 0.08–0.10 | 7.6 MiB | 4/4 → 1/1 | 33.4 → 8.2 µs | 803–1,508 → 372 µs |
| window | 0–4.3 MiB | 0–0.05 | 7.5 MiB | 1–3 → 1/1 | 13.2 → 8.1 µs | 438 → 366 µs |
| new keys | 33.6–52.8 MiB (1.0–1.6 times) | 0.45–0.70 | 39–61 MiB | 6–7/3–4 → 4–5/1 | 24.6–34.2 → 8.4 µs | 473–496 → 383 µs |

That is 7–10% of the bytes the worker rewrote during ingestion for
overwrites and 24–41% for new keys, once. No merge reached an input limit.
A snapshot held through it kept every input allocated (5.2–33.6 MiB) until
dropped, so transient memory is up to twice the rewritten runs. Overlap 2 was
cheaper (0.1–0.7 times stored) but left two candidates.

Unexplained: long scans of one consolidated run were 25–35% slower in the
previous commit's build in both sessions, while scanning that run directly
cost the same. The read path is identical, so this is taken as code layout,
not policy.

### Run key filters

Each run its builder makes carries an in-memory Bloom filter over its
distinct user keys: 10 bits a key, 7 probes from one SipHash-1-3 hash by double
hashing, about 0.8% false positives. Point lookups skip a run whose range
covers the key only when its filter proves the key absent; scans do not use
filters. `scripts/validate.sh bloom` (about 70 s, `diagnostics` feature)
compares the engine's filtered lookup with an unfiltered `RunSet` over the
same run views, 20,000 probes a row, every answer compared. The base state is
compacted to one run; "16 flushes" adds sixteen 1 MiB memtables of overwrites
or of new keys without compacting, as during ingestion.

| dataset, state | runs | present: blocks, µs mean | absent in range: blocks, µs mean | filters of stored |
|---|---|---|---|---|
| json, base and 16 flushes | 17 | 17.0 → 1.83, 169 → 19.0 | 17.0 → 0.14, 169 → 1.8 | 5.9% |
| json, compacted (overlap 4) | 3 | 3.0 → 1.62, 29.7 → 16.1 | 3.0 → 0.02, 29.6 → 0.34 | 5.9% |
| json, base and 1 flush | 2 | 2.0 → 1.06, 18.2 → 10.4 | 2.0 → 0.02, 18.2 → 0.24 | 6.0% |
| json, 1 run | 1 | 1.0 → 1.0, 9.84 → 9.86 | 1.0 → 0.01, 9.78 → 0.15 | 6.0% |
| json, new keys, 16 flushes | 17 | 5.31 → 1.03, 53.9 → 10.7 | 5.31 → 0.04, 53.8 → 0.72 | 5.8% |
| 8-byte, base and 16 flushes | 17 | 17.0 → 1.58, 262 → 25.6 | 17.0 → 0.14, 262 → 2.7 | 8.6% |
| 8-byte, compacted (overlap 4) | 3 | 3.0 → 1.42, 46.5 → 22.6 | 3.0 → 0.02, 46.5 → 0.50 | 8.7% |
| 8-byte, 1 run | 1 | 1.0 → 1.0, 15.6 → 15.7 | 1.0 → 0.01, 15.6 → 0.20 | 8.8% |
| 8-byte, new keys, 16 flushes | 17 | 4.05 → 1.02, 62.0 → 16.6 | 4.05 → 0.03, 61.8 → 0.78 | 8.5% |

Observed false positives were 0.7–0.9% of candidates without the key, and
lookups allocated nothing. A present key still decodes every run holding a
version of it (1.4–1.7 runs under overwrites), so present-key gains are
largest with many small runs. With one run, where a filter can only help an
absent key, present lookups cost 0.2–0.8% more. A proof of concept on the
same states (filters built by the benchmark) gave the same counts at 5%, 1%
and 0.1% targets: 3.7%, 5.6% and 8.4% of stored bytes for json, with absent
lookups at 8.6, 2.0 and 0.5 µs on 17 runs; 1% was kept.

End to end (`scripts/validate.sh reads`, the previous commit from a copy, two
rounds each, one session): gets sampled during unpaced ingestion fell from
40–60 to 21–43 µs p50 under uniform overwrites, 41–65 to 11–12 µs for new
keys and 20–68 to 10–21 µs for the window. On the settled states, blocks per
lookup fell from 4.0 to 1.55 (uniform present), 2.69 to 1.03 (new keys) and
4.0 to 0.03 (absent), and get p50 from 34.2 to 8.8 µs, 25.8 to 8.5 µs and
about 33 to 0.1 µs. Worker CPU rose 2–5%, unpaced writer throughput fell
1.5–6%, and one of the two unpaced new-key rounds with filters had one flush
refused at the 64-run limit (none without). Prefix scans of the same single
consolidated run were within 4%.

Building filters costs 8 bytes per distinct key while a run is built (the
hashes, reported as `filter_build_bytes`; the buffer doubles explicitly, so it
stays under 16 bytes per key, and in a compaction within what the workspace
estimate leaves of `max_workspace_bytes`) and one hash per record. Flushing
json records cost the same per record with and without filters in two
alternating sessions (291–321 against 291–306 ns); 8-byte records cost 20–30%
more (267–282 against 338–350 ns). The same sessions varied by up to 60% on
rows that filters do not touch. Those flush rows are single timed passes; the
repeated measurements under "Filter construction cost" put the cost at
5–17%.

### Filter construction cost

`scripts/validate.sh filtercost` (Milestone 5J) times the filter steps alone,
copied from `src/run/filter.rs`, on the sorted records that `RunBuilder`
encodes, and times a memtable flush and a `RunBuilder` build of the same
records as medians of 15 rounds (1 MiB) or 5 (16 MiB). Builds without a
filter come from the same benchmark on a copy whose builder skips the filter
(`RunBuilder::add` without `key_hashes.add`, `finish` with no filter, run with
`TESSERA_SOURCE=<copy>`), alternated with the real one in two sessions. ns per record:

| records | flush without | flush with | build without | build with | hash | hash, tracking and growth | finalize | finalize, one line per key |
|---|---|---|---|---|---|---|---|---|
| 8-byte, tree keys, 54k | 111–113 | 117–123 | 99–100 | 105–112 | 5.4 | 5.5 | 2.1 | 2.9 |
| 8-byte, tree keys, 858k | 129–136 | 142–162 | 99–102 | 108–113 | 5.4 | 5.5 | 5.0 | 3.2 |
| 32-byte, 49-byte shared-prefix keys, 369k | 188–193 | 201–205 | 248–252 | 262–267 | 9.8 | 9.9 | 4.2 | 3.0 |
| json, tree keys, 174k | 192–200 | 209–211 | 174–177 | 182–185 | 6.8 | 7.0 | 3.4 | 3.0 |
| 4 KiB values, 4k | 2914–3022 | 3071–3157 | 2498–2580 | 2521–2579 | 5.6 | 5.8 | 2.2 | 2.8 |
| 8-byte, random 16-byte keys, 699k | 147–154 | 159–165 | 118–122 | 92–134 | 20–37 | 19–37 | 4.9 | 3.1 |

- The filter adds 8–15 ns per record: 5–12% of a build and 5–17% of a flush
  for records up to json size, and nothing measurable for 4 KiB values. The
  steps timed alone sum to 8–14 ns, which accounts for the difference. The
  20–30% reported for 5I came from single timed passes.
- SipHash-1-3 is half to two thirds of it, 5.4 ns for an 18-byte key and
  9.8 ns for a 49-byte one. Even a hash that cost nothing would save at most
  5% of a small-record build, so no alternative was evaluated and no
  dependency added. (The 20–37 ns for random keys is the cost of reaching keys
  scattered in memory, which a build pays once whichever step reads them.)
- Distinct-key tracking and buffer growth together cost about 0.1–0.5 ns.
- Finalizing costs 2 ns per key while the filter fits in cache and 4–5 ns at
  0.5–1 MiB. Placing a key's seven probes in one 64-byte line would save about
  2 ns per key at that size, about 2% of a build, so the layout is unchanged.
- False positives, probing each stored key with a zero byte appended: 0.74–
  0.83% for tree, shared-prefix and random binary keys, against 0.82% in
  theory for 10 bits and 7 probes.

Larger runs, from the engine's reports (one flush of a 256 MiB memtable with a
1 GiB target run, a snapshot, an overwriting flush, and one compaction of both
into one run), in MiB; peaks are the live Rust heap above the start, counting
a reallocation as a copy:

| values | records flushed | stored | filter | hash buffer | flush peak | records compacted | stored | filter | hash buffer | compaction peak | snapshot retains |
|---|---|---|---|---|---|---|---|---|---|---|---|
| 8 bytes | 1,743,087 | 22.1 | 2.08 | 16.0 | 70.4 | 2,614,631 | 32.2 | 3.1 | 32.0 | 96.6 | 22.1 + 2.1 |
| 100 bytes | 1,091,201 | 29.1 | 1.30 | 16.0 | 77.6 | 1,636,802 | 43.6 | 2.0 | 16.0 | 124.6 | 29.1 + 1.3 |
| 4 KiB | 63,280 | 63.9 | 0.08 | 0.5 | 129.7 | 94,920 | 95.9 | 0.1 | 1.0 | 227.5 | 63.9 + 0.1 |

- The hash buffer is the largest transient filter cost: 8 bytes per distinct
  key, under 16 after doubling, here 6–12 times the filter it builds and
  13–33% of the flush and compaction peaks for records up to 100 bytes. It
  is freed when each run is finished, reported as `filter_build_bytes`, and in
  a compaction held within what the workspace estimate leaves of
  `max_workspace_bytes` (61 MiB under the defaults here).
- Flushes built 5.8–7.8 million records a second and compactions read 5.3–6.9
  million; a snapshot retained exactly the stored and filter bytes of the run
  it held, and nothing after it was dropped.
- Runs of more than 2^24 distinct keys are not built here (they would need a
  memtable of several GiB); unit tests cover the filterless fallback.

### Sustained operation (assessment)

`scripts/validate.sh operate` (Milestone 5K; about 3 minutes, `diagnostics`
feature) drives the engine and its background worker with 4 MiB memtables and
the default limits and compaction options. Overwrite workloads start from
200,000 settled tree keys. A sampler thread reads one key every 2 ms and the
run shape every 50 ms. `operate concurrency` and `operate pressure` run one
section alone. Two sessions after the change below, one before; within-session
comparisons only.

#### Sustained workloads

1.2 million puts (60,000 of 4 KiB over 20,000 keys), unpaced, one writer that
flushes when the memtable is full. "Sustainable" divides the puts by write time
plus the catch-up after the last put. "Written per flushed byte" counts the
flush and every compaction output.

| values | workload | k puts/s | sustainable | put p99 µs | put max ms | worker CPU s | written per flushed byte | mean/max overlap | catch-up s | get p50/p99 µs while writing | runs after |
|---|---|---|---|---|---|---|---|---|---|---|---|
| 8-byte | new keys | 2,057–2,086 | 1,676–1,926 | 0.4–0.5 | 4–5 | 0.50–0.59 | 2.9–3.1 | 6–8 / 10–17 | 0.05–0.13 | 4–19 / 248–838 | 3–4 |
| 8-byte | uniform overwrite | 2,109–2,193 | 2,075–2,157 | 0.4–0.5 | 0 | 0.41–0.42 | 2.4 | 5.6 / 9 | 0.01 | 34–36 / 110–208 | 4 |
| 8-byte | hot: 90% to 1% of keys | 3,155–3,333 | same | 0.3 | 0 | 0.02 | 1.7 | 2.5 / 4 | 0 | 20–24 / 51–59 | 9 |
| json | new keys | 1,693–1,823 | 1,199–1,254 | 0.4–0.5 | 4–5 | 0.87–0.93 | 3.1 | 11 / 25–26 | 0.29–0.30 | 12–15 / 50–70 | 3 |
| json | uniform overwrite | 1,770–1,915 | 1,644–1,764 | 0.4 | 4–5 | 0.63–0.67 | 2.3 | 9 / 14–16 | 0.05 | 25–30 / 67–82 | 4 |
| json | hot: 90% to 1% of keys | 3,116–3,275 | 2,868–3,018 | 0.3 | 4–5 | 0.06 | 2.2 | 3–3.5 / 6 | 0.03 | 14–17 / 37–50 | 12 |
| 4 KiB | new keys | 355–375 | 147–154 | 0.5–0.6 | 3 | 0.38–0.39 | 2.9 | 13 / 25–28 | 0.23–0.24 | 5–6 / 14–20 | 5 |
| 4 KiB | uniform overwrite | 338–353 | 158–164 | 0.5–0.6 | 3 | 0.35–0.36 | 2.6 | 13 / 23–25 | 0.20 | 14–17 / 31–44 | 6 |

- Foreground writes run at memtable speed (p50 0.21–0.33 µs). The worst put
  is a flush, 2–5 ms (build 2.2–4.1 ms per 4 MiB table; the longest
  publication 0.1–4.5 ms, highest beside a busy reader). No flush was refused at the default 64 runs.
- The worker kept up for small and json values: catch-up after 1.2 million
  unpaced puts took 0.01–0.30 s, so sustainable throughput is 70–100% of the
  foreground rate. 4 KiB values arrive at 1.4 GB/s of payload unpaced. There
  the worker fell behind (overlap up to 28 runs) and the sustainable rate is
  about 43% of the foreground rate.
- Rewrite stayed at 0.7–2.1 compacted bytes per flushed byte (1.7–3.1 written
  in all). Hot-key workloads rewrite least, because each flush is mostly
  duplicates.
- Reads during unpaced ingestion pay for the overlap; their p99 is dominated
  by sampling on an oversubscribed 2-CPU machine (writer, worker and sampler).
- Run bytes plus filters at the end: 2.9–25 MiB for small and json values.
  The live Rust heap peaked 14–63 MiB above the start (the memtables are 8 MiB
  of that), and RSS was 33–146 MiB for the whole benchmark process, which
  includes the value pools and freed but unreturned memory.

#### Point reads held the state lock while decoding

Before this milestone, `Engine::get` decoded run blocks while holding the
state lock shared, about 10–20 µs per get. A writer waits for that lock, so
one busy reader cut a writer from 1.95–1.98 million puts per second to
0.76–0.79 million, with put p99 rising from 0.4 to 29 µs. `get` now takes the
run list under the lock and decodes after releasing it, which is correct
because runs are immutable and shared. Same session, two pairs:

| writers, readers | before: k puts/s, put p99 µs, k gets/s | after |
|---|---|---|
| 1, 1 | 760–791, 28.7–29.3, 26–27 | 1,243–1,482, 0.5, 31–35 |
| 1, 2 | 593–609, 31.6–32.2, 32–34 | 932–973, 0.5, 59–62 |
| 2, 2 | 912–1,045, 0.8, 34–35 | 1,186–1,567, 0.7, 39–43 |

The json uniform workload with a busy reader in the same thread mix went from
800 to 1,066–1,128 k puts/s (8-byte: 672 to 1,427–1,472), put p99 from 24–42
to 0.4–0.6 µs. Single-threaded get latency did not change (19.5–19.6 µs settled,
9.8 µs on one run, both builds). What remains is CPU: on 2 CPUs a writer, a
reader and the worker share two cores.

#### Concurrency (after the change)

600,000 json uniform overwrites from 200,000 settled keys, split between
writers; readers get as fast as they can.

| writers | readers | worker | k puts/s | put p99 µs | k gets/s | process CPU s |
|---|---|---|---|---|---|---|
| 1 | 0 | off | 1,886–1,924 | 0.4 | | 0.32 |
| 1 | 0 | on | 1,808–1,960 | 0.4 | | 0.56–0.61 |
| 2 | 0 | on | 1,780–2,093 | 0.8–0.9 | | 0.58–0.67 |
| 4 | 0 | on | 1,988–2,156 | 0.7–0.8 | | 0.56–0.60 |
| 1 | 1 | on | 1,192–1,426 | 0.4–0.5 | 28–35 | 0.84–1.00 |
| 1 | 2 | on | 841–950 | 0.5–0.6 | 48–50 | 1.26–1.43 |
| 1 | 0, snapshot full scans | on | 1,377–1,477 | 0.4–0.5 | 6–7 scans of 70–76 ms | 0.96–0.97 |

- Writes do not scale with writers: every write takes the exclusive lock, and
  2 or 4 writers reach the throughput of one (within 10%). That costs little
  here, because one writer already spends 0.2 µs per put. The lock limits
  write throughput only if puts get cheaper or there are more cores than 2.
- The background worker takes 0.25–0.3 CPU s per 600,000 puts, and with it a
  lone writer is within 5% of no worker.
- Snapshot scans of all 200,000 keys run beside a writer without blocking it.
  The writer slows 25–30% for CPU, and its p99 is unchanged. Back-to-back
  full scans through a `ReadGuard` let a writer in about once per scan (as
  documented) and are not measured.

#### Limits, backpressure and recovery

| scenario | result |
|---|---|
| 8 runs allowed, 1 MiB memtables, 600,000 new json keys | 13–17 of 133 flushes refused, 0.36–0.41 s spent waiting for compaction, 789–850 k puts/s; no compaction failed |
| one snapshot of the settled 3.9 MiB state, 2 MiB retention limit, 16 runs | compaction refused once retention reached 2.1 MiB (2.0 run + 0.13 filter); flushes refused at 16 runs after 532,445 puts. Dropping the snapshot: flushes alone recovered in 45 ms (12 retries); 200,000 more puts with 1 refused flush; nothing retained |
| dropping the engine with 18 overlapping runs, the worker busy, and a snapshot scan running | dropped in 7–10 ms (the compaction in progress is published); the scan finished all 400,000 records |

A snapshot retains at most the state it saw: at the 8 MiB limit the same
scenario never blocked. Before this milestone, the recovery in the second
row did not happen. A refused flush did not ask the worker for a pass, and
the pass that compaction refused was not retried after the snapshot was
dropped, so a writer that only retries flushes stayed refused until something
called `wait_for_compaction`. A refused flush now requests a pass (one line;
`a_flush_refused_after_retention_blocked_compaction_recovers_on_its_own`).

A refused flush drops the runs it built, and the retry builds them again; in
the first row that is up to 17 extra builds of a 1 MiB table.

#### Decoded-block caching

A simulated LRU of decoded 16 KiB blocks, replaying the blocks that filtered
point lookups decode (only runs holding the key; filter false positives left
out), 30,000 lookups after a 20% warm-up:

| state | lookups | blocks in runs | hit at 1 MiB | 4 MiB | 16 MiB |
|---|---|---|---|---|---|
| settled, 3–4 runs (6.6–6.9 MiB stored) | uniform | 1,922–1,996 | 3.2–3.3% | 12–13% | 51–53% |
| | 90% to 1% of keys, scattered | | 4.9–5.1% | 19% | 68–70% |
| | 90% to 1% of keys, contiguous | | 90% | 91% | 95% |
| one run (3.9 MiB stored) | uniform | 1,146 | 5.5% | 22% | 89% |
| | scattered hot keys | | 8.2% | 32% | 98% |
| | contiguous hot keys | | 91% | 92% | 98% |

Every decoded block holds about 100 json records, so a cache hits only when
lookups cluster in key order, or when it holds most of the decoded data:
16 MiB of decoded blocks for 3.9 MiB stored. That gives up the compression
that is the engine's reason to exist. A hit would save one decode (about
10 µs of a 10–20 µs get). It would also need shared ownership of decoded
blocks across readers, synchronization on every lookup and eviction. Not
implemented: it pays only for key-clustered hot sets, and those can be
measured if a workload needs them.

#### Deliberate consolidation

`Engine::compact` with `max_overlap` 1 on the settled uniform state (3–4 runs):

| | before | after |
|---|---|---|
| get p50 / p99 µs | 19.5–19.6 / 31.4–31.5 | 9.8 / 13.4–13.7 |
| blocks per get | 1.68–1.74 | 1.00 |
| gets/s, 1 and 2 threads | 57–59k, 112–116k | 101k, 197k |
| 20-key / 4,000-key scan µs | 33.9–43.4 / 802–804 | 11.7–11.8 / 371–374 |

It cost one compaction: 6.6–6.9 MiB read, 3.9 MiB written, 0.08 CPU s and a
heap peak 10.1 MiB above the start. That is about 0.1 s of the worker's 0.63 s
for the ingestion before it. For a read-heavy phase after ingestion, one
consolidation roughly doubles point-read throughput and halves long-scan time.
The existing `Engine::compact` with `max_overlap` 1 already does it, so no new
API or scheduler is needed. In one session the worker happened to leave one
run, and consolidation had nothing to do.

### Checkpoints

`scripts/validate.sh checkpoint` (Milestone 5L) loads an engine (16 MiB
memtables, default limits, the worker), checkpoints it, reads the files and
recovers them, then measures a writer of 1.5 million json overwrites and a
reader beside checkpoints taken back to back. It runs once into the
container's tmpfs and once into the cache volume on the VM's disk; the
benchmark removes its directory. Recovery reads files the page cache already
holds. Two sessions agree within 15% except where noted.

| values | keys | payload MiB | runs MiB | checkpoint MiB | checkpoint s | k records/s | heap peak MiB | recover s | recover MiB/s | recover heap MiB |
|---|---|---|---|---|---|---|---|---|---|---|
| json | 1,000,000 | 98.4 | 18.3 (14 runs) | 18.3 | 0.27–0.29 | 3,470–3,680 | 34.8 | 0.14 | 127–128 | 25.0 |
| 8-byte | 2,000,000 | 49.6 | 24.6 (12 runs) | 24.6 | 0.34–0.36 | 5,600–5,840 | 68.2 | 0.15 | 163–166 | 43.6 |
| 4 KiB | 50,000 | 196.2 | 50.5 (13 runs) | 50.5 | 0.17–0.20 | 255–297 | 49.3 | 0.08 | 628–637 | 50.6 |

- A checkpoint is as large as the compacted runs: it rewrites the live keys
  into runs of the same format. Its time is decoding and re-encoding, not I/O:
  tmpfs and the VM disk differ by 0–15%. The VM disk's `fsync` is fast because
  the hypervisor acknowledges it from the host's cache, so these times say
  nothing about a real device's sync latency.
- The heap peak beside the engine is one run under construction (up to the
  64 MiB input target), its key-hash buffer and the scan's decoded blocks: 35–68 MiB,
  bounded by `target_run_len`, not by the size of the database.
- Recovery spends 3–6 ms reading (cached) files; the rest is the full run
  check and rebuilding the filters, 128–166 MiB/s of stored runs for small
  and json values. Its heap is the runs and filters it returns.

| checkpoints | k puts/s | put p99 µs | k gets/s | get p99 µs | checkpoints | compactions (failures) | most retained MiB |
|---|---|---|---|---|---|---|---|
| none | 1,154–1,376 | 0.6–0.7 | 40–42 | 42–43 | | 5–8 (0) | 0 |
| back to back | 614–874 | 0.7–0.8 | 29–31 | 43–52 | 3–4 of 555–707 ms | 6–10 (0) | 37.8–45.0 |

- Writes never wait for a checkpoint: the put p99 is unchanged. Throughput
  falls 30–52% because the checkpoint's re-encoding takes a core on a 2-CPU
  machine already running a writer, a reader and the worker. Checkpoints back
  to back are the worst case.
- Under load a checkpoint takes about twice as long as idle (0.56–0.71 s).
  Compaction keeps running; what it replaces stays allocated for the
  checkpoint's snapshot, up to 45 MiB retained here, with 16 MiB memtables and
  18 MiB of runs.

### Durable writes

These are the Milestone 5M results; 5N replaced the benchmark with the one
under [Group commit](#group-commit), so they are not rerun as written.

`scripts/validate.sh durable` (Milestone 5M) ran one writer of uniform json
overwrites over a million keys (16 MiB memtables, the worker) for at most
300,000 puts or 20 s, RAM-only and durable, then recovers the database from
its log and from a checkpoint, then puts a durable writer beside a reader
with and without durable checkpoints back to back (2 MiB memtables so that
flushes and compactions happen). It runs once into the container's tmpfs and
once into the cache volume on the VM's disk; the benchmark removes its
databases. Ranges are two sessions.

| mode | k puts/s | put p50 µs | p99 µs | p99.9 µs | log MiB | segments |
|---|---|---|---|---|---|---|
| RAM-only (either) | 1,768–1,854 | 0.2 | 0.7–0.8 | 1.3–1.4 | | |
| durable, tmpfs, 64 MiB segments | 880–911 | 0.8 | 1.4–1.5 | 4.9–5.2 | 37.8 | 1 |
| durable, tmpfs, 1 MiB segments | 883–897 | 0.8 | 1.4 | 5.1–5.4 | 37.8 | 38 |
| durable, VM disk, 64 MiB segments | 6.7 | 146–147 | 185–193 | 365–424 | 16.9 | 1 |
| durable, VM disk, 1 MiB segments | 6.7 | 147 | 185–188 | 381–390 | 16.9 | 17 |

- One durable write is one `write` and one `fdatasync` on the caller's
  thread. On tmpfs, where the sync does nothing, encoding, checksums and the
  two system calls add 0.6 µs a write and halve throughput. On the VM disk the
  sync is the cost: about 150 µs, so about 6,700 writes a second from one
  writer, since there is no group commit. Real devices will differ either way.
- Rotation costs nothing visible: 38 segments instead of one change nothing.
- Every row's maximum (17–19 ms, RAM-only too) is a 16 MiB flush the
  benchmark runs inline when the memtables are full.
- RAM-only writes, A/B against the 5L commit with a separate microbenchmark
  (one thread, 1M puts of 100-byte values, seven runs each, twice): 2,837–2,909
  k puts/s against 2,937–2,972 k, 1.5–4% slower, near the run-to-run spread of
  about 2%. The same tree without the test for a log matched 5L (2,929–2,933
  against 2,942), so the cost is that one branch on `OnceLock::get` and the
  code layout it changes, not the memtable's split admission check.

| recovery | s | log MiB | log MiB/s | records | heap peak MiB |
|---|---|---|---|---|---|
| tmpfs, from the log alone | 0.13–0.14 | 37.8 | 277–281 | 300,000 replayed | 20.0 |
| tmpfs, from the checkpoint | 0.04 | 0.0 | | 0 replayed | 8.2 |
| VM disk, from the log alone | 0.06 | 16.6–16.8 | 291–298 | 132–133 k replayed | 16.8 |
| VM disk, from the checkpoint | 0.02 | 0.0 | | 0 replayed | 4.0 |

- Replay runs at about 2.2 million records a second, reading files the page
  cache holds; its heap is the memtables and runs it rebuilds, plus one 64 KiB
  read buffer and one record buffer.
- The durable checkpoint between the two opens took 0.04–0.08 s, wrote 2.5–5.2
  MiB and pruned the one full segment, leaving only the header of the active
  segment: the log shrinks to nothing a checkpoint covers.

| disk | checkpoints | k puts/s | put p99 µs | k gets/s | get p99 µs | get max ms | taken | pruned | compactions (failures) | most log MiB |
|---|---|---|---|---|---|---|---|---|---|---|
| tmpfs | none | 663–711 | 1.5–1.8 | 458–512 | 10.8–13.6 | 4.0 | | | 11 (0) | 37.3–37.5 |
| tmpfs | back to back | 373–458 | 1.5–1.8 | 266–319 | 11.6–15.0 | 7.0–8.0 | 32–37 | 2 | 13–14 (0) | 17.9–19.5 |
| VM disk | none | 5.1–6.4 | 373–538 | 991–1,091 | 11.5–13.6 | 1.0–4.2 | | | 2–4 (0) | 12.9–16.1 |
| VM disk | back to back | 5.7–5.9 | 470–488 | 895–1,110 | 12.7–16.9 | 2.4–5.3 | 87–207 | 0 | 3 (0) | 14.4–14.8 |

- Reads do not wait for syncs. A durable write reserves its sequence number
  under the state lock, syncs without it, and publishes under it again.
  The first version held the lock through the sync: on the VM disk (16 MiB
  memtables) the reader then managed 1.9–3.3 k gets a second with a 2.0 s
  worst get, because the writer, given priority, retook the lock after every
  sync. After the change the same run read 1,877 k gets a second with a worst
  get of 0.8–6 ms.
- Checkpoints back to back cost the tmpfs writer 30–45% (CPU on two cores) and
  the disk writer little (it is waiting on syncs). They keep the log at about
  half its size; on the VM disk it never filled a 16 MiB segment, so nothing
  was pruned.
- Compaction ran beside the log with no failures.

### Group commit

`scripts/validate.sh durable` (Milestone 5N) runs W writer threads of
uniform json overwrites over a million keys (16 MiB memtables, the
worker), 200,000 writes in all or 10 s, beside one reader of uniform gets:
RAM-only; durable with batches of one write (`max_batch_records: 1`, a sync
per write, as in 5M); and durable with group commit (64 writes or 1 MiB a
batch, 16 MiB of queue). Then group commit with 1 MiB segments, with durable
checkpoints back to back, and recovery. Into the container's tmpfs, then the
cache volume on the VM's disk. Ranges are two sessions. The VM disk is a
virtual disk behind Colima's hypervisor, not a characterised device: its
sync times say nothing about NVMe or any real drive, and whether its
`fdatasync` reaches stable media is not known.

VM disk:

| mode | W | k writes/s | p50 µs | p99 µs | syncs/write | mean batch | writer CPU µs/write | k gets/s |
|---|---|---|---|---|---|---|---|---|
| RAM-only | 1 / 2 / 4 | 1,325–1,361 / 1,511–1,519 / 1,522–1,783 | 0.2–0.3 | 1.3–1.7 | | | 0.6–0.7 | 393–987 |
| sync per write | 1 | 7.7–7.9 | 123–126 | 166–182 | 1.000 | 1.0 | 9.5–10.3 | 2,755 |
| sync per write | 2 | 7.9–8.1 | 242–247 | 299 | 1.000 | 1.0 | 10.9 | 2,955 |
| sync per write | 4 | 7.2–7.9 | 493–530 | 611–1,300 | 1.000 | 1.0 | 12.8–14 | 2,922 |
| group commit | 1 | 7.8 | 121–126 | 166–255 | 1.000 | 1.0 | 9.7–9.9 | 2,858 |
| group commit | 2 | 11.3–11.8 | 135–139 | 293–393 | 0.667 | 1.5 | 8.3 | 2,150 |
| group commit | 4 | 18.9–19.0 | 239–242 | 318–384 | 0.399 | 2.5 | 6.5 | 1,543 |
| group commit | 8 | 35.4–36.2 | 246–248 | 336–434 | 0.196 | 5.1 | 5.2 | 1,377 |
| group, 1 MiB segments | 4 | 18.0–18.7 | 242 | 345 | 0.399 | 2.5 | 6.5 | 1,599 |
| group, checkpoints back to back | 4 | 12.5–12.7 | 254 | 2,400–2,461 | 0.377 | 2.7 | 8.0 | 1,808 |

tmpfs (the sync does nothing; this is the software cost):

| mode | W=1 | W=2 | W=4 | W=8 | p99 µs at W=4 | syncs/write at W=4 / 8 |
|---|---|---|---|---|---|---|
| RAM-only, k writes/s | 1,201–1,320 | 1,287–1,432 | 1,553–1,714 | | 1.2 | |
| sync per write | 490–572 | 464–563 | 328–338 | | 26 | 1.000 |
| group commit | 500–617 | 500–545 | 444–448 | 288–297 | 30 | 0.94–0.95 / 0.62 |
| group, 1 MiB segments | | | 411–443 | | 33 | 0.95 |
| group, checkpoints back to back | | | 228–268 | | 35 | 0.94 |

- On the VM disk a sync costs about 120 µs, and with a sync per write extra
  writers only queue: 7–8 k writes a second at any W, p50 rising with W.
  Group commit turns the waiting into batching: the mean batch is about
  two thirds of W, syncs a write fall to 0.2 at W=8, and throughput grows 4.6× to
  35–36 k writes a second at the same p50 as one writer's two syncs. A
  writer that arrives while a sync is in progress waits for it and then the
  next, so p50 settles near two syncs.
- One writer pays nothing for batching: 7.8 k writes a second, one sync and
  batch a write, the same latency as a sync per write. It never waits for
  company; there is no delay setting.
- Writer CPU a write (at the kernel's 10 ms tick) falls from 10–14 µs with a
  sync per write to 5.2 µs at W=8: the sync's own CPU is shared.
- On tmpfs batching rarely happens (syncs are free, so the leader seldom
  finds anyone queued): group commit and a sync per write are within noise,
  and both lose to RAM-only by the encoding, checksum and system calls.
- Rotation: 1 MiB segments (about 24 in a row) cost 1–5% at W=4 on the
  disk and 1–8% on tmpfs; a batch crossing a segment syncs the old segment
  before creating the next.
- Checkpoints back to back (500–580 in a row on the disk) cost a third of
  the disk throughput and raise p99 to 2.4 ms: the checkpoint's own syncs
  and its pruning, which waits for the batch being published.
- The reader keeps its throughput beside the syncs (1.4–3.0 M gets a second
  on the disk; it slows as writers take more CPU, as RAM-only does). Its p99
  stays at 10 µs.
- Queue memory peaked at 1–2 KiB (logical encoded bytes plus 128 a request,
  not RSS); the 16 MiB bound was never approached by these 4 KiB values.
- Recovery is unchanged: 37.8 MiB of log in 0.13–0.14 s on tmpfs (274–287
  MiB/s, 20 MiB heap) and 8.3 MiB in 0.02 s on the disk; from a checkpoint
  0.01–0.04 s.
- RAM-only writes, A/B against the 5M commit (the same microbenchmark as
  5M, one thread, 1M puts): unchanged, within run-to-run spread; the RAM path
  does not touch the group code.
- One durable writer on tmpfs, A/B against 5M (paired runs): 743 against
  905 and 871 against 1,062 k puts a second, about 0.2 µs a write more,
  from the queue's mutex, the per-request allocation and the outcome table.
  On a disk that is lost in the sync. The first version also signalled the
  condition variable on every write and cost twice that; it now signals only
  when someone sleeps.
- Admission in arrival order (added in review, so that a large write is not
  starved): a first version encoded each record only after admission,
  taking the queue's mutex twice a write, and measured 30.3–31.0 k writes a
  second at W=8 on the disk (two runs). The version kept encodes first and
  re-encodes only after a wait; it measured 29.6–34.7 k in three runs (one
  in 5N, two after 5O, whose write path is the same). The
  difference is within the W=8 spread on this disk, so the cost of the
  second lock is not established; the single-lock path was kept since it
  does no more work. W=1–4 were unchanged throughout.

#### Recovery hardening (5O)

One regression run of the same benchmark after Milestone 5O (open syncs
the last segment, recovery opens one segment at a time and bounds its file
reads; the write path is unchanged), plus one more disk run:

| | 5O | 5N ranges above |
|---|---|---|
| RAM-only, tmpfs, W=1 / 4, k writes/s | 1,319 / 1,648 | 1,201–1,320 / 1,553–1,714 |
| group commit, tmpfs, W=1, k writes/s | 620 | 500–617 |
| group commit, VM disk, W=1, k writes/s (p50 µs) | 6.1–7.9 (125–129) | 7.8 (121–126) |
| group commit, VM disk, W=4 / 8, k writes/s | 18.5 / 29.6–32.2 | 18.7–19.0 / 35.4–36.2 before the review, 34.7 after |
| reader beside 8 disk writers, k gets/s | 1,168–1,203 | 1,377 |
| recovery from the log, tmpfs, s (MiB/s) | 0.14 (276) | 0.13–0.14 (274–287) |
| recovery from the checkpoint, s | 0.01–0.04 | 0.01–0.04 |

The disk rows vary by about 20% between runs of the same code (W=1 fell
to 6.1 k in one run with p95 351 µs, a disk hiccup, not a code change).
Nothing here is outside that spread, and the one extra sync at open is not
visible in recovery time.

### Automatic flushing (5R)

Writes now flush the memtables themselves when they are full. One A/B
harness, built against the 5Q commit (`3fb2c9f`), where the caller flushes
on `BudgetExceeded` and retries, and against 5R, where that arm is never
taken: W writers put 8-byte keys and 100-byte values (half random, so
about 0.28 stored per payload byte) over 100,000 keys, beside one reader
doing point gets and, every 16th read, a 20-key prefix scan through a read
guard. One run per flush (`target_run_len` 1 GiB), no compaction, tmpfs,
release build, the bounded container. `fits` has a 1 GiB budget (400,000
writes, no flush); `flushing` and `durable` a 4 MiB budget (400,000 and
100,000 writes; durable with default group commit, syncs on tmpfs). The
harness was not kept.

| row | W | 5Q k writes/s | 5R k writes/s | 5Q runs (caller flushes) | 5R runs | 5Q max ms | 5R max ms | scan p99 µs, 5Q / 5R |
|---|---|---|---|---|---|---|---|---|
| fits | 1 | 1,271–1,314 | 1,223–1,288 | 0 | 0 | 0.1–0.2 | 0.06–0.2 | 8 / 8 |
| fits | 4 | 2,077–2,283 | 2,009–2,181 | 0 | 0 | 6–10 | 6 | 6–7 / 6–9 |
| flushing | 1 | 1,627–1,647 | 1,605–1,626 | 20 (20) | 20 | 4.9–5.1 | 4.9–5.3 | 166 / 164–168 |
| flushing | 2 | 1,902–1,964 | 1,888–1,894 | 20–22 (23–24) | 20 | 15 | 9–11 | 184–191 / 177 |
| flushing | 4 | 1,285–2,416 (7 runs) | 1,949–2,512 (8 runs) | 20–24 (20–25) | 20 | 14–35 | 10–19 | 200–1,339 / 177–201 |
| durable | 1 | 503–506 | 496–522 | 5 (5) | 5 | 4.8–4.9 | 4.8–5.0 | 37–39 / 37–39 |
| durable | 4 | 189–381 (7 runs), median 346 | 199–396 (8 runs), median 304 | 5–7 (5–9) | 5 | 11–29 | 9–35 | 37–74 / 36–54 |
| durable | 8 | 141–318 (7 runs), median 182 | 199–301 (8 runs), median 228 | 6–11 (7–16) | 5 | 10–44 | 8–20 | 60–107 / 36–48 |

- The fast path is unchanged in structure (one exclusive lock hold; the
  slow path is a separate cold function), and `fits` is within the spread
  between passes; a 2–3% difference in the medians is not established.
- With several writers, 5Q callers that found the table full each flushed,
  so later ones flushed small tables: up to 24 flushes for 20 tables, and
  more runs. In 5R writers that run out of room together share one flush:
  always 20 (5 durable), one per full table.
- Stored bytes flushed are in the same ranges (11.4–11.8 MiB; 2.4–3.0 MiB
  durable), and so are the peak memtable charge (at most 8.0 MiB, two
  budgets), the process peak (12–38 MiB) and the share of writer time in
  writes of 100 µs or more (the writes that flush, 12–77% by row; the flush
  is inside a write in both versions).
- The contended rows vary by up to a factor of two between passes in both
  versions (8 writers and a reader on 2 CPUs). Across them 5R is at least
  level, with lower maxima and scan tails. Durable W=8 had a higher median
  latency in 5R (11–21 µs in 7 of 8 passes against 1.5–7.6 µs) at equal
  or higher throughput; batch sizes were not recorded.
- A first 5R version built the durable flush while still leading its
  batch, so no batch committed during the build: W=4 measured 347–356
  against 376–381. The leader now freezes the table, finishes its batch,
  and builds and publishes the flush after the next batch may start.
- No disk rows were run: the change does not touch the sync path, and the
  disk rows write to the host SSD.

The older benchmark modes drove flushes from `BudgetExceeded`. In 5R they
were changed to rely on the engine's flushes; results recorded above for
those modes predate 5R and were not re-run.
