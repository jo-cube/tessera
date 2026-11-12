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
| `durable` | 200,000 durable writes from 1, 4 and 8 writers | group commit: throughput, latency and writes per sync |
| `recovery` | checkpoints of 500,000 records, recovery, and a reopen replaying 100,000 logged writes | checkpoint, recovery and replay rates |

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
nothing, and consolidation removes the case entirely. Deferred.

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
