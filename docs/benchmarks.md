# Benchmarks

The benchmarks are one example, `examples/block_bench.rs`. Datasets are generated
from fixed seeds, so the bytes and probes are identical on every run. Timings
vary with the machine; compare configurations only within one environment.

Run them through `scripts/validate.sh`, which applies the memory, CPU, process
and time limits, and repeat a whole pass rather than trusting one:

```sh
mkdir -p target/bench
for pass in 1 2 3; do
  scripts/validate.sh geometry > "target/bench/geometry-$pass.log"
done
scripts/summarize_bench.py target/bench/geometry-*.log
```

`summarize_bench.py` takes the median of each cell across passes, reports how
far passes disagree, and fits the Zstandard decode model below.

The provisional run defaults, 16 KiB blocks and Zstandard level 3, follow from the
findings below. `RunBuilder::with_target_block_len` and an explicit `Codec` keep
every configuration available for experiments.

## `geometry`: block size and codec

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

### Findings

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

## `prefix`: seeks and prefix scans

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

### Findings

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

## Merged iteration

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

### Findings

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

## Latest-state reads

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

### Findings

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

## Key-range pruning

`block_bench ranges` splits 32 MiB of logical data across 1, 2, 4, 8 and 16 runs
of Zstandard at 16 KiB, with unique keys and with version chains, in the
`disjoint`, `partial` and `overlap` layouts above. It measures hits, misses,
short and medium prefixes and forward seeks through `RunSet`, using only API that
existed before runs stored a key range, so the same harness runs on older code.
Run it with `scripts/validate.sh ranges`.

### Findings

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

## Memtable

`block_bench memtable` measures three datasets through `Memtable` and
`FrozenMemtable`: `json` (173,536 records, about 100 bytes each), `tiny` (428,865
records with 8-byte values) and `large` (4,084 records with 4 KiB values). Run it
with `scripts/validate.sh memtable`. Each memory row is a fresh process.
`requested` is what Rust asks the allocator for, and RSS is the growth of the
process's resident memory, so it includes allocator rounding and headers. It is
a baseline, not a target. Times include the counting allocator.

### Findings

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

## Integrated engine

`block_bench engine` drives `Engine` with the `json` dataset (173,536 records,
17.1 MiB of payload), beside the memtable and run-set operations it is built on,
so the engine's own cost shows as the difference. Run it with
`scripts/validate.sh engine`. Contention rows sample every sixteenth write's
latency, on the reference environment's 2 CPUs.

### Findings

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

## Compaction

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

### Findings

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

## Snapshots

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

### Findings

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

## Retention under sustained snapshots

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

### Findings

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

## Where write time goes

`scripts/validate.sh profile` (about 40 s) attributes memtable and engine
cost by measuring components separately, since the bounded container has no
sampling profiler or hardware counters. Absolute times on the reference host
varied by more than twofold between runs, so only ratios within one run are
compared.

### Findings

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

## Compaction selection

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

### Where the work went

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

### Alternatives (same session, `max_overlap` 4 unless noted)

| alternative | effect |
|---|---|
| size-tiered: the fewest smallest runs that restore the allowance, then each next run no larger than all taken | rewrite 2.4–4.2 at every rate; compactor CPU down 55–80% when paced; settles at up to the allowed overlap |
| minimal: only the fewest that restore the allowance | rewrite up to 15.7: the large run soon becomes one of the fewest |
| earlier policy with `max_overlap` 8 | rewrite 0.8–4.3, but the window workload settles at 47 runs and reads load 4.8 blocks |
| size-tiered with `max_overlap` 3 or 2 | the lopsided merges return (60–95% of input) and rewrite reaches 7–18 |

Size-tiered selection is now the policy. It keeps every invariant of the
merge (inputs share the deepest key, so the overlap there falls and nowhere
rises), and needs no new state.

### Before and after

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

## Read amplification (investigation)

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

### Sustained ingestion

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

### Point lookups

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

### Prefix scans

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

### Consolidation

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

## Run key filters

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

## Filter construction cost

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
