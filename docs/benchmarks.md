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
