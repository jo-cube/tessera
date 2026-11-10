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

Same reference environment, three passes. Times differ between passes by a
median of 5.5% and by up to 41%, the largest being `json` traversal. Freezing is
at or below the timer's resolution, about 40 ns, and allocates nothing.

| dataset, random order | payload MiB | charged MiB | requested MiB | RSS MiB | RSS after overwrite | RSS/charged |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| json | 17.1 | 38.2 | 27.4 | 32.0 | 33.3 | 0.84 |
| tiny | 10.6 | 63.0 | 36.1 | 52.1 | 55.4 | 0.83 |
| large | 16.0 | 16.5 | 16.3 | 16.4 | 16.4 | 0.99 |

| ns per operation | sequential insert | random insert | random overwrite | random delete | get hit | get miss | traversal per record |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| json | 216 | 744 | 556 | 542 | 475 | 469 | 8.6 |
| tiny | 216 | 882 | 669 | 658 | 606 | 618 | 13.4 |
| large | 286 | 512 | 451 | 149 | 109 | 104 | 2.1 |

| flush | target MiB | runs | stored/payload | ms | payload MiB/s | peak collected MiB | peak streamed MiB |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| json | 64 | 1 | 0.202 | 34 | 498 | 7.6 | 7.6 |
| json | 4 | 5 | 0.202 | 34 | 502 | 4.5 | 1.8 |
| tiny | 64 | 1 | 0.547 | 58 | 183 | 14.0 | 14.0 |
| tiny | 4 | 5 | 0.547 | 58 | 185 | 7.5 | 3.4 |
| large | 64 | 1 | 0.507 | 12 | 1329 | 24.4 | 24.4 |
| large | 4 | 5 | 0.507 | 12 | 1364 | 10.1 | 4.1 |

- Measured RSS growth stayed at or below the charge in all six insert cases,
  including after overwriting. The margins are small: `large` reached 16.5 MiB
  against 16.5 charged and `tiny` in sorted order 62.0 against 63.0. That is an
  observation on these datasets and this allocator, not a limit. The 128-byte
  entry charge is calculated for a worst-case `BTreeMap` occupancy that no
  dataset here reaches.
- Amplification is a small-record problem. Requested memory is 1.02 times the
  payload at 4 KiB values, 1.6 to 1.8 at 100 bytes and 3.4 to 4.0 at 26 bytes.
- An insert makes 2.13 to 2.17 allocations: its key, its value and a share of
  a `BTreeMap` node. An overwrite makes one, a delete, a lookup and a traversal
  none, and a freeze none.
- Splitting costs nothing measurable in flush time. It does bound what is held: for `large`, the 24.4 MiB peak
  of one run falls to 10.1 MiB when all five runs are kept and 4.1 MiB when each
  is released as it is produced. These peaks are on top of the frozen table and
  leave out Zstandard's own context.
- Dropping a frozen table takes 11 ms for `json` and 28 ms for `tiny`, because
  each entry is freed individually.

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
