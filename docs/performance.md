# Performance

These are the numbers behind the README's summary: Tessera 0.1 against
RocksDB 11.13.2 on two synthetic JSON datasets far larger than either
engine's memtables, on one machine. They show where Tessera stands, not what an
application will see; measure with your own workload before relying on them.

## Datasets and workloads

Both datasets use 16-byte keys: an 8-byte group ID and an 8-byte member
number, big-endian. Records are regenerated from seeds during each phase,
with bounded input buffers, so the harness holds no copy of the data.

- **A, grouped JSON, 8 GiB raw** (8,120,691 records). Groups of 20 to 1,956
  records share most of each JSON value (descriptions, attributes,
  preferences), so neighbouring records compress well together. Values
  average about 1,040 bytes.
- **B, weak-locality JSON, 4 GiB raw** (6,654,300 records). Groups hold 100
  records, and each value carries about 500 random hexadecimal characters,
  so records share little beyond their field names. Values average about
  630 bytes.

Each engine runs the same phases in its own process:

- **load:** every record, in a random order, in batches of 256 records, then
  a wait until background work has drained;
- **update:** half the records updated and 2% deleted, in a random order, in
  batches of 256, then drained;
- **get_1t / get_4t:** 200,000 random point reads of the updated state, 10%
  of them misses, from 1 or 4 reader threads.

Every engine executes the same deterministic traces, and the timings
include regenerating and permuting the input. After load and update a
correctness gate reads 20,000 sampled keys and compares each value with the
generator's; every gate passed, and no timed read returned a wrong value.

## Machine and configurations

- RocksDB 11.13.2 built with
  `make static_lib DEBUG_LEVEL=0 PORTABLE=1 USE_RTTI=1`.
- RocksDB stores its files on an 8 GiB APFS RAM disk, with the write-ahead
  log disabled and no syncs. Tessera writes nothing to disk in these phases.
- **Tessera:** default options: 64 MiB memtables, level ratio 10, four
  background threads on this machine, Zstandard with 1 KiB stored blocks.
- **RocksDB:** four background jobs, leveled compaction with dynamic level
  sizes, two 64 MiB write buffers, LZ4 above the bottom level and Zstandard
  with 16 KiB dictionaries at the bottom, 16 KiB blocks, partitioned
  10-bit Bloom filters, index and filter blocks in the cache, and a
  HyperClockCache of **128 MiB (`r-small`)** or **1 GiB (`r-mid`)**.

## Metrics

- **Writes** are shown as `k records/s · CPU s · write amplification`: the
  phase's records divided by its wall time including the drain; the
  process's user plus system CPU time; and bytes written by flushes and
  compactions divided by bytes flushed.
- **Gets** are `k reads/s · CPU s`.
- **Memory** is `live heap / RSS / physical footprint / stored MiB` after the
  phase drains. Live heap is allocated bytes above the measurement taken
  before the engine opened, and the fairest comparison of what each engine
  keeps. RSS and footprint are the process's, and for RocksDB include the
  RAM-disk bytes its files use. Stored is the engine's own count of its
  compressed data.
- Ratios divide Tessera's figure by RocksDB's. For heap and CPU lower is
  better; for gets higher is better. With identical traces, a CPU ratio is
  also the ratio of CPU per record.

## Results

### A, grouped JSON, 8 GiB

| phase | Tessera | `r-small` | `r-mid` |
| --- | ---: | ---: | ---: |
| load | 193.3 · 94.73 · 3.65 | 343.0 · 47.88 · 2.76 | 443.4 · 40.38 · 2.55 |
| update | 126.2 · 78.13 · 5.43 | 280.2 · 26.01 · 3.68 | 316.0 · 29.07 · 3.75 |
| get_1t | 215.9 · 0.92 | 114.6 · 1.74 | 130.9 · 1.53 |
| get_4t | 851.8 · 0.93 | 415.8 · 1.92 | 496.8 · 1.46 |
| load memory | 243 / 567 / 443 / 236.1 | 632 / 885 / 725 / 455.9 | 767 / 997 / 841 / 569.6 |
| update memory | 271 / 670 / 422 / 264.1 | 858 / 1,239 / 996 / 698.8 | 1,304 / 1,650 / 1,494 / 705.5 |

Tessera ÷ RocksDB, `r-small` / `r-mid`:

| metric | load | update |
| --- | ---: | ---: |
| live heap | 0.38× / 0.32× | 0.32× / 0.21× |
| write CPU per record | 1.98× / 2.35× | 3.00× / 2.69× |
| write rate | 0.56× / 0.44× | 0.45× / 0.40× |

Point reads: 1.88× / 1.65× with one thread, 2.05× / 1.71× with four.

### B, weak locality, 4 GiB

| phase | Tessera | `r-small` | `r-mid` |
| --- | ---: | ---: | ---: |
| load | 109.2 · 121.76 · 5.74 | 198.9 · 84.45 · 3.56 | 196.0 · 86.62 · 3.56 |
| update | 48.8 · 107.37 · 8.63 | 123.1 · 67.74 · 4.54 | 123.5 · 67.65 · 4.54 |
| get_1t | 225.5 · 0.88 | 61.3 · 3.19 | 59.5 · 3.06 |
| get_4t | 879.4 · 0.90 | 249.0 · 3.20 | 248.1 · 2.34 |
| load memory | 2,080 / 2,579 / 2,478 / 2,041.3 | 1,971 / 2,449 / 2,244 / 1,946.2 | 2,042 / 2,461 / 2,111 / 1,946.2 |
| update memory | 2,271 / 3,190 / 2,694 / 2,230.1 | 2,095 / 2,689 / 2,272 / 2,066.7 | 2,521 / 2,953 / 2,603 / 2,066.7 |

Tessera ÷ RocksDB, `r-small` / `r-mid`:

| metric | load | update |
| --- | ---: | ---: |
| live heap | 1.06× / 1.02× | 1.08× / 0.90× |
| write CPU per record | 1.44× / 1.41× | 1.59× / 1.59× |
| write rate | 0.55× / 0.56× | 0.40× / 0.39× |

Point reads: 3.68× / 3.79× with one thread, 3.53× / 3.54× with four.

## Linux resident memory

On Linux with glibc, RSS can stay well above Tessera's live data after
compaction, because glibc keeps freed buffers. Measured with the `counters`
tool in a 3 GiB container (2 CPUs, aarch64, 2,097,152 records of 600-byte
values):

| case | peak RSS MiB | final RSS MiB | `memory_used` MiB |
| --- | ---: | ---: | ---: |
| A, grouped | 218 | 196 | 40 |
| B, weak locality | 1,683 | 1,678 | 964 |
| B with `glibc.malloc.mmap_threshold=524288` | 1,389 | 1,041 | 1,006 |

Fixing the mmap threshold with
`GLIBC_TUNABLES=glibc.malloc.mmap_threshold=524288` cut B's final RSS by 38%,
so most of the gap is allocator retention rather than live data. The tunable
applies to every allocation in the process and may change throughput.
Allocators that return freed memory, such as mimalloc or jemalloc with
default settings, avoid the retention as well.

## Caveats

- One machine and single runs. Between two runs of the same code, Tessera's
  B update rate differed by 17% and its gets by up to 3%, so treat
  differences of that size as noise.
- macOS timings; Linux servers may rank differently.
- RocksDB runs on a RAM disk with its write-ahead log off, which removes its
  I/O costs but leaves its file-system and block-cache work; it remains a
  disk engine measured in its best case, and Tessera an in-memory one.
- RocksDB's configuration is reasonable but not tuned per dataset. Other
  block sizes, caches or compression settings would move its numbers.
- The datasets are synthetic. Real data compresses differently.

## Reproducing

Install the prerequisites in [tools/bench/README.md](../tools/bench/README.md),
then run:

```sh
just bench --mode streaming
```

It builds the harness, creates and later detaches the RAM disk, and writes
`results.csv`, `summary.md`, per-second `samples-*.csv`, RocksDB `OPTIONS`
files and `environment.txt` under `.work/bench/<date>-<commit>/`. The run takes
about seven minutes on the machine above.
