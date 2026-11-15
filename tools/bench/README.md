# Tessera vs RocksDB comparison

`tools/bench/run.sh` builds the harness, attaches a 4 GiB APFS RAM disk at
`/Volumes/tessbench`, runs every case once (about 3 minutes) and detaches the
disk. It writes `results.csv`, `summary.md`, `environment.txt` and the RocksDB
OPTIONS files to `.work/bench/<date>-<sha>/`. `TESSERA_BENCH_SCALE=32` divides
the dataset sizes for a quick smoke run.

Options select a subset, for example
`run.sh --dataset B --engine tessera,r-small --case update`. Cases are
`load,update,reads,scan,mixed,bulk`. Reads, scans and the mixed case run on
the updated DB, so they also run the load and update. A filtered run writes
to a time-suffixed directory, and without Tessera in it the mixed case offers
70% of the engine's own update rate.

It links the RocksDB 11.13.2 static library cached in
`~/.cache/tessera-bench/rocksdb-11.13.2/`, built from the `v11.13.2` tag with
`make static_lib DEBUG_LEVEL=0 PORTABLE=1 USE_RTTI=1` against Homebrew LZ4 and
Zstandard. `native.cc` is a small C adapter over its C++ API.

One child process runs per dataset and engine; each aborts if its RSS plus
RAM-disk use exceeds 8 GiB, and the parent kills any child after 300 s.

- Datasets (spike-04 generators): A, 1 GiB of grouped JSON; B, 512 MiB of
  weak-locality JSON. Both are generated in memory before timing starts.
- Engines: Tessera with `EngineOptions::default()`; RocksDB with WAL off and
  syncs suppressed, 4 background jobs, leveled compaction with dynamic level
  bytes, 2 × 64 MiB write buffers, LZ4 upper levels, ZSTD with a 16 KiB
  dictionary at the bottom, 16 KiB blocks, Bloom 10 bits/key, partitioned
  index and filters with the top level pinned, HyperClockCache of 128 MiB
  (`r-small`) or 1.25 × the raw dataset (`r-large`).
- Cases: shuffled load in 256-record batches; sorted bulk load in 64 MiB
  batches (`ingest_sorted`, or SST files plus `IngestExternalFile`); updating
  50% of the keys and then deleting 2%; 220k point reads (10% misses) with
  1 and 4 threads and with `multi_get` batches of 100 random or 100 adjacent
  sorted keys; 20k prefix scans of about 100 records; 20 s of four readers
  against a writer that offers 70% of Tessera's update rate in 1 s bursts.
  `r-large` skips the bulk and mixed cases. Write cases include the drain.
- Memory reports both RSS (`engine_ram_mib`) and physical footprint
  (`engine_footprint_mib`) after a drain, minus their respective baselines
  measured with the dataset generated and no engine open. Footprint comes
  from macOS `proc_pid_rusage(RUSAGE_INFO_V2).ri_phys_footprint` and excludes
  allocator pages marked reusable. Both columns add RAM-disk bytes for
  RocksDB. Write amplification is (flush + compaction bytes written) ÷ flush bytes.
- After every load and update, 20k sampled keys and 200 group scans are
  checked against an in-harness oracle.

## Streaming scale suite

`run.sh --mode streaming` uses A at 8 GiB raw and B at 4 GiB raw on a
6 GiB RAM disk. It runs Tessera defaults, `r-small` (128 MiB cache) and
`r-mid` (1 GiB cache), each once: shuffled load, 50% updates plus 2% deletes,
then exactly 200k random gets (10% deliberately absent keys) on 1 and 4 threads.
Scans, multi-get, bulk and mixed workloads are excluded. Filters and
`TESSERA_BENCH_SCALE` still work; the regular in-memory suite is unchanged.

Regeneration preserves the regular generator's exact keys and both value
versions. A small per-group RNG checkpoint index replaces the input arena;
there is no per-record state or shuffled order array. A six-round Feistel
bijection with cycle walking supplies write order for arbitrary record counts.
Only 256 owned records are buffered per write; RocksDB pointers remain valid
until the synchronous write returns. Write timing/CPU includes regeneration
and permutation, so it is not directly comparable with the in-memory suite.
The 20k-key gates regenerate and compare complete expected values; timed
queries keep only a fixed 200k-key sample with expected lengths.

Tessera `samples-*.csv` records footprint above baseline and stored bytes at
2 s intervals, including drains and phase endpoints. Memory after each drain
is sampled before its correctness gate. Group-index memory is in the baseline.
The streaming process aborts at 12 GiB RSS plus RAM-disk allocation, and the
suite has a 720 s deadline (300 s per child). Each child reports peak RSS.
