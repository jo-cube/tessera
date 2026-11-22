# macOS comparison harness

This macOS-specific tool compares Tessera with RocksDB using APFS RAM disks,
Mach memory counters, Homebrew libraries and a C++ adapter. It measures load,
updates, point reads, multi-get, scans, mixed reader/writer work and sorted
bulk loading. It also checks sampled reads and scans against an input oracle.

## Build and run

Install Homebrew LZ4 and Zstandard. Build RocksDB 11.13.2 from tag `v11.13.2`
with `make static_lib DEBUG_LEVEL=0 PORTABLE=1 USE_RTTI=1` and place its source
and static library in `~/.cache/tessera-bench/rocksdb-11.13.2/`.

Build without running measurements:

```sh
ROCKSDB_DIR="$HOME/.cache/tessera-bench/rocksdb-11.13.2" \
  cargo build --locked --release --manifest-path tools/bench/Cargo.toml
```

`tools/bench/run.sh` builds and runs the regular suite. It creates a 4 GiB
APFS RAM disk at `/Volumes/tessbench` and detaches it on exit. Results go to
`target/bench/<date>-<sha>/`, which git ignores: `results.csv`, `summary.md`,
`environment.txt` and RocksDB OPTIONS files. Filtered runs include a time
suffix. `environment.txt` records the host's hardware and toolchain; keep it
local and out of commits.

```sh
tools/bench/run.sh --dataset B --engine tessera,r-small --case update
TESSERA_BENCH_SCALE=32 tools/bench/run.sh --engine tessera
```

Cases are `load,update,reads,scan,mixed,bulk` (default selection), plus
`reader_scale`, `writer_scale`, and `zipf` when named via `--case`. Datasets
are `A`, `B` (default) and `C` (small records, via `--dataset A,B,C`). Read
cases (`reads`, `scan`, `mixed`, `reader_scale`, `zipf`) prepare the load and
updated state first; `writer_scale` opens a fresh store per configuration.
Tessera-only selections skip RAM-disk setup. Each child process has a
300-second deadline and a memory guard of 8 GiB RSS plus used RAM-disk bytes
for the regular suite.

## Workloads and accounting

Dataset A is 1 GiB of grouped JSON; B is 512 MiB of weak-locality JSON; C is
512 MiB of small records (20-byte structured keys, ~52-byte compact JSON
values, 64 keys per 8-byte group prefix). All are generated before timing.
Regular workloads use shuffled 256-record write batches, 50% updates and 2%
deletes, a 220k-key point-read set with 10% misses, multi-get batches of
100 random or adjacent keys, 20k prefix scans, and 64 MiB sorted bulk
batches: Tessera writes each as one `WriteBatch`, which it builds into runs
outside its lock, and RocksDB ingests each as an SST file. Four-thread point
reads and random multi-get cycle through the query set in 1.5 s windows,
starting after all readers warm up. Mixed work reports p99 and maximum
writer batch latency and offers
writes at 70% of Tessera's update rate (or the selected engine's rate when
Tessera is absent). Set `TESSERA_BENCH_MIXED_RATE` to a common rate in
records per second when comparing builds at the same writer load. Write
amplification includes maintenance drains. `r-large`
skips bulk and mixed. Load and update report batch latency separately from
the maintenance drain.

For Tessera steady-state comparisons, set `TESSERA_BENCH_QUIET_MS` to delay
the samples after each regular load and update drain. Use a delay longer
than the engine's quiet period, and use the same delay on both sides of a
comparison. The `load_quiet` and `update_quiet` rows include stored bytes,
heap, and CPU and compaction bytes spent after the ordinary drain. The timed
load and update rows exclude the delay and its extra maintenance. Leave the
variable unset to run without these pauses. With quiet sampling enabled,
`TESSERA_BENCH_IDLE_SAMPLE_MS` adds a post-update window that records CPU,
worker passes, and compaction steps. `TESSERA_BENCH_LEVEL_DIAG=1` adds level
and memtable accounting, including charged active and frozen bytes at the
end of writes and after each sample.

Additional selectable cases:

- `reader_scale`: 1, 2, 4, 8 reader threads (plus `maxt` at available
  parallelism when larger), measuring latest-state `Reader` and (on Tessera)
  `Snapshot` readers over uniform and scrambled Zipfian (θ = 0.99) lookups,
  both read-only and with one background writer offering 20k records/s in
  256-record batches. Every read is checked against the oracle.
- `writer_scale`: 1, 2, 4, 8 writer threads across batch sizes 1, 16 and 256
  records, on both disjoint per-thread key ranges and an overlapping 4,096-key
  pool, with post-drain oracle checks.
- `zipf`: scrambled Zipfian (θ = 0.99) point gets (1 and 4 threads, 10%
  misses) and 4-thread read/update mixes at 95/5 and 50/50 read/write ratios
  with live and post-drain oracle checks.

Tessera uses default options and one keyspace, and reads through one
`Reader` per thread. RocksDB disables WAL and syncs, uses four
background jobs, dynamic leveled compaction, two 64 MiB write buffers, LZ4
upper levels, Zstandard at the bottom, 16 KiB blocks and Bloom filters.
`r-small` has a 128 MiB block cache; `r-large` uses 1.25 times the raw dataset.

Memory is live allocated heap relative to the measurement taken after input
generation and before opening the engine. RSS and physical footprint are also
recorded; RocksDB includes used RAM-disk bytes. Write amplification is flush
plus compaction output divided by flush output. Correctness gates check 20k
sampled keys and 200 group scans after each load and update.

## Streaming mode

`run.sh --mode streaming` regenerates records with bounded input buffers:
A is 8 GiB and B is 4 GiB raw, with an 8 GiB RAM disk. Engines are Tessera,
`r-small` and `r-mid` (1 GiB cache). It measures load, updates/deletes and
200k random gets with 1 or 4 readers. Regeneration and write permutation are
included in timing. Group boundaries and RNG checkpoints supply the oracle;
only 256 owned records are buffered per write.

`samples-*.csv` records heap, RSS, footprint and stored bytes every two seconds
and at workload endpoints. Each child has a 300-second deadline and a 12 GiB
RSS-plus-used-disk guard. Input generation is excluded from engine memory.
`TESSERA_BENCH_SCALE` divides sizes; `TESSERA_BENCH_DISK_GIB` sets RAM-disk
capacity. `TESSERA_BENCH_LEVEL_RATIO` and `TESSERA_BENCH_BACKGROUND_THREADS`
override Tessera options for controlled comparisons.
