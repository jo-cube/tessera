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
