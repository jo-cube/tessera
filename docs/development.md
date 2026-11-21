# Development

`rust-toolchain.toml` pins Rust 1.98.1 with rustfmt and Clippy, which rustup
installs on first use. The library's minimum is Rust 1.89 (`rust-version`),
which `just msrv` checks. The other tools are [just](https://just.systems),
Python 3.9 or newer for `scripts/release.py`, and a C compiler for the bundled
Zstandard. Container validation also needs Colima and Docker.

## Recipes

The `justfile` is the one list of checks. CI runs its recipes, and so does
`scripts/validate.sh check`, one recipe per bounded step.

| Recipe | What it runs |
| --- | --- |
| `just` | the recipe list |
| `just fmt` | `cargo fmt --check`; `cargo fmt` fixes it |
| `just clippy` | Clippy on every target and feature, warnings denied |
| `just test` | the default tests, then the integration tests that need `diagnostics` |
| `just doc` | rustdoc with warnings and missing docs denied |
| `just examples` | runs each program in `examples/`; each checks its own results |
| `just long` | the ignored long correctness campaigns, one at a time |
| `just build` | a release build of the library |
| `just package` | `cargo package --locked`, then a check that the `.crate` holds only `src`, `tests`, `examples`, the README, license, changelog and architecture notes |
| `just msrv` | `cargo check` and Clippy on every target on Rust 1.89 |
| `just scripts-test` | the unit tests of `scripts/release.py` |
| `just all` | fmt, clippy, test, doc, examples, long, package and scripts-test: CI's `checks` job |
| `just validate [STEP]` | `scripts/validate.sh STEP` |
| `just bench [ARGS]` | the macOS comparison harness, `tools/bench/run.sh ARGS` |
| `just release-prep X.Y.Z` | release preparation; see [releases](releases.md#releasing) |

`just package` packages the committed tree, so commit before running it.
The default suite includes bounded mutation tests, a concurrent sequence
model with a 20-second process deadline and a model of five keyspaces with
drops, snapshots and reopening. The long campaigns are longer variants of
these; together they take about two and a half minutes in the container,
half the `long` step's bound. The large-allocation boundary tests are also ignored and
run only in the container (`scripts/validate.sh limits`).

## Container validation

Run tests, Clippy, fault injection and anything else that can loop or
allocate without bound through `scripts/validate.sh`, not in a macOS shell.
macOS cannot enforce a per-process memory ceiling, and a broken change
under test can exhaust the host: a fault-injection run once used all the memory of
a development machine and forced a restart. The script runs each step in a
container inside the Colima VM, which the hypervisor caps at 4 GiB, under a
3 GiB cgroup with no swap, two CPUs, 256 processes and its own timeout. Tests
write only to a 512 MiB tmpfs. Every step except `image` and `deps` is offline.

```sh
colima start
scripts/validate.sh verify       # prove the memory, pid and time limits hold
scripts/validate.sh image        # once: the toolchain image (online)
scripts/validate.sh msrv-image   # once: add Rust 1.89 (online)
scripts/validate.sh deps         # after a dependency change (online)
scripts/validate.sh              # `check`: every `just all` recipe, then a release build
scripts/validate.sh full         # check, then the large-allocation limits
scripts/validate.sh msrv         # just msrv
scripts/validate.sh exec 60 CMD  # any command under the same limits
```

The image adds just 1.59.0 as the prebuilt musl binary from its GitHub
release, checked against the SHA-256 in the release's `SHA256SUMS`; no build
script runs while the network is up. Each step prints its exit status, peak
memory, out-of-memory kills, wall time and bytes written. To test a modified
copy of the sources, put it under `target/` and set `TESSERA_SOURCE` to it, so
that it gets its own build cache.

## Diagnostics and counters

The `diagnostics` feature adds hidden work and memory counters by level. It is
not part of the stable API. `just test` runs the two integration tests that
need it, `lookup_allocations` and `retention`. The `counters` example prints
the counters for three large scenarios as CSV:

```sh
cargo run --release --features diagnostics --example counters
```

[tools/README.md](../tools/README.md) describes its scenarios, flags and audits.
Run large scenarios with `scripts/validate.sh exec`.

## macOS comparison with RocksDB

`tools/bench` is a separate crate that compares Tessera with RocksDB on APFS
RAM disks. It needs Homebrew LZ4 and Zstandard, and RocksDB 11.13.2 built from
source into `~/.cache/tessera-bench/rocksdb-11.13.2/`, a cache kept outside the
repository so it survives clean checkouts. `just bench` builds and runs the
suite and writes results under `.work/bench/`. It is not a release gate.
[tools/bench/README.md](../tools/bench/README.md) covers the build, workloads
and accounting, and [performance](performance.md) the published results.

## Commits

Commits follow [Conventional Commits](https://www.conventionalcommits.org):
`type(scope): lower-case imperative summary`, at most 72 characters, with no
attribution trailers.

- **Types:** feat, fix, perf, refactor, test, docs, build, ci, chore, revert.
- **Scopes:** engine, api, memtable, run, block, compaction, read, checkpoint,
  memory, bench, tools, ci, docs, deps.

The changelog is generated from them. `feat`, `fix` and `perf` go under
Features, Fixes and Performance. Any commit scoped `api` goes under API.
`test`, `ci`, `chore` and `docs` are left out, and everything else goes under
Other. Mark a breaking change with `!` after the type or scope.
