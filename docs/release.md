# Releasing a private alpha

Tessera is released as a Git tag that applications depend on by revision.
It is never published to crates.io (`publish = false`). Nothing here is
automated: each step is run and its result recorded by the person cutting
the release.

## Checklist

### 1. Source state

- [ ] The release commit is on the branch the release is cut from, and
      `git status --short` prints nothing (no modified or untracked files).
- [ ] Its hash is recorded in the release notes.
- [ ] `Cargo.lock` and `tools/compare/Cargo.lock` are committed. No step
      below may change them: every one runs with `--locked`, and
      `git diff --exit-code Cargo.lock tools/compare/Cargo.lock` still
      passes afterwards. Update dependencies only deliberately, with
      `scripts/validate.sh deps`, in a commit of their own before the
      release commit.

### 2. Metadata

- [ ] `version` in `Cargo.toml` is the version being released (for this
      alpha, `0.1.0-alpha.1`), and the `tessera` entries in both lockfiles
      match it.
- [ ] `rust-version` is still the oldest toolchain that passes
      `scripts/validate.sh msrv`. The image tag follows it, so a change
      needs `scripts/validate.sh msrv-image` first.
- [ ] `publish = false`, `license`, `description`, `repository` and
      `readme` are as intended.

### 3. Required validation

The release is blocked until all of these pass on the release commit:

- [ ] `scripts/validate.sh verify`: the container's limits are enforced.
- [ ] `scripts/validate.sh full`: formatting, Clippy with warnings denied
      on every target, unit, integration and documentation tests (the
      README's examples among them), rustdoc with warnings denied, a release
      build of the library and examples, and the large-allocation boundary
      tests.
- [ ] `scripts/validate.sh msrv`: check and Clippy on the minimum Rust
      version.
- [ ] CI (`.github/workflows/ci.yml`) has passed for the commit on every
      platform it runs, or the release notes list each platform where it did
      not run as untested.

### 4. Platforms

- [ ] The release notes give the tested platforms, those that passed step 3,
      separately from the targets that are only expected to work. Only Linux
      aarch64 has passed the full validation so far.

### 5. Documentation

- [ ] The README's Status, Platform and Durability sections describe this
      release, including its known limitations (below).
- [ ] `cargo doc` is part of step 3. Read the `engine` module page once:
      it holds the consumer-facing contracts.

### 6. Tag

Only after an explicit decision to release:

```sh
git tag -a v0.1.0-alpha.1 -m "Tessera 0.1.0-alpha.1 (private alpha)"
git push origin v0.1.0-alpha.1
```

The tag is annotated, named `v` plus the `Cargo.toml` version, and points at
the validated commit. A tag is never moved: a fix is a new alpha.

### 7. Release notes

- [ ] Fill in the template below and attach it to the tag's GitHub release,
      marked as a pre-release and kept private to the repository.

## Optional evidence

These are not release gates. They are slow, sensitive to the machine, and
compare configurations rather than prove correctness:

- `scripts/validate.sh bench` and the passes in
  [`benchmarks.md`](benchmarks.md#benchmarks).
- The RocksDB comparison in `tools/compare`, which needs its own image.

Rerun them only to check a change that could affect performance. Quote them
with their environment, and only against the configurations measured.

## Known limitations for 0.1.0-alpha.1

- Tested only on Linux aarch64 (Rust 1.98.1, in a 2-CPU, 4 GiB VM). Linux
  x86-64 and macOS arm64 are configured in CI but have not run; Windows is
  unsupported.
- Durability assumes that a completed `fsync` reaches stable media and that
  an interrupted write leaves a prefix of its bytes. Neither has been tested
  under real power loss. Network filesystems are unsupported, and macOS
  durability is outside the contract.
- `close` does not checkpoint: RAM-only and checkpoint-only engines lose
  every write since their latest checkpoint, and durable engines keep
  exactly what a crash would keep.
- No transactions, no multi-key atomic writes, no replication, no disk
  tiering: the dataset must fit in memory. Checkpoints are full, never
  incremental, and never automatic.
- Process memory is not bounded: the limits are admission checks on
  estimated bytes. Peak resident memory ran up to 546 MiB above the
  accounted bytes with the default 64 MiB memtables and 256 MiB jobs, and
  freed memory can stay in the allocator's heap.
- Flushes run on the writing thread (about 0.1–0.2 s per 64 MiB memtable),
  and one background worker compacts, so sustained writes are paced to it.
- Reopening under a less dense codec with compaction limits under four
  runs can waste merges at the output limit or refuse jobs; keep the
  defaults' proportion.
- Measured up to 16 million writes and 1.43 GiB resident in a 3 GiB
  container, not beyond.
- A `ReadGuard` blocks writers while held.
- A worker panic stops background compaction for the engine's lifetime.
- The API and on-disk formats may change before a stable release, without
  migration.

## Release notes template

```markdown
# Tessera 0.1.0-alpha.1 (private alpha)

Commit: <hash>. Depend on it with
`tessera = { git = "https://github.com/jo-cube/tessera", tag = "v0.1.0-alpha.1" }`.
Requires Rust 1.89 or later and a C compiler.

## What it is

A RAM-first, compressed LSM key-value engine embedded in the application's
process: point lookups and ordered or prefix scans from compressed runs in
memory, consistent snapshots, background compaction, and opt-in checkpoints
and a synced write-ahead log.

## Validated

- Linux aarch64, Rust 1.98.1: `scripts/validate.sh full` (<n> tests) and
  `scripts/validate.sh msrv` (Rust 1.89.0, check and Clippy).
- CI: <platforms that passed, or "not run">.

## Not validated

<platforms that did not run>; real power loss.

## Known limitations

<copy the list from docs/release.md>
```
