# Releasing a private alpha

Tessera is consumed by path or Git revision and is not published to crates.io
(`publish = false`). The API and checkpoint format may change without migration.

## Checklist

- Record the release commit and verify that the tracked working tree is clean.
- Verify package version, license, repository, description and Rust version in
  `Cargo.toml`; keep the package version in `Cargo.lock` consistent.
- Keep dependencies locked. After validation, `git diff --exit-code Cargo.lock`
  must pass. Update dependencies deliberately in a separate commit.
- Run `scripts/validate.sh verify`, `scripts/validate.sh full` and
  `scripts/validate.sh msrv` using the validation images described by the script.
  These check formatting, Clippy, unit/integration/documentation tests, rustdoc,
  the release library build and large-allocation boundaries.
- Record CI results for each configured platform and list untested platforms.
- Check that the README and generated engine documentation describe this release.
- Record test counts, toolchain versions and known limitations in release notes.

After the release decision, create an annotated version tag on that validated
commit. Do not move an existing release tag; fixes get a new alpha version.

## Limitations to carry into release notes

- The dataset must fit in RAM; admission limits do not cap resident memory.
- Recovery loads only the latest committed explicit full checkpoint. There is
  no per-write persistence or automatic checkpointing. Closing writes nothing.
- Checkpoints rely on filesystem sync, advisory locks and atomic rename; tests
  do not prove behavior under real power loss. Network filesystems are unsupported.
- Transactions, multi-key atomic writes, replication and disk tiering are absent.
- A `ReadGuard` blocks writers for its lifetime; snapshots remain readable
  independently of the engine.
- A worker panic stops background compaction for that engine's lifetime.
- API and format compatibility are not promised before a stable release.

Compression layout and dictionary measurements are optional research evidence,
not release gates.
