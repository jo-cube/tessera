# Release checklist

Tessera is consumed by path or Git revision (`publish = false`). Releases
identify a validated commit and its supported API and checkpoint formats.

- Verify that the tracked working tree is clean and record the release commit.
- Check version, license, repository, description, keywords, categories and MSRV
  in `Cargo.toml`; keep both Cargo lockfiles consistent with the package version.
- Keep dependencies locked; update dependencies deliberately in a separate commit.
- Run `scripts/validate.sh verify`, `scripts/validate.sh full` and
  `scripts/validate.sh msrv` using the script's documented validation images.
  These cover formatting, Clippy, tests, rustdoc, a release library build,
  large-allocation boundaries and the MSRV.
- Run `cargo test --locked --lib long_ -- --ignored` for the long correctness
  campaigns. CI runs these on x86-64 Linux as well as ordinary checks on
  x86-64 Linux, arm64 Linux and macOS.
- On macOS, build `tools/bench` in release mode as described in its README;
  running measurements is optional and is not a release gate.
- Verify that generated documentation and the README describe the current API.
- Record test counts, toolchain versions, platform results and limitations.

After the release decision, create an annotated version tag on the validated
commit. Keep existing release tags immutable; fixes receive a new version.

## Limitations for release notes

- The dataset must fit in RAM; admission budgets do not cap resident memory.
- Persistence requires explicit checkpoints. Closing writes nothing.
- Checkpoints depend on filesystem sync, advisory locks and atomic rename;
  tests cannot establish real power-loss behavior. Network filesystems are unsupported.
- Multi-key atomic batches are supported. Read/write transactions, replication
  and disk tiering are outside the API.
- A `ReadGuard` blocks writers for its lifetime; snapshots retain their sources.
- A worker panic stops that worker; callers can inspect its failure.
- Document the supported run and manifest versions; recovery rejects others.
