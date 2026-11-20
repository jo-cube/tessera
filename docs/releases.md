# Installation and releases

Tessera is a library. A release is an annotated `vX.Y.Z` Git tag with a
[GitHub Release](https://github.com/jo-cube/tessera/releases) whose notes are
that version's [changelog](../CHANGELOG.md) section. The `.crate` source
package and its `SHA256SUMS` are attached for archiving. Nothing is published
to crates.io (`publish = false`), and there are no binaries.

## Install

```toml
[dependencies]
tessera = { git = "https://github.com/jo-cube/tessera.git", tag = "v0.1.0" }
```

Use a tag once its release is published. Cargo records the tagged commit in
the application's `Cargo.lock`; change the tag to upgrade. While the repository
is private, Cargo needs Git credentials: setting `net.git-fetch-with-cli = true`
in `.cargo/config.toml` lets it use the ones `git` already has.

Building needs Rust 1.89 or newer and a C compiler, which builds the bundled
Zstandard through `zstd-sys`. There are no other system dependencies.

## Versioning and compatibility

Versions follow [semantic versioning](https://semver.org). Before 1.0:

- a minor version (0.1 → 0.2) may break the API, the checkpoint format or the
  minimum Rust version, and its changelog says which;
- a patch version (0.1.0 → 0.1.1) keeps all three.

The checkpoint format is run format 6 with manifest version 5. Recovery
rejects versions it does not support, and there is no converter: to cross a
format change, an application recovers with the old version and writes its
data into an engine of the new one. The `diagnostics` and `tools-memory`
features and their counters are not part of the stable API.

## Supported platforms

| Platform | Runner | Checked by CI |
| --- | --- | --- |
| Linux x86-64 | `ubuntu-24.04` | `just all`, tests, release build, MSRV |
| Linux arm64 | `ubuntu-24.04-arm` | tests, release build |
| macOS arm64 | `macos-15` | tests, release build |
| macOS x86-64 | `macos-15-intel` | tests, release build |

**Windows is not supported.** Checkpoints sync each directory they change by
opening it with `File::open` and calling `sync_all`. On Windows that call is
`FlushFileBuffers` on a read-only handle, which fails with access denied, so
every checkpoint would fail. The in-memory engine would build, but a port
needs its own durability design rather than workarounds, so CI does not run
Windows. Other Unix
systems may work and are untested.

## Limitations

- The dataset must fit in RAM; admission budgets do not cap resident memory.
- Persistence requires explicit checkpoints. Closing writes nothing, and all
  writes since the latest checkpoint are lost on process exit.
- Checkpoints depend on file and directory fsync, advisory locks and atomic
  rename on a local filesystem; tests cannot establish real power-loss
  behaviour. Network filesystems are unsupported.
- Multi-key atomic batches are supported. Read/write transactions,
  replication and disk tiering are outside the API.
- A `ReadGuard` blocks writers for its lifetime; snapshots retain their sources.
- A worker panic stops that worker; callers can inspect its failure.

## Releasing

Releases come from `main`, which only ever fast-forwards to `dev`.

1. With `dev` green in CI, run the local gates in the container:
   `scripts/validate.sh verify`, `scripts/validate.sh full` and
   `scripts/validate.sh msrv` (see [development](development.md)).
2. Fast-forward `main`: `git checkout main && git merge --ff-only dev`.
3. Run `just release-prep X.Y.Z`. It refuses a dirty tree, a branch other
   than `main`, an older version, or a tag or changelog section that already
   exists. It sets the version in `Cargo.toml`, `Cargo.lock` and
   `tools/bench/Cargo.lock`. It then prepends a `CHANGELOG.md` section built
   from the conventional commits since the previous tag, moving anything under
   `## Unreleased` into it, and commits `chore: release X.Y.Z`. Review the
   section and amend the commit if the wording needs it.
   `just release-prep X.Y.Z --dry-run FILE` writes the section to `FILE` and
   changes nothing.
4. Run the commands it prints: `git tag -a vX.Y.Z`, `git push origin main`,
   `git push origin refs/tags/vX.Y.Z`, then fast-forward `dev` to `main`.
   Push `main` before the tag, since the tag must be reachable from it.

Pushing the tag starts the **release** workflow:

- **prepare** checks that the tag is `vX.Y.Z` and annotated, names the
  checked-out commit, matches `Cargo.toml` and both lockfiles, is reachable
  from `main`, and has a changelog section. It resolves that commit once.
- **verify** runs **ci** on the commit through `workflow_call`, with
  `package: true`: `just all`, the four platform jobs and the MSRV job. It
  uploads the `.crate` that `just package` built and checked.
- **publish**, the only job with write access, checks the tag again on GitHub,
  refuses if a release or draft for it exists, creates a draft with the notes,
  uploads `tessera-X.Y.Z.crate` and `SHA256SUMS`, checks their sizes and
  digests, and publishes the draft.

Running **release** by hand with a `ref` does prepare and verify, so a
candidate can be checked before tagging; it never publishes.

### If a release fails midway

Look at the run and at the Releases page before changing anything.

- **prepare failed:** nothing was built or published. The message names the
  check. If the tag itself is wrong (lightweight, wrong commit, missing
  changelog) and nobody can have used it, delete it locally and on GitHub
  (`git push origin :refs/tags/vX.Y.Z`), fix the cause and tag again.
  Otherwise release the fix as the next patch version.
- **verify failed:** nothing was published. Re-run the failed jobs if the
  failure was a runner problem. A real failure is fixed on `dev` and released
  as the next patch version; the failed tag stays and gets no release.
- **publish failed:** an incomplete draft may exist. Delete the draft, keep
  the tag and re-run the failed jobs; the uploaded `.crate` artifact lasts 14
  days. A published release is never replaced; a bad one is followed by a new
  version.

Protect `v*` tags with a repository rule so that published tags cannot be
moved or deleted.
