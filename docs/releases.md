# Installation and releases

Tessera is a library. A release is a `vX.Y.Z` Git tag with a
[GitHub Release](https://github.com/jo-cube/tessera/releases) whose notes are
written by the maintainer, with the version's [changelog](../CHANGELOG.md)
section as a fallback. The `.crate` source package and its `SHA256SUMS` are
attached for archiving. Nothing is published to crates.io (`publish = false`),
and there are no binaries.

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

### Checkpoint format

Tessera 0.1 writes **format version 1**: run files, the manifest (with its
keyspace table) and `CURRENT` are each at version 1;
[the guide](guide.md#recovery-and-the-on-disk-layout) describes the layout.

- A patch release reads and writes exactly the format of its minor version.
- A minor release that changes the format bumps the version of each file it
  changes and says so in its changelog. Recovery rejects any version it does
  not support with a `Corruption` error naming it ("unsupported manifest
  version N") and never rewrites the directory.
- There is no converter. To cross a format change, open the directory with
  the release that wrote it, read every keyspace and write it into an
  engine of the target release, then checkpoint that engine into a fresh
  directory.

The `diagnostics` and `tools-memory` features and their counters are not part
of the stable API.

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
- Atomic batches across keyspaces are supported. Read/write transactions,
  replication and disk tiering are outside the API.
- Snapshots, and scans of the latest state, retain the sources they read
  until dropped, and that memory counts against writer admission.
- An engine holds at most 4096 live keyspaces.
- A worker panic stops that worker; `Engine::close` reports it.

## Releasing

1. With `dev` green in CI and a clean working tree, run `just release-prep X.Y.Z`
   on `dev`. It refuses an older version, or a tag or changelog section that
   already exists. It sets the version in `Cargo.toml`, `Cargo.lock` and
   `tools/bench/Cargo.lock`, prepends a `CHANGELOG.md` section built from the
   conventional commits since the previous tag, moves anything under
   `## Unreleased` into it, and commits `chore: release X.Y.Z`. Review the
   section and commit any wording changes. Preparation works on any branch.
   `just release-prep X.Y.Z --dry-run FILE` writes the section to `FILE` and
   changes nothing else.
2. Run the local gates in the container: `scripts/validate.sh verify`,
   `scripts/validate.sh full` and `scripts/validate.sh msrv`
   (see [development](development.md)). Merge the reviewed release commit to
   `main` through the usual pull request.
3. In the [GitHub Releases UI](https://github.com/jo-cube/tessera/releases/new),
   create the `vX.Y.Z` release targeting `main`. Write its title and notes, then
   publish it. Creating the tag starts the **release** workflow; inspect the
   run and wait for both assets before using the release.

The workflow keeps the same commit through all three jobs:

- **prepare** accepts lightweight and annotated `vX.Y.Z` tags. It checks that
  the tag names the checked-out and triggering commit, matches `Cargo.toml`
  and both lockfiles, is reachable from `origin/main`, and has a changelog
  section. It resolves that commit once.
- **verify** runs **ci** on the commit through `workflow_call`, with
  `package: true`: `just all`, the four platform jobs and the MSRV job. It
  uploads the `.crate` that `just package` built and checked.
- **publish**, the only job with write access, checks the tag again on GitHub
  and adopts the existing release if it has no assets. It preserves its title
  and notes, filling an empty body from the changelog. If no release exists,
  it creates a draft with changelog notes. It uploads `tessera-X.Y.Z.crate`
  and `SHA256SUMS`, verifies their names, sizes, completed state and server
  SHA-256 digests, rechecks the remote tag, and publishes only if it was a
  draft. Multiple releases for the tag or any existing assets stop publication;
  assets are never overwritten.

GitHub's automatic policy selects Latest. Avoid concurrent edits to the tag,
release or assets while the workflow runs.

Running **release** by hand with a `ref` only prepares and verifies that
candidate; it never publishes or attaches assets, even when the ref is a tag.

### If a release fails midway

Inspect the run and the Releases page before retrying. A release created in
the UI can already be public even when verification or upload fails.

- **prepare failed:** no assets were built or attached. Fix the named cause.
  If the tag identifies bad source, fix it on `dev` and use a new version.
  Keep the existing tag in place.
- **verify failed:** no assets were attached. Re-run the failed jobs for a
  runner problem. Fix a source failure on `dev` and release a new version.
- **publish failed, release has no assets:** keep the release and tag and
  re-run the failed jobs. The publisher can adopt a draft or published release
  with no assets. Verified workflow artifacts are retained for 14 days.
- **publish failed, release has partial assets:** retries refuse it. Inspect
  the assets first. For an incomplete draft, delete the draft explicitly,
  keep its tag and re-run the failed jobs. A published release is never
  replaced; use a new version for an incomplete or bad published release.
- **publish timed out with both assets present:** it may have succeeded.
  Check the names, sizes and checksums and whether the release is still a
  draft before taking action. A retry will refuse the existing assets.

Protect `v*` tags with a repository rule so that published tags cannot be
moved or deleted.
