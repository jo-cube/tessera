# Tessera's checks. `just all` is exactly CI's `checks` job; CI and
# `scripts/validate.sh check` call these recipes rather than repeat them.

# The minimum supported Rust version, from `rust-version` in Cargo.toml.
msrv := `sed -n 's/^rust-version = "\(.*\)"$/\1.0/p' Cargo.toml`

default:
    @just --list

# Check formatting; `cargo fmt` fixes it.
fmt:
    cargo fmt --check

clippy:
    cargo clippy --locked --all-targets --all-features -- -D warnings

# Default features, then the integration tests that need `diagnostics`.
test:
    cargo test --locked
    cargo test --locked --features diagnostics --test lookup_allocations --test retention

doc:
    RUSTDOCFLAGS="-D warnings -D missing_docs" cargo doc --locked --no-deps

# Run every example program; each checks its own results.
examples:
    cargo run --locked --example basic
    cargo run --locked --example keyspaces
    cargo run --locked --example offsets
    cargo run --locked --example hot_reads

# The ignored long correctness campaigns, one at a time.
long:
    cargo test --locked --lib long_ -- --ignored --test-threads=1

build:
    cargo build --locked --release --lib

# Build and verify the .crate, then check it holds only what a consumer needs.
package:
    cargo package --locked
    python3 scripts/release.py check-package

# Type-check and Clippy every target on the minimum supported Rust version.
msrv:
    cargo +{{msrv}} check --locked --all-targets --all-features
    cargo +{{msrv}} clippy --locked --all-targets --all-features -- -D warnings

scripts-test:
    python3 -m unittest discover -s scripts/tests

all: fmt clippy test doc examples long package scripts-test

# The checks in a resource-limited Linux container; see docs/development.md.
validate *args:
    scripts/validate.sh {{args}}

# The macOS comparison with RocksDB; see tools/bench/README.md.
bench *args:
    tools/bench/run.sh {{args}}

# Bump the version, prepend the changelog and commit; prints the tag commands.
release-prep version *args:
    python3 scripts/release.py release-prep {{version}} {{args}}
