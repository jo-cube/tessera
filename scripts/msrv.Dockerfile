# The minimum supported Rust toolchain, added to the validation image. Built
# only by `scripts/validate.sh msrv-image`, which uses the network; the
# version comes from `rust-version` in Cargo.toml.
ARG BASE=tessera-validate:1.98.1
FROM ${BASE}
ARG TOOLCHAIN
RUN rustup toolchain install "$TOOLCHAIN" --profile minimal --component clippy
