# The validation toolchain. Built only by `scripts/validate.sh image`, which is
# the one step that uses the network.
FROM rust:1.98.1-bookworm@sha256:93ce27a88655056a51dbdd8f5f2d7ddc071c7b0070fb288a37b5a285fc83971e
RUN rustup component add rustfmt clippy
