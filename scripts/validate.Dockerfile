# The validation toolchain. Built only by `scripts/validate.sh image`, which is
# the one step that uses the network.
FROM rust:1.98.1-bookworm@sha256:93ce27a88655056a51dbdd8f5f2d7ddc071c7b0070fb288a37b5a285fc83971e
RUN rustup component add rustfmt clippy
# just 1.59.0, the prebuilt musl binary, checked against the SHA-256 published
# in the release's SHA256SUMS. Downloading a checked binary runs no build
# script while the network is up.
ARG JUST=1.59.0
RUN set -eu; \
    case "$(uname -m)" in \
      aarch64) target=aarch64-unknown-linux-musl; sum=23871b1702bfd550fb5f901778fd4290ecf2ee034a45a889fbe96d89b188086d ;; \
      x86_64) target=x86_64-unknown-linux-musl; sum=409044617b6f06517c1035e55ca38c8c856ff8b650e41bc2aa1ef82d5903a23e ;; \
      *) echo "no just binary for $(uname -m)" >&2; exit 1 ;; \
    esac; \
    archive="just-$JUST-$target.tar.gz"; \
    curl -fsSLo "/tmp/$archive" "https://github.com/casey/just/releases/download/$JUST/$archive"; \
    echo "$sum  /tmp/$archive" | sha256sum -c -; \
    tar -xzf "/tmp/$archive" -C /usr/local/bin just; \
    rm "/tmp/$archive"; \
    just --version
# The source mounts read-only at /work, owned by the host user; Cargo's
# packaging reads its Git status.
RUN git config --system --add safe.directory /work
