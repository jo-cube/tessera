#!/usr/bin/env bash
# Runs Tessera's validation inside a resource-limited Linux container.
#
#   scripts/validate.sh image              build the toolchain image (online)
#   scripts/validate.sh deps               resolve and fetch crates (online)
#   scripts/validate.sh [check]            fmt, Clippy, tests, docs and a release
#                                          build (default)
#   scripts/validate.sh limits             the large-allocation boundary tests
#   scripts/validate.sh full               check, then limits
#   scripts/validate.sh msrv-image         add the minimum Rust toolchain (online)
#   scripts/validate.sh msrv               check and Clippy on that toolchain
#   scripts/validate.sh verify             prove the limits are enforced
#   scripts/validate.sh exec SECONDS CMD…  any command under the same limits
#
# macOS cannot enforce a per-process memory ceiling, and Cargo job counts bound
# parallelism rather than memory. The hard limits here are the Colima VM, which
# the hypervisor caps at 4 GiB in total, and a 3 GiB cgroup inside it. The
# cgroup sits below the VM size so an out-of-memory kill lands on the workload
# instead of on the guest kernel or Docker daemon.
#
# Anything that can loop or allocate without bound, including tests of
# deliberately broken code, belongs in this script and not in a host shell.
#
# Validation is offline. Only `image` and `deps` reach the network, and both
# run inside the same 4 GiB VM. `deps` downloads crates and updates Cargo.lock
# without compiling anything, so dependency build scripts only ever run in the
# offline container. Every other step uses the locked crates read-only.

set -euo pipefail

repo="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source_dir="${TESSERA_SOURCE:-$repo}"
image="${TESSERA_IMAGE:-tessera-validate:1.98.1}"
# The minimum supported Rust version, from Cargo.toml, as a full toolchain.
msrv="$(sed -n 's/^rust-version = "\(.*\)"$/\1/p' "$repo/Cargo.toml")"
case "$msrv" in *.*.*) ;; *) msrv="$msrv.0" ;; esac
msrv_image="$image-msrv$msrv"
docker=(docker --context "${TESSERA_DOCKER_CONTEXT:-colima}")

# Cargo fingerprints by path and mtime, and every source mounts at /work, so a
# modified copy must never share the repository's build cache. An anonymous
# volume is removed with its container. Downloaded crates are the same for
# every source and live in their own volume.
if [ -n "${TESSERA_SOURCE:-}" ]; then
  cache_mount=/cache
else
  cache_mount=tessera-cache:/cache
fi
crates_mount=tessera-crates:/crates:ro
network=none
offline=true
extra_mounts=()

memory=3g
# The /tmp tmpfs, charged to the container's memory.
tmpfs_size=512m
cpus=2
pids=256
run_label="tessera.validate=$$"

cleanup() {
  local ids
  ids="$("${docker[@]}" ps -q --filter "label=$run_label" 2>/dev/null || true)"
  if [ -n "$ids" ]; then
    # Word splitting is intended: one argument per container.
    # shellcheck disable=SC2086
    "${docker[@]}" kill $ids >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT INT TERM

preflight() {
  local limits
  if ! limits="$("${docker[@]}" info --format '{{.MemoryLimit}} {{.SwapLimit}} {{.PidsLimit}}' 2>/dev/null)"; then
    echo "error: the container runtime is not reachable. Start it with: colima start" >&2
    exit 2
  fi
  if [ "$limits" != "true true true" ]; then
    echo "error: runtime cannot enforce memory, swap and pid limits: $limits" >&2
    exit 2
  fi
}

require_image() {
  if ! "${docker[@]}" image inspect "$image" >/dev/null 2>&1; then
    echo "error: image $image is missing. Build it with: scripts/validate.sh image" >&2
    exit 2
  fi
}

build_image() {
  "${docker[@]}" build --pull=false --tag "$image" - < "$repo/scripts/validate.Dockerfile"
}

# The timeout runs inside the container, so it holds even if this script dies.
# Peak memory is the cgroup's and includes page cache, which is reclaimable.
# disk_written is what the container's cgroup had written to block devices
# when the step ended (the build cache on the VM's disk, whose image lives on
# the host's SSD): a lower bound, since dirty pages written back after the
# step are not counted, and logical, since the host's own writes are not
# visible here. Writes to the /tmp tmpfs are not among them. tmp_left is what
# /tmp still holds.
readonly INNER='
limit=$1; shift
start=$(date +%s)
status=0
timeout --kill-after=5 "$limit" "$@" || status=$?
oom=$(awk "\$1 == \"oom_kill\" { print \$2 }" /sys/fs/cgroup/memory.events)
peak=$(( $(cat /sys/fs/cgroup/memory.peak) / 1048576 ))
wrote=$(awk "{ for (i = 2; i <= NF; i++) if (\$i ~ /^wbytes=/) { sub(/wbytes=/, \"\", \$i); n += \$i } } END { printf \"%d\", n / 1048576 }" /sys/fs/cgroup/io.stat 2>/dev/null || echo "?")
tmp=$(df -Pm /tmp 2>/dev/null | awk "NR == 2 { print \$3 }")
[ -n "$tmp" ] || tmp="?"
echo "[$STEP] exit=$status peak=${peak}MiB oom_kills=$oom wall=$(( $(date +%s) - start ))s disk_written=${wrote}MiB tmp_left=${tmp}MiB"
exit $status
'

# Usage: bounded LABEL SECONDS COMMAND...
bounded() {
  local label=$1 seconds=$2
  shift 2
  "${docker[@]}" run --rm --init --pull never --network "$network" \
    --label "$run_label" \
    --memory "$memory" --memory-swap "$memory" --cpus "$cpus" --pids-limit "$pids" \
    --ulimit core=0 --cap-drop ALL --security-opt no-new-privileges \
    --tmpfs "/tmp:rw,exec,size=$tmpfs_size,mode=1777" \
    --volume "$source_dir:/work:ro" --volume "$cache_mount" --volume "$crates_mount" \
    ${extra_mounts[@]+"${extra_mounts[@]}"} --workdir /work \
    --env STEP="$label" --env CARGO_HOME=/crates --env CARGO_TARGET_DIR=/cache/target \
    --env CARGO_BUILD_JOBS=2 --env RUST_TEST_THREADS=2 --env CARGO_NET_OFFLINE="$offline" \
    "$image" sh -c "$INNER" sh "$seconds" "$@"
}

# /tmp is a 512 MiB tmpfs, charged to the container's memory, so checkpoint
# tests write only to bounded scratch space that disappears with the
# container. fsync is free there; durability is not what those tests check.
# It must allow exec: rustdoc runs merged doctest binaries from /tmp.

# The one writable path in the repository is Cargo.lock, mounted over the
# read-only source. `cargo fetch` honours existing pins and never compiles.
fetch_crates() {
  network=bridge
  offline=false
  crates_mount=tessera-crates:/crates
  extra_mounts=(--volume "$repo/Cargo.lock:/work/Cargo.lock")
  bounded deps 300 cargo fetch
}

check() {
  bounded fmt 60 cargo fmt --check
  bounded clippy 300 cargo clippy --locked --all-targets --all-features -- -D warnings
  bounded build-tests 300 cargo test --locked --no-run
  bounded test 300 cargo test --locked
  bounded test-diagnostics 300 cargo test --locked --features diagnostics --test lookup_allocations --test retention
  bounded doc 120 env RUSTDOCFLAGS="-D warnings" cargo doc --locked --no-deps

  bounded release 300 cargo build --locked --release --lib
}

# The minimum Rust version is a compile contract: every target type-checks and
# passes Clippy on it with the locked crates. Its artifacts live apart from
# the main toolchain's, and `check` writes metadata only, not binaries.
msrv_check() {
  image="$msrv_image"
  require_image
  bounded msrv-check 300 env CARGO_TARGET_DIR=/cache/msrv cargo "+$msrv" check --locked --all-targets --all-features
  bounded msrv-clippy 300 env CARGO_TARGET_DIR=/cache/msrv cargo "+$msrv" clippy --locked --all-targets --all-features -- -D warnings
}

limits() {
  bounded limits 180 cargo test --locked --lib limits_tests -- --ignored --test-threads=1
}

# Write budgets: tests and fixtures stay on the 512 MiB /tmp tmpfs (charged
# to the container's 3 GiB), a few MiB per database and never more than a
# few hundred MiB at once; each test removes its directory. Builds write the
# cache volume (measured: 20–40 MiB for an incremental test build, a few
# hundred MiB when every test binary relinks); prefer one final `full` to
# repeated ones.
# Each probe is bounded on its own, so a failed limit cannot run away.
verify() {
  local failures=0 status output

  output="$(bounded limits-visible 10 sh -c 'echo "memory.max=$(cat /sys/fs/cgroup/memory.max) swap.max=$(cat /sys/fs/cgroup/memory.swap.max) cpu.max=$(cat /sys/fs/cgroup/cpu.max) pids.max=$(cat /sys/fs/cgroup/pids.max) nproc=$(nproc) timeout=$(command -v timeout)"')"
  echo "$output"
  case "$output" in
    *"memory.max=3221225472 swap.max=0 cpu.max=200000 100000 pids.max=$pids"*) ;;
    *) echo "FAIL: reported limits are wrong"; failures=$((failures + 1)) ;;
  esac

  # Touches at most 4 GiB, which exceeds the cgroup limit.
  status=0
  output="$(bounded memory-probe 60 perl -e 'my @b; for (1 .. 4096) { push @b, "x" x (1 << 20) } print "allocated all\n"' 2>&1)" || status=$?
  echo "$output"
  case "$output" in
    *"allocated all"*) echo "FAIL: memory limit did not stop the probe"; failures=$((failures + 1)) ;;
    *"oom_kills=0"*) echo "FAIL: no out-of-memory kill was recorded"; failures=$((failures + 1)) ;;
    *) echo "PASS: memory limit killed the probe (exit $status)" ;;
  esac

  status=0
  output="$(bounded pid-probe 10 perl -e 'my $n = 0; while ($n < 400) { my $p = fork(); last unless defined $p; if (!$p) { sleep 5; exit } $n++ } print "forked $n\n"' 2>&1)" || status=$?
  echo "$output"
  case "$output" in
    *"forked 400"*) echo "FAIL: pid limit did not stop the probe"; failures=$((failures + 1)) ;;
    *forked*) echo "PASS: pid limit stopped forking" ;;
    *) echo "FAIL: pid probe gave no result"; failures=$((failures + 1)) ;;
  esac

  status=0
  bounded timeout-probe 2 sleep 30 || status=$?
  if [ "$status" -eq 124 ]; then
    echo "PASS: timeout stopped the command"
  else
    echo "FAIL: timeout probe exited $status"
    failures=$((failures + 1))
  fi

  [ "$failures" -eq 0 ]
}

preflight
command="${1:-check}"
[ $# -eq 0 ] || shift
case "$command" in image | msrv-image | msrv) ;; *) require_image ;; esac
case "$command" in
  image) build_image ;;
  msrv-image)
    require_image
    "${docker[@]}" build --pull=false --tag "$msrv_image" \
      --build-arg BASE="$image" --build-arg TOOLCHAIN="$msrv" - < "$repo/scripts/msrv.Dockerfile"
    ;;
  msrv) msrv_check ;;
  deps) fetch_crates ;;
  check) check ;;
  limits) limits ;;
  full) check; limits ;;
  verify) verify ;;
  exec) seconds="$1"; shift; bounded exec "$seconds" "$@" ;;
  *) sed -n '2,15p' "$0"; exit 2 ;;
esac
