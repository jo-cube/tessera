#!/usr/bin/env bash
# Runs Tessera's validation inside a resource-limited Linux container.
#
#   scripts/validate.sh image              build the toolchain image (online)
#   scripts/validate.sh deps               resolve and fetch crates (online)
#   scripts/validate.sh [check]            fmt, Clippy, tests and docs (default)
#   scripts/validate.sh limits             the large-allocation boundary tests
#   scripts/validate.sh full               check, then limits
#   scripts/validate.sh bench              the block and run benchmark
#   scripts/validate.sh geometry           run block size and codec comparison
#   scripts/validate.sh prefix             prefix scan comparison
#   scripts/validate.sh merge              multi-run merge measurements
#   scripts/validate.sh logical            latest-state read measurements
#   scripts/validate.sh ranges             key-range pruning measurements
#   scripts/validate.sh memtable           memtable and flush measurements
#   scripts/validate.sh engine             integrated engine measurements
#   scripts/validate.sh compaction         sustained writes, three compaction modes
#   scripts/validate.sh snapshot           snapshots compared with read guards
#   scripts/validate.sh frequency          frequent and long-lived snapshots
#   scripts/validate.sh retention          sustained snapshot retention and backpressure
#   scripts/validate.sh persistent         memtable tree costs under held snapshots
#   scripts/validate.sh pacing             background compaction under frequent snapshots
#   scripts/validate.sh profile            memtable and engine write cost by component
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
readonly INNER='
limit=$1; shift
start=$(date +%s)
status=0
timeout --kill-after=5 "$limit" "$@" || status=$?
oom=$(awk "\$1 == \"oom_kill\" { print \$2 }" /sys/fs/cgroup/memory.events)
peak=$(( $(cat /sys/fs/cgroup/memory.peak) / 1048576 ))
echo "[$STEP] exit=$status peak=${peak}MiB oom_kills=$oom wall=$(( $(date +%s) - start ))s"
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
    --volume "$source_dir:/work:ro" --volume "$cache_mount" --volume "$crates_mount" \
    ${extra_mounts[@]+"${extra_mounts[@]}"} --workdir /work \
    --env STEP="$label" --env CARGO_HOME=/crates --env CARGO_TARGET_DIR=/cache/target \
    --env CARGO_BUILD_JOBS=2 --env RUST_TEST_THREADS=2 --env CARGO_NET_OFFLINE="$offline" \
    "$image" sh -c "$INNER" sh "$seconds" "$@"
}

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
  bounded clippy 300 cargo clippy --locked --all-targets -- -D warnings
  bounded build-tests 300 cargo test --locked --no-run
  bounded test 300 cargo test --locked
  bounded doc 120 env RUSTDOCFLAGS="-D warnings" cargo doc --locked --no-deps
}

limits() {
  bounded limits 180 cargo test --locked --test block_limits -- --ignored --test-threads=1
}

bench() {
  bounded bench 900 cargo run --locked --release --example block_bench
}

geometry() {
  bounded geometry 900 cargo run --locked --release --example block_bench -- geometry
}

prefix() {
  bounded prefix 900 cargo run --locked --release --example block_bench -- prefix
}

merge() {
  bounded merge 900 cargo run --locked --release --example block_bench -- merge
}

logical() {
  bounded logical 900 cargo run --locked --release --example block_bench -- logical
}

ranges() {
  bounded ranges 900 cargo run --locked --release --example block_bench -- ranges
}

memtable() {
  bounded memtable 900 cargo run --locked --release --example block_bench -- memtable
}

engine() {
  bounded engine 900 cargo run --locked --release --example block_bench -- engine
}

compaction() {
  bounded compaction 900 cargo run --locked --release --example block_bench -- compaction
}

snapshot() {
  bounded snapshot 900 cargo run --locked --release --example block_bench -- snapshot
}

frequency() {
  bounded frequency 900 cargo run --locked --release --example block_bench -- snapshot-frequency
}

retention() {
  bounded retention 900 cargo run --locked --release --example block_bench -- retention
}

persistent() {
  bounded persistent 900 cargo run --locked --release --example block_bench -- persistent
}

pacing() {
  bounded pacing 900 cargo run --locked --release --example block_bench -- pacing
}

profile() {
  bounded profile 900 cargo run --locked --release --example block_bench -- profile
}

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
[ "$command" = image ] || require_image
case "$command" in
  image) build_image ;;
  deps) fetch_crates ;;
  check) check ;;
  limits) limits ;;
  full) check; limits ;;
  bench) bench ;;
  geometry) geometry ;;
  prefix) prefix ;;
  merge) merge ;;
  logical) logical ;;
  ranges) ranges ;;
  memtable) memtable ;;
  engine) engine ;;
  compaction) compaction ;;
  snapshot) snapshot ;;
  frequency) frequency ;;
  retention) retention ;;
  persistent) persistent ;;
  pacing) pacing ;;
  profile) profile ;;
  verify) verify ;;
  exec) seconds="$1"; shift; bounded exec "$seconds" "$@" ;;
  *) sed -n '2,25p' "$0"; exit 2 ;;
esac
