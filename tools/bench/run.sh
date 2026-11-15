#!/usr/bin/env bash
# Runs the full Tessera vs RocksDB comparison on a 4 GiB APFS RAM disk.
# Results go to .work/bench/<date>-<sha>/. See README.md.
set -euo pipefail
root=$(cd "$(dirname "$0")/../.." && pwd)
rocks=$HOME/.cache/tessera-bench/rocksdb-11.13.2
[ -f "$rocks/librocksdb.a" ] || { echo "missing $rocks/librocksdb.a; see tools/bench/README.md" >&2; exit 1; }
export ROCKSDB_DIR=$rocks CARGO_TARGET_DIR=$root/target
export MACOSX_DEPLOYMENT_TARGET=$(sw_vers -productVersion | cut -d. -f1).0
cargo build --release --locked --quiet --manifest-path "$root/tools/bench/Cargo.toml"

sha=$(git -C "$root" rev-parse --short HEAD)
out=$root/.work/bench/$(date +%Y-%m-%d)-$sha${TESSERA_BENCH_SCALE:+-scale$TESSERA_BENCH_SCALE}${1:+-$(date +%H%M%S)}
mkdir -p "$out"
[ ! -e /Volumes/tessbench ] || { echo "/Volumes/tessbench already exists" >&2; exit 1; }
sectors=8388608
if [[ " $* " == *" --mode streaming "* ]]; then sectors=12582912; fi
dev=$(hdiutil attach -nomount ram://$sectors | awk '{print $1}')
trap 'hdiutil detach "$dev" >/dev/null || hdiutil detach -force "$dev" >/dev/null' EXIT
diskutil erasevolume APFS tessbench "$dev" >/dev/null

{
  echo "date: $(date '+%Y-%m-%d %H:%M:%S %z')"
  echo "sha: $(git -C "$root" rev-parse HEAD) (branch $(git -C "$root" branch --show-current), $(git -C "$root" status --porcelain | wc -l | tr -d ' ') uncommitted paths)"
  echo "machine: $(sysctl -n machdep.cpu.brand_string), $(sysctl -n hw.physicalcpu) cores ($(sysctl -n hw.perflevel0.physicalcpu)P+$(sysctl -n hw.perflevel1.physicalcpu)E), $(( $(sysctl -n hw.memsize) >> 30 )) GiB"
  echo "macOS: $(sw_vers -productVersion); $(rustc --version)"
  echo "power: $(pmset -g batt | head -1)"
  echo "rocksdb: $rocks (librocksdb.a sha256 $(shasum -a 256 "$rocks/librocksdb.a" | cut -c1-16))"
} > "$out/environment.txt"
"$CARGO_TARGET_DIR/release/tessera-bench" all "$out" "$@"
