#!/usr/bin/env bash
# A/B throughput: build BASE (default 5fc1252) and the working tree, run the
# Go bench RUNS times per MODE on throwaway servers bound to free ports,
# alternating sides, and print per-benchmark medians.
#   scripts/ab_bench.sh                  BASE=c391cfd RUNS=3 MODES="key" scripts/ab_bench.sh
set -euo pipefail
root="$(cd "$(dirname "$0")/.." && pwd)"
base="${BASE:-5fc1252}"
runs="${RUNS:-5}"
modes="${MODES:-key mix}"
out="${OUT:-${TMPDIR:-/tmp}/fyrodb-ab}"
mkdir -p "$out"

src="$out/src-$base"
if [[ ! -d "$src" ]]; then
    git -C "$root" worktree add --detach "$src" "$base" >/dev/null
fi
(cd "$src" && CARGO_TARGET_DIR="$out/target-$base" cargo build --release -q)
(cd "$root" && cargo build --release -q)
base_bin="$out/target-$base/release/fyro_db"
cand_bin="${CARGO_TARGET_DIR:-$root/target}/release/fyro_db"
(cd "$root/bench" && go build -o "$out/fyrobench" .)

rm -f "$out"/run-*.json
for i in $(seq 1 "$runs"); do
    for m in $modes; do
        order="base cand"
        (( i % 2 == 0 )) && order="cand base"
        for side in $order; do
            bin="$base_bin"
            [[ "$side" == cand ]] && bin="$cand_bin"
            FYRODB_BIN="$bin" BENCH_BIN="$out/fyrobench" \
                "$root/scripts/bench_local.sh" -m "$m" -json "$out/run-$side-$m-$i.json" >/dev/null
            echo "run $i $m $side done" >&2
        done
    done
done
python3 "$root/scripts/bench_median.py" "$out" --base-label "$base"
