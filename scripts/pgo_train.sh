#!/usr/bin/env bash
# Training workload for PGO/BOLT: run the instrumented server under the
# representative bench mix (KV, pipelined runs, data types, vector search).
#   scripts/pgo_train.sh [bolt]
set -euo pipefail
root="$(cd "$(dirname "$0")/.." && pwd)"
triple="$(rustc -vV | sed -n 's/^host: //p')"
target="${CARGO_TARGET_DIR:-$root/target}"
if [[ "${1:-}" == "bolt" ]]; then
    bin="$target/$triple/release/fyro_db-bolt-instrumented"
else
    bin="$target/$triple/release/fyro_db"
fi
export FYRODB_BIN="$bin"
"$root/scripts/bench_local.sh" -m key
"$root/scripts/bench_local.sh" -m mix
"$root/scripts/bench_local.sh" -m lat -dur 10s -keys 500000
"$root/scripts/bench_local.sh" -m ann -ann-synth-n 50000 -ann-q 500 -ann-efs 40 -ann-conc 1,8
