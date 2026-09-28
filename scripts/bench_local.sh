#!/usr/bin/env bash
# Start a throwaway FyroDB, run the Go bench against it, stop the server.
#   scripts/bench_local.sh [bench args...]        e.g. -m lat -rate 500000
# Env: FYRODB_BIN (default target/release/fyro_db), FYRODB_WORKERS, BENCH_BIN.
set -euo pipefail
root="$(cd "$(dirname "$0")/.." && pwd)"
bin="${FYRODB_BIN:-${CARGO_TARGET_DIR:-$root/target}/release/fyro_db}"
work="$(mktemp -d)"
cleanup() {
    if ! kill -0 "$pid" 2>/dev/null; then
        echo "── server exited early; log tail:" >&2
        tail -20 "$work/server.log" >&2
    fi
    kill "$pid" 2>/dev/null || true
    wait "$pid" 2>/dev/null || true
    rm -rf "$work"
}
trap cleanup EXIT

FYRODB_RDB_PATH="$work/bench.rdb" "$bin" >"$work/server.log" 2>&1 &
pid=$!
for _ in $(seq 1 100); do
    if (exec 3<>/dev/tcp/127.0.0.1/8000) 2>/dev/null; then break; fi
    sleep 0.05
done

# Paths given by the caller stay relative to the caller, not bench/.
args=()
prev=""
for a in "$@"; do
    if [[ ( "$prev" == "-json" || "$prev" == "-ann-dir" ) && "$a" != /* ]]; then
        a="$PWD/$a"
    fi
    args+=("$a")
    prev="$a"
done

cd "$root/bench"
if [[ -n "${BENCH_BIN:-}" ]]; then
    "$BENCH_BIN" -pid "$pid" "${args[@]}"
else
    go run . -pid "$pid" "${args[@]}"
fi
