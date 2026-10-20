#!/usr/bin/env bash
# ==============================================================================
# task-4: FyroDB 4-core / 8-worker server  +  2-core / 4-worker bench runner
#
# Server  → pinned to cores 0-3  (8 worker threads via FYRODB_WORKERS=8)
# Bench   → pinned to cores 4-5  (4 goroutines via GOMAXPROCS=4)
#
# Usage:
#   scripts/bench_task4.sh [extra bench flags]
#   task task-4                              # same, no extra flags
#   task task-4 -- -m key                   # KV-only mode
#
# Env overrides:
#   FYRODB_BIN      – path to fyro_db binary  (default: target/release/fyro_db)
#   FYRODB_PORT     – TCP port                 (default: auto-selected free port)
#   SERVER_CORES    – taskset cpu-list for server  (default: 0-3)
#   BENCH_CORES     – taskset cpu-list for bench   (default: 4-5)
# ==============================================================================
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BIN="${FYRODB_BIN:-${CARGO_TARGET_DIR:-$ROOT/target}/release/fyro_db}"
SERVER_CORES="${SERVER_CORES:-0-3}"
BENCH_CORES="${BENCH_CORES:-4-5}"
WORK="$(mktemp -d)"
PID=""

# ── helpers ──────────────────────────────────────────────────────────────────
die()     { echo "❌ $*" >&2; exit 1; }
section() { echo; echo "── $* ──────────────────────────────────────────────"; }

cleanup() {
    if [[ -n "$PID" ]] && ! kill -0 "$PID" 2>/dev/null; then
        section "Server exited early – last 30 lines of log"
        tail -30 "$WORK/server.log" >&2 || true
    fi
    kill "$PID" 2>/dev/null || true
    wait "$PID"  2>/dev/null || true
    rm -rf "$WORK"
}
trap cleanup EXIT

# ── binary check ─────────────────────────────────────────────────────────────
[[ -x "$BIN" ]] || die "Binary not found: $BIN  (run: cargo build --release)"

# ── pick a free port ─────────────────────────────────────────────────────────
PORT="${FYRODB_PORT:-$(python3 -c \
    'import socket; s=socket.socket(); s.bind(("127.0.0.1",0)); print(s.getsockname()[1]); s.close()')}"

# ── check taskset availability ───────────────────────────────────────────────
USE_TASKSET=0
if command -v taskset &>/dev/null; then
    USE_TASKSET=1
fi

# ── launch server (8 workers, 4-core pin) ─────────────────────────────────────
section "Starting FyroDB — 8 workers (server pinned to cores ${SERVER_CORES})"
echo "    Binary : $BIN"
echo "    Port   : $PORT"
echo "    Workers: 8"

SERVER_ENV=(
    MIMALLOC_ALLOW_THP=0
    MIMALLOC_ABANDONED_PAGE_PURGE=1
    MIMALLOC_PURGE_DECOMMITS=1
    MIMALLOC_PURGE_DELAY=0
    MIMALLOC_ARENA_RESERVE=0
    MIMALLOC_ARENA_EAGER_COMMIT=0
    FYRODB_WORKERS=8
    FYRODB_PORT="$PORT"
    FYRODB_RDB_PATH="$WORK/bench.rdb"
)

if [[ "$USE_TASKSET" == 1 ]]; then
    env "${SERVER_ENV[@]}" taskset -c "$SERVER_CORES" "$BIN" \
        >"$WORK/server.log" 2>&1 &
else
    echo "    ⚠ taskset not available – server not pinned to cores"
    env "${SERVER_ENV[@]}" "$BIN" >"$WORK/server.log" 2>&1 &
fi
PID=$!

# ── wait for server ready ─────────────────────────────────────────────────────
section "Waiting for server on port $PORT"
READY=0
for _ in $(seq 1 120); do
    if ! kill -0 "$PID" 2>/dev/null; then
        section "Server crashed before accepting connections"
        cat "$WORK/server.log" >&2
        exit 1
    fi
    if (exec 3<>/dev/tcp/127.0.0.1/"$PORT") 2>/dev/null; then
        READY=1
        break
    fi
    sleep 0.05
done
[[ "$READY" == 1 ]] || { cat "$WORK/server.log" >&2; die "Server did not become ready in time"; }
echo "    ✅ Server ready (PID $PID)"

# ── run benchmark (4 goroutines, 2-core pin) ──────────────────────────────────
section "Running full benchmark — GOMAXPROCS=4 (bench pinned to cores ${BENCH_CORES})"
echo "    Bench goroutines : 4 (GOMAXPROCS=4)"

# Resolve any relative path args to absolute so they survive the cd into bench/
ARGS=()
PREV=""
for A in "$@"; do
    if [[ ( "$PREV" == "-json" || "$PREV" == "-ann-dir" ) && "$A" != /* ]]; then
        A="$PWD/$A"
    fi
    ARGS+=("$A")
    PREV="$A"
done

cd "$ROOT/bench"

BENCH_CMD=(go run . -p "$PORT" -pid "$PID" "${ARGS[@]+"${ARGS[@]}"}")

if [[ "$USE_TASKSET" == 1 ]]; then
    GOMAXPROCS=4 taskset -c "$BENCH_CORES" "${BENCH_CMD[@]}"
else
    echo "    ⚠ taskset not available – bench not pinned to cores"
    GOMAXPROCS=4 "${BENCH_CMD[@]}"
fi
