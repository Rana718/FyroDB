#!/usr/bin/env bash
# ==============================================================================
# FyroDB 10M & 20M+ QPS High-Performance Benchmark Runner
# ==============================================================================
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
TARGET="${1:-all}"

echo "=========================================================================="
echo "          🚀 FYRODB EXTREME THROUGHPUT BENCHMARK (10M / 20M+ QPS)        "
echo "=========================================================================="

case "$TARGET" in
    10m|10M|"2core"|"2workers")
        echo -e "\n🔥 Running 10M+ QPS Benchmark on EXACTLY 2 WORKERS..."
        cargo test --release --test two_worker_bench -- --nocapture
        ;;
    20m|20M|"4workers"|"scale")
        echo -e "\n🔥 Running 20M+ QPS Benchmark on EXACTLY 4 WORKERS (2 CORES)..."
        cargo test --release --test four_worker_two_core_bench -- --nocapture
        ;;
    all|ALL)
        echo -e "\n[1/2] Running 10M+ QPS Suite (2 Workers)..."
        cargo test --release --test two_worker_bench -- --nocapture
        echo -e "\n[2/2] Running 20M+ QPS Suite (Scale-Up Workers)..."
        cargo test --release --test twenty_million_qps_bench -- --nocapture
        ;;
    *)
        echo "Usage: $0 [10m | 20m | all]"
        exit 1
        ;;
esac

echo -e "\n✅ Benchmark run completed successfully!"
