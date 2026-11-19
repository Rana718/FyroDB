# FyroDB

FyroDB is a high-performance database optimized for low latency and high concurrency.

## CI/CD Status
The CI pipeline is fully automated and requires the following checks for all PRs:
- **Test**: Workspace-wide integration and unit tests.
- **Lint**: Clippy with strict warning enforcement.
- **Verus**: Formal safety invariant verification.
- **Benchmark client validation**: Verification of benchmark harness.
- **E2E Integration**: Stress testing via Dockerized deployment.

## Benchmarks

On an Intel i5-11400H with 12 workers, the native Go client measured **49.75M pipelined GET/s** and **44.27M pipelined SET/s** over loopback TCP. The embedded Rust suite reached **128.74M GET/s** and **82.59M SET/s**. Each figure is the median of three runs at `021baed` with ten million operations per KV phase and five-byte values. Median whole-suite peak TCP server RSS was **1,096 MiB**. [Full results and reproduction commands](https://fyrodb.vercel.app/docs/benchmarks) · [Measurements in this checkout](docs/public/benchmarks/2026-11-19/results.json)
