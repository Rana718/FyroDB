# FyroDB

FyroDB is a high-performance database optimized for low latency and high concurrency.

## CI/CD Status

PRs to `main` run the following required checks for changes to Rust code (including tests, benchmarks, and proofs), Cargo manifests and lockfiles, or workspace Cargo and toolchain configuration. Docs-only and Go-client-only PRs skip these jobs after a changed-file check. Mixed PRs with Rust changes run the full suite.

- **Test**: Workspace-wide integration and unit tests.
- **Lint**: Clippy with strict warning enforcement.
- **Verus**: Formal safety invariant verification.
- **Benchmark client validation**: Verification of benchmark harness.
- **E2E Integration**: Stress testing via Dockerized deployment.

## Benchmarks

On an Intel i5-11400H with 12 workers, the native Go client measured **49.75M pipelined GET/s** and **44.27M pipelined SET/s** over loopback TCP. The embedded Rust suite reached **128.74M GET/s** and **82.59M SET/s**. Each figure is the median of three runs at `021baed` with ten million operations per KV phase and five-byte values. Median whole-suite peak TCP server RSS was **1,096 MiB**. [Full results and reproduction commands](https://fyrodb.vercel.app/docs/benchmarks) · [Measurements in this checkout](docs/public/benchmarks/2026-11-19/results.json)
