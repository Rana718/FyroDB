# FyroDB

FyroDB is a high-performance database optimized for low latency and high concurrency.

## CI/CD Status

PRs run intelligent CI/CD routing to ensure fast feedback and conserve runner resources:

- **Initial PR Creation**: When a PR is opened, the entire test suite runs once (`Test`, `Lint`, `Verus`, `Benchmark client validation`, and `E2E Integration`).
- **Subsequent Pushes**: When additional commits are pushed to an open PR, fast feedback checks run (`Test`, `Lint`, and `Verus`).
- **Targeted Label Triggers**:
  - `test-case`: Explicitly runs the workspace tests and protocol regressions.
  - `e2e-testcase`: Explicitly runs the Docker E2E integration and stress test suite.
  - `benchmark-validation`: Explicitly runs the Go benchmark client and matrix verification.
  - PRs are automatically labeled by the PR Labeler based on modified files.
- **Direct Pushes to Main/Dev**: Run the full validation suite.

## Benchmarks

On an Intel i5-11400H with 12 workers, the native Go client measured **49.75M pipelined GET/s** and **44.27M pipelined SET/s** over loopback TCP. The embedded Rust suite reached **128.74M GET/s** and **82.59M SET/s**. Each figure is the median of three runs at `021baed` with ten million operations per KV phase and five-byte values. Median whole-suite peak TCP server RSS was **1,096 MiB**. [Full results and reproduction commands](https://fyrodb.vercel.app/docs/benchmarks) · [Measurements in this checkout](docs/public/benchmarks/2026-11-19/results.json)
