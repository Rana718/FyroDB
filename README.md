# FyroDB

FyroDB is a high-performance database optimized for low latency and high concurrency.

## CI/CD Status
The CI pipeline is fully automated and requires the following checks for all PRs:
- **Test**: Workspace-wide integration and unit tests.
- **Lint**: Clippy with strict warning enforcement.
- **Verus**: Formal safety invariant verification.
- **Benchmark client validation**: Verification of benchmark harness.
- **E2E Integration**: Stress testing via Dockerized deployment.

## Performance Audit & Algorithms
For detailed algorithm descriptions, recent changes, and latency metrics, please refer to [PERFORMANCE_AUDIT.md](PERFORMANCE_AUDIT.md).
