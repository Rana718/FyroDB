# Contributing to FyroDB

Thank you for your interest in contributing to FyroDB! 🎉

FyroDB is a high-performance system combining two engines: a Redis-compatible in-memory key-value store (RESP2/RESP3) and a PostgreSQL query caching proxy (full wire protocol). Both run in the same Rust binary. Performance, low latency, safe concurrency, and minimal resource footprints are core project goals.

Please review this guide before submitting issues or pull requests.

---

## Table of Contents

1. [Code of Conduct & Core Principles](#core-principles)
2. [Prerequisites & Environment Setup](#prerequisites--environment-setup)
3. [Development Workflow](#development-workflow)
   - [Building](#building)
   - [Running Tests](#running-tests)
   - [Linting and Code Quality](#linting-and-code-quality)
   - [Running Benchmarks](#running-benchmarks)
   - [Documentation](#documentation)
4. [Architecture & Coding Guidelines](#architecture--coding-guidelines)
   - [Zero-Regression Policy](#zero-regression-policy)
   - [Concurrency & Memory Safety](#concurrency--memory-safety)
   - [Zero-Copy & Allocation Awareness](#zero-copy--allocation-awareness)
   - [Hash Collections & Data Structures](#hash-collections--data-structures)
   - [Adding a New Command](#adding-a-new-command)
   - [Binary Size Discipline](#binary-size-discipline)
5. [Submitting Pull Requests](#submitting-pull-requests)
6. [Issue Reporting](#issue-reporting)

---

## Core Principles

- **Performance First**: Any PR that causes a performance regression (even ~1%) without strong justification will not be merged.
- **Zero Allocations on Hot Paths**: Hot paths (`GET`, `SET`, `INCR`, `LPUSH`, `HSET`, etc.) must avoid unnecessary heap allocations, String clones, or heavy formatting macros.
- **Compatibility**: Redis wire protocol (RESP2/RESP3) compatibility must be preserved for supported commands.
- **Simplicity**: Prefer clear, bounded, cache-friendly data structures with explicit protection and ownership.

---

## Prerequisites & Environment Setup

- **Rust Toolchain**: Rust 2024 edition (stable channel).
- **Linker** (optional, recommended for fast builds): `clang` and `lld`.
- **Taskfile** (optional, recommended): [Task](https://taskfile.dev/) (`go-task`).
- **Go** (for running benchmark suites in `bench/`): Go 1.20+.
- **Docker & Docker Compose** (optional, for Redis Cluster baseline benchmarks).

To check your Rust setup:
```bash
rustc --version
cargo --version
```

---

## Development Workflow

We provide a `Taskfile.yml` for convenience. You can use `task <cmd>` or standard `cargo` commands.

### Building

```bash
# Debug build
cargo build

# Optimized release build
task build
# or
cargo build --release
```

### Running Tests

Unit and integration tests live in `tests/`:

```bash
task test
# Complete workspace validation:
cargo test --workspace
cargo test --features io-uring --test large_replies
```

To run a specific test suite:
```bash
cargo test --test string
cargo test --test pubsub
```

### Linting and Code Quality

All code must pass `clippy` with zero warnings before submitting a PR:

```bash
task lint
# or
cargo clippy -- -D warnings
```

Format code according to standard Rust formatting:
```bash
cargo fmt --check
```

### Running Benchmarks

FyroDB includes an automated benchmarking suite written in Go (`bench/`):

```bash
# Start FyroDB in release mode (terminal 1)
task run

# Run full benchmark against FyroDB (terminal 2)
task bench-fyrodb

# Run key-value only benchmarks
task bench-key-fyrodb

# Run Pub/Sub benchmarks
task bench-pub-fyrodb
```

To compare against local Redis or Redis Cluster:
```bash
# Redis standalone benchmark
task bench-redis

# Start 6-node Redis cluster via Docker
task up
task bench-redis-cluster
task down
```

Compare like-for-like builds with the Docker normal runner and independent clients; see [BENCHMARKS.md](BENCHMARKS.md). Keep medians, min/max, all attempts, keyspace, payloads, immutable images, CPU placement, process priorities, RSS and container memory. A shared-client sweep is a different budget from dedicated clients. Verify equal niceness before timing; do not silently drop contended or failed attempts.

The versioned evidence and SVGs are generated deterministically:

```bash
python3 scripts/update_benchmark_docs.py
python3 scripts/update_benchmark_docs.py --check
```

Regenerate after editing the benchmark page's source text because BENCHMARKS.md mirrors it. Add a new evidence campaign rather than replacing historical numbers with selected maxima. The generator checks archived hashes and selected raw-sample/summary consistency; it does not run a benchmark.

### Documentation

The documentation lives in the `docs/` folder:

```bash
task docs-dev    # Start local docs dev server
task docs-build  # Build static docs
```

---

## Architecture & Coding Guidelines

Before writing code, please read [`ARCHITECTURE.md`](ARCHITECTURE.md) to understand command isolation, map admission, per-entry read/write guards, EBR and the worker event loops.

### Zero-Regression Policy

Performance goals must be supported by matched measurements; no fixed speedup over another database is assumed. When touching critical execution paths:
1. Benchmark before and after your changes.
2. Ensure instruction counts, cache misses, and heap allocations do not increase.
3. Validate binary size does not inflate unexpectedly.

### Concurrency & Memory Safety

- Preserve command isolation for EVAL/EVALSHA, WATCH and EXEC; normal work shares worker slots and atomic work excludes every slot.
- Mutable values require an entry read/write guard or a successfully admitted map read epoch. EBR keeps retired memory alive; it does not prevent data races on String, Vec or collections.
- Mutations require map write admission and the entry write lock. Keep snapshot pre-image capture, WATCH invalidation and durable/replicated write hooks in optimized paths.
- Keep sequentially consistent admission and acquire/release publication orderings unless a separately reviewed proof and tests establish an alternative.
- Test partial/error exits, nested scopes, duplicate keys, short socket writes, concurrent reads/writes and SAVE/restart.
- Reject an optimization when real workloads regress or the gain is unproven; fewer instructions or locks alone is insufficient.

### Zero-Copy & Allocation Awareness

- Write RESP replies directly into `out: &mut Vec<u8>` using helpers in `src/utils/resp.rs` rather than `format!()` or allocating intermediate strings.
- Command parsing should operate on byte slices or `&str` references directly from the connection read buffer.

### Hash Collections & Data Structures

- For internal associative collections (e.g. within stored values, sets, stream groups, connection maps), use `foldhash::HashMap` / `foldhash::HashSet` rather than `std::collections::HashMap` with SipHash.
- `foldhash` provides 20–40% faster hashing for trusted keys and avoids duplicate hash machinery monomorphisation.

### Adding a New KV Command

When adding or expanding Redis commands:
1. **Define the command enum**: Add the variant and uppercase string to `string_enum!` in `src/commands/mod.rs`. (Command resolution is $O(1)$ via a static `foldhash` map).
2. **Implement storage logic**: Add the data structure mutation/query in `src/storage/<type>.rs`.
3. **Implement command handler**: Add the parameter parsing and response generation in `src/commands/<type>.rs`.
4. **Hot-path dispatch (optional)**: If the command is high-frequency, add a first-byte fast-path check in `src/handler/dispatch.rs`.
5. **Add tests**: Create comprehensive test cases under `tests/<type>.rs`.

### Working on the DB Cache Proxy

The proxy lives entirely in `crates/fyro-dbcache/`. Key files:

- `src/proxy/mio_worker.rs` — the mio event loop: `ReadBuf`/`WriteBuf`, `ProxyConn` state machine, `BlockingPool`, `run_proxy_worker()`
- `src/proxy/session.rs` — upstream connection handling and `handle_sync` with `flush_out_batched`
- `src/cache/encoded.rs` — PostgreSQL wire-format cache hit serving
- `src/query.rs` — SQL fingerprinting and table extraction

Guidelines:
- Cache-hit traffic must stay inline inside the mio event loop — never hand a hit to the blocking pool.
- `flush_out_batched` in `session.rs` must coalesce writes up to `RELAY_FLUSH_BYTES` (32KB) — do not replace it with a direct `flush_out()` call on any hot path.
- Tests live in `tests/dbcache_proxy_e2e.rs`. Tests that use pre-stored cache entries must use `start_with(DbProxyMode::Dual, None)` (no upstream) so `context_hash = 0` and fingerprints match stored entries.
- Extended-protocol tests: do not call `read_until_ready()` after `client.parse()` — `ParseComplete` coalesces with the first `Sync` response.

### Binary Size Discipline

Release profiles are configured with `lto = true`, `codegen-units = 1`, `panic = "abort"`, and `strip = true`. Keep binaries lean:
- Avoid adding heavy external dependencies.
- Avoid large macros that expand to extensive repetitive code or unnecessary trait implementations.

---

## Submitting Pull Requests

1. **Fork and Branch**: Create a feature branch from `main` (e.g. `feat/bitfield-command` or `perf/custommap-probe`).
2. **Follow Conventional Commits**:
   - `perf:` for performance optimizations
   - `feat:` for new commands or features
   - `fix:` for bug fixes
   - `test:` for test additions or improvements
   - `docs:` for documentation updates
   - `refactor:` for code restructuring without behavioral changes
3. **Verify Locally**:
   - `cargo test` passes
   - `cargo clippy -- -D warnings` has no warnings
   - Benchmarks show no regressions for performance-related PRs
4. **Open a PR**: Fill out the pull request template with description, rationale, and benchmark numbers (if applicable).

---

## Issue Reporting

- **Bug Reports**: Please include minimum reproducible steps, Redis client used, OS, hardware, and expected vs actual behavior.
- **Feature / Command Requests**: Include Redis specification references and sample use cases.
- **Performance Inquiries**: Include full benchmark parameters (`bench/` command line, client count, hardware specs).

---

Thank you for helping make FyroDB lightning fast! 🚀
