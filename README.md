# FyroDB

<p align="center">
  <a href="https://hub.docker.com/r/rana718/fyrodb"><img src="https://img.shields.io/docker/pulls/rana718/fyrodb?style=flat-square" alt="Docker Pulls"></a>
  <img src="https://img.shields.io/badge/language-Rust-orange?style=flat-square&logo=rust" alt="Rust">
  <a href="https://fyrodb.vercel.app/docs"><img src="https://img.shields.io/badge/docs-online-brightgreen?style=flat-square" alt="Docs"></a>
  <a href="LICENSE"><img src="https://img.shields.io/badge/license-Apache--2.0-green?style=flat-square" alt="License"></a>
</p>

A Redis-compatible in-memory KV store **and** a PostgreSQL query caching proxy — both in one Rust binary. The KV engine speaks RESP2/RESP3; any Redis client works. The proxy speaks full PostgreSQL wire protocol; any Postgres driver works.

**v0.3.1** — Redis Lua scripting engine, zero-allocation write run coalescing, in-process L1 proxy caching, right-sized shard memory, and pub/sub fanout pipeline scaling. → [Changelog](https://fyrodb.vercel.app/docs/changelog-0-3-1)

---

## Install

```bash
# Docker
docker run -p 8000:8000 rana718/fyrodb:latest

# From source
cargo build --release && ./target/release/fyro_db
```

```bash
# Connect (KV / Redis-compatible)
redis-cli -p 8000 SET name rana
redis-cli -p 8000 GET name
```

---

## KV Engine

Redis-compatible — any Redis client, SDK, or CLI works unchanged.

```yaml
services:
  fyrodb:
    image: rana718/fyrodb:latest
    ports:
      - "8000:8000"
    environment:
      FYRODB_PORT: "8000"
      FYRODB_WORKERS: "0"        # 0 = auto (CPU cores)
      FYRODB_MAX_KEYS: "5000000"
      FYRODB_AUTH: "secret"
      FYRODB_RDB_PATH: /data/fyrodb.rdb
    volumes:
      - fyrodb-data:/data
volumes:
  fyrodb-data:
```

**Historical reported rates (6-core i5-11400H, 100 clients, best of three):**

| Benchmark | FyroDB | Redis Cluster (12 nodes) | Speedup |
|---|---|---|---|
| Pipelined GET | 53.38M ops/sec | 11.62M ops/sec | 4.59× |
| Pipelined SET | 39.62M ops/sec | 6.53M ops/sec | 6.07× |
| Mixed SET/GET | 45.87M ops/sec | 5.82M ops/sec | 7.88× |
| INCR (counters) | 108.56M ops/sec | 6.54M ops/sec | 16.6× |
| Pub/Sub delivery | 113.98M msg/sec | 8.09M msg/sec | 14.1× |
| Hot Key contention | 37.68M ops/sec | 1.61M ops/sec | 23.4× |

Full benchmark table → [Benchmarks docs](https://fyrodb.vercel.app/docs/benchmarks)

The current harness uses 10M operations per KV phase, 2M per mixed phase, and 10M for the hot-key phase. The benchmark docs repeat some figures under both TCP and in-process headings; treat the table above as a historical reference, not a matched current measurement. In-process batched storage calls also credit equivalent logical operations and are not TCP request/reply rates.

For repeatable TCP measurements with fixed CPU placement, discarded warmups, seven alternating runs, raw samples, confidence intervals, and memory readings:

```bash
task bench-stable
# Compare another saved binary using the same workload:
task bench-stable -- --server baseline=/absolute/path/to/fyro_db --out temp/bench/comparison
```

Results are written to `temp/bench/stable/summary.json`. Compare medians and variability, not the fastest individual sample. The runner starts isolated servers on free loopback ports, waits up to 60 seconds for a quiet host, and rejects incomplete pub/sub delivery. Use a fresh `--out` directory for another run. Host pressure and server CPU time per completed operation are retained with the samples.

For a sustained hot-key check, add `--modes hotkey --hotkey-seconds 8 --inner 1`. CPU time per completed request helps distinguish a server optimization from a client or host bottleneck.

For ANN comparisons against older builds, verify that background indexing has settled before timing:

```bash
cd bench
ANN_READY_RECALL=0.99 go run . -m ann -ann-synth-n 100000 -ann-synth-dim 128 -ann-q 1000
```

The optional gate requires three consecutive probes at the requested recall and a clear `FT.INFO` indexing flag. It checks 32 queries at EF 320 before the timed sweeps and fails after two minutes if the threshold is not reached. Choose a threshold appropriate to the dataset and index settings; compare query speed together with recall.

Measure the direct engine separately:

```bash
mkdir -p temp/bench
cargo bench --bench without_tcp -- -m bench --json temp/bench/direct.json
```

**Data types:** String · Hash · List · Set · Sorted Set · JSON · Stream · Bitmap · HyperLogLog · Geospatial · Vector Search · Full-Text Search

---

## DB Cache Proxy

Sits in front of any PostgreSQL database. Change one env var in your app — no code changes needed.

```bash
# Before
DATABASE_URL=postgres://user:pass@postgres:5432/mydb

# After (identical wire protocol, cached automatically)
DATABASE_URL=postgres://user:pass@fyrodb:5432/mydb
```

First query for each SQL pattern → forwarded to Postgres, result cached. Every subsequent identical query → served from memory in ~1 µs, no database call.

```yaml
services:
  app:
    build: .
    environment:
      REDIS_URL: redis://fyrodb:8000        # KV / sessions
      DATABASE_URL: postgres://appuser:secret@fyrodb:5432/mydb  # cached queries

  fyrodb:
    image: rana718/fyrodb:latest
    ports:
      - "8000:8000"    # KV / Redis
      - "5432:5432"    # DB Cache Proxy
    environment:
      FYRODB_PROXY_ENABLED: "true"
      FYRODB_PROXY_UPSTREAM_URL: postgres://appuser:secret@postgres:5432/mydb
      FYRODB_PROXY_TRUST_AUTH: "true"
      FYRODB_PROXY_CACHE_TTL_MS: "30000"
      FYRODB_PROXY_WORKERS: "2"

  postgres:
    image: postgres:16
    environment:
      POSTGRES_USER: appuser
      POSTGRES_PASSWORD: secret
      POSTGRES_DB: mydb
```

→ [DB Cache Proxy docs](https://fyrodb.vercel.app/docs/db-cache-proxy)

---

## Configuration

| Variable | Default | Description |
|---|---|---|
| `FYRODB_PORT` | `8000` | KV listen port |
| `FYRODB_WORKERS` | `0` (auto) | Worker threads |
| `FYRODB_AUTH` | (none) | KV password |
| `FYRODB_MAX_KEYS` | `0` (unlimited) | Key ceiling |
| `FYRODB_RDB_PATH` | `fyrodb.rdb` | Snapshot path |
| `FYRODB_PROXY_ENABLED` | `false` | Enable DB cache proxy |
| `FYRODB_PROXY_PORT` | `5432` | Proxy listen port |
| `FYRODB_PROXY_UPSTREAM_URL` | (none) | `postgres://...` upstream |
| `FYRODB_PROXY_WORKERS` | `0` (auto) | Proxy mio threads |
| `FYRODB_PROXY_CACHE_TTL_MS` | `60000` | Result TTL |
| `FYRODB_PROXY_TRUST_AUTH` | `false` | Skip client auth |
| `FYRODB_PROXY_MODE` | `dual` | `dual` or `cache-only` |

Full reference → [Configuration docs](https://fyrodb.vercel.app/docs/configuration)

---

## Links

| | |
|---|---|
| Documentation | https://fyrodb.vercel.app/docs/introduction |
| Architecture | [ARCHITECTURE.md](ARCHITECTURE.md) |
| Changelog | [CHANGELOG.md](CHANGELOG.md) |
| Contributing | [CONTRIBUTING.md](CONTRIBUTING.md) |
| Docker Hub | https://hub.docker.com/r/rana718/fyrodb |

Built on [`customhash`](https://www.ranadolui.me/blog/custom-concurrent-hashmap-rust) — a sharded lock-free concurrent hash map with epoch-based reclamation and per-key seqlock.

## License

Apache 2.0
