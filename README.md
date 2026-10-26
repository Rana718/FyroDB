# FyroDB

A Redis-compatible in-memory KV store **and** a PostgreSQL query caching proxy — both in one Rust binary. The KV engine speaks RESP2/RESP3; any Redis client works. The proxy speaks full PostgreSQL wire protocol; any Postgres driver works.

**v0.2.4** — mio-based non-blocking proxy event loop, +32% extended-protocol throughput, deep test coverage. → [Changelog](https://fyrodb.vercel.app/docs/changelog-0-2-4)

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

**Key numbers (6-core i5-11400H, 100 clients, 1M ops):**

| Benchmark | FyroDB | Redis Cluster (12 nodes) | Speedup |
|---|---|---|---|
| Pipelined GET | 53.38M ops/sec | 11.62M ops/sec | 4.59× |
| Pipelined SET | 39.62M ops/sec | 6.53M ops/sec | 6.07× |
| Mixed SET/GET | 45.87M ops/sec | 5.82M ops/sec | 7.88× |
| INCR (counters) | 108.56M ops/sec | 6.54M ops/sec | 16.6× |
| Pub/Sub delivery | 113.98M msg/sec | 8.09M msg/sec | 14.1× |
| Hot Key contention | 37.68M ops/sec | 1.61M ops/sec | 23.4× |

Full benchmark table → [Benchmarks docs](https://fyrodb.vercel.app/docs/benchmarks)

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

**Proxy numbers (cache hits, no upstream):**

| Benchmark | Throughput |
|---|---|
| Simple query TCP | ~7.6M ops/sec |
| Extended protocol TCP | ~5M ops/sec |
| In-memory cache lookup | ~10M ops/sec |

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
