<p align="center">
  <img src="docs/public/logo.png" width="220" alt="FyroDB Logo" />
</p>

<h1 align="center">⚡ FyroDB</h1>

<p align="center">
  <b>Ultra-high throughput, sub-millisecond in-memory multi-model database & query acceleration cache built in Rust.</b>
</p>

<p align="center">
  <i>Drop-in Redis® wire-compatible &bull; Native Vector Search (HNSW) &bull; Native JSON &bull; PostgreSQL Cache Proxy &bull; Thread-per-core</i>
</p>

<p align="center">
  <a href="https://github.com/Rana718/FyroDB/releases"><img src="https://img.shields.io/github/v/release/Rana718/FyroDB?color=orange&label=version" alt="Latest Release"></a>
  <a href="https://hub.docker.com/r/rana718/fyrodb"><img src="https://img.shields.io/docker/pulls/rana718/fyrodb?color=blue&logo=docker" alt="Docker Pulls"></a>
  <a href="https://github.com/Rana718/FyroDB/actions/workflows/ci.yml"><img src="https://github.com/Rana718/FyroDB/actions/workflows/ci.yml/badge.svg" alt="CI Status"></a>
  <a href="https://github.com/Rana718/FyroDB/actions/workflows/verus.yml"><img src="https://github.com/Rana718/FyroDB/actions/workflows/verus.yml/badge.svg" alt="Verus Invariants"></a>
  <a href="https://fyrodb.vercel.app"><img src="https://img.shields.io/badge/docs-fyrodb.vercel.app-6366f1?logo=vercel" alt="Documentation"></a>
  <a href="LICENSE"><img src="https://img.shields.io/badge/license-Apache%202.0%20%7C%20MIT-blue.svg" alt="License"></a>
</p>

---

## 📖 Table of Contents

- [Overview](#-overview)
- [Key Features](#-key-features)
- [Quick Start with Docker](#-quick-start-with-docker)
- [Quick Start from Source](#-quick-start-from-source)
- [Connecting with Existing Redis Clients](#-connecting-with-existing-redis-clients)
- [Supported Data Structures & Commands](#-supported-data-structures--commands)
- [PostgreSQL DB-Cache Proxy Mode](#-postgresql-db-cache-proxy-mode)
- [Performance & Benchmarks](#-performance--benchmarks)
- [Configuration Reference](#-configuration-reference)
- [Documentation & Community](#-documentation--community)
- [License](#-license)

---

## 🌟 Overview

**FyroDB** is a modern, ultra-high-performance in-memory database and caching layer built from the ground up in Rust. It serves as a seamless, drop-in replacement for Redis® (wire-compatible with both RESP2 and RESP3 protocols) while delivering higher concurrency, lower memory overhead, and multi-model capabilities.

Unlike traditional single-threaded architectures, FyroDB uses a **thread-per-core** model with sharded lock-free hash tables (`customhash`), optimistic zero-copy seqlock reads (`SmallStr`), and epoch-based memory reclamation (EBR).

In addition to standard Key-Value operations, FyroDB natively integrates:
- **Vector Similarity Search**: High-performance HNSW indexing with cosine, Euclidean, and inner-product distance metrics, scalar quantization (SQ8), and hybrid filtering.
- **Document Store**: Full JSON document querying and mutation (`JSON.*`).
- **PostgreSQL Acceleration Proxy**: An intelligent read-through/write-through caching layer that speaks the PostgreSQL wire protocol, serving cached queries at 5M+ ops/sec with automatic table-aware invalidation.
- **Formal Verification**: Critical concurrency invariants and memory boundaries are mathematically verified using Verus.

---

## ⚡ Key Features

- **🚀 Extreme Throughput & Low Latency**: 20M+ to 97M+ ops/sec in-memory, 200k+ ops/sec over standard loopback TCP sockets per core with p99 latency < 0.2 ms.
- **🔌 100% Redis® Wire-Compatible**: Speaks standard RESP2/RESP3. Zero code changes required for existing clients (`redis-cli`, `redis-py`, `ioredis`, `go-redis`, `redis-rs`), ORMs, or microservices.
- **🧠 Native Vector Search (HNSW)**: Approximate Nearest Neighbor (ANN) search with cosine, L2, and IP distance kernels, SIMD acceleration (AVX-512, AVX2, NEON), and scalar quantization.
- **📄 Native JSON Documents**: Full support for hierarchical JSON storage, path-based access, and atomic mutations.
- **🛡️ Formal Invariant Verification**: Memory safety invariants, non-tearing seqlock reads, CRC routing, and buffer framing are formally proved with **Verus**.
- **🐘 PostgreSQL Query Cache Proxy**: Transparently intercepts SQL queries in front of Postgres, returning sub-millisecond cache hits with automated table-aware invalidation on `INSERT`/`UPDATE`/`DELETE`.
- **🌐 Distributed Clustering**: 16,384 hash slots, Raft-based master-replica synchronization, gossip bus, and automated slot migration.
- **💾 Dual Durability**: Point-in-time snapshotting (RDB) and Append-Only File (AOF) persistence for crash-safe durability.

---

## 🐳 Quick Start with Docker

The fastest way to get started with FyroDB is using our official Docker image.

### Run with Docker

```bash
# Pull the latest image
docker pull rana718/fyrodb:latest

# Run FyroDB mapping Redis port 6379 to FyroDB's internal port 8000
docker run -d \
  --name fyrodb \
  -p 6379:8000 \
  -v fyrodb-data:/data \
  rana718/fyrodb:latest
```

### Verify with `redis-cli`

```bash
# Ping the server
redis-cli -p 6379 PING
# Output: PONG

# Set and Get a key
redis-cli -p 6379 SET mykey "Hello from FyroDB!"
# Output: OK

redis-cli -p 6379 GET mykey
# Output: "Hello from FyroDB!"
```

### Run with Docker Compose

Save the following as `docker-compose.yml`:

```yaml
version: "3.8"

services:
  fyrodb:
    image: rana718/fyrodb:latest
    container_name: fyrodb
    ports:
      - "6379:8000"
    environment:
      - FYRODB_WORKERS=4
      - FYRODB_RDB_PATH=/data/fyrodb.rdb
      - FYRODB_RDB_INTERVAL=300
    volumes:
      - fyrodb-data:/data
    restart: unless-stopped

volumes:
  fyrodb-data:
```

Launch with:

```bash
docker compose up -d
```

---

## 🛠️ Quick Start from Source

### Prerequisites

- **Rust**: Version 1.85+ (or `stable`).
- **Build Tools**: `clang`, `llvm`, `make`, `pkg-config`, and `openssl`.

### Build & Run

```bash
# 1. Clone repository
git clone https://github.com/Rana718/FyroDB.git
cd FyroDB

# 2. Build in release mode
cargo build --release

# 3. Start the server (binds to 0.0.0.0:6379)
./target/release/fyro_db --port 6379
```

---

## 💻 Connecting with Existing Redis Clients

Because FyroDB is 100% wire-compatible with Redis, you can use your existing client libraries:

### Python (`redis-py`)

```python
import redis

client = redis.Redis(host="localhost", port=6379, decode_responses=True)

client.set("framework", "FyroDB")
print(client.get("framework"))  # Output: FyroDB

# Atomic counters
client.incr("page_views")
print(client.get("page_views"))  # Output: 1
```

### Node.js / TypeScript (`ioredis`)

```typescript
import Redis from "ioredis";

const redis = new Redis({ host: "localhost", port: 6379 });

async function main() {
  await redis.set("user:1000", JSON.stringify({ name: "Alice", role: "admin" }));
  const data = await redis.get("user:1000");
  console.log("User:", JSON.parse(data!));
}
main();
```

### Go (`go-redis`)

```go
package main

import (
    "context"
    "fmt"
    "github.com/redis/go-redis/v9"
)

func main() {
    ctx := context.Background()
    rdb := redis.NewClient(&redis.Options{
        Addr: "localhost:6379",
    })

    err := rdb.Set(ctx, "session:token", "xyz987", 0).Err()
    if err != nil {
        panic(err)
    }

    val, _ := rdb.Get(ctx, "session:token").Result()
    fmt.Println("session:token =", val)
}
```

### Rust (`redis-rs`)

```rust
use redis::Commands;

fn main() -> redis::RedisResult<()> {
    let client = redis::Client::open("redis://127.0.0.1:6379/")?;
    let mut con = client.get_connection()?;

    con.set("cluster_state", "healthy")?;
    let state: String = con.get("cluster_state")?;
    println!("State: {}", state);

    Ok(())
}
```

---

## 🧩 Supported Data Structures & Commands

FyroDB implements all primary Redis data structures and popular extensions:

| Category | Commands | Description |
| :--- | :--- | :--- |
| **Strings & Ints** | `GET`, `SET`, `SETNX`, `MGET`, `MSET`, `INCR`, `DECR`, `APPEND`, `STRLEN` | Optimistic seqlock reads with stack-copy anti-tearing |
| **Lists** | `LPUSH`, `RPUSH`, `LPOP`, `RPOP`, `LRANGE`, `LINDEX`, `LLEN`, `BLPOP` | Memory-efficient hybrid chunked `QuickList` |
| **Hashes** | `HSET`, `HGET`, `HMGET`, `HDEL`, `HINCRBY`, `HGETALL`, `HKEYS`, `HVALS` | Compact arrays for small hashes, fast hash maps for large |
| **Sets** | `SADD`, `SREM`, `SMEMBERS`, `SISMEMBER`, `SCARD`, `SINTER`, `SUNION` | Int-set specialization + compact vector representations |
| **Sorted Sets** | `ZADD`, `ZREM`, `ZRANK`, `ZREVRANK`, `ZRANGE`, `ZSCORE`, `ZINCRBY` | High-concurrency skip lists with score/lex indexing |
| **Bitmaps** | `SETBIT`, `GETBIT`, `BITCOUNT`, `BITOP`, `BITPOS` | SIMD-unrolled popcount with amortized capacity headroom |
| **Streams** | `XADD`, `XRANGE`, `XREVRANGE`, `XLEN`, `XREAD`, `XACK` | Append-only event streams with monotonic ID generation |
| **HyperLogLog** | `PFADD`, `PFCOUNT`, `PFMERGE` | Cardinality estimation with 12k register hyperloglogs |
| **Geospatial** | `GEOADD`, `GEODIST`, `GEOPOS`, `GEORADIUS`, `GEOSEARCH` | 52-bit Geohash encoding mapped onto Sorted Sets |
| **Pub / Sub** | `PUBLISH`, `SUBSCRIBE`, `PSUBSCRIBE`, `UNSUBSCRIBE` | Lockless grouped worker notification rings |
| **Transactions** | `MULTI`, `EXEC`, `DISCARD`, `WATCH`, `UNWATCH` | Optimistic serializable multi-key transaction blocks |
| **JSON** | `JSON.SET`, `JSON.GET`, `JSON.DEL`, `JSON.TYPE`, `JSON.ARRAPPEND` | Native RFC-8259 JSON document manipulation |
| **Vector Search** | `FT.CREATE`, `FT.SEARCH`, `FT.INFO`, `FT.DROPINDEX` | HNSW ANN search with Cosine, L2, IP metrics & SQ8 |

---

## 🐘 PostgreSQL DB-Cache Proxy Mode

FyroDB can run as an intelligent caching proxy in front of PostgreSQL:

```
[ App / Backend ] ---> [ PostgreSQL Wire Protocol ] ---> [ FyroDB Proxy ] ---> [ PostgreSQL Database ]
                                                                |
                                                      Sub-millisecond Hits
                                                       (5M+ ops/sec memory)
```

1. **Zero Code Changes**: Connect with any standard PostgreSQL driver (`psycopg2`, `node-postgres`, `asyncpg`, GORM, Diesel).
2. **Table-Aware Invalidation**: Whenever a transaction commits writes (`INSERT`, `UPDATE`, `DELETE`, `DDL`) to a table (e.g., `orders`), all cached queries that touch `orders` are automatically invalidated in real-time.
3. **Prepared Statements**: Parameterized queries and statement fingerprints are fully cached across connections.

To enable the proxy, start FyroDB with upstream PostgreSQL credentials:

```bash
fyro_db \
  --dbcache-upstream-host 127.0.0.1 \
  --dbcache-upstream-port 5432 \
  --dbcache-listen-port 6432
```

---

## 📊 Performance & Benchmarks

Measured on a standard modern x86-64 machine (6-core Intel i5-11400H / 12 workers) using in-memory engine benchmarks and real-time loopback TCP socket benchmarks:

### In-Memory Engine Throughput

| Workload | Throughput | Median Latency |
| :--- | :--- | :--- |
| **Pipelined GET** | **97.69M ops/sec** | 10 ns |
| **Pipelined SET** | **77.57M ops/sec** | 13 ns |
| **Mixed SET / GET (50/50)** | **38.41M ops/sec** | 26 ns |
| **Pub/Sub Delivery** | **5.69B ops/sec** | 0.2 ns |
| **Atomic Counters (`INCR`)** | **3.11B ops/sec** | 0.3 ns |
| **Hot Key Contention (1 Key)** | **122.09M ops/sec** | 8 ns |
| **JSON.GET Documents** | **65.13M ops/sec** | 15 ns |
| **HyperLogLog (`PFADD/PFCOUNT`)**| **307.17M ops/sec** | 3 ns |

### Real TCP redis-benchmark (16 concurrent clients over loopback)

| Command | Requests / sec | p50 Latency | p99 Latency |
| :--- | :--- | :--- | :--- |
| **SET** | **204,081 req/s** | 0.127 ms | 0.311 ms |
| **GET** | **195,312 req/s** | 0.119 ms | 0.287 ms |
| **INCR** | **202,839 req/s** | 0.127 ms | 0.295 ms |
| **MSET (10 keys)** | **206,185 req/s** | 0.135 ms | 0.325 ms |

*Detailed benchmark methodologies, reproduction scripts, and comparisons are available on our [Documentation Benchmarks Page](https://fyrodb.vercel.app/docs/benchmarks).*

---

## ⚙️ Configuration Reference

All configuration options can be passed via command-line arguments or environment variables:

| Environment Variable | CLI Option | Default | Description |
| :--- | :--- | :--- | :--- |
| `FYRODB_PORT` | `-p`, `--port` | `8000` | Listening port for client connections |
| `FYRODB_BIND` | `-b`, `--bind` | `0.0.0.0` | IP address or interface to bind |
| `FYRODB_UNIX_SOCKET` | `--unix-socket` | *None* | Unix domain socket path for zero-overhead local IPC |
| `FYRODB_WORKERS` | `-w`, `--workers` | Auto (CPUs) | Number of worker event-loop threads |
| `FYRODB_AUTH` | `--auth` | *None* | Require authentication password (`AUTH <pwd>`) |
| `FYRODB_MAX_CLIENTS` | `--max-clients` | `10000` | Maximum simultaneous client connections |
| `FYRODB_RDB_PATH` | `--rdb-path` | `dump.rdb` | Target destination file for point-in-time snapshots |
| `FYRODB_RDB_INTERVAL` | `--rdb-interval` | `300` | Seconds between background RDB snapshots (0 = disabled) |
| `FYRODB_IO_URING` | *N/A* | `false` | Enable Linux `io_uring` batched socket sends |
| `FYRODB_CLUSTER_ENABLED`| *N/A* | `false` | Enable distributed Redis cluster mode |

---

## 📚 Documentation & Community

- **Official Website & Documentation**: [https://fyrodb.vercel.app](https://fyrodb.vercel.app)
- **Architecture Overview**: [ARCHITECTURE.md](ARCHITECTURE.md)
- **Release Changelog**: [CHANGELOG.md](CHANGELOG.md)
- **Bug Reports & Issues**: [GitHub Issues](https://github.com/Rana718/FyroDB/issues)

---

## 📄 License

FyroDB is open-sourced under the terms of both the **Apache License (Version 2.0)** and the **MIT License**. You may choose either license at your option. See [LICENSE](LICENSE) for details.
