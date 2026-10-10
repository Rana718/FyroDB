# FyroDB

A Redis-compatible in-memory key-value store written in Rust. Speaks RESP2/RESP3, so any Redis client works out of the box. Single node, no external coordination needed.

**v0.2.2** — Vector search (HNSW + FLAT + BM25 hybrid), arena milestone-shrink, lock-free search pool, O(bytes) bitmap ops. → [Changelog](https://fyrodb.vercel.app/docs/changelog-0-2-2)

---

## Install

### From source

```bash
git clone https://github.com/Rana718/FyroDB.git
cd FyroDB
cargo build --release
./target/release/fyro_db
```

### Docker

```bash
# Pull and run
docker pull rana718/fyrodb:latest
docker run -p 8000:8000 rana718/fyrodb:latest

# With persistence
docker run -p 8000:8000 -v $(pwd)/data:/data \
  -e FYRODB_RDB_PATH=/data/fyrodb.rdb \
  rana718/fyrodb:latest

# With auth + custom port
docker run -p 6380:6380 \
  -e FYRODB_PORT=6380 \
  -e FYRODB_AUTH=secret \
  rana718/fyrodb:latest
```

### Docker Compose

```yaml
services:
  fyrodb:
    image: rana718/fyrodb:latest
    ports:
      - "8000:8000"
    volumes:
      - fyrodb-data:/data
    environment:
      FYRODB_RDB_PATH: /data/fyrodb.rdb
      FYRODB_WORKERS: "0"       # auto = CPU cores
volumes:
  fyrodb-data:
```

### Connect

```bash
redis-cli -p 8000
127.0.0.1:8000> SET name rana
OK
127.0.0.1:8000> GET name
"rana"
```

Any Redis client works: ioredis, redis-py, go-redis, redis-rs, Jedis. → [Client examples](https://fyrodb.vercel.app/docs/nodejs)

---

## Performance

6-core Intel i5-11400H (12 hardware threads), loopback TCP, 100 clients, 1M ops. Single FyroDB node vs 12-node Redis Cluster (13 containers) and single-node DragonflyDB.

| Benchmark | FyroDB (1 node) | Redis Cluster (12 nodes) | DragonflyDB (1 node) | Speedup vs Redis |
| ------------------------- | ------------------- | ------------------------ | -------------------- | ---------------- |
| Pipeline-64 SET           | 9.14M ops/sec       | 5.00M ops/sec            | 4.09M ops/sec        | 1.83×            |
| Pipelined SET             | 20.60M ops/sec      | 6.53M ops/sec            | 4.16M ops/sec        | 3.15×            |
| Pipelined GET             | 29.52M ops/sec      | 11.62M ops/sec           | 4.09M ops/sec        | 2.54×            |
| Mixed SET/GET (50/50)     | 16.25M ops/sec      | 5.82M ops/sec            | 5.15M ops/sec        | 2.79×            |
| INCR (counters)           | 47.12M ops/sec      | 6.54M ops/sec            | 6.84M ops/sec        | 7.2×             |
| HSET/HGET (sessions)      | 33.03M ops/sec      | 6.63M ops/sec            | 5.81M ops/sec        | 4.98×            |
| LPUSH/RPOP (queue)        | 42.86M ops/sec      | 7.17M ops/sec            | 6.23M ops/sec        | 5.98×            |
| SADD (sets)               | 30.43M ops/sec      | 6.27M ops/sec            | 5.00M ops/sec        | 4.85×            |
| ZADD (sorted sets)        | 19.09M ops/sec      | 4.78M ops/sec            | 4.36M ops/sec        | 3.99×            |
| JSON.SET/GET              | 13.22M ops/sec      | 3.23M ops/sec            | 151.5K ops/sec       | 4.09×            |
| SET+EXPIRE (cache TTL)    | 8.62M ops/sec       | 2.41M ops/sec            | 2.41M ops/sec        | 3.58×            |
| Hot Key (contention)      | 46.69M ops/sec      | 1.61M ops/sec            | 2.15M ops/sec        | 29.0×            |
| Producer/Consumer         | 28.64M ops/sec      | 1.70M ops/sec            | 1.75M ops/sec        | 16.8×            |
| Pub/Sub delivery          | 70.44M msg/sec      | 8.09M msg/sec            | 15.43M msg/sec       | 8.7×             |
| XADD (streams)            | 9.88M ops/sec       | 4.27M ops/sec            | —                    | 2.31×            |
| SETBIT/BITCOUNT (bitmap)  | 20.92M ops/sec      | 5.94M ops/sec            | —                    | 3.52×            |
| PFADD/PFCOUNT (HLL)       | 36.29M ops/sec      | 6.03M ops/sec            | —                    | 6.02×            |
| GEOADD/GEODIST            | 6.30M ops/sec       | 3.07M ops/sec            | —                    | 2.05×            |
| Vector HSET load          | 160.7K ops/sec      | 66.4K ops/sec †          | —                    | 2.42×            |
| Vector KNN-10 (FLAT)      | 1.0K q/s            | 104 q/s †                | —                    | 9.6×             |
| Vector KNN-10 +filter     | 1.7K q/s            | 162 q/s †                | —                    | 10.5×            |
| Text term search          | 42.6K q/s           | 1.6K q/s †               | —                    | 26.6×            |
| Text negation             | 21.7K q/s           | 361 q/s †                | —                    | 60.1×            |
| Vector HNSW KNN-10        | 5.0K q/s            | 1.1K q/s †               | —                    | 4.55×            |

† Redis Cluster does not distribute vector or graph operations across nodes — these benchmarks ran against a single Redis node.

### Resource Usage

Measured over the full suite including 50k×768d HNSW vector workloads:

| Metric   | FyroDB (1 node) | Redis Cluster (12 nodes) |
| -------- | --------------- | ------------------------ |
| Peak RSS | 1005.84 MB      | 756.74 MB                |
| Peak CPU | 1219.9%         | 761.0%                   |

FyroDB's higher RSS reflects the HNSW graph in memory. Redis's lower numbers are because vector load fell on one node; the other 11 were idle.

---

## Supported Data Types

String · Hash · List · Set · Sorted Set · JSON · Stream · Bitmap · HyperLogLog · Geospatial · **Vector Search** · **Full-Text Search**

Full command reference → [Commands docs](https://fyrodb.vercel.app/docs/commands-strings)

---

## Vector & Full-Text Search

```bash
# Create an index with a vector field
redis-cli -p 8000 FT.CREATE idx ON HASH PREFIX 1 doc: \
  SCHEMA title TEXT tag TAG vec VECTOR HNSW 6 TYPE FLOAT32 DIM 768 DISTANCE_METRIC COSINE

# KNN search with a tag pre-filter
redis-cli -p 8000 FT.SEARCH idx "(@tag:{news})=>[KNN 10 @vec $v AS score]" \
  PARAMS 2 v "<blob>" DIALECT 2

# BM25 full-text + vector hybrid
redis-cli -p 8000 FT.HYBRID idx QUERY "rust concurrency" VECTOR $v KNN 10 FUSION RRF
```

HNSW and FLAT indexes, `INT8`/`RABITQ` quantization, exact rerank, tag/numeric/text pre-filters, cursor-paged `FT.AGGREGATE`. → [Search docs](https://fyrodb.vercel.app/docs/architecture)

---

## Cluster

```bash
task fyro-up                            # 3 nodes on :8000 :8001 :8002
redis-cli -c -p 8000 SET user:1 alice   # -c follows MOVED redirects
redis-cli -p 8000 CLUSTER INFO
task fyro-down
```

16384 hash slots, `MOVED`/`ASK` redirects, `CROSSSLOT` detection, hash tags, gossip failure detection. Any cluster-aware Redis client works. → [Cluster docs](https://fyrodb.vercel.app/docs/cluster)

---

## Configuration

| Variable | Default | Description |
| -------- | ------- | ----------- |
| `FYRODB_PORT` | `8000` | TCP listening port |
| `FYRODB_WORKERS` | `0` (auto) | Worker threads (0 = CPU cores) |
| `FYRODB_MAX_KEYS` | `0` (unlimited) | Key ceiling |
| `FYRODB_AUTH` | (none) | Password for AUTH |
| `FYRODB_RDB_PATH` | `fyrodb.rdb` | Snapshot file |
| `FYRODB_RDB_INTERVAL` | `300` | Auto-save interval (seconds) |
| `FYRODB_AOF` | off | `1` enables the append-only log |
| `FYRODB_SEARCH_THREADS` | CPU cores | Vector/search pool size |
| `FYRODB_AFFINITY` | off | `auto` pins workers and pool to disjoint cores |

Full configuration reference → [Configuration docs](https://fyrodb.vercel.app/docs/configuration)

---

## Links

| | |
| --- | --- |
| Documentation | https://fyrodb.vercel.app/docs/introduction |
| Architecture | [ARCHITECTURE.md](ARCHITECTURE.md) |
| Changelog | [CHANGELOG.md](CHANGELOG.md) |
| Contributing | [CONTRIBUTING.md](CONTRIBUTING.md) |
| Docker Hub | https://hub.docker.com/r/rana718/fyrodb |

Built on [`customhash`](https://www.ranadolui.me/blog/custom-concurrent-hashmap-rust) — a sharded lock-free concurrent hash map with epoch-based reclamation and per-key seqlock.

## License

Apache 2.0
