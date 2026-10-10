# FyroDB

> **Previously known as FlashDB** — renamed to FyroDB.

## Changelog

**v0.2.2** — Vector search (HNSW + FLAT + hybrid), BM25 full-text, arena milestone-shrink, lock-free search pool, two-pass FLAT scan, O(bytes) bitmap ops, zero clippy warnings. See the [v0.2.2 changelog](https://fyrodb.vercel.app/docs/changelog-0-2-2).

---

A Redis-compatible in-memory key-value store written in Rust. Speaks the RESP protocol so any Redis client works out of the box. Supports all major Redis data types and commands with a lock-free concurrent architecture.

Built on [`customhash`](https://www.ranadolui.me/blog/custom-concurrent-hashmap-rust) — a sharded, lock-free concurrent hash map with epoch-based reclamation and per-key seqlock for safe in-place mutation.

## Performance

6-core Intel i5-11400H (12 hardware threads), loopback TCP, 100 clients, 1M operations per benchmark.

Comparing a single FyroDB node against a 12-node Redis Cluster (13 containers) and a single multi-threaded DragonflyDB node.

| Benchmark | FyroDB (1 node) | Redis Cluster (12 nodes) | DragonflyDB (1 node) | Speedup vs Redis |
| ------------------------- | ------------------- | ------------------------ | -------------------- | ---------------- |
| Pipeline-64 SET           | 9.14M ops/sec       | 5.00M ops/sec            | 4.09M ops/sec        | 1.83×            |
| Pipelined SET             | 20.60M ops/sec      | 6.53M ops/sec            | 4.16M ops/sec        | 3.15×            |
| Pipelined GET             | 29.52M ops/sec      | 11.62M ops/sec           | 4.09M ops/sec        | 2.54×            |
| Pub/Sub publish           | 1.41M ops/sec       | 161.9K ops/sec           | 315.5K ops/sec       | 8.7×             |
| Pub/Sub delivery          | 70.44M msg/sec      | 8.09M msg/sec            | 15.43M msg/sec       | 8.7×             |
| Mixed SET/GET (50/50)     | 16.25M ops/sec      | 5.82M ops/sec            | 5.15M ops/sec        | 2.79×            |
| INCR (counters)           | 47.12M ops/sec      | 6.54M ops/sec            | 6.84M ops/sec        | 7.2×             |
| HSET/HGET (sessions)      | 33.03M ops/sec      | 6.63M ops/sec            | 5.81M ops/sec        | 4.98×            |
| LPUSH/RPOP (queue)        | 42.86M ops/sec      | 7.17M ops/sec            | 6.23M ops/sec        | 5.98×            |
| SADD (sets)               | 30.43M ops/sec      | 6.27M ops/sec            | 5.00M ops/sec        | 4.85×            |
| ZADD (zsets)              | 19.09M ops/sec      | 4.78M ops/sec            | 4.36M ops/sec        | 3.99×            |
| JSON.SET/GET (docs)       | 13.22M ops/sec      | 3.23M ops/sec            | 151.5K ops/sec       | 4.09×            |
| SET+EXPIRE (cache TTL)    | 8.62M ops/sec       | 2.41M ops/sec            | 2.41M ops/sec        | 3.58×            |
| Hot Key (contention)      | 46.69M ops/sec      | 1.61M ops/sec            | 2.15M ops/sec        | 29.0×            |
| Producer/Consumer         | 28.64M ops/sec      | 1.70M ops/sec            | 1.75M ops/sec        | 16.8×            |
| XADD (streams)            | 9.88M ops/sec       | 4.27M ops/sec            | —                    | 2.31×            |
| SETBIT/BITCOUNT (bitmap)  | 20.92M ops/sec      | 5.94M ops/sec            | —                    | 3.52×            |
| PFADD/PFCOUNT (HLL)       | 36.29M ops/sec      | 6.03M ops/sec            | —                    | 6.02×            |
| GEOADD/GEODIST (geo)      | 6.30M ops/sec       | 3.07M ops/sec            | —                    | 2.05×            |
| Vector HSET load          | 160.7K ops/sec      | 66.4K ops/sec †          | —                    | 2.42×            |
| Vector KNN-10 (FLAT)      | 1.0K q/s            | 104 q/s †                | —                    | 9.6×             |
| Vector KNN-10 +filter     | 1.7K q/s            | 162 q/s †                | —                    | 10.5×            |
| Filter Tag+Num            | 5.0K q/s            | 313 q/s †                | —                    | 16.0×            |
| Text term                 | 42.6K q/s           | 1.6K q/s †               | —                    | 26.6×            |
| Text + Tag                | 24.7K q/s           | 1.8K q/s †               | —                    | 13.7×            |
| Text negation             | 21.7K q/s           | 361 q/s †                | —                    | 60.1×            |
| Vector KNN-10 (8 clients) | 3.0K q/s            | 97 q/s †                 | —                    | 30.9×            |
| Vector HNSW KNN-10        | 5.0K q/s            | 1.1K q/s †               | —                    | 4.55×            |
| Vector HNSW M=32 KNN-10   | 2.9K q/s            | 1.1K q/s †               | —                    | 2.64×            |

DragonflyDB is a single multi-threaded process, so it is the closest single-node comparison to a single FyroDB node — it uses every core at once, yet FyroDB is faster on every workload. DragonflyDB numbers are from an earlier benchmark run.

† Redis Cluster does not distribute vector or graph operations across nodes. These benchmarks ran against a single Redis node; the other 11 cluster nodes contributed no compute for these workloads.

How to read some of these:

- **Pipeline-64 SET** includes hash table growth from empty to 1M keys. On a pre-warmed server it reaches ~17M ops/sec. See [Production Tips](https://fyrodb.vercel.app/docs/production-tips).
- **SET+EXPIRE** fuses pipelined `SET` and `EXPIRE` pairs into a single atomic write-with-TTL operation (8.62M pairs/sec is ~17M commands/sec).
- **Pub/Sub publish** fans one publish out to 50 subscribers; the delivery row is the same work counted per subscriber (70.44M msg/sec).
- **Hot Key** and **Producer/Consumer** hammer a single key. With pipelined batch coalescing and backoff spinlocks, FyroDB achieves 46.69M ops/sec under contention and 28.64M ops/sec on producer/consumer queues.

### Resource Usage

Measured over the full benchmark suite (including 50k×768d HNSW vector workloads) across all processes:

| Metric      | FyroDB (1 node) | Redis Cluster (12 nodes, 13 containers) |
| ----------- | --------------- | --------------------------------------- |
| Peak RSS    | 1005.84 MB      | 756.74 MB                               |
| Avg RSS     | 784.87 MB       | 583.18 MB                               |
| Peak CPU    | 1219.9%         | 761.0%                                  |
| Avg CPU     | 1051.3%         | 117.9%                                  |

FyroDB's higher peak RSS reflects the 50k×768d HNSW graph loaded during the vector benchmark suite — each query uses all 12 workers simultaneously. Redis Cluster's lower memory is because vector operations cannot be distributed across cluster nodes; all vector load landed on a single Redis node while the other 11 sat idle.

## Quick Start

```bash
cargo build --release
./target/release/fyro_db

redis-cli -p 8000
127.0.0.1:8000> SET name rana
OK
127.0.0.1:8000> GET name
"rana"
```

## Docker

```bash
docker run -p 8000:8000 rana718/fyrodb:latest
```

## Cluster

FyroDB implements Redis Cluster: 16384 hash slots, `MOVED`/`ASK` redirects, `CROSSSLOT` detection, hash tags, and a gossip bus for heartbeats and failure detection. Any cluster-aware Redis client works.

```bash
task fyro-up                            # 3 nodes on :8000, :8001, :8002
redis-cli -c -p 8000 set user:1 alice   # -c follows redirects
redis-cli -p 8000 cluster info
task fyro-down
```

Full setup, `nodes.conf` format, slot migration, and client examples: [Cluster docs](https://fyrodb.vercel.app/docs/cluster).

## Supported Data Types

- **String** — GET, SET, INCR, APPEND, GETRANGE, MSET, MGET, LCS, and more
- **Hash** — HSET, HGET, HGETALL, HINCRBY, HRANDFIELD, HSCAN
- **List** — LPUSH, RPUSH, LPOP, RPOP, LRANGE, LMOVE, BLPOP, BRPOP, BLMOVE, BRPOPLPUSH
- **Set** — SADD, SREM, SMEMBERS, SINTER, SUNION, SDIFF, SSCAN
- **Sorted Set** — ZADD, ZRANGE, ZRANGEBYSCORE, ZPOPMIN, BZPOPMIN, BZPOPMAX, ZUNIONSTORE, ZSCAN
- **JSON** — JSON.SET, JSON.GET, JSON.DEL, JSON.ARRAPPEND, JSON.OBJKEYS
- **Stream** — XADD, XREAD (BLOCK), XRANGE, XREVRANGE, XTRIM (MAXLEN/MINID, `~`), XDEL, consumer groups: XGROUP, XREADGROUP (BLOCK, NOACK), XACK, XPENDING, XCLAIM, XAUTOCLAIM, XINFO
- **Bitmap** — SETBIT, GETBIT, BITCOUNT, BITOP, BITFIELD
- **HyperLogLog** — PFADD, PFCOUNT, PFMERGE
- **Geospatial** — GEOADD, GEODIST, GEOSEARCH, GEOSEARCHSTORE
- **Search** — FT.CREATE, FT.SEARCH (KNN, VECTOR_RANGE, filters, BM25 text), FT.AGGREGATE (GROUPBY/REDUCE, cursors), FT.HYBRID (RRF / LINEAR fusion), FT.INFO, FT.ALTER, FT.DROPINDEX, aliases

## Vector and Hybrid Search

FT.* commands index plain hashes. HNSW and FLAT vector fields support FLOAT32, FLOAT16, BFLOAT16, INT8 and UINT8 elements, with COSINE, L2 and IP metrics.

- **Off-worker execution** — queries run on a persistent, optionally pinned thread pool while the network workers keep serving
- **Quantization** — `QUANTIZATION INT8` (default for HNSW fields of 256 dims or more) or `RABITQ`, with exact rerank from the stored vector
- **Filtered KNN** — bitset allow-lists; the strategy (brute force, ACORN-1, in-traversal) follows filter selectivity
- **Incremental indexes** — a write hook marks dirty keys, and queries apply only those keys
- **Persistence** — graphs and index settings persist in the RDB, so restarts skip the rebuild
- **Hybrid** — BM25 text and KNN legs run in parallel and fuse with RRF or a linear blend

```bash
redis-cli -p 8000 FT.CREATE idx ON HASH PREFIX 1 doc: SCHEMA title TEXT tag TAG \
  vec VECTOR HNSW 6 TYPE FLOAT32 DIM 768 DISTANCE_METRIC COSINE
redis-cli -p 8000 FT.SEARCH idx "(@tag:{news})=>[KNN 10 @vec $v AS score]" PARAMS 2 v "<blob>" DIALECT 2
```

## Supported Commands

Full Redis command compatibility including:

**Keys:** DEL, UNLINK, EXISTS, TTL, PTTL, EXPIRE, PEXPIRE, EXPIREAT, EXPIRETIME, PEXPIRETIME, PERSIST, RENAME, RENAMENX, COPY, RANDOMKEY, KEYS, SCAN, TOUCH, OBJECT, SORT, TYPE

**Server:** PING, ECHO, INFO, DBSIZE, BGSAVE, SAVE, LASTSAVE, TIME, COMMAND, HELLO, SELECT, AUTH, QUIT, RESET, CLIENT, CONFIG, FLUSHALL, FLUSHDB, SLOWLOG, ACL

**Cluster:** CLUSTER INFO, MYID, SLOTS, SHARDS, NODES, KEYSLOT, COUNTKEYSINSLOT, GETKEYSINSLOT, MEET, FORGET, ADDSLOTS, DELSLOTS, SETSLOT, REPLICATE, RESET, SAVECONFIG, ASKING

**Pub/Sub:** SUBSCRIBE, UNSUBSCRIBE, PSUBSCRIBE, PUNSUBSCRIBE, PUBLISH, PUBSUB

**Transactions:** MULTI, EXEC, DISCARD, WATCH, UNWATCH

**Search:** FT.CREATE, FT.SEARCH, FT.AGGREGATE, FT.CURSOR, FT.HYBRID, FT.INFO, FT.ALTER, FT.DROPINDEX, FT._LIST, FT.ALIASADD, FT.ALIASUPDATE, FT.ALIASDEL, FT.ALIASLIST, FT.CONFIG

## Design

- **Lock-free reads** — epoch-based reclamation with seqlock validation for iteration safety
- **Zero-clone writes** — per-key spinlock with in-place mutation, no CAS retry loops
- **Tagged-pointer probing** — a hash tag rides in each slot's unused high bits, so a probe step rejects a non-match without touching the entry's cache line
- **Backoff under contention** — waiters back off exponentially then yield, so a hot key no longer scales negatively with worker count
- **Thread-per-core** — one epoll loop per CPU core, SO_REUSEPORT for kernel-level connection distribution
- **Zero-copy GET** — writes directly from stored value to TCP buffer
- **Allocation-free pops** — `LPOP`/`RPOP` write the reply straight into the output buffer
- **Inline fast path** — SET, GET, INCR, LPUSH, LPOP, RPOP, SADD, DEL dispatched from raw RESP bytes
- **Compact storage** — keys and values up to 23 bytes stay inline in a 64-byte entry; lists are packed quicklist chunks, sorted sets are chunked with a member index, streams are varint-packed nodes
- **Pipeline prefetch** — queued commands' slots and entries are prefetched in two stages so their cache misses overlap
- **Parked connections** — blocking commands and heavy searches park the connection instead of stalling the worker; waiters wake only on writes to their keys
- **Adaptive expiry** — a 10 Hz budgeted expire cycle that keeps going while expired keys are dense
- **Adaptive memory reclaim** — lazy shard growth, EBR collection, allocator purging, shard compaction, cursor-based value defragmentation
- **Lazy connection buffers** — an accepted-but-idle connection commits no read or write buffer
- **Batched I/O** — all epoll events processed before flushing, reducing syscall count
- **Arc-snapshot Pub/Sub** — publish path reads with zero locks, per-subscriber lock-free queues
- **Flat cluster routing** — slot ownership resolves through a shared 16384-entry table, revalidated per connection with one atomic load
- **Lock-free search pool** — vector/search job dispatch via `SegQueue`, no lock on submission
- **Two-pass FLAT scan** — Int8 pre-filter narrows candidates before exact f32 rerank, codes held in thread-local scratch
- **HNSW arena milestone-shrink** — arena buffers shrink at each power-of-two node count, improving cache density for streaming inserts

## Persistence

RDB snapshots, same trigger model as Redis:

- Loads `fyrodb.rdb` on startup
- Auto-saves every 5 minutes (configurable)
- Saves on SIGTERM/Ctrl+C
- `SAVE` / `BGSAVE` for manual trigger
- Atomic write (temp file → fsync → rename)
- **Forkless and point-in-time** — no `fork()`, no copy-on-write memory spike. A per-entry snapshot bit lets writers hand over pre-images, so the file reflects the moment the save started
- Streams keep their consumer groups and pending lists; search indexes keep their HNSW graphs

Optional append-only log for crash durability between snapshots:

```bash
FYRODB_AOF=1 FYRODB_AOF_FSYNC=everysec ./target/release/fyro_db
```

Each worker appends to its own file (no shared lock). Records are sequence-numbered state updates, merged on startup after the RDB loads, and rewritten after every snapshot.

## Benchmarking

```bash
cd bench && go run .                 # Full suite
cd bench && go run . -m key          # KV only
cd bench && go run . -m pub          # Pub/Sub only
cd bench && go run . -m mix          # Mixed workload only
cd bench && go run . -p 6379         # Against Redis
```

Cluster comparisons, paired so both sides get the same core count:

```bash
# Whole machine: 12 Redis masters vs 12 FyroDB workers
task redis-up-12 && task bench-redis-cluster-12 && task redis-down-12
FYRODB_CLUSTER_WORKERS=4 task fyro-up && task bench-fyro-cluster-12

# Equal cores: 6 Redis masters vs 6 FyroDB workers
task redis-up && task bench-redis-cluster && task redis-down
task fyro-up && task bench-fyro-cluster
```

| Flag        | Default | Description                                                        |
| ----------- | ------- | ------------------------------------------------------------------ |
| `-p`        | `8000`  | Server port                                                        |
| `-m`        | `all`   | Mode: `all`, `key`, `pub`, `mix`, `lat`, `mem`, `ann`, `vector`    |
| `-json`     |         | Write results as JSON (for `scripts/bench_compare.py`)             |
| `--cluster` |         | Comma-separated cluster addresses                                  |

Latency, memory and ANN recall runs against a throwaway server:

```bash
task bench-lat          # closed-loop latency percentiles
task bench-lat-open     # open-loop (fixed arrival rate), no coordinated omission
task bench-mem          # bytes per key for each data type
task bench-ann          # recall@k / QPS on a synthetic clustered corpus
task bench-ann-sift     # same on SIFT1M (task sift-download first)
task bench-rust         # criterion micro-benchmarks
```

Build variants: `task build-avx` (x86-64-v3), `task pgo` (profile-guided, needs `cargo-pgo`), `task build-uring` (experimental io_uring sends). The Docker image targets x86-64-v3 on amd64; `task docker-build-portable` builds a baseline image.

## Configuration

| Variable              | Default        | Description                                       |
| --------------------- | -------------- | ------------------------------------------------- |
| `FYRODB_PORT`         | `8000`         | TCP listening port                                |
| `FYRODB_BIND`         | `0.0.0.0`      | Bind address                                      |
| `FYRODB_WORKERS`      | `0` (auto)     | Worker threads (0 = CPU cores)                    |
| `FYRODB_SHARDS`       | `0` (auto)     | Hash map shards (0 = workers × 4)                 |
| `FYRODB_MAX_KEYS`     | `0` (unlimited)| Key ceiling, matching Redis's `maxmemory 0`        |
| `FYRODB_MAX_CLIENTS`  | `10000`        | Max concurrent connections                        |
| `FYRODB_AUTH`         | (none)         | Password for AUTH (empty = no auth)               |
| `FYRODB_RDB_PATH`     | `fyrodb.rdb`   | Snapshot file path                                |
| `FYRODB_RDB_INTERVAL` | `300`          | Auto-save interval in seconds                     |
| `FYRODB_AOF`          | off            | `1` enables the per-thread append-only log        |
| `FYRODB_AOF_DIR`      | `<rdb dir>/aof`| AOF directory                                     |
| `FYRODB_AOF_FSYNC`    | `everysec`     | `always`, `everysec` or `no`                      |
| `FYRODB_SEARCH_THREADS` | CPU cores    | Search / vector pool size                         |
| `FYRODB_SEARCH_CORES` | (unpinned)     | CPU list for the search pool, e.g. `6-11`         |
| `FYRODB_WORKER_CORES` | (unpinned)     | CPU list for network workers, e.g. `0-5`          |
| `FYRODB_AFFINITY`     | (off)          | `auto` pins workers and the pool to disjoint cores |
| `FYRODB_BUSY_POLL_US` | `0`            | Busy-poll window after activity, in microseconds  |
| `FYRODB_SEARCH_INLINE`| `0`            | `1` runs FT.* on the network worker               |
| `FYRODB_DISABLE_AVX512` | `0`          | `1` forces the AVX2 kernels                       |
| `FYRODB_IO_URING`     | `0`            | `1` enables batched io_uring sends (feature build) |

Cluster variables are documented in the [Cluster docs](https://fyrodb.vercel.app/docs/cluster).

## Architecture

See [ARCHITECTURE.md](ARCHITECTURE.md) for the current memory layout, concurrency model, maintenance threads, and complexity reference.

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines on development, benchmarks, zero-regression policy, and submitting pull requests.

## License

Apache 2.0
