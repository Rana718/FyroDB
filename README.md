# FyroDB

> **Previously known as FlashDB** — renamed to FyroDB.

## Changelog

**v0.2.0** — Redis Cluster compatibility, high-availability replication, automated failover, slot migration, and extreme concurrency optimizations. See the [v0.2.0 changelog](https://fyrodb.vercel.app/docs/changelog-0-2-0).

---

A Redis-compatible in-memory key-value store written in Rust. Speaks the RESP protocol so any Redis client works out of the box. Supports all major Redis data types and commands with a lock-free concurrent architecture.

Built on [`customhash`](https://www.ranadolui.me/blog/custom-concurrent-hashmap-rust) — a sharded, lock-free concurrent hash map with epoch-based reclamation and per-key seqlock for safe in-place mutation.

## Performance

6-core Intel i5-11400H (12 hardware threads), loopback TCP, 100 clients, 1M operations per benchmark.

Both sides get the **whole machine**: FyroDB runs one node with 12 workers, Redis runs 12 single-threaded masters pinned one per hardware thread. Redis is single-threaded per master, so master count is its core count — comparing against 6 masters would leave half the box idle on its side.

| Benchmark            | FyroDB (1 node, 12 workers) | Redis Cluster (12 masters) | Speedup |
| -------------------- | --------------------------- | -------------------------- | ------- |
| Pipeline-64 SET      | 9.00M ops/sec               | 5.26M ops/sec              | 1.7×    |
| Pipeline-100 SET     | 22.12M ops/sec              | 7.39M ops/sec              | 3.0×    |
| Pipeline-100 GET     | 26.61M ops/sec              | 8.26M ops/sec              | 3.2×    |
| Mixed SET/GET        | 19.23M ops/sec              | 5.34M ops/sec              | 3.6×    |
| INCR (counters)      | 29.81M ops/sec              | 6.59M ops/sec              | 4.5×    |
| HSET/HGET            | 21.72M ops/sec              | 5.98M ops/sec              | 3.6×    |
| LPUSH/RPOP           | 37.51M ops/sec              | 5.48M ops/sec              | 6.8×    |
| SADD                 | 21.28M ops/sec              | 5.52M ops/sec              | 3.9×    |
| ZADD                 | 12.77M ops/sec              | 3.68M ops/sec              | 3.5×    |
| JSON.SET/GET         | 14.67M ops/sec              | — (module required)        | —       |
| SET+EXPIRE           | 9.32M ops/sec               | 2.25M ops/sec              | 4.1×    |
| Hot Key (contention) | 13.32M ops/sec              | 1.57M ops/sec              | 8.5×    |
| Pub/Sub publish      | 523.0K ops/sec              | 49.7K ops/sec              | 10.5×   |
| Pub/Sub delivery     | 26.14M msg/sec              | 2.49M msg/sec              | 10.5×   |
| Producer/Consumer    | 6.13M ops/sec               | 824.0K ops/sec             | 7.4×    |

How to read some of these:

- **Pipeline-64 SET** includes hash table growth from empty to 1M keys. On a pre-warmed server it reaches ~17M ops/sec. See [Production Tips](https://fyrodb.vercel.app/docs/production-tips).
- **SET+EXPIRE** counts one op per `SET`+`EXPIRE` *pair*, so 9.32M ops/sec is ~18.6M commands/sec.
- **Pub/Sub publish** fans one publish out to 50 subscribers; the delivery row is the same work counted per subscriber.
- **Hot Key** and **Producer/Consumer** both hammer a single key. Producer/Consumer is 100% writes, and a single key serializes on one entry lock — the contention-free ceiling for single-key writes on this machine measures ~9.5M ops/sec, so 6.13M is ~65% of the theoretical best rather than a soft number.

### Resource Usage

Measured over the same full suite, RSS summed across all processes.

|                     | FyroDB (1 node) | Redis Cluster (12 masters) |
| ------------------- | --------------- | -------------------------- |
| Idle RSS            | 5 MB            | ~120 MB (total)            |
| Loaded RSS          | 57 MB           | 120 MB (total)             |
| After `FLUSHALL`    | 16 MB           | 120 MB (total)             |

A single FyroDB node beats a 12-master Redis Cluster on every workload while holding roughly half the memory. Neither returns everything to the OS after a flush — see [known limits](https://fyrodb.vercel.app/docs/production-tips).

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
- **List** — LPUSH, RPUSH, LPOP, RPOP, LRANGE, LMOVE, BLPOP, BRPOP
- **Set** — SADD, SREM, SMEMBERS, SINTER, SUNION, SDIFF, SSCAN
- **Sorted Set** — ZADD, ZRANGE, ZRANGEBYSCORE, ZPOPMIN, ZUNIONSTORE, ZSCAN
- **JSON** — JSON.SET, JSON.GET, JSON.DEL, JSON.ARRAPPEND, JSON.OBJKEYS
- **Stream** — XADD, XREAD, XRANGE, XGROUP, XACK, XTRIM
- **Bitmap** — SETBIT, GETBIT, BITCOUNT, BITOP, BITFIELD
- **HyperLogLog** — PFADD, PFCOUNT, PFMERGE
- **Geospatial** — GEOADD, GEODIST, GEOSEARCH, GEOSEARCHSTORE

## Supported Commands

Full Redis command compatibility including:

**Keys:** DEL, UNLINK, EXISTS, TTL, PTTL, EXPIRE, PEXPIRE, EXPIREAT, EXPIRETIME, PEXPIRETIME, PERSIST, RENAME, RENAMENX, COPY, RANDOMKEY, KEYS, SCAN, TOUCH, OBJECT, SORT, TYPE

**Server:** PING, ECHO, INFO, DBSIZE, BGSAVE, SAVE, LASTSAVE, TIME, COMMAND, HELLO, SELECT, AUTH, QUIT, RESET, CLIENT, CONFIG, FLUSHALL, FLUSHDB, SLOWLOG, ACL

**Cluster:** CLUSTER INFO, MYID, SLOTS, SHARDS, NODES, KEYSLOT, COUNTKEYSINSLOT, GETKEYSINSLOT, MEET, FORGET, ADDSLOTS, DELSLOTS, SETSLOT, REPLICATE, RESET, SAVECONFIG, ASKING

**Pub/Sub:** SUBSCRIBE, UNSUBSCRIBE, PSUBSCRIBE, PUNSUBSCRIBE, PUBLISH, PUBSUB

**Transactions:** MULTI, EXEC, DISCARD, WATCH, UNWATCH

## Design

- **Lock-free reads** — epoch-based reclamation with seqlock validation for iteration safety
- **Zero-clone writes** — per-key spinlock with in-place mutation, no CAS retry loops
- **Tagged-pointer probing** — a hash tag rides in each slot's unused high bits, so a probe step rejects a non-match without touching the entry's cache line
- **Backoff under contention** — waiters back off exponentially then yield, so a hot key no longer scales negatively with worker count
- **Thread-per-core** — one epoll loop per CPU core, SO_REUSEPORT for kernel-level connection distribution
- **Zero-copy GET** — writes directly from stored value to TCP buffer
- **Allocation-free pops** — `LPOP`/`RPOP` write the reply straight into the output buffer
- **Inline fast path** — SET, GET, INCR, LPUSH, LPOP, RPOP, SADD, DEL dispatched from raw RESP bytes
- **Compact storage** — short keys and values stay inline; small hashes, lists, and sets avoid full hash-table overhead
- **Adaptive memory reclaim** — lazy shard growth, EBR collection, allocator purging, shard compaction, cursor-based value defragmentation
- **Lazy connection buffers** — an accepted-but-idle connection commits no read or write buffer
- **Batched I/O** — all epoll events processed before flushing, reducing syscall count
- **Arc-snapshot Pub/Sub** — publish path reads with zero locks, per-subscriber lock-free queues
- **Flat cluster routing** — slot ownership resolves through a shared 16384-entry table, revalidated per connection with one atomic load

## Persistence

RDB snapshots, same model as Redis:

- Loads `fyrodb.rdb` on startup
- Auto-saves every 5 minutes (configurable)
- Saves on SIGTERM/Ctrl+C
- `BGSAVE` for manual trigger
- Atomic write (temp file → fsync → rename)

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

| Flag        | Default | Description                       |
| ----------- | ------- | --------------------------------- |
| `-p`        | `8000`  | Server port                       |
| `-m`        | `all`   | Mode: `all`, `key`, `pub`, `mix`  |
| `--cluster` |         | Comma-separated cluster addresses |

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

Cluster variables are documented in the [Cluster docs](https://fyrodb.vercel.app/docs/cluster).

## Architecture

See [ARCHITECTURE.md](ARCHITECTURE.md) for the current memory layout, concurrency model, maintenance threads, and complexity reference.

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines on development, benchmarks, zero-regression policy, and submitting pull requests.

## License

Apache 2.0
