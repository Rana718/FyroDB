# Changelog

All notable changes to FyroDB are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

---

## [0.3.0] - 2026-10-26

### Added
- **DB Cache Proxy: mio event loop** — replaced thread-per-connection with N mio worker threads, each owning an `SO_REUSEPORT` `TcpListener` and a private `mio::Poll`. Thousands of connections are multiplexed on a fixed thread count with no thread spawning per client. `FYRODB_PROXY_WORKERS` controls the thread count (default: `num_cpus / 2`).
- **`ProxyConn` state machine** — connections move through `Startup → Active → Dead` (or `Offloaded` for upstream/miss work). Cache-hit traffic is handled inline in `ActiveState` with no blocking pool involvement.
- **`BlockingPool`** — bounded OS thread pool for upstream connections and cache misses. Cache-hit traffic — the dominant steady-state path — never enters the pool.
- **`flush_out_batched`** — `handle_sync` accumulates response frames until `RELAY_FLUSH_BYTES` (32 KB) or the read buffer is drained, issuing one `write(2)` per pipeline batch instead of one per response. Extended-protocol throughput: **3.80M → 5.0M ops/sec** (+32%).
- **CustomHash `occupied` guard in `find()`** — closes a narrow race where a concurrent removal between a tag match and an epoch pin could expose an uninitialized entry to a reader.

### Optimized
- **`Acquire` load on entry state** — replaced `Relaxed` load + explicit `fence(Acquire)` with a single `Acquire` load on the entry state word in the fast string read path, providing the same happens-before guarantee with one fewer memory barrier.
- **CustomHash entry-line prefetch in probe loops** — `probe_occupied` and `probe_for_insert` prefetch the entry pointed to by each slot during the tag comparison of the next slot, overlapping L2 latency across the probe sequence.
- **`walk_shard` prefetch hint** — switched from `T2` to `T1` (shorter prefetch distance), improving L1 hit rate during expiry and defrag sweeps.
- **Zero-allocation KV hot paths** — eliminated heap allocations across `hash`, `get`, `setnx`, `srandmember`, `zrevrange`, `zadd`, `lindex`, canonical integer checks, member byte-length computation, and SmallStr RESP bulk header formatting. RESP replies now build directly into the output buffer on all these paths.
- **ZSet right-sizing** — `ZSetData` no longer over-allocates chunk capacity on init; idle shard tables are right-sized on compaction, reducing RSS under write-heavy workloads.
- **Cluster hot paths** (PR #23 by [@ananddub](https://github.com/ananddub)) — slot routing and topology resolution no longer allocate on the fast path; CRC16 single-key path drops a redundant branch.

### Fixed
- **Proxy wire cache invalidation** — deleting proxy cache keys through the KV port (`DEL db:*`, `FLUSHALL`) now correctly invalidates wire-format cache entries. Previously, the proxy's invalidation path was skipped for deletions originating from the KV engine, leaving stale encoded results in memory.
- **aarch64 CI** — resolved integer comparison failure in the RESP parser test suite on ARM64.
- **ZSet CI stability** — relaxed an overly tight timing threshold in the sorted-set concurrency test that intermittently failed under CI load.

---

## [0.2.4] - 2026-10-20

### Added
- **Pipelined Raw-Byte Cache Hit Path**:
  - `CompactKey::eq_bytes` performs byte-level key comparisons directly against prefetched `Entry` pointers in `dispatch_get_run`, bypassing UTF-8 string validation (`std::str::from_utf8`) on cache hits.
  - Zero-copy RESP bulk response generation directly in connection write buffers pushes **Pipelined GET** to an all-time record **53.38M ops/sec** (187ms for 10M keys).
- **Atomic In-Place SET Mutation Path**:
  - `SmallStr::overwrite_bytes` mutates string values directly in-place under per-key spinlock without memory reallocation, pushing **Pipelined SET** to **39.62M ops/sec** (252ms for 10M keys).
- **Redundant Prefetch Bypass for Pipeline Runs**:
  - `prefetch_ahead` early filter detects pipelined GET (`*2\r\n$3\r\nGET`) and SET (`*3\r\n$3\r\nSET`) runs, yielding directly to specialized run engines and eliminating 32 duplicate hash calculations and slot probes per pipeline batch.
  - Slashes **Mixed SET/GET (50/50)** elapsed time to **44ms** (**45.87M ops/sec**).
- **Parser Word-Level Branch Reduction**:
  - Unaligned 64-bit word reads (`u64::from_ne_bytes`) for `$3\r\nGET\r` and `$3\r\nSET\r` wire sequences in `try_fast_get_at`, `try_fast_get_key_at`, `try_fast_set_at`, and `try_fast_set_key_val_at` reduce branch count across all hot pipeline paths.
- **Stream Varint Coalescing (`WriteOp::Xadd`)**:
  - Integrated `WriteOp::Xadd` and `Store::xadd_batch` varint streaming, dropping **XADD streams** elapsed to **84ms** (**23.76M ops/sec**).
- **Fast Morton Interleaving (`spread26`) for Geospatial**:
  - Replaced 52-step division loop in `src/storage/geo.rs` with 5-instruction Morton bit-interleaving, reducing `GEOADD/GEODIST` elapsed time under **199ms**.
- **Sorted Set Direct RESP Serialization**:
  - Direct buffer writing in `zadd_added_flags_into` eliminates intermediate `Vec<bool>` allocations; right-append optimization in `ZSetData::place` with `CHUNK_MAX = 64` drops `ZADD` elapsed to **112ms** (**17.83M ops/sec**).
- **Fast-Path Document Parsing**:
  - Dedicated fast-path parser `try_fast_json_set_key_val_at` and `dispatch_json_set_run` drops `JSON.SET` elapsed to **123ms** (**16.28M ops/sec**) and `JSON.GET` to **76ms** (**26.21M ops/sec**).

### Optimized
- **Hot Key Contention Latency Cut by 2.4×**:
  - Single-key contention latency dropped from 647ms to **265ms** (**37.68M ops/sec** across 100 concurrent clients on a single contended key).
- **Pipeline Batch Expansion**:
  - Expanded pipeline prefetch batches to `BATCH = 256` in `fast_path.rs`, allowing 100-key bursts in `-m key` and 250-key bursts in `-m mix` to execute in a single prefetch and write pass without chunk splitting.
- **Strict Memory Safety**:
  - Maintained 24-byte `SmallStr` footprint and 64-byte `Entry` size in `mimalloc`, with peak RSS at **876.28 MB** (strictly below 900MB limit).

## [0.2.3] - 2026-10-19

### Added
- **Engine Performance & Latency Audit**:
  - In-process benchmark execution harness (`without_tcp`) profiling elapsed time, throughput, and per-op latency across 12 hardware threads.
  - Sub-10ns execution latency on atomic counters (8.3 ns) and contended key paths (8.15 ns).
  - Comprehensive vector and full-text search profiling: HNSW ANN queries (5.2k q/s at ~192.5 µs latency), FLAT KNN, and BM25 text term search (41.5k q/s at ~24.1 µs).
- **io-uring Batched Completions**:
  - Worker loops gate ring submissions to batch pending socket writes when multiple connections have unwritten data, amortizing completion queue submission costs.

### Optimized
- **Hot Key Contention Scaling**:
  - TTAS exponential spin backoff (`MAX_SPIN_BACKOFF = 64`) coupled with thread yielding prevents cache-line bouncing, lifting Hot Key throughput to **122.68M ops/sec** (76.2× vs Redis Cluster).
- **Atomic Counters**:
  - Direct 64-byte aligned `Entry` in-place integer mutation reaches **120.49M ops/sec** (18.4× vs Redis Cluster).
- **Pub/Sub Fan-Out Delivery**:
  - Lock-free Arc snapshot channel distribution reaches **152.20M msg/sec** delivery (18.8× vs Redis Cluster).
- **Pipelined KV Throughput**:
  - Pipelined GET achieves **40.21M ops/sec** (~24.9 ns/op).
  - Pipelined SET achieves **28.32M ops/sec** (~35.3 ns/op).
  - Pipeline-64 SET reaches **12.14M ops/sec** (~82.4 ns/op).
- **Queues, Hashes & Documents**:
  - LPUSH/RPOP achieves **67.72M ops/sec** (9.44× vs Redis Cluster).
  - HSET/HGET achieves **62.51M ops/sec** (9.43× vs Redis Cluster).
  - JSON.GET documents achieves **22.96M ops/sec**; JSON.SET achieves **11.07M ops/sec**.
  - HyperLogLog PFADD/PFCOUNT reaches **61.82M ops/sec** (10.3× vs Redis Cluster).

## [0.2.2] - 2026-10-10

### Added
- **Vector Search & AI (RedisVL-compatible)**:
  - Native `FT.CREATE`, `FT.INFO`, `FT.DROPINDEX`, `FT.ALTER`, `FT.ALIASADD`/`UPDATE`/`DEL`/`LIST`, `FT.SEARCH`, `FT.AGGREGATE` (with `WITHCURSOR` + `FT.CURSOR READ/DEL`), `FT.CONFIG`, `FT._LIST`, and `MODULE LIST`.
  - KNN (`*=>[KNN k @field $param AS alias]`) and `VECTOR_RANGE` queries with `PARAMS`, tag/numeric/text pre-filters (incl. negation, unions, exclusive bounds), `NOCONTENT`/`WITHSCORES`/`LIMIT`/`RETURN`/`SORTBY`/`INKEYS`/`DIALECT`.
  - Binary-safe `HSET` end to end (float32 embedding blobs via redisvl `array_to_buffer`), zero-copy `bytemuck` distance kernels (cosine/L2/IP) with bounded top-K selection.
  - **HNSW approximate index**: per (index, field) graph with incremental updates on new keys, flat-arena vector storage, `M`/`EF_CONSTRUCTION` config and `EF_RUNTIME` queries — 190x faster KNN than the old exact-scan path at 50k×768d (warm 0.04–0.06ms/q), top-k matching exact FLAT.
  - **Synchronous FT.CREATE backfill**: `FT.CREATE` over a populated keyspace builds every vector index inside the command (the documented RediSearch contract), so the first query never pays the multi-minute build — the old lazy first-query build stalled 14m on 50k×768d; it is now a 0.2ms warm query after a 24s (DRAM-bound, hnswlib-competitive) CREATE. Background builds remain for cold entries and streaming deltas.
  - **Attribute inverted index** for `FT.SEARCH` filters: generation-stamped tag/numeric/text postings + sorted numeric arrays turn a filtered search from a full keyspace scan (per-doc tag split + numeric parse) into a postings intersection that materializes only the `LIMIT` page — 36x faster tag+numeric filtering (112 → 4.0k q/s at 50k docs). Falls back to the exact scan for tag/text globs, `SORTBY`/`INKEYS`, or when the keyspace tracks TTLs (lazy expiry moves no generation).
  - **Text postings** (same generation-stamped index): text terms resolve through token postings that mirror the scan's word split + ASCII-case folding, and negations (`-@field:term`, `-@tag:{x}`) resolve as complements of the matching set — 90-109x faster text terms (656 → 59-71k q/s at 50k docs), 46x faster text negation (269 → 12.3k q/s). Text predicates also unlock the pre-filtered KNN allow-set path.
  - **`M`/`EF_CONSTRUCTION` actually wired**: `FT.CREATE ... M 32 EF_CONSTRUCTION 200` now sizes the graph's edge caps and candidate-search width (previously parsed and ignored), level 0 keeps `2×M` per hnswlib sizing (measured: recall@10 0.940 → 0.970 at 5k×64d, ~27% more query time — recall wins at 0.08ms/q), and `FT.INFO` reports both attributes. Build time now scales with M (50k×768d: M=16 33s, M=32 85s).
  - **`VECTOR_RANGE` filters**: the filter expression before the vector clause (`(@category:{tech} @embedding:[VECTOR_RANGE $r $v]`) is parsed and applied instead of silently dropped.
  - **Query parser paren fixes**: `@title:(a b)` groups no longer lose their last character to an off-by-one depth count (terms were mangled mid-query), whitespace-separated terms in a group are ANDed, `|` alternation is a real union, and vector-clause tails `[...]` in filter position scan to the correct closing bracket.
  - **KNN bounds**: `KNN k` is capped at 10,000 and `EF_RUNTIME` at 100,000 — a query with `k = usize::MAX` previously aborted the whole process on the heap preallocation (`panic = "abort"`); it now returns an error. An inverted numeric range (`@views:[40 20]`) returns an empty result instead of a slice panic in the postings fast path.
  - **Graph-build generation stamp** is taken before the keyspace scan, so a write landing mid-build leaves the entry stale and the next query's delta picks it up (post-scan stamping published the post-write generation and hid those writes indefinitely).
  - **Pre-filtered KNN**: a resolvable filter maps to a vector-id allow-set, then exact top-k over the subset (parallel when it exceeds L3) or unfiltered graph traversal collecting only matches (hnswlib semantics) — 23x faster filtered KNN (10 → 251 q/s).
  - Parallel shard-range scans (up to 8 threads) for FLAT KNN on ≥20k-key keyspaces; lazy field materialization only for the returned page.
  - O(1) `FT.INFO` doc counts via keyspace-length-keyed cache; real `num_records`/`doc_table_size_mb` stats; flat `FT.ALIASLIST` shape.
  - Search index + binary value persistence in the RDB snapshot and replication stream.
- **Compatibility**: `redis_version` in `INFO`/`HELLO`, search module registration (`ver 20811`), `parking_lot` locks, `bytemuck` dep.
- **Benchmarks**: `-m vec` mode — pipelined binary vector load (205k docs/s), FLAT KNN, filtered KNN, tag+numeric filters, text term/mixed/negation phases, concurrent clients, and HNSW phases incl. an `M 32 EF_CONSTRUCTION 200` variant; HNSW build time measured around `FT.CREATE` (which owns the synchronous backfill), not the first query; `task build-avx` for x86-64-v3 (FMA) codegen.
- **Search engine overhaul** (`crates/fyro-vector`, `src/search`):
  - HNSW with fixed-stride `u32` level-0 blocks, parallel insertion, the `mL = 1/ln M` level distribution, Malkov's neighbour heuristic, tombstones with compaction, and BFS/RCM reordering after bulk builds.
  - ISA-dispatched kernels (AVX-512 incl. VNNI/VPOPCNTDQ, AVX2+FMA, NEON, scalar), plus FLOAT16/BFLOAT16/INT8/UINT8 vector types.
  - `QUANTIZATION INT8 | RABITQ | NONE` with exact rerank from the stored field (INT8 default for HNSW ≥256 dims).
  - Filtered KNN over bitset allow-lists, with the brute-force / ACORN-1 / in-traversal strategy chosen by selectivity. Graph-based `VECTOR_RANGE`.
  - FT.* queries run on a persistent, pinnable thread pool; the connection parks meanwhile. FLAT scans batch concurrent queries into one pass.
  - Incremental index maintenance through a keyspace write hook and per-index dirty sets. Graphs and index settings persist in RDB v3.
  - BM25 text ranking with a shared Unicode tokenizer and stopwords, `FT.HYBRID` with RRF/LINEAR fusion, and streaming `FT.AGGREGATE` GROUPBY/REDUCE.
- **Blocking commands**: BLPOP, BRPOP, BLMOVE, BRPOPLPUSH, BZPOPMIN, BZPOPMAX, XREAD BLOCK and XREADGROUP BLOCK park the connection and wake only on writes to their keys, with per-worker timeout heaps.
- **Stream consumer groups**: XREADGROUP (`>`, history, NOACK), XPENDING (summary/extended), XCLAIM, XAUTOCLAIM, XGROUP SETID/CREATECONSUMER/DELCONSUMER, XINFO STREAM/GROUPS/CONSUMERS; XADD `ms-*` IDs, MINID and approximate trimming with LIMIT. Groups persist in the RDB.
- **Forkless point-in-time snapshots**: a per-entry snapshot bit lets writers hand pre-images to the saver, and values are encoded without cloning.
- **Optional AOF** (`FYRODB_AOF=1`): per-thread state logs with global sequence numbers, k-way-merge replay, and a rewrite after every snapshot.
- **Runtime knobs**: `FYRODB_AFFINITY=auto`, `FYRODB_WORKER_CORES`, `FYRODB_SEARCH_CORES`, `FYRODB_BUSY_POLL_US`, and an experimental `io-uring` feature for batched sends (`FYRODB_IO_URING=1`).
- **Measurement**: open- and closed-loop latency histograms (`-m lat`), bytes per key (`-m mem`), ANN recall on SIFT/GIST or synthetic data (`-m ann`), JSON output with `scripts/bench_compare.py`, criterion micro-benchmarks, Valkey / Redis 8 baselines, perf/flamegraph/heaptrack tasks, and the `cargo pgo` flow.

### Optimized
- **HNSW arena milestone-shrink**: arena buffers now shrink at each power-of-two node count milestone tracked by `last_shrink: AtomicUsize`, so cache density improves on streaming inserts (not only after bulk builds).
- **Thread-local Scratch zero-alloc**: `Scratch` gains `q_int8`, `acorn_hop`, and `acorn_frontier` fields; ACORN traversal and two-pass FLAT scans share pre-allocated working memory with zero hot-path allocation after warmup.
- **Two-pass FLAT scan**: an Int8 pre-filter pass narrows candidates before exact f32 rerank; `q_int8` codes are carried in `Scratch` to avoid per-query allocation.
- **Lock-free search pool**: job dispatch switches from `Mutex<VecDeque<Job>>` to `crossbeam::SegQueue<Job>`, removing the lock on every job submission.
- **`prepare_batch` for burst HSET**: N consecutive HSET writes on the same vector field take one write-lock instead of N, cutting lock contention on bulk ingestion.
- **`cursor_read` zero-clone**: replaced clone-and-drain with `Vec::split_off`, eliminating one full cursor-row clone per `FT.CURSOR READ` page.
- **`complement_ids` sorted-merge**: negated filter sets use an O(output) sorted merge over the ascending id range instead of a 100 KB boolean array per query.
- **ZSet no-alloc update**: `ZSetData::insert` probes the member index before allocating a `SmallStr`, skipping all allocation on score-update paths.
- **O(bytes) bitmap ops**: `BITCOUNT` and `BITPOS` in byte mode operate at the byte level using hardware `count_ones()` / `leading_zeros()` instead of per-bit loops.
- Zero clippy warnings across all crates.

### Changed
- `Entry` stays 64 bytes without a stored hash (shards rehash on growth). Inline keys and strings grew from 15 to 23 bytes; `FyroDB` stays 24 bytes, enforced by static asserts.
- Lists are a packed quicklist; sorted sets are chunked with a member → score index (ZSCORE O(1), ZRANK/ZREM O(log N) instead of O(N)); streams are varint-packed nodes.
- Pipelined commands prefetch their slots and entries ahead of execution.
- Active expiry is an adaptive 10 Hz cycle with a 25 ms budget instead of a fixed once-per-second scan.
- The Docker image builds for x86-64-v3 on amd64 (`--build-arg TARGET_CPU=x86-64` for a baseline image).
- Source layout: `src/commends` → `src/commands`; the connection, store and RDB modules are split into directories; search lives in `src/search`, vector code in `crates/fyro-vector`.

### Fixed
- XREAD treated the given ID as inclusive; it is now exclusive, as in Redis. `$` resolves once at block time.
- XACK now also clears the consumer's pending list, and XGROUP CREATE on an existing group returns BUSYGROUP instead of resetting it.
- Error replies no longer carry a doubled `ERR ERR` prefix, and `NOGROUP` / `BUSYGROUP` codes pass through.
- The expiry sweep re-checks the TTL under the entry lock, so a key that just got a new TTL is not deleted.
- SCAN, RANDOMKEY and RDB save no longer clone every value they visit.
- `LRANGE` on a drained list or with start > stop returns an empty array; drained lists read as absent.
- The RESP parser accepts commands with more than 1024 arguments (cap 16M) and resumes large commands without quadratic reparsing.
- DEL, EXPIRE and FLUSHALL are captured for the AOF (DEL/EXPIRE bypassed the generic capture path).

## [0.2.1] - 2026-09-06

### Optimized
- **Pipelined Batch Coalescing**:
  - Pipelined same-key queue commands (`LPUSH`, `RPUSH`, `LPOP`, `RPOP`) coalesce into a single lock acquisition per run, producing byte-identical replies and slashing queue lock contention.
  - Pipelined `SET` + `EXPIRE` / `PEXPIRE` / `EXPIREAT` / `PEXPIREAT` pairs fuse into atomic set-with-TTL operations under a single lock acquisition.
  - Same-channel `PUBLISH` runs batch into grouped per-worker fan-out queues, eliminating redundant per-message queue pushes.
  - Run-of-1 fast path with zero-alloc dispatch in the core connection event loop.
- **Zero-Allocation Command Paths**:
  - Direct output buffer serialization for `HGETALL`, `SMEMBERS`, `LRANGE`, `ZRANGE`, `HSET`, `HMGET`, `SMISMEMBER`, `ZMSCORE`, `HGET`, `SADD`, `ZADD`, `JSON.SET`, `JSON.GET`, `LPOP`, and `RPOP`.
  - Zero-alloc inline key parsing (`CompactKey`) and SmallStr inline string storage up to 23 bytes.
  - Dropped temporary intermediate `Vec` and heap string allocations across hash, list, set, and json command handlers.
- **Storage & Probing Performance**:
  - Fixed quadratic `ZADD` member re-add scan (eliminating 37x warm-server stalls) with score-ordered indexing optimizations.
  - Floor-probe decomposition in `customhash` for accelerated linear open-addressing table probing.
  - Feature-gated `glibc` and `jemalloc` allocator backends in `rust-zmalloc` (keeping `mimalloc` as default).
- **Cluster Vectorized Transport**:
  - Replaced per-frame buffer assembly with `writev` vectorized I/O for inter-node cluster bus framing (2.6x speedup per frame).

### Added
- **Performance Probes**: Standalone performance verification harnesses: `bench/expireprobe`, `bench/floorprobe`, `bench/queueprobe`, and `bench/probe`.
- **End-to-End Test Suites**: Dedicated integration tests for pipelined queue coalescing (`queue_coalescing.rs`), atomic TTL pairs (`set_expire_pairs.rs`), grouped publish batching (`publish_batching.rs`), and write runs (`write_runs.rs`).
- **Benchmark Flags**: Added `-f` (no-flush) flag in benchmark harness for persistent keyspace accumulation testing.

### Fixed
- **Concurrency & Safety**:
  - Resolved `insert_if_absent` race condition in hash map shards.
  - Fixed `SmallStr` clone double-free edge case.
  - Fixed CRC16 table generation in benchmark cluster harness.
- **Refactoring & Cleanliness**:
  - Cleaned all clippy warnings across tests (`clippy --tests` clean).
  - Bundled `zadd`, `geo`, and `cluster` arguments into named parameter structs.
  - Refactored server worker threads to use `std::thread::scope` for clean thread lifecycle management.

---

## [0.2.0] - 2026-08-28

### Added
- **Redis Cluster Support**: Complete 16,384 hash slots, CRC16 hashing, and hash tag (`{...}`) extraction for single-slot key affinity.
- **Client Redirections**: Automatic `MOVED <slot> <ip>:<port>` and `ASK <slot> <ip>:<port>` redirect replies.
- **Cross-slot Validation**: `CROSSSLOT` error detection for multi-key commands spanning multiple slots.
- **Cluster Management Commands**:
  - `CLUSTER INFO`, `CLUSTER MYID`, `CLUSTER NODES`, `CLUSTER SLOTS`, `CLUSTER SHARDS`
  - `CLUSTER KEYSLOT`, `CLUSTER COUNTKEYSINSLOT`, `CLUSTER GETKEYSINSLOT`
  - `CLUSTER MEET`, `CLUSTER FORGET`
  - `CLUSTER ADDSLOTS`, `CLUSTER DELSLOTS`, `CLUSTER SETSLOT` (`MIGRATING`, `IMPORTING`, `STABLE`, `NODE`)
  - `CLUSTER REPLICATE`, `CLUSTER RESET` (`HARD`/`SOFT`), `CLUSTER SAVECONFIG`, `CLUSTER BUMPEPOCH`, `CLUSTER FAILOVER`
  - `ASKING` command flag support per client connection.
- **High-Availability Replication & Auto-Failover**:
  - Master-replica replication engine (`ReplicationEngine`) with an in-memory replication backlog ring buffer.
  - Asynchronous partial resync (PSYNC) and full RDB resynchronization stream.
  - Gossip-based failure detector (`PFAIL`/`FAIL`) and consensus failover election.
- **Cluster Bus**: Inter-node cluster bus protocol with binary framing over port `port + 10000`.
- **Persistent Configuration**: Redis-compatible `nodes.conf` auto-loading, saving, and epoch management.
- **Benchmarks & Tooling**: 12-master and 6-master Redis Cluster benchmark comparison suite with Docker Compose and Taskfile automation.

### Optimized
- **Tagged-Pointer Hash Probing**: Embedded 7-bit hash tags into `customhash::CustomMap` slot metadata to skip non-matching entries during linear probing without pulling entry memory into CPU cache lines.
- **Contention Spin Backoff**: Exponential backoff with thread yield under spinlock contention to eliminate cache line thrashing and negative scaling on hot keys.
- **Flat 16K Routing Table**: Slot ownership resolved via shared flat routing table, cached per connection and verified in 1 atomic load.
- **Zero-Cost Standalone Path**: Complete bypass of cluster routing checks when cluster mode is disabled (`ClusterRouting::Disabled`).
- **Allocation-Free Pops**: `LPOP` and `RPOP` write replies directly into the TCP output buffer without temporary allocations.
- **Lazy Connection Buffers**: Connections allocate I/O buffers on demand rather than preallocating at accept, reducing idle connection memory footprint.
- **Cache Line Layout**: Cleaned up `Store` layout to prevent false sharing and cache line pollution on the hot dispatch path.
- **Striped TTL Counters & Bloom Filters**: Optimized background expiration scan and maintenance intervals.

### Changed
- **Key Admission Default**: Changed `FYRODB_MAX_KEYS` default to `0` (unlimited, matching Redis `maxmemory 0`).

---

## [0.1.2] - 2026-08-20

### Added
- **Compact Storage**: `CompactKey` (inline keys up to 15 bytes) and `SmallStr` (inline values up to 23 bytes).
- **Compact Collections**: Hashes and lists start in compact sequential representation and promote after 64 elements.
- **Sorted Set Memory Redesign**: Single score-ordered vector replacing two redundant indexes.
- **Compact JSON**: Compact text storage with on-demand parsing for path operations.
- **Internal Allocator**: `rust-zmalloc` crate backed by mimalloc with live allocation tracking and raw EBR helpers.
- **Adaptive Memory Reclamation**: Background EBR collection, page purging, value defragmentation, and shard compaction.

### Optimized
- Peak RSS reduced by 59% (from ~600 MB to 247 MB).
- Packed writer lock, occupied flag, and seqlock generation into a single atomic state word in `CustomMap`.
- In-place value storage in map entries, eliminating `ValueBox` allocation indirection.

---

## [0.1.1] - 2026-08-15

### Added
- **Data Types**: List, Set, Sorted Set, JSON, Bitmap, HyperLogLog, Geospatial, Stream.
- **Commands**: `SUBSTR`, `LCS`, `HRANDFIELD`, `HSCAN`, `EXPIRETIME`, `PEXPIRETIME`, `TOUCH`, `OBJECT`, `SORT`, `MULTI`/`EXEC`/`DISCARD`/`WATCH`/`UNWATCH`, `HELLO 3` (RESP3 protocol support).
- **Authentication**: `FYRODB_AUTH` password protection.
- **RDB v2**: Serialization and persistence for all data types.

### Optimized
- Zero-clone concurrency model using seqlock validation and per-key spinlocks.
- 326 unit and integration test coverage.

---

## [0.1.0] - 2026-08-01

### Added
- Initial release of FyroDB (formerly FlashDB).
- RESP2 protocol server with multi-threaded epoll I/O and lock-free concurrency.
- Core String and Hash commands with RDB snapshot persistence.
