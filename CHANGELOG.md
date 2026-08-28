# Changelog

All notable changes to FyroDB are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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
