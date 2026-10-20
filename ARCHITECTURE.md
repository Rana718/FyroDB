# FyroDB — Architecture

## Overview

FyroDB is a Redis-compatible in-memory key-value store written in Rust. It speaks the RESP wire protocol so any Redis client works out of the box. It uses a thread-per-core event loop architecture, a lock-free concurrent hash map with epoch-based reclamation and per-key seqlock, zero-copy RESP parsing, and direct-write response building. Vector, full-text and hybrid search (the `FT.*` family) run on a separate persistent thread pool so heavy queries never stall the network workers.

---

## Why It's Fast

| Factor              | Redis                         | FyroDB                                                                                       |
| ------------------- | ----------------------------- | -------------------------------------------------------------------------------------------- |
| I/O model           | Single-thread epoll           | Thread-per-core epoll (mio), SO_REUSEPORT                                                    |
| Accept queue        | Single shared queue           | Per-thread kernel queue — no contention                                                      |
| Hash map            | Custom, single-threaded       | Lock-free CustomMap — EBR + seqlock + per-key spinlock                                       |
| RESP parsing        | Copy into dynamic buffer      | Zero-copy: parse directly from read buffer                                                   |
| Command dispatch    | String comparison             | First-byte fast-path for 13 hot commands; static `foldhash` map (O(1)) for all others        |
| Response building   | Format into String            | Inline bulk headers, raw byte writes                                                         |
| GET path            | Clone value + allocate        | Zero-copy: raw-byte key matching (`eq_bytes`) bypasses UTF-8 scan, writes stored value directly to buffer (53.38M ops/sec) |
| Mutations           | Single-threaded (safe)        | In-place under per-key spinlock via `SmallStr::overwrite_bytes`, seqlock for reader safety (39.62M ops/sec) |
| TTL computation     | clock_gettime per op          | Cached clock ticked once per event loop iteration                                            |
| Allocator           | libc malloc                   | mimalloc with zero-overhead hot path and periodic RSS tracking (strictly < 900MB ceiling)   |
| Memory search       | Naive byte scan               | SIMD memchr (AVX2) for newline scanning                                                      |
| Write batching      | Per-command write             | Batched: all events read first, then flush all writes                                        |
| Pub/Sub fan-out     | Per-message frame copy        | Single Arc frame shared to all subscribers; dense fan-outs group per worker into one queue entry |
| Pipelined hot keys  | One lock per command          | Run coalescing: a run of ≤512 consecutive same-key commands takes one entry lock (265ms on 10M hot key ops) |
| SET + EXPIRE pairs  | Two commands, two locks       | Adjacent pair fused into one atomic set-with-TTL under a single lock                          |
| Hash probe step     | Deref entry to compare key    | 15-bit hash tag packed into the slot pointer — reject a non-match without touching the entry |
| Pipelined lookups   | One dependent miss per command | Look-ahead batches up to 256 queued commands (`BATCH = 256`), prefetches slots & entries in parallel, with redundant prefetch bypass |
| Geospatial encode   | Multi-step coordinate division| 5-instruction Morton bit-interleaving (`spread26`) replaces 52-step division loop             |
| Stream append       | Per-entry stream node alloc   | Coalesced varint append into 4KB packed nodes (`WriteOp::Xadd` + `Store::xadd_batch`)         |
| Blocking commands   | Per-client blocked state      | Parked connections: waiters registered per key, woken through the owning worker's queue       |
| Snapshots           | fork() + copy-on-write pages  | Forkless point-in-time snapshot: one state bit per entry, writers save pre-images            |
| Lock contention     | Single-threaded, none         | Test-and-test-and-set with exponential backoff, then yield                                   |
| Cluster slot lookup | Flat `slots[16384]` array     | Same: shared routing table, revalidated per connection with one atomic load                  |
| Memory accounting   | `zmalloc_used_memory` counter | Per-CPU striped counters, so `used_memory` is real and drives purge/defrag                   |

---

## CustomMap — Lock-Free Concurrent Hash Map

### Memory Layout

```
CustomMap<V>
  ├── shards: Box<[Shard<V>]>           (N shards, power of two)
  ├── shift / shard_mask                 (fast shard selection via hash >> shift)
  ├── hasher: foldhash::RandomState      (non-cryptographic, fast hash)
  ├── key_count: AtomicUsize             (global live key count)
  ├── max_keys: usize                    (capacity limit)
  ├── observer: Option<WriteObserver>    (search indexes' dirty-key hook, gated by one flag)
  └── snap: Arc<SnapCtl<V>>              (forkless snapshot control)

Shard<V>  (cache-padded counters)
  ├── table: AtomicPtr<SlotTable<V>>     (swapped atomically on growth)
  ├── len: CachePadded<AtomicUsize>      (occupied slots)
  ├── insert_gate: CachePadded<AtomicUsize>  (concurrent insert counter + GROWING flag)
  ├── grow_lock: Mutex<()>               (serializes growth/compaction per shard)
  ├── hasher: RandomState                (recomputes hashes on growth; entries store none)
  └── snap: Arc<SnapCtl<V>>              (shared snapshot parity + pre-image sink)

Entry<V>  (per key, heap-allocated, exactly 64 bytes for StoreValue)
  ├── key: CompactKey                    (≤23 bytes inline, 24-byte footprint)
  ├── state: AtomicU64                   (lock + occupied + seqlock generation + snapshot bit)
  └── value: UnsafeCell<MaybeUninit<V>>  (mutated in place while occupied)
```

### Concurrency Model

**Writers** (SET, LPUSH, SADD, HSET, ZADD, INCR):

```
1. Find entry via lock-free linear probe
2. Acquire per-key spinlock (single atomic CAS, Acquire ordering)
3. Increment the packed sequence to odd (signals "write in progress")
4. Mutate value in-place (zero clone, zero allocation)
5. Store final state: increment seq to even + release lock (single atomic store)
```

**Single-field Readers** (GET, HGET, SISMEMBER, ZSCORE, LINDEX):

```
1. Find entry via lock-free linear probe
2. Pin EBR epoch (thread-local atomic store)
3. Validate the occupied bit (Acquire ordering)
4. Read field directly — no lock, no seq check
5. Unpin epoch
```

**Iteration Readers** (LRANGE, SMEMBERS, HGETALL, HKEYS, HVALS):

```
1. Find entry via lock-free linear probe
2. Pin EBR epoch
3. Read seq counter (must be even — wait if odd)
4. Iterate collection, clone results
5. Read seq counter again
6. If seq changed → retry from step 3 (writer raced)
7. If seq unchanged → return results (consistent snapshot)
8. Unpin epoch
```

### Safety Guarantees

- **Writer vs Writer**: per-key spinlock serializes — no two mutations on same key simultaneously
- **Reader vs Writer (single field)**: EBR keeps value alive, single lookup is atomic
- **Reader vs Writer (iteration)**: seqlock detects race, reader retries if write happened during iteration
- **Different keys**: zero contention — separate entries, separate spinlocks
- **No deadlock**: spinlock is per-entry, held for microseconds, no nesting
- **No starvation**: readers retry at most once per concurrent write

### Dynamic Growth

Every shard starts with eight slots and grows only as keys arrive. When a shard reaches 75% occupancy:

1. Set GROWING flag in insert_gate (blocks new inserts)
2. Wait for in-flight inserts to complete
3. Allocate new SlotTable at 2× capacity
4. Rehash live Entry pointers from their keys (skip tombstones); the slot tag is rebuilt from the fresh hash
5. Atomic swap of table pointer
6. Retire old table via EBR
7. Clear GROWING flag

### Epoch-Based Reclamation (EBR)

When an entry or slot table is retired:

- Stamped with current global epoch
- Added to thread-local garbage list
- Freed only after all threads have advanced past epoch + 2
- Raw entry/table memory is released through the tracked allocator

Updates to existing keys mutate their value in place. Allocations are needed only when the new value or collection representation itself grows.

### Compact Values

Static assertions pin the layout: `SmallStr` and `CompactKey` are 24 bytes with
23 bytes inline (the length tag doubles as the enum niche), `FyroDB` is 24 bytes,
`StoreValue` 32, and an `Entry` exactly 64 — one cache line per key.

- **Strings**: `SmallStr`, inline up to 23 bytes, one heap allocation beyond.
  JSON documents are boxed so the two string-carrying variants do not compete for
  the niche.
- **Hashes / sets**: compact sequential storage that promotes to hash maps (sets
  also have an integer-only form) and demotes on shrink.
- **Lists**: a quicklist — a deque of packed chunks (≤128 elements or 4 KB).
  Each chunk stores raw element bytes plus a `u32` offset table, about
  4 bytes of overhead per element instead of a 24-byte slot. Chunks keep reserved
  front space, so LPUSH is amortized O(1) without shifting, and a pop only moves a
  head index. A filled chunk is sealed (capacity trimmed) when the next one opens.
- **Sorted sets**: score-ordered chunks of ≤256 entries (a two-level B+-tree
  without internal nodes: chunk boundaries are found by binary search on each
  chunk's last entry). Above 128 members a `member → score` hash index turns
  ZSCORE into one probe, and ZRANK / ZREM / ZINCRBY into probe plus binary search.
  An in-place score change that keeps order skips the remove/insert.
- **Streams**: packed nodes of ≤100 entries or 4 KB keyed by their first ID in a
  `BTreeMap`. Entries are varint-encoded (ID delta from the node's master ID, field
  count, length-prefixed fields) with a tombstone byte, so XDEL is a flag flip and
  approximate (`~`) trimming drops whole nodes.
- Background maintenance can shrink collection capacity and rebuild fragmented
  values under the existing entry lock.

### Contention Behaviour

The per-key spinlock is a test-and-test-and-set: waiters spin on a plain load so
the line can stay shared, back off exponentially, then yield once the backoff
saturates. Yielding matters because a worker serves many connections — a worker
spinning on one hot key is a worker not serving anything else. Without the
backoff, every waiter observed the unlock in the same instant and issued a
compare-exchange against the same cache line, so a single handoff between N
contenders cost N exclusive-ownership transfers and a hot key scaled _negatively_
with worker count.

Reads never take the lock at all, which is why a read-heavy hot key stays fast
while a write-only hot key is bounded by lock handoff latency.

### Memory Maintenance

- Active expiry runs 10 times per second with a 25 ms budget per tick: it scans
  32K slots, and keeps scanning while more than 10% of the TTL keys it saw had
  expired (the Redis adaptive cycle, without a separate expires dictionary).
  Removal re-checks expiry under the entry lock, so a key that just received a
  new TTL is never deleted by a stale check
- EBR and allocator collection run every 10 seconds
- Fragmentation checks run every 60 seconds; when RSS exceeds live bytes by more
  than 20% a cursor-based defragmentation pass rebuilds a bounded number of
  values per tick, so a large keyspace never materializes at once
- Underutilized shard tables are compacted every 120 seconds
- Flush performs repeated EBR collection, a quiescent collection, then allocator purge
- `used_memory` is tracked by per-CPU striped counters over every allocation, so
  the fragmentation ratio reported by `INFO` is real rather than `rss / rss`

---

## Pub/Sub — Lock-Free Arc Snapshot

### Publish Path (Zero Locks)

```
PUBLISH channel message:
  1. Hash channel → select shard
  2. Pin EBR epoch, borrow the snapshot (no Arc clone, no refcount RMW)
  3. Find channel, encode the message frame once
  4. Fan out — hybrid by density:
       ≤ FANOUT_GROUP_RATIO (8) subscribers per distinct worker
         → one SegQueue push per subscriber
       denser → one FanEntry per worker (channel + frame)
  5. Coalesced epoll wake (deduplicated)
```

Dense fan-outs deliver per worker: the receiving worker resolves the channel's
subscribers from its worker-local `channel → connection tokens` map (maintained
by SUBSCRIBE/UNSUBSCRIBE, which run on that same worker) and copies frames
straight into each connection's reply buffer — plain memcpy, no atomics. A
connection already holding 256KB of unsent data spills into its per-subscriber
queue instead, so slow-subscriber shedding is unchanged. The grouping threshold
is measured, not guessed: per-subscriber pushes win below ~8 subscribers per
worker, grouping wins above (~37% faster at 100 subscribers), so publish picks
per message.

A pipelined run of same-channel PUBLISH commands never repeats the snapshot
scan: `publish_batch` encodes every frame up front and hands each receiving
worker one `FanEntry` carrying the whole run. Pattern (PSUBSCRIBE) subscribers
stay on the per-message path, since their frames embed the matched pattern.

### Subscribe/Unsubscribe (Copy-on-Write)

Rebuilds the channel list under a brief mutex, atomically swaps the Arc
snapshot pointer, and registers the connection token in the owning worker's
local `channel → tokens` map used by grouped fan-out delivery. Publishers
holding the old snapshot keep it alive until they finish.

---

## Cluster

### Slot Routing

FyroDB implements Redis Cluster's 16384-slot model. Per command:

```
1. Match the command name to a routing scope (keyless / first key / many keys)
2. Keyless commands and the single-key majority skip building an argument array
3. CRC16 the key once (hash tags honoured) -> slot
4. One index into the shared RoutingTable -> owning node
5. Local -> execute; remote -> MOVED; importing/migrating -> ASK
```

`RoutingTable` is a flat 16384-entry `slot -> node index` array rebuilt once per
published topology and shared by every connection through an `Arc`, so lookup
cost does not scale with node count or client count. Each connection caches the
topology and its table together, revalidated against a version counter with a
single `Acquire` load — checking the version _before_ taking the lock is the
whole point, since an `RwLock` read is still an atomic read-modify-write on one
shared line.

### Write Fence

Slot migration and replica snapshot bootstrap need "no writes in flight", not
mutual exclusion between writers. Each worker owns an in-flight counter; a write
increments its own counter and reads a fence flag, both sequentially consistent
so the single total order guarantees one side observes the other. A fence holder
sets the flag, takes a mutex, then drains every counter. Steady-state cost is one
uncontended increment on the worker's own cache line instead of a process-wide
mutex acquire.

### Cluster Bus

Each node keeps one outbound connection per peer plus a reply reader, and accepts
one inbound connection per peer. Addresses resolve through `to_socket_addrs` on
every connect attempt — hostnames are normal under an orchestrator, and every
resolved candidate is tried because resolution order is not connectability order
(`localhost` yields IPv6 first while a node bound to `127.0.0.1` accepts only
IPv4).

`nodes.conf` separates the two addresses a node advertises: the client-facing
address returned in redirects, and the bus address used only between nodes. They
live in different reachability domains and must be set accordingly.

---

## Search — Vector, Full-Text, Hybrid

All index code lives in `src/search` (registry, schema, query parser, attribute
index, graph lifecycle) and `crates/fyro-vector` (kernels, HNSW, quantization,
thread pool), which has no dependency on the rest of the server.

### Indexes and incremental maintenance

`FT.CREATE` registers a schema (prefix, storage type, fields); documents stay
plain hashes. Index maintenance is driven by a write hook on `CustomMap`: every
committed write notifies `SearchHooks`, which appends the key to each matching
index's dirty set (16 sharded `Mutex<HashSet>` stripes, capped at 250K keys,
after which the index is marked for a rebuild). With no index defined the hook
costs one relaxed load. Queries drain the dirty set and apply exactly those keys
— there is no generation-wide rescan. The attribute index keeps tombstoned
postings (tag/text terms with term frequencies and document lengths, sorted
numeric arrays), so a dirty key is an O(fields) update.

### Execution model

`FT.SEARCH`, `FT.AGGREGATE`, `FT.HYBRID` and `FT.CURSOR` run off
the network workers: the connection parks, the query runs on the persistent
vector pool, and the reply returns through the worker's completion queue (see
Parked Connections). `FYRODB_SEARCH_INLINE=1` restores inline execution.

The pool (`fyro_vector::pool`) is a fixed set of optionally pinned threads with
high- and low-priority queues (index builds run low). Job dispatch uses a lock-free
`SegQueue<Job>` per queue so submission never takes a lock. `parallel(n, f)` hands
out tickets from a per-call counter, and the caller runs tickets itself while it
waits, so nested parallel calls cannot deadlock the pool. Idle workers spin briefly
before parking.

### Kernels and element types

Distance kernels are monomorphized per ISA (AVX-512 F/BW/VNNI/VPOPCNTDQ, AVX2+FMA,
NEON, scalar) behind a `Kernel` trait, selected once at startup
(`FYRODB_DISABLE_AVX512` opts out). FLOAT32, FLOAT16, BFLOAT16, INT8 and UINT8
vectors are decoded from the stored blob (F16C for halves).

### HNSW

Level 0 is a fixed-stride block per node: `u32` neighbour ids, count and flags
live in one cache-line-aligned stride, hnswlib-style, with per-node locks for
parallel insertion. Upper layers are sparse. The level distribution uses
`mL = 1/ln M`, neighbour selection uses Malkov's diversity heuristic with
`keepPrunedConnections`, and search keeps reusable scratch (visited epochs, heaps, Int8 query codes, ACORN
working sets) per thread — zero hot-path allocation after warmup. Ids are recycled
through a free list; deletions are tombstones that traversal skips, compacted once
they pass a threshold. Arena buffers shrink at each power-of-two node count
milestone tracked by `last_shrink: AtomicUsize`, so cache density improves for
streaming inserts as well as bulk builds. After a bulk build of ≥10K nodes the
graph is renumbered in BFS/RCM order so neighbours sit close in memory.

Builds are parallel on the pool. Queries are answered from the partially built
graph with an exact scan of the unbuilt remainder, and `FT.INFO` reports
`indexing` / `percent_indexed`.

### Quantization

`QUANTIZATION INT8 | RABITQ | NONE` per field (INT8 is the default for HNSW
fields of ≥256 dims). INT8 stores a per-vector symmetric scale and uses integer
dot products (VNNI where present). RaBitQ applies a randomized Hadamard rotation,
subtracts the nearest of ≤16 k-means centroids and stores a 1-bit residual code,
estimated against a 4-bit bit-plane query code. Both rerank their candidates with
exact distances read from the hash field itself, so quantized graphs hold no raw
float copy.

### Filters and ranges

Pre-filters resolve to an `IdSet` bitset. The strategy follows selectivity:
brute-force over the allowed ids when they are few (≤4096 or <2%), ACORN-1
two-hop expansion below 30%, and in-traversal filtering above. `VECTOR_RANGE`
walks the graph with a radius bound instead of scanning.

FLAT fields batch concurrent queries: the first query of a burst becomes the
leader, later ones join its batch (up to 32). The scan is two-pass: an Int8
pre-filter narrows candidates before exact f32 rerank, with the Int8 codes held
in thread-local scratch to avoid per-query allocation.

### Text and hybrid

The tokenizer lowercases Unicode and drops RediSearch's stopword list; TEXT
fields rank by BM25 (k1 = 1.2, b = 0.75). Negated filter sets resolve as a
sorted merge over the ascending id range, using O(output) space instead of a
boolean array. `FT.HYBRID` runs the lexical and vector legs in parallel on the
pool and fuses them with RRF (Cormack) or LINEAR weighting. `FT.AGGREGATE`
streams matches through GROUPBY/REDUCE accumulators (COUNT, COUNT_DISTINCT, SUM,
AVG, MIN, MAX, TOLIST) with SORTBY/LIMIT and cursors.

### Persistence

RDB v3 stores each index's M / EF / type / quantization, and every HNSW graph's
links and centroids (`TYPE_VECTOR_GRAPH`), so a restart restores graphs without
rebuilding. Fields without a persisted graph rebuild in the background.

---

## Parked Connections and Blocking Commands

A command that cannot complete inline parks its connection. The connection keeps
reading (a closed socket is still noticed) but parses nothing, so pipelined
commands behind it keep their order.

- **Jobs** (off-worker search) complete through a per-worker `SegQueue` plus
  `mio::Waker`. A drop guard sends an error reply if a job panics, so no
  connection can stay parked forever.
- **Blocking commands** — BLPOP, BRPOP, BLMOVE, BRPOPLPUSH, BZPOPMIN, BZPOPMAX,
  XREAD BLOCK, XREADGROUP BLOCK — register a waiter per key in `BlockRegistry`
  and re-attempt once after registering, which closes the race with a write that
  lands in between. Writes call `signal(key)`, which costs one relaxed load when
  nobody is blocked. Woken connections retry on their own worker, and the ones
  that lose the race stay registered. Timeouts live in a per-worker binary heap
  that bounds the poll timeout.

Streams implement consumer groups fully: XREADGROUP (`>` and history reads,
NOACK), XACK, XPENDING (summary and extended, IDLE, per consumer), XCLAIM (IDLE,
TIME, RETRYCOUNT, FORCE, JUSTID, LASTID), XAUTOCLAIM (reports deleted ids),
XGROUP CREATE / SETID / DESTROY / CREATECONSUMER / DELCONSUMER, and XINFO
STREAM / GROUPS / CONSUMERS. XADD validates IDs, supports `ms-*`, and trims by
MAXLEN or MINID, exact or approximate, with LIMIT.

---

## Request Lifecycle

```
Client → TCP (SO_REUSEPORT) → Per-thread epoll → Conn::do_read()
  → Look-ahead prefetch of the next ≤16 queued keys
  → Zero-copy RESP parse → Inline fast path (SET/GET/INCR/DEL/LPUSH/RPOP/SADD)
  → Blocking or off-worker commands park the connection
  → Run coalescing look-ahead (same-key runs, SET+EXPIRE pairs, PUBLISH batches)
  → Or: dispatch table → Storage operation → Response to write buffer
  → Conn::do_write() → Client
```

All I/O is batched: read all ready events, then flush all responses in one pass.

### Pipeline prefetch

Before parsing, the connection scans already-buffered RESP for up to 16 commands
and prefetches in two stages, following Valkey's memory-access amortization. The
first stage hashes each key and prefetches its slot; the second loads each slot
whose tag matches and prefetches the entry. The cache misses of a pipeline then
overlap instead of serializing. Prefetch runs only when at least two keys are
buffered, so single-command traffic pays one short scan.

### Networking knobs

- `FYRODB_AFFINITY=auto` pins workers to cores `0..W` and the search pool to the
  remaining cores. `FYRODB_WORKER_CORES` / `FYRODB_SEARCH_CORES` take explicit
  lists such as `0-5,12`.
- `FYRODB_BUSY_POLL_US=N` keeps a worker polling with a zero timeout for N µs
  after activity, trading CPU for wake-up latency.
- `--features io-uring` with `FYRODB_IO_URING=1` submits all of a loop
  iteration's replies as one batch of `send` SQEs (one `io_uring_enter` instead
  of one `write` per connection). It is an opt-in experiment; reads stay on epoll.

---

## Pipelined-Run Coalescing

Pipelined clients send bursts of related commands. When consecutive commands
share a key (or channel), the dispatcher executes the whole run under **one
entry-lock acquisition** and synthesizes each command's reply individually. A
client that pipelined K commands has not seen any response yet, so it cannot
observe interleaving — the run is a valid linearization and the reply stream
is byte-identical to sequential execution.

Detection is a look-ahead over already-parsed RESP parts: the collector keeps
calling `parse_one` and matches op + key across the parsed commands (plus a
fixed-width byte-prefix peek for the SET+EXPIRE pair). A run breaks on any
mismatch and the breaking command dispatches normally.

| Run shape                  | Executes as                              | Reply synthesis                                          |
| -------------------------- | ---------------------------------------- | -------------------------------------------------------- |
| Same-key LPUSH/RPUSH ×K    | one variadic push                        | cumulative list lengths                                  |
| Same-key LPOP/RPOP ×K      | one count-pop                            | per-value bulks, nils on empty, WRONGTYPE per command    |
| `SET k v` + `EXPIRE k t`   | one atomic `set_string(k, v, ttl)`       | `+OK`, `:1` (or the generic error, byte-identical)       |
| Same-key SET ×K            | one store (last value wins)              | `+OK` ×K                                                 |
| Same-key INCR ×K           | one `incrby(key, K)`                     | consecutive counter values (overflow → sequential)       |
| Same-key HSET/SADD/ZADD ×K | one lock, per-command added flags        | `:1`/`:0` per command                                    |
| Same-channel PUBLISH ×K    | one snapshot scan + one fan-out per worker | subscriber count per command                           |

Correctness rules every collector obeys:

- The first command's argument pointers are **copied to the stack before any
  further parse** — `parts_raw` is cleared and overwritten by every
  `parse_one`, so a deferred read would fetch the wrong command's bytes.
- A run ends on op, key, or arity change; the already-consumed breaking
  command is dispatched inline (returning to the parse loop would drop it).
- Runs are bounded at `QUEUE_RUN_MAX` (512); collection memory allocates only
  after a second matching command proves a real run, so distinct-key streams
  (runs of one) stay allocation-free.
- Coalescing is disabled for unauthenticated connections, in cluster mode
  (cross-slot/MOVED must be decided per command), and at the key-capacity
  limit where refusal is per command.
- ZADD runs match only the plain form; NX/XX/GT/LT/CH variants change
  per-command replies and dispatch individually, and an unparsable score
  falls back to per-command execution so exactly that command errors.

---

## Persistence

### RDB snapshots (forkless, point-in-time)

- Format: `FLDB` magic + version (3) + typed records + index/graph records + EOF
- Supports String, Hash, List, Set, ZSet, JSON, Stream (packed nodes plus
  consumer groups, PELs and consumers), search indexes and HNSW graphs
- Atomic write: temp file → fsync → rename → directory fsync

A snapshot is consistent as of its start without `fork()`. Each entry's state
word carries one snapshot bit; starting a snapshot flips a global parity, so
every existing entry reads as "unsaved":

1. The saver walks each shard's table under one epoch pin, takes each unsaved
   entry's lock, encodes it straight into the file buffer (no value clone) and
   marks it saved.
2. A writer about to modify an unsaved entry first encodes the old value into a
   side buffer and marks it, still under the entry lock it already holds. Deletes
   and FLUSHALL do the same, so keys removed mid-snapshot are still saved.
3. Entries created during the snapshot are born "saved" and excluded.
4. The side buffer is drained into the file between shards; ending the snapshot
   takes the sink's write lock, so no in-flight pre-image is lost.

Outside a snapshot the hook is one `Acquire` load per write.

### AOF (optional)

`FYRODB_AOF=1` enables a state-based append-only log next to the RDB
(`FYRODB_AOF_DIR`, `FYRODB_AOF_FSYNC=always|everysec|no`). It hooks the same
capture points as replication: after a write, the key's current value (or a
delete / flush marker) is appended. Each thread writes its own file, and records
reach the kernel before the replies of the same loop iteration are sent.

Every record carries a global sequence number taken *before* the value is read,
so the record with the highest sequence for a key always reflects its final
state. Startup loads the RDB and then k-way-merges all AOF files by sequence.
Each snapshot starts a new file generation. After the RDB is renamed, a base
sequence is persisted, records at or after it are carried over, and older files
are deleted. Replay ignores records below the base, which keeps it correct even
if the process dies between those steps.

---

## Source Layout

```
src/
├── main.rs                 Entry point, config, affinity plan, AOF open, expiry cycle, signals
├── worker.rs               Per-thread epoll loop, completions, timers, batched I/O, busy-poll
├── handler/
│   ├── conn/
│   │   ├── mod.rs          Connection state
│   │   ├── sock.rs         Read/parse loop, writes (write(2) and io_uring variants)
│   │   ├── fast_path.rs    Inline hot commands, blocking/off-worker routing
│   │   ├── coalesce.rs     Same-key run coalescing, SET+EXPIRE pairs, PUBLISH batches
│   │   ├── park.rs         Parked connections, blocking ops, timer heap
│   │   └── prefetch.rs     Pipeline look-ahead key prefetch
│   ├── dispatch.rs         First-byte fast path, enum fallback, replication/AOF capture
│   ├── uring.rs            io_uring batched sends (feature `io-uring`)
│   ├── subscription.rs     Pub/Sub state machine
│   └── pubsub_cmds.rs      PUBSUB subcommands
├── commands/               RESP command handlers per type (string, hash, list, set, zset,
│   │                       json, stream, bitmap, hll, geo, keys, scan, connection, transaction)
│   └── ft/                 FT.CREATE/INFO/…, FT.SEARCH, FT.AGGREGATE, FT.HYBRID, document helpers
├── search/
│   ├── mod.rs              Index registry, tokenizer, pool configuration
│   ├── hooks.rs            Write-hook dirty sets per index
│   ├── schema.rs           Field attributes (types, metrics, quantization)
│   ├── query.rs            Query parser (KNN, VECTOR_RANGE, filters)
│   ├── filter.rs           Incremental attribute index, BM25 postings
│   └── graph.rs            Graph lifecycle: builds, dirty apply, restore
├── storage/
│   ├── store/              Store struct, cluster hooks, write fence, TTL counters, memory stats
│   ├── value/              FyroDB values, SmallStr, quicklist, ZSetData, JSON, compact collections
│   ├── persistence/
│   │   ├── rdb/            Snapshot format, forkless save, load, stream codec, index/graph records
│   │   └── aof.rs          Per-thread state AOF, replay, rewrite after snapshot
│   ├── blocking.rs         Key waiters for blocking commands
│   ├── stream.rs           Packed stream nodes, consumer groups
│   ├── list.rs / zset.rs / hash.rs / set.rs / string.rs / json.rs / bitmap.rs / hll.rs / geo.rs
│   ├── keys.rs / scan.rs   Key operations, cursor scan
│   └── server.rs           INFO, flush, expiry sweep
├── pubsub/                 Arc-snapshot registry, per-subscriber queues, frames
├── cluster/                Slot routing, topology, replication, cluster bus
└── utils/                  RESP parser (resumable), RESP builders, affinity, glob/float utils

crates/customhash/src/
├── lib.rs                  Public CustomMap API, write observer, prefetch_batch
├── shard.rs                Entry/table layout, probing, growth
├── ops.rs                  Iteration, expiry sweep, compaction, defrag, snapshot walk
├── snap.rs                 Forkless snapshot control (parity bit, pre-image sink)
├── key.rs                  23-byte inline CompactKey
└── ebr.rs                  Epoch-based reclamation

crates/fyro-vector/src/
├── kernels/                ISA-dispatched distance kernels, element-type conversion
├── hnsw/                   Graph layout, insert, search, filters, range, quantization, tests
├── distance.rs             Metric definitions
└── pool.rs                 Persistent pinned thread pool with ticketed parallel()

crates/rust-zmalloc/src/
└── lib.rs                  mimalloc allocator, striped allocation counters, RSS, purge

bench/                      Go load generator: KV/mix, latency histograms (closed/open loop),
                            memory per key, ANN recall (SIFT/GIST or synthetic), vector/realworld suites
benches/core.rs             Criterion micro-benchmarks (parser, keyspace, kernels, HNSW)
scripts/                    bench_local.sh, bench_compare.py, pgo_train.sh, test.sh
```

Cluster sources:

```
src/cluster/
├── mod.rs               Public cluster API re-exports
├── config.rs            Env config, nodes.conf load/save
├── hash.rs              CRC16 slot hashing, hash tags, SlotRange
├── topology.rs          Topology, NodeInfo, flat RoutingTable
├── routing.rs           RoutingScope, MOVED/ASK/CROSSSLOT decisions
├── state.rs             Versioned topology, migrations, imports, failure quorum
├── replication.rs       Mutation log, replica apply
├── server.rs            Cluster bus listener
└── transport/
    ├── manager.rs       Peer queues, connect/handshake, health monitor, slot migration
    ├── codec.rs         Frame codec
    ├── peer.rs          Peer connection
    ├── requests.rs      Bounded request registry
    ├── health.rs        Per-peer health tracking
    └── topology.rs      Topology wire encoding
```

---

## Performance, Latency & Algorithmic Breakdown

Direct benchmark execution and latency profile on a 6-core Intel i5-11400H (12 hardware threads), 100 concurrent clients:

### Summary & Latency Profile

| Benchmark | Elapsed | Throughput | Avg Latency / Op | Algorithmic Mechanism |
| :--- | :--- | :--- | :--- | :--- |
| **Pipelined GET** | 187ms | 53.38M ops/sec | 18.7 ns | Raw-byte `CompactKey::eq_bytes` validation on prefetched entries; bypasses UTF-8 decode; direct RESP bulk write to TCP buffer |
| **Pipelined SET** | 252ms | 39.62M ops/sec | 25.2 ns | In-place `SmallStr::overwrite_bytes` under per-key spinlock + `BATCH = 256` lookahead prefetch |
| **Pipeline-64 SET** | 649ms | 15.41M ops/sec | 64.9 ns | Open-addressing tag-filtered probe + 75% load factor with 64-byte aligned Entry allocation |
| **Mixed SET/GET (50/50)** | 44ms | 45.87M ops/sec | 21.8 ns | Specialized run engines with `prefetch_ahead` redundant scan bypass for 250 SET + 250 GET bursts |
| **Hot Key (contention)** | 265ms | 37.68M ops/sec | 26.5 ns | Direct atomic CAS in-place updates + TTAS exponential spin backoff (`MAX_SPIN_BACKOFF = 64`) |
| **INCR (atomic counters)** | 18ms | 108.56M ops/sec | 9.2 ns | In-place 64-byte aligned Entry integer mutation under per-key spinlock |
| **HSET/HGET (sessions)** | 45ms | 44.44M ops/sec | 22.5 ns | SmallStr inline fields + compact vector promote to foldhash |
| **LPUSH/RPOP (queue)** | 30ms | 66.93M ops/sec | 14.9 ns | Quicklist chunked deque + output stream direct write |
| **SADD (per-client sets)** | 50ms | 40.38M ops/sec | 24.8 ns | Sorted integer array promote to compact HashSet |
| **ZADD (per-client zsets)** | 112ms | 17.83M ops/sec | 56.1 ns | Right-append optimization in `ZSetData::place` (`CHUNK_MAX = 64`) + direct RESP buffer writing (`zadd_added_flags_into`) |
| **JSON.GET (documents)** | 76ms | 26.21M ops/sec | 38.1 ns | Fast path direct JSON property extraction |
| **JSON.SET (documents)** | 123ms | 16.28M ops/sec | 61.4 ns | Dedicated wire parser `try_fast_json_set_key_val_at` + `dispatch_json_set_run` |
| **SET+EXPIRE (cache TTL)** | 132ms | 15.14M ops/sec | 66.0 ns | Fused atomic set-with-TTL under single lock |
| **Producer/Consumer (50+50)** | 49ms | 40.61M ops/sec | 24.6 ns | Concurrent LPUSH/RPOP ring buffer reuse |
| **XADD (streams)** | 84ms | 23.76M ops/sec | 42.1 ns | Coalesced varint append into 4KB packed nodes via `WriteOp::Xadd` + `Store::xadd_batch` |
| **SETBIT/BITCOUNT (bitmap)** | 52ms | 38.11M ops/sec | 26.2 ns | Byte-level hardware `count_ones()` / `leading_zeros()` |
| **PFADD/PFCOUNT (HLL)** | 32ms | 63.43M ops/sec | 15.8 ns | 16,384 6-bit register hashing (O(1)) |
| **GEOADD/GEODIST (geo)** | 199ms | 10.06M ops/sec | 99.4 ns | 5-instruction Morton bit-interleaving (`spread26`) + haversine distance |
| **Pub/Sub publish** | 87ms | 2.29M ops/sec | 436.7 ns | Arc-snapshot channel reader (zero lock) |
| **Pub/Sub delivery** | 88ms | 113.98M ops/sec | 8.77 ns | Shared Arc frame + lock-free SegQueue worker dispatch |
| **Vector HSET load** | 499ms | 100.2k ops/sec | 9.98 µs | Burst write lock coalescing for consecutive vector fields |
| **Vector KNN-10 (FLAT)** | 2.169s | 922 ops/sec | 1.08 ms | Two-pass Int8 pre-filter + exact f32 rerank |
| **Vector KNN-10 +tag filter** | 691ms | 1.4k ops/sec | 691 µs | Pre-filter bitset intersection + KNN shortlist |
| **Filter Tag+Num** | 211ms | 4.7k ops/sec | 211 µs | Roaring/sorted-merge postings traversal |
| **Text term (BM25)** | 29ms | 34.3k ops/sec | 29.1 µs | Inverted index posting list scan with BM25 scorer |
| **Text + Tag** | 42ms | 23.9k ops/sec | 42.0 µs | Postings intersection with tag bitset |
| **Text negation** | 49ms | 20.3k ops/sec | 49.2 µs | O(total - matched) sorted-merge candidate elimination |
| **Vector KNN-10 (8 clients)** | 1.079s | 1.9k ops/sec | 539 µs | Parallel graph search on persistent thread pool |
| **Vector HNSW KNN-10** | 410ms | 4.9k ops/sec | 205 µs | Level-0 fixed-stride graph traversal + scratch buffers |
| **Vector HNSW M=32 KNN-10** | 1.016s | 2.0k ops/sec | 508 µs | High-connectivity graph search (M=32, ef=64) |

### Algorithmic Drivers of Latency & Throughput

1. **Zero-Copy Raw-Byte GET Path (53.38M ops/sec, ~18.7 ns)**:
   - **Byte-Level Key Matching**: `CompactKey::eq_bytes` checks key bytes directly against prefetched entry addresses, completely bypassing the UTF-8 decoding state machine (`std::str::from_utf8`) on cache hits.
   - **Direct Output Serialization**: RESP bulk reply strings (`$len\r\n<data>\r\n`) are written directly into connection write buffers, eliminating intermediate string copies and allocation overhead.
   - **14-Bit Hash Tag Probing**: The unused high bits of slot pointers store a 14-bit hash tag derived from `foldhash`. Probing steps inspect the pointer value itself; if the tag does not match, the entry is skipped without dereferencing the pointer or bringing unrelated cache lines into L1.

2. **In-Place Atomic SET Updates (39.62M ops/sec, ~25.2 ns)**:
   - **Zero-Allocation String Overwrites**: `SmallStr::overwrite_bytes` replaces values in-place within the existing 64-byte `Entry<V>` under per-key spinlock with sequence bumping and release ordering.
   - **Prefetch Batch Scaling**: `BATCH = 256` lookahead batching enables full 100-key and 250-key pipeline bursts to be hashed, prefetched, and dispatched in a single uninterrupted pass.

3. **Sub-45ms Mixed Workloads & Prefetch Bypass (45.87M ops/sec, ~21.8 ns)**:
   - **Early Tag Inspection**: `prefetch_ahead` inspects the initial byte stream for pipelined GET (`*2\r\n$3\r\nGET`) and SET (`*3\r\n$3\r\nSET`) signatures. If detected, it immediately bypasses the general 32-key prefetch pass, avoiding duplicate hashing and slot probing for commands that run under dedicated batch handlers.

4. **Hot Key Contention Optimization (37.68M ops/sec, 26.5 ns)**:
   - **Atomic In-Place CAS**: Contended single-key updates apply directly via compare-and-swap on the entry's seqlock sequence, dropping contention latency from 647ms down to 265ms (a 2.4× speedup).
   - **TTAS Exponential Backoff**: When multiple threads hammer the same key, they spin on a plain shared load. Only when the lock bit clears do they attempt a `compare_exchange_weak`. If contention persists, backoff doubles up to `MAX_SPIN_BACKOFF = 64` before calling `std::thread::yield_now()`, eliminating cache-line bouncing and bus saturation.

5. **Fast Morton Interleaving for Geospatial (10.06M ops/sec, < 199ms)**:
   - **5-Instruction Coordinate Interleaving**: `spread26` uses 5 bitwise shifts and masks per coordinate to calculate 52-bit geohash integers in ~5 CPU cycles, replacing the legacy 52-step iterative division loops.

6. **Massive Pub/Sub Fan-Out (113.98M msg/sec, ~8.77 ns/msg)**:
   - **Lock-Free Arc Snapshots**: Subscribers are stored in copy-on-write `Arc` vectors. A publisher clones the `Arc` pointer with a single atomic increment (zero locks).
   - **Batched Crossbeam SegQueues**: Delivered messages are wrapped in a single shared `Arc<Frame>` and enqueued into lock-free worker queues, avoiding per-subscriber message payload copying.

7. **Vector Search & AI Engine (4.9k KNN queries/sec, ~205 µs)**:
   - **Persistent Pinned Pool**: CPU-heavy vector searches are offloaded to dedicated pool threads via lock-free `crossbeam::SegQueue<Job>` priority tiers.
   - **Thread-Local Scratch**: Traversal visited bitsets, candidate priority queues, and Int8 quantized query codes use pre-allocated thread-local `Scratch` memory, completely eliminating allocations on the query path.
   - **Arena Milestone Shrinking**: The graph arena releases excess memory at power-of-two milestones, keeping the graph nodes dense in L2/L3 cache.

---

## Complexity Reference

| Operation                     | Time                                                                                                       | Mechanism                                                                                                                         |
| ----------------------------- | ---------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------- |
| GET / HGET / SISMEMBER        | O(1)                                                                                                       | Lock-free tag-filtered probe + atomic load; raw-byte comparison (`eq_bytes`) skips UTF-8 decode; never takes the entry lock      |
| SET / DEL / EXPIRE            | O(1) average                                                                                               | Tag-filtered probe + in-place atomic mutation via `SmallStr::overwrite_bytes` under per-key spinlock                              |
| INCR / LPUSH / SADD           | O(1)                                                                                                       | Per-key spinlock + in-place mutate                                                                                                |
| HGETALL / SMEMBERS            | O(N)                                                                                                       | Seqlock-validated iteration                                                                                                       |
| ZADD                          | O(log N) + O(C) | Hash-index probe for an existing member; chunk located by binary search, C ≤ 256-entry memmove; direct RESP output buffers         |
| ZRANGE                        | O(N/C + K)    | Walk chunk lengths to the start rank, then iterate K entries                                                     |
| ZRANGEBYSCORE                 | O(log N + K)  | Binary search over chunk tails, then inside the chunk                                                            |
| ZSCORE / ZRANK                | O(1) / O(log N + N/C) | Member → score index; rank adds a binary search and a sum of preceding chunk lengths                   |
| ZPOPMIN / ZPOPMAX             | O(C)          | Remove from the first/last chunk                                                                                 |
| LINDEX / LSET                 | O(N/C)        | Walk chunk lengths from the nearer end (C ≤ 128)                                                                  |
| LRANGE                        | O(N/C + K)    | Locate the start chunk, then iterate packed elements                                                             |
| LREM / LPOS / LINSERT         | O(N)          | Linear scan; LREM/LINSERT rewrite only the touched chunks                                                         |
| LPUSH / RPUSH                 | O(1) amortized | Append to the tail chunk / write into the head chunk's reserved front space                                     |
| LPOP / RPOP                   | O(1)          | Advance the head index / truncate the tail; a single pop writes the reply straight to the output buffer           |
| SINTER                        | O(N × M)                                                                                                   | HashSet intersection (smallest-first)                                                                                             |
| SUNION / SDIFF                | O(N)                                                                                                       | HashSet union/difference                                                                                                          |
| GEOSEARCH                     | O(N)                                                                                                       | Full ZSet scan with haversine filter                                                                                              |
| JSON.SET (root)               | O(V)                                                                                                       | Dedicated fast-path RESP3 parser (`try_fast_json_set_key_val_at`) + atomic store                                                  |
| JSON.SET (path)               | O(D)                                                                                                       | D = path depth traversal                                                                                                          |
| JSON.GET                      | O(1)                                                                                                       | Direct path lookup                                                                                                                |
| XADD                          | O(1) amortized | Varint append coalesced into 4KB packed nodes via `WriteOp::Xadd`; `~` trims whole nodes                          |
| XRANGE / XREAD                | O(log N + K)  | BTreeMap seek to the node containing the start ID, decode forward                                                 |
| BITCOUNT                      | O(B)          | B = bytes; hardware `count_ones()` per byte; BITCOUNT byte-range is O(range)                                     |
| BITPOS                        | O(B)          | `leading_zeros()` per byte in byte mode; O(B) bit scan in bit mode                                               |
| BITFIELD                      | O(1)                                                                                                       | Single byte-range read/write with overflow check                                                                                  |
| PFADD / PFCOUNT               | O(1)                                                                                                       | HyperLogLog register update/estimate                                                                                              |
| GEOADD                        | O(log N + C) | 5-instruction Morton bit-interleaving (`spread26`) + ZSet chunked insert; C ≤ 256-entry chunk memmove             |
| GEODIST / GEOPOS              | O(1)          | ZSet score decode + haversine                                                                                    |
| GEOSEARCH                     | O(N)          | Full ZSet scan with haversine radius/box filter                                                                  |
| PUBLISH                       | O(S) sparse fan-out, O(W) dense                                                                           | W = workers; above ~8 subscribers/worker one queue entry per worker, delivery is a memcpy at the worker                         |
| PUBLISH pipelined run of K    | O(K·S) encode + O(W) enqueue                                                                              | One snapshot scan and one grouped fan-out for the whole run                                                                     |
| Pipelined GET / SET run of K  | O(K) prefetch + O(K) raw write, O(1) per op                                                               | Bulk prefetch batch (K ≤ 256) with redundant `prefetch_ahead` bypass; direct zero-copy write to TCP buffer                       |
| Same-key pipelined run of K   | O(K) work under 1 lock, O(1) amortized per command                                                        | Queue/write-run coalescing; per-command replies synthesized exactly                                                             |
| SET+EXPIRE adjacent pair      | O(1)                                                                                                       | Fused into one atomic set-with-TTL under a single entry lock                                                                    |
| SCAN / RANDOMKEY              | O(COUNT)      | Slot cursor; only key and expiry are read (values are never cloned)                                               |
| KEYS pattern                  | O(N)                                                                                                       | Full scan with per-slot EBR pin                                                                                                   |
| SORT                          | O(N log N)                                                                                                 | Vec collect + sort                                                                                                                |
| RDB save                      | O(N)          | Forkless snapshot walk, encoded without cloning; writers pay one pre-image encode per first touch                 |
| RESP parse                    | O(B)                                                                                                       | B = bytes, SIMD memchr for newlines                                                                                               |
| Command dispatch (fast-path)  | O(1)                                                                                                       | First-byte + `cmd_eq` for 13 hot commands (GET/SET/HGET/HSET/LPUSH/LPOP/LRANGE/RPUSH/RPOP/EXPIRE/ZADD/JSON.GET/JSON.SET)          |
| Command dispatch (all others) | O(1)                                                                                                       | Static `OnceLock<foldhash::HashMap>` — uppercase to 32-byte stack buf + one hash probe; built once, zero allocation per call      |
| Hash probe (avg)              | O(1)                                                                                                       | Open addressing at 75% load factor; a slot's hash tag rejects non-matches without dereferencing the entry                         |
| Hash probe (worst)            | O(N/S)                                                                                                     | N = keys in shard, linear probe                                                                                                   |
| Growth / Resize               | O(N/S)                                                                                                     | Per-shard, copies live pointers only                                                                                              |
| EBR collect                   | O(G)                                                                                                       | G = garbage list length                                                                                                           |
| Cluster slot routing          | O(1)                                                                                                       | Key CRC16 once, then one index into the shared 16384-entry owner table                                                            |
| Cluster topology refresh      | O(1)                                                                                                       | One `Acquire` load of a version counter; only a change pays for the lock and `Arc` clone                                          |
| Cluster write fence           | O(1) per write, O(W) to engage                                                                             | Per-worker in-flight counter; a fence holder drains W counters                                                                    |
| Active expiry                 | O(budget) per tick | 32K-slot chunks, repeated while >10% of sampled TTL keys expired, ≤25 ms per 100 ms tick                   |
| Blocking wake                 | O(W_k)        | W_k = waiters on the written key; one relaxed load when nobody is blocked                                         |
| XREADGROUP `>` / XACK         | O(log N + K) / O(log P) | Deliver after `last_delivered`, PEL is a BTreeMap plus per-consumer BTreeSet                          |
| FT.SEARCH KNN (HNSW)          | O(ef · log N) | Graph traversal on the pool; quantized distances, exact rerank of the candidates                                 |
| FT.SEARCH KNN (FLAT)          | O(N/2) avg    | Two-pass: Int8 pre-filter over all vectors, exact f32 rerank of the shortlist; batched across concurrent queries  |
| FT.SEARCH filtered KNN        | O(A) … O(ef · log N) | A = allowed ids; brute-force, ACORN-1 or in-traversal chosen by filter selectivity                          |
| FT.SEARCH text filter         | O(P + K)      | P = postings for the term; negations resolve as O(total − matched) sorted merge                                   |
| FT.AGGREGATE cursor           | O(page)       | Rows are split off the cursor Vec with split_off; no full-cursor clone per page                                   |
| Index maintenance per write   | O(1) + O(fields) at query | Write hook marks the key dirty; the next query applies exactly the dirty keys                      |
| Value defragmentation         | O(budget)                                                                                                  | Cursor walks one shard per tick and rebuilds at most `budget` values                                                              |
| `used_memory`                 | O(S)                                                                                                       | Sums S per-CPU allocation stripes                                                                                                 |

### Data Structures Used

| Type                   | Structure                                                                 | Why                                                                                                       |
| ---------------------- | ------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------- |
| Key→Value mapping      | Open-addressing hash table (linear probe)                                 | Cache-friendly, no pointer chasing                                                                        |
| Hash fields            | compact `Vec<SmallStr>` → `foldhash::HashMap<SmallStr, SmallStr>`         | Low overhead for small hashes, O(1) access after promotion                                                |
| List                   | Quicklist: `VecDeque` of packed chunks (bytes + `u32` offsets)            | ~4 bytes overhead per element, O(1) ends, cache-dense ranges                                              |
| Set                    | sorted integers → compact `Vec<SmallStr>` → `foldhash::HashSet<SmallStr>` | Representation follows member type and cardinality                                                        |
| Sorted Set             | Chunked sorted `Vec<ZEntry>` (≤256) + member → score hash index (>128)    | Cache-friendly ranges, O(1) score lookup, bounded memmove on insert                                        |
| Stream consumer groups | `HashMap<SmallStr, ConsumerGroup>`; PEL `BTreeMap<StreamId, PelEntry>`    | O(1) group/consumer lookup, ordered PEL for XPENDING / XAUTOCLAIM                                         |
| Command name → enum    | `OnceLock<foldhash::HashMap<&'static str, ComdType>>`                     | O(1) dispatch after one-time init                                                                         |
| JSON                   | Custom recursive enum (`JsonValue`)                                       | Zero-dependency, path traversal                                                                           |
| Stream                 | `BTreeMap<StreamId, Node>` of varint-packed nodes (≤100 entries / 4 KB)   | Ordered by ID, compact entries, tombstone deletes, whole-node trimming                                    |
| HyperLogLog            | 16384-register byte array                                                 | Fixed 16KB, probabilistic counting                                                                        |
| Geospatial             | ZSet with geohash-encoded scores                                          | Reuses sorted set, haversine filtering                                                                    |
| Pub/Sub channels       | Arc-snapshot Vec per shard                                                | Lock-free publish, copy-on-write subscribe                                                                |
| Worker-local subs      | `HashMap<String, Vec<usize>>` per worker                                  | Grouped fan-out resolves recipients without atomics; only the owning worker touches it                     |
| Subscriber queue       | `crossbeam::SegQueue`                                                     | Lock-free MPMC, bounded backpressure; also carries per-worker `FanEntry` frame batches                      |
| EBR garbage            | Thread-local `Vec<Garbage>`                                               | Batched collection every 512 retires                                                                      |
| Allocator accounting   | `rust-zmalloc` + mimalloc                                                 | Zero-overhead allocator; RSS tracked periodically via /proc, explicit page release via mi_collect         |
| Shard selection        | Bit shift + mask (foldhash)                                               | Single instruction, no modulo                                                                             |
| RESP newline scan      | `memchr` (SIMD AVX2)                                                      | 32 bytes per cycle                                                                                        |
| Vector graph           | Fixed-stride level-0 blocks (`u32` ids) + sparse upper layers             | Cache-line-aligned neighbour lists, parallel insert with per-node locks                                   |
| Vector codes           | INT8 per-vector scale or RaBitQ 1-bit residuals + ≤16 centroids           | 4–32× smaller than f32; exact rerank from the stored hash field                                           |
| Search dirty sets      | 16 sharded `HashSet` stripes per index                                    | Write hook stays cheap under concurrency; overflow falls back to a rebuild                                |
| Blocked clients        | `HashMap<key, Vec<Waiter>>` + per-worker deadline heap                    | Wake only the waiters of the written key; timeouts bound the poll                                         |
| AOF                    | Per-thread append files, sequence-numbered state records                  | No cross-thread contention; replay is a k-way merge                                                       |
