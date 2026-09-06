# FyroDB — Architecture

## Overview

FyroDB is a Redis-compatible in-memory key-value store written in Rust. It speaks the RESP wire protocol so any Redis client works out of the box. It uses a thread-per-core event loop architecture, a lock-free concurrent hash map with epoch-based reclamation and per-key seqlock, zero-copy RESP parsing, and direct-write response building.

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
| GET path            | Clone value + allocate        | Zero-copy: write directly from stored value to buffer                                        |
| Mutations           | Single-threaded (safe)        | In-place under per-key spinlock, seqlock for reader safety                                   |
| TTL computation     | clock_gettime per op          | Cached clock ticked once per event loop iteration                                            |
| Allocator           | libc malloc                   | mimalloc with zero-overhead hot path and periodic RSS tracking                               |
| Memory search       | Naive byte scan               | SIMD memchr (AVX2) for newline scanning                                                      |
| Write batching      | Per-command write             | Batched: all events read first, then flush all writes                                        |
| Pub/Sub fan-out     | Per-message frame copy        | Single Arc frame shared to all subscribers; dense fan-outs group per worker into one queue entry |
| Pipelined hot keys  | One lock per command          | Run coalescing: a run of ≤512 consecutive same-key commands takes one entry lock               |
| SET + EXPIRE pairs  | Two commands, two locks       | Adjacent pair fused into one atomic set-with-TTL under a single lock                          |
| Hash probe step     | Deref entry to compare key    | 15-bit hash tag packed into the slot pointer — reject a non-match without touching the entry |
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
  └── max_keys: usize                    (capacity limit)

Shard<V>  (cache-padded counters)
  ├── table: AtomicPtr<SlotTable<V>>     (swapped atomically on growth)
  ├── len: CachePadded<AtomicUsize>      (occupied slots)
  ├── insert_gate: CachePadded<AtomicUsize>  (concurrent insert counter + GROWING flag)
  └── grow_lock: Mutex<()>               (serializes growth/compaction per shard)

Entry<V>  (per key, heap-allocated)
  ├── hash: u64                          (full 64-bit hash, cached)
  ├── key: CompactKey                    (≤15 bytes inline)
  ├── state: AtomicU64                   (lock + occupied bit + seqlock generation)
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
4. Copy live Entry pointers (skip tombstones)
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

- `SmallStr` stores strings up to 23 bytes inline
- Small hashes and lists use compact sequential storage
- Sets use integer, compact-vector, or full hash-set representations and promote/demote with size
- Sorted sets use one score-ordered `Vec<ZEntry>` with a bloom filter for fast negative member lookups; exact (score, member) hits are binary-searched, and score-range operations use binary partition points
- Background maintenance can shrink collection capacity and rebuild fragmented values under the existing entry lock

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

## Request Lifecycle

```
Client → TCP (SO_REUSEPORT) → Per-thread epoll → Conn::do_read()
  → Zero-copy RESP parse → Inline fast path (SET/GET/INCR/DEL/LPUSH/RPOP/SADD)
  → Run coalescing look-ahead (same-key runs, SET+EXPIRE pairs, PUBLISH batches)
  → Or: dispatch table → Storage operation → Response to write buffer
  → Conn::do_write() → Client
```

All I/O is batched: read all ready events, then flush all responses in one pass.

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

## Persistence — RDB Snapshots

- Format: `FLDB` magic + version + typed entries + EOF marker
- Supports: String, Hash, List, Set, ZSet, JSON, Stream
- Atomic write: temp file → fsync → rename
- Per-slot EBR pin during save (no multi-second GC stalls)
- Expired keys skipped during load
- Truncation-safe: loads partial files with warning

---

## Source Layout

```
src/
├── main.rs              Entry point, config, signal handling
├── worker.rs            Per-thread epoll loop, batched I/O
├── handler/
│   ├── conn.rs          Connection state, inline fast paths, run coalescing (queue/write/publish runs, SET+EXPIRE pairs)
│   ├── dispatch.rs      First-byte fast-path + enum fallback
│   ├── subscription.rs  Pub/Sub state machine
│   └── pubsub_cmds.rs   PUBSUB subcommands
├── commends/
│   ├── mod.rs           Command enum + dispatcher
│   ├── string.rs        String commands
│   ├── hash.rs          Hash commands
│   ├── list.rs          List commands
│   ├── set.rs           Set commands
│   ├── zset.rs          Sorted Set commands
│   ├── json.rs          JSON commands
│   ├── stream.rs        Stream commands
│   ├── bitmap.rs        Bitmap commands
│   ├── hll.rs           HyperLogLog commands
│   ├── geo.rs           Geospatial commands
│   ├── keys.rs          Key management commands
│   ├── scan.rs          SCAN cursor implementation
│   ├── connection.rs    Server/connection commands
│   └── transaction.rs   MULTI/EXEC/DISCARD
├── storage/
│   ├── store.rs         Store struct (CustomMap + counters)
│   ├── value/           FyroDB values, SmallStr, compact collections, JSON, ZSetData
│   ├── string.rs        String storage operations
│   ├── hash.rs          Hash storage operations
│   ├── list.rs          List storage operations
│   ├── set.rs           Set storage operations
│   ├── zset.rs          Sorted Set storage operations
│   ├── json.rs          JSON storage operations
│   ├── stream.rs        Stream storage operations
│   ├── bitmap.rs        Bitmap storage operations
│   ├── hll.rs           HyperLogLog storage operations
│   ├── geo.rs           Geospatial storage operations
│   ├── keys.rs          Key operations (expire, rename, copy)
│   ├── scan.rs          Cursor-based scan
│   ├── server.rs        Info, flush, cleanup_expired
│   └── rdb.rs           RDB persistence
├── pubsub/
│   ├── registry.rs      Arc-snapshot pub/sub registry
│   ├── slot.rs          Per-subscriber queue, worker notifier, grouped FanEntry fan-out
│   └── frame.rs         RESP message encoding
└── utils/
    ├── parser.rs        Zero-copy RESP parser (SIMD memchr)
    ├── resp.rs          RESP response builders
    ├── resp3.rs         RESP3 protocol types
    └── util.rs          Glob matching, float formatting

crates/customhash/src/
├── lib.rs               Public CustomMap API
├── shard.rs             Entry/table layout, probing, growth
├── ops.rs               iteration, clear, compaction, defragmentation
├── key.rs               15-byte inline CompactKey
└── ebr.rs               Epoch-based reclamation

crates/rust-zmalloc/src/
└── lib.rs               mimalloc allocator, striped allocation counters, RSS, purge (mi_collect)
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

## Complexity Reference

| Operation                     | Time                                                                                                       | Mechanism                                                                                                                         |
| ----------------------------- | ---------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------- |
| GET / HGET / SISMEMBER        | O(1)                                                                                                       | Lock-free tag-filtered probe + atomic load; never takes the entry lock                                                            |
| SET / DEL / EXPIRE            | O(1) average                                                                                               | Tag-filtered probe + per-entry mutation/removal                                                                                   |
| INCR / LPUSH / SADD           | O(1)                                                                                                       | Per-key spinlock + in-place mutate                                                                                                |
| HGETALL / SMEMBERS            | O(N)                                                                                                       | Seqlock-validated iteration                                                                                                       |
| ZADD                          | O(1) amortized new member with ascending score; O(log N) same-score re-add; O(N) different-score update | 5-probe bloom filter (≈0.5% false positives) guards the scan; an exact (score, member) hit is binary-searched in the sorted Vec |
| ZRANGE                        | O(K)                                                                                                       | Contiguous sorted Vec slice                                                                                                       |
| ZRANGEBYSCORE                 | O(log N + K)                                                                                               | Binary partition points + slice iteration                                                                                         |
| ZRANK / ZSCORE                | O(N)                                                                                                       | Linear member lookup                                                                                                              |
| ZPOPMIN / ZPOPMAX             | O(N) / O(1)                                                                                                | Vec front removal / tail pop                                                                                                      |
| LINDEX                        | O(N)                                                                                                       | VecDeque index access                                                                                                             |
| LRANGE                        | O(N)                                                                                                       | Seqlock + VecDeque slice iteration                                                                                                |
| LREM / LPOS                   | O(N)                                                                                                       | Linear scan of VecDeque                                                                                                           |
| LPUSH / RPUSH                 | O(1)                                                                                                       | VecDeque push_front/push_back under spinlock                                                                                      |
| LPOP / RPOP                   | O(1)                                                                                                       | VecDeque pop under spinlock; a single-element pop writes the reply straight to the output buffer with no Vec or String allocation |
| SINTER                        | O(N × M)                                                                                                   | HashSet intersection (smallest-first)                                                                                             |
| SUNION / SDIFF                | O(N)                                                                                                       | HashSet union/difference                                                                                                          |
| GEOSEARCH                     | O(N)                                                                                                       | Full ZSet scan with haversine filter                                                                                              |
| JSON.SET (root)               | O(V)                                                                                                       | JSON parse + atomic store                                                                                                         |
| JSON.SET (path)               | O(D)                                                                                                       | D = path depth traversal                                                                                                          |
| JSON.GET                      | O(1)                                                                                                       | Direct path lookup                                                                                                                |
| XADD                          | O(1)                                                                                                       | BTreeMap append (auto-incrementing ID)                                                                                            |
| XRANGE                        | O(log N + K)                                                                                               | BTreeMap range query                                                                                                              |
| BITCOUNT                      | O(N)                                                                                                       | Byte-level popcount                                                                                                               |
| PFADD / PFCOUNT               | O(1)                                                                                                       | HyperLogLog register update/estimate                                                                                              |
| PUBLISH                       | O(S) sparse fan-out, O(W) dense                                                                           | W = workers; above ~8 subscribers/worker one queue entry per worker, delivery is a memcpy at the worker                         |
| PUBLISH pipelined run of K    | O(K·S) encode + O(W) enqueue                                                                              | One snapshot scan and one grouped fan-out for the whole run                                                                     |
| Same-key pipelined run of K   | O(K) work under 1 lock, O(1) amortized per command                                                        | Queue/write-run coalescing; per-command replies synthesized exactly                                                             |
| SET+EXPIRE adjacent pair      | O(1)                                                                                                       | Fused into one atomic set-with-TTL under a single entry lock                                                                    |
| SCAN                          | O(COUNT)                                                                                                   | Hash-based cursor, stable across mutations                                                                                        |
| KEYS pattern                  | O(N)                                                                                                       | Full scan with per-slot EBR pin                                                                                                   |
| SORT                          | O(N log N)                                                                                                 | Vec collect + sort                                                                                                                |
| RDB save                      | O(N)                                                                                                       | Per-slot iteration, buffered I/O                                                                                                  |
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
| Value defragmentation         | O(budget)                                                                                                  | Cursor walks one shard per tick and rebuilds at most `budget` values                                                              |
| `used_memory`                 | O(S)                                                                                                       | Sums S per-CPU allocation stripes                                                                                                 |

### Data Structures Used

| Type                   | Structure                                                                 | Why                                                                                                       |
| ---------------------- | ------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------- |
| Key→Value mapping      | Open-addressing hash table (linear probe)                                 | Cache-friendly, no pointer chasing                                                                        |
| Hash fields            | compact `Vec<SmallStr>` → `foldhash::HashMap<SmallStr, SmallStr>`         | Low overhead for small hashes, O(1) access after promotion                                                |
| List                   | compact/full `VecDeque<SmallStr>`                                         | Inline short values and O(1) push/pop both ends                                                           |
| Set                    | sorted integers → compact `Vec<SmallStr>` → `foldhash::HashSet<SmallStr>` | Representation follows member type and cardinality                                                        |
| Sorted Set             | score-ordered `Vec<ZEntry>` + bloom filter                                | Compact memory, O(1) append for ascending scores, O(log n) score ranges, bloom skips scan for new members |
| Stream consumer groups | `foldhash::HashMap<String, ConsumerGroup>`                                | O(1) group/consumer lookup                                                                                |
| Command name → enum    | `OnceLock<foldhash::HashMap<&'static str, ComdType>>`                     | O(1) dispatch after one-time init                                                                         |
| JSON                   | Custom recursive enum (`JsonValue`)                                       | Zero-dependency, path traversal                                                                           |
| Stream                 | `BTreeMap<StreamId, Vec<(String, String)>>`                               | Ordered by ID, O(log N) range                                                                             |
| HyperLogLog            | 16384-register byte array                                                 | Fixed 16KB, probabilistic counting                                                                        |
| Geospatial             | ZSet with geohash-encoded scores                                          | Reuses sorted set, haversine filtering                                                                    |
| Pub/Sub channels       | Arc-snapshot Vec per shard                                                | Lock-free publish, copy-on-write subscribe                                                                |
| Worker-local subs      | `HashMap<String, Vec<usize>>` per worker                                  | Grouped fan-out resolves recipients without atomics; only the owning worker touches it                     |
| Subscriber queue       | `crossbeam::SegQueue`                                                     | Lock-free MPMC, bounded backpressure; also carries per-worker `FanEntry` frame batches                      |
| EBR garbage            | Thread-local `Vec<Garbage>`                                               | Batched collection every 512 retires                                                                      |
| Allocator accounting   | `rust-zmalloc` + mimalloc                                                 | Zero-overhead allocator; RSS tracked periodically via /proc, explicit page release via mi_collect         |
| Shard selection        | Bit shift + mask (foldhash)                                               | Single instruction, no modulo                                                                             |
| RESP newline scan      | `memchr` (SIMD AVX2)                                                      | 32 bytes per cycle                                                                                        |
