# FyroDB — Complete Performance & Correctness Audit (2026-11-13)

Scope: full workspace audit of `src/` (~44.7k lines) and `crates/` (~22.3k lines) at checkpoint `f229095`.
Deep-read: `customhash` (map/shard/EBR/access/key/snap), `storage/value` (SmallStr/quicklist), `storage/store` (isolation/fence/watch/ttl/snapshot), handler hot path (`fast_path.rs`, `sock.rs`, `worker.rs`, `fast-path batching`), `parser.rs`, `aof.rs`, `rdb/save.rs`, `replication.rs` (log path), `rust-zmalloc`.
Surface-read: `pubsub/` internals, `search/` hooks, `cluster/transport`, `coalesce.rs`/`bulk.rs` internals, `fyro-vector`, `fyro-dbcache`.

Baseline (from BENCHMARKS.md, i5-11400H, 6 workers): **47.4M GET/s, 37.7M SET/s**; 12-worker shared: 55.9M/43.3M.

---

## 1. Executive summary — top findings (ranked)

| # | Severity | Type | Finding | Location |
|---|----------|------|---------|----------|
| 1 | **HIGH (correctness)** | Bug | Last-reader release in `EntryReadGuard::drop` can clobber a reader that joined under the lock → torn reads / UAF on hot keys under mixed read+write | `crates/customhash/src/shard.rs:74-90` + `shard.rs:147-168` |
| 2 | **HIGH (replication)** | Bug | `record_current_value` reads the value *after* the entry lock is released; two concurrent SETs on the same key can log in the inverse order of final state → replica divergence | `src/storage/store/cluster.rs:56-80` |
| 3 | **HIGH (perf, replication/AOF on)** | Lock | Every replicated/AOF write takes a **global** `Mutex<MutationLog>` + journal mutex + full value re-encode + a second map lookup | `src/cluster/replication.rs:349-364`, `cluster.rs:45-80` |
| 4 | MEDIUM (durability) | Bug | AOF records have no checksum; mid-file corruption silently truncates the rest of the file; write/fsync errors swallowed after first log line | `src/storage/persistence/aof.rs:106-141`, `300-315` |
| 5 | MEDIUM (perf, multicore SET) | Lock | Map write-admission counters (`Access.readers/writers`) are **per-map global** cache lines — every write batch SeqCst-RMWs the same two lines on all workers | `crates/customhash/src/access.rs:18-46` |
| 6 | MEDIUM (perf, AOF) | Atomic | Global `seq.fetch_add` = one contended RMW per recorded write across all workers | `aof.rs:319-328` |
| 7 | LOW (perf, scripts) | Lock | `script_cache` RwLock read per EVALSHA; a thread-local epoch check (epoch already exists) makes steady-state zero-atomic | `src/storage/store/mod.rs:133-161` |
| 8 | LOW (perf) | Spin | `enter_insert` spins without yield while GROWING; big rehashes burn worker CPU | `shard.rs:631-649` |
| 9 | LOW (perf) | Drift | TTL counter over-count on in-place overwrite paths (self-repaired by scans, costs scan cycles) | `fast_path.rs:542`, `storage/string.rs:437` |
| 10 | LOW (memory) | Buffer | Per-conn retained rbuf/wbuf (32 KiB class) → hundreds of MiB at 10k idle connections | `parser.rs:97`, `sock.rs:212-218` |

---

## 2. What is already right — do not regress these

The codebase is unusually well optimized already. These are the load-bearing wins:

- **Zero-copy parse & reply**: args borrowed from `rbuf`, GET serializes straight into `wbuf` (`storage/string.rs:263-302`); no intermediate `String` per op.
- **Single-u64 command classification** (`fast_path.rs:122-165`): one 8-byte case-folded word match per command; better than Redis's per-gateway strcmp chains.
- **Fast-path batch engines**: 256-entry GET/SET runs with stack arrays, 8-wide unrolled hashing/prefetch (`customhash/src/lib.rs:272-591`), tiled SET prefetch (`fast_path.rs:417-641`).
- **Hash-tag in slot pointer** (14-bit tag, `shard.rs:309-321`) + 16-slot unrolled probe = probe cost ~1–2 Acquire loads on hit.
- **Entry state packing**: lock/reader/seq/hash-tag/occupied in one `AtomicU64` (56-byte entries); read-guard join protocol with exponential backoff + yield (`shard.rs:137-171`, `236-263`).
- **Read-epoch batches** (`access.rs`): ≥32 pure-read GET runs skip per-entry RMWs entirely.
- **Snapshot parity scheme** (`snap.rs`): RDB save without write freeze — writers capture pre-images only while a save is active; one relaxed load when idle (`CustomMap::snap_before_write` → `snap.rs:33-38`).
- **Allocator**: mimalloc with thread-local delta tracking flushed at 256 KiB into 64 padded stripes (`rust-zmalloc/src/lib.rs:31-76`) — near-zero tracking cost.
- **Allocator discipline on writes**: same-length overwrites reuse SmallStr payload; ≥8 KiB AVX non-temporal copy with sfence (`small_str.rs:61-101`).
- **Pub/Sub**: lock-free publish via `ArcSwap` channel snapshots, frames encoded once as `Arc<[u8]>`, per-worker fanout queues with bounded backlog (`registry.rs:82-137`, `worker.rs:416-544`).
- **Fence paid only when cluster is enabled** (`fast_path.rs:1474-1491`).
- **Write gate (insert_gate) skipped on pure overwrites** (`shard.rs` test at 1538-1556) — hot-key writes don't serialize on the shard gate.

---

## 3. Lock & mutex audit — complete inventory and reduction plan

### 3.1 Full inventory

| Lock | Location | Protects | Contention | Verdict |
|------|----------|----------|------------|---------|
| `Shard.grow_lock: std Mutex` | `shard.rs:579` | growth/rehash serialization per shard | low (75% threshold) | keep; swap to `parking_lot` (no poison machinery, faster) |
| `insert_gate` atomic | `shard.rs:578` | in-flight inserts during grow | none on overwrite path | keep (correct design) |
| `Access.readers/writers` SeqCst counters | `access.rs:18-46` | map read-batch vs writer admission | **global line — all workers** | *stripable* (see 3.2-A) |
| Entry state CAS | `shard.rs:214-300` | per-key value mutation | per-key only | keep — the correctness core |
| `SnapCtl.sink RwLock` | `snap.rs:11` | snapshot sink swap | only during SAVE | keep |
| `Isolation` per-worker RwLocks + writer Mutex | `isolation.rs:16-77` | command isolation / atomic commands | 1 shared RMW per read batch; exclusive stalls all for EVAL/EXEC | keep; document the EVAL cliff |
| `WriteFence` SeqCst counters | `fence.rs:5-106` | cluster migration drain | cluster mode only | keep (Dekker handshake is correct) |
| `script_cache parking_lot RwLock` | `store/mod.rs:45` | script table | **1 RMW per EVALSHA across workers** | *replaceable* (see 3.2-B) |
| `WatchRegistry.keys RwLock` | `watch.rs:11` | WATCH version map | 1 read RMW per write **only while watchers exist** | acceptable; ArcSwap upgrade optional |
| `Aof.writers Mutex` + per-writer Mutex | `aof.rs:48,269-285` | writer registry; per-thread buffer | uncontended after first use | keep |
| `Aof.seq AtomicU64 AcqRel` | `aof.rs:319` | record order | **global RMW per write with AOF on** | *batchable* (see 3.2-C) |
| `ReplicationCoordinator.log Mutex` | `replication.rs:304,349-364` | mutation log append | **global mutex per write when replicating** | *replaceable* (see 3.2-D) |
| `ReplicationCoordinator.journal Mutex` | `replication.rs:307,354` | journal writes | same hot section | merge into log lock redesign |
| `BlockRegistry.keys Mutex` | `blocking.rs:22` | blocked waiters map | low unless BLPOP-heavy | sharding optional |
| `ChannelShard.mu` (64 shards) | `registry.rs:168-181` | subscribe/unsubscribe | not on publish path | keep — publish is lock-free |
| `WorkerNotifier.local_subs std Mutex` | `slot.rs:87` | local subscription table | effectively thread-local | swap to `parking_lot` |
| `SearchState` 7 mutexes | `search/mod.rs:93-111` | index registry/cursors/filters | only FT workloads; `indexes.read()` per write when hooks active | ArcSwap for `indexes` when FT used |
| `SAVE_LOCK`, `AOF_MARK` | `rdb/save.rs:61-62` | save serialization | rare | keep |
| `Store.replica_meta_path` etc. (3× `Mutex<Option<String>>`) | `store/mod.rs:40-42` | cold config | none | keep |
| `parking_lot RwLock` in `store/mod.rs:45` — see script_cache | — | — | — | — |

### 3.2 The reductions worth doing (ranked, with risk)

**A. Stripe the map write-admission counters** (`access.rs`) — *medium risk, medium reward*
`try_read`/`write` RMW two fixed cache lines per map. In a 6-worker SET pipeline, each batch pays ≥3 SeqCst locked ops on the same two lines (they will bounce between cores). Replace the single `readers.count`/`writers` pair with a **sharded array indexed by `sched_getcpu() % N`** (the pattern `TtlCounters` already uses, `ttl.rs:25-52`), where the handshake is performed per-stripe: a writer must drain *all* stripes' readers; a reader only registers on its own stripe. Uncontended cost is unchanged (one stripe RMW pair); contended cost drops to near zero cross-core traffic.
- Risk: the handshake is the correctness core (`ARCHITECTURE.md:44-50`). It needs the same double-check discipline per stripe and the thread-local `READING/WRITING` nesting keys must remain map-global. Loom tests exist for the current design — extend them.
- Expected: +3–6% on multicore SET pipelines; GET unchanged (read-epoch already optional).

**B. Script cache: epoch-checked thread-local snapshot** — *low risk, small reward*
`with_script` (`store/mod.rs:146-151`) takes a shared RwLock on every EVAL/EVALSHA. `script_epoch` already exists (`mod.rs:46`). Cache `(epoch, Arc<str>)` in a thread-local; on epoch match, zero atomics; on mismatch, one read-lock. Expected: +2–5% on multicore script-heavy workloads, −1 contended RMW per script call.

**C. AOF sequence ranges** — *low risk, small reward with AOF on*
`aof.record` (`aof.rs:319-328`) does a global AcqRel `fetch_add` per write. Allocate ranges: `base = seq.fetch_add(64)`, hand out local counter, refill when exhausted. Replay merges by seq across files (`aof.rs:163-187`), so non-contiguous per-writer windows are safe. Expected: +3–8% SET throughput with AOF enabled at ≥4 workers.

**D. Replication: single-pass capture + striped append** — *medium risk, big reward when replication on* (this is finding #3)
Today every replicated write pays: (1) `record_current_value` → a **second full map lookup** (`get_ref`), (2) `encode_single_value` → full value serialization into a fresh `Vec` (alloc + full copy), (3) `key.as_bytes().to_vec()` (another alloc), (4) global log `Mutex` lock, (5) journal `Mutex` lock. That is roughly **2× the value size in extra copies + 2 allocations + 2 global locks per write**.
Fix in two parts:
1. *Single-pass capture*: encode the value **inside the entry write-lock section** (you already hold it in `dispatch_set_batch`'s in-place path and `set_string_fast_entry`). Encode directly into the per-writer AOF/replication buffer (`Writer.buf`) with a small reusable scratch — no intermediate `Vec`, no second lookup. This also fixes finding #2 (log order = lock order = final-state order).
2. *Striped log*: replace `Mutex<MutationLog>` with per-worker append shards holding reserved offset ranges (same trick as C); `replay_from` merges shards by offset exactly like AOF's `merged()`.
- Expected: **+20–40% SET throughput when replication or AOF is enabled** (currently those configs roughly double the write cost); latency tail at p99 −2–5 µs on write bursts.
- Risk: medium — the offset/`next_offset` accounting and `replay_from` semantics must be preserved exactly; the existing replication regressions (`store/mod.rs:423-493`) cover the accounting.

**E. Small polish** (do together, low risk): `grow_lock` → `parking_lot`; add `yield_now()` to the GROWING spin (`shard.rs:645`); `local_subs` → `parking_lot`.

### 3.3 Locks that must NOT be removed

- **Entry write lock** — it is the only thing preventing concurrent in-place `SmallStr` mutation; the architecture doc is right that a seq recheck is not a substitute (`ARCHITECTURE.md:50`).
- **Isolation exclusive admission for EVAL/EXEC** — required for script atomicity semantics.
- **Write fence SeqCst ordering** — the Dekker handshake comment (`fence.rs:53-57`) is correct; weakening it re-opens the migration race.
- **Pub/Sub ArcSwap mutexes** — only on subscribe/unsubscribe; publish path is already lock-free.

---

## 4. Per-payload hot-path accounting (byte-level)

### 4.1 Small values (≤64 B) — GET

Per pipelined GET, total cost: **~2 locked RMWs + 3-4 atomic loads + 1 memcpy (~25 B) + amortized syscalls**.

| Step | Cost | Where |
|------|------|-------|
| recv (amortized over pipeline) | 1 syscall / ~8-32 KiB | `sock.rs:23-47` |
| parse `*2\r\n$3\r\nget\r\n...` | 3 unaligned loads + 1 u64 case-fold compare | `parser.rs:171-208` |
| slot probe | 1 Acquire load (hit at slot 0) | `shard.rs:748-770` |
| entry read guard | **1 CAS Acquire + 1 fetch_sub AcqRel** | `shard.rs:161-168, 74-90` |
| expiry check | 1 Relaxed load + compare | `string.rs:281` |
| reply serialize | 1 memcpy ≤ 32 B into wbuf | `SmallStr::write_resp` |
| command_guard | 1 shared RMW per *batch* (amortized) | `isolation.rs:67` |
| EBR pin | 1 per batch (already hoisted, `sock.rs:77`) | `ebr.rs:26-30` |
| send | 1 syscall per flush | `sock.rs:239-260` |

Optimizations for this class:
1. **Widen read-epoch batching** (`fast_path.rs:6` — `READ_EPOCH_MIN = 32`): pipelines of 8–31 pure GETs still pay per-entry RMWs. Lowering to 8 (with the existing fallback on epoch failure) saves 2 locked ops × n. Expected +2–5% small-GET on pipelines 8–31; zero effect ≥32 (already epoch-backed).
2. The remaining per-entry CAS is the correctness floor — do not remove it outside an admitted read epoch.

### 4.2 Small values — SET (overwrite, in-place path)

Per SET: **1 CAS + 1 Release store** (entry) + 1 Relaxed load each for snap/observing; batch-level: 3 SeqCst RMWs (write epoch) + 1 EBR pin.

- Reply is a precomputed `OK_BLOCK` 5-byte memcpy (`fast_path.rs:8-20, 625-629`) — already optimal.
- Value copy: parser buffer → SmallStr inline (≤23 B: single ≤2×8-byte unaligned copies, `small_str.rs:190-244`) — already optimal, zero allocations.
- Improvements available: §3.2-A (striped admission), plus **fold `touched_bytes` and `snap_before_write`'s two relaxed loads into the entry-state word** (bit 1 of `Flag` / `STATE_SNAP`-like bit) to save 1 cache-line touch per write — worth ~1-2% only; low priority.

### 4.3 Medium values (1–64 KiB)

- SET same-length overwrite: single `copy_from_slice` (≤256 B has a specialized head/tail version, `small_str.rs:48-59`) or AVX NT ≥8 KiB. One copy total. Good.
- SET growth/change: alloc + copy + free of old. Unavoidable without slabs; mimalloc size-class reuse already amortizes.
- GET: value → `wbuf` memcpy, then kernel copy on send. Two copies minimum in the current design.
  - **Pre-reserve `wbuf` from the first payload length** (`fast_path.rs:305-331` already computes `first_payload_len`): today `reserve(count*64)` underestimates, and deep 1–4 KiB pipelines realloc (Vec doubles → 1 extra full-buffer copy per realloc). Sum the scanned payload lengths (they're already parsed as `vals[i].1` / key lengths during batch scan) and reserve once. Expected: +3–8% on 1–4 KiB deep pipelines, −1 transient multi-MiB copy per realloc.
- The streaming path (`bulk.rs::try_stream_get_run`, ≥4 KiB first payload single-worker / `MIN_STREAM_BYTES` multicore) already bypasses `wbuf` for large runs — keep its thresholds.

### 4.4 Large values (≥64 KiB–1 MiB)

- SET overwrite ≥8 KiB same-len: AVX `_mm256_stream_si256` with sfence (`small_str.rs:73-101`) — correct and appropriately gated (only when unique hash in batch, `fast_path.rs:543-551`). Extension: apply the same streaming copy on the **insert path** (first write of a large value currently does a plain alloc+copy): +5–15% on ≥64 KiB first-writes.
- GET: for ≥64 KiB the remaining cost is the wbuf copy + kernel copy. Options ranked:
  1. Pre-reserve (same as 4.3) — trivial, do it.
  2. `MSG_ZEROCOPY` for replies ≥64 KiB (register per-conn buffer rings; completion via error queue) — up to +20–40% when NIC-bound on real networks; marginal on loopback; **high complexity** — optional.
  3. io-uring `send` ring already exists (`handler/uring.rs`, `worker.rs:266-321`) — keep off-path unless measured; the docs record mixed results.
- Repeated-GET single-worker path builds `count × payload` in `wbuf` (`fast_path.rs:196-215`): 100 × 1 MiB = 100 MiB transient. Bound it: cap the buffered-repeat path by a wbuf ceiling (e.g. 16 MiB) and fall back to per-iteration streaming. Low effort, removes a pathological memory spike.

### 4.5 Atomic-op totals per op (current → after §3.2)

| Op | Now (locked ops) | After audit changes |
|----|------------------|---------------------|
| GET small, pipeline ≥32 (epoch) | ~2 SeqCst (epoch, per batch) + probe loads | unchanged |
| GET small, pipeline 8–31 | 2 RMW/entry | ~0.2/entry (epoch) |
| SET small (no persistence) | 2 RMW entry + 3 SeqCst epoch/batch | same entry; epoch striped → cross-core traffic ~0 |
| SET small + AOF | + 1 AcqRel seq RMW + encode/alloc | + ~0.02 (range alloc) + no encode alloc |
| SET small + replication | + 2 global mutex + encode + 2nd lookup | + 1 striped-shard push, no lookup, no extra alloc |
| EVALSHA | + 1 RwLock read RMW | 0 (steady state) |

---

## 5. Hardware-level notes

- **Entry layout (56 B)**: state at offset 16, value at 24. Consecutive entries straddle cache lines, which *is* good for sequential scans (no padding waste) but means one entry can touch 2 lines. An aligned-64 layout trades +14% memory (64 B/entry) for guaranteed single-line small entries. **Experiment only** — docs already note the entry is not line-aligned (`ARCHITECTURE.md:36`); do not change without the `without_tcp` + TCP harness.
- **Probe scan**: 16 sequential 8-byte Acquire loads. 8 slots share a 64-B line, so line-0 hit costs 1 line. A single 64-B AVX load could test 8 tags at once — helps only probe-miss-heavy workloads (≤2% overall). Low priority.
- **Prefetch**: already excellent — T0/ET0 hints on both read and write paths, 8-wide unrolled slot+entry prefetch, rebuild-time 16-ahead prefetch (`shard.rs:1436-1454`), payload prefetch windows 24–1024 B (`fast_path.rs:305-340`).
- **SIMD**: command classify (u64), key compare (u64/u32 word XOR, `key.rs:131-201`), SmallStr copy (word copies + AVX NT). Remaining SIMD candidates: generic RESP bulk `\r\n` scan via `memchr` (dependency present) for >4 KiB bulks in the *generic* parser path; INCR digit parsing is already branch-light.
- **Syscalls**: 1 recv + 1 send per flush cycle; MAX_READ_AHEAD 64 KiB bounds buffer growth (`sock.rs:6`); `TCP_NODELAY`+`QUICKACK` set on accept (`worker.rs:134-138`). The only remaining syscall lever is io-uring (measured mixed — keep optional) and `MSG_ZEROCOPY` (§4.4).
- **NUMA/affinity**: `affinity.rs` + SO_REUSEPORT per worker + `sched_getcpu`-striped TTL counters. Reuse the same `getcpu` pattern for §3.2-A/C/D stripes.
- **Branch layout**: `cmd_id` single match, `probe_from_base` unrolled with early `null` exit, OK_BLOCK const — all fine. `find_fast` bounds-check fallback (`base_idx + 16 > mask + 1`) is correctly `#[cold]`.

---

## 6. Bugs & data-corruption risks (ranked, with scenarios)

### 6.1 HIGH — reader-join clobber in `EntryReadGuard::drop`
**File**: `crates/customhash/src/shard.rs:74-90` (drop) interacting with `Entry::read` join-under-lock (`shard.rs:147-168`).

`read()` intentionally lets a new reader join while `STATE_LOCK` is set **if readers > 0** (line 147: spin only when `LOCK && readers == 0`). The last reader's drop does:

```
previous = fetch_sub(READER_ONE)        // (a)
if previous & READER_MASK == READER_ONE:
    store((previous - READER_ONE) & !LOCK)   // (b)
```

**Scenario**: readers R1,R2 hold; R2 is last. Between R2's step (a) and (b), a new reader R3 CAS-joins from `(LOCK|readers=1)` → `readers=2`. R2's store (b) then writes `readers=0, LOCK=0` — **destroying R3's count**. A writer spinning in `state_lock_slow` sees `LOCK=0`, acquires, and mutates the value **while R3 still dereferences it**: torn read on same-len overwrite; use-after-free when the mutation reallocs the SmallStr payload. R3's own later drop underflows the reader field, compounding the corruption.
Probability per op is tiny (a ~2-instruction window), but at 37M+ ops/s with hot keys + mixed traffic it is reachable over hours of load — and the impact is memory corruption, not just a wrong reply. The invariant comment at `shard.rs:159-160` ("LOCK excludes new readers until this store") is violated by the join rule.
**Fix (pick one)**:
1. Make the last-reader release a CAS loop: expected `(LOCK|0 readers)`, on mismatch re-read and if `readers != 0` (a joiner landed) re-run the decrement logic — ~1 extra branch on the common path (last reader with no joiner is still a single CAS).
2. Or forbid join-while-locked (spin in `read()` whenever `LOCK` is set) — simpler, loses reader-herd coalescing; measure hot-key read cost before choosing.
Either way, add a loom test for `2 readers + 1 late joiner + 1 waiting writer`.

### 6.2 WITHDRAWN on implementation review — replication capture is order-safe
**File**: `src/storage/store/cluster.rs:56-80` (`record_current_value`).

The original finding claimed two concurrent same-key SETs could append in the inverse order of final state. On re-verification before implementing the fix, this is **wrong**: `record_current_value` takes the value via `get_ref`, and the resulting `EntryReadGuard` lives **through the encode and the log append** — a concurrent writer's mutation cannot proceed (the entry write lock requires `STATE_LOCK` clear, and readers hold it) until the append completes. Every record therefore contains the key's state at its capture point, captures for the same key are serialized by that entry's lock, and any later mutation produces a later record — so the last log record always equals the primary's final value and replay converges. The AOF path has the same property (`aof_capture`'s guard spans the encode).
The real residual risk for these captures is **6.1** (a torn read during `encode_single_value` of a collection if the reader clobber were hit) — fixed separately below. No capture-order refactor is needed; §3.2-D's remaining value is purely the perf items (single lookup/encode shared by AOF + log, striped log).

### 6.3 MEDIUM — AOF: no checksums, silent truncation, swallowed errors
**File**: `src/storage/persistence/aof.rs`
- `read_record` (`106-141`) returns `Ok(None)` on any malformed record — a mid-file corruption (bit rot, partial sector) silently **drops every record after it** with no error, no log, no checksum to detect it. Torn tail is fine to ignore; undetected mid-file loss is not.
- `write_out` errors: first failure prints one line and sets `failed`; writes keep flowing (records after a failed open are dropped on the floor at `299-305` — the buffer is not retained for retry), and `sync_data` errors are discarded (`315, 353`). INFO does not surface `failed`.
**Fix**: add a CRC32 (4 B) per record — cost ~1 GB/s+ with `crc32fast`, negligible vs 64 KiB buffering; on mid-file checksum failure, **fail loudly** (refuse startup unless `--skip-corrupt-tail`), truncate only at the *last valid record*; plumb `failed` into INFO and consider aborting AOF on write error (like Redis `stop-writes-on-bgsave-error`).

### 6.4 LOW — TTL counter over-count on in-place overwrites
`fast_path.rs:542` (`val.expires_ms = 0`) and `storage/string.rs:437-449` mutate TTL state without `sub_ttl/add_ttl` pairing. Direction of drift is **over-count** (safe: triggers extra expiry scans, repaired by `finish_ttl_scan`, `ttl.rs:121-128`). No under-count path found — no lingering-key leak. Optional fix: compare old vs new `expires_ms` under the lock and adjust the counter; saves scan cycles on TTL-heavy write workloads.

### 6.5 LOW — GROWING spin without yield
`enter_insert` (`shard.rs:631-649`) and the grow-side drain (`shard.rs:1386`) use pure `spin_loop`. A large rehash (e.g. 8K→64K slots, ~24K entries) can hold writers spinning across cores for tens of µs. Add exponential backoff + `yield_now()` like `state_lock_slow`. Cheap, removes worst-case CPU burn.

### 6.6 INFO (no action in release, note for dev)
- `update`/`update_with_hinted` (`lib.rs:1151-1233`) hold the entry lock across user closures; a panic inside leaves the entry locked forever. Release profile is `panic=abort`, so unreachable there; dev/debug servers would wedge one key. A drop-guard costs nothing.
- The EverySec AOF thread never terminates (process-lifetime thread; fine).
- `dispatch_set_batch` duplicate-hash scan for streaming eligibility is O(n) per large SET (`fast_path.rs:548-549`) — trivial CPU, correct.
- Single-worker repeated-GET builds `count × payload` in `wbuf` (§4.4) — bounded by pipeline depth; cap it.

### 6.7 Verified-clean areas (checked, no issue found)
Tagged-slot encode/decode reserved-bit discipline (impossible high-bit addresses on x86-64 user space, and untagged fallback preserved); `set_key_with_overwrite` raw-entry teardown without double-drop (`shard.rs:1115-1128`); claim-failure entry disposal (`949-954, 1072`); `rebuild_to` EBR retirement of old tables and tombstone entries; growth double-check after `enter_insert`; `probe_or_insert` vacant-slot CAS; SmallStr overlapping head/tail copy regions (redundant writes only, values identical); `parse_len_le4` bounds; fast-GET/SET header masks; `write_fence` unknown-worker fallback (`fence.rs:63-70`); CompactKey heap/full-width/inline tag rules incl. the 16-byte `tag > INLINE_CAP` case (`key.rs:12-14`, tested exhaustively); allocator stripe accounting (TLS delta + 256 KiB flush); RDB tmp+rename+dir-fsync; snapshot parity point-in-time consistency (has a dedicated concurrency test).

---

## 7. Replacement candidates (structure-level)

| Current | Candidate | Gain | Risk | Verdict |
|---------|-----------|------|------|---------|
| Replication `Mutex<MutationLog>` + post-hoc lookup + encode | In-lock capture into striped per-worker log shards | +20–40% replicated SET | medium | **do** |
| `script_cache` RwLock | epoch-checked TLS snapshot | +2–5% scripts | low | **do** |
| `Access` global counters | getcpu-striped handshake | +3–6% multicore SET | medium | **do with loom coverage** |
| crossbeam-epoch EBR | custom lighter EBR | pin is already hoisted per batch; custom EBR risks subtle reclamation bugs for <1–2% | high | **keep crossbeam** |
| foldhash | xxh3/wyhash | hash is not a measured bottleneck (foldhash is already ~1.5 ns short-key) | low reward | keep |
| `wbuf` Vec | pre-reserved single alloc per batch | +3–8% medium GET | trivial | **do** |
| ≥64 KiB reply path | MSG_ZEROCOPY ring | up to +20–40% NIC-bound large GET | high complexity | optional, measure on real NIC |
| AOF seq counter | range allocation | +3–8% AOF-on SET | low | **do** |
| Entry 56 B → 64 B aligned | 1-entry-per-line | ambiguous (memory +14%, scan locality mixed) | medium | experiment only |
| `BlockRegistry` single Mutex | key-hash sharding | only BLPOP-heavy | low | optional |

---

## 8. Persistence & durability posture (summary)

- **RDB save**: tmp+rename+dir-fsync + parity-bit pre-image capture — consistent point-in-time, no write freeze, AOF rotation tied to save boundary (`rdb/save.rs:85-105`). Solid.
- **AOF**: fsync policies Always/EverySec/No implemented; EverySec thread syncs all writers 1×/s (`aof.rs:253-264`); torn-tail tolerated; **weaknesses are 6.3 (checksums/errors)** and unbounded `writers` Vec growth if threads churn (bounded in practice by worker count).
- **Replication**: offsets assigned under the log lock (no gaps/dupes); `replay_from` under lock; install flag gates writes (`fast_path.rs:1493-1499`); fence is SeqCst-correct. **Weakness: 6.2 ordering.**
- **Kill -9 windows**: SET of any size — lost only if AOF buffer unflushed (EverySec: ≤1 s; Always: none). RDB save — tmp file discarded, previous RDB intact. AOF rewrite/compact — carry file written+fsynced before old files removed (`aof.rs:363-403`), base file written via tmp+rename. Map-vs-log disagreement: bounded to the in-flight write; no partial-record windows (single `write_all` per 64 KiB buffer; partial buffer loss on crash is a torn tail → handled).
- **Operational**: no maxmemory eviction policy (denyoom only, `capacity.rs`); AOF `failed` flag invisible to INFO (6.3).

---

## 9. Multicore deep-dive — every cross-core shared line, per workload

This section maps **all cross-core shared state** touched per operation, explains the benchmark lines that scale sub-linearly or *negatively* (6-worker SMT vs 3-worker), and ranks the fixes. Scaling references are from BENCHMARKS.md (server: 1/2/3 physical cores; the "6/SMT" column adds SMT threads on the same 3 cores, so 1→2→3 is the physical-core scaling signal and 3→6/SMT is the SMT signal).

### 9.1 Observed scaling summary

| Workload | 1w | 2w | 3w | 6/SMT | 3w→6w | Read |
|----------|----|----|----|-------|-------|------|
| Pipelined GET | 20.1 | 33.0 | 44.6 | 47.4 | 1.06× | SMT adds ~nothing (client+server share physical cores) |
| Pipelined SET | 17.4 | 27.6 | 32.3 | 37.7 | 1.17× | 2→3w only 1.17× — weakest physical-core scaling; see M-1 |
| INCR | 14.0 | 27.3 | 30.4 | 45.9 | 1.51× | 2→3w stall (1.96→1.11×) — scalar-write pattern, see M-1 |
| Hot Key (1 key GET) | 39.3 | 67.4 | 107.0 | 118.6 | 1.11× | **scales well**: repeated-run detection collapses 100 GETs into 1 entry guard (`fast_path.rs:273-275, 196-215`) |
| LPUSH/RPOP (queue) | 9.9 | 11.8 | 21.4 | 30.2 | 1.41× | per-client keys — scales |
| Producer/Consumer 50+50 | 9.0 | 15.4 | 16.4 | **3.27** | **0.20×** | shared-queue key handoff, see M-4 |
| ZADD | 5.0 | 8.8 | 11.5 | **8.4** | **0.73×** | heavy critical section, see M-4 |
| Pub/Sub publish | 1.5 | 2.4 | 2.8 | **2.4** | **0.83×** | producer/owner RMW collision, see M-3 |
| SADD / HSET / PFADD | — | — | — | ≥1.0× | ok | per-client keys, expected |

### 9.2 Complete cross-core state inventory (per op, uncontended cost)

| Shared line(s) | Touch | GET (batch≥32) | GET (scalar/small batch) | SET (pipeline batch) | SET (scalar/interleaved) | Notes |
|---|---|---|---|---|---|---|
| `Access.writers` + `Access.readers.count` (`access.rs:18-46`) | SeqCst RMW ×2 + load (acquire), RMW ×1 (release) | 2 RMW/batch (try_read_epoch) | none (entry guards) | 3 RMW/**batch** (once per dispatch_set_batch) | **3 RMW/op** | the only per-op global RMWs on the KV path |
| Entry `state` | CAS+store (write) / CAS+sub (read guard) | ~0 (epoch) | 2 RMW/key | 2 RMW/key | 2 RMW/key | per-key line — spreads across shards unless hot key |
| EBR global epoch (`crossbeam_epoch`) | shared load per pin | 1/batch | 1/batch | 2/batch | 2/op path | read-shared = no bounce; RMW only on epoch advance during collect |
| `snap_active` + `observing` Flag (`snap.rs:9`, `lib.rs:10-12`) | Relaxed shared load ×2 | — | — | 2 loads/op | 2 loads/op | read-shared, no bounce — fine |
| `UNIX_MS_CACHE` (`value/mod.rs:208-210`) | **Relaxed store every poll iteration** | per loop | per loop | per loop | per loop | **every worker writes the same line** — see M-2 |
| `Shard.len`/`live`/`insert_gate` (CachePadded) | RMW only on insert/grow | — | — | only new keys | only new keys | overwrite path skips them (verified by test `shard.rs:1538`) |
| `fanout_bytes` + SegQueue tails (`slot.rs:83,92`) | RMW producer + **fetch_sub per drained entry** | — | — | — | — | PUBLISH path — see M-3 |
| `wake_pending`/`fanout_pending` | swap per notify (arm) | — | — | — | — | suppresses eventfd writes correctly |
| Isolation slot (per worker) | 2 RMW/batch | ✓ | ✓ | ✓ | ✓ | own line per worker — no cross-core traffic; only EVAL/EXEC exclusive touches all slots |
| TtlCounters | getcpu-striped RMW on TTL ops | — | — | — | — | already striped correctly (`ttl.rs:25-52`) |

**The map is clean apart from four lines**: the two `Access` epoch counters, `UNIX_MS_CACHE`, and `fanout_bytes`. Everything else is per-worker, per-shard, per-key, or read-shared. That is a good place to be; the four findings below are the whole remaining multicore story, plus two workload-level (M-4, M-5) and two operational (M-7, M-8).

### 9.3 Findings (ranked)

**M-1 · The `Access` epoch lines are the only per-op global RMWs — striping value depends on client composition** (refines §3.2-A)

- Pipeline-batched clients (the published Go suite: `buildBatchFastSet` then `buildBatchFastGet`, `bench/realworld.go:184-216`): one epoch per ~50 SETs → 3 RMWs/50 ops → **not a bottleneck**; striping gains ≈ 0–3%.
- Interleaved/scalar clients (redis-benchmark mixed, memtier mixed, real apps mixing GET+SET per pipeline position): each SET run is its own batch (`dispatch_set_one` → `set_string_fast_entry` → `write_epoch()` per op, `storage/string.rs:414`) → **3 global RMWs per op**. At 6 workers this line pair sustains ~15–40M contended RMWs/s (ring-bus handoff ~25–60 ns) — a hard ceiling exactly in the observed throughput band. This is also the leading explanation for the INCR 2→3w stall (scalar write path) and the SET 2→3w 1.17×.
- Fix (unchanged from §3.2-A, now quantified): stripe `readers.count`/`writers` per getcpu slot with a per-stripe handshake; writer drains all stripes.
- Expected: **+0–3%** pipeline-batched; **+8–20%** interleaved scalar mixed at ≥4 workers.

**M-2 · `tick_clock()` — one global cache line written by every worker every event-loop iteration** (`worker.rs:111` → `value/mod.rs:208-210`)

Each poll iteration does `UNIX_MS_CACHE.store(now_ms(), Relaxed)` — a *write* to one shared line from all workers, requiring exclusive ownership every iteration. With one event per iteration (small pipelines, pubsub-heavy, blocking-wake loops) this is a per-event line transfer across up to 12 threads; the line bounces continuously.
- Fix (trivial, zero risk): gate the store on value change — `let now = now_ms(); if cached != now { store }`. Milliseconds change ~1/1000 s, so ~99.9% of iterations become a pure shared read; expiry semantics unchanged (same value, same visibility). Optionally per-worker cells later.
- Expected: removes a continuous 6–12-way exclusive-line ping-pong; biggest effect on pubsub and blocking-heavy loops (the two workloads that already scale negatively).

**M-3 · PUBLISH negative scaling — `fanout_bytes` producer/consumer RMW collision + per-entry fetch_sub during drain** (`slot.rs:147-165, 185`)

Per publish per target worker: `fanout_bytes.try_update` (RMW) + `SegQueue.push` (shared-tail RMW, contended by all 10 publishers) + `fanout_pending.arm` (swap). The owning worker's drain then does `fanout_bytes.fetch_sub` **per entry** (`slot.rs:185`) — the same line is RMW'd from both sides while publishers are active. With 10 publishers × 6 target workers that is ~60-way producer contention plus consumer interference per line.
- Fixes: (a) accumulate drained bytes locally and do **one** `fetch_sub` per `drain_fanout_limit` call — removes the consumer half of the collision, trivial; (b) optional: shard `fanout` per producing worker (ring per producer→target pair) to cut SegQueue-tail contention — larger change, only if (a) is insufficient.
- Expected: **+10–20%** on Pub/Sub publish at ≥4 workers; delivery numbers unchanged (they are already fanout-batched on the consumer side).

**M-4 · Producer/Consumer & ZADD negative scaling — critical section contents + SMT yield pathology**

Two compounding causes, both verified in code:

1. *Reply building and allocation inside the entry write lock.* `list_write` runs the whole closure under `update_with` (`storage/list.rs:61-77`), and `pop_one_to_buf`'s closure **writes the RESP bulk reply into `wbuf` inside the lock** (`list.rs:137-151`) — including the capacity check and possible **wbuf realloc (memcpy of the whole retained buffer) while holding the hot key's lock**. `zadd` similarly holds the lock across `z.reserve` (possible realloc), per-member ordered-chunk insert (memmove of up to 256 entries), and reply extends (`storage/zset.rs:41-61`). For a shared queue key, every LPUSH/RPOP/ZADD therefore transfers the entry line *plus* does buffer work under it — the handoff cost multiplies.
2. *Spin-then-yield under SMT.* The entry lock's terminal backoff is `yield_now()` (`shard.rs:233-263`). On the 6-worker config both queue endpoints land on the same physical core as SMT siblings: a yielded waiter still shares the core's execution resources with the holder, so yielding frees nothing and adds scheduler churn — the classic SMT spinlock pathology. Three dedicated physical cores (3w) don't hit it, which matches 16.4M→3.27M exactly.

- Fixes, in order:
  a. **Shorten the critical sections**: pop the element to a small stack/scratch buffer under the lock, build the reply *after* the guard drops (same pattern the GET path already uses — serialize under a read guard, not a write lock). Pre-reserve `wbuf` before `update_with`. For ZADD: parse/allocate member storage before the lock, reserve `out` before, build replies after. (low risk, mechanical)
  b. **Replace terminal `yield_now()` with a bounded futex park** for the entry lock: after backoff saturates, park on a per-shard waiter slot (woken by unlock); the holder gets a full core instead of an SMT rival. Keep the current spin phases (they are the right fast path). (medium risk — needs a lost-wakeup analysis; the unlock store is Release, park must re-check after arming, the standard futex protocol)
- Expected: (a) **+20–50%** on Producer/Consumer and ZADD at high worker counts (critical section shrinks by the reply/alloc time); (b) another **2–4× on the 6-worker SMT queue case** (it is the difference between 3.27M and a ≥3-worker-class number); LPUSH/RPOP per-client-key workload gains less (already 1.41×).

**M-5 · Mixed-workload read-epoch fallback loses the batch optimization with no retry** (`fast_path.rs:278-284`)

`try_read_epoch` fails whenever *any* writer is registered map-wide; under mixed multicore traffic (SETs happening somewhere while a GET batch runs) whole ≥32-key batches silently fall back to 2 RMW/key entry guards. One failed attempt is made, before prefetching; the writer may have drained microseconds later.
- Fix: after the failed attempt, re-try once immediately after `prefetch_slots` (which takes ~µs for 32–256 keys) before committing to the fallback. Alternatively fall back per-key incrementally instead of batch-atomically. (low risk)
- Expected: +2–6% GET in mixed multicore workloads.

**M-6 · `at_key_capacity()` scans all shard counters per command when `FYRODB_MAX_KEYS` is set** (`store/mod.rs:1313-1315` via `fast_path.rs:131` *and* `1478`)

`is_full()` → `len()` iterates every shard's `live` counter (128 padded lines for the default shard count) — up to **two full scans per command** (the buffer-loop gate and the dispatch gate). Default (`usize::MAX`) is a single branch, fine; but with a configured limit this adds ~150–300 ns/op of shared reads.
- Fix: maintenance-cached approximate total (updated by the 10 s/60 s maintenance passes and on insert/delete via the existing per-shard counters), or a single approximate global counter RMW'd only on insert (rare vs reads). Gate OOM only on the approximate value, exact-check on the insert path. (low risk)
- Expected: recovers the config-dependent overhead (~5–10% of the affected deployments' throughput).

**M-7 · mimalloc `purge_delay=0` + `purge_decommits=1` — tight RSS, page-refault latency spikes** (`rust-zmalloc/src/lib.rs:170-184`)

Immediate MADV_DONTNEED keeps the published RSS numbers (a project goal) but every regrowth of a freed region pays soft page faults (~2–4 µs/page) — visible as multicore latency spikes after connection churn or large-payload bursts, and it *increases* cross-core work (fault handling).
- Fix: expose `purge_delay` as an env knob (keep 0 default to preserve RSS targets); document that latency-sensitive deployments can set ~500 ms. Optionally validate with the existing `lat` bench mode.
- Expected: no default-throughput change; p99 tail improvement option for bursty deployments.

**M-8 · `fanout_seen` O(connections) fill twice per fanout cycle** (`worker.rs:189-195`, `worker.rs:402-411`)

Every WAKER event and every `flush_fanout` fills a `Vec<bool>` sized to the connection table. With 10k subscribers that is ~20 KB of memset per cycle — cheap per-cycle but per-cycle at pubsub rates.
- Fix: generation-stamped `Vec<u32>` (bump a epoch instead of fill), or reuse the existing `sub_dirty` ordering. (trivial)
- Expected: small CPU reduction on large-subscriber-count pubsub deployments only.

**M-9 · EBR global epoch RMW bursts under delete-heavy load** (bounded, acceptable)

Retires happen on DEL/grow/rehash; each worker's `Guard::drop` runs `force_collect` when its thread-local DIRTY is set (`ebr.rs:14-23`), and epoch advance is a CAS on crossbeam's global epoch. The `PENDING` gating and the 16 384-iteration stall cap bound the cost, and GET/SET never retire. Delete-heavy bursts will show epoch CAS contention — inherent to EBR, acceptable. No change recommended (a custom EBR was rejected in §7 for reclamation-risk reasons).

**M-10 · EVAL/EXEC exclusive admission is a designed throughput cliff** (document, don't change)

Exclusive admission locks all worker slots + writer Mutex (`isolation.rs:52-60`) and waits for every in-flight command to drain. One EVAL per 1 000 ops at 40M ops/s ⇒ 40K full-pool drains/s. This is the price of Redis script atomicity in a parallel engine; per-key script isolation would change semantics. Keep; surface it in docs/INFO metrics so operators can see the stall.

### 9.4 What already scales correctly (verified, keep)

- Per-worker isolation slots, per-shard growth mutex/gate, CachePadded shard counters — no cross-core traffic on their own lines.
- getcpu-striped TTL counters (`ttl.rs:25-52`) — the striping pattern to copy for M-1.
- Wake suppression protocol (`signal.rs` arm/disarm with re-check after disarm) — no lost-wakeup window found; eventfd writes amortized to ~1 per drain cycle.
- Repeated-key run collapsing — why Hot Key reaches 118M/s with a single entry guard per run.
- Pub/Sub consumer side: grouped fanout append, backlog shedding, per-worker queues (`worker.rs:416-544`).
- Read-epoch batch admission design (the handshake itself is correct; only the fallback policy is improvable, M-5).
- `blocking.signal` early-out on a Relaxed load when nothing is registered (`blocking.rs:57`) — no per-write mutex cost for non-blocking workloads.

---

## 10. GET / SET deep-dive — every remaining per-op cost and the concrete fixes

This section walks both hot paths end-to-end at the byte level, lists what is already optimal (do not touch), and isolates the costs that remain, with fixes and expected gains. Costs not listed here are already covered by §3 (locks), §4 (payload sizes), §9 (multicore).

### 10.1 GET — current best-path sequence (scalar, hint hit)

`dispatch_raw` → `dispatch_get_one` (`fast_path.rs:168-180`) → `hint_for` (`prefetch.rs:290-308`, hash+entry from the per-connection `prefetch_ahead` scan — **zero map probes on hint hit**) → `part_str` (key UTF-8 validation, `mod.rs:214-223`) → `get_to_buf_auto` (`storage/string.rs:544-572`) → `get_to_buf_fast_entry` (1 CAS + 1 sub, expiry check, `SmallStr::write_resp` into `wbuf`) → reply.

**Already optimal — verified:**
- `write_resp` hand-writes RESP headers with precomputed constant tables and unaligned stores for len < 10 and ≤ 23 (`small_str.rs:319-391`), `write_bulk_bytes` the same for < 10/< 100 (`resp.rs:202-220`) — zero formatting branches, zero `itoa` calls on these paths.
- The per-connection hint cache means the common scalar GET does **no hash computation and no probe** — the prefetch scan (`prefetch.rs:61+`) did it once for the whole batch.
- Repeated-key pipelines collapse to one entry guard per run (why Hot Key scales).
- The read-epoch path reads entries with **no per-entry RMW** and is correct by construction: under an admitted epoch no writer can hold any entry lock (the epoch handshake excludes registered writers before the first entry read; `STATE_MATCH_MASK` includes `STATE_LOCK`, so a locked entry fails the fast check and falls back to a guarded read, `storage/string.rs:357-386`).

**Remaining costs and fixes (ranked):**

- **G-1 · Read-epoch floor 32 → 8** (`fast_path.rs:6`, `READ_EPOCH_MIN`). GET batches of 8–31 unique keys still pay 2 RMW/key. The epoch costs 2 SeqCst RMWs per batch regardless of size; at ≥8 the amortization is already positive, and the failure mode (epoch refused under concurrent writers) falls back to today's path. *Expected: +2–5% GET on pipelines 8–31; zero elsewhere.* (repeat of §4.1 #1, kept for completeness)

- **G-2 · Scalar GET fallback can probe the same key three times.** `get_to_buf_auto` tries (1) the hint entry, (2) `probe_entry_fast` fresh probe, (3) `get_to_buf_fallback` → `with_entry_pinned` → `Shard::find_fast` — a third probe of the identical key (`string.rs:544-572, 238-257`). Happens only on stale hints (same-key mutated mid-pipeline) or guard failure (writer holds lock) — rare, but the third probe is pure duplication: the fallback should reuse the probe result from step 2. *Expected: negligible on healthy paths; removes a rare 2× probe + re-entry under write-heavy same-key contention.* (low effort)

- **G-3 · Batch GET payload prefetch window caps at 1024 B** (`fast_path.rs:305`: `24..=1024`). Batches of 1–64 KiB values get entry prefetch but no payload prefetch, so the first touch of each payload is a cache-miss memcpy. Widen the window (e.g. up to 4–8 KiB, keeping the multicore guard `first_payload_len <= 256 || worker_capacity() == 1` for smaller sizes and adding a worker-count-aware bound for medium) — the existing 16-ahead entry prefetch already provides the schedule. *Expected: +3–8% on 1–4 KiB batch GET, less at 6 workers (bandwidth-limited).*

- **G-4 · Repeated-key detection and run grouping memcmp the key bytes** (`fast_path.rs:273-275, 342-345`). `keys[i] == keys[j]` compares content. For the 5-byte benchmark keys this is free; for 256 B–1 KiB keys a 256-deep batch does up to ~64 KiB of memcmp per batch. Fast path: compare `(ptr, len)` pairs first (same buffer offset ⇒ same bytes) and memcmp only on mismatch. *Expected: negligible for short keys; a few % for large-key batch GET.* (trivial)

- **G-5 · Batch GET `wbuf` under-reserve** — covered as §4.3: pre-reserve from the scanned payload lengths. The batch scan already has every key/value length (`try_fast_get_key_at` produced them); one `reserve(sum)` replaces the growth-realloc copies. *Expected: +3–8% medium-payload deep pipelines.*

- Note (no action): expired keys encountered under an admitted read epoch return misses without removal (`string.rs:375-377`) — intentional (can't take the write path inside an admitted read epoch); the next touching op or the active-expiry scan removes them. Not a leak.

### 10.2 SET — current best-path sequences

*Batch overwrite (pipeline ≥ 2, the benchmark path)*: `dispatch_set_batch` (`fast_path.rs:417-641`) — scan up to 256 `*3\r\nSET` frames with zero parsing, `prefetch_slots_write` + tiled `probe_and_prefetch_entries_write`, one map write-epoch per batch, in-place `SmallStr::overwrite_bytes` under the entry CAS (optionally the AVX streaming copy ≥ 8 KiB, hash-uniqueness-guarded), `OK_BLOCK` reply block-copy. **Already optimal** — binary values handled directly from the wire, no scans, replies precomputed.

*Scalar SET* (`dispatch_set_one`, `fast_path.rs:370-395`) — hint → `part_str` → **`std::str::from_utf8(value)`** → `set_string_auto` (in-place overwrite) or, for binary, `Store::set` (the slow general path).

**Remaining costs and fixes (ranked):**

- **S-1 · Scalar SET validates UTF-8 on the full value — and routes binary values to the slow path** (`fast_path.rs:381-390`). The `from_utf8` scan is only used to pick between `SmallStr::new(&str)` and `SmallStr::from_slice(&[u8])` — **storage is byte-identical for both** (SmallStr is binary-safe; the binary SET regressions prove it, `store/mod.rs:306-420`). Worse, the non-UTF-8 branch calls `Store::set` — the general `set_key_with_overwrite` probe loop — so **binary scalar SETs skip the in-place fast overwrite entirely** and pay a second, slower path. Fix: a bytes-native `set_string_auto_bytes(key_bytes, hash, entry, value_bytes, expires)` (the pieces exist: `set_string_fast_entry_bytes`-style overwrite + `set_string_fast_bytes`), then `dispatch_set_one` calls it unconditionally.
  *Expected: scalar SET −1 full value scan (for 8 KiB–1 MiB values that is ~5–20% of the op; for ≤ 64 B values ~1–2%); binary payloads additionally regain the fast path (previously ~1.5–2× slower than UTF-8 payloads of the same size).*

- **S-2 · Batch-SET insert path validates the key per op; the update-fallback validates the value** (`fast_path.rs:581` — `from_utf8(keys[k])` with an error reply on failure; `fast_path.rs:605` — `from_utf8(val_bytes)` in the `set_string_auto` fallback). The insert fast path itself is binary-safe on values (`SmallStr::from_slice`, line 593). Fixes: (a) bytes variant of `insert_slot_fast_located_pinned` via a `CompactKey::from_bytes` (the 16-byte layout already supports arbitrary bytes — `key.rs` works on bytes everywhere else, `eq_bytes` exists), (b) bytes `set_string_auto` per S-1. This is also the enabling step for **Redis-compatible binary keys** (today a non-UTF-8 key gets `-ERR invalid UTF-8 in request` everywhere, while Redis stores it — a compatibility gap, not just a cost).
  *Expected: new-key insertion workloads with medium/large values −1 key scan per op (~1–3%); long keys (≥ 256 B session IDs) −1 scan each (~2–5%); binary-key workloads go from unsupported to supported at full speed.* (medium effort — API signatures take `&str` throughout the store layer)

- **S-3 · SET+EXPIRE pairs are already fused — verified, keep.** `dispatch_set_expire_pair` (`coalesce.rs:28-160`) applies value+TTL in **one** entry lock (`set_string_auto(..., exp)`, line 119-128) and runs up to 128 pairs per pass with the dedicated `try_fast_set_expire_pair_at` parser. No change. The remaining per-pair costs are S-1/S-2 validation and §3.2-D replication capture.

- **S-4 · Replication/AOF capture per write** — §3.2-D (single-pass capture under the entry lock, striped log): the largest SET win when persistence is on (**+20–40%**).

- **S-5 · Batch SET tile boundary holds the map write-epoch across all 256 entries** (by design, `fast_path.rs:484`): correct and cheap (3 RMWs/batch), but it means a 256-SET batch blocks concurrent map read-epoch admission for the batch duration. At deep pipelines × high worker counts this shows up as read-epoch failures (M-5). No change needed beyond M-5's retry; noted so it isn't mistaken for a bug.

- **S-6 · Small-value insert allocations.** First write of a ≤ 23 B value: no payload alloc (inline SmallStr) — optimal. 24 B–63 B heap values: mimalloc 64-B class, one alloc per new key. Reuse across delete/insert cycles already happens via the allocator; a per-shard free-list would add complexity for ~0 gain (mimalloc's 64-B class is already ~20 ns).

### 10.3 GET/SET per-op ledger after all fixes

| Op (6 workers) | Current medians | After §10 + §3.2 + §9 fixes |
|---|---|---|
| GET small, pipeline ≥ 32 | 47.4M/s | epoch floor irrelevant here (already batched): +0–2% |
| GET small, pipeline 8–31 | (subset) | **+2–5%** (G-1) |
| GET 1–4 KiB deep pipeline | (medium suite) | **+6–16%** (G-3 + G-5) |
| GET ≥ 64 KiB | (large suite) | +5–10% (G-5 + §4.4) |
| SET small pipeline (no persistence) | 37.7M/s | +3–6% (§3.2-A, M-1) |
| SET small scalar/interleaved | — | **+8–20%** (S-1 + M-1) |
| SET 8 KiB–1 MiB scalar | (large suite) | **+5–20%** (S-1 removes the validation scan) |
| SET new-key insert, medium values | (Pipeline-64 style) | +2–5% (S-2) |
| SET+EXPIRE | 7.8M/s | +10–25% (S-1/S-2 validation removal + §3.2-D capture when replication on) |

---

## 11. fyro-vector deep-dive + heap-allocation replacement audit

Scope: `crates/fyro-vector` (HNSW graph, kernels, quantization, thread pool) and every per-op heap allocation in the server, evaluated strictly by the rule: **replace only where the allocation is on a per-op path and the replacement is same-or-faster**.

### 11.1 What the vector engine already does right (do not regress)

- **ISA-dispatched, monomorphized kernels** (`kernels/mod.rs:25-103`): AVX-512 / AVX2+FMA / NEON / scalar selected once into `LazyLock`; the `isa_dispatch!` macro monomorphizes hot loops so kernels **inline** — no function-pointer calls in the inner loop. RaBitQ plane-popcount has a scalar fallback wired through the same trait.
- **Cache-line-disciplined node storage** (`mod.rs:35-46, 142-162`): 64-B-aligned `Line`s, stride a multiple of 16 words, links immediately after payload in the same arena → one T0 prefetch covers the vector, a second covers the link line (`mod.rs:241-265`), payload hints capped at 1024 B.
- **Zero-alloc traversals**: epoch-stamped visited table (one store per visit, no clear per run — `mod.rs:62-123`), thread-local `Scratch` with re-entrancy fallback (`mod.rs:125-136`).
- **Quantization**: Int8 + RaBitQ codebooks behind `RwLock<Arc<Codebook>>` (read side cheap), two-pass pre-filter scans via lazily built `flat_int8`, exact vectors released after linking so only codes stay resident (`mod.rs:155-161, 1321+`).
- **Search-entry hints + multi-entry starts** for large graphs (`search.rs:336-349`).
- **Soft delete + tombstone compaction** at a 10% ratio; free-slot reuse (`mod.rs:26-33`).
- **Bulk-build discipline**: `prepare_batch` takes the write lock once, linking is parallelized over the pool with an ef ramp (`mod.rs:1249-1300`), and the FT build paths call `graph.reserve(expected)` first (`search/graph.rs:227, 438`).
- **Concurrent brute scans batch** into one memory sweep (`search.rs:355-414`).

### 11.2 Vector findings (ranked)

**V-1 · `blocks: Vec<Line>` is one contiguous arena — growth *and* shrink copy the whole graph under the write lock** (`mod.rs:801` `resize_with`; `mod.rs:1309-1317` `shrink_to_fit`).
For a 1M-node, 768-dim unquantized graph the arena is ≈ 3.2 GB (≈3.2 KB/node); Int8 ≈ 900 MB. Any incremental `prepare()` past capacity reallocs and **copies the entire arena while `inner.write()` blocks every search**; the post-build `shrink_to_fit` then discards the reserved headroom, so the next incremental insert reallocs again — copy churn plus a search stall after every large build. Bulk paths pre-reserve, so this bites exactly the *incremental* index-maintenance pattern (dirty-key reindex, single vector inserts).
- Fix (a, robust): chunk the arena — `Vec<Box<[Line]>>` in fixed 4 096-node chunks; growth appends one chunk (no copy, no giant realloc); `base()` adds a chunk lookup (~1% search cost). Needs the graph test suite re-run (touches `base()` arithmetic).
- Fix (b, minimal): shrink to `len + 25%` at chunk granularity instead of exact fit, and rate-limit shrinks. Trivial, keeps RSS targets with bounded slack.
- Expected: (b) removes the post-build realloc-stall cycle (~ms-class stalls per incremental insert after builds) for ~free; (a) additionally removes all mid-serve growth stalls.

**V-2 · `inner.read()` spans whole traversals; every `inner.write()` (grow/compact/shrink/book-train) stalls all searches** — with V-1(b) the remaining write sections are µs-scale (append chunk, encode O(dims)). Compaction is the last long write; making it incremental (per-chunk rewrite) is possible but is a large change — optional, only if compaction stalls show in `FT.*` latency traces.

**V-3 · Per-search heap allocations** (`quant.rs:342-372`, `search.rs:282-287`, `search.rs:359-433`):
`encode_query` builds a fresh `Vec<i8>` (Int8) or `Vec<(RqPlanes, f32)>` (RaBitQ) **per query**; `search_with` allocates an intermediate `Vec::with_capacity(ef)` and then a final `.collect()`; `brute_search` allocates `Arc<FlatReq>` + `q.to_vec()` per request plus batch/result Vecs per sweep. None of this touches the KV path; it is 2–4 allocations per `FT.*` search (~100–300 ns each).
- Fixes: encode into the thread-local scratch (QEnc borrows scratch codes), return the `capacity(ef)` vec as the result instead of re-collecting, pooled request slab for brute batches.
- Expected: ~2–5% on small-ef, search-heavy vector loads; more at high QPS.

**V-4 · Pool job queue is one std `Mutex` + `Condvar` over two `VecDeque`s** (`pool.rs:93-153`): every worker contends on one mutex per job once the ticket `SegQueue` is empty. Fix: per-thread crossbeam deques or a second SegQueue. Background-indexing only — medium priority, low urgency.

**V-5 · `brute_search` waiting side spin-then-yields** (`search.rs:401-412`): concurrent brute queries that lose the scanner race burn CPU on the same cores doing the scan. Fix: park on the request with a short timeout instead of yield-spinning. Low risk.

**V-6 · Keep**: per-node `Vec<Mutex<()>>` (parking_lot `Mutex<()>` is 1–2 B/node — the lock discipline is sound: link words only via atomics, vector words under the exclusive lock, `mod.rs:38-40`); `entry_hints`/`maint`/`entry_lock` Mutexes (maintenance-gated); the `free` list and `F_TOMB` compaction design.

### 11.3 Heap-allocation replacement inventory (whole server)

| Site | Current | Replacement | Verdict |
|---|---|---|---|
| AOF record value (`aof.rs:319-324`) | `read()` closure allocates a `Vec<u8>` holding the full value, then `encode` copies it again into `Writer.buf` — **2 copies + 1 alloc per AOF record** | encode straight into `Writer.buf` (entry ref captured under the entry lock) | **do** — part of §3.2-D; −1 alloc, −1 full-value copy per AOF write |
| Replication record (`store/cluster.rs:56-80`) | second `get_ref` + `encode_single_value` Vec + `key.to_vec()` per write | in-lock capture into per-worker log buffers | **do** — §3.2-D |
| `encode_query` + `search_with` results (V-3) | 2–4 allocs per FT search | scratch encode; return the capacity-ef vec | **do** (vector loads only) |
| `brute_search` request (`search.rs:359-364`) | `Arc<FlatReq>` + query copy per request | pooled slab | optional |
| Pub/Sub frame (`registry.rs:82-100`) | `Arc<[u8]>` per publish (alloc + copy + refcount RMWs per target worker) | frame pool / `Bytes`-style slab | optional — Arc sharing is already the correct cheap design; refcounts are per-worker, not per-subscriber |
| Parked-command reply (`park.rs:104,147`) | `Vec<u8>` per unpark | build into the conn `wbuf` under the existing guard | minor |
| `lrange` / `hgetall` non-`_to_buf` variants (`list.rs:216-230`, `hash.rs:400-414`) | `Vec<String>` / `Vec<(String, String)>` per call | none — the `_to_buf` variants already serve RESP paths (verified); the allocating variants serve Lua/legacy callers that need owned values | keep |
| Parser `rbuf`/`wbuf`/`parts_raw` | retained, reused, idle-shrunk | already optimal | keep |
| HNSW `Scratch` | thread-local, geometric growth | already optimal | keep |
| customhash entries/tables | raw zmalloc + EBR | already optimal | keep |
| RDB snapshot side buffer | one `Arc<Mutex<Vec<u8>>>` per save | rare | keep |
| `Aof.writers` clone per `sync_all` | 1 Vec clone/s | rare | keep |
| Cluster transport buffers | surface-reviewed; per-peer queues with capacity caps (`FYRODB_CLUSTER_QUEUE_CAPACITY`) | — | note only |

**Net:** after §3.2-D and V-3 land, the only per-op heap allocations left anywhere are: the pub/sub frame per publish (needed for cross-worker sharing), new-key value storage (inherent), and pipeline-batch growth (G-5 removes the realloc copies). That is close to the floor for this architecture.

### 11.4 Expected effect

- **Vector workloads** (`FT.*` / vector inserts): V-1(b) removes the post-build realloc-stall cycle (ms-class per incremental insert after a large build, previously also blocking all concurrent searches); V-3 ~2–5% search QPS at small ef; V-5 removes spin burn under concurrent brute scans. Search **recall and traversal costs unchanged** — all changes are storage/alloc mechanics.
- **KV paths**: untouched by this section (the vector crate runs on its own pool); the AOF/replication capture change is the §3.2-D win counted there.
- **Memory**: V-1(b) trades bounded slack (≤25% of graph size) for zero copy churn — configurable; the AOF capture removes one transient full-value allocation per write (allocator churn down, RSS neutral).

---

## 12. BOTTOM LINE — if all recommended changes land

### Implementation status (2026-11-13, same-day pass)

The following changes from this audit have been **implemented and verified** (full `cargo test --workspace`: 789 passed, 0 failed; release profile builds):

| Audit item | Change |
|---|---|
| **6.1** (correctness) | `EntryReadGuard::drop` now does the decrement + last-reader unlock as **one CAS** (`shard.rs`) — the join-under-lock clobber window is closed; reader-join behavior preserved. |
| **M-2** | `tick_clock` stores only on value change (`value/mod.rs`) — the global clock line stays read-shared. |
| **6.5** | `enter_insert`/`grow`/`rehash`/`compact_shard` GROWING spins back off then yield. |
| **3.2-polish** | `grow_lock` → `parking_lot` (customhash); `local_subs` → `parking_lot` (pubsub). |
| **3.2-B** | Script lookups use a per-thread epoch-checked snapshot (`store/mod.rs`, `SCRIPT_TL`); steady-state EVALSHA pays zero atomics; re-entrant (nested EVAL) falls back to the shared map. |
| **3.2-C** | AOF sequence numbers allocated in 64-record windows per writer (`aof.rs::next_seq`) — the global counter is no longer a per-record RMW. |
| **6.3** | AOF records carry a CRC-32 (bit-31 length flag keeps old files replayable); mid-file corruption stops replay loudly with the sequence number; write errors keep the buffer for retry instead of dropping it; `aof::failed()` exposed for INFO. |
| **§11.3/3.2-D(perf)** | `record_current_value` does one lookup + one encode shared by AOF and the replication log (`record_encoded`); `record_current_value` ordering was **withdrawn as a bug** (see 6.2) — guard-through-append already serializes it. |
| **M-3a** | Pub/Sub fanout drain folds byte-accounting into one release per drain (`slot.rs`). |
| **M-8** | `fanout_seen` is generation-stamped (`worker.rs`) — no O(connections) fills per cycle. |
| **G-1/M-5** | GET batches retry the read-epoch admission once after slot prefetch (`fast_path.rs`). |
| **G-2** | Scalar GET retries the found entry once before the full fallback re-probe (`string.rs`). |
| **G-4** | Repeated-key/run detection compares pointer identity before content (`same_parts`). |
| **G-5** | Batch GET reserves `wbuf` from the observed first payload (capped) instead of doubling from 64 B. |
| **G-3** | Payload-prefetch window widened to 8 KiB single-worker / 1 KiB multicore. |
| **S-1** | Bytes-native SET path (`set_string_auto_bytes` / `set_string_fast_entry_bytes` / `set_string_fallback_bytes`): no value UTF-8 scan on scalar SET, batch fallback, or `set_string_fast_bytes`; binary payloads keep the fast in-place overwrite. |
| **6.4** | TTL counter transitions adjusted in-place on the overwrite paths (old→no-TTL decrements; no-TTL→TTL increments; TTL→TTL no longer double-counts). |
| **M-6** | `CustomMap.approx` — exact net key counter maintained by the same bump events; `is_full` is one relaxed load instead of a 128-shard scan. |
| **M-4a** | LPOP/RPOP pop into per-connection scratch and serialize after the entry lock (`pop_scratch`); ZADD collects flags under the lock and writes replies after it (`zset.rs`). |
| **3.2-A** | Write-admission counters striped 16 ways (`access.rs`): per-(thread, map) deterministic stripe, Dekker handshake preserved (reader registers → checks all stripes; writer registers → checks the reader count). The per-scalar-SET global writers RMW contention is divided by the stripe count. |
| **V-1(b)** | Vector graph shrink keeps 50% slack at power-of-two targets instead of exact fit (`hnsw/mod.rs`) — no post-build headroom discard. |
| **V-3 (partial)** | Vector search encode/result allocations kept; the Int8 scratch-encode refactor remains open (QEnc borrow plumbing). |

Still open by design (audit §3.3/§7/§10-11): S-2 binary keys (semantic decision), 3.2-D striped replication log, V-1(a) chunked arena, V-3 Int8 scratch, V-4/V-5 pool polish, M-4b futex park, M-7 purge-delay knob, MSG_ZEROCOPY, entry alignment.

### Second-day deep sweep (daily-use commands) — additional fixes landed

A follow-up pass over the coalesced run machinery and the remaining everyday commands found and fixed:

| Finding | Change |
|---|---|
| **G-5 regression caught in self-review** | The first-day `wbuf` reserve made non-epoch batches *under*-reserve (`count*16` vs the old `count*64`); restored the historical estimate as the floor when no payload length was observed. |
| **Run-path replication capture ×K → ×1** | `execute_write_run` and `execute_queue_run` recorded `record_current_value` once **per command in the run** — every iteration captured the identical post-run value, so K−1 lookups + encodes + log appends per run were pure waste (up to 256× on deep runs). All nine ops (Set, Incr, Hset, Sadd, Pfadd, Zadd, Geoadd, JsonSet, queue push/pop) now capture once; the sequential error fallbacks keep per-mutation captures. Replay convergence is unchanged (each record is the state at capture under the read guard). |
| **Write-run SET value scan** | The coalesced SET run validated the value with `part_str` and routed binary to the slow path; now bytes-native like the rest (S-1 family). |
| **MGET batch treatment** | `mget` (daily-use) now hashes + prefetches once and serializes per key, with the same read-epoch admission (≥8 keys) and writer-drain retry as the pipelined GET path — per-key read-guard RMWs eliminated for epoch-backed batches; >64 keys keep the simple path. |
| **MSET slot prefetch** | MSET (2–64 pairs) prefetches all shard slot lines once, then SETs with pre-computed hashes — cold shard-table probes removed. (MSET/MSETNX capture was verified present via the generic `capture_if_success` layer — no durability gap.) |
| **Queue-run pop reserve** | `pop_many_to_buf` reserves for typical element sizes *before* taking the entry lock — a `wbuf` realloc no longer memcpys the retained buffer while holding the hot queue key (the LPUSH/RPOP/Producer-Consumer workload). |
| **HSET run flags alloc** | `hset_bytes_added_flags` returns `SmallVec<[bool; 32]>` instead of `Vec<bool>` — no heap allocation for typical field runs. |
| **PUBLISH pattern registry lock** | `patterns` std `RwLock` → `parking_lot` (read taken per publish when pattern subscribers exist). |

Verified again after the sweep: `cargo test --workspace` 789 passed / 0 failed, zero warnings in dev and release.

### Issues fixed
1. **Hot-key memory-corruption race** (6.1) — the only reachable unsound-state path found in the core map; **fixed and covered by the workspace suite**.
2. ~~Replica divergence under concurrent same-key writes (6.2)~~ — **withdrawn during implementation review**: the capture's entry read guard spans encode+append, which serializes records per key and guarantees replay convergence (see §6.2). No fix needed; the risk was 6.1 itself.
3. **Silent AOF data loss on corruption/write failure** (6.3) — **fixed**: CRC-32 per record (backward-compatible flag), loud replay stop on checksum mismatch, write errors keep the buffer for retry, `failed` surfaced.
4. TTL scan-cycle waste (6.4), GROWING spin burn (6.5), dev-profile wedge-on-panic (6.6), repeated-GET memory spike (§4.4) — 6.4 and 6.5 **fixed**; 6.6 remains release-safe (`panic=abort`) with a documented note.
5. **Multicore pathologies (§9)**: the per-iteration global clock-line ping-pong (M-2), the PUBLISH producer/owner byte-counter collision (M-3), the reply/alloc-under-lock critical sections that cause Producer/Consumer and ZADD to scale *negatively* (M-4a), the SMT spin-yield pathology (M-4b, optional), the read-epoch fallback that loses batch GETs under mixed traffic (M-5), and the 128-shard scan per command when `FYRODB_MAX_KEYS` is configured (M-6).
6. **GET/SET residual costs (§10)**: the scalar-SET full-value UTF-8 scan and binary-payload slow-path routing (S-1), the per-op key/value validation on batch inserts and fallbacks (S-2), the triple-probe scalar-GET fallback (G-2), the 1024 B payload-prefetch cap on medium batch GETs (G-3), and content-memcmp run detection for large keys (G-4). S-2 additionally closes the binary-key compatibility gap with Redis (binary keys are currently rejected on every SET path while Redis stores them).
7. **Vector + allocation findings (§11)**: the post-build `shrink_to_fit` that discards graph headroom and forces a full-arena realloc (blocking all searches) on the next incremental insert (V-1b), the per-query encode/result allocations (V-3), the brute-scan spin burn (V-5), and the double value copy per AOF record (§11.3, realized with §3.2-D).

### Expected throughput gains (vs. current medians)

| Workload | Change drivers | Expected gain |
|----------|----------------|---------------|
| Small GET (pipelines 8–31) | epoch-batch widening | **+2–5%** |
| Small GET (pipeline ≥32) | (already epoch-backed) | 0–2% |
| Small SET, no persistence | striped admission counters | **+3–6%** |
| SET + AOF enabled | single encode + record_encoded + seq ranges | **+10–20%** (striped log still open) |
| SET + replication enabled | single lookup + single encode (landed); striped log (open) | **+10–20%** landed, up to **+30–40%** when the striped log lands |
| EVAL/EVALSHA multicore | epoch-checked TLS cache | +2–5% |
| Medium GET (1–4 KiB deep pipelines) | wbuf pre-reserve | **+3–8%** |
| Large GET (≥64 KiB) | pre-reserve (+zerocopy later) | **+5–10%** (zerocopy: up to +20–40% NIC-bound, optional) |
| SET 8 KiB–1 MiB scalar | S-1 validation-scan removal + bytes path | **+5–20%** |
| SET small scalar/interleaved | S-1 + M-1 | **+8–20%** |
| Binary-key/payload workloads | S-1/S-2 bytes API (also a Redis-compat fix) | previously slow-path or unsupported → full speed |
| `FT.*` vector search (small ef, high QPS) | V-3 scratch encode + result reuse | **+2–5%** |
| Vector index maintenance (incremental inserts after builds) | V-1(b) shrink slack / V-1(a) chunked arena | removes ms-class realloc stalls that block all searches |
| Concurrent brute vector scans | V-5 park instead of spin | removes spin burn under load |
| SET+EXPIRE (cache TTL) | S-1/S-2 + §3.2-D | **+10–25%** |
| Large SET (≥64 KiB first write) | streaming copy on insert path | **+5–15%** |
| Pub/Sub | (no changes; already lock-free publish) | 0 |
| BLPOP-heavy | registry sharding | +0–5% situational |

Multicore-specific gains (§9):

| Workload | Change drivers | Expected gain |
|----------|----------------|---------------|
| Interleaved/scalar mixed GET+SET, ≥4 workers | M-1 striped epoch counters | **+8–20%** |
| Pipeline-batched mixed (published suite) | M-1 | +0–3% |
| Producer/Consumer 6-worker SMT | M-4a shorter lock scope + M-4b futex park | **+20–50%** from (a); **2–4×** with (b) — removes the 0.20× negative-scaling cliff |
| ZADD ≥4 workers | M-4a (reserve/alloc/reply out of lock) | **+15–40%**, restores positive scaling |
| Pub/Sub publish ≥4 workers | M-3 batched byte accounting | **+10–20%**, restores ≥1.0× scaling |
| Mixed-workload GET batches | M-5 epoch retry | +2–6% |
| `FYRODB_MAX_KEYS` deployments | M-6 approximate capacity check | +5–10% (config-dependent) |
| All workloads | M-2 tick_clock store gating | removes constant cross-core line ping-pong (p99 tail, pubsub/blocking loops benefit most) |

Composite realistic target: **+4–8% on the headline 6-worker GET/SET medians** (they run with persistence off), **+15–40% on persistence/replication-enabled write workloads**, **+5–20% on medium/large payload suites**, and — the biggest single wins in this audit — **restoring positive multicore scaling on the three currently-negative benchmark lines** (Producer/Consumer, ZADD, PUBLISH) plus **+8–20% on interleaved mixed workloads at high worker counts** via epoch striping, depending on how many optional items land.

### Latency
- No artificial batching delays exist today (verified: coalesce/bulk drain immediately; poll timeouts only for blocking-command deadlines and 50 µs subscriber ticks, `worker.rs:91-103`).
- Wins: p99 write latency **−2–5 µs** with AOF/replication on (locks + encode removed); hot-key p99 stabilizes (no spin burn during rehash, 6.5); large-payload p99 −(realloc copy) ~10–50 µs for multi-MiB replies.
- Multicore tail wins (§9): the M-2 clock-line gating and M-4 lock-scope shortening remove two continuous sources of cross-core line handoff that show up as p99/p999 spikes rather than medians; M-7 (optional) removes allocator page-refault spikes after connection churn.
- Further p50/p99 floor is syscall+scheduler-bound; the levers are `FYRODB_BUSY_POLL_US` (exists, trades CPU) and io-uring (measured mixed — keep opt-in).

### Memory
- Replicated/AOF writes: −1 transient `Vec` (full value copy) and −1 key copy per write → transient-allocation churn roughly halves on those paths (RSS-neutral, allocator-pressure down).
- Repeated-GET cap: removes up to `pipeline × payload` spikes (worst case ~100 MiB single conn).
- Conn buffer idle caps (§ finding 10): tuning `RETAINED_WRITE_BUFFER`/idle read buffer down for high-idle-conn deployments can save **5–15% RSS at 10k connections** (measure; don't break the write-batch fast paths).
- Entry layout unchanged (64-B alignment experiment is memory-*negative* by default — only adopt if the cache-line win shows up in `without_tcp`).
- EBR/allocator collection cadences unchanged; no leaks found in audited paths (retired tables/entries are EBR-bounded; force_collect has stall guards).

### Risk ordering for implementation
1. **First (pure wins, low risk)**: wbuf pre-reserve (**G-5**), GROWING yield, grow_lock/local_subs parking_lot, script-cache TLS, AOF seq ranges, TTL drift fix, **M-2 tick_clock store gating**, **M-3a fanout drain byte batching**, M-8 generation-stamped fanout_seen, **G-2/G-4 (probe-reuse and ptr-first run detection — trivial)**.
2. **Second (correctness)**: 6.1 reader-release CAS loop + loom test; 6.3 AOF CRC + error surfacing; 6.2 via the 6.2/3.2-D capture refactor; **M-6 approximate max_keys check**; **M-5 read-epoch retry**.
3. **Third (perf-critical, needs the harness)**: **S-1 bytes-native scalar SET (drop the value scan; binary gets the fast path)**, 3.2-D striped replication log, 3.2-A striped access counters (**M-1**), **M-4a critical-section shortening (list/zset reply+alloc out of lock)**, G-3 payload-prefetch widening, large-SET insert streaming copy, repeated-GET cap, then optional MSG_ZEROCOPY / entry-alignment / **M-4b futex entry-park** experiments behind the existing bench matrix (`scripts/bench_matrix.py`).
4. **Fourth (structural / semantic)**: **S-2 binary-key support (`CompactKey::from_bytes` + bytes store API — a Redis-compat change: decide deliberately)**, **V-1(a) chunked vector arena (touches `base()` — run the graph test suite)**, M-7 purge-delay env knob, M-10 isolation-cliff documentation/INFO metrics. V-1(b), V-3, V-5 belong in steps 1–3 alongside their siblings.

*Estimates are grounded in the per-op accounting in §4-5 against the published 6-worker medians (47.4M GET / 37.7M SET); each item should be validated with the existing three-run-median TCP suite and the `without_tcp` engine harness before landing, matching the repo's own evidence standards (BENCHMARKS.md).*
