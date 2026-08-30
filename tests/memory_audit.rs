use fyro_db::storage::store::{Store, rss_bytes};
use fyro_db::storage::value::{
    FyroDB, HashInner, JsonValue, ListInner, SetInner, SmallStr, StoreValue, ZEntry, ZSetData,
};
use std::collections::{HashMap, HashSet, VecDeque};
use std::mem;

fn mb(bytes: usize) -> f64 {
    bytes as f64 / (1024.0 * 1024.0)
}

#[test]
fn audit_empty_store_baseline() {
    customhash::force_collect_quiescent();
    let before = rss_bytes();
    let alloc_before = rust_zmalloc::stats();
    let store = Store::with_config(1, 1_000);
    let after = rss_bytes();
    let alloc_after = rust_zmalloc::stats();
    println!("\nEMPTY STORE BASELINE");
    println!(
        "  rss before/after: {:.2} / {:.2} MB",
        mb(before),
        mb(after)
    );
    println!(
        "  allocator allocated: {:.2} -> {:.2} MB",
        mb(alloc_before.allocated),
        mb(alloc_after.allocated)
    );
    println!("  allocator active: {:.2} MB", mb(alloc_after.active));
    println!("  allocator resident: {:.2} MB", mb(alloc_after.resident));
    println!("  allocator retained: {:.2} MB", mb(alloc_after.retained));
    println!(
        "  shards/slots: {} / {}",
        store.map_shard_count(),
        store.map_shard_slot_count(0)
    );
    assert_eq!(store.dbsize(), 0);
}

fn measure<F: FnOnce()>(label: &str, f: F) -> usize {
    customhash::force_collect();
    let before = rss_bytes();
    f();
    customhash::force_collect();
    let after = rss_bytes();
    let delta = after.saturating_sub(before);
    println!(
        "  {label:<42} RSS: {:>8.2} MB → {:>8.2} MB  (delta {:>7.2} MB)",
        mb(before),
        mb(after),
        mb(delta),
    );
    delta
}

#[test]
fn audit_struct_sizes() {
    println!("\n═══════════════════════════════════════════════════════════════");
    println!("  PHASE 1 — Struct Sizes (size_of)");
    println!("═══════════════════════════════════════════════════════════════\n");

    println!("  Value types:");
    println!(
        "    SmallStr            = {:>3} bytes  (15-byte inline str)",
        mem::size_of::<SmallStr>()
    );
    println!(
        "    FyroDB enum         = {:>3} bytes  (disc + largest variant SmallStr=24)",
        mem::size_of::<FyroDB>()
    );
    println!(
        "    StoreValue          = {:>3} bytes  (FyroDB + expires_ms:u64)",
        mem::size_of::<StoreValue>()
    );

    println!("\n  Collection types:");
    println!(
        "    HashInner            = {:>3} bytes  (enum: Vec<String> | Box<HashMap>)",
        mem::size_of::<HashInner>()
    );
    println!(
        "    SetInner             = {:>3} bytes  (enum: Vec<String> | Box<HashSet>)",
        mem::size_of::<SetInner>()
    );
    println!(
        "    ListInner            = {:>3} bytes  (enum: VecDeque | Box<VecDeque>)",
        mem::size_of::<ListInner>()
    );
    println!(
        "    ZSetData             = {:>3} bytes  (compact Vec<ZEntry>)",
        mem::size_of::<ZSetData>()
    );
    println!(
        "    ZEntry               = {:>3} bytes  (f64 + SmallStr)",
        mem::size_of::<ZEntry>()
    );
    println!(
        "    JsonValue            = {:>3} bytes  (recursive enum)",
        mem::size_of::<JsonValue>()
    );

    println!("\n  std collection struct sizes:");
    println!(
        "    String               = {:>3} bytes  (ptr+len+cap)",
        mem::size_of::<String>()
    );
    println!(
        "    Vec<String>          = {:>3} bytes  (ptr+len+cap)",
        mem::size_of::<Vec<String>>()
    );
    println!(
        "    VecDeque<String>     = {:>3} bytes  (ptr+len+cap)",
        mem::size_of::<VecDeque<String>>()
    );
    println!(
        "    HashMap<String,Str>  = {:>3} bytes  (table base)",
        mem::size_of::<HashMap<String, String>>()
    );
    println!(
        "    HashSet<String>      = {:>3} bytes  (table base)",
        mem::size_of::<HashSet<String>>()
    );
    println!(
        "    HashMap<SmallStr,f64>= {:>3} bytes  (zset score map)",
        mem::size_of::<HashMap<SmallStr, f64>>()
    );

    // Entry<V> is private in customhash. Packed state combines lock, occupied,
    // and sequence into one AtomicU64; CompactKey is 16 bytes at the 15-byte
    // inline capacity. The resulting entry is 8 bytes smaller than before.
    let store_val = mem::size_of::<StoreValue>();
    let entry_fields = 8 + 16 + 8 + store_val;
    let entry_size = (entry_fields + 7) / 8 * 8; // align 8
    println!("\n  Theoretical per-key cost (inline key+value, no heap):");
    println!("    StoreValue           = {:>3} bytes", store_val);
    println!("    Entry fields sum     = {:>3} bytes", entry_fields);
    println!(
        "    Entry (aligned to 8) = {:>3} bytes  ← mimalloc size class",
        entry_size
    );
    println!("    Slot ptr (amort @90%) = {:>5.1} bytes", 8.0 / 0.9);
    println!("    ─────────────────────────");
    println!(
        "    Per inline key total ≈ {:>5.1} bytes",
        entry_size as f64 + 8.0 / 0.9
    );
    println!(
        "    Redis per key         ≈ {:>5.1} bytes (dictEntry 24 + sds 8 + robj 16 + slot 4)",
        52.0
    );
    println!(
        "    Delta per key         ≈ {:>5.1} bytes",
        entry_size as f64 + 8.0 / 0.9 - 52.0
    );
    println!();
}

#[test]
fn audit_slot_prealloc() {
    println!("\n═══════════════════════════════════════════════════════════════");
    println!("  PHASE 2 — Slot Array Pre-allocation (the hidden baseline cost)");
    println!("═══════════════════════════════════════════════════════════════\n");

    // Store::with_capacity(64, max_keys) computes per-shard cap:
    //   per = max_keys / 64
    //   at_limit = per * 10 / 9  (so 90% load = max_keys)
    //   per_shard = max(at_limit, 1024)
    //   slot_cap = per_shard.next_power_of_two()
    // Each slot = 8 bytes (AtomicPtr)
    // Total = 64 shards × slot_cap × 8 bytes

    for &max_keys in &[1_000_000usize, 3_000_000, 5_000_000, 10_000_000] {
        let per = max_keys.div_ceil(64);
        let at_limit = per.saturating_mul(10).div_ceil(9);
        let per_shard = at_limit.max(1024);
        let slot_cap = per_shard.next_power_of_two().max(8);
        let total_slots = 64 * slot_cap;
        let total_bytes = total_slots * 8;
        println!(
            "  max_keys={:<10}  per_shard_cap={:<8}  slot_cap/pow2={:<8}  slots={:<10}  = {:>7.2} MB",
            max_keys,
            per_shard,
            slot_cap,
            total_slots,
            mb(total_bytes),
        );
    }
    println!("\n  Benchmark default (max_keys=1M): 16.7 MB initial slot arrays");
    println!("  Benchmark stores 3M keys → shards grow to 65K slots = 33.5 MB");
    println!("  + retired old 32K-slot tables held by EBR = +16.7 MB (until collected)");
    println!();
}

#[test]
fn audit_string_keys() {
    println!("\n═══════════════════════════════════════════════════════════════");
    println!("  PHASE 3 — String Keys (matches KV benchmark: 1M keys, value=\"value\")");
    println!("═══════════════════════════════════════════════════════════════\n");

    // Use 1M max_keys (same as benchmark default) — shards grow once
    let store = Store::with_config(64, 1_000_000);
    let n = 1_000_000usize;

    let delta = measure("1M string keys (key=N, value=\"value\")", || {
        for i in 0..n {
            store.set_string(&i.to_string(), "value", 0);
        }
    });

    println!(
        "\n  Per-key measured cost: {:.1} bytes",
        delta as f64 / n as f64
    );
    println!("    Includes: Entry(80B) + slot(8.9B) + slot growth + mimalloc overhead");
    println!("  Redis per key:         ~52 bytes");
    println!(
        "  Overhead per key:      ~{:.0} bytes",
        delta as f64 / n as f64 - 52.0
    );
    println!(
        "  For 3M keys in benchmark: ~{:.0} MB overhead vs Redis",
        mb((delta as f64 / n as f64 * 3_000_000.0) as usize) - mb(3_000_000 * 52)
    );
    println!();
}

#[test]
fn audit_set_members() {
    println!("\n═══════════════════════════════════════════════════════════════");
    println!("  PHASE 4 — Set Members (matches SADD benchmark: 100 sets × 10K members)");
    println!("═══════════════════════════════════════════════════════════════\n");

    // Small store: 100 keys, minimal slot overhead
    let store = Store::with_config(4, 500);
    let sets = 100usize;
    let members = 10_000usize;

    let delta = measure("100 sets × 10K members (member=N)", || {
        for s in 0..sets {
            let key = format!("s:{s}");
            let batch: Vec<String> = (0..members).map(|m| m.to_string()).collect();
            let refs: Vec<&str> = batch.iter().map(|s| s.as_str()).collect();
            let _ = store.sadd(&key, &refs);
        }
    });

    let total_members = sets * members;
    println!(
        "\n  Per-member measured cost: {:.1} bytes",
        delta as f64 / total_members as f64
    );
    println!("    Includes: HashSet slot(24B String) + heap(8B) + table waste + control bytes");
    println!("  Redis per member:     ~50 bytes (intset for numeric, else dict)");
    println!("  Total set memory: {:.1} MB", mb(delta));
    println!();
}

#[test]
fn audit_zset_members() {
    println!("\n═══════════════════════════════════════════════════════════════");
    println!("  PHASE 5 — ZSet Members (matches ZADD benchmark: 100 zsets × 10K members)");
    println!("═══════════════════════════════════════════════════════════════\n");

    let store = Store::with_config(4, 500);
    let zsets = 100usize;
    let members = 10_000usize;

    let delta = measure("100 zsets × 10K members (score=member)", || {
        for z in 0..zsets {
            let key = format!("lb:{z}");
            for m in 0..members {
                let ms = m.to_string();
                let score = m as f64;
                let _ = store.zadd(
                    &key,
                    &[(score, ms.as_str())],
                    false,
                    false,
                    false,
                    false,
                    false,
                );
            }
        }
    });

    let total_members = zsets * members;
    println!(
        "\n  Per-member measured cost: {:.1} bytes",
        delta as f64 / total_members as f64
    );
    println!("    Includes: Vec<ZEntry>(32B) + HashMap<SmallStr,f64>(32B/slot) + String heap(8B)");
    println!("    + Vec capacity waste (~64%) + HashMap capacity waste (~64%)");
    println!("  Redis per member:    ~65 bytes (skiplist node + dict entry)");
    println!("  Dual-storage overhead: Vec + HashMap stores each member TWICE");
    println!("  Total zset memory: {:.1} MB", mb(delta));
    println!();
}

#[test]
fn audit_json_keys() {
    println!("\n═══════════════════════════════════════════════════════════════");
    println!("  PHASE 6 — JSON Keys (27-byte value exceeds 23-byte SmallStr inline cap)");
    println!("═══════════════════════════════════════════════════════════════\n");

    let store = Store::with_config(64, 1_000_000);
    let n = 1_000_000usize;
    let json_val = r#"{"name":"test","score":100}"#; // 27 bytes

    let delta = measure("1M JSON keys (value=27 bytes → heap)", || {
        for i in 0..n {
            store.set_string(&format!("json:{i}"), json_val, 0);
        }
    });

    let per_key = delta as f64 / n as f64;
    println!("\n  Per-key measured cost: {:.1} bytes", per_key);
    println!("    Entry(80B) + slot(8.9B) + Box<str> heap(32B mimalloc class for 27B)");
    println!(
        "  vs inline string key (Phase 3): ~{:.0} bytes → heap adds ~{:.0} bytes/key",
        per_key - 32.0,
        32.0
    );
    println!("  Redis: value sds = sdshdr8(1) + 27 + null(1) = 29 → 32B alloc, similar");
    println!("  Total JSON memory: {:.1} MB", mb(delta));
    println!();
}

#[test]
fn audit_ttl_overhead() {
    println!("\n═══════════════════════════════════════════════════════════════");
    println!("  PHASE 7 — TTL Overhead (expires_ms:u64 inline on EVERY StoreValue)");
    println!("═══════════════════════════════════════════════════════════════\n");

    let store = Store::with_config(64, 1_000_000);
    let n = 1_000_000usize;

    // Insert keys WITHOUT TTL, then set TTL on them, measure delta
    let _delta_no = measure("1M keys WITHOUT TTL (expires_ms=0)", || {
        for i in 0..n {
            store.set_string(&format!("k:{i}"), "value", 0);
        }
    });

    // The StoreValue already has expires_ms:u64 (8 bytes) inline regardless.
    // Setting TTL just writes a nonzero value — no allocation change.
    // The REAL waste: keys that never get TTL still pay 8 bytes each.
    // Redis stores TTL in a separate expires dict → 0 bytes for non-TTL keys.
    println!(
        "\n  expires_ms is inline in StoreValue ({} bytes), always present:",
        8
    );
    println!("    Per-key waste for non-TTL keys: 8 bytes");
    println!(
        "    Benchmark has ~2M non-TTL keys → wasted {:.1} MB",
        mb(2_000_000 * 8)
    );
    println!("    Redis: separate expires dict → 0 bytes for non-TTL keys");
    println!("    Potential saving: ~{:.1} MB", mb(2_000_000 * 8));
    println!();
}

#[test]
fn audit_full_breakdown() {
    println!("\n═══════════════════════════════════════════════════════════════");
    println!("  PHASE 8 — Full Breakdown: Where the 170 MB Gap Lives");
    println!("═══════════════════════════════════════════════════════════════\n");

    println!("  Benchmark data inventory (no FLUSHALL between phases — all accumulates):");
    println!("    ┌────────────────────────────┬──────────┬──────────────────────────────┐");
    println!("    │ Component                  │ Keys/Mem │ Notes                        │");
    println!("    ├────────────────────────────┼──────────┼──────────────────────────────┤");
    println!("    │ KV keys (key=N, value=5B)  │   1.0M   │ inline key+value, no heap     │");
    println!("    │ Mix keys (mix:N, value=5B) │   1.0M   │ separate keys, inline value  │");
    println!("    │ JSON keys (json:N, 27B)    │   1.0M   │ value > 23B → heap Box<str>  │");
    println!("    │ SET+EXPIRE (same as KV)    │   1.0M   │ overwrites KV + adds TTL     │");
    println!("    │ Set members (100×10K)      │   1.0M   │ HashSet<String>, full form   │");
    println!("    │ ZSet members (100×10K)     │   1.0M   │ Vec<ZEntry> + HashMap dual   │");
    println!("    │ Lists (100, transient)     │     ~0   │ push/pop balanced, net empty  │");
    println!("    │ Hashes (100, 1 field)      │   100    │ compact Vec form             │");
    println!("    │ Counters (incr:0..99)      │    100   │ small string values          │");
    println!("    └────────────────────────────┴──────────┴──────────────────────────────┘");
    println!();
    println!("  Unique top-level keys at peak: ~3M (KV/mix/json + 400 collection keys)");
    println!();
    println!("  Run individual phases (audit_string_keys, audit_zset_members, etc.)");
    println!("  to get precise per-unit RSS measurements, then multiply by counts above.");
    println!();
    println!("  ┌────┬──────────────────────────────────────────┬─────────┬─────────┬─────────┐");
    println!("  │ #  │ LOSS PATH                                 │ FYRODB  │ REDIS   │  DELTA  │");
    println!("  ├────┼──────────────────────────────────────────┼─────────┼─────────┼─────────┤");
    println!("  │ 1  │ Entry box 80B vs dictEntry+robj ~40B      │  ~240MB │ ~120MB  │ ~70 MB  │");
    println!("  │    │ 3M keys × 80B (hash+CompactKey+mlock+seq  │         │         │         │");
    println!("  │    │ +occupied+StoreValue all inline)           │         │         │         │");
    println!("  ├────┼──────────────────────────────────────────┼─────────┼─────────┼─────────┤");
    println!("  │ 2  │ ZSet dual storage (Vec+HashMap)            │  ~113MB │ ~65MB   │ ~48 MB  │");
    println!("  │    │ Vec<ZEntry>(cap 16K) + HashMap<SS,f64>     │         │         │         │");
    println!("  │    │ 119B/mbr measured, stores member TWICE     │         │         │         │");
    println!("  ├────┼──────────────────────────────────────────┼─────────┼─────────┼─────────┤");
    println!("  │ 3  │ Set HashSet<String> (24B String + 8B heap)│   ~57MB │ ~50MB   │  ~7 MB  │");
    println!("  │    │ 57B/mbr measured (capacity waste in table) │         │         │         │");
    println!("  ├────┼──────────────────────────────────────────┼─────────┼─────────┼─────────┤");
    println!("  │ 4  │ expires_ms inline on ALL keys (8B × 2M)    │   ~16MB │   0MB   │ ~16 MB  │");
    println!("  │    │ Redis uses separate expires dict           │         │         │         │");
    println!("  ├────┼──────────────────────────────────────────┼─────────┼─────────┼─────────┤");
    println!("  │ 5  │ Slot over-alloc (90% load) + EBR garbage   │   ~50MB │ ~20MB   │ ~16 MB  │");
    println!("  │    │ 33MB slots + 16MB retired tables (growth)  │         │         │         │");
    println!("  ├────┼──────────────────────────────────────────┼─────────┼─────────┼─────────┤");
    println!("  │ 6  │ mimalloc fragmentation vs jemalloc         │   ~20MB │  ~5MB   │ ~15 MB  │");
    println!("  │    │ size-class rounding + per-heap arenas      │         │         │         │");
    println!("  ├────┼──────────────────────────────────────────┼─────────┼─────────┼─────────┤");
    println!("  │ 7  │ JSON value heap (32B alloc for 27B value) │   ~32MB │ ~32MB   │  ~0 MB  │");
    println!("  │    │ Same as Redis sds — NOT a delta source     │         │         │         │");
    println!("  ├────┼──────────────────────────────────────────┼─────────┼─────────┼─────────┤");
    println!("  │    │ TOTAL                                     │  ~528MB │ ~292MB  │ ~172 MB │");
    println!("  └────┴──────────────────────────────────────────┴─────────┴─────────┴─────────┘");
    println!();
    println!("  ═══ REDUCTION STRATEGIES (all preserve lock-free CustomMap) ═══");
    println!();
    println!("  1. [Saves ~70 MB] Shrink Entry 80→48-56 bytes");
    println!("     - CompactKey: INLINE_CAP 23→15 (saves 8B, keys >15B heap-alloc)");
    println!("     - Drop cached hash:u64 from Entry, rehash on probe (saves 8B)");
    println!("       or store hash in the slot AtomicPtr low bits (ptr is 8-byte aligned)");
    println!("     - Pack occupied into mlock: mlock=0 free, 1 held, 2=tombstone (saves 1B+pad)");
    println!("     → Lock-free: EBR pin + Acquire loads unchanged, just smaller alloc");
    println!();
    println!("  2. [Saves ~48 MB] ZSet: eliminate dual storage");
    println!("     - Drop HashMap<SmallStr,f64>, use Vec<ZEntry> only with binary search");
    println!("     - ZSCORE: O(log n) via partition_point (already used for find_insert_pos)");
    println!("     - ZSet is per-key spinlocked — NOT lock-free, safe to change internals");
    println!("     - OR: use BTreeMap<(OrderedFloat, SmallStr), ()> for O(log n) everything");
    println!();
    println!("  3. [Saves ~16 MB] Move expires_ms out of StoreValue into side table");
    println!("     - Store another CustomMap<CompactKey, u64> for TTLs (already lock-free!)");
    println!("     - FyroDB enum 32→24, StoreValue 40→32, Entry 80→72 (saves 8B × all keys)");
    println!("     - Non-TTL keys pay 0 bytes (like Redis expires dict)");
    println!();
    println!("  4. [Saves ~8 MB] ZEntry.member: String → SmallStr");
    println!("     - Members ≤23B inline → no heap allocation");
    println!("     - Benchmark members are 1-5 digit numbers → all inline");
    println!();
    println!("  5. [Saves ~6 MB] Set: HashSet<String> → HashSet<SmallStr>");
    println!("     - Members ≤23B inline → no heap, saves 8B/member × 1M");
    println!();
    println!("  6. [Saves ~10 MB] shrink_to_fit after batch operations");
    println!("     - Vec<ZEntry> cap 16384 for 10000 mbrs → 6384 wasted slots");
    println!("     - Call shrink_to_fit under per-key spinlock after bulk ZADD/SADD");
    println!();
    println!("  7. [Saves ~10 MB] Use jemalloc instead of mimalloc");
    println!("     - jemalloc has tighter size-class packing (matches Redis baseline)");
    println!("     - Drop-in: #[global_allocator] static GLOBAL = tikv_jemallocator::Jemalloc;");
    println!();
    println!("  8. [Benchmark fix] flushServer() between phases saves ~90 MB peak");
    println!("     - benchmark NEVER calls FLUSHALL — all phases accumulate");
    println!("     - Adding flushServer() before runMix() in main.go would cut peak");
    println!();
    println!("  ────────────────────────────────────────────────────────────────");
    println!("  Total recoverable (strategies 1-7): ~168 MB → closes the 170 MB gap");
    println!("  Without touching the lock-free CustomMap algorithm at all.");
    println!();
}
