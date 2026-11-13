# customhash

A generic, growable sharded map used by FyroDB for mutable values. Table probing uses atomic slots and tagged pointers; value access is protected by entry read/write guards and map read/write admission. Epoch-based reclamation keeps retired tables and entries alive while pinned.

## Public API

```rust
use customhash::CustomMap;

let map = CustomMap::with_capacity(8, 1_000_000);
assert!(map.insert("key".to_owned(), "value".to_owned()));
assert_eq!(map.get("key").as_deref(), Some("value"));
let value = map.get_ref("key").unwrap();
assert_eq!(value.as_str(), "value");
```

`get` returns a cloned value; `get_ref` returns a guarded reference retaining the entry read guard and EBR pin. Keep guarded references short-lived because a writer to the same entry may wait for them.

## Concurrency and safety

- Atomic slot probing does not make the full mutable-value path lock-free or wait-free. Readers and writers may retry, spin, yield or wait for admission.
- Writers enter the owning map's write epoch and hold the entry write lock while mutating. Snapshot pre-images are captured before the mutation.
- A successful `try_read_epoch` excludes registered map writers for a bounded read-only batch. Raw entry/table pointers additionally require an EBR pin; the admission guard does not replace reclamation protection.
- Ordinary reads use entry read guards. EBR alone does not make unsynchronized access to String, Vec or other mutable values safe.
- Same-thread nested write epochs preserve registration until the last guard exits, including nested maps and out-of-order drops. Mutation inside a protected read epoch is rejected.
- Inserts coordinate with per-shard growth; growth/compaction uses a mutex and retired table publication uses EBR. Removed/tombstoned capacity can be reused or compacted; this is not an immutable-slot map.
- FyroDB's Lua/MULTI/EXEC isolation and WATCH registry live above this crate. A generic map operation does not by itself make a multi-key transfer atomic.

## Layout and memory

On the current 64-bit target CompactKey is 16 bytes, with fifteen inline key bytes and a special encoding for eligible sixteen-byte keys. FyroDB's SmallStr is separately 24 bytes with twenty-three inline value bytes; StoreValue is 32 bytes and Entry<StoreValue> is 56 bytes. These are asserted layouts, excluding allocator rounding and external payload storage.

Empty shards start with eight slots and grow at the configured load threshold. A 14-bit hash tag plus a marker occupies eligible pointer bits; addresses that use the reserved bits fall back to untagged storage.

See [the architecture](../../ARCHITECTURE.md) for the protection layers and [benchmarks](../../BENCHMARKS.md) for TCP, engine, payload, memory and rejected-experiment evidence. The regression suite is `cargo test -p customhash`; admission tests include concurrent readers/writers, nested maps, out-of-order drops and unwinding.
