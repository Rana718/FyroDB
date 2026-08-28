mod ebr;
mod key;
mod ops;
mod shard;

pub use ebr::{force_collect, force_collect_quiescent};

use std::marker::PhantomData;
use std::ops::Deref;
use std::sync::atomic::{AtomicUsize, Ordering};

use crossbeam_utils::CachePadded;
use foldhash::fast::RandomState;
use std::hash::BuildHasher;

use shard::*;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Full;

impl std::fmt::Display for Full {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("command not allowed when used memory > 'maxmemory'.")
    }
}
impl std::error::Error for Full {}

pub struct ValueRef<'a, V> {
    ptr: *const V,
    _guard: ebr::Guard,
    _map: PhantomData<&'a CustomMap<V>>,
}

impl<V> Deref for ValueRef<'_, V> {
    type Target = V;
    #[inline(always)]
    fn deref(&self) -> &V {
        unsafe { &*self.ptr }
    }
}

pub struct CustomMap<V> {
    shards: Box<[Shard<V>]>,
    shift: u32,
    shard_mask: usize,
    hasher: RandomState,
    key_count: CachePadded<AtomicUsize>,
    max_keys: usize,
}

impl<V: Clone + Send + Sync + 'static> CustomMap<V> {
    pub fn with_shards(n: usize) -> Self {
        Self::build(
            n.next_power_of_two().max(1),
            DEFAULT_SHARD_CAPACITY,
            usize::MAX,
        )
    }

    pub fn with_capacity(shard_count: usize, expected_keys: usize) -> Self {
        let n = shard_count.next_power_of_two().max(1);
        // `expected_keys` is a capacity limit, not a reason to reserve the
        // complete table up front. Lazy growth keeps idle RSS small and still
        // uses the same lock-free shard growth path under load.
        let _ = expected_keys;
        Self::build(n, INITIAL_SHARD_CAPACITY, expected_keys)
    }

    fn build(n: usize, per_shard: usize, max_keys: usize) -> Self {
        CustomMap {
            shift: (u64::BITS - n.trailing_zeros()).min(63),
            shard_mask: n - 1,
            shards: (0..n)
                .map(|_| Shard::new(per_shard))
                .collect::<Vec<_>>()
                .into_boxed_slice(),
            hasher: RandomState::default(),
            key_count: CachePadded::new(AtomicUsize::new(0)),
            max_keys,
        }
    }

    #[inline(always)]
    fn locate(&self, key: &str) -> (u64, usize) {
        let h = self.hasher.hash_one(key);
        (h, ((h >> self.shift) as usize) & self.shard_mask)
    }

    #[inline(always)]
    pub fn locate_key(&self, key: &str) -> (u64, usize) {
        self.locate(key)
    }

    #[inline]
    pub fn with_entry<R>(
        &self,
        key: &str,
        hash: u64,
        idx: usize,
        f: impl FnOnce(&V) -> R,
    ) -> Option<R> {
        ebr::with_pin(|_| {
            let entry = unsafe { self.shards.get_unchecked(idx) }.find(key, hash)?;
            if !state_occupied(&entry.state, Ordering::Acquire) {
                return None;
            }
            Some(f(unsafe { (*entry.value.get()).assume_init_ref() }))
        })
    }

    #[inline]
    pub fn contains_key(&self, key: &str) -> bool {
        let (h, idx) = self.locate(key);
        ebr::with_pin(|_| {
            unsafe { self.shards.get_unchecked(idx) }
                .find(key, h)
                .is_some_and(|e| state_occupied(&e.state, Ordering::Acquire))
        })
    }

    #[inline]
    pub fn get(&self, key: &str) -> Option<V> {
        let (h, idx) = self.locate(key);
        ebr::with_pin(|_| {
            let e = unsafe { self.shards.get_unchecked(idx) }.find(key, h)?;
            if !state_occupied(&e.state, Ordering::Acquire) {
                return None;
            }
            Some(unsafe { (*e.value.get()).assume_init_ref().clone() })
        })
    }

    #[inline]
    pub fn get_ref(&self, key: &str) -> Option<ValueRef<'_, V>> {
        let (h, idx) = self.locate(key);
        let guard = ebr::pin();
        let e = unsafe { self.shards.get_unchecked(idx) }.find(key, h)?;
        if !state_occupied(&e.state, Ordering::Acquire) {
            return None;
        }
        Some(ValueRef {
            ptr: unsafe { (*e.value.get()).as_ptr() },
            _guard: guard,
            _map: PhantomData,
        })
    }

    #[inline]
    pub fn insert(&self, key: String, value: V) -> bool {
        let (h, idx) = self.locate(&key);
        let is_new = unsafe { self.shards.get_unchecked(idx) }.insert_hashed(key, value, h);
        if is_new {
            self.key_count.fetch_add(1, Ordering::Relaxed);
        }
        is_new
    }

    #[inline]
    pub fn try_insert(&self, key: String, value: V) -> Result<bool, Full> {
        if self.key_count.load(Ordering::Relaxed) >= self.max_keys {
            let (h, idx) = self.locate(&key);
            let exists = ebr::with_pin(|_| {
                unsafe { self.shards.get_unchecked(idx) }
                    .find(&key, h)
                    .is_some_and(|e| state_occupied(&e.state, Ordering::Acquire))
            });
            if !exists {
                return Err(Full);
            }
        }
        Ok(self.insert(key, value))
    }

    #[inline]
    pub fn set(&self, key: &str, value: V, key_owned: impl FnOnce() -> String) -> bool {
        let (h, idx) = self.locate(key);
        let _guard = ebr::pin();
        let shard = unsafe { self.shards.get_unchecked(idx) };
        if let Some(existing) = shard.find(key, h) {
            state_lock(&existing.state);
            // Re-check under the lock: the pre-lock occupancy test can race a
            // concurrent remove, and dropping an already-dropped value would
            // be a double free.
            let was_occupied = state_occupied(&existing.state, Ordering::Relaxed);
            write_begin(&existing.state);
            if was_occupied {
                unsafe { (*existing.value.get()).assume_init_drop() };
            }
            unsafe { (*existing.value.get()).write(value) };
            write_end(&existing.state, true);
            if was_occupied {
                return false;
            }
            self.key_count.fetch_add(1, Ordering::Relaxed);
            return true;
        }
        let is_new = shard.insert_new(key_owned(), value, h);
        if is_new {
            self.key_count.fetch_add(1, Ordering::Relaxed);
        }
        is_new
    }

    #[inline]
    pub fn try_set(
        &self,
        key: &str,
        value: V,
        key_owned: impl FnOnce() -> String,
    ) -> Result<bool, Full> {
        if self.key_count.load(Ordering::Relaxed) >= self.max_keys {
            let (h, idx) = self.locate(key);
            let exists = ebr::with_pin(|_| {
                unsafe { self.shards.get_unchecked(idx) }
                    .find(key, h)
                    .is_some_and(|e| state_occupied(&e.state, Ordering::Acquire))
            });
            if !exists {
                return Err(Full);
            }
        }
        Ok(self.set(key, value, key_owned))
    }

    #[inline]
    pub fn remove(&self, key: &str) -> Option<V> {
        let (h, idx) = self.locate(key);
        let _guard = ebr::pin();
        let entry = unsafe { self.shards.get_unchecked(idx) }.find(key, h)?;
        if !state_occupied(&entry.state, Ordering::Acquire) {
            return None;
        }
        state_lock(&entry.state);
        if !state_occupied(&entry.state, Ordering::Relaxed) {
            state_unlock(&entry.state);
            return None;
        }
        write_begin(&entry.state);
        let value = unsafe { (*entry.value.get()).assume_init_ref().clone() };
        unsafe { (*entry.value.get()).assume_init_drop() };
        write_end(&entry.state, false);
        self.key_count.fetch_sub(1, Ordering::Relaxed);
        Some(value)
    }

    #[inline]
    pub fn remove_no_clone(&self, key: &str) -> bool {
        let (h, idx) = self.locate(key);
        let _guard = ebr::pin();
        let entry = match unsafe { self.shards.get_unchecked(idx) }.find(key, h) {
            Some(e) => e,
            None => return false,
        };
        if !state_occupied(&entry.state, Ordering::Acquire) {
            return false;
        }
        state_lock(&entry.state);
        if !state_occupied(&entry.state, Ordering::Relaxed) {
            state_unlock(&entry.state);
            return false;
        }
        write_begin(&entry.state);
        unsafe { (*entry.value.get()).assume_init_drop() };
        write_end(&entry.state, false);
        self.key_count.fetch_sub(1, Ordering::Relaxed);
        true
    }

    #[inline]
    pub fn remove_with<R>(&self, key: &str, f: impl FnOnce(&V) -> R) -> Option<R> {
        let (h, idx) = self.locate(key);
        let _guard = ebr::pin();
        let entry = unsafe { self.shards.get_unchecked(idx) }.find(key, h)?;
        if !state_occupied(&entry.state, Ordering::Acquire) {
            return None;
        }
        state_lock(&entry.state);
        if !state_occupied(&entry.state, Ordering::Relaxed) {
            state_unlock(&entry.state);
            return None;
        }
        let result = f(unsafe { (*entry.value.get()).assume_init_ref() });
        write_begin(&entry.state);
        unsafe { (*entry.value.get()).assume_init_drop() };
        write_end(&entry.state, false);
        self.key_count.fetch_sub(1, Ordering::Relaxed);
        Some(result)
    }

    #[inline]
    pub fn update<R>(&self, key: &str, mut f: impl FnMut(&V) -> (V, R)) -> Option<R> {
        let (h, idx) = self.locate(key);
        let _guard = ebr::pin();
        let entry = unsafe { self.shards.get_unchecked(idx) }.find(key, h)?;

        state_lock(&entry.state);
        if !state_occupied(&entry.state, Ordering::Relaxed) {
            state_unlock(&entry.state);
            return None;
        }
        let (new_val, result) = f(unsafe { (*entry.value.get()).assume_init_ref() });
        write_begin(&entry.state);
        unsafe { (*entry.value.get()).assume_init_drop() };
        unsafe { (*entry.value.get()).write(new_val) };
        write_end(&entry.state, true);
        Some(result)
    }

    #[inline]
    pub fn try_update<R>(&self, key: &str, mut f: impl FnMut(&V) -> Option<(V, R)>) -> Option<R> {
        let (h, idx) = self.locate(key);
        let _guard = ebr::pin();
        let entry = unsafe { self.shards.get_unchecked(idx) }.find(key, h)?;

        state_lock(&entry.state);
        if !state_occupied(&entry.state, Ordering::Relaxed) {
            state_unlock(&entry.state);
            return None;
        }
        let result = f(unsafe { (*entry.value.get()).assume_init_ref() });
        match result {
            Some((new_val, r)) => {
                write_begin(&entry.state);
                unsafe { (*entry.value.get()).assume_init_drop() };
                unsafe { (*entry.value.get()).write(new_val) };
                write_end(&entry.state, true);
                Some(r)
            }
            None => {
                state_unlock(&entry.state);
                None
            }
        }
    }

    #[inline]
    pub fn update_with<R>(&self, key: &str, mut f: impl FnMut(&mut V) -> R) -> Option<R> {
        let (h, idx) = self.locate(key);
        let _guard = ebr::pin();
        let entry = unsafe { self.shards.get_unchecked(idx) }.find(key, h)?;

        state_lock(&entry.state);
        if !state_occupied(&entry.state, Ordering::Relaxed) {
            state_unlock(&entry.state);
            return None;
        }
        write_begin(&entry.state);
        let result = f(unsafe { (*entry.value.get()).assume_init_mut() });
        write_end(&entry.state, true);
        Some(result)
    }

    #[inline]
    pub fn get_locked<R>(&self, key: &str, f: impl FnOnce(&V) -> R) -> Option<R> {
        let (h, idx) = self.locate(key);
        let _guard = ebr::pin();
        let entry = unsafe { self.shards.get_unchecked(idx) }.find(key, h)?;

        state_lock(&entry.state);
        if !state_occupied(&entry.state, Ordering::Relaxed) {
            state_unlock(&entry.state);
            return None;
        }
        let result = f(unsafe { (*entry.value.get()).assume_init_ref() });
        state_unlock(&entry.state);
        Some(result)
    }

    #[inline]
    pub fn read_consistent<R>(&self, key: &str, f: impl Fn(&V) -> R) -> Option<R> {
        let (h, idx) = self.locate(key);
        let _guard = ebr::pin();
        let entry = unsafe { self.shards.get_unchecked(idx) }.find(key, h)?;

        loop {
            let seq1 = state_seq(entry.state.load(Ordering::Acquire));
            if seq1 & 1 != 0 {
                std::hint::spin_loop();
                continue;
            }
            if !state_occupied(&entry.state, Ordering::Acquire) {
                return None;
            }
            let result = f(unsafe { (*entry.value.get()).assume_init_ref() });
            std::sync::atomic::fence(Ordering::Acquire);
            let seq2 = state_seq(entry.state.load(Ordering::Acquire));
            if seq1 == seq2 {
                return Some(result);
            }
        }
    }

    #[inline]
    pub fn insert_if_absent(&self, key: String, value: V) -> bool {
        let (h, idx) = self.locate(&key);
        let _guard = ebr::pin();
        let shard = unsafe { self.shards.get_unchecked(idx) };
        if let Some(entry) = shard.find(&key, h) {
            if state_occupied(&entry.state, Ordering::Acquire) {
                return false;
            }
            state_lock(&entry.state);
            if state_occupied(&entry.state, Ordering::Relaxed) {
                state_unlock(&entry.state);
                return false;
            }
            write_begin(&entry.state);
            unsafe { (*entry.value.get()).write(value) };
            write_end(&entry.state, true);
            self.key_count.fetch_add(1, Ordering::Relaxed);
            return true;
        }
        let is_new = shard.insert_new(key, value, h);
        if is_new {
            self.key_count.fetch_add(1, Ordering::Relaxed);
        }
        is_new
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.key_count.load(Ordering::Relaxed)
    }

    /// Whether the configured key ceiling has been reached.
    ///
    /// Costs a single relaxed load, and is trivially false when no limit is
    /// configured, so callers can gate every write on it.
    #[inline(always)]
    pub fn is_full(&self) -> bool {
        self.max_keys != usize::MAX && self.key_count.load(Ordering::Relaxed) >= self.max_keys
    }

    /// The configured key ceiling, or `usize::MAX` when unlimited.
    #[inline]
    pub fn capacity_limit(&self) -> usize {
        self.max_keys
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

unsafe impl<V: Send + Sync> Sync for CustomMap<V> {}
unsafe impl<V: Send + Sync> Send for CustomMap<V> {}

#[cfg(test)]
mod tests {
    use super::CustomMap;
    use std::collections::HashSet;
    use std::sync::Arc;

    #[test]
    fn bounded_retain_covers_each_slot_once() {
        let map = CustomMap::with_capacity(1, 16);
        for i in 0..16 {
            map.insert(format!("key-{i}"), i);
        }

        let mut cursor = 0usize;
        let mut seen = HashSet::new();
        loop {
            let (next, capacity) = map
                .retain_shard_range(0, cursor, 17, |key, value| {
                    assert!(seen.insert(key.to_owned()));
                    value % 2 == 0
                })
                .unwrap();
            if next == capacity {
                break;
            }
            assert!(next > cursor);
            cursor = next;
        }

        assert_eq!(seen.len(), 16);
        assert_eq!(map.len(), 8);
        for i in 0..16 {
            assert_eq!(map.contains_key(&format!("key-{i}")), i % 2 == 0);
        }
    }

    #[test]
    fn concurrent_readers_survive_repeated_growth() {
        let map = Arc::new(CustomMap::with_capacity(1, 1));
        for i in 0..128 {
            map.insert(format!("stable-{i}"), i);
        }

        std::thread::scope(|scope| {
            for _ in 0..4 {
                let map = Arc::clone(&map);
                scope.spawn(move || {
                    for _ in 0..20_000 {
                        for i in 0..128 {
                            assert_eq!(map.get(&format!("stable-{i}")), Some(i));
                        }
                    }
                });
            }

            for writer in 0..4 {
                let map = Arc::clone(&map);
                scope.spawn(move || {
                    for i in 0..2_000 {
                        let key = format!("writer-{writer}-{i}");
                        assert!(map.insert(key, i));
                    }
                });
            }
        });

        assert_eq!(map.len(), 8_128);
        for writer in 0..4 {
            for i in 0..2_000 {
                assert_eq!(map.get(&format!("writer-{writer}-{i}")), Some(i));
            }
        }
    }

    #[test]
    fn concurrent_clear_read_write_is_safe() {
        let map = Arc::new(CustomMap::with_capacity(8, 100_000));
        std::thread::scope(|scope| {
            for worker in 0..4 {
                let map = Arc::clone(&map);
                scope.spawn(move || {
                    for i in 0..50_000 {
                        let key = format!("key-{worker}-{}", i % 2_000);
                        map.set(&key, i, || key.clone());
                        let _ = map.get(&key);
                    }
                });
            }

            let map = Arc::clone(&map);
            scope.spawn(move || {
                for _ in 0..100 {
                    map.clear();
                    std::thread::yield_now();
                }
            });
        });

        map.clear();
        assert_eq!(map.len(), 0);
    }

    #[test]
    fn repeated_clear_reclaims_without_invalidating_new_tables() {
        let map = CustomMap::with_capacity(8, 10_000);
        for round in 0..100 {
            for i in 0..2_000 {
                map.insert(format!("round-{round}-{i}"), i);
            }
            map.clear();
            assert!(map.is_empty());
            assert!(!map.contains_key("round-0-0"));
        }
    }

    /// `set` and `insert_hashed` used to test occupancy *before* taking the
    /// entry lock and then unconditionally drop the old value, so a remove
    /// landing in that window caused a double free. Values here own a heap
    /// allocation so the allocator catches it.
    #[test]
    fn concurrent_overwrite_and_remove_never_double_frees() {
        let map = Arc::new(CustomMap::<String>::with_capacity(4, 4_096));
        const KEYS: usize = 64;

        std::thread::scope(|scope| {
            for writer in 0..4 {
                let map = Arc::clone(&map);
                scope.spawn(move || {
                    for round in 0..20_000 {
                        let key = format!("key-{}", round % KEYS);
                        let payload = format!("value-{writer}-{round}-aaaaaaaaaaaaaaaaaaaaaaaa");
                        if round % 2 == 0 {
                            map.set(&key, payload, || key.clone());
                        } else {
                            map.insert(key, payload);
                        }
                    }
                });
            }
            for _ in 0..2 {
                let map = Arc::clone(&map);
                scope.spawn(move || {
                    for round in 0..20_000 {
                        let key = format!("key-{}", round % KEYS);
                        map.remove_no_clone(&key);
                    }
                });
            }
            for _ in 0..2 {
                let map = Arc::clone(&map);
                scope.spawn(move || {
                    for round in 0..20_000 {
                        let key = format!("key-{}", round % KEYS);
                        // Reads must never observe a torn or freed value.
                        if let Some(value) = map.read_consistent(&key, |v| v.clone()) {
                            assert!(value.starts_with("value-"), "torn read: {value:?}");
                        }
                    }
                });
            }
        });

        assert!(map.len() <= KEYS);
    }

    /// `key_count` has to stay in step with what is actually reachable, across
    /// the tombstone-revival path that `set` and `insert_if_absent` share.
    #[test]
    fn key_count_survives_insert_remove_revive_cycles() {
        let map = CustomMap::<u64>::with_capacity(2, 1_024);
        for round in 0..500u64 {
            for i in 0..32u64 {
                let key = format!("key-{i}");
                map.set(&key, round, || key.clone());
            }
            assert_eq!(map.len(), 32, "after set round {round}");
            for i in 0..32u64 {
                assert!(map.remove_no_clone(&format!("key-{i}")));
            }
            assert_eq!(map.len(), 0, "after remove round {round}");
            for i in 0..32u64 {
                assert!(map.insert_if_absent(format!("key-{i}"), round));
            }
            assert_eq!(map.len(), 32, "after revive round {round}");
            for i in 0..32u64 {
                assert!(map.remove_no_clone(&format!("key-{i}")));
            }
        }
        assert_eq!(map.len(), 0);
    }
}
