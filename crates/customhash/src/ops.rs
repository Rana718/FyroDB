use std::sync::atomic::Ordering;

use super::CustomMap;
use super::ebr;
use super::shard::*;

impl<V: Clone + Send + Sync + 'static> CustomMap<V> {
    pub fn for_each(&self, mut f: impl FnMut(&str, &V)) {
        for shard in self.shards.iter() {
            let _guard = ebr::pin();
            let t = shard.table();
            for slot in t.slots().iter() {
                let p = slot.load(Ordering::Acquire);
                if p.is_null() {
                    continue;
                }
                let e = unsafe { &*p };
                if state_occupied(&e.state, Ordering::Acquire) {
                    f(e.key.as_str(), unsafe {
                        (*e.value.get()).assume_init_ref()
                    });
                }
            }
        }
    }

    /// Rebuild live values in place under their entry locks. This is the
    /// ownership-safe equivalent of Redis active defrag for Rust values: the
    /// entry address never moves, so pinned lock-free readers remain valid,
    /// while fragmented child allocations are replaced and reclaimed later by
    /// EBR.
    pub fn defragment_values(&self, budget: usize, mut rebuild: impl FnMut(&mut V)) -> usize {
        let keys = self.keys();
        let mut rebuilt = 0;
        for key in keys.into_iter().take(budget) {
            if self
                .update_with(&key, |value| {
                    rebuild(value);
                })
                .is_some()
            {
                rebuilt += 1;
            }
        }
        rebuilt
    }

    pub fn keys(&self) -> Vec<String> {
        let mut out = Vec::new();
        self.for_each(|k, _| out.push(k.to_owned()));
        out
    }

    pub fn retain(&self, mut f: impl FnMut(&str, &V) -> bool) {
        for shard in self.shards.iter() {
            let _guard = ebr::pin();
            let t = shard.table();
            for slot in t.slots().iter() {
                let p = slot.load(Ordering::Acquire);
                if p.is_null() {
                    continue;
                }
                let entry = unsafe { &*p };
                if !state_occupied(&entry.state, Ordering::Acquire) {
                    continue;
                }
                if !f(entry.key.as_str(), unsafe {
                    (*entry.value.get()).assume_init_ref()
                }) {
                    state_lock(&entry.state);
                    if state_occupied(&entry.state, Ordering::Relaxed) {
                        state_seq_add(&entry.state, Ordering::Release);
                        unsafe { (*entry.value.get()).assume_init_drop() };
                        state_set_occupied(&entry.state, false, Ordering::Release);
                        state_seq_add(&entry.state, Ordering::Release);
                        self.key_count.fetch_sub(1, Ordering::Relaxed);
                    }
                    state_unlock(&entry.state);
                }
            }
        }
    }

    pub fn retain_shard(&self, shard_idx: usize, mut f: impl FnMut(&str, &V) -> bool) {
        let _ = self.retain_shard_range(shard_idx, 0, usize::MAX, |key, value| f(key, value));
    }

    pub fn retain_shard_range(
        &self,
        shard_idx: usize,
        start_slot: usize,
        max_slots: usize,
        mut f: impl FnMut(&str, &V) -> bool,
    ) -> Option<(usize, usize)> {
        let Some(shard) = self.shards.get(shard_idx) else {
            return None;
        };
        let _guard = ebr::pin();
        let t = shard.table();
        let capacity = t.capacity();
        let start = start_slot.min(capacity);
        let end = start.saturating_add(max_slots).min(capacity);
        for slot in &t.slots()[start..end] {
            let p = slot.load(Ordering::Acquire);
            if p.is_null() {
                continue;
            }
            let entry = unsafe { &*p };
            if !state_occupied(&entry.state, Ordering::Acquire) {
                continue;
            }
            if f(entry.key.as_str(), unsafe {
                (*entry.value.get()).assume_init_ref()
            }) {
                continue;
            }
            state_lock(&entry.state);
            if state_occupied(&entry.state, Ordering::Relaxed) {
                state_seq_add(&entry.state, Ordering::Release);
                unsafe { (*entry.value.get()).assume_init_drop() };
                state_set_occupied(&entry.state, false, Ordering::Release);
                state_seq_add(&entry.state, Ordering::Release);
                self.key_count.fetch_sub(1, Ordering::Relaxed);
            }
            state_unlock(&entry.state);
        }
        Some((end, capacity))
    }

    pub fn compact_shard(&self, shard_idx: usize) {
        let Some(shard) = self.shards.get(shard_idx) else {
            return;
        };
        let _lock = shard.grow_lock.lock().unwrap_or_else(|e| e.into_inner());
        while shard
            .insert_gate
            .compare_exchange_weak(0, GROWING, Ordering::AcqRel, Ordering::Relaxed)
            .is_err()
        {
            std::hint::spin_loop();
        }

        let old_ptr = shard.table.load(Ordering::Acquire);
        let old_table = unsafe { &*old_ptr };

        let mut live_count: usize = 0;
        for slot in old_table.slots().iter() {
            let p = slot.load(Ordering::Acquire);
            if !p.is_null() && state_occupied(&unsafe { &*p }.state, Ordering::Acquire) {
                live_count += 1;
            }
        }

        let new_cap = ((live_count * LOAD_DEN / LOAD_NUM) + 1)
            .next_power_of_two()
            .max(8);
        if new_cap >= old_table.capacity() {
            shard.insert_gate.store(0, Ordering::Release);
            return;
        }

        let new_table = Box::new(SlotTable::<V>::new(new_cap));
        for slot in old_table.slots().iter() {
            let p = slot.load(Ordering::Acquire);
            if p.is_null() {
                continue;
            }
            let e = unsafe { &*p };
            if !state_occupied(&e.state, Ordering::Acquire) {
                unsafe { ebr::retire_raw(p.cast(), drop_raw_entry::<V>) };
                continue;
            }
            let mut i = (e.hash as usize) & new_table.mask;
            loop {
                let new_slot = unsafe { new_table.slots().get_unchecked(i) };
                if new_slot.load(Ordering::Relaxed).is_null() {
                    new_slot.store(p, Ordering::Relaxed);
                    break;
                }
                i = (i + 1) & new_table.mask;
            }
        }

        shard.len.store(live_count, Ordering::Relaxed);
        let new_ptr = alloc_table(*new_table);
        shard.table.store(new_ptr, Ordering::Release);
        unsafe { ebr::retire_raw(old_ptr.cast(), drop_raw_table::<V>) };
        shard.insert_gate.store(0, Ordering::Release);
    }

    pub fn clear(&self) {
        for shard in self.shards.iter() {
            let _lock = shard.grow_lock.lock().unwrap_or_else(|e| e.into_inner());
            while shard
                .insert_gate
                .compare_exchange_weak(0, GROWING, Ordering::AcqRel, Ordering::Relaxed)
                .is_err()
            {
                std::hint::spin_loop();
            }
            let new_table = alloc_table(SlotTable::<V>::new(INITIAL_SHARD_CAPACITY));
            let old_ptr = shard.table.swap(new_table, Ordering::AcqRel);
            shard.len.store(0, Ordering::Relaxed);
            if !old_ptr.is_null() {
                let old_table = unsafe { &*old_ptr };
                for slot in old_table.slots().iter() {
                    let p = slot.load(Ordering::Relaxed);
                    if !p.is_null() {
                        unsafe { ebr::retire_raw(p.cast(), drop_raw_entry::<V>) };
                    }
                }
                unsafe { ebr::retire_raw(old_ptr.cast(), drop_raw_table::<V>) };
            }
            shard.insert_gate.store(0, Ordering::Release);
        }
        self.key_count.store(0, Ordering::Release);
        ebr::force_collect();
    }

    #[inline]
    pub fn shard_count(&self) -> usize {
        self.shards.len()
    }

    #[inline]
    pub fn shard_slot_count(&self, shard: usize) -> usize {
        let _guard = ebr::pin();
        self.shards[shard].table().capacity()
    }

    pub fn shard_layout_matches(&self, capacities: &[usize]) -> bool {
        if capacities.len() != self.shards.len() {
            return false;
        }
        let _guard = ebr::pin();
        self.shards
            .iter()
            .zip(capacities)
            .all(|(shard, &capacity)| shard.table().capacity() == capacity)
    }

    pub fn peek_slot(&self, shard: usize, slot: usize) -> Option<(String, V)> {
        let s = &self.shards[shard];
        let t = s.table();
        if slot >= t.capacity {
            return None;
        }
        let _guard = ebr::pin();
        let p = t.slots()[slot].load(Ordering::Acquire);
        if p.is_null() {
            return None;
        }
        let entry = unsafe { &*p };
        if !state_occupied(&entry.state, Ordering::Acquire) {
            return None;
        }
        Some((entry.key.as_str().to_owned(), unsafe {
            (*entry.value.get()).assume_init_ref().clone()
        }))
    }

    pub fn peek_slot_with<R>(
        &self,
        shard: usize,
        slot: usize,
        f: impl FnOnce(&str, &V) -> R,
    ) -> Option<R> {
        let s = &self.shards[shard];
        let t = s.table();
        if slot >= t.capacity {
            return None;
        }
        let _guard = ebr::pin();
        let p = t.slots()[slot].load(Ordering::Acquire);
        if p.is_null() {
            return None;
        }
        let entry = unsafe { &*p };
        if !state_occupied(&entry.state, Ordering::Acquire) {
            return None;
        }
        Some(f(entry.key.as_str(), unsafe {
            (*entry.value.get()).assume_init_ref()
        }))
    }
}
