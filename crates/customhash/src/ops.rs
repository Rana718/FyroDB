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
    ///
    /// Walks one shard from `start_slot`, stopping after `budget` rebuilds.
    /// Returns `(next_slot, capacity, rebuilt)`; `next_slot == capacity` means
    /// the shard is finished. Callers keep the cursor so a large keyspace is
    /// covered across ticks — an earlier version materialized every key into a
    /// `Vec<String>` just to rebuild `budget` of them, which spiked memory in
    /// the code path meant to reduce it.
    pub fn defragment_shard_range(
        &self,
        shard_idx: usize,
        start_slot: usize,
        budget: usize,
        mut rebuild: impl FnMut(&mut V),
    ) -> Option<(usize, usize, usize)> {
        let shard = self.shards.get(shard_idx)?;
        let _guard = ebr::pin();
        let t = shard.table();
        let capacity = t.capacity();
        let mut slot = start_slot.min(capacity);
        let mut rebuilt = 0usize;

        while slot < capacity && rebuilt < budget {
            let p = unsafe { t.slots().get_unchecked(slot) }.load(Ordering::Acquire);
            slot += 1;
            if p.is_null() {
                continue;
            }
            let entry = unsafe { &*p };
            if !state_occupied(&entry.state, Ordering::Acquire) {
                continue;
            }
            state_lock(&entry.state);
            if state_occupied(&entry.state, Ordering::Relaxed) {
                write_begin(&entry.state);
                rebuild(unsafe { (*entry.value.get()).assume_init_mut() });
                write_end(&entry.state, true);
                rebuilt += 1;
            } else {
                state_unlock(&entry.state);
            }
        }

        Some((slot, capacity, rebuilt))
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
                        write_begin(&entry.state);
                        unsafe { (*entry.value.get()).assume_init_drop() };
                        write_end(&entry.state, false);
                        self.key_count.fetch_sub(1, Ordering::Relaxed);
                    } else {
                        state_unlock(&entry.state);
                    }
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
                write_begin(&entry.state);
                unsafe { (*entry.value.get()).assume_init_drop() };
                write_end(&entry.state, false);
                self.key_count.fetch_sub(1, Ordering::Relaxed);
            } else {
                state_unlock(&entry.state);
            }
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
                    new_slot.store(p, e.hash, Ordering::Relaxed);
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

#[cfg(test)]
mod tests {
    use super::super::CustomMap;

    /// Defrag used to materialize every key into a `Vec<String>` before
    /// rebuilding `budget` of them. The cursor form must instead visit each
    /// slot at most once per pass and honour the budget exactly.
    #[test]
    fn cursor_defrag_covers_every_value_within_budget() {
        let map = CustomMap::with_capacity(1, 512);
        for i in 0..200 {
            map.insert(format!("key-{i}"), vec![i; 4]);
        }

        let mut cursor = 0usize;
        let mut total_rebuilt = 0usize;
        let mut passes = 0usize;
        loop {
            let (next, capacity, rebuilt) = map
                .defragment_shard_range(0, cursor, 16, |value| value.shrink_to_fit())
                .unwrap();
            total_rebuilt += rebuilt;
            assert!(rebuilt <= 16, "budget exceeded: {rebuilt}");
            passes += 1;
            assert!(passes < 1_000, "cursor failed to advance");
            if next >= capacity {
                break;
            }
            assert!(next > cursor);
            cursor = next;
        }

        assert_eq!(total_rebuilt, 200);
        for i in 0..200 {
            assert_eq!(map.get(&format!("key-{i}")), Some(vec![i; 4]));
        }
    }

    #[test]
    fn cursor_defrag_reports_no_progress_for_a_missing_shard() {
        let map = CustomMap::<u32>::with_capacity(1, 8);
        assert!(map.defragment_shard_range(9, 0, 4, |_| {}).is_none());
    }
}
