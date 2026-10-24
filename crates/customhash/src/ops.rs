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

   /// Iterate a half-open range of shards under an EBR pin per shard.
   /// Disjoint shard ranges may run concurrently on separate threads.
   pub fn for_each_shard_range(&self, from: usize, to: usize, mut f: impl FnMut(&str, &V)) {
      for shard_idx in from..to.min(self.shards.len()) {
         let _guard = ebr::pin();
         let t = self.shards[shard_idx].table();
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

   /// Entry addresses never move (readers stay valid); reclaimed memory goes
   /// to EBR. Returns (next_slot, capacity, rebuilt).
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
            self.snap.before_write(entry);
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
      let mut removed = false;
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
                  self.snap.before_write(entry);
                  write_begin(&entry.state);
                  unsafe { (*entry.value.get()).assume_init_drop() };
                  write_end(&entry.state, false);
                  shard.live.fetch_sub(1, Ordering::Relaxed);
                  removed = true;
                  if self.observing.load(Ordering::Relaxed) {
                     self.notify(entry.key.as_str());
                  }
               } else {
                  state_unlock(&entry.state);
               }
            }
         }
      }
      if removed {}
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
      let shard = self.shards.get(shard_idx)?;
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
            self.snap.before_write(entry);
            write_begin(&entry.state);
            unsafe { (*entry.value.get()).assume_init_drop() };
            write_end(&entry.state, false);
            shard.live.fetch_sub(1, Ordering::Relaxed);
            if self.observing.load(Ordering::Relaxed) {
               self.notify(entry.key.as_str());
            }
         } else {
            state_unlock(&entry.state);
         }
      }
      Some((end, capacity))
   }

   pub fn expire_shard_range(
      &self,
      shard_idx: usize,
      start_slot: usize,
      max_slots: usize,
      expired: impl Fn(&V) -> Option<bool>,
   ) -> Option<(usize, usize, usize, usize)> {
      let shard = self.shards.get(shard_idx)?;
      let _guard = ebr::pin();
      let t = shard.table();
      let capacity = t.capacity();
      let start = start_slot.min(capacity);
      let end = start.saturating_add(max_slots).min(capacity);
      let (mut removed, mut live) = (0usize, 0usize);
      for slot in &t.slots()[start..end] {
         let p = slot.load(Ordering::Acquire);
         if p.is_null() {
            continue;
         }
         let entry = unsafe { &*p };
         if !state_occupied(&entry.state, Ordering::Acquire) {
            continue;
         }
         match expired(unsafe { (*entry.value.get()).assume_init_ref() }) {
            None => continue,
            Some(false) => {
               live += 1;
               continue;
            }
            Some(true) => {}
         }
         state_lock(&entry.state);
         let still = state_occupied(&entry.state, Ordering::Relaxed)
            && expired(unsafe { (*entry.value.get()).assume_init_ref() }) == Some(true);
         if still {
            self.snap.before_write(entry);
            write_begin(&entry.state);
            unsafe { (*entry.value.get()).assume_init_drop() };
            write_end(&entry.state, false);
            shard.live.fetch_sub(1, Ordering::Relaxed);
            removed += 1;
            if self.observing.load(Ordering::Relaxed) {
               self.notify(entry.key.as_str());
            }
         } else {
            state_unlock(&entry.state);
            live += 1;
         }
      }
      Some((end, capacity, removed, live))
   }

   pub fn compact_shard(&self, shard_idx: usize) {
      let Some(shard) = self.shards.get(shard_idx) else {
         return;
      };

      // Cheap gate: this runs on every dbsize drop and every maintenance
      // tick, and most shards have nothing to reclaim. `len` counts inserts
      // since the last rebuild and `live` the occupied slots, so their
      // difference approximates the tombstone population.
      let live_hint = shard.live.load(Ordering::Relaxed);
      let capacity_hint = shard.mask.load(Ordering::Relaxed) + 1;
      let dead_hint = dead_estimate(shard.len.load(Ordering::Relaxed), live_hint);
      if live_hint == 0 && dead_hint == 0 {
         return;
      }
      if capacity_for(live_hint) >= capacity_hint && dead_hint < (capacity_hint / 4).max(16) {
         return;
      }

      let _lock = shard.grow_lock.lock().unwrap_or_else(|e| e.into_inner());
      shard.insert_gate.fetch_or(GROWING, Ordering::AcqRel);
      while (shard.insert_gate.load(Ordering::Acquire) & !GROWING) != 0 {
         std::hint::spin_loop();
      }

      let old_ptr = shard.table.load(Ordering::Acquire);
      let old_table = unsafe { &*old_ptr };

      let mut live_count: usize = 0;
      let mut dead_count: usize = 0;
      for slot in old_table.slots().iter() {
         let p = slot.load(Ordering::Acquire);
         if p.is_null() {
            continue;
         }
         if state_occupied(&unsafe { &*p }.state, Ordering::Acquire) {
            live_count += 1;
         } else {
            dead_count += 1;
         }
      }

      let target = capacity_for(live_count);
      if target >= old_table.capacity() && dead_count < (old_table.capacity() / 4).max(16) {
         shard.insert_gate.fetch_and(!GROWING, Ordering::Release);
         return;
      }

      // Shrink when sparse, otherwise rebuild at the same capacity to drop
      // tombstones without increasing memory.
      let new_cap = target.min(old_table.capacity());
      shard.rebuild_to(old_ptr, new_cap);
      shard.insert_gate.fetch_and(!GROWING, Ordering::Release);
   }

   pub fn clear(&self) {
      for shard in self.shards.iter() {
         let _lock = shard.grow_lock.lock().unwrap_or_else(|e| e.into_inner());
         shard.insert_gate.fetch_or(GROWING, Ordering::AcqRel);
         while (shard.insert_gate.load(Ordering::Acquire) & !GROWING) != 0 {
            std::hint::spin_loop();
         }
         let new_table = alloc_table(SlotTable::<V>::new(shard.initial_capacity));
         let old_ptr = shard.table.swap(new_table, Ordering::AcqRel);
         let new_slots = unsafe { (*new_table).slots };
         let new_mask = unsafe { (*new_table).mask };
         let new_threshold = unsafe { (*new_table).threshold };
         shard.slots.store(new_slots, Ordering::Release);
         shard.mask.store(new_mask, Ordering::Release);
         shard.threshold.store(new_threshold, Ordering::Release);
         let sf_ptr = shard.fast.load(Ordering::Relaxed);
         if !sf_ptr.is_null() {
            unsafe {
               (*sf_ptr).slots.store(new_slots, Ordering::Release);
               (*sf_ptr).mask.store(new_mask, Ordering::Release);
            }
         }
         shard.len.store(0, Ordering::Relaxed);
         if !old_ptr.is_null() {
            unsafe { ebr::retire_raw(old_ptr.cast(), drop_raw_table_and_entries::<V>) };
         }
         shard.insert_gate.fetch_and(!GROWING, Ordering::Release);
      }
      for shard in self.shards.iter() {
         shard.live.store(0, Ordering::Release);
      }
      if self.observing.load(Ordering::Relaxed)
         && let Some(o) = self.observer.get()
      {
         o.on_clear();
      }
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
      self
         .shards
         .iter()
         .zip(capacities)
         .all(|(shard, &capacity)| shard.table().capacity() == capacity)
   }

   pub fn begin_snapshot(&self, sink: crate::SnapSink<V>) -> bool {
      let ok = self.snap.begin(sink);
      if ok {
         self.snap_active.store(true, Ordering::Release);
      }
      ok
   }

   pub fn end_snapshot(&self) {
      self.snap_active.store(false, Ordering::Release);
      self.snap.end();
   }

   pub fn snapshot_shard(&self, shard_idx: usize, mut visit: impl FnMut(&str, &V)) {
      let Some(shard) = self.shards.get(shard_idx) else {
         return;
      };
      let _guard = ebr::pin();
      let parity = self.snap.parity();
      let t = shard.table();
      for slot in t.slots() {
         let p = slot.load(Ordering::Acquire);
         if p.is_null() {
            continue;
         }
         let e = unsafe { &*p };
         if e.state.load(Ordering::Acquire) & STATE_SNAP == parity {
            continue;
         }
         state_lock(&e.state);
         let st = e.state.load(Ordering::Relaxed);
         if st & STATE_SNAP != parity {
            if st & STATE_OCCUPIED != 0 {
               visit(e.key.as_str(), unsafe {
                  (*e.value.get()).assume_init_ref()
               });
            }
            e.state
               .store((st & !STATE_SNAP) | parity, Ordering::Relaxed);
         }
         state_unlock(&e.state);
      }
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

   /// Regression: cursor defrag must visit each slot at most once per pass
   /// and honour the budget exactly.
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
