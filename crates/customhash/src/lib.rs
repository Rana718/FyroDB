mod ebr;
mod key;
mod ops;
mod shard;
mod snap;
pub use snap::SnapSink;

#[repr(align(128))]
#[derive(Default)]
pub struct Flag(AtomicBool);

impl Deref for Flag {
   type Target = AtomicBool;
   #[inline(always)]
   fn deref(&self) -> &AtomicBool {
      &self.0
   }
}

pub use ebr::{Guard, force_collect, force_collect_quiescent, pin, retire_raw};

use std::marker::PhantomData;
use std::ops::Deref;
use std::sync::atomic::{AtomicBool, Ordering};

use foldhash::fast::RandomState;
use key::CompactKey;
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

// Notification always follows the write, so a draining reader sees a consistent snapshot.
pub trait WriteObserver: Send + Sync + 'static {
   fn on_write(&self, key: &str);
   /// The whole map was cleared.
   fn on_clear(&self);
}

pub struct CustomMap<V> {
   shards: Box<[Shard<V>]>,
   shift: u32,
   shard_mask: usize,
   hasher: RandomState,
   max_keys: usize,
   observer: std::sync::OnceLock<Box<dyn WriteObserver>>,
   /// One relaxed load on every write when nothing observes. Shared so
   /// the observer's owner can toggle it without a map reference.
   observing: std::sync::Arc<Flag>,
   snap: std::sync::Arc<snap::SnapCtl<V>>,
}

impl<V: Clone + Send + Sync + 'static> CustomMap<V> {
   /// Install the write observer (once); enable it with `set_observing`.
   pub fn set_observer(&self, observer: Box<dyn WriteObserver>) {
      let _ = self.observer.set(observer);
   }

   pub fn set_observing(&self, on: bool) {
      self
         .observing
         .store(on && self.observer.get().is_some(), Ordering::Release);
   }

   /// The flag gating observer calls, for the observer's owner to toggle.
   pub fn observing_handle(&self) -> std::sync::Arc<Flag> {
      std::sync::Arc::clone(&self.observing)
   }

   #[inline(always)]
   fn observing(&self) -> bool {
      self.observing.load(Ordering::Relaxed)
   }

   /// A write to `key` completed.
   #[inline(always)]
   pub(crate) fn touched(&self, key: &str) {
      if self.observing() {
         self.notify(key);
      }
   }

   #[cold]
   #[inline(never)]
   pub(crate) fn notify(&self, key: &str) {
      if let Some(o) = self.observer.get() {
         o.on_write(key);
      }
   }

   pub const DEFAULT_SHARDS: usize = 128;

   pub fn new() -> Self {
      Self::with_shards(Self::DEFAULT_SHARDS)
   }

   pub fn with_shards(n: usize) -> Self {
      Self::build(
         n.next_power_of_two().max(1),
         DEFAULT_SHARD_CAPACITY,
         usize::MAX,
      )
   }

   pub fn with_capacity(shard_count: usize, expected_keys: usize) -> Self {
      let n = shard_count.next_power_of_two().max(1);
      let per_shard = if expected_keys == usize::MAX || expected_keys == 0 {
         INITIAL_SHARD_CAPACITY
      } else {
         let target = (expected_keys.saturating_mul(LOAD_DEN) / LOAD_NUM) / n;
         target.next_power_of_two().max(INITIAL_SHARD_CAPACITY)
      };
      Self::build(n, per_shard, expected_keys)
   }

   fn build(n: usize, per_shard: usize, max_keys: usize) -> Self {
      let hasher = RandomState::default();
      let snap = std::sync::Arc::new(snap::SnapCtl::new());
      CustomMap {
         shift: (u64::BITS - n.trailing_zeros()).min(63),
         shard_mask: n - 1,
         shards: (0..n)
            .map(|_| Shard::new(per_shard, snap.clone()))
            .collect::<Vec<_>>()
            .into_boxed_slice(),
         hasher,
         max_keys,
         observer: std::sync::OnceLock::new(),
         observing: std::sync::Arc::new(Flag::default()),
         snap,
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

   #[inline(always)]
   pub fn shard_of(&self, hash: u64) -> usize {
      ((hash >> self.shift) as usize) & self.shard_mask
   }

   pub fn prefetch_batch(&self, keys: &[&[u8]], hashes: &mut [u64]) {
      let n = keys.len().min(PREFETCH_BATCH).min(hashes.len());
      if n == 0 {
         return;
      }
      ebr::with_pin(|_| {
         let mut slots: [(*const shard::TaggedSlot<V>, u64); PREFETCH_BATCH] =
            [(std::ptr::null(), 0); PREFETCH_BATCH];
         for ((slot, key), out) in slots.iter_mut().zip(&keys[..n]).zip(hashes.iter_mut()) {
            let Ok(k) = std::str::from_utf8(key) else {
               continue;
            };
            let (h, idx) = self.locate(k);
            *out = h;
            let t = unsafe { self.shards.get_unchecked(idx) }.table();
            let s = unsafe { t.slots.add((h as usize) & t.mask) };
            prefetch_line(s);
            *slot = (s, h);
         }
         for &(s, h) in &slots[..n] {
            if s.is_null() {
               continue;
            }
            if let Ok(p) = unsafe { &*s }.load_if_tag_matches(h, Ordering::Relaxed)
               && !p.is_null()
            {
               prefetch_line(p);
            }
         }
      })
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

   #[inline(always)]
   pub fn get(&self, key: &str) -> Option<V> {
      let (h, idx) = self.locate(key);
      ebr::with_pin(|_| unsafe { self.shards.get_unchecked(idx) }.get(key, h))
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
      let observed = self.observing().then(|| key.clone());
      let is_new = unsafe { self.shards.get_unchecked(idx) }.insert_hashed(key, value, h);
      if is_new {
         self.bump(idx, true);
      }
      if let Some(k) = observed {
         self.notify(&k);
      }
      is_new
   }

   /// `insert` for borrowed keys: no key allocation for inline (≤15 byte)
   /// keys, one boxed allocation for longer ones.
   #[inline]
   pub fn insert_str(&self, key: &str, value: V) -> bool {
      let (h, idx) = self.locate(key);
      let is_new = unsafe { self.shards.get_unchecked(idx) }.insert_hashed_key(
         CompactKey::from_str(key),
         value,
         h,
      );
      if is_new {
         self.bump(idx, true);
      }
      self.touched(key);
      is_new
   }

   #[inline]
   pub fn try_insert(&self, key: String, value: V) -> Result<bool, Full> {
      if self.is_full() {
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
      self.set_compact(key, value, || CompactKey::from_string(key_owned()))
   }

   /// Borrowed-key `set`: no key allocation for inline (≤15B) keys,
   /// and only when actually absent.
   #[inline]
   pub fn set_str(&self, key: &str, value: V) -> bool {
      self.set_compact(key, value, || CompactKey::from_str(key))
   }

   #[inline]
   pub fn set_str_hinted(&self, key: &str, hash: Option<u64>, value: V) -> bool {
      let (h, idx) = match hash {
         Some(h) => (h, self.shard_of(h)),
         None => self.locate(key),
      };
      self.set_located(key, h, idx, value, || CompactKey::from_str(key))
   }

   fn set_compact(&self, key: &str, value: V, key_owned: impl FnOnce() -> CompactKey) -> bool {
      let (h, idx) = self.locate(key);
      self.set_located(key, h, idx, value, key_owned)
   }

   #[inline(always)]
   fn set_located(
      &self,
      key: &str,
      h: u64,
      idx: usize,
      value: V,
      key_owned: impl FnOnce() -> CompactKey,
   ) -> bool {
      let _guard = ebr::pin();
      let shard = unsafe { self.shards.get_unchecked(idx) };
      let is_new = shard.set_key(key, h, value, key_owned);
      if is_new {
         self.bump(idx, true);
      }
      self.touched(key);
      is_new
   }

   #[inline]
   pub fn try_set(
      &self,
      key: &str,
      value: V,
      key_owned: impl FnOnce() -> String,
   ) -> Result<bool, Full> {
      if self.is_full() {
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

   /// `try_set` for borrowed keys: no key allocation for inline (≤15
   /// byte) keys.
   #[inline]
   pub fn try_set_str(&self, key: &str, value: V) -> Result<bool, Full> {
      if self.is_full() {
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
      Ok(self.set_str(key, value))
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
      self.snap.before_write(entry);
      write_begin(&entry.state);
      let value = unsafe { (*entry.value.get()).assume_init_ref().clone() };
      unsafe { (*entry.value.get()).assume_init_drop() };
      write_end(&entry.state, false);
      self.bump(idx, false);
      self.touched(key);
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
      self.snap.before_write(entry);
      write_begin(&entry.state);
      unsafe { (*entry.value.get()).assume_init_drop() };
      write_end(&entry.state, false);
      self.bump(idx, false);
      self.touched(key);
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
      self.snap.before_write(entry);
      write_begin(&entry.state);
      unsafe { (*entry.value.get()).assume_init_drop() };
      write_end(&entry.state, false);
      self.bump(idx, false);
      self.touched(key);
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
      self.snap.before_write(entry);
      write_begin(&entry.state);
      unsafe { (*entry.value.get()).assume_init_drop() };
      unsafe { (*entry.value.get()).write(new_val) };
      write_end(&entry.state, true);
      self.touched(key);
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
            self.snap.before_write(entry);
            write_begin(&entry.state);
            unsafe { (*entry.value.get()).assume_init_drop() };
            unsafe { (*entry.value.get()).write(new_val) };
            write_end(&entry.state, true);
            self.touched(key);
            Some(r)
         }
         None => {
            state_unlock(&entry.state);
            None
         }
      }
   }

   #[inline]
   pub fn update_with<R>(&self, key: &str, f: impl FnMut(&mut V) -> R) -> Option<R> {
      self.update_with_hinted(key, None, f)
   }

   #[inline]
   pub fn update_with_hinted<R>(
      &self,
      key: &str,
      hash: Option<u64>,
      mut f: impl FnMut(&mut V) -> R,
   ) -> Option<R> {
      let (h, idx) = match hash {
         Some(h) => (h, self.shard_of(h)),
         None => self.locate(key),
      };
      let _guard = ebr::pin();
      let entry = unsafe { self.shards.get_unchecked(idx) }.find(key, h)?;

      state_lock(&entry.state);
      if !state_occupied(&entry.state, Ordering::Relaxed) {
         state_unlock(&entry.state);
         return None;
      }
      self.snap.before_write(entry);
      write_begin(&entry.state);
      let result = f(unsafe { (*entry.value.get()).assume_init_mut() });
      write_end(&entry.state, true);
      self.touched(key);
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
   pub fn read_consistent<R>(&self, key: &str, mut f: impl FnMut(&V) -> R) -> Option<R> {
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
      let observed = self.observing().then(|| key.clone());
      let is_new = shard.set_compact_key(CompactKey::from_string(key), h, value, false);
      if is_new {
         self.bump(idx, true);
      }
      if let Some(k) = observed {
         self.notify(&k);
      }
      is_new
   }

   /// `insert_if_absent` for borrowed keys: no key allocation for inline
   /// (≤15 byte) keys, one boxed allocation for longer ones.
   #[inline]
   pub fn insert_if_absent_str(&self, key: &str, value: V) -> bool {
      self.insert_if_absent_str_hinted(key, None, value)
   }

   #[inline]
   pub fn insert_if_absent_str_hinted(&self, key: &str, hash: Option<u64>, value: V) -> bool {
      let (h, idx) = match hash {
         Some(h) => (h, self.shard_of(h)),
         None => self.locate(key),
      };
      let _guard = ebr::pin();
      let shard = unsafe { self.shards.get_unchecked(idx) };
      let is_new = shard.set_key_with_overwrite(key, h, value, false, || CompactKey::from_str(key));
      if is_new {
         self.bump(idx, true);
      }
      self.touched(key);
      is_new
   }

   #[inline]
   pub fn len(&self) -> usize {
      self
         .shards
         .iter()
         .map(|s| s.live.load(Ordering::Relaxed))
         .fold(0usize, usize::wrapping_add)
         .min(isize::MAX as usize)
   }

   #[inline(always)]
   fn bump(&self, idx: usize, up: bool) {
      let live = &unsafe { self.shards.get_unchecked(idx) }.live;
      if up {
         live.fetch_add(1, Ordering::Relaxed);
      } else {
         live.fetch_sub(1, Ordering::Relaxed);
      }
   }

   /// One relaxed load; trivially false with no limit configured.
   #[inline(always)]
   pub fn is_full(&self) -> bool {
      self.max_keys != usize::MAX && self.len() >= self.max_keys
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

impl<V: Clone + Send + Sync + 'static> Default for CustomMap<V> {
   fn default() -> Self {
      Self::new()
   }
}

pub const PREFETCH_BATCH: usize = 16;

#[inline(always)]
fn prefetch_line<T>(p: *const T) {
   #[cfg(target_arch = "x86_64")]
   unsafe {
      std::arch::x86_64::_mm_prefetch::<{ std::arch::x86_64::_MM_HINT_T0 }>(p.cast());
   }
   #[cfg(target_arch = "aarch64")]
   unsafe {
      std::arch::asm!("prfm pldl1keep, [{0}]", in(reg) p, options(nostack, readonly, preserves_flags));
   }
   #[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
   let _ = p;
}

pub const fn entry_size<V>() -> usize {
   std::mem::size_of::<shard::Entry<V>>()
}

#[cfg(test)]
mod tests {
   use super::CustomMap;
   use std::collections::HashSet;
   use std::sync::Arc;

   #[test]
   fn snapshot_is_point_in_time_under_concurrent_writes() {
      use std::sync::Mutex;
      for round in 0..3 {
         let map = Arc::new(CustomMap::<i64>::with_capacity(16, 1 << 20));
         for i in 0..20_000i64 {
            map.insert(format!("k{i}"), i);
         }
         let side: Arc<Mutex<Vec<(String, i64)>>> = Arc::new(Mutex::new(Vec::new()));
         let s2 = Arc::clone(&side);
         assert!(map.begin_snapshot(Arc::new(move |k: &str, v: &i64| {
            s2.lock().unwrap().push((k.to_string(), *v));
         })));
         assert!(!map.begin_snapshot(Arc::new(|_: &str, _: &i64| {})));
         let writer = {
            let map = Arc::clone(&map);
            std::thread::spawn(move || {
               for i in 0..20_000i64 {
                  map.update_with(&format!("k{i}"), |v| *v += 1_000_000);
                  if i % 7 == 0 {
                     map.remove(&format!("k{}", (i * 13) % 20_000));
                  }
                  if i % 5 == 0 {
                     map.insert(format!("n{i}"), -1);
                  }
               }
            })
         };
         let mut saved = Vec::new();
         for shard in 0..map.shard_count() {
            map.snapshot_shard(shard, |k, v| saved.push((k.to_string(), *v)));
         }
         writer.join().unwrap();
         map.end_snapshot();
         saved.extend(side.lock().unwrap().drain(..));
         saved.sort();
         let mut want: Vec<(String, i64)> = (0..20_000i64).map(|i| (format!("k{i}"), i)).collect();
         want.sort();
         assert_eq!(saved.len(), want.len(), "round {round}");
         assert_eq!(saved, want, "round {round}");
         map.update_with("k1", |v| *v = 7);
         assert_eq!(map.get("k1"), Some(7));
      }
   }

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
   fn insert_if_absent_never_overwrites_a_won_race() {
      let map = Arc::new(CustomMap::with_capacity(1, 16));
      std::thread::scope(|scope| {
         for t in 0..8 {
            let map = Arc::clone(&map);
            scope.spawn(move || {
               for _ in 0..20_000 {
                  // update-then-create-if-missing, the INCR pattern
                  if map.update_with("counter", |v: &mut i64| *v += 1).is_none() {
                     let created = map.insert_if_absent("counter".into(), 1);
                     if !created {
                        // Lost the create race; the increment still
                        // must happen via a retried update.
                        let mut spins = 0;
                        while map.update_with("counter", |v| *v += 1).is_none() {
                           spins += 1;
                           if spins > 1_000_000 {
                              panic!("update never succeeded after lost race");
                           }
                        }
                     }
                  }
                  let _ = t;
               }
            });
         }
      });
      assert_eq!(map.get("counter"), Some(160_000));
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
      let _serialize = crate::ebr::TEST_SERIALIZE
         .lock()
         .unwrap_or_else(|e| e.into_inner());
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

   /// Values own a heap allocation so the allocator will catch a double-free.
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

   /// `len` has to stay in step with what is actually reachable, across
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
