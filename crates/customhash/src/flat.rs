use std::alloc::Layout;
use std::hash::{BuildHasher, Hash, Hasher};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use crossbeam_utils::CachePadded;
use foldhash::fast::FixedState;

pub const EMPTY_TAG: u8 = 0xFF;
pub const TOMBSTONE_TAG: u8 = 0x80;

#[inline(always)]
pub fn ctrl_h2(hash: u64) -> u8 {
   ((hash >> 57) as u8) & 0x7F
}

#[derive(Clone, Copy, Default, Debug)]
pub struct FastState;

impl BuildHasher for FastState {
   type Hasher = FastHasher;
   #[inline(always)]
   fn build_hasher(&self) -> Self::Hasher {
      FastHasher(0)
   }
}

pub struct FastHasher(u64);

impl Hasher for FastHasher {
   #[inline(always)]
   fn finish(&self) -> u64 {
      let mut x = self.0;
      x ^= x >> 30;
      x = x.wrapping_mul(0xbf58476d1ce4e5b9);
      x ^= x >> 27;
      x = x.wrapping_mul(0x94d049bb133111eb);
      x ^= x >> 31;
      x
   }
   #[inline(always)]
   fn write(&mut self, bytes: &[u8]) {
      for &b in bytes {
         self.0 = (self.0 ^ (b as u64)).wrapping_mul(0x100000001b3);
      }
   }
   #[inline(always)]
   fn write_u64(&mut self, i: u64) {
      self.0 = i;
   }
   #[inline(always)]
   fn write_usize(&mut self, i: usize) {
      self.0 = i as u64;
   }
   #[inline(always)]
   fn write_i64(&mut self, i: i64) {
      self.0 = i as u64;
   }
}

/// Matches a 16-byte control group against a target fingerprint using SIMD.
///
/// # Safety
///
/// `ctrl_ptr` must be valid for reads of at least 16 contiguous bytes.
#[inline(always)]
pub unsafe fn match_group_16(ctrl_ptr: *const u8, h2: u8) -> (u16, u16) {
   #[cfg(target_arch = "x86_64")]
   unsafe {
      use std::arch::x86_64::*;
      let group = _mm_loadu_si128(ctrl_ptr.cast::<__m128i>());
      let target = _mm_set1_epi8(h2 as i8);
      let empty = _mm_set1_epi8(-1i8);
      let match_mask = _mm_movemask_epi8(_mm_cmpeq_epi8(group, target)) as u16;
      let empty_mask = _mm_movemask_epi8(_mm_cmpeq_epi8(group, empty)) as u16;
      (match_mask, empty_mask)
   }
   #[cfg(not(target_arch = "x86_64"))]
   unsafe {
      let mut match_mask = 0u16;
      let mut empty_mask = 0u16;
      for j in 0..16 {
         let b = *ctrl_ptr.add(j);
         if b == h2 {
            match_mask |= 1 << j;
         }
         if b == EMPTY_TAG {
            empty_mask |= 1 << j;
         }
      }
      (match_mask, empty_mask)
   }
}

#[derive(Clone, Copy)]
#[repr(C)]
pub struct FlatSlot<K, V> {
   pub key: K,
   pub val: V,
}

#[repr(align(128))]
pub struct FlatShard<K, V> {
   ctrl: *mut u8,
   slots: *mut FlatSlot<K, V>,
   capacity: usize,
   mask: usize,
   len: CachePadded<AtomicUsize>,
}

unsafe impl<K: Send, V: Send> Send for FlatShard<K, V> {}
unsafe impl<K: Sync, V: Sync> Sync for FlatShard<K, V> {}

impl<K: Copy + PartialEq + Hash, V: Copy> FlatShard<K, V> {
   pub fn new(cap: usize) -> Self {
      let cap = cap.next_power_of_two().max(16);
      let ctrl_layout = Layout::from_size_align(cap + 16, 64).expect("ctrl layout");
      let slots_layout =
         Layout::from_size_align((cap + 16) * std::mem::size_of::<FlatSlot<K, V>>(), 64)
            .expect("slots layout");

      let ctrl = unsafe { rust_zmalloc::alloc_raw(ctrl_layout) }.cast::<u8>();
      if ctrl.is_null() {
         std::alloc::handle_alloc_error(ctrl_layout);
      }
      unsafe { std::ptr::write_bytes(ctrl, EMPTY_TAG, cap + 16) };

      let slots = unsafe { rust_zmalloc::alloc_raw_zeroed(slots_layout) }.cast::<FlatSlot<K, V>>();
      if slots.is_null() {
         std::alloc::handle_alloc_error(slots_layout);
      }

      FlatShard {
         ctrl,
         slots,
         capacity: cap,
         mask: cap - 1,
         len: CachePadded::new(AtomicUsize::new(0)),
      }
   }

   #[inline(always)]
   pub fn get(&self, key: &K, hash: u64) -> Option<V> {
      let h2 = ctrl_h2(hash);
      let mut base = (hash as usize) & self.mask;
      let mut probes = 0usize;

      #[cfg(target_arch = "x86_64")]
      use std::arch::x86_64::*;
      #[cfg(target_arch = "x86_64")]
      let target = unsafe { _mm_set1_epi8(h2 as i8) };
      #[cfg(target_arch = "x86_64")]
      let empty = unsafe { _mm_set1_epi8(-1i8) };

      loop {
         if probes >= self.capacity {
            return None;
         }
         #[cfg(target_arch = "x86_64")]
         unsafe {
            let group = _mm_loadu_si128(self.ctrl.add(base).cast::<__m128i>());
            let match_mask = _mm_movemask_epi8(_mm_cmpeq_epi8(group, target)) as u16;
            if match_mask != 0 {
               let offset = match_mask.trailing_zeros() as usize;
               let slot = &*self.slots.add(base + offset);
               if slot.key == *key {
                  return Some(slot.val);
               }
               let mut m = match_mask & (match_mask - 1);
               while m != 0 {
                  let off = m.trailing_zeros() as usize;
                  let s = &*self.slots.add(base + off);
                  if s.key == *key {
                     return Some(s.val);
                  }
                  m &= m - 1;
               }
            }
            let empty_mask = _mm_movemask_epi8(_mm_cmpeq_epi8(group, empty)) as u16;
            if empty_mask != 0 {
               return None;
            }
         }
         #[cfg(not(target_arch = "x86_64"))]
         unsafe {
            let (match_mask, empty_mask) = match_group_16(self.ctrl.add(base), h2);
            if match_mask != 0 {
               let offset = match_mask.trailing_zeros() as usize;
               let slot = &*self.slots.add(base + offset);
               if slot.key == *key {
                  return Some(slot.val);
               }
               let mut m = match_mask & (match_mask - 1);
               while m != 0 {
                  let off = m.trailing_zeros() as usize;
                  let s = &*self.slots.add(base + off);
                  if s.key == *key {
                     return Some(s.val);
                  }
                  m &= m - 1;
               }
            }
            if empty_mask != 0 {
               return None;
            }
         }
         probes += 16;
         base = (base + 16) & self.mask;
      }
   }

   #[inline(always)]
   pub fn set(&self, key: K, val: V, hash: u64) {
      let h2 = ctrl_h2(hash);
      let mut base = (hash as usize) & self.mask;

      #[cfg(target_arch = "x86_64")]
      use std::arch::x86_64::*;
      #[cfg(target_arch = "x86_64")]
      let target = unsafe { _mm_set1_epi8(h2 as i8) };
      #[cfg(target_arch = "x86_64")]
      let empty = unsafe { _mm_set1_epi8(-1i8) };

      loop {
         unsafe {
            #[cfg(target_arch = "x86_64")]
            {
               let group = _mm_loadu_si128(self.ctrl.add(base).cast::<__m128i>());
               let match_mask = _mm_movemask_epi8(_mm_cmpeq_epi8(group, target)) as u16;
               if match_mask != 0 {
                  let offset = match_mask.trailing_zeros() as usize;
                  let slot = &mut *self.slots.add(base + offset);
                  if slot.key == key {
                     slot.val = val;
                     let idx = (base + offset) & self.mask;
                     if idx < 16 {
                        (*self.slots.add(self.capacity + idx)).val = val;
                     }
                     return;
                  }
                  let mut m = match_mask & (match_mask - 1);
                  while m != 0 {
                     let off = m.trailing_zeros() as usize;
                     let s = &mut *self.slots.add(base + off);
                     if s.key == key {
                        s.val = val;
                        let idx = (base + off) & self.mask;
                        if idx < 16 {
                           (*self.slots.add(self.capacity + idx)).val = val;
                        }
                        return;
                     }
                     m &= m - 1;
                  }
               }
               let empty_mask = _mm_movemask_epi8(_mm_cmpeq_epi8(group, empty)) as u16;
               if empty_mask != 0 {
                  let offset = empty_mask.trailing_zeros() as usize;
                  let idx = (base + offset) & self.mask;
                  let slot = &mut *self.slots.add(idx);
                  slot.key = key;
                  slot.val = val;
                  *self.ctrl.add(idx) = h2;
                  if idx < 16 {
                     *self.ctrl.add(self.capacity + idx) = h2;
                     *self.slots.add(self.capacity + idx) = *slot;
                  }
                  self.len.fetch_add(1, Ordering::Relaxed);
                  return;
               }
            }
            #[cfg(not(target_arch = "x86_64"))]
            {
               let (match_mask, empty_mask) = match_group_16(self.ctrl.add(base), h2);
               if match_mask != 0 {
                  let offset = match_mask.trailing_zeros() as usize;
                  let slot = &mut *self.slots.add(base + offset);
                  if slot.key == key {
                     slot.val = val;
                     let idx = (base + offset) & self.mask;
                     if idx < 16 {
                        (*self.slots.add(self.capacity + idx)).val = val;
                     }
                     return;
                  }
                  let mut m = match_mask & (match_mask - 1);
                  while m != 0 {
                     let off = m.trailing_zeros() as usize;
                     let s = &mut *self.slots.add(base + off);
                     if s.key == key {
                        s.val = val;
                        let idx = (base + off) & self.mask;
                        if idx < 16 {
                           (*self.slots.add(self.capacity + idx)).val = val;
                        }
                        return;
                     }
                     m &= m - 1;
                  }
               }
               if empty_mask != 0 {
                  let offset = empty_mask.trailing_zeros() as usize;
                  let idx = (base + offset) & self.mask;
                  let slot = &mut *self.slots.add(idx);
                  slot.key = key;
                  slot.val = val;
                  *self.ctrl.add(idx) = h2;
                  if idx < 16 {
                     *self.ctrl.add(self.capacity + idx) = h2;
                     *self.slots.add(self.capacity + idx) = *slot;
                  }
                  self.len.fetch_add(1, Ordering::Relaxed);
                  return;
               }
            }
         }
         base = (base + 16) & self.mask;
      }
   }

   #[inline(always)]
   pub fn set_dual(&self, k0: K, v0: V, h0: u64, k1: K, v1: V, h1: u64) {
      #[cfg(target_arch = "x86_64")]
      unsafe {
         use std::arch::x86_64::*;
         let h2_0 = ctrl_h2(h0);
         let h2_1 = ctrl_h2(h1);
         let base0 = (h0 as usize) & self.mask;
         let base1 = (h1 as usize) & self.mask;

         let target0 = _mm_set1_epi8(h2_0 as i8);
         let target1 = _mm_set1_epi8(h2_1 as i8);
         let empty = _mm_set1_epi8(-1i8);

         let group0 = _mm_loadu_si128(self.ctrl.add(base0).cast::<__m128i>());
         let group1 = _mm_loadu_si128(self.ctrl.add(base1).cast::<__m128i>());

         let match0 = _mm_movemask_epi8(_mm_cmpeq_epi8(group0, target0)) as u16;
         let match1 = _mm_movemask_epi8(_mm_cmpeq_epi8(group1, target1)) as u16;

         // For 0:
         if match0 != 0 {
            let offset0 = match0.trailing_zeros() as usize;
            let slot0 = &mut *self.slots.add(base0 + offset0);
            if slot0.key == k0 {
               slot0.val = v0;
               let idx0 = (base0 + offset0) & self.mask;
               if idx0 < 16 {
                  (*self.slots.add(self.capacity + idx0)).val = v0;
               }
            } else {
               self.set(k0, v0, h0);
            }
         } else {
            let empty0 = _mm_movemask_epi8(_mm_cmpeq_epi8(group0, empty)) as u16;
            if empty0 != 0 {
               let offset0 = empty0.trailing_zeros() as usize;
               let idx0 = (base0 + offset0) & self.mask;
               let slot0 = &mut *self.slots.add(idx0);
               slot0.key = k0;
               slot0.val = v0;
               *self.ctrl.add(idx0) = h2_0;
               if idx0 < 16 {
                  *self.ctrl.add(self.capacity + idx0) = h2_0;
                  *self.slots.add(self.capacity + idx0) = *slot0;
               }
               self.len.fetch_add(1, Ordering::Relaxed);
            } else {
               self.set(k0, v0, h0);
            }
         }

         // For 1:
         if match1 != 0 {
            let offset1 = match1.trailing_zeros() as usize;
            let slot1 = &mut *self.slots.add(base1 + offset1);
            if slot1.key == k1 {
               slot1.val = v1;
               let idx1 = (base1 + offset1) & self.mask;
               if idx1 < 16 {
                  (*self.slots.add(self.capacity + idx1)).val = v1;
               }
            } else {
               self.set(k1, v1, h1);
            }
         } else {
            let empty1 = _mm_movemask_epi8(_mm_cmpeq_epi8(group1, empty)) as u16;
            if empty1 != 0 {
               let offset1 = empty1.trailing_zeros() as usize;
               let idx1 = (base1 + offset1) & self.mask;
               let slot1 = &mut *self.slots.add(idx1);
               slot1.key = k1;
               slot1.val = v1;
               *self.ctrl.add(idx1) = h2_1;
               if idx1 < 16 {
                  *self.ctrl.add(self.capacity + idx1) = h2_1;
                  *self.slots.add(self.capacity + idx1) = *slot1;
               }
               self.len.fetch_add(1, Ordering::Relaxed);
            } else {
               self.set(k1, v1, h1);
            }
         }
      }
      #[cfg(not(target_arch = "x86_64"))]
      {
         self.set(k0, v0, h0);
         self.set(k1, v1, h1);
      }
   }

   #[inline(always)]
   pub fn insert(&self, key: K, val: V, hash: u64) -> bool {
      let h2 = ctrl_h2(hash);
      let mut base = (hash as usize) & self.mask;

      #[cfg(target_arch = "x86_64")]
      use std::arch::x86_64::*;
      #[cfg(target_arch = "x86_64")]
      let target = unsafe { _mm_set1_epi8(h2 as i8) };
      #[cfg(target_arch = "x86_64")]
      let empty = unsafe { _mm_set1_epi8(-1i8) };

      loop {
         unsafe {
            #[cfg(target_arch = "x86_64")]
            {
               let group = _mm_loadu_si128(self.ctrl.add(base).cast::<__m128i>());
               let match_mask = _mm_movemask_epi8(_mm_cmpeq_epi8(group, target)) as u16;
               if match_mask != 0 {
                  let offset = match_mask.trailing_zeros() as usize;
                  let slot = &mut *self.slots.add(base + offset);
                  if slot.key == key {
                     slot.val = val;
                     let idx = (base + offset) & self.mask;
                     if idx < 16 {
                        (*self.slots.add(self.capacity + idx)).val = val;
                     }
                     return false;
                  }
                  let mut m = match_mask & (match_mask - 1);
                  while m != 0 {
                     let off = m.trailing_zeros() as usize;
                     let s = &mut *self.slots.add(base + off);
                     if s.key == key {
                        s.val = val;
                        let idx = (base + off) & self.mask;
                        if idx < 16 {
                           (*self.slots.add(self.capacity + idx)).val = val;
                        }
                        return false;
                     }
                     m &= m - 1;
                  }
               }
               let empty_mask = _mm_movemask_epi8(_mm_cmpeq_epi8(group, empty)) as u16;
               if empty_mask != 0 {
                  let offset = empty_mask.trailing_zeros() as usize;
                  let idx = (base + offset) & self.mask;
                  let slot = &mut *self.slots.add(idx);
                  slot.key = key;
                  slot.val = val;
                  *self.ctrl.add(idx) = h2;
                  if idx < 16 {
                     *self.ctrl.add(self.capacity + idx) = h2;
                     *self.slots.add(self.capacity + idx) = *slot;
                  }
                  self.len.fetch_add(1, Ordering::Relaxed);
                  return true;
               }
            }
            #[cfg(not(target_arch = "x86_64"))]
            {
               let (match_mask, empty_mask) = match_group_16(self.ctrl.add(base), h2);
               if match_mask != 0 {
                  let offset = match_mask.trailing_zeros() as usize;
                  let slot = &mut *self.slots.add(base + offset);
                  if slot.key == key {
                     slot.val = val;
                     let idx = (base + offset) & self.mask;
                     if idx < 16 {
                        (*self.slots.add(self.capacity + idx)).val = val;
                     }
                     return false;
                  }
                  let mut m = match_mask & (match_mask - 1);
                  while m != 0 {
                     let off = m.trailing_zeros() as usize;
                     let s = &mut *self.slots.add(base + off);
                     if s.key == key {
                        s.val = val;
                        let idx = (base + off) & self.mask;
                        if idx < 16 {
                           (*self.slots.add(self.capacity + idx)).val = val;
                        }
                        return false;
                     }
                     m &= m - 1;
                  }
               }
               if empty_mask != 0 {
                  let offset = empty_mask.trailing_zeros() as usize;
                  let idx = (base + offset) & self.mask;
                  let slot = &mut *self.slots.add(idx);
                  slot.key = key;
                  slot.val = val;
                  *self.ctrl.add(idx) = h2;
                  if idx < 16 {
                     *self.ctrl.add(self.capacity + idx) = h2;
                     *self.slots.add(self.capacity + idx) = *slot;
                  }
                  self.len.fetch_add(1, Ordering::Relaxed);
                  return true;
               }
            }
         }
         base = (base + 16) & self.mask;
      }
   }

   #[inline(always)]
   pub fn remove(&self, key: &K, hash: u64) -> bool {
      let h2 = ctrl_h2(hash);
      let mut base = (hash as usize) & self.mask;
      let mut probes = 0usize;

      loop {
         if probes >= self.capacity {
            return false;
         }
         unsafe {
            let (match_mask, empty_mask) = match_group_16(self.ctrl.add(base), h2);
            if match_mask != 0 {
               let offset = match_mask.trailing_zeros() as usize;
               let idx = (base + offset) & self.mask;
               let slot = &*self.slots.add(idx);
               if slot.key == *key {
                  *self.ctrl.add(idx) = TOMBSTONE_TAG;
                  if idx < 16 {
                     *self.ctrl.add(self.capacity + idx) = TOMBSTONE_TAG;
                  }
                  self.len.fetch_sub(1, Ordering::Relaxed);
                  return true;
               }
               let mut m = match_mask & (match_mask - 1);
               while m != 0 {
                  let off = m.trailing_zeros() as usize;
                  let idx = (base + off) & self.mask;
                  let slot = &*self.slots.add(idx);
                  if slot.key == *key {
                     *self.ctrl.add(idx) = TOMBSTONE_TAG;
                     if idx < 16 {
                        *self.ctrl.add(self.capacity + idx) = TOMBSTONE_TAG;
                     }
                     self.len.fetch_sub(1, Ordering::Relaxed);
                     return true;
                  }
                  m &= m - 1;
               }
            }
            if empty_mask != 0 {
               return false;
            }
         }
         probes += 16;
         base = (base + 16) & self.mask;
      }
   }
}

impl<K, V> Drop for FlatShard<K, V> {
   fn drop(&mut self) {
      if !self.ctrl.is_null() {
         let ctrl_layout = Layout::from_size_align(self.capacity + 16, 64).expect("ctrl layout");
         unsafe { rust_zmalloc::dealloc_raw(self.ctrl.cast(), ctrl_layout) };
      }
      if !self.slots.is_null() {
         let slots_layout = Layout::from_size_align(
            (self.capacity + 16) * std::mem::size_of::<FlatSlot<K, V>>(),
            64,
         )
         .expect("slots layout");
         unsafe { rust_zmalloc::dealloc_raw(self.slots.cast(), slots_layout) };
      }
   }
}

pub struct FlatSwissMap<K, V, S = FixedState> {
   shards: Arc<[FlatShard<K, V>]>,
   shift: u32,
   shard_mask: usize,
   hasher: S,
}

impl<K, V, S> FlatSwissMap<K, V, S> {
   pub const DEFAULT_SHARDS: usize = 128;
}

impl<K: Copy + PartialEq + Hash, V: Copy> FlatSwissMap<K, V, FixedState> {
   pub fn new(expected_capacity: usize) -> Self {
      Self::with_shards(Self::DEFAULT_SHARDS, expected_capacity)
   }

   pub fn with_shards(num_shards: usize, expected_capacity: usize) -> Self {
      Self::with_hasher(FixedState::default(), num_shards, expected_capacity)
   }
}

impl<K: Copy + PartialEq + Hash, V: Copy> FlatSwissMap<K, V, FastState> {
   pub fn with_fast_hasher(expected_capacity: usize) -> Self {
      Self::with_hasher(FastState, Self::DEFAULT_SHARDS, expected_capacity)
   }
}

impl<K: Copy + PartialEq + Hash, V: Copy, S: BuildHasher> FlatSwissMap<K, V, S> {
   pub fn with_hasher(hasher: S, num_shards: usize, expected_capacity: usize) -> Self {
      let n = num_shards.next_power_of_two().max(1);
      let per_shard = (expected_capacity / n).next_power_of_two().max(16);
      let mut shards = Vec::with_capacity(n);
      for _ in 0..n {
         shards.push(FlatShard::new(per_shard * 2));
      }
      FlatSwissMap {
         shards: shards.into_boxed_slice().into(),
         shift: 64 - n.trailing_zeros(),
         shard_mask: n - 1,
         hasher,
      }
   }

   #[inline(always)]
   fn shard_idx(&self, hash: u64) -> usize {
      ((hash >> self.shift) as usize) & self.shard_mask
   }

   #[inline(always)]
   pub fn get(&self, key: &K) -> Option<V> {
      let h = self.hasher.hash_one(key);
      let s = self.shard_idx(h);
      unsafe { self.shards.get_unchecked(s) }.get(key, h)
   }

   #[inline(always)]
   pub fn get_hinted(&self, key: &K, hash: u64) -> Option<V> {
      let s = self.shard_idx(hash);
      unsafe { self.shards.get_unchecked(s) }.get(key, hash)
   }

   #[inline(always)]
   pub fn prefetch_key(&self, key: &K) -> u64 {
      #[cfg(target_arch = "x86_64")]
      use std::arch::x86_64::*;
      let h = self.hasher.hash_one(*key);
      let s = self.shard_idx(h);
      let shard = unsafe { self.shards.get_unchecked(s) };
      let base = (h as usize) & shard.mask;
      #[cfg(target_arch = "x86_64")]
      unsafe {
         _mm_prefetch(shard.ctrl.add(base).cast::<i8>(), _MM_HINT_T0);
         _mm_prefetch(shard.slots.add(base).cast::<i8>(), _MM_HINT_T0);
      }
      h
   }

   #[inline(always)]
   pub fn get_batch<const N: usize>(&self, keys: &[K; N], out: &mut [Option<V>; N]) {
      #[cfg(target_arch = "x86_64")]
      use std::arch::x86_64::*;
      let mut hashes = [0u64; N];
      for i in 0..N {
         let h = self.hasher.hash_one(keys[i]);
         hashes[i] = h;
         let s = self.shard_idx(h);
         let shard = unsafe { self.shards.get_unchecked(s) };
         let base = (h as usize) & shard.mask;
         #[cfg(target_arch = "x86_64")]
         unsafe {
            _mm_prefetch(shard.ctrl.add(base).cast::<i8>(), _MM_HINT_T0);
            _mm_prefetch(shard.slots.add(base).cast::<i8>(), _MM_HINT_T0);
         }
      }
      for i in 0..N {
         out[i] = self.get_hinted(&keys[i], hashes[i]);
      }
   }

   #[inline(always)]
   pub fn set(&self, key: K, val: V) {
      let h = self.hasher.hash_one(key);
      let s = self.shard_idx(h);
      unsafe { self.shards.get_unchecked(s) }.set(key, val, h);
   }

   #[inline(always)]
   pub fn set_hinted(&self, key: K, val: V, hash: u64) {
      let s = self.shard_idx(hash);
      unsafe { self.shards.get_unchecked(s) }.set(key, val, hash);
   }

   #[inline(always)]
   pub fn set_dual(&self, k0: K, v0: V, h0: u64, k1: K, v1: V, h1: u64) {
      let s0 = self.shard_idx(h0);
      let s1 = self.shard_idx(h1);
      if s0 == s1 {
         unsafe { self.shards.get_unchecked(s0) }.set_dual(k0, v0, h0, k1, v1, h1);
      } else {
         unsafe {
            self.shards.get_unchecked(s0).set(k0, v0, h0);
            self.shards.get_unchecked(s1).set(k1, v1, h1);
         }
      }
   }

   #[inline(always)]
   pub fn set_batch<const N: usize>(&self, keys: &[K; N], vals: &[V; N]) {
      #[cfg(target_arch = "x86_64")]
      use std::arch::x86_64::*;
      let mut hashes = [0u64; N];
      for i in 0..N {
         let h = self.hasher.hash_one(keys[i]);
         hashes[i] = h;
         let s = self.shard_idx(h);
         let shard = unsafe { self.shards.get_unchecked(s) };
         let base = (h as usize) & shard.mask;
         #[cfg(target_arch = "x86_64")]
         unsafe {
            _mm_prefetch(shard.ctrl.add(base).cast::<i8>(), _MM_HINT_T0);
            _mm_prefetch(shard.slots.add(base).cast::<i8>(), _MM_HINT_T0);
         }
      }
      for i in 0..N {
         self.set_hinted(keys[i], vals[i], hashes[i]);
      }
   }

   #[inline(always)]
   pub fn insert(&self, key: K, val: V) -> bool {
      let h = self.hasher.hash_one(key);
      let s = self.shard_idx(h);
      unsafe { self.shards.get_unchecked(s) }.insert(key, val, h)
   }

   #[inline(always)]
   pub fn insert_hinted(&self, key: K, val: V, hash: u64) -> bool {
      let s = self.shard_idx(hash);
      unsafe { self.shards.get_unchecked(s) }.insert(key, val, hash)
   }

   #[inline(always)]
   pub fn remove(&self, key: &K) -> bool {
      let h = self.hasher.hash_one(key);
      let s = self.shard_idx(h);
      unsafe { self.shards.get_unchecked(s) }.remove(key, h)
   }

   #[inline(always)]
   pub fn hasher(&self) -> &S {
      &self.hasher
   }

   #[inline(always)]
   pub fn remove_hinted(&self, key: &K, hash: u64) -> bool {
      let s = self.shard_idx(hash);
      unsafe { self.shards.get_unchecked(s) }.remove(key, hash)
   }

   #[inline(always)]
   pub fn contains_key(&self, key: &K) -> bool {
      self.get(key).is_some()
   }

   pub fn len(&self) -> usize {
      self
         .shards
         .iter()
         .map(|s| s.len.load(Ordering::Relaxed))
         .sum()
   }

   pub fn is_empty(&self) -> bool {
      self.len() == 0
   }
}

impl<K, V, S: Clone> Clone for FlatSwissMap<K, V, S> {
   fn clone(&self) -> Self {
      FlatSwissMap {
         shards: Arc::clone(&self.shards),
         shift: self.shift,
         shard_mask: self.shard_mask,
         hasher: self.hasher.clone(),
      }
   }
}

pub const SLOTS_PER_CHUNK: usize = 7;

#[derive(Clone, Copy, Default, Debug)]
#[repr(C)]
pub struct ChunkSlot<K, V> {
   pub key: K,
   pub val: V,
}

#[repr(C, align(128))]
pub struct Chunk<K, V> {
   pub ctrl: [u8; 16],
   pub slots: [ChunkSlot<K, V>; SLOTS_PER_CHUNK],
}

pub struct ChunkTable<K, V> {
   chunks: *mut Chunk<K, V>,
   num_chunks: usize,
   chunk_mask: usize,
   len: CachePadded<AtomicUsize>,
}

unsafe impl<K: Send, V: Send> Send for ChunkTable<K, V> {}
unsafe impl<K: Sync, V: Sync> Sync for ChunkTable<K, V> {}

impl<K: Copy + PartialEq, V: Copy> ChunkTable<K, V> {
   pub fn new(expected_capacity: usize) -> Self {
      let needed = (expected_capacity / SLOTS_PER_CHUNK * 2).next_power_of_two().max(16);
      let layout = Layout::from_size_align(
         needed * std::mem::size_of::<Chunk<K, V>>(),
         128,
      )
      .expect("chunk layout");

      let ptr = unsafe { rust_zmalloc::alloc_raw_zeroed(layout) }.cast::<Chunk<K, V>>();
      if ptr.is_null() {
         std::alloc::handle_alloc_error(layout);
      }
      unsafe {
         for i in 0..needed {
            let chunk = &mut *ptr.add(i);
            chunk.ctrl.fill(EMPTY_TAG);
         }
      }

      ChunkTable {
         chunks: ptr,
         num_chunks: needed,
         chunk_mask: needed - 1,
         len: CachePadded::new(AtomicUsize::new(0)),
      }
   }

   #[inline(always)]
   pub fn prefetch(&self, hash: u64) {
      #[cfg(target_arch = "x86_64")]
      unsafe {
         use std::arch::x86_64::*;
         let idx = (hash as usize) & self.chunk_mask;
         _mm_prefetch(self.chunks.add(idx).cast::<i8>(), _MM_HINT_T0);
      }
   }

   #[inline(always)]
   pub fn get(&self, key: &K, hash: u64) -> Option<V> {
      let h2 = ctrl_h2(hash);
      let mut c_idx = (hash as usize) & self.chunk_mask;

      #[cfg(target_arch = "x86_64")]
      use std::arch::x86_64::*;
      #[cfg(target_arch = "x86_64")]
      let target = unsafe { _mm_set1_epi8(h2 as i8) };
      #[cfg(target_arch = "x86_64")]
      let empty = unsafe { _mm_set1_epi8(-1i8) };

      loop {
         unsafe {
            let chunk = &*self.chunks.add(c_idx);
            #[cfg(target_arch = "x86_64")]
            {
               let group = _mm_load_si128(chunk.ctrl.as_ptr().cast::<__m128i>());
               let match_mask = (_mm_movemask_epi8(_mm_cmpeq_epi8(group, target)) as u32) & 0x7F;
               if match_mask != 0 {
                  let offset = match_mask.trailing_zeros() as usize;
                  if chunk.slots[offset].key == *key {
                     return Some(chunk.slots[offset].val);
                  }
                  let mut m = match_mask & (match_mask - 1);
                  while m != 0 {
                     let off = m.trailing_zeros() as usize;
                     if chunk.slots[off].key == *key {
                        return Some(chunk.slots[off].val);
                     }
                     m &= m - 1;
                  }
               }

               let empty_mask = (_mm_movemask_epi8(_mm_cmpeq_epi8(group, empty)) as u32) & 0x7F;
               if empty_mask != 0 {
                  return None;
               }
            }
            #[cfg(not(target_arch = "x86_64"))]
            {
               for i in 0..SLOTS_PER_CHUNK {
                  if chunk.ctrl[i] == h2 && chunk.slots[i].key == *key {
                     return Some(chunk.slots[i].val);
                  }
                  if chunk.ctrl[i] == EMPTY_TAG {
                     return None;
                  }
               }
            }
         }
         c_idx = (c_idx + 1) & self.chunk_mask;
      }
   }

   #[inline(always)]
   pub fn set(&self, key: K, val: V, hash: u64) {
      let h2 = ctrl_h2(hash);
      let mut c_idx = (hash as usize) & self.chunk_mask;

      #[cfg(target_arch = "x86_64")]
      use std::arch::x86_64::*;
      #[cfg(target_arch = "x86_64")]
      let target = unsafe { _mm_set1_epi8(h2 as i8) };
      #[cfg(target_arch = "x86_64")]
      let empty = unsafe { _mm_set1_epi8(-1i8) };

      loop {
         unsafe {
            let chunk = &mut *self.chunks.add(c_idx);
            #[cfg(target_arch = "x86_64")]
            {
               let group = _mm_load_si128(chunk.ctrl.as_ptr().cast::<__m128i>());
               let match_mask = (_mm_movemask_epi8(_mm_cmpeq_epi8(group, target)) as u32) & 0x7F;
               if match_mask != 0 {
                  let offset = match_mask.trailing_zeros() as usize;
                  if chunk.slots[offset].key == key {
                     chunk.slots[offset].val = val;
                     return;
                  }
                  let mut m = match_mask & (match_mask - 1);
                  while m != 0 {
                     let off = m.trailing_zeros() as usize;
                     if chunk.slots[off].key == key {
                        chunk.slots[off].val = val;
                        return;
                     }
                     m &= m - 1;
                  }
               }

               let empty_mask = (_mm_movemask_epi8(_mm_cmpeq_epi8(group, empty)) as u32) & 0x7F;
               if empty_mask != 0 {
                  let offset = empty_mask.trailing_zeros() as usize;
                  chunk.ctrl[offset] = h2;
                  chunk.slots[offset].key = key;
                  chunk.slots[offset].val = val;
                  self.len.fetch_add(1, Ordering::Relaxed);
                  return;
               }
            }
            #[cfg(not(target_arch = "x86_64"))]
            {
               for i in 0..SLOTS_PER_CHUNK {
                  if chunk.ctrl[i] == h2 && chunk.slots[i].key == key {
                     chunk.slots[i].val = val;
                     return;
                  }
                  if chunk.ctrl[i] == EMPTY_TAG {
                     chunk.ctrl[i] = h2;
                     chunk.slots[i].key = key;
                     chunk.slots[i].val = val;
                     self.len.fetch_add(1, Ordering::Relaxed);
                     return;
                  }
               }
            }
         }
         c_idx = (c_idx + 1) & self.chunk_mask;
      }
   }

   #[inline(always)]
   pub fn set_dual(&self, k0: K, v0: V, h0: u64, k1: K, v1: V, h1: u64) {
      #[cfg(target_arch = "x86_64")]
      unsafe {
         use std::arch::x86_64::*;
         let h2_0 = ctrl_h2(h0);
         let h2_1 = ctrl_h2(h1);
         let c_idx0 = (h0 as usize) & self.chunk_mask;
         let c_idx1 = (h1 as usize) & self.chunk_mask;

         let target0 = _mm_set1_epi8(h2_0 as i8);
         let target1 = _mm_set1_epi8(h2_1 as i8);
         let empty = _mm_set1_epi8(-1i8);

         let chunk0 = &mut *self.chunks.add(c_idx0);
         let chunk1 = &mut *self.chunks.add(c_idx1);

         let group0 = _mm_load_si128(chunk0.ctrl.as_ptr().cast::<__m128i>());
         let group1 = _mm_load_si128(chunk1.ctrl.as_ptr().cast::<__m128i>());

         let match0 = (_mm_movemask_epi8(_mm_cmpeq_epi8(group0, target0)) as u32) & 0x7F;
         let match1 = (_mm_movemask_epi8(_mm_cmpeq_epi8(group1, target1)) as u32) & 0x7F;

         if match0 != 0 {
            let offset0 = match0.trailing_zeros() as usize;
            if chunk0.slots[offset0].key == k0 {
               chunk0.slots[offset0].val = v0;
            } else {
               self.set(k0, v0, h0);
            }
         } else {
            let empty0 = (_mm_movemask_epi8(_mm_cmpeq_epi8(group0, empty)) as u32) & 0x7F;
            if empty0 != 0 {
               let offset0 = empty0.trailing_zeros() as usize;
               chunk0.ctrl[offset0] = h2_0;
               chunk0.slots[offset0].key = k0;
               chunk0.slots[offset0].val = v0;
               self.len.fetch_add(1, Ordering::Relaxed);
            } else {
               self.set(k0, v0, h0);
            }
         }

         if match1 != 0 {
            let offset1 = match1.trailing_zeros() as usize;
            if chunk1.slots[offset1].key == k1 {
               chunk1.slots[offset1].val = v1;
            } else {
               self.set(k1, v1, h1);
            }
         } else {
            let empty1 = (_mm_movemask_epi8(_mm_cmpeq_epi8(group1, empty)) as u32) & 0x7F;
            if empty1 != 0 {
               let offset1 = empty1.trailing_zeros() as usize;
               chunk1.ctrl[offset1] = h2_1;
               chunk1.slots[offset1].key = k1;
               chunk1.slots[offset1].val = v1;
               self.len.fetch_add(1, Ordering::Relaxed);
            } else {
               self.set(k1, v1, h1);
            }
         }
      }
      #[cfg(not(target_arch = "x86_64"))]
      {
         self.set(k0, v0, h0);
         self.set(k1, v1, h1);
      }
   }

   #[inline(always)]
   pub fn insert(&self, key: K, val: V, hash: u64) -> bool {
      let h2 = ctrl_h2(hash);
      let mut c_idx = (hash as usize) & self.chunk_mask;

      #[cfg(target_arch = "x86_64")]
      use std::arch::x86_64::*;
      #[cfg(target_arch = "x86_64")]
      let target = unsafe { _mm_set1_epi8(h2 as i8) };
      #[cfg(target_arch = "x86_64")]
      let empty = unsafe { _mm_set1_epi8(-1i8) };

      loop {
         unsafe {
            let chunk = &mut *self.chunks.add(c_idx);
            #[cfg(target_arch = "x86_64")]
            {
               let group = _mm_load_si128(chunk.ctrl.as_ptr().cast::<__m128i>());
               let match_mask = (_mm_movemask_epi8(_mm_cmpeq_epi8(group, target)) as u32) & 0x7F;
               if match_mask != 0 {
                  let offset = match_mask.trailing_zeros() as usize;
                  if chunk.slots[offset].key == key {
                     chunk.slots[offset].val = val;
                     return false;
                  }
                  let mut m = match_mask & (match_mask - 1);
                  while m != 0 {
                     let off = m.trailing_zeros() as usize;
                     if chunk.slots[off].key == key {
                        chunk.slots[off].val = val;
                        return false;
                     }
                     m &= m - 1;
                  }
               }

               let empty_mask = (_mm_movemask_epi8(_mm_cmpeq_epi8(group, empty)) as u32) & 0x7F;
               if empty_mask != 0 {
                  let offset = empty_mask.trailing_zeros() as usize;
                  chunk.ctrl[offset] = h2;
                  chunk.slots[offset].key = key;
                  chunk.slots[offset].val = val;
                  self.len.fetch_add(1, Ordering::Relaxed);
                  return true;
               }
            }
            #[cfg(not(target_arch = "x86_64"))]
            {
               for i in 0..SLOTS_PER_CHUNK {
                  if chunk.ctrl[i] == h2 && chunk.slots[i].key == key {
                     chunk.slots[i].val = val;
                     return false;
                  }
                  if chunk.ctrl[i] == EMPTY_TAG {
                     chunk.ctrl[i] = h2;
                     chunk.slots[i].key = key;
                     chunk.slots[i].val = val;
                     self.len.fetch_add(1, Ordering::Relaxed);
                     return true;
                  }
               }
            }
         }
         c_idx = (c_idx + 1) & self.chunk_mask;
      }
   }

   #[inline(always)]
   pub fn remove(&self, key: &K, hash: u64) -> bool {
      let h2 = ctrl_h2(hash);
      let mut c_idx = (hash as usize) & self.chunk_mask;

      #[cfg(target_arch = "x86_64")]
      use std::arch::x86_64::*;
      #[cfg(target_arch = "x86_64")]
      let target = unsafe { _mm_set1_epi8(h2 as i8) };
      #[cfg(target_arch = "x86_64")]
      let empty = unsafe { _mm_set1_epi8(-1i8) };

      loop {
         unsafe {
            let chunk = &mut *self.chunks.add(c_idx);
            #[cfg(target_arch = "x86_64")]
            {
               let group = _mm_load_si128(chunk.ctrl.as_ptr().cast::<__m128i>());
               let match_mask = (_mm_movemask_epi8(_mm_cmpeq_epi8(group, target)) as u32) & 0x7F;
               if match_mask != 0 {
                  let offset = match_mask.trailing_zeros() as usize;
                  if chunk.slots[offset].key == *key {
                     chunk.ctrl[offset] = TOMBSTONE_TAG;
                     self.len.fetch_sub(1, Ordering::Relaxed);
                     return true;
                  }
                  let mut m = match_mask & (match_mask - 1);
                  while m != 0 {
                     let off = m.trailing_zeros() as usize;
                     if chunk.slots[off].key == *key {
                        chunk.ctrl[off] = TOMBSTONE_TAG;
                        self.len.fetch_sub(1, Ordering::Relaxed);
                        return true;
                     }
                     m &= m - 1;
                  }
               }

               let empty_mask = (_mm_movemask_epi8(_mm_cmpeq_epi8(group, empty)) as u32) & 0x7F;
               if empty_mask != 0 {
                  return false;
               }
            }
            #[cfg(not(target_arch = "x86_64"))]
            {
               for i in 0..SLOTS_PER_CHUNK {
                  if chunk.ctrl[i] == h2 && chunk.slots[i].key == *key {
                     chunk.ctrl[i] = TOMBSTONE_TAG;
                     self.len.fetch_sub(1, Ordering::Relaxed);
                     return true;
                  }
                  if chunk.ctrl[i] == EMPTY_TAG {
                     return false;
                  }
               }
            }
         }
         c_idx = (c_idx + 1) & self.chunk_mask;
      }
   }
}

impl<K, V> Drop for ChunkTable<K, V> {
   fn drop(&mut self) {
      if !self.chunks.is_null() {
         let layout = Layout::from_size_align(
            self.num_chunks * std::mem::size_of::<Chunk<K, V>>(),
            128,
         )
         .expect("chunk layout");
         unsafe { rust_zmalloc::dealloc_raw(self.chunks.cast(), layout) };
      }
   }
}

pub struct ChunkMap<K, V, S = FastState> {
   table: Arc<ChunkTable<K, V>>,
   hasher: S,
}

impl<K: Copy + PartialEq + Hash, V: Copy> ChunkMap<K, V, FastState> {
   pub fn new(expected_capacity: usize) -> Self {
      Self::with_hasher(FastState, expected_capacity)
   }
}

impl<K: Copy + PartialEq + Hash, V: Copy, S: BuildHasher> ChunkMap<K, V, S> {
   pub fn with_hasher(hasher: S, expected_capacity: usize) -> Self {
      ChunkMap {
         table: Arc::new(ChunkTable::new(expected_capacity)),
         hasher,
      }
   }

   #[inline(always)]
   pub fn get(&self, key: &K) -> Option<V> {
      let h = self.hasher.hash_one(*key);
      self.table.get(key, h)
   }

   #[inline(always)]
   pub fn get_hinted(&self, key: &K, hash: u64) -> Option<V> {
      self.table.get(key, hash)
   }

   #[inline(always)]
   pub fn set(&self, key: K, val: V) {
      let h = self.hasher.hash_one(key);
      self.table.set(key, val, h);
   }

   #[inline(always)]
   pub fn set_hinted(&self, key: K, val: V, hash: u64) {
      self.table.set(key, val, hash);
   }

   #[inline(always)]
   pub fn set_dual(&self, k0: K, v0: V, h0: u64, k1: K, v1: V, h1: u64) {
      self.table.set_dual(k0, v0, h0, k1, v1, h1);
   }

   #[inline(always)]
   pub fn insert(&self, key: K, val: V) -> bool {
      let h = self.hasher.hash_one(key);
      self.table.insert(key, val, h)
   }

   #[inline(always)]
   pub fn insert_hinted(&self, key: K, val: V, hash: u64) -> bool {
      self.table.insert(key, val, hash)
   }

   #[inline(always)]
   pub fn remove(&self, key: &K) -> bool {
      let h = self.hasher.hash_one(*key);
      self.table.remove(key, h)
   }

   #[inline(always)]
   pub fn remove_hinted(&self, key: &K, hash: u64) -> bool {
      self.table.remove(key, hash)
   }

   #[inline(always)]
   pub fn contains_key(&self, key: &K) -> bool {
      self.get(key).is_some()
   }

   #[inline(always)]
   pub fn prefetch(&self, key: &K) -> u64 {
      let h = self.hasher.hash_one(*key);
      self.table.prefetch(h);
      h
   }

   #[inline(always)]
   pub fn get_batch<const N: usize>(&self, keys: &[K; N], out: &mut [Option<V>; N]) {
      let mut hashes = [0u64; N];
      for i in 0..N {
         let h = self.hasher.hash_one(keys[i]);
         hashes[i] = h;
         self.table.prefetch(h);
      }
      for i in 0..N {
         out[i] = self.table.get(&keys[i], hashes[i]);
      }
   }

   #[inline(always)]
   pub fn set_batch<const N: usize>(&self, keys: &[K; N], vals: &[V; N]) {
      let mut hashes = [0u64; N];
      for i in 0..N {
         let h = self.hasher.hash_one(keys[i]);
         hashes[i] = h;
         self.table.prefetch(h);
      }
      for i in 0..N {
         self.table.set(keys[i], vals[i], hashes[i]);
      }
   }

   pub fn len(&self) -> usize {
      self.table.len.load(Ordering::Relaxed)
   }

   pub fn is_empty(&self) -> bool {
      self.len() == 0
   }

   pub fn hasher(&self) -> &S {
      &self.hasher
   }
}

impl<K, V, S: Clone> Clone for ChunkMap<K, V, S> {
   fn clone(&self) -> Self {
      ChunkMap {
         table: Arc::clone(&self.table),
         hasher: self.hasher.clone(),
      }
   }
}

pub struct TieredSwissMap<K, V, S = FastState> {
   hot: Arc<ChunkTable<K, V>>,
   main: Arc<ChunkTable<K, V>>,
   hasher: S,
}

impl<K: Copy + PartialEq + Hash, V: Copy> TieredSwissMap<K, V, FastState> {
   pub fn new(hot_capacity: usize, main_capacity: usize) -> Self {
      Self::with_hasher(FastState, hot_capacity, main_capacity)
   }
}

impl<K: Copy + PartialEq + Hash, V: Copy, S: BuildHasher> TieredSwissMap<K, V, S> {
   pub fn with_hasher(hasher: S, hot_capacity: usize, main_capacity: usize) -> Self {
      TieredSwissMap {
         hot: Arc::new(ChunkTable::new(hot_capacity)),
         main: Arc::new(ChunkTable::new(main_capacity)),
         hasher,
      }
   }

   #[inline(always)]
   pub fn get(&self, key: &K) -> Option<V> {
      let h = self.hasher.hash_one(*key);
      if let Some(v) = self.hot.get(key, h) {
         return Some(v);
      }
      self.main.get(key, h)
   }

   #[inline(always)]
   pub fn get_hinted(&self, key: &K, hash: u64) -> Option<V> {
      if let Some(v) = self.hot.get(key, hash) {
         return Some(v);
      }
      self.main.get(key, hash)
   }

   #[inline(always)]
   pub fn set(&self, key: K, val: V, is_hot: bool) {
      let h = self.hasher.hash_one(key);
      if is_hot {
         self.hot.set(key, val, h);
      }
      self.main.set(key, val, h);
   }

   #[inline(always)]
   pub fn set_hinted(&self, key: K, val: V, hash: u64, is_hot: bool) {
      if is_hot {
         self.hot.set(key, val, hash);
      }
      self.main.set(key, val, hash);
   }

   #[inline(always)]
   #[allow(clippy::too_many_arguments)]
   pub fn set_dual(&self, k0: K, v0: V, h0: u64, is_hot0: bool, k1: K, v1: V, h1: u64, is_hot1: bool) {
      if is_hot0 && is_hot1 {
         self.hot.set_dual(k0, v0, h0, k1, v1, h1);
      } else {
         if is_hot0 { self.hot.set(k0, v0, h0); }
         if is_hot1 { self.hot.set(k1, v1, h1); }
      }
      self.main.set_dual(k0, v0, h0, k1, v1, h1);
   }

   #[inline(always)]
   pub fn insert(&self, key: K, val: V, is_hot: bool) -> bool {
      let h = self.hasher.hash_one(key);
      if is_hot {
         self.hot.set(key, val, h);
      }
      self.main.insert(key, val, h)
   }

   #[inline(always)]
   pub fn remove(&self, key: &K) -> bool {
      let h = self.hasher.hash_one(*key);
      let r1 = self.hot.remove(key, h);
      let r2 = self.main.remove(key, h);
      r1 || r2
   }

   #[inline(always)]
   pub fn contains_key(&self, key: &K) -> bool {
      self.get(key).is_some()
   }

   #[inline(always)]
   pub fn prefetch(&self, key: &K) -> u64 {
      let h = self.hasher.hash_one(*key);
      self.hot.prefetch(h);
      self.main.prefetch(h);
      h
   }

   pub fn len(&self) -> usize {
      self.main.len.load(Ordering::Relaxed)
   }

   pub fn is_empty(&self) -> bool {
      self.len() == 0
   }

   pub fn hasher(&self) -> &S {
      &self.hasher
   }
}

impl<K, V, S: Clone> Clone for TieredSwissMap<K, V, S> {
   fn clone(&self) -> Self {
      TieredSwissMap {
         hot: Arc::clone(&self.hot),
         main: Arc::clone(&self.main),
         hasher: self.hasher.clone(),
      }
   }
}

#[cfg(test)]
mod tests {
   use super::{ChunkMap, FlatSwissMap, TieredSwissMap};

   #[test]
   fn test_flat_map_crud() {
      let map = FlatSwissMap::<u64, u64>::new(1_000);
      assert_eq!(map.len(), 0);
      assert!(map.is_empty());

      for i in 0..500u64 {
         assert!(map.insert(i, i * 10));
      }
      assert_eq!(map.len(), 500);

      for i in 0..500u64 {
         assert_eq!(map.get(&i), Some(i * 10));
      }
      assert_eq!(map.get(&999), None);
      assert!(!map.contains_key(&999));

      for i in 0..500u64 {
         assert!(map.remove(&i));
      }
      assert_eq!(map.len(), 0);
      assert_eq!(map.get(&0), None);
   }

   #[test]
   fn test_flat_map_set_overwrite() {
      let map = FlatSwissMap::<u64, u64>::new(1_000);
      for i in 0..500u64 {
         map.set(i, i * 10);
      }
      assert_eq!(map.len(), 500);
      for i in 0..500u64 {
         assert_eq!(map.get(&i), Some(i * 10));
      }
      for i in 0..500u64 {
         map.set(i, i * 20);
      }
      assert_eq!(map.len(), 500);
      for i in 0..500u64 {
         assert_eq!(map.get(&i), Some(i * 20));
      }
   }

   #[test]
   fn test_flat_map_concurrent_reads() {
      let map = FlatSwissMap::<u64, u64>::new(10_000);
      for i in 0..5_000u64 {
         map.insert(i, i * 2);
      }

      std::thread::scope(|s| {
         for _ in 0..8 {
            let map = map.clone();
            s.spawn(move || {
               for i in 0..5_000u64 {
                  assert_eq!(map.get(&i), Some(i * 2));
               }
            });
         }
      });
   }

   #[test]
   fn test_chunk_map_crud() {
      let map = ChunkMap::<u64, u64>::new(1_000);
      assert_eq!(map.len(), 0);
      assert!(map.is_empty());

      for i in 0..500u64 {
         assert!(map.insert(i, i * 10));
      }
      assert_eq!(map.len(), 500);

      for i in 0..500u64 {
         assert_eq!(map.get(&i), Some(i * 10));
      }
      assert_eq!(map.get(&999), None);
      assert!(!map.contains_key(&999));

      for i in 0..500u64 {
         map.set(i, i * 20);
      }
      assert_eq!(map.len(), 500);
      for i in 0..500u64 {
         assert_eq!(map.get(&i), Some(i * 20));
      }

      for i in 0..500u64 {
         assert!(map.remove(&i));
      }
      assert_eq!(map.len(), 0);
      assert_eq!(map.get(&0), None);
   }

   #[test]
   fn test_chunk_map_batch() {
      let map = ChunkMap::<u64, u64>::new(100);
      let keys = [1u64, 2, 3, 4, 5, 6, 7, 8];
      let vals = [10u64, 20, 30, 40, 50, 60, 70, 80];
      map.set_batch(&keys, &vals);

      let mut out = [None; 8];
      map.get_batch(&keys, &mut out);
      for i in 0..8 {
         assert_eq!(out[i], Some(vals[i]));
      }
   }

   #[test]
   fn test_tiered_swiss_map_crud() {
      let map = TieredSwissMap::<u64, u64>::new(100, 1_000);
      assert_eq!(map.len(), 0);
      assert!(map.is_empty());

      // Insert hot keys
      for i in 0..50u64 {
         map.set(i, i * 10, true);
      }
      // Insert cold keys
      for i in 50..200u64 {
         map.set(i, i * 10, false);
      }

      for i in 0..200u64 {
         assert_eq!(map.get(&i), Some(i * 10));
      }
      assert_eq!(map.get(&999), None);

      for i in 0..200u64 {
         assert!(map.remove(&i));
      }
      assert_eq!(map.get(&0), None);
   }
}
