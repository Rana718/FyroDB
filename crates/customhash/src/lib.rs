mod access;
pub use access::{ReadEpochGuard, WriteEpochGuard};
mod ebr;
mod key;
mod ops;
mod shard;
mod snap;
pub use snap::SnapSink;

#[repr(align(128))]
#[derive(Default)]
pub struct Flag(AtomicUsize);

impl Flag {
   #[inline(always)]
   pub fn load(&self, order: Ordering) -> bool {
      self.0.load(order) != 0
   }

   pub fn store(&self, on: bool, order: Ordering) {
      self.set_bit(1, on, order);
   }

   pub fn set_watching(&self, on: bool) {
      self.set_bit(2, on, Ordering::Release);
   }

   #[inline]
   pub fn is_watching(&self) -> bool {
      self.0.load(Ordering::Relaxed) & 2 != 0
   }

   fn set_bit(&self, bit: usize, on: bool, order: Ordering) {
      if on {
         self.0.fetch_or(bit, order);
      } else {
         self.0.fetch_and(!bit, order);
      }
   }
}

pub use ebr::{Guard, force_collect, force_collect_quiescent, pin, retire_raw};

use std::marker::PhantomData;
use std::ops::Deref;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use crossbeam_utils::CachePadded;
use foldhash::fast::RandomState;
pub use key::CompactKey;
use std::hash::{BuildHasher, Hasher};

use shard::*;
pub use shard::{
   Entry, EntryReadGuard, FastSet, InsertFastResult, STATE_LOCK, STATE_MATCH_MASK, STATE_OCCUPIED,
   bump_seq, unlock_write,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Full;

impl std::fmt::Display for Full {
   fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
      f.write_str("command not allowed when used memory > 'maxmemory'.")
   }
}
impl std::error::Error for Full {}

pub struct ValueRef<'a, V> {
   read: EntryReadGuard<'a, V>,
   _guard: ebr::Guard,
   _map: PhantomData<&'a CustomMap<V>>,
}

impl<V> Deref for ValueRef<'_, V> {
   type Target = V;
   #[inline(always)]
   fn deref(&self) -> &V {
      &self.read
   }
}

// Notification always follows the write, so a draining reader sees a consistent snapshot.
pub trait WriteObserver: Send + Sync + 'static {
   fn on_write(&self, key: &str);
   /// The whole map was cleared.
   fn on_clear(&self);
}

pub struct CustomMap<V> {
   access: access::Access,
   shards: Box<[Shard<V>]>,
   shard_fast: Box<[ShardFast<V>]>,
   shift: u32,
   shard_mask: usize,
   hasher: RandomState,
   max_keys: usize,
   observer: std::sync::OnceLock<Box<dyn WriteObserver>>,
   /// One relaxed load on every write when nothing observes. Shared so
   /// the observer's owner can toggle it without a map reference.
   observing: std::sync::Arc<Flag>,
   observing_ptr: *const Flag,
   snap_active: AtomicBool,
   snap: std::sync::Arc<snap::SnapCtl<V>>,
   /// Net insert/remove count, kept exact by the same bump calls that
   /// update shard live counters: `is_full` reads one shared line instead
   /// of scanning every shard, which mattered whenever a key ceiling was
   /// configured. Reset exactly by `clear`.
   approx: CachePadded<AtomicUsize>,
}

impl<V: Clone + Send + Sync + 'static> CustomMap<V> {
   #[inline]
   pub fn write_epoch(&self) -> WriteEpochGuard<'_> {
      self.access.write()
   }

   #[inline]
   /// Protect a read-only batch; retain an EBR pin when using raw pointers.
   pub fn try_read_epoch(&self) -> Option<ReadEpochGuard<'_>> {
      self.access.try_read()
   }

   #[inline(always)]
   pub fn read_epoch_active(&self) -> bool {
      self.access.reading()
   }

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
      unsafe { (*self.observing_ptr).load(Ordering::Relaxed) }
   }

   /// A write to `key` completed.
   #[inline(always)]
   pub fn touched(&self, key: &str) {
      if self.observing() {
         self.notify(key);
      }
   }

   #[inline(always)]
   pub fn touched_bytes(&self, bytes: &[u8]) {
      if self.observing()
         && let Ok(key) = std::str::from_utf8(bytes)
      {
         self.notify(key);
      }
   }

   #[inline(always)]
   pub fn snap_before_write(&self, e: &Entry<V>) {
      if self.snap_active.load(Ordering::Relaxed) {
         self.snap.before_write(e);
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
         INITIAL_SHARD_CAPACITY,
         usize::MAX,
      )
   }

   pub fn with_capacity(shard_count: usize, expected_keys: usize) -> Self {
      let n = shard_count.next_power_of_two().max(1);
      let per_shard = if expected_keys == usize::MAX || expected_keys == 0 {
         INITIAL_SHARD_CAPACITY
      } else {
         let target = expected_keys.saturating_mul(LOAD_DEN) / LOAD_NUM / n;
         target.next_power_of_two().max(8)
      };
      Self::build(n, per_shard, expected_keys)
   }

   fn build(n: usize, per_shard: usize, max_keys: usize) -> Self {
      let hasher = RandomState::default();
      let snap = std::sync::Arc::new(snap::SnapCtl::new());
      let shards: Vec<_> = (0..n)
         .map(|_| Shard::new(per_shard, snap.clone()))
         .collect();
      let shard_fast: Box<[ShardFast<V>]> = shards
         .iter()
         .map(|s| {
            ShardFast::new(
               s.slots.load(Ordering::Relaxed),
               s.mask.load(Ordering::Relaxed),
            )
         })
         .collect::<Vec<_>>()
         .into_boxed_slice();
      for (i, s) in shards.iter().enumerate() {
         let sf_ptr = unsafe { shard_fast.as_ptr().add(i) } as *mut ShardFast<V>;
         s.fast.store(sf_ptr, Ordering::Relaxed);
      }
      let observing = std::sync::Arc::new(Flag::default());
      let observing_ptr = std::sync::Arc::as_ptr(&observing);
      CustomMap {
         access: access::Access::new(),
         shift: (u64::BITS - n.trailing_zeros()).min(63),
         shard_mask: n - 1,
         shards: shards.into_boxed_slice(),
         shard_fast,
         hasher,
         max_keys,
         observer: std::sync::OnceLock::new(),
         observing_ptr,
         observing,
         snap_active: AtomicBool::new(false),
         snap,
         approx: CachePadded::new(AtomicUsize::new(0)),
      }
   }

   #[inline(always)]
   fn locate(&self, key: &str) -> (u64, usize) {
      self.locate_bytes(key.as_bytes())
   }

   #[inline(always)]
   fn locate_bytes(&self, key: &[u8]) -> (u64, usize) {
      let mut hasher = self.hasher.build_hasher();
      hasher.write(key);
      hasher.write_u8(0xff);
      let h = hasher.finish();
      (h, self.shard_of(h))
   }

   #[inline(always)]
   pub fn locate_key(&self, key: &str) -> (u64, usize) {
      self.locate(key)
   }

   #[inline(always)]
   pub fn shard_of(&self, hash: u64) -> usize {
      ((hash >> self.shift) as usize) & self.shard_mask
   }

   #[inline(always)]
   pub fn probe_slot_entry(&self, hash: u64, idx: usize) -> *mut Entry<V> {
      let sf = unsafe { &*self.shard_fast.as_ptr().add(idx) };
      let (slots, mask) = sf.layout();
      let base = unsafe { slots.add((hash as usize) & mask) };
      unsafe { TaggedSlot::probe_from_base(base, hash, 16) }
   }

   #[inline(always)]
   pub fn prefetch_slots<K: AsRef<[u8]>>(
      &self,
      keys: &[K],
      hashes: &mut [u64],
      tables: &mut [*const ()],
   ) {
      let n = keys.len().min(hashes.len()).min(tables.len());
      let sf_base = self.shard_fast.as_ptr();
      let k_ptr = keys.as_ptr();
      let h_ptr = hashes.as_mut_ptr();
      let t_ptr = tables.as_mut_ptr();
      let mut i = 0;
      while i + 8 <= n {
         unsafe {
            let (h0, idx0) = self.locate_bytes((*k_ptr.add(i)).as_ref());
            let (h1, idx1) = self.locate_bytes((*k_ptr.add(i + 1)).as_ref());
            let (h2, idx2) = self.locate_bytes((*k_ptr.add(i + 2)).as_ref());
            let (h3, idx3) = self.locate_bytes((*k_ptr.add(i + 3)).as_ref());
            let (h4, idx4) = self.locate_bytes((*k_ptr.add(i + 4)).as_ref());
            let (h5, idx5) = self.locate_bytes((*k_ptr.add(i + 5)).as_ref());
            let (h6, idx6) = self.locate_bytes((*k_ptr.add(i + 6)).as_ref());
            let (h7, idx7) = self.locate_bytes((*k_ptr.add(i + 7)).as_ref());

            *h_ptr.add(i) = h0;
            *h_ptr.add(i + 1) = h1;
            *h_ptr.add(i + 2) = h2;
            *h_ptr.add(i + 3) = h3;
            *h_ptr.add(i + 4) = h4;
            *h_ptr.add(i + 5) = h5;
            *h_ptr.add(i + 6) = h6;
            *h_ptr.add(i + 7) = h7;

            let sf0 = &*sf_base.add(idx0);
            let sf1 = &*sf_base.add(idx1);
            let sf2 = &*sf_base.add(idx2);
            let sf3 = &*sf_base.add(idx3);
            let sf4 = &*sf_base.add(idx4);
            let sf5 = &*sf_base.add(idx5);
            let sf6 = &*sf_base.add(idx6);
            let sf7 = &*sf_base.add(idx7);

            let (slots0, mask0) = sf0.layout();
            let s0 = slots0.add((h0 as usize) & mask0);
            let (slots1, mask1) = sf1.layout();
            let s1 = slots1.add((h1 as usize) & mask1);
            let (slots2, mask2) = sf2.layout();
            let s2 = slots2.add((h2 as usize) & mask2);
            let (slots3, mask3) = sf3.layout();
            let s3 = slots3.add((h3 as usize) & mask3);
            let (slots4, mask4) = sf4.layout();
            let s4 = slots4.add((h4 as usize) & mask4);
            let (slots5, mask5) = sf5.layout();
            let s5 = slots5.add((h5 as usize) & mask5);
            let (slots6, mask6) = sf6.layout();
            let s6 = slots6.add((h6 as usize) & mask6);
            let (slots7, mask7) = sf7.layout();
            let s7 = slots7.add((h7 as usize) & mask7);

            prefetch_line(s0);
            prefetch_line(s1);
            prefetch_line(s2);
            prefetch_line(s3);
            prefetch_line(s4);
            prefetch_line(s5);
            prefetch_line(s6);
            prefetch_line(s7);

            *t_ptr.add(i) = s0.cast();
            *t_ptr.add(i + 1) = s1.cast();
            *t_ptr.add(i + 2) = s2.cast();
            *t_ptr.add(i + 3) = s3.cast();
            *t_ptr.add(i + 4) = s4.cast();
            *t_ptr.add(i + 5) = s5.cast();
            *t_ptr.add(i + 6) = s6.cast();
            *t_ptr.add(i + 7) = s7.cast();
         }
         i += 8;
      }
      while i < n {
         unsafe {
            let (h, idx) = self.locate_bytes((*k_ptr.add(i)).as_ref());
            *h_ptr.add(i) = h;
            let sf = &*sf_base.add(idx);
            let (slots, mask) = sf.layout();
            let s = slots.add((h as usize) & mask);
            prefetch_line(s);
            *t_ptr.add(i) = s.cast();
         }
         i += 1;
      }
   }

   #[inline(always)]
   pub fn prefetch_slots_write<K: AsRef<[u8]>>(
      &self,
      keys: &[K],
      hashes: &mut [u64],
      tables: &mut [*const ()],
   ) {
      let n = keys.len().min(hashes.len()).min(tables.len());
      let sf_base = self.shard_fast.as_ptr();
      let k_ptr = keys.as_ptr();
      let h_ptr = hashes.as_mut_ptr();
      let t_ptr = tables.as_mut_ptr();
      let mut i = 0;
      while i + 8 <= n {
         unsafe {
            let (h0, idx0) = self.locate_bytes((*k_ptr.add(i)).as_ref());
            let (h1, idx1) = self.locate_bytes((*k_ptr.add(i + 1)).as_ref());
            let (h2, idx2) = self.locate_bytes((*k_ptr.add(i + 2)).as_ref());
            let (h3, idx3) = self.locate_bytes((*k_ptr.add(i + 3)).as_ref());
            let (h4, idx4) = self.locate_bytes((*k_ptr.add(i + 4)).as_ref());
            let (h5, idx5) = self.locate_bytes((*k_ptr.add(i + 5)).as_ref());
            let (h6, idx6) = self.locate_bytes((*k_ptr.add(i + 6)).as_ref());
            let (h7, idx7) = self.locate_bytes((*k_ptr.add(i + 7)).as_ref());

            *h_ptr.add(i) = h0;
            *h_ptr.add(i + 1) = h1;
            *h_ptr.add(i + 2) = h2;
            *h_ptr.add(i + 3) = h3;
            *h_ptr.add(i + 4) = h4;
            *h_ptr.add(i + 5) = h5;
            *h_ptr.add(i + 6) = h6;
            *h_ptr.add(i + 7) = h7;

            let sf0 = &*sf_base.add(idx0);
            let sf1 = &*sf_base.add(idx1);
            let sf2 = &*sf_base.add(idx2);
            let sf3 = &*sf_base.add(idx3);
            let sf4 = &*sf_base.add(idx4);
            let sf5 = &*sf_base.add(idx5);
            let sf6 = &*sf_base.add(idx6);
            let sf7 = &*sf_base.add(idx7);

            let (slots0, mask0) = sf0.layout();
            let s0 = slots0.add((h0 as usize) & mask0);
            let (slots1, mask1) = sf1.layout();
            let s1 = slots1.add((h1 as usize) & mask1);
            let (slots2, mask2) = sf2.layout();
            let s2 = slots2.add((h2 as usize) & mask2);
            let (slots3, mask3) = sf3.layout();
            let s3 = slots3.add((h3 as usize) & mask3);
            let (slots4, mask4) = sf4.layout();
            let s4 = slots4.add((h4 as usize) & mask4);
            let (slots5, mask5) = sf5.layout();
            let s5 = slots5.add((h5 as usize) & mask5);
            let (slots6, mask6) = sf6.layout();
            let s6 = slots6.add((h6 as usize) & mask6);
            let (slots7, mask7) = sf7.layout();
            let s7 = slots7.add((h7 as usize) & mask7);

            prefetch_line_write(s0);
            prefetch_line_write(s1);
            prefetch_line_write(s2);
            prefetch_line_write(s3);
            prefetch_line_write(s4);
            prefetch_line_write(s5);
            prefetch_line_write(s6);
            prefetch_line_write(s7);

            *t_ptr.add(i) = s0.cast();
            *t_ptr.add(i + 1) = s1.cast();
            *t_ptr.add(i + 2) = s2.cast();
            *t_ptr.add(i + 3) = s3.cast();
            *t_ptr.add(i + 4) = s4.cast();
            *t_ptr.add(i + 5) = s5.cast();
            *t_ptr.add(i + 6) = s6.cast();
            *t_ptr.add(i + 7) = s7.cast();
         }
         i += 8;
      }
      while i < n {
         unsafe {
            let (h, idx) = self.locate_bytes((*k_ptr.add(i)).as_ref());
            *h_ptr.add(i) = h;
            let sf = &*sf_base.add(idx);
            let (slots, mask) = sf.layout();
            let s = slots.add((h as usize) & mask);
            prefetch_line_write(s);
            *t_ptr.add(i) = s.cast();
         }
         i += 1;
      }
   }

   #[inline(always)]
   pub fn probe_and_prefetch_entries(
      &self,
      tables: &[*const ()],
      hashes: &[u64],
      entries: &mut [*const ()],
   ) {
      let n = tables.len().min(hashes.len()).min(entries.len());
      let t_ptr = tables.as_ptr();
      let h_ptr = hashes.as_ptr();
      let e_ptr = entries.as_mut_ptr();
      let mut i = 0;
      while i + 8 <= n {
         unsafe {
            let b0 = (*t_ptr.add(i)).cast::<TaggedSlot<V>>();
            let b1 = (*t_ptr.add(i + 1)).cast::<TaggedSlot<V>>();
            let b2 = (*t_ptr.add(i + 2)).cast::<TaggedSlot<V>>();
            let b3 = (*t_ptr.add(i + 3)).cast::<TaggedSlot<V>>();
            let b4 = (*t_ptr.add(i + 4)).cast::<TaggedSlot<V>>();
            let b5 = (*t_ptr.add(i + 5)).cast::<TaggedSlot<V>>();
            let b6 = (*t_ptr.add(i + 6)).cast::<TaggedSlot<V>>();
            let b7 = (*t_ptr.add(i + 7)).cast::<TaggedSlot<V>>();

            let p0 = TaggedSlot::probe_from_base(b0, *h_ptr.add(i), 16);
            let p1 = TaggedSlot::probe_from_base(b1, *h_ptr.add(i + 1), 16);
            let p2 = TaggedSlot::probe_from_base(b2, *h_ptr.add(i + 2), 16);
            let p3 = TaggedSlot::probe_from_base(b3, *h_ptr.add(i + 3), 16);
            let p4 = TaggedSlot::probe_from_base(b4, *h_ptr.add(i + 4), 16);
            let p5 = TaggedSlot::probe_from_base(b5, *h_ptr.add(i + 5), 16);
            let p6 = TaggedSlot::probe_from_base(b6, *h_ptr.add(i + 6), 16);
            let p7 = TaggedSlot::probe_from_base(b7, *h_ptr.add(i + 7), 16);

            *e_ptr.add(i) = p0.cast();
            *e_ptr.add(i + 1) = p1.cast();
            *e_ptr.add(i + 2) = p2.cast();
            *e_ptr.add(i + 3) = p3.cast();
            *e_ptr.add(i + 4) = p4.cast();
            *e_ptr.add(i + 5) = p5.cast();
            *e_ptr.add(i + 6) = p6.cast();
            *e_ptr.add(i + 7) = p7.cast();

            prefetch_entry(p0);
            prefetch_entry(p1);
            prefetch_entry(p2);
            prefetch_entry(p3);
            prefetch_entry(p4);
            prefetch_entry(p5);
            prefetch_entry(p6);
            prefetch_entry(p7);
         }
         i += 8;
      }
      while i < n {
         unsafe {
            let s_ptr = *t_ptr.add(i);
            if s_ptr.is_null() {
               *e_ptr.add(i) = std::ptr::null();
            } else {
               let base = s_ptr.cast::<TaggedSlot<V>>();
               let p = TaggedSlot::probe_from_base(base, *h_ptr.add(i), 16);
               *e_ptr.add(i) = p.cast();
               prefetch_entry(p);
            }
         }
         i += 1;
      }
   }

   #[inline(always)]
   pub fn probe_and_prefetch_entries_write(
      &self,
      tables: &[*const ()],
      hashes: &[u64],
      entries: &mut [*const ()],
   ) {
      let n = tables.len().min(hashes.len()).min(entries.len());
      let t_ptr = tables.as_ptr();
      let h_ptr = hashes.as_ptr();
      let e_ptr = entries.as_mut_ptr();
      let mut i = 0;
      while i + 8 <= n {
         unsafe {
            let b0 = (*t_ptr.add(i)).cast::<TaggedSlot<V>>();
            let b1 = (*t_ptr.add(i + 1)).cast::<TaggedSlot<V>>();
            let b2 = (*t_ptr.add(i + 2)).cast::<TaggedSlot<V>>();
            let b3 = (*t_ptr.add(i + 3)).cast::<TaggedSlot<V>>();
            let b4 = (*t_ptr.add(i + 4)).cast::<TaggedSlot<V>>();
            let b5 = (*t_ptr.add(i + 5)).cast::<TaggedSlot<V>>();
            let b6 = (*t_ptr.add(i + 6)).cast::<TaggedSlot<V>>();
            let b7 = (*t_ptr.add(i + 7)).cast::<TaggedSlot<V>>();

            let p0 = TaggedSlot::probe_from_base(b0, *h_ptr.add(i), 16);
            let p1 = TaggedSlot::probe_from_base(b1, *h_ptr.add(i + 1), 16);
            let p2 = TaggedSlot::probe_from_base(b2, *h_ptr.add(i + 2), 16);
            let p3 = TaggedSlot::probe_from_base(b3, *h_ptr.add(i + 3), 16);
            let p4 = TaggedSlot::probe_from_base(b4, *h_ptr.add(i + 4), 16);
            let p5 = TaggedSlot::probe_from_base(b5, *h_ptr.add(i + 5), 16);
            let p6 = TaggedSlot::probe_from_base(b6, *h_ptr.add(i + 6), 16);
            let p7 = TaggedSlot::probe_from_base(b7, *h_ptr.add(i + 7), 16);

            *e_ptr.add(i) = p0.cast();
            *e_ptr.add(i + 1) = p1.cast();
            *e_ptr.add(i + 2) = p2.cast();
            *e_ptr.add(i + 3) = p3.cast();
            *e_ptr.add(i + 4) = p4.cast();
            *e_ptr.add(i + 5) = p5.cast();
            *e_ptr.add(i + 6) = p6.cast();
            *e_ptr.add(i + 7) = p7.cast();

            prefetch_entry_write(p0);
            prefetch_entry_write(p1);
            prefetch_entry_write(p2);
            prefetch_entry_write(p3);
            prefetch_entry_write(p4);
            prefetch_entry_write(p5);
            prefetch_entry_write(p6);
            prefetch_entry_write(p7);
         }
         i += 8;
      }
      while i < n {
         unsafe {
            let s_ptr = *t_ptr.add(i);
            if s_ptr.is_null() {
               *e_ptr.add(i) = std::ptr::null();
            } else {
               let base = s_ptr.cast::<TaggedSlot<V>>();
               let p = TaggedSlot::probe_from_base(base, *h_ptr.add(i), 16);
               *e_ptr.add(i) = p.cast();
               prefetch_entry_write(p);
            }
         }
         i += 1;
      }
   }

   pub fn prefetch_batch_entries<K: AsRef<[u8]>>(
      &self,
      keys: &[K],
      hashes: &mut [u64],
      entries: &mut [*const ()],
   ) {
      let n = keys
         .len()
         .min(PREFETCH_BATCH)
         .min(hashes.len())
         .min(entries.len());
      if n == 0 {
         return;
      }
      let mut tables: [*const (); PREFETCH_BATCH] = [std::ptr::null(); PREFETCH_BATCH];
      self.prefetch_slots(&keys[..n], &mut hashes[..n], &mut tables[..n]);
      self.probe_and_prefetch_entries(&tables[..n], &hashes[..n], &mut entries[..n]);
   }

   #[inline(always)]
   pub fn prefetch_batch<K: AsRef<[u8]>>(&self, keys: &[K], hashes: &mut [u64]) {
      let mut entries = [std::ptr::null(); PREFETCH_BATCH];
      self.prefetch_batch_entries(keys, hashes, &mut entries);
   }

   #[inline(always)]
   pub fn probe_entry_fast(&self, key: &str, hash: u64, idx: usize) -> Option<*const Entry<V>> {
      let sf = unsafe { &*self.shard_fast.as_ptr().add(idx) };
      let (slots, mask) = sf.layout();
      let base = unsafe { slots.add((hash as usize) & mask) };
      let p = unsafe { TaggedSlot::probe_from_base(base, hash, 16) };
      if !p.is_null() {
         let e = unsafe { &*p };
         let state = e.state.load(Ordering::Relaxed);
         let expected = ((hash as u32 as u64) << 32) | STATE_OCCUPIED;
         if (state & (0xFFFF_FFFF_0000_0000 | STATE_OCCUPIED)) == expected && e.key.eq_str(key) {
            return Some(p as *const Entry<V>);
         }
      }
      None
   }

   #[inline(always)]
   pub fn with_entry_pinned<R>(
      &self,
      key: &str,
      hash: u64,
      idx: usize,
      f: impl FnOnce(&V) -> R,
   ) -> Option<R> {
      let _guard = ebr::pin();
      let sf = unsafe { &*self.shard_fast.as_ptr().add(idx) };
      let (slots, mask) = sf.layout();
      let entry = Shard::find_fast(slots, mask, key, hash)?;
      let read = entry.read()?;
      Some(f(&read))
   }

   #[inline(always)]
   pub fn with_entry_hint<R>(
      &self,
      key: &str,
      hash: u64,
      entry_hint: *const (),
      f: impl FnOnce(&V) -> R,
   ) -> Option<R> {
      if !entry_hint.is_null() {
         let p = entry_hint.cast::<Entry<V>>();
         let e = unsafe { &*p };
         let state = e.state.load(Ordering::Relaxed);
         let expected = ((hash as u32 as u64) << 32) | STATE_OCCUPIED;
         if (state & (0xFFFF_FFFF_0000_0000 | STATE_OCCUPIED)) == expected && e.key.eq_str(key) {
            let read = e.read()?;
            return Some(f(&read));
         }
      }
      let idx = self.shard_of(hash);
      self.with_entry_pinned(key, hash, idx, f)
   }

   #[inline]
   pub fn with_entry<R>(
      &self,
      key: &str,
      hash: u64,
      idx: usize,
      f: impl FnOnce(&V) -> R,
   ) -> Option<R> {
      ebr::with_pin(|_| self.with_entry_pinned(key, hash, idx, f))
   }

   #[inline(always)]
   pub fn contains_key(&self, key: &str) -> bool {
      let (h, idx) = self.locate(key);
      let _guard = ebr::pin();
      let sf = unsafe { &*self.shard_fast.as_ptr().add(idx) };
      let (slots, mask) = sf.layout();
      ebr::with_pin(|_| {
         Shard::find_fast(slots, mask, key, h)
            .is_some_and(|e| state_occupied(&e.state, Ordering::Acquire))
      })
   }

   #[inline(always)]
   pub fn get(&self, key: &str) -> Option<V> {
      let (h, idx) = self.locate(key);
      let _guard = ebr::pin();
      let sf = unsafe { &*self.shard_fast.as_ptr().add(idx) };
      let (slots, mask) = sf.layout();
      Shard::get_pinned_fast(slots, mask, key, h)
   }

   /// Like `get` without re-pinning EBR when caller already holds a Guard.
   #[inline(always)]
   pub fn get_pinned(&self, key: &str, _guard: &ebr::Guard) -> Option<V> {
      let (h, idx) = self.locate(key);
      let sf = unsafe { &*self.shard_fast.as_ptr().add(idx) };
      let (slots, mask) = sf.layout();
      Shard::get_pinned_fast(slots, mask, key, h)
   }

   /// Like `get_ref` when caller holds an EBR Guard for returned reference lifetime.
   #[inline]
   pub fn get_ref_pinned<'g>(
      &'g self,
      key: &str,
      _guard: &'g ebr::Guard,
   ) -> Option<ValueRef<'g, V>> {
      let (h, idx) = self.locate(key);
      let sf = unsafe { &*self.shard_fast.as_ptr().add(idx) };
      let (slots, mask) = sf.layout();
      let e = Shard::find_fast(slots, mask, key, h)?;
      let read = e.read()?;
      Some(ValueRef {
         read,
         _guard: ebr::pin(),
         _map: PhantomData,
      })
   }

   #[inline]
   pub fn get_ref(&self, key: &str) -> Option<ValueRef<'_, V>> {
      let (h, idx) = self.locate(key);
      let guard = ebr::pin();
      let sf = unsafe { &*self.shard_fast.as_ptr().add(idx) };
      let (slots, mask) = sf.layout();
      let e = Shard::find_fast(slots, mask, key, h)?;
      let read = e.read()?;
      Some(ValueRef {
         read,
         _guard: guard,
         _map: PhantomData,
      })
   }

   #[inline]
   pub fn insert(&self, key: String, value: V) -> bool {
      let _write_epoch = self.write_epoch();
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

   /// `insert` for borrowed keys: fast lock-free slot probe first, falls
   /// back to full insertion on exhaustion.
   #[inline(always)]
   pub fn insert_str(&self, key: &str, value: V) -> bool {
      let (h, idx) = self.locate(key);
      self.insert_str_hashed(key, value, h, idx)
   }

   #[inline(always)]
   pub fn insert_str_hashed(&self, key: &str, value: V, h: u64, idx: usize) -> bool {
      let pin = ebr::pin();
      let _write_epoch = self.write_epoch();
      let shard = unsafe { self.shards.get_unchecked(idx) };
      match shard.insert_or_update_slot_fast(h, key, value, &pin) {
         InsertFastResult::Inserted => {
            self.bump(idx, true);
            self.touched(key);
            true
         }
         InsertFastResult::Updated => {
            self.touched(key);
            false
         }
         InsertFastResult::Fallback(val) => {
            let is_new = shard.insert_hashed_key(CompactKey::from_str(key), val, h);
            if is_new {
               self.bump(idx, true);
            }
            self.touched(key);
            is_new
         }
      }
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
   pub fn try_insert_str(&self, key: &str, value: V) -> Result<bool, Full> {
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
      Ok(self.insert_str(key, value))
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

   #[inline(always)]
   pub fn insert_slot_fast(&self, hash: u64, key: &str, value: V) -> bool {
      let idx = self.shard_of(hash);
      self.insert_slot_fast_located(hash, idx, key, value)
   }

   #[inline(always)]
   pub fn insert_slot_fast_located(&self, hash: u64, idx: usize, key: &str, value: V) -> bool {
      let epoch = self.write_epoch();
      let pin = ebr::pin();
      unsafe { self.insert_slot_fast_located_pinned(hash, idx, key, value, &epoch, &pin) }
   }

   /// # Safety
   /// idx must be valid, with this map's write epoch and an active EBR pin.
   #[inline(always)]
   pub unsafe fn insert_slot_fast_located_pinned(
      &self,
      hash: u64,
      idx: usize,
      key: &str,
      value: V,
      epoch: &WriteEpochGuard<'_>,
      pin: &ebr::Guard,
   ) -> bool {
      debug_assert!(epoch.guards(&self.access));
      let shard = unsafe { self.shards.get_unchecked(idx) };
      match shard.insert_or_update_slot_fast(hash, key, value, pin) {
         InsertFastResult::Inserted => {
            self.bump(idx, true);
            true
         }
         InsertFastResult::Updated => true,
         InsertFastResult::Fallback(_) => false,
      }
   }

   /// Probes for key; inserts make_value() on miss or returns existing entry.
   #[inline(always)]
   pub fn probe_or_insert_fast<F>(&self, hash: u64, key: &str, make_value: F) -> FastSet<V>
   where
      F: FnOnce() -> V,
   {
      let _pin = ebr::pin();
      let _write_epoch = self.write_epoch();
      let idx = self.shard_of(hash);
      let shard = unsafe { self.shards.get_unchecked(idx) };
      let outcome = shard.probe_or_insert(hash, key, make_value);
      if matches!(outcome, FastSet::Inserted) {
         self.bump(idx, true);
      }
      outcome
   }

   #[inline(always)]
   pub fn set_str_hinted_pinned(&self, key: &str, hash: Option<u64>, value: V) -> bool {
      let (h, idx) = match hash {
         Some(h) => (h, self.shard_of(h)),
         None => self.locate(key),
      };
      self.set_located_pinned(key, h, idx, value, || CompactKey::from_str(key))
   }

   #[inline(always)]
   pub fn set_with_hint(&self, key: &str, hash: u64, entry_hint: *const (), value: V) -> bool {
      let _write_epoch = self.write_epoch();
      if !entry_hint.is_null() {
         let p = entry_hint.cast::<Entry<V>>();
         let e = unsafe { &*p };
         let current = e.state.load(Ordering::Relaxed);
         let expected = ((hash as u32 as u64) << 32) | STATE_OCCUPIED;
         if (current & STATE_MATCH_MASK) == expected && e.key.eq_str(key) {
            let locked = bump_seq(current) | STATE_LOCK;
            if e
               .state
               .compare_exchange_weak(current, locked, Ordering::Acquire, Ordering::Relaxed)
               .is_ok()
            {
               self.snap_before_write(e);
               unsafe { (*e.value.get()).assume_init_drop() };
               unsafe { (*e.value.get()).write(value) };
               unlock_write(&e.state, locked, true);
               self.touched(key);
               return false;
            }
         }
      }
      let idx = self.shard_of(hash);
      self.set_located_pinned(key, hash, idx, value, || CompactKey::from_str(key))
   }

   #[inline(always)]
   pub fn update_with_hint<R>(
      &self,
      key: &str,
      hash: u64,
      entry_hint: *const (),
      f: impl FnOnce(&mut V) -> R,
   ) -> Option<R> {
      let _write_epoch = self.write_epoch();
      if !entry_hint.is_null() {
         let p = entry_hint.cast::<Entry<V>>();
         let e = unsafe { &*p };
         let current = e.state.load(Ordering::Relaxed);
         let expected = ((hash as u32 as u64) << 32) | STATE_OCCUPIED;
         if (current & STATE_MATCH_MASK) == expected && e.key.eq_str(key) {
            let locked = bump_seq(current) | STATE_LOCK;
            if e
               .state
               .compare_exchange_weak(current, locked, Ordering::Acquire, Ordering::Relaxed)
               .is_ok()
            {
               self.snap.before_write(e);
               let res = f(unsafe { (*e.value.get()).assume_init_mut() });
               unlock_write(&e.state, locked, true);
               self.touched(key);
               return Some(res);
            }
         }
      }
      let idx = self.shard_of(hash);
      if let Some(e) = unsafe { self.shards.get_unchecked(idx) }.find(key, hash) {
         let current = e.state.load(Ordering::Relaxed);
         if (current & (STATE_OCCUPIED | STATE_LOCK)) == STATE_OCCUPIED {
            let locked = bump_seq(current) | STATE_LOCK;
            if e
               .state
               .compare_exchange_weak(current, locked, Ordering::Acquire, Ordering::Relaxed)
               .is_ok()
            {
               self.snap.before_write(e);
               let res = f(unsafe { (*e.value.get()).assume_init_mut() });
               unlock_write(&e.state, locked, true);
               self.touched(key);
               return Some(res);
            }
         }
      }
      None
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
      self.set_located_pinned(key, h, idx, value, key_owned)
   }

   #[inline(always)]
   pub(crate) fn set_located_pinned(
      &self,
      key: &str,
      h: u64,
      idx: usize,
      value: V,
      key_owned: impl FnOnce() -> CompactKey,
   ) -> bool {
      let _write_epoch = self.write_epoch();
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
      let _write_epoch = self.write_epoch();
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
      self.remove_if(key, |_| true)
   }

   #[inline]
   pub fn remove_if(&self, key: &str, predicate: impl FnOnce(&V) -> bool) -> bool {
      let _write_epoch = self.write_epoch();
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
      if !predicate(unsafe { (*entry.value.get()).assume_init_ref() }) {
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
      let _write_epoch = self.write_epoch();
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
      let _write_epoch = self.write_epoch();
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
      let _write_epoch = self.write_epoch();
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
      let _write_epoch = self.write_epoch();
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

      let read = entry.read()?;
      Some(f(&read))
   }

   #[inline]
   pub fn read_consistent<R>(&self, key: &str, f: impl FnOnce(&V) -> R) -> Option<R> {
      self.get_locked(key, f)
   }

   #[inline]
   pub fn insert_if_absent(&self, key: String, value: V) -> bool {
      let _write_epoch = self.write_epoch();
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
      let _write_epoch = self.write_epoch();
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
         self.approx.fetch_add(1, Ordering::Relaxed);
      } else {
         live.fetch_sub(1, Ordering::Relaxed);
         self.approx.fetch_sub(1, Ordering::Relaxed);
      }
   }

   /// One relaxed load; trivially false with no limit configured. The
   /// approximate counter is exact (same bump events that drive the shard
   /// live counters), so no re-check fallback is needed.
   #[inline(always)]
   pub fn is_full(&self) -> bool {
      self.max_keys != usize::MAX && self.approx.load(Ordering::Relaxed) >= self.max_keys
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

pub const PREFETCH_BATCH: usize = 32;

#[inline(always)]
fn prefetch_entry<V>(p: *const Entry<V>) {
   prefetch_line(p);
   if std::mem::size_of::<Entry<V>>() > 32 {
      prefetch_line(
         p.cast::<u8>()
            .wrapping_add(std::mem::size_of::<Entry<V>>() - 1),
      );
   }
}

#[inline(always)]
fn prefetch_entry_write<V>(p: *const Entry<V>) {
   prefetch_line_write(p);
   if std::mem::size_of::<Entry<V>>() > 32 {
      prefetch_line_write(
         p.cast::<u8>()
            .wrapping_add(std::mem::size_of::<Entry<V>>() - 1),
      );
   }
}

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

#[inline(always)]
fn prefetch_line_write<T>(p: *const T) {
   #[cfg(target_arch = "x86_64")]
   unsafe {
      std::arch::x86_64::_mm_prefetch::<{ std::arch::x86_64::_MM_HINT_ET0 }>(p.cast());
   }
   #[cfg(target_arch = "aarch64")]
   unsafe {
      std::arch::asm!("prfm pstl1keep, [{0}]", in(reg) p, options(nostack, readonly, preserves_flags));
   }
   #[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
   let _ = p;
}

pub const fn entry_size<V>() -> usize {
   std::mem::size_of::<shard::Entry<V>>()
}

#[cfg(test)]
mod tests {

   #[test]
   fn reused_tombstones_do_not_inflate_table_occupancy() {
      let map = super::CustomMap::with_capacity(1, 8);
      let table = map.shards[0].table.load(super::Ordering::Relaxed);
      for value in 0..1000 {
         map.insert_str("key", value);
         assert_eq!(map.shards[0].len.load(super::Ordering::Relaxed), 1);
         assert!(map.remove_no_clone("key"));
      }
      assert_eq!(map.shards[0].table.load(super::Ordering::Relaxed), table);
   }
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

      let l = map.len();
      assert!(l <= KEYS, "map.len() was {l}, expected <= {KEYS}");
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

   /// Churn rebuilds table in place without expanding capacity.
   #[test]
   fn churn_rebuilds_in_place_instead_of_growing() {
      let map = CustomMap::<u64>::build(1, 32, usize::MAX);
      assert_eq!(map.shard_slot_count(0), 32);
      for round in 0..6u64 {
         for i in 0..10u64 {
            let key = format!("key-{i}");
            map.set(&key, round * 10 + i, || key.clone());
         }
         assert_eq!(map.len(), 10, "round {round}");
         for i in 0..10u64 {
            assert!(map.remove_no_clone(&format!("key-{i}")));
         }
         assert_eq!(map.len(), 0, "round {round}");
         assert_eq!(
            map.shard_slot_count(0),
            32,
            "round {round}: tombstone churn must not grow the table"
         );
      }
      map.set("fresh", 7, || "fresh".to_owned());
      assert_eq!(map.get("fresh"), Some(7));
      assert_eq!(map.shard_slot_count(0), 32);
   }

   /// Compaction still shrinks genuinely sparse shards, and a no-op pass
   /// must not rebuild anything.
   #[test]
   fn compact_shard_reclaims_sparse_shards_only() {
      let map = CustomMap::<u64>::build(1, 32, usize::MAX);
      map.set("a", 1, || "a".to_owned());
      map.compact_shard(0);
      assert_eq!(map.shard_slot_count(0), 8);
      assert_eq!(map.get("a"), Some(1));

      map.compact_shard(0);
      assert_eq!(map.shard_slot_count(0), 8);

      assert!(map.remove_no_clone("a"));
      map.compact_shard(0);
      assert_eq!(map.shard_slot_count(0), 8);
   }

   #[test]
   fn fused_probe_or_insert_reports_both_outcomes() {
      use crate::FastSet;
      let map = CustomMap::<u64>::build(1, 1024, usize::MAX);
      let (hash, _) = map.locate_key("alpha");

      match map.probe_or_insert_fast(hash, "alpha", || 1) {
         FastSet::Inserted => {}
         _ => panic!("first probe must insert"),
      }
      assert_eq!(map.get("alpha"), Some(1));
      assert_eq!(map.len(), 1);

      match map.probe_or_insert_fast(hash, "alpha", || 2) {
         FastSet::Existing(_) => {}
         _ => panic!("second probe must find the existing entry"),
      }
      assert_eq!(map.get("alpha"), Some(1));
      assert_eq!(map.len(), 1);

      assert!(map.remove_no_clone("alpha"));
      match map.probe_or_insert_fast(hash, "alpha", || 3) {
         FastSet::Inserted => {}
         // The tombstone is revived by the general path.
         FastSet::Retry => {
            map.set("alpha", 3, || "alpha".to_owned());
         }
         FastSet::Existing(_) => panic!("removed entry must not be reported as live"),
      }
      assert_eq!(map.get("alpha"), Some(3));
      assert_eq!(map.len(), 1);
   }
}
