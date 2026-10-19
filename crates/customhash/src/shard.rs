use std::alloc::Layout;
use std::cell::UnsafeCell;
use std::ptr;
use std::sync::atomic::{AtomicPtr, AtomicU64, AtomicUsize, Ordering};

use crossbeam_utils::CachePadded;

use super::ebr;
use super::key::CompactKey;

pub const STATE_LOCK: u64 = 1;
pub const STATE_OCCUPIED: u64 = 1 << 1;
pub(crate) const STATE_SEQ_ONE: u64 = 1 << 2;
pub(crate) const STATE_SEQ_MASK: u64 = ((1 << 29) - 1) << 2;
pub(crate) const STATE_SNAP: u64 = 1 << 31;
pub(crate) const STATE_HASH_SHIFT: u32 = 32;
pub const STATE_MATCH_MASK: u64 = 0xFFFF_FFFF_0000_0000 | STATE_OCCUPIED | STATE_LOCK;

#[inline(always)]
pub(crate) fn hash_bits(hash: u64) -> u64 {
   (hash as u32 as u64) << STATE_HASH_SHIFT
}

#[inline(always)]
pub(crate) fn stored_hash(state: u64) -> u64 {
   state >> STATE_HASH_SHIFT
}

pub(crate) const LOAD_NUM: usize = 3;
pub(crate) const LOAD_DEN: usize = 4;
pub(crate) const DEFAULT_SHARD_CAPACITY: usize = 262_144;
// Keep idle shards tiny; tables grow lock-free as keys arrive.
pub(crate) const INITIAL_SHARD_CAPACITY: usize = 8;
pub(crate) const GROWING: usize = 1usize << (usize::BITS - 1);

#[repr(C, align(64))]
pub struct Entry<V> {
   pub key: CompactKey,
   pub state: AtomicU64,
   pub value: UnsafeCell<std::mem::MaybeUninit<V>>,
}

pub(crate) unsafe fn drop_raw_entry<V>(ptr: *mut u8) {
   let entry = ptr.cast::<Entry<V>>();
   unsafe {
      std::ptr::drop_in_place(entry);
   }
   unsafe {
      rust_zmalloc::dealloc_raw(ptr, Layout::new::<Entry<V>>());
   }
}

#[inline(always)]
pub(crate) fn alloc_entry<V>(key: CompactKey, state: u64, value: V) -> *mut Entry<V> {
   let layout = Layout::new::<Entry<V>>();
   let ptr = unsafe { rust_zmalloc::alloc_raw(layout) }.cast::<Entry<V>>();
   if ptr.is_null() {
      std::alloc::handle_alloc_error(layout);
   }
   unsafe {
      ptr.write(Entry {
         key,
         state: AtomicU64::new(state),
         value: UnsafeCell::new(std::mem::MaybeUninit::new(value)),
      });
   }
   ptr
}

impl<V> Entry<V> {
   #[inline(always)]
   #[allow(dead_code)]
   pub(crate) fn new(key: CompactKey, state: u64, value: V) -> Self {
      Self {
         key,
         state: AtomicU64::new(state),
         value: UnsafeCell::new(std::mem::MaybeUninit::new(value)),
      }
   }
}

impl<V> Drop for Entry<V> {
   fn drop(&mut self) {
      if (*self.state.get_mut() & STATE_OCCUPIED) != 0 {
         unsafe { self.value.get_mut().assume_init_drop() };
      }
   }
}

unsafe impl<V: Send> Send for Entry<V> {}
unsafe impl<V: Send + Sync> Sync for Entry<V> {}

#[inline]
pub(crate) fn state_seq(state: u64) -> u32 {
   ((state & STATE_SEQ_MASK) >> 2) as u32
}

#[inline(always)]
pub fn bump_seq(state: u64) -> u64 {
   (state & !STATE_SEQ_MASK) | (state.wrapping_add(STATE_SEQ_ONE) & STATE_SEQ_MASK)
}

#[inline(always)]
pub(crate) fn state_occupied(state: &AtomicU64, order: Ordering) -> bool {
   state.load(order) & STATE_OCCUPIED != 0
}

#[inline(always)]
pub(crate) fn state_set_occupied(state: &AtomicU64, occupied: bool, order: Ordering) {
   if occupied {
      state.fetch_or(STATE_OCCUPIED, order);
   } else {
      state.fetch_and(!STATE_OCCUPIED, order);
   }
}

#[inline(always)]
pub(crate) fn state_lock(state: &AtomicU64) {
   let mut current = state.load(Ordering::Relaxed);
   loop {
      if current & STATE_LOCK != 0 {
         state_lock_slow(state);
         return;
      }
      match state.compare_exchange_weak(
         current,
         current | STATE_LOCK,
         Ordering::Acquire,
         Ordering::Relaxed,
      ) {
         Ok(_) => return,
         Err(observed) => current = observed,
      }
   }
}

/// A spinning worker serves no other connections; back off to yield.
const MAX_SPIN_BACKOFF: u32 = 64;

#[cold]
fn state_lock_slow(state: &AtomicU64) {
   // TTAS with exponential backoff: without it N contenders hammer the same
   // line with CAS and one handoff costs N ownership transfers.
   let mut backoff = 1u32;
   loop {
      // Plain load keeps the line shared; a CAS takes it exclusively and
      // invalidates every other waiter.
      while state.load(Ordering::Relaxed) & STATE_LOCK != 0 {
         for _ in 0..backoff {
            std::hint::spin_loop();
         }
         if backoff < MAX_SPIN_BACKOFF {
            backoff <<= 1;
         } else {
            // The holder may be descheduled; hand the CPU over.
            std::thread::yield_now();
         }
      }
      let current = state.load(Ordering::Relaxed);
      if current & STATE_LOCK == 0
         && state
            .compare_exchange_weak(
               current,
               current | STATE_LOCK,
               Ordering::Acquire,
               Ordering::Relaxed,
            )
            .is_ok()
      {
         return;
      }
   }
}

#[inline(always)]
pub(crate) fn state_unlock(state: &AtomicU64) {
   state.fetch_and(!STATE_LOCK, Ordering::Release);
}

/// Plain store (no RMW): caller holds the lock. The release fence keeps
/// value writes behind the odd sequence number for seqlock readers.
#[inline(always)]
pub(crate) fn write_begin(state: &AtomicU64) {
   let current = state.load(Ordering::Relaxed);
   state.store(bump_seq(current), Ordering::Relaxed);
   std::sync::atomic::fence(Ordering::Release);
}

/// Publishes occupied flag and releases the lock in one release store.
#[inline(always)]
pub(crate) fn write_end(state: &AtomicU64, occupied: bool) {
   let current = state.load(Ordering::Relaxed);
   let mut next = bump_seq(current) & !STATE_LOCK;
   if occupied {
      next |= STATE_OCCUPIED;
   } else {
      next &= !STATE_OCCUPIED;
   }
   state.store(next, Ordering::Release);
}

#[inline(always)]
pub fn unlock_write(state: &AtomicU64, locked: u64, occupied: bool) {
   let next = if occupied {
      (bump_seq(locked) & !STATE_LOCK) | STATE_OCCUPIED
   } else {
      (bump_seq(locked) & !STATE_LOCK) & !STATE_OCCUPIED
   };
   state.store(next, Ordering::Release);
}

pub(crate) struct SlotTable<V> {
   pub(crate) slots: *mut TaggedSlot<V>,
   pub(crate) capacity: usize,
   pub(crate) mask: usize,
   pub(crate) threshold: usize,
}

/// Userspace pointers leave bits 48..63 clear (x86-64/AArch64 4-level),
/// so the tag rides in the slot word; bit 63 marks presence.
const TAG_FLAG: usize = 1 << 63;
const TAG_BITS: usize = 14;
const TAG_SHIFT: u32 = 49;
const TAG_MAX: usize = (1 << TAG_BITS) - 1;
const TAG_MASK: usize = TAG_MAX << TAG_SHIFT;
const RESERVED_MASK: usize = TAG_MASK | TAG_FLAG;
const PTR_MASK: usize = !RESERVED_MASK;

#[inline(always)]
fn hash_tag(hash: u64) -> usize {
   ((hash >> 18) as usize) & TAG_MAX
}

/// The tag makes the raw word non-dereferenceable; this type deposits
/// and strips it.
#[repr(transparent)]
pub(crate) struct TaggedSlot<V>(AtomicPtr<Entry<V>>);

impl<V> TaggedSlot<V> {
   #[cfg(test)]
   pub(crate) fn null() -> Self {
      Self(AtomicPtr::new(ptr::null_mut()))
   }

   /// Combine a pointer with its tag, or return it verbatim when it already
   /// occupies the reserved bits.
   #[inline(always)]
   fn encode(entry: *mut Entry<V>, hash: u64) -> *mut Entry<V> {
      let addr = entry as usize;
      if addr & RESERVED_MASK != 0 {
         return entry;
      }
      (addr | TAG_FLAG | (hash_tag(hash) << TAG_SHIFT)) as *mut Entry<V>
   }

   #[inline(always)]
   fn decode(word: usize) -> *mut Entry<V> {
      if word & TAG_FLAG != 0 {
         (word & PTR_MASK) as *mut Entry<V>
      } else {
         word as *mut Entry<V>
      }
   }

   /// Load the slot's entry pointer, tag stripped and safe to dereference.
   #[inline(always)]
   pub(crate) fn load(&self, order: Ordering) -> *mut Entry<V> {
      Self::decode(self.0.load(order) as usize)
   }

   #[cfg(test)]
   #[inline(always)]
   pub(crate) fn load_if_tag_matches(
      &self,
      hash: u64,
      order: Ordering,
   ) -> Result<*mut Entry<V>, ()> {
      let word = self.0.load(order) as usize;
      if word == 0 {
         return Ok(ptr::null_mut());
      }
      if word & TAG_FLAG != 0 {
         let expected_tag = (hash_tag(hash) << TAG_SHIFT) | TAG_FLAG;
         if (word & RESERVED_MASK) != expected_tag {
            return Err(());
         }
         return Ok((word & PTR_MASK) as *mut Entry<V>);
      }
      Ok(word as *mut Entry<V>)
   }

   #[inline(always)]
   pub(crate) fn store(&self, entry: *mut Entry<V>, hash: u64, order: Ordering) {
      self.0.store(Self::encode(entry, hash), order);
   }

   /// Claim an empty slot for `entry`.
   #[inline(always)]
   pub(crate) fn claim(&self, entry: *mut Entry<V>, hash: u64) -> bool {
      self
         .0
         .compare_exchange(
            ptr::null_mut(),
            Self::encode(entry, hash),
            Ordering::Release,
            Ordering::Acquire,
         )
         .is_ok()
   }

   #[inline(always)]
   pub(crate) unsafe fn probe_from_base(
      base: *const TaggedSlot<V>,
      hash: u64,
      max_probes: usize,
   ) -> *mut Entry<V> {
      let expected_hi = (hash_tag(hash) | (1 << TAG_BITS)) as u16;
      let word0 = unsafe { (*base).0.load(Ordering::Relaxed) } as usize;
      if (word0 >> TAG_SHIFT) as u16 == expected_hi {
         return (word0 & PTR_MASK) as *mut Entry<V>;
      }
      if word0 == 0 {
         return ptr::null_mut();
      }
      let word1 = unsafe { (*base.add(1)).0.load(Ordering::Relaxed) } as usize;
      if (word1 >> TAG_SHIFT) as u16 == expected_hi {
         return (word1 & PTR_MASK) as *mut Entry<V>;
      }
      if word1 == 0 {
         return ptr::null_mut();
      }
      for j in 2..max_probes {
         let word = unsafe { (*base.add(j)).0.load(Ordering::Relaxed) } as usize;
         if (word >> TAG_SHIFT) as u16 == expected_hi {
            return (word & PTR_MASK) as *mut Entry<V>;
         }
         if word == 0 {
            return ptr::null_mut();
         }
      }
      ptr::null_mut()
   }
}

unsafe impl<V: Send> Send for SlotTable<V> {}
unsafe impl<V: Send> Sync for SlotTable<V> {}

impl<V> Drop for SlotTable<V> {
   fn drop(&mut self) {
      if self.slots.is_null() {
         return;
      }
      let layout =
         Layout::array::<TaggedSlot<V>>(self.capacity + 16).expect("slot array layout overflow");
      unsafe {
         rust_zmalloc::dealloc_raw(self.slots.cast(), layout);
      }
   }
}

pub(crate) unsafe fn drop_raw_table<V>(ptr: *mut u8) {
   let table = ptr.cast::<SlotTable<V>>();
   unsafe {
      std::ptr::drop_in_place(table);
   }
   unsafe {
      rust_zmalloc::dealloc_raw(ptr, Layout::new::<SlotTable<V>>());
   }
}

pub(crate) fn alloc_table<V>(table: SlotTable<V>) -> *mut SlotTable<V> {
   let layout = Layout::new::<SlotTable<V>>();
   let ptr = unsafe { rust_zmalloc::alloc_raw(layout) }.cast::<SlotTable<V>>();
   if ptr.is_null() {
      std::alloc::handle_alloc_error(layout);
   }
   unsafe {
      ptr.write(table);
   }
   ptr
}

impl<V> SlotTable<V> {
   pub(crate) fn new(cap: usize) -> Self {
      let cap = cap.next_power_of_two().max(8);
      let layout = Layout::array::<TaggedSlot<V>>(cap + 16).expect("slot array layout overflow");
      let slots = unsafe { rust_zmalloc::alloc_raw_zeroed(layout) }.cast::<TaggedSlot<V>>();
      if slots.is_null() {
         std::alloc::handle_alloc_error(layout);
      }
      SlotTable {
         slots,
         capacity: cap,
         mask: cap - 1,
         threshold: cap * LOAD_NUM / LOAD_DEN,
      }
   }

   #[inline(always)]
   pub(crate) unsafe fn slot(&self, i: usize) -> &TaggedSlot<V> {
      unsafe { &*self.slots.add(i) }
   }

   #[inline(always)]
   pub(crate) fn slots(&self) -> &[TaggedSlot<V>] {
      unsafe { std::slice::from_raw_parts(self.slots, self.capacity) }
   }

   pub(crate) fn capacity(&self) -> usize {
      self.capacity
   }
}

#[repr(C)]
pub(crate) struct ShardFast<V> {
   pub(crate) slots: AtomicPtr<TaggedSlot<V>>,
   pub(crate) mask: AtomicUsize,
}

#[repr(align(128))]
pub(crate) struct Shard<V> {
   pub(crate) table: AtomicPtr<SlotTable<V>>,
   pub(crate) slots: AtomicPtr<TaggedSlot<V>>,
   pub(crate) mask: AtomicUsize,
   pub(crate) threshold: AtomicUsize,
   pub(crate) fast: AtomicPtr<ShardFast<V>>,
   pub(crate) len: CachePadded<AtomicUsize>,
   pub(crate) live: CachePadded<AtomicUsize>,
   pub(crate) insert_gate: CachePadded<AtomicUsize>,
   pub(crate) grow_lock: std::sync::Mutex<()>,
   pub(crate) snap: std::sync::Arc<crate::snap::SnapCtl<V>>,
   pub(crate) initial_capacity: usize,
}

pub(crate) struct InsertGuard<'a> {
   gate: &'a AtomicUsize,
}

impl Drop for InsertGuard<'_> {
   #[inline(always)]
   fn drop(&mut self) {
      self.gate.fetch_sub(1, Ordering::Release);
   }
}

impl<V: Clone + Send + Sync + 'static> Shard<V> {
   pub(crate) fn new(cap: usize, snap: std::sync::Arc<crate::snap::SnapCtl<V>>) -> Self {
      let t = SlotTable::new(cap);
      let slots = t.slots;
      let mask = t.mask;
      let threshold = t.threshold;
      let table = alloc_table(t);
      Shard {
         table: AtomicPtr::new(table),
         slots: AtomicPtr::new(slots),
         mask: AtomicUsize::new(mask),
         threshold: AtomicUsize::new(threshold),
         fast: AtomicPtr::new(ptr::null_mut()),
         len: CachePadded::new(AtomicUsize::new(0)),
         live: CachePadded::new(AtomicUsize::new(0)),
         insert_gate: CachePadded::new(AtomicUsize::new(0)),
         grow_lock: std::sync::Mutex::new(()),
         snap,
         initial_capacity: cap,
      }
   }

   #[inline(always)]
   pub(crate) fn table(&self) -> &SlotTable<V> {
      unsafe { &*self.table.load(Ordering::Acquire) }
   }


   #[inline(always)]
   fn enter_insert(&self) -> InsertGuard<'_> {
      loop {
         let val = self.insert_gate.fetch_add(1, Ordering::Acquire);
         if val & GROWING == 0 {
            return InsertGuard {
               gate: &self.insert_gate,
            };
         }
         self.insert_gate.fetch_sub(1, Ordering::Relaxed);
         while self.insert_gate.load(Ordering::Relaxed) & GROWING != 0 {
            std::hint::spin_loop();
         }
      }
   }

   #[inline(always)]
   pub(crate) fn find(&self, key: &str, hash: u64) -> Option<&Entry<V>> {
      let t = self.table();
      let expected_tag = TAG_FLAG | (hash_tag(hash) << TAG_SHIFT);
      let expected_hash = hash as u32 as u64;
      let mut i = (hash as usize) & t.mask;
      let mut probes = 0usize;

      loop {
         if probes >= t.capacity {
            return None;
         }
         probes += 1;

         let slot = unsafe { t.slot(i) };
         let word = slot.0.load(Ordering::Relaxed) as usize;
         if word == 0 {
            return None;
         }
         if (word & RESERVED_MASK) == expected_tag {
            let p = (word & PTR_MASK) as *mut Entry<V>;
            let e = unsafe { &*p };
            if stored_hash(e.state.load(Ordering::Relaxed)) == expected_hash && e.key.eq_str(key) {
               return Some(e);
            }
         } else if word & TAG_FLAG == 0 {
            let p = word as *mut Entry<V>;
            let e = unsafe { &*p };
            if stored_hash(e.state.load(Ordering::Relaxed)) == expected_hash && e.key.eq_str(key) {
               return Some(e);
            }
         }
         i = (i + 1) & t.mask;
      }
   }

   #[inline(always)]
   pub(crate) fn get(&self, key: &str, hash: u64) -> Option<V> {
      let t = self.table();
      let expected_tag = TAG_FLAG | (hash_tag(hash) << TAG_SHIFT);
      let expected_hash = hash as u32 as u64;
      let mut i = (hash as usize) & t.mask;
      let mut probes = 0usize;

      loop {
         if probes >= t.capacity {
            return None;
         }
         probes += 1;

         let slot = unsafe { t.slot(i) };
         let word = slot.0.load(Ordering::Acquire) as usize;
         if word == 0 {
            return None;
         }
         if (word & RESERVED_MASK) == expected_tag {
            let p = (word & PTR_MASK) as *mut Entry<V>;
            let e = unsafe { &*p };
            let state = e.state.load(Ordering::Acquire);
            if (state & STATE_OCCUPIED) != 0
               && stored_hash(state) == expected_hash
               && e.key.eq_str(key)
            {
               return Some(unsafe { (*e.value.get()).assume_init_ref().clone() });
            }
         } else if word & TAG_FLAG == 0 {
            let p = word as *mut Entry<V>;
            let e = unsafe { &*p };
            let state = e.state.load(Ordering::Acquire);
            if (state & STATE_OCCUPIED) != 0
               && stored_hash(state) == expected_hash
               && e.key.eq_str(key)
            {
               return Some(unsafe { (*e.value.get()).assume_init_ref().clone() });
            }
         }
         i = (i + 1) & t.mask;
      }
   }

   #[allow(dead_code)]
   #[inline(always)]
   pub(crate) fn insert_slot_fast(
      &self,
      hash: u64,
      key: &str,
      value: V,
   ) -> bool {
      if self.insert_gate.load(Ordering::Relaxed) & GROWING != 0 {
         return false;
      }
      let mask = self.mask.load(Ordering::Relaxed);
      let slots = self.slots.load(Ordering::Relaxed);
      let base_idx = (hash as usize) & mask;

      for offset in 0..4 {
         let slot = unsafe { &*slots.add((base_idx + offset) & mask) };
         let word = slot.0.load(Ordering::Relaxed) as usize;
         if word == 0 {
            let entry = alloc_entry(
               CompactKey::from_str(key),
               self.snap.fresh_state() | hash_bits(hash),
               value,
            );
            if slot.claim(entry, hash) {
               let new_len = self.len.fetch_add(1, Ordering::Relaxed) + 1;
               if new_len >= self.threshold.load(Ordering::Relaxed) {
                  self.grow();
               }
               return true;
            }
            unsafe {
               rust_zmalloc::dealloc_raw(entry.cast(), std::alloc::Layout::new::<Entry<V>>());
            }
            return false;
         }
      }
      false
   }

   pub(crate) fn set_key<F>(&self, key: &str, hash: u64, value: V, key_owned: F) -> bool
   where
      F: FnOnce() -> CompactKey,
   {
      self.set_key_with_overwrite(key, hash, value, true, key_owned)
   }

   pub(crate) fn set_key_with_overwrite<F>(
      &self,
      key: &str,
      hash: u64,
      value: V,
      overwrite: bool,
      key_owned: F,
   ) -> bool
   where
      F: FnOnce() -> CompactKey,
   {
      let expected_tag = TAG_FLAG | (hash_tag(hash) << TAG_SHIFT);
      let expected_hash = hash as u32 as u64;
      let mut value = Some(value);
      let mut key_owned = Some(key_owned);
      let mut compact_key: Option<CompactKey> = None;

      loop {
         let insert_guard = self.enter_insert();
         let t = self.table();
         if self.len.load(Ordering::Relaxed) >= t.threshold {
            drop(insert_guard);
            self.grow();
            continue;
         }

         let mut i = (hash as usize) & t.mask;
         let mut entry: *mut Entry<V> = ptr::null_mut();
         let mut probes = 0usize;

         loop {
            if probes >= t.capacity {
               if !entry.is_null() {
                  let owned_key = unsafe { std::ptr::read(&(*entry).key) };
                  let extracted_val = unsafe { (*(*entry).value.get()).assume_init_read() };
                  unsafe {
                     rust_zmalloc::dealloc_raw(entry.cast(), std::alloc::Layout::new::<Entry<V>>());
                  }
                  compact_key = Some(owned_key);
                  value = Some(extracted_val);
               }
               drop(insert_guard);
               self.grow();
               break;
            }
            probes += 1;

            let slot = unsafe { t.slot(i) };
            let word = slot.0.load(Ordering::Acquire) as usize;

            if word == 0 {
               if entry.is_null() {
                  let owned_key = match compact_key.take() {
                     Some(k) => k,
                     None => (key_owned.take().expect("key_owned called once"))(),
                  };
                  let val = value.take().expect("value taken once");
                  entry = alloc_entry(
                     owned_key,
                     self.snap.fresh_state() | hash_bits(hash),
                     val,
                  );
               }

               if slot.claim(entry, hash) {
                  let new_len = self.len.fetch_add(1, Ordering::Relaxed) + 1;
                  drop(insert_guard);
                  if new_len >= t.threshold {
                     self.grow();
                  }
                  return true;
               }
               continue;
            }

            let is_match = if (word & RESERVED_MASK) == expected_tag {
               let p = (word & PTR_MASK) as *mut Entry<V>;
               let e = unsafe { &*p };
               if stored_hash(e.state.load(Ordering::Relaxed)) == expected_hash && e.key.eq_str(key)
               {
                  Some(e)
               } else {
                  None
               }
            } else if word & TAG_FLAG == 0 {
               let p = word as *mut Entry<V>;
               let e = unsafe { &*p };
               if stored_hash(e.state.load(Ordering::Relaxed)) == expected_hash && e.key.eq_str(key)
               {
                  Some(e)
               } else {
                  None
               }
            } else {
               None
            };

            if let Some(e) = is_match {
               let val = if !entry.is_null() {
                  let extracted_val = unsafe { (*(*entry).value.get()).assume_init_read() };
                  unsafe {
                     state_set_occupied(&(*entry).state, false, Ordering::Relaxed);
                     drop_raw_entry::<V>(entry.cast());
                  }
                  extracted_val
               } else {
                  value.take().expect("value taken once")
               };
               state_lock(&e.state);
               let was_occupied = state_occupied(&e.state, Ordering::Relaxed);
               if !overwrite && was_occupied {
                  state_unlock(&e.state);
                  drop(insert_guard);
                  return false;
               }
               self.snap.before_write(e);
               write_begin(&e.state);
               if was_occupied {
                  unsafe { (*e.value.get()).assume_init_drop() };
               }
               unsafe { (*e.value.get()).write(val) };
               write_end(&e.state, true);
               drop(insert_guard);

               if !was_occupied {
                  self.len.fetch_add(1, Ordering::Relaxed);
                  return true;
               }
               return false;
            }

            i = (i + 1) & t.mask;
         }
      }
   }

   pub(crate) fn set_compact_key(
      &self,
      key: CompactKey,
      hash: u64,
      value: V,
      overwrite: bool,
   ) -> bool {
      let expected_tag = TAG_FLAG | (hash_tag(hash) << TAG_SHIFT);
      let expected_hash = hash as u32 as u64;
      let mut value = Some(value);
      let mut key = Some(key);

      loop {
         let insert_guard = self.enter_insert();
         let t = self.table();
         if self.len.load(Ordering::Relaxed) >= t.threshold {
            drop(insert_guard);
            self.grow();
            continue;
         }

         let mut i = (hash as usize) & t.mask;
         let mut entry: *mut Entry<V> = ptr::null_mut();
         let mut probes = 0usize;

         loop {
            if probes >= t.capacity {
               if !entry.is_null() {
                  let owned_key = unsafe { std::ptr::read(&(*entry).key) };
                  let extracted_val = unsafe { (*(*entry).value.get()).assume_init_read() };
                  unsafe {
                     rust_zmalloc::dealloc_raw(entry.cast(), std::alloc::Layout::new::<Entry<V>>());
                  }
                  key = Some(owned_key);
                  value = Some(extracted_val);
               }
               drop(insert_guard);
               self.grow();
               break;
            }
            probes += 1;

            let slot = unsafe { t.slot(i) };
            let word = slot.0.load(Ordering::Acquire) as usize;

            if word == 0 {
               if entry.is_null() {
                  let owned_key = key.take().expect("key taken once");
                  let val = value.take().expect("value taken once");
                  entry = alloc_entry(
                     owned_key,
                     self.snap.fresh_state() | hash_bits(hash),
                     val,
                  );
               }

               if slot.claim(entry, hash) {
                  let new_len = self.len.fetch_add(1, Ordering::Relaxed) + 1;
                  drop(insert_guard);
                  if new_len >= t.threshold {
                     self.grow();
                  }
                  return true;
               }
               continue;
            }

            let is_match = if (word & RESERVED_MASK) == expected_tag {
               let p = (word & PTR_MASK) as *mut Entry<V>;
               let e = unsafe { &*p };
               let key_str = match &key {
                  Some(k) => k.as_str(),
                  None => unsafe { (*entry).key.as_str() },
               };
               if stored_hash(e.state.load(Ordering::Relaxed)) == expected_hash
                  && e.key.eq_str(key_str)
               {
                  Some(e)
               } else {
                  None
               }
            } else if word & TAG_FLAG == 0 {
               let p = word as *mut Entry<V>;
               let e = unsafe { &*p };
               let key_str = match &key {
                  Some(k) => k.as_str(),
                  None => unsafe { (*entry).key.as_str() },
               };
               if stored_hash(e.state.load(Ordering::Relaxed)) == expected_hash
                  && e.key.eq_str(key_str)
               {
                  Some(e)
               } else {
                  None
               }
            } else {
               None
            };

            if let Some(e) = is_match {
               let val = if !entry.is_null() {
                  let extracted_val = unsafe { (*(*entry).value.get()).assume_init_read() };
                  unsafe {
                     state_set_occupied(&(*entry).state, false, Ordering::Relaxed);
                     drop_raw_entry::<V>(entry.cast());
                  }
                  extracted_val
               } else {
                  value.take().expect("value taken once")
               };
               state_lock(&e.state);
               let was_occupied = state_occupied(&e.state, Ordering::Relaxed);
               if !overwrite && was_occupied {
                  state_unlock(&e.state);
                  drop(insert_guard);
                  return false;
               }
               self.snap.before_write(e);
               write_begin(&e.state);
               if was_occupied {
                  unsafe { (*e.value.get()).assume_init_drop() };
               }
               unsafe { (*e.value.get()).write(val) };
               write_end(&e.state, true);
               drop(insert_guard);

               if !was_occupied {
                  self.len.fetch_add(1, Ordering::Relaxed);
                  return true;
               }
               return false;
            }

            i = (i + 1) & t.mask;
         }
      }
   }

   #[inline(always)]
   pub(crate) fn insert_hashed(&self, key: String, value: V, hash: u64) -> bool {
      self.insert_hashed_key(CompactKey::from_string(key), value, hash)
   }

   pub(crate) fn insert_hashed_key(&self, key: CompactKey, value: V, hash: u64) -> bool {
      let _guard = ebr::pin();
      self.set_compact_key(key, hash, value, true)
   }

   #[allow(dead_code)]
   pub(crate) fn insert_new(&self, key: String, value: V, hash: u64, overwrite: bool) -> bool {
      self.insert_new_key(CompactKey::from_string(key), value, hash, overwrite)
   }

   #[allow(dead_code)]
   pub(crate) fn insert_new_key(
      &self,
      key: CompactKey,
      value: V,
      hash: u64,
      overwrite: bool,
   ) -> bool {
      let _guard = ebr::pin();
      self.set_compact_key(key, hash, value, overwrite)
   }

   fn grow(&self) {
      let _lock = self.grow_lock.lock().unwrap_or_else(|e| e.into_inner());

      self.insert_gate.fetch_or(GROWING, Ordering::AcqRel);
      while (self.insert_gate.load(Ordering::Acquire) & !GROWING) != 0 {
         std::hint::spin_loop();
      }
      self.grow_locked();
      self.insert_gate.fetch_and(!GROWING, Ordering::Release);
   }

   fn grow_locked(&self) {
      let old_ptr = self.table.load(Ordering::Acquire);
      let old_table = unsafe { &*old_ptr };
      let cur_len = self.len.load(Ordering::Relaxed);

      if cur_len < old_table.threshold {
         return;
      }

      let new_cap = (old_table.capacity() * 2).next_power_of_two();
      let new_table = Box::new(SlotTable::<V>::new(new_cap));

      let mut live_count: usize = 0;
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

         live_count += 1;
         let h = stored_hash(e.state.load(Ordering::Relaxed));
         let mut i = (h as usize) & new_table.mask;
         loop {
            let new_slot = unsafe { new_table.slots().get_unchecked(i) };
            if new_slot.load(Ordering::Relaxed).is_null() {
               new_slot.store(p, h, Ordering::Relaxed);
               break;
            }
            i = (i + 1) & new_table.mask;
         }
      }

      self.len.store(live_count, Ordering::Relaxed);

      let new_slots = new_table.slots;
      let new_mask = new_table.mask;
      let new_threshold = new_table.threshold;
      let new_ptr = alloc_table(*new_table);
      self.slots.store(new_slots, Ordering::Release);
      self.mask.store(new_mask, Ordering::Release);
      self.threshold.store(new_threshold, Ordering::Release);
      let fast_ptr = self.fast.load(Ordering::Relaxed);
      if !fast_ptr.is_null() {
         unsafe {
            (*fast_ptr).slots.store(new_slots, Ordering::Release);
            (*fast_ptr).mask.store(new_mask, Ordering::Release);
         }
      }
      self.table.store(new_ptr, Ordering::Release);
      unsafe { ebr::retire_raw(old_ptr.cast(), drop_raw_table::<V>) };
   }
}

impl<V> Drop for Shard<V> {
   fn drop(&mut self) {
      let t_ptr = self.table.load(Ordering::Relaxed);
      if !t_ptr.is_null() {
         let t = unsafe { &*t_ptr };
         for slot in t.slots().iter() {
            let p = slot.load(Ordering::Relaxed);
            if !p.is_null() {
               unsafe { drop_raw_entry::<V>(p.cast()) };
            }
         }
         unsafe {
            drop_raw_table::<V>(t_ptr.cast());
         }
      }
   }
}

unsafe impl<V: Send> Sync for Shard<V> {}
unsafe impl<V: Send> Send for Shard<V> {}

#[cfg(test)]
mod tests {
   use super::*;

   #[test]
   fn tags_round_trip_and_never_collide_with_the_pointer() {
      let mut entry = Entry::<u32>::new(
         CompactKey::from_string("k".to_owned()),
         STATE_OCCUPIED,
         7,
      );
      let raw: *mut Entry<u32> = &mut entry;
      let slot = TaggedSlot::<u32>::null();

      for hash in [0u64, 1, u64::MAX, 0x1234_5678_9abc_def0, 1 << 32] {
         slot.store(raw, hash, Ordering::Relaxed);
         assert_eq!(slot.load(Ordering::Relaxed), raw, "hash {hash:#x}");
         assert_eq!(
            slot.load_if_tag_matches(hash, Ordering::Relaxed),
            Ok(raw),
            "matching tag should hand back the pointer for {hash:#x}"
         );
      }

      // A hash landing on a different tag must be rejected without a deref.
      slot.store(raw, 0, Ordering::Relaxed);
      let stored_tag = hash_tag(0);
      let other = (1..)
         .map(|n| (n as u64) << 18)
         .find(|&h| hash_tag(h) != stored_tag)
         .unwrap();
      assert_eq!(slot.load_if_tag_matches(other, Ordering::Relaxed), Err(()));

      std::mem::forget(entry);
   }

   #[test]
   fn tag_derivation_stays_within_the_reserved_width() {
      for shift in 0..64 {
         assert!(hash_tag(1u64 << shift) <= TAG_MAX);
      }
      for hash in 0..4096u64 {
         assert!(hash_tag(hash << 18) <= TAG_MAX);
      }
      let tags: std::collections::HashSet<usize> =
         (0..1024u64).map(|n| hash_tag(n << 18)).collect();
      assert_eq!(tags.len(), 1024);
   }

   /// Slot words must stay dereferenceable after the tag is stripped, and the
   /// tag must occupy only the reserved bits.
   #[test]
   fn tag_bits_do_not_overlap_the_address() {
      let value = Box::into_raw(Box::new(0u64));
      let addr = value as usize;
      assert_eq!(
         addr & RESERVED_MASK,
         0,
         "test allocation already uses reserved bits"
      );
      let encoded = TaggedSlot::<u64>::encode(value.cast(), 0xffff_ffff_ffff_ffff);
      assert_ne!(encoded as usize & TAG_FLAG, 0, "tag flag was not applied");
      assert_eq!(encoded as usize & !RESERVED_MASK, addr);
      assert_eq!(TaggedSlot::<u64>::decode(encoded as usize), value.cast());
      unsafe { drop(Box::from_raw(value)) };
   }

   /// 5-level/LVA pointers may use those bits: stored verbatim, never
   /// masked unconditionally.
   #[test]
   fn addresses_using_reserved_bits_survive_untagged() {
      let high = ((1usize << TAG_SHIFT) | 0x40) as *mut Entry<u64>;
      assert_eq!(
         TaggedSlot::<u64>::encode(high, u64::MAX) as usize,
         high as usize,
         "high address must be left alone"
      );

      let slot = TaggedSlot::<u64>::null();
      slot.store(high, u64::MAX, Ordering::Relaxed);
      assert_eq!(slot.load(Ordering::Relaxed), high);
      // With no tag present there is nothing to filter on, so every hash has
      // to be handed the pointer rather than skipped.
      assert_eq!(slot.load_if_tag_matches(0, Ordering::Relaxed), Ok(high));
      assert_eq!(
         slot.load_if_tag_matches(u64::MAX, Ordering::Relaxed),
         Ok(high)
      );
   }
}
