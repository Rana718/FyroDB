use std::alloc::Layout;
use std::cell::UnsafeCell;
use std::ptr;
use std::sync::atomic::{AtomicPtr, AtomicU64, AtomicUsize, Ordering};

use crossbeam_utils::CachePadded;

use super::ebr;
use super::key::CompactKey;

pub(crate) const STATE_LOCK: u64 = 1;
pub(crate) const STATE_OCCUPIED: u64 = 1 << 1;
pub(crate) const STATE_SEQ_ONE: u64 = 1 << 2;

pub(crate) const LOAD_NUM: usize = 3;
pub(crate) const LOAD_DEN: usize = 4;
pub(crate) const DEFAULT_SHARD_CAPACITY: usize = 32_768;
// Keep idle shards tiny; tables grow lock-free as keys arrive.
pub(crate) const INITIAL_SHARD_CAPACITY: usize = 8;
pub(crate) const GROWING: usize = 1usize << (usize::BITS - 1);

pub(crate) struct Entry<V> {
    pub(crate) hash: u64,
    pub(crate) key: CompactKey,
    pub(crate) state: AtomicU64,
    pub(crate) value: UnsafeCell<std::mem::MaybeUninit<V>>,
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

pub(crate) fn alloc_entry<V>(entry: Entry<V>) -> *mut Entry<V> {
    let layout = Layout::new::<Entry<V>>();
    let ptr = unsafe { rust_zmalloc::alloc_raw(layout) }.cast::<Entry<V>>();
    if ptr.is_null() {
        std::alloc::handle_alloc_error(layout);
    }
    unsafe {
        ptr.write(entry);
    }
    ptr
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
    (state >> 2) as u32
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

/// Spin iterations before a waiter stops burning its timeslice and yields.
///
/// Workers each serve many connections, so a worker spinning on one hot entry
/// is a worker not serving anything else. Backing off and then yielding turns
/// contention back into latency instead of lost throughput.
const MAX_SPIN_BACKOFF: u32 = 64;

#[cold]
fn state_lock_slow(state: &AtomicU64) {
    // Test-and-test-and-set with exponential backoff. Without the backoff every
    // waiter observes the unlock in the same instant and issues a
    // compare-exchange against the same cache line, so one handoff between N
    // contenders costs N exclusive-ownership transfers. On a single hot key that
    // inverted scaling outright: twelve workers ran ~2x slower than one.
    let mut backoff = 1u32;
    loop {
        // Spin on a plain load. The line can stay shared in this core's cache
        // until the holder writes, whereas a CAS takes it exclusively on every
        // attempt and invalidates every other waiter.
        while state.load(Ordering::Relaxed) & STATE_LOCK != 0 {
            for _ in 0..backoff {
                std::hint::spin_loop();
            }
            if backoff < MAX_SPIN_BACKOFF {
                backoff <<= 1;
            } else {
                // More runnable threads than cores is the normal case here, so
                // the holder may not even be scheduled. Hand the CPU over
                // rather than spinning against a descheduled owner.
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

/// Open a seqlock write window on an entry whose `STATE_LOCK` the caller
/// already holds.
///
/// Uses a plain store (no RMW) since no other writer can race. The release
/// fence prevents value writes from being reordered ahead of the odd sequence
/// number, which would let a `read_consistent` reader see a half-written value
/// on a weakly-ordered target.
#[inline(always)]
pub(crate) fn write_begin(state: &AtomicU64) {
    let current = state.load(Ordering::Relaxed);
    state.store(current.wrapping_add(STATE_SEQ_ONE), Ordering::Relaxed);
    std::sync::atomic::fence(Ordering::Release);
}

/// Close the write window, publish the occupied flag, and release the lock in
/// a single release store, replacing four separate read-modify-write ops on
/// the same cache line.
#[inline(always)]
pub(crate) fn write_end(state: &AtomicU64, occupied: bool) {
    let current = state.load(Ordering::Relaxed);
    let mut next = current.wrapping_add(STATE_SEQ_ONE) & !STATE_LOCK;
    if occupied {
        next |= STATE_OCCUPIED;
    } else {
        next &= !STATE_OCCUPIED;
    }
    state.store(next, Ordering::Release);
}

pub(crate) struct SlotTable<V> {
    pub(crate) slots: *mut TaggedSlot<V>,
    pub(crate) capacity: usize,
    pub(crate) mask: usize,
    pub(crate) threshold: usize,
}

/// Bits of a slot word reserved for a hash tag.
///
/// Userspace pointers leave bits 48..63 clear on x86-64 (4-level paging) and
/// AArch64 (39/48-bit VA), so a tag rides in the slot array itself. Probing
/// rejects non-matching entries without dereferencing them, turning each probe
/// step from a dependent cache miss into a register compare.
///
/// Bit 63 marks tag presence. On 5-level x86-64 or AArch64 LVA a pointer may
/// use bits 49..62; storing verbatim and masking only when TAG_FLAG is set
/// avoids silently truncating it.
const TAG_FLAG: usize = 1 << 63;
const TAG_BITS: usize = 14;
const TAG_SHIFT: u32 = 49;
const TAG_MAX: usize = (1 << TAG_BITS) - 1;
const TAG_MASK: usize = TAG_MAX << TAG_SHIFT;
const RESERVED_MASK: usize = TAG_MASK | TAG_FLAG;

/// Derive a slot tag from a key hash.
///
/// Uses bits 32..45, which neither the shard selector (top `log2(shards)`
/// bits) nor the slot index (low `log2(capacity)` bits) consumes, so the tag
/// keeps its full entropy inside a shard.
#[inline(always)]
fn hash_tag(hash: u64) -> usize {
    ((hash >> 32) as usize) & TAG_MAX
}

/// A slot holding an `Entry` pointer with an optional hash tag in its high
/// bits. The tag makes the raw word non-dereferenceable, so this type deposits
/// and strips it rather than exposing the word.
#[repr(transparent)]
pub(crate) struct TaggedSlot<V>(AtomicPtr<Entry<V>>);

impl<V> TaggedSlot<V> {
    #[inline(always)]
    #[allow(dead_code)]
    fn null() -> Self {
        TaggedSlot(AtomicPtr::new(ptr::null_mut()))
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
            (word & !RESERVED_MASK) as *mut Entry<V>
        } else {
            word as *mut Entry<V>
        }
    }

    /// Load the slot's entry pointer, tag stripped and safe to dereference.
    #[inline(always)]
    pub(crate) fn load(&self, order: Ordering) -> *mut Entry<V> {
        Self::decode(self.0.load(order) as usize)
    }

    /// Load the entry pointer only if the slot's tag can match `hash`.
    ///
    /// Returns `Err(())` when the slot holds an entry whose tag rules out a
    /// match — the caller must keep probing without touching the entry.
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
            if (word & TAG_MASK) >> TAG_SHIFT != hash_tag(hash) {
                return Err(());
            }
            return Ok((word & !RESERVED_MASK) as *mut Entry<V>);
        }
        // Untagged: no information to filter on, so the entry must be read.
        Ok(word as *mut Entry<V>)
    }

    #[inline(always)]
    pub(crate) fn store(&self, entry: *mut Entry<V>, hash: u64, order: Ordering) {
        self.0.store(Self::encode(entry, hash), order);
    }

    /// Claim an empty slot for `entry`.
    #[inline(always)]
    pub(crate) fn claim(&self, entry: *mut Entry<V>, hash: u64) -> bool {
        self.0
            .compare_exchange(
                ptr::null_mut(),
                Self::encode(entry, hash),
                Ordering::Release,
                Ordering::Acquire,
            )
            .is_ok()
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
            Layout::array::<TaggedSlot<V>>(self.capacity).expect("slot array layout overflow");
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
        let layout = Layout::array::<TaggedSlot<V>>(cap).expect("slot array layout overflow");
        // calloc gives zeroed pages from the kernel; mimalloc skips touching
        // them until first use, so a fresh table costs no cache pollution.
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
    pub(crate) fn slots(&self) -> &[TaggedSlot<V>] {
        unsafe { std::slice::from_raw_parts(self.slots, self.capacity) }
    }

    pub(crate) fn capacity(&self) -> usize {
        self.capacity
    }
}

#[repr(align(128))]
pub(crate) struct Shard<V> {
    pub(crate) table: AtomicPtr<SlotTable<V>>,
    pub(crate) len: CachePadded<AtomicUsize>,
    pub(crate) insert_gate: CachePadded<AtomicUsize>,
    pub(crate) grow_lock: std::sync::Mutex<()>,
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
    pub(crate) fn new(cap: usize) -> Self {
        let table = alloc_table(SlotTable::new(cap));
        Shard {
            table: AtomicPtr::new(table),
            len: CachePadded::new(AtomicUsize::new(0)),
            insert_gate: CachePadded::new(AtomicUsize::new(0)),
            grow_lock: std::sync::Mutex::new(()),
        }
    }

    #[inline(always)]
    pub(crate) fn table(&self) -> &SlotTable<V> {
        unsafe { &*self.table.load(Ordering::Acquire) }
    }

    #[inline(always)]
    fn enter_insert(&self) -> InsertGuard<'_> {
        loop {
            let state = self.insert_gate.load(Ordering::Relaxed);
            if state & GROWING != 0 {
                std::hint::spin_loop();
                continue;
            }
            if self
                .insert_gate
                .compare_exchange_weak(state, state + 1, Ordering::Acquire, Ordering::Relaxed)
                .is_ok()
            {
                return InsertGuard {
                    gate: &self.insert_gate,
                };
            }
        }
    }

    #[inline(always)]
    pub(crate) fn find(&self, key: &str, hash: u64) -> Option<&Entry<V>> {
        let t = self.table();
        let mut i = (hash as usize) & t.mask;
        loop {
            // Slots whose tag rules out a match are skipped without touching
            // the entry, so a long probe chain stays inside the slot array
            // instead of chasing one cache line per step.
            match unsafe { t.slots().get_unchecked(i) }.load_if_tag_matches(hash, Ordering::Acquire)
            {
                Ok(p) => {
                    if p.is_null() {
                        return None;
                    }
                    let e = unsafe { &*p };
                    if e.hash == hash && e.key == key {
                        return Some(e);
                    }
                }
                Err(()) => {}
            }
            i = (i + 1) & t.mask;
        }
    }

    #[inline(always)]
    pub(crate) fn insert_hashed(&self, key: String, value: V, hash: u64) -> bool {
        let _guard = ebr::pin();
        if let Some(existing) = self.find(&key, hash) {
            state_lock(&existing.state);
            // Re-check under the lock: a concurrent remove between the probe
            // and the lock would otherwise lead to dropping an already-dropped
            // value.
            let was_occupied = state_occupied(&existing.state, Ordering::Relaxed);
            write_begin(&existing.state);
            if was_occupied {
                unsafe { (*existing.value.get()).assume_init_drop() };
            }
            unsafe { (*existing.value.get()).write(value) };
            write_end(&existing.state, true);
            return !was_occupied;
        }
        self.insert_new(key, value, hash)
    }

    pub(crate) fn insert_new(&self, key: String, value: V, hash: u64) -> bool {
        let entry: *mut Entry<V> = alloc_entry(Entry {
            hash,
            key: CompactKey::from_string(key),
            state: AtomicU64::new(STATE_OCCUPIED),
            value: UnsafeCell::new(std::mem::MaybeUninit::new(value)),
        });
        let key_ref: &str = unsafe { (*entry).key.as_str() };

        loop {
            let insert_guard = self.enter_insert();
            let t = self.table();
            if self.len.load(Ordering::Relaxed) >= t.threshold {
                drop(insert_guard);
                self.grow();
                continue;
            }
            let mut reserved = false;

            let t = self.table();
            let mut i = (hash as usize) & t.mask;
            loop {
                let slot = unsafe { t.slots().get_unchecked(i) };
                let p = slot.load(Ordering::Acquire);

                if p.is_null() {
                    if !reserved {
                        let cur = self.len.load(Ordering::Relaxed);
                        if cur >= t.threshold {
                            drop(insert_guard);
                            self.grow();
                            break;
                        }
                        match self.len.compare_exchange_weak(
                            cur,
                            cur + 1,
                            Ordering::Relaxed,
                            Ordering::Relaxed,
                        ) {
                            Ok(_) => {
                                reserved = true;
                            }
                            Err(_) => {
                                std::hint::spin_loop();
                                continue;
                            }
                        }
                    }

                    if slot.claim(entry, hash) {
                        return true;
                    }
                    continue;
                }

                let e = unsafe { &*p };
                if e.hash == hash && e.key == key_ref {
                    if reserved {
                        self.len.fetch_sub(1, Ordering::Relaxed);
                    }
                    state_lock(&e.state);
                    let was_occupied = state_occupied(&e.state, Ordering::Relaxed);
                    write_begin(&e.state);
                    if was_occupied {
                        unsafe { (*e.value.get()).assume_init_drop() };
                    }
                    let moved_value = unsafe { (*(*entry).value.get()).assume_init_read() };
                    unsafe { (*e.value.get()).write(moved_value) };
                    write_end(&e.state, true);
                    unsafe {
                        state_set_occupied(&(*entry).state, false, Ordering::Relaxed);
                        drop_raw_entry::<V>(entry.cast());
                    }
                    return !was_occupied;
                }

                i = (i + 1) & t.mask;
            }
        }
    }

    fn grow(&self) {
        let _lock = self.grow_lock.lock().unwrap_or_else(|e| e.into_inner());

        while self
            .insert_gate
            .compare_exchange_weak(0, GROWING, Ordering::AcqRel, Ordering::Relaxed)
            .is_err()
        {
            std::hint::spin_loop();
        }
        self.grow_locked();
        self.insert_gate.store(0, Ordering::Release);
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

        self.len.store(live_count, Ordering::Relaxed);

        let new_ptr = alloc_table(*new_table);
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
        let mut entry = Entry::<u32> {
            hash: 0,
            key: CompactKey::from_string("k".to_owned()),
            state: AtomicU64::new(STATE_OCCUPIED),
            value: UnsafeCell::new(std::mem::MaybeUninit::new(7)),
        };
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
            .map(|n| (n as u64) << 32)
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
            assert!(hash_tag(hash << 32) <= TAG_MAX);
        }
        // Distinct hashes must actually produce distinct tags, otherwise the
        // filter buys nothing.
        let tags: std::collections::HashSet<usize> =
            (0..1024u64).map(|n| hash_tag(n << 32)).collect();
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

    /// A pointer that genuinely occupies bits 49..62 — possible under x86-64
    /// 5-level paging or AArch64 LVA — must be stored and read back verbatim.
    /// Masking it unconditionally would hand out a truncated address.
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
