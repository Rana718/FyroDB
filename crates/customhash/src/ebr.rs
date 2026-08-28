use std::cell::UnsafeCell;
use std::marker::PhantomData;
use std::ptr;
use std::sync::atomic::{AtomicPtr, AtomicU64, Ordering, fence};

use crossbeam_utils::CachePadded;

const INACTIVE: u64 = 0;
/// Marks a participant slot whose thread has exited. Reclaimable by the next
/// thread that registers. `collect` already ignores it: the epoch check only
/// rejects values below the global epoch, and this is the maximum.
const RETIRED: u64 = u64::MAX;
const COLLECT_INTERVAL: usize = 512;

static GLOBAL_EPOCH: AtomicU64 = AtomicU64::new(1);
static PARTICIPANTS: AtomicPtr<Participant> = AtomicPtr::new(ptr::null_mut());
static ORPHANS: AtomicPtr<OrphanNode> = AtomicPtr::new(ptr::null_mut());

struct Participant {
    local: CachePadded<AtomicU64>,
    next: *mut Participant,
}

unsafe impl Sync for Participant {}
unsafe impl Send for Participant {}

struct Garbage {
    ptr: *mut u8,
    drop_fn: unsafe fn(*mut u8),
    epoch: u64,
}

unsafe impl Send for Garbage {}

struct OrphanNode {
    garbage: Vec<Garbage>,
    next: *mut OrphanNode,
}

struct Local {
    participant: *const Participant,
    garbage: Vec<Garbage>,
    depth: usize,
    retires: usize,
    collect_on_unpin: bool,
    initialized: bool,
}

impl Local {
    const fn uninit() -> Self {
        Local {
            participant: ptr::null(),
            garbage: Vec::new(),
            depth: 0,
            retires: 0,
            collect_on_unpin: false,
            initialized: false,
        }
    }

    #[cold]
    fn initialize(&mut self) {
        // Participant nodes are never unlinked — `collect` walks the list
        // lock-free, so mutating `next` is not safe. Instead a thread that
        // exits marks its slot RETIRED and a later thread claims it, which
        // bounds the list at peak concurrent thread count rather than letting
        // it grow with every thread ever spawned.
        let mut candidate = PARTICIPANTS.load(Ordering::Acquire);
        while !candidate.is_null() {
            let node = unsafe { &*candidate };
            if node
                .local
                .compare_exchange(RETIRED, INACTIVE, Ordering::AcqRel, Ordering::Relaxed)
                .is_ok()
            {
                self.participant = candidate;
                self.garbage = Vec::with_capacity(COLLECT_INTERVAL * 2);
                self.initialized = true;
                return;
            }
            candidate = node.next;
        }

        let p = Box::into_raw(Box::new(Participant {
            local: CachePadded::new(AtomicU64::new(INACTIVE)),
            next: ptr::null_mut(),
        }));
        loop {
            let head = PARTICIPANTS.load(Ordering::Acquire);
            unsafe { (*p).next = head };
            if PARTICIPANTS
                .compare_exchange_weak(head, p, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                break;
            }
        }
        self.participant = p;
        self.garbage = Vec::with_capacity(COLLECT_INTERVAL * 2);
        self.initialized = true;
    }

    #[inline(always)]
    fn ensure_init(&mut self) {
        if !self.initialized {
            self.initialize();
        }
    }

    #[inline(always)]
    fn pin(&mut self) {
        self.ensure_init();
        self.depth += 1;
        if self.depth == 1 {
            let participant = unsafe { &*self.participant };
            loop {
                let e = GLOBAL_EPOCH.load(Ordering::Relaxed);
                participant.local.store(e, Ordering::Release);
                fence(Ordering::Acquire);
                if GLOBAL_EPOCH.load(Ordering::Acquire) == e {
                    break;
                }
                participant.local.store(INACTIVE, Ordering::Release);
            }
        }
    }

    #[inline(always)]
    fn unpin(&mut self) {
        self.depth -= 1;
        if self.depth == 0 {
            unsafe { &*self.participant }
                .local
                .store(INACTIVE, Ordering::Release);
            if self.collect_on_unpin {
                self.collect();
                self.collect();
                self.collect_on_unpin = !self.garbage.is_empty();
            }
        }
    }

    fn collect(&mut self) {
        let mut merged = false;
        if !ORPHANS.load(Ordering::Relaxed).is_null() {
            let mut p = ORPHANS.swap(ptr::null_mut(), Ordering::AcqRel);
            while !p.is_null() {
                let node = unsafe { Box::from_raw(p) };
                p = node.next;
                self.garbage.extend(node.garbage);
                merged = true;
            }
        }
        // `garbage` is otherwise append-ordered by a monotonic epoch, which is
        // what the `partition_point` below relies on. Adopted garbage carries
        // another thread's epochs, so restore the ordering before scanning.
        if merged {
            self.garbage.sort_unstable_by_key(|g| g.epoch);
        }

        let global = GLOBAL_EPOCH.load(Ordering::Acquire);
        let mut all_caught_up = true;
        let mut p = PARTICIPANTS.load(Ordering::Acquire);
        while !p.is_null() {
            let node = unsafe { &*p };
            let e = node.local.load(Ordering::Acquire);
            if e != INACTIVE && e < global {
                all_caught_up = false;
                break;
            }
            p = node.next;
        }
        if all_caught_up {
            let _ = GLOBAL_EPOCH.compare_exchange(
                global,
                global + 1,
                Ordering::AcqRel,
                Ordering::Relaxed,
            );
        }

        let safe = GLOBAL_EPOCH.load(Ordering::Acquire);
        let reclaimable = self.garbage.partition_point(|g| g.epoch + 2 <= safe);
        if reclaimable != 0 {
            for item in self.garbage.drain(..reclaimable) {
                unsafe { (item.drop_fn)(item.ptr) };
            }
        }
    }

    #[inline(always)]
    fn retire_raw(&mut self, ptr: *mut u8, drop_fn: unsafe fn(*mut u8)) {
        let epoch = GLOBAL_EPOCH.load(Ordering::Relaxed);
        self.garbage.push(Garbage {
            ptr,
            drop_fn,
            epoch,
        });
        self.retires += 1;
        if self.retires % COLLECT_INTERVAL == 0 {
            self.collect();
        }
    }
}

impl Drop for Local {
    /// A terminating thread still owns retired pointers that no reader can
    /// reach but whose grace period may not have elapsed. Hand them to the
    /// global orphan list so a surviving thread reclaims them; dropping the
    /// vector alone would leak every entry and table this thread retired.
    fn drop(&mut self) {
        if !self.initialized {
            return;
        }
        unsafe { &*self.participant }
            .local
            .store(INACTIVE, Ordering::Release);

        // One last attempt to retire locally: cheaper than orphaning, and in
        // the common case the grace period has already passed.
        self.collect();

        unsafe { &*self.participant }
            .local
            .store(RETIRED, Ordering::Release);

        if self.garbage.is_empty() {
            return;
        }

        let node = Box::into_raw(Box::new(OrphanNode {
            garbage: std::mem::take(&mut self.garbage),
            next: ptr::null_mut(),
        }));
        loop {
            let head = ORPHANS.load(Ordering::Acquire);
            unsafe { (*node).next = head };
            if ORPHANS
                .compare_exchange_weak(head, node, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                break;
            }
        }
    }
}

pub fn force_collect() {
    LOCAL.with(|c| {
        let l = unsafe { &mut *c.get() };
        // Register even if this thread has never pinned or retired: collection
        // also adopts garbage orphaned by exited threads, and skipping that
        // would leave it stranded whenever the caller is a maintenance thread
        // that only ever reads.
        l.ensure_init();
        l.collect();
        l.collect();
        l.collect();
    });
}

/// Demand-driven quiescence used by destructive commands.  It never frees an
/// object while a reader is pinned; it simply gives concurrent readers a short
/// chance to leave their critical section before collecting retired storage.
pub fn force_collect_quiescent() {
    for _ in 0..64 {
        force_collect();
        std::thread::yield_now();
    }
}

thread_local! {
    static LOCAL: UnsafeCell<Local> = UnsafeCell::new(Local::uninit());
}

pub struct Guard {
    _p: PhantomData<*const ()>,
}

impl Drop for Guard {
    #[inline(always)]
    fn drop(&mut self) {
        LOCAL.with(|c| unsafe { &mut *c.get() }.unpin());
    }
}

#[inline(always)]
pub fn pin() -> Guard {
    LOCAL.with(|c| unsafe { &mut *c.get() }.pin());
    Guard { _p: PhantomData }
}

/// Retire an explicitly allocated object. The callback must destroy the
/// object and release its allocation exactly once after the EBR grace period.
#[inline]
pub unsafe fn retire_raw(ptr: *mut u8, drop_fn: unsafe fn(*mut u8)) {
    if ptr.is_null() {
        return;
    }
    LOCAL.with(|c| {
        let l = unsafe { &mut *c.get() };
        l.ensure_init();
        l.retire_raw(ptr, drop_fn);
        l.collect_on_unpin = true;
    });
}

#[inline(always)]
pub fn with_pin<R>(f: impl FnOnce(&()) -> R) -> R {
    LOCAL.with(|c| {
        let l = unsafe { &mut *c.get() };
        l.pin();
        let r = f(&());
        l.unpin();
        r
    })
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicUsize, Ordering};

    /// These tests inspect process-global EBR state, so they must not overlap
    /// with each other.
    static SERIALIZE: Mutex<()> = Mutex::new(());

    static HANDOFF_DROPPED: AtomicUsize = AtomicUsize::new(0);

    unsafe fn count_handoff_drop(ptr: *mut u8) {
        HANDOFF_DROPPED.fetch_add(1, Ordering::Relaxed);
        drop(unsafe { Box::from_raw(ptr.cast::<u64>()) });
    }

    fn retire_on_new_thread(count: usize) {
        std::thread::spawn(move || {
            for value in 0..count as u64 {
                let leaked = Box::into_raw(Box::new(value)).cast::<u8>();
                unsafe { super::retire_raw(leaked, count_handoff_drop) };
            }
        })
        .join()
        .unwrap();
    }

    /// Retired pointers used to die with the thread that produced them: `Local`
    /// had no destructor, so its garbage vector was dropped without ever
    /// invoking the stored `drop_fn`. Two producer threads also interleave
    /// epochs, which is what forces the merged-garbage sort to be correct —
    /// `collect` trusts `partition_point` over an epoch-sorted vector.
    #[test]
    fn garbage_from_exited_threads_is_still_reclaimed() {
        let _serialize = SERIALIZE.lock().unwrap_or_else(|e| e.into_inner());
        const PER_THREAD: usize = 16;
        const THREADS: usize = 2;

        let before = HANDOFF_DROPPED.load(Ordering::Relaxed);
        for _ in 0..THREADS {
            retire_on_new_thread(PER_THREAD);
        }

        // The exiting thread hands whatever it could not reclaim to ORPHANS;
        // any surviving thread adopts it on its next collect. That may be a
        // thread from another test, so allow time for it to come back around.
        let target = before + THREADS * PER_THREAD;
        for _ in 0..200 {
            super::force_collect();
            if HANDOFF_DROPPED.load(Ordering::Relaxed) >= target {
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(10));
        }

        assert_eq!(HANDOFF_DROPPED.load(Ordering::Relaxed), target);
    }

    /// A slot released by an exited thread must be handed to the next thread
    /// that registers, otherwise the participant list grows with every thread
    /// the process ever spawns and `collect` walks all of them.
    #[test]
    fn participant_slots_are_reused_across_thread_lifetimes() {
        let _serialize = SERIALIZE.lock().unwrap_or_else(|e| e.into_inner());

        fn participant_count() -> usize {
            let mut count = 0;
            let mut p = super::PARTICIPANTS.load(Ordering::Acquire);
            while !p.is_null() {
                count += 1;
                p = unsafe { &*p }.next;
            }
            count
        }

        // Register once so at least one reusable slot exists.
        std::thread::spawn(|| drop(super::pin())).join().unwrap();

        const ROUNDS: usize = 64;
        let before = participant_count();
        for _ in 0..ROUNDS {
            std::thread::spawn(|| drop(super::pin())).join().unwrap();
        }
        let after = participant_count();
        assert!(
            after < before + ROUNDS / 2,
            "participant list grew from {before} to {after} over {ROUNDS} thread lifetimes"
        );
    }
}
