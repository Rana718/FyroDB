use crossbeam_utils::CachePadded;
use parking_lot::lock_api::RawRwLock as _;
use parking_lot::{Mutex, MutexGuard, RawRwLock};
use std::cell::Cell;
use std::marker::PhantomData;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

/// Slots reserved for threads that are not workers (expiry, persistence,
/// search jobs, dbcache proxy sessions). Striped so that hundreds of proxy
/// session threads do not all bounce one lock's cache line, while still
/// never sharing a line with a worker.
const AUX_SLOTS: usize = 4;

thread_local! {
   pub(crate) static CURRENT: Cell<(*const Isolation, bool)> = const { Cell::new((std::ptr::null(), false)) };
   static AUX_STRIPE: usize = {
      static NEXT: AtomicUsize = AtomicUsize::new(0);
      NEXT.fetch_add(1, Ordering::Relaxed) % AUX_SLOTS
   };
}

pub struct Isolation {
   /// One slot per worker, then `AUX_SLOTS` for every other thread.
   slots: Box<[CachePadded<RawRwLock>]>,
   workers: usize,
   writer: Mutex<()>,
   /// Total exclusive-admission acquisitions (EVAL / EXEC / WATCH).
   exclusive_total: AtomicU64,
}

enum Held<'a> {
   Nested,
   Shared(&'a RawRwLock),
   AtomicShared(&'a Isolation),
   Exclusive {
      isolation: &'a Isolation,
      _guard: MutexGuard<'a, ()>,
   },
}

pub struct CommandGuard<'a> {
   held: Held<'a>,
   previous: (*const Isolation, bool),
   _thread: PhantomData<*mut ()>,
}

impl Isolation {
   pub(super) fn new(workers: usize) -> Self {
      let workers = workers.max(1);
      Self {
         slots: (0..workers + AUX_SLOTS)
            .map(|_| CachePadded::new(RawRwLock::INIT))
            .collect::<Vec<_>>()
            .into_boxed_slice(),
         workers,
         writer: Mutex::new(()),
         exclusive_total: AtomicU64::new(0),
      }
   }

   #[inline]
   pub(super) fn enter(&self, worker: usize, mode: IsolationMode) -> CommandGuard<'_> {
      let previous = CURRENT.get();
      let exclusive = matches!(mode, IsolationMode::Exclusive);
      let held = if std::ptr::eq(previous.0, self) {
         assert!(
            !exclusive || previous.1,
            "command isolation cannot be upgraded"
         );
         Held::Nested
      } else {
         match mode {
            IsolationMode::Exclusive => {
               self.exclusive_total.fetch_add(1, Ordering::Relaxed);
               let writer = self.writer.lock();
               for slot in &self.slots {
                  slot.lock_exclusive();
               }
               CURRENT.set((self, true));
               Held::Exclusive {
                  isolation: self,
                  _guard: writer,
               }
            }
            IsolationMode::AtomicShared => {
               for slot in &self.slots {
                  slot.lock_shared();
               }
               CURRENT.set((self, false));
               Held::AtomicShared(self)
            }
            IsolationMode::Shared => {
               // Auxiliary threads (and out-of-range indexes) use the reserved
               // stripes after the worker slots, never a worker's line.
               let index = if worker < self.workers {
                  worker
               } else {
                  self.workers + AUX_STRIPE.with(|stripe| *stripe)
               };
               let slot = &*self.slots[index];
               slot.lock_shared();
               CURRENT.set((self, false));
               Held::Shared(slot)
            }
         }
      };
      CommandGuard {
         held,
         previous,
         _thread: PhantomData,
      }
   }
}

pub enum IsolationMode {
   Shared,
   AtomicShared,
   Exclusive,
}

impl Drop for CommandGuard<'_> {
   #[inline]
   fn drop(&mut self) {
      CURRENT.set(self.previous);
      match &self.held {
         Held::Nested => {}
         Held::Shared(slot) => unsafe { slot.unlock_shared() },
         Held::AtomicShared(isolation) => {
            for slot in isolation.slots.iter().rev() {
               unsafe { slot.unlock_shared() };
            }
         }
         Held::Exclusive { isolation, .. } => {
            for slot in isolation.slots.iter().rev() {
               unsafe { slot.unlock_exclusive() };
            }
         }
      }
   }
}

impl super::Store {
   /// Configured worker slots, excluding the auxiliary slots.
   #[inline(always)]
   pub(crate) fn worker_capacity(&self) -> usize {
      self.isolation.workers
   }

   /// Total exclusive-admission acquisitions (EVAL / EXEC / WATCH).
   pub fn isolation_exclusive_total(&self) -> u64 {
      self
         .isolation
         .exclusive_total
         .load(std::sync::atomic::Ordering::Relaxed)
   }

   #[inline]
   pub fn command_guard(&self, worker: usize) -> CommandGuard<'_> {
      self.isolation.enter(worker, IsolationMode::Shared)
   }

   pub fn atomic_command_guard(&self) -> CommandGuard<'_> {
      self.isolation.enter(usize::MAX, IsolationMode::Exclusive)
   }

   pub fn atomic_shared_command_guard(&self) -> CommandGuard<'_> {
      self
         .isolation
         .enter(usize::MAX, IsolationMode::AtomicShared)
   }
}

#[cfg(test)]
mod aux_slot_tests {
   use super::*;

   #[test]
   fn auxiliary_threads_spread_over_reserved_slots_and_exclusive_still_waits() {
      let isolation = std::sync::Arc::new(Isolation::new(2));
      // Which slot each of several auxiliary threads takes.
      let used: std::collections::HashSet<usize> = (0..16)
         .map(|_| {
            let isolation = std::sync::Arc::clone(&isolation);
            std::thread::spawn(move || {
               let guard = isolation.enter(usize::MAX, IsolationMode::Shared);
               let Held::Shared(slot) = &guard.held else {
                  panic!("shared")
               };
               let index = isolation
                  .slots
                  .iter()
                  .position(|s| std::ptr::eq(&**s, *slot))
                  .unwrap();
               drop(guard);
               index
            })
            .join()
            .unwrap()
         })
         .collect();
      assert!(
         used.iter().all(|&i| i >= 2),
         "auxiliary threads never use a worker slot"
      );
      assert!(
         used.len() > 1,
         "auxiliary threads are striped, not all on one line"
      );

      // An exclusive holder still excludes every auxiliary stripe.
      let exclusive = isolation.enter(0, IsolationMode::Exclusive);
      let blocked = {
         let isolation = std::sync::Arc::clone(&isolation);
         std::thread::spawn(move || {
            let started = std::time::Instant::now();
            let _guard = isolation.enter(usize::MAX, IsolationMode::Shared);
            started.elapsed()
         })
      };
      std::thread::sleep(std::time::Duration::from_millis(50));
      drop(exclusive);
      assert!(blocked.join().unwrap() >= std::time::Duration::from_millis(40));
   }
}
