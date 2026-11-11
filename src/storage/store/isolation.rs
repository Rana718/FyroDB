use crossbeam_utils::CachePadded;
use parking_lot::lock_api::RawRwLock as _;
use parking_lot::{Mutex, MutexGuard, RawRwLock};
use std::cell::Cell;
use std::marker::PhantomData;
use std::sync::atomic::{AtomicUsize, Ordering};

thread_local! {
   static CURRENT: Cell<(*const Isolation, bool)> = const { Cell::new((std::ptr::null(), false)) };
   static AUTO_SLOT: usize = {
      static NEXT: AtomicUsize = AtomicUsize::new(0);
      NEXT.fetch_add(1, Ordering::Relaxed)
   };
}

pub(super) struct Isolation {
   slots: Box<[CachePadded<RawRwLock>]>,
   writer: Mutex<()>,
}

enum Held<'a> {
   Nested,
   Shared(&'a RawRwLock),
   Exclusive(&'a Isolation, #[allow(dead_code)] MutexGuard<'a, ()>),
}

pub struct CommandGuard<'a> {
   held: Held<'a>,
   previous: (*const Isolation, bool),
   _thread: PhantomData<*mut ()>,
}

impl Isolation {
   pub(super) fn new(workers: usize) -> Self {
      Self {
         slots: (0..workers.max(1) + 1)
            .map(|_| CachePadded::new(RawRwLock::INIT))
            .collect::<Vec<_>>()
            .into_boxed_slice(),
         writer: Mutex::new(()),
      }
   }

   #[inline]
   pub(super) fn enter(&self, worker: usize, exclusive: bool) -> CommandGuard<'_> {
      let previous = CURRENT.get();
      let held = if std::ptr::eq(previous.0, self) {
         assert!(
            !exclusive || previous.1,
            "command isolation cannot be upgraded"
         );
         Held::Nested
      } else if exclusive {
         let writer = self.writer.lock();
         for slot in &self.slots {
            slot.lock_exclusive();
         }
         CURRENT.set((self, true));
         Held::Exclusive(self, writer)
      } else {
         let index = if worker == usize::MAX {
            AUTO_SLOT.with(|index| index % self.slots.len())
         } else {
            worker.min(self.slots.len() - 1)
         };
         let slot = &*self.slots[index];
         slot.lock_shared();
         CURRENT.set((self, false));
         Held::Shared(slot)
      };
      CommandGuard {
         held,
         previous,
         _thread: PhantomData,
      }
   }
}

impl Drop for CommandGuard<'_> {
   #[inline]
   fn drop(&mut self) {
      CURRENT.set(self.previous);
      // Release exactly the raw locks acquired by enter on this thread.
      match &self.held {
         Held::Nested => {}
         Held::Shared(slot) => unsafe { slot.unlock_shared() },
         Held::Exclusive(isolation, _) => {
            for slot in isolation.slots.iter().rev() {
               unsafe { slot.unlock_exclusive() };
            }
         }
      }
   }
}

impl super::Store {
   #[inline]
   pub fn command_guard(&self, worker: usize) -> CommandGuard<'_> {
      self.isolation.enter(worker, false)
   }

   pub fn atomic_command_guard(&self) -> CommandGuard<'_> {
      self.isolation.enter(usize::MAX, true)
   }
}
