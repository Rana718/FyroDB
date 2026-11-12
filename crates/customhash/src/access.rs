use crossbeam_utils::CachePadded;
use std::cell::{Cell, RefCell};
use std::marker::PhantomData;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

type LocalWrite = (u64, usize);
static NEXT_ID: AtomicU64 = AtomicU64::new(1);
struct ReadState {
   count: AtomicUsize,
   id: u64,
}
thread_local! {
   static READING: Cell<u64> = const { Cell::new(0) };
   static WRITING: Cell<LocalWrite> = const { Cell::new((0, 0)) };
   static OTHER_WRITES: RefCell<Vec<LocalWrite>> = const { RefCell::new(Vec::new()) };
}

pub(crate) struct Access {
   readers: CachePadded<ReadState>,
   writers: CachePadded<AtomicUsize>,
}

#[must_use]
pub struct ReadEpochGuard<'a> {
   access: &'a Access,
   _thread: PhantomData<*mut ()>,
}

#[must_use]
pub struct WriteEpochGuard<'a> {
   access: &'a Access,
   _thread: PhantomData<*mut ()>,
}

impl Access {
   pub(crate) fn new() -> Self {
      let id = NEXT_ID
         .try_update(Ordering::Relaxed, Ordering::Relaxed, |id| id.checked_add(1))
         .expect("map access IDs exhausted");
      Self {
         readers: CachePadded::new(ReadState {
            count: AtomicUsize::new(0),
            id,
         }),
         writers: CachePadded::new(AtomicUsize::new(0)),
      }
   }

   #[inline(always)]
   pub(crate) fn reading(&self) -> bool {
      READING.get() == self.readers.id
   }

   #[inline]
   pub(crate) fn try_read(&self) -> Option<ReadEpochGuard<'_>> {
      if READING.get() != 0 || self.writers.load(Ordering::Acquire) != 0 {
         return None;
      }
      self.readers.count.fetch_add(1, Ordering::SeqCst);
      // The second check pairs with writer registration so either the
      // writer waits for this reader, or this reader falls back.
      if self.writers.load(Ordering::SeqCst) != 0 {
         self.readers.count.fetch_sub(1, Ordering::Release);
         return None;
      }
      READING.set(self.readers.id);
      Some(ReadEpochGuard {
         access: self,
         _thread: PhantomData,
      })
   }

   #[inline]
   pub(crate) fn write(&self) -> WriteEpochGuard<'_> {
      let current = WRITING.get();
      if current.0 == self.readers.id {
         WRITING.set((current.0, current.1 + 1));
      } else if !OTHER_WRITES.with(|writes| {
         let mut writes = writes.borrow_mut();
         if let Some(slot) = writes.iter_mut().find(|slot| slot.0 == self.readers.id) {
            slot.1 += 1;
            true
         } else {
            false
         }
      }) {
         assert!(!self.reading(), "mutation inside a protected read batch");
         self.writers.fetch_add(1, Ordering::SeqCst);
         let mut spins = 0;
         while self.readers.count.load(Ordering::SeqCst) != 0 {
            if spins < 64 {
               spins += 1;
               std::hint::spin_loop();
            } else {
               std::thread::yield_now();
            }
         }
         if current.0 != 0 {
            OTHER_WRITES.with(|writes| writes.borrow_mut().push(current));
         }
         WRITING.set((self.readers.id, 1));
      }
      WriteEpochGuard {
         access: self,
         _thread: PhantomData,
      }
   }
}

impl Drop for ReadEpochGuard<'_> {
   #[inline]
   fn drop(&mut self) {
      READING.set(0);
      self.access.readers.count.fetch_sub(1, Ordering::Release);
   }
}

impl Drop for WriteEpochGuard<'_> {
   #[inline]
   fn drop(&mut self) {
      let current = WRITING.get();
      let last = if current.0 == self.access.readers.id {
         if current.1 > 1 {
            WRITING.set((current.0, current.1 - 1));
            false
         } else {
            let previous = OTHER_WRITES.with(|writes| writes.borrow_mut().pop());
            WRITING.set(previous.unwrap_or((0, 0)));
            true
         }
      } else {
         OTHER_WRITES.with(|writes| {
            let mut writes = writes.borrow_mut();
            let index = writes
               .iter()
               .position(|slot| slot.0 == self.access.readers.id)
               .expect("write epoch guard missing");
            writes[index].1 -= 1;
            if writes[index].1 == 0 {
               writes.swap_remove(index);
               true
            } else {
               false
            }
         })
      };
      if last {
         self.access.writers.fetch_sub(1, Ordering::Release);
      }
   }
}

impl WriteEpochGuard<'_> {
   pub(crate) fn guards(&self, access: &Access) -> bool {
      std::ptr::eq(self.access, access)
   }
}
