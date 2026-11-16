use crossbeam_utils::CachePadded;
use std::cell::Cell;
use std::marker::PhantomData;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

type LocalWrite = (u64, usize);
static NEXT_ID: AtomicU64 = AtomicU64::new(1);
static NEXT_MARKER: AtomicUsize = AtomicUsize::new(1);

struct ReadState {
   count: AtomicUsize,
   id: u64,
}

thread_local! {
   static READING: Cell<u64> = const { Cell::new(0) };
   static WRITING: Cell<LocalWrite> = const { Cell::new((0, 0)) };
   // Item 22: Fixed inline stack avoids RefCell borrow checks and heap Vec allocations.
   static OTHER_WRITES: Cell<[LocalWrite; 4]> = const { Cell::new([(0, 0); 4]) };
   static WRITE_MARKER: Cell<usize> = const { Cell::new(0) };
}

/// Writer-admission stripes (scaled to 64 to eliminate pigeonhole stripe collisions across cores).
const WRITE_STRIPES: usize = 64;

pub(crate) struct Access {
   readers: CachePadded<ReadState>,
   writers: Box<[CachePadded<AtomicUsize>]>,
   active_writers: CachePadded<AtomicU64>,
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
      let writers = (0..WRITE_STRIPES)
         .map(|_| CachePadded::new(AtomicUsize::new(0)))
         .collect::<Vec<_>>()
         .into_boxed_slice();
      Self {
         readers: CachePadded::new(ReadState {
            count: AtomicUsize::new(0),
            id,
         }),
         writers,
         active_writers: CachePadded::new(AtomicU64::new(0)),
      }
   }

   #[inline(always)]
   pub(crate) fn reading(&self) -> bool {
      READING.get() == self.readers.id
   }

   /// Deterministic per (thread, map) stripe: nested writes on one map
   /// always hit the same slot without per-thread lookup tables. The
   /// marker is one cold counter fetch per thread.
   #[inline(always)]
   fn write_stripe(&self) -> usize {
      let marker = WRITE_MARKER.with(|m| {
         let value = m.get();
         if value == 0 {
            let fresh = NEXT_MARKER.fetch_add(1, Ordering::Relaxed);
            m.set(fresh);
            fresh
         } else {
            value
         }
      });
      (marker.rotate_left(17) ^ self.readers.id as usize) % self.writers.len()
   }

   #[inline]
   pub(crate) fn try_read(&self) -> Option<ReadEpochGuard<'_>> {
      if READING.get() != 0 {
         return None;
      }
      // Check active writers bitmask with in-cache relaxed load (1 cycle, 0 bus lock)
      if self.active_writers.load(Ordering::Relaxed) != 0 {
         return None;
      }
      // The reader registers before re-checking stripes: the SeqCst RMW
      // plus check form the Dekker handshake — whichever side registers second observes the other.
      self.readers.count.fetch_add(1, Ordering::SeqCst);
      if self.active_writers.load(Ordering::SeqCst) != 0 {
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
      } else {
         let mut nested = false;
         let mut others = OTHER_WRITES.get();
         for slot in others.iter_mut() {
            if slot.0 == self.readers.id {
               slot.1 += 1;
               nested = true;
               break;
            }
         }
         if nested {
            OTHER_WRITES.set(others);
         } else {
            assert!(!self.reading(), "mutation inside a protected read batch");
            // Stripe-isolated write admission: each core updates its own cache line exclusively.
            let stripe_idx = self.write_stripe();
            let stripe = &self.writers[stripe_idx];
            let prev = stripe.fetch_add(1, Ordering::SeqCst);
            if prev == 0 {
               self.active_writers.fetch_or(1u64 << (stripe_idx % 64), Ordering::SeqCst);
            }
            let mut spins = 0;
            // TTAS with pause backoff: check relaxed in-cache before forcing SeqCst bus sync
            while self.readers.count.load(Ordering::Relaxed) != 0 || self.readers.count.load(Ordering::SeqCst) != 0 {
               if spins < 1024 {
                  spins += 1;
                  std::hint::spin_loop();
               } else {
                  std::thread::yield_now();
                  spins = 0;
               }
            }
            if current.0 != 0 {
               for slot in others.iter_mut() {
                  if slot.0 == 0 {
                     *slot = current;
                     break;
                  }
               }
               OTHER_WRITES.set(others);
            }
            WRITING.set((self.readers.id, 1));
         }
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
            let mut others = OTHER_WRITES.get();
            let mut previous = (0, 0);
            for i in (0..others.len()).rev() {
               if others[i].0 != 0 {
                  previous = others[i];
                  others[i] = (0, 0);
                  break;
               }
            }
            OTHER_WRITES.set(others);
            WRITING.set(previous);
            true
         }
      } else {
         let mut others = OTHER_WRITES.get();
         let mut is_last = false;
         for slot in others.iter_mut() {
            if slot.0 == self.access.readers.id {
               slot.1 -= 1;
               if slot.1 == 0 {
                  *slot = (0, 0);
                  is_last = true;
               }
               break;
            }
         }
         OTHER_WRITES.set(others);
         is_last
      };
      if last {
         let stripe_idx = self.access.write_stripe();
         let stripe = &self.access.writers[stripe_idx];
         if stripe.fetch_sub(1, Ordering::SeqCst) == 1 {
            self.access.active_writers.fetch_and(!(1u64 << (stripe_idx % 64)), Ordering::SeqCst);
         }
      }
   }
}

impl WriteEpochGuard<'_> {
   pub(crate) fn guards(&self, access: &Access) -> bool {
      std::ptr::eq(self.access, access)
   }
}
