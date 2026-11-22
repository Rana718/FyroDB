use crossbeam_utils::CachePadded;
use std::cell::{Cell, RefCell};
use std::marker::PhantomData;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};

type LocalWrite = (u64, usize);
/// One stripe per worker (bound with `bind_thread_stripe`) plus room for
/// auxiliary threads, which take stripes from the top so they do not share a
/// line with low-numbered workers.
const WRITE_STRIPES: usize = 16;
static NEXT_AUX_STRIPE: AtomicUsize = AtomicUsize::new(0);
static NEXT_ID: AtomicU64 = AtomicU64::new(1);
const UNBOUND: usize = usize::MAX;

struct ReadState {
   count: AtomicUsize,
   id: u64,
}

thread_local! {
   static STRIPE: Cell<usize> = const { Cell::new(UNBOUND) };
   static READING: Cell<u64> = const { Cell::new(0) };
   static WRITING: Cell<LocalWrite> = const { Cell::new((0, 0)) };
   static OTHER_WRITES: RefCell<Vec<LocalWrite>> = const { RefCell::new(Vec::new()) };
}

/// This thread's writer stripe, assigned on first use if not bound.
#[inline(always)]
fn stripe() -> usize {
   STRIPE.with(|cell| {
      let index = cell.get();
      if index != UNBOUND {
         return index;
      }
      let index =
         WRITE_STRIPES - 1 - NEXT_AUX_STRIPE.fetch_add(1, Ordering::Relaxed) % WRITE_STRIPES;
      cell.set(index);
      index
   })
}

/// Bind the calling thread to a writer stripe (a worker passes its index).
/// A thread that already wrote keeps its stripe: changing it while a write
/// epoch is open would unbalance the stripe's started/finished counters.
pub fn bind_thread_stripe(index: usize) {
   STRIPE.with(|cell| {
      if cell.get() == UNBOUND {
         cell.set(index % WRITE_STRIPES);
      }
   });
}

#[derive(Default)]
struct WriterStripe {
   started: AtomicU64,
   finished: AtomicU64,
}

impl WriterStripe {
   fn active(&self) -> bool {
      self.finished.load(Ordering::SeqCst) != self.started.load(Ordering::SeqCst)
   }
}

pub struct ReadStamp {
   id: u64,
   versions: [u64; WRITE_STRIPES],
}

pub(crate) struct Access {
   readers: CachePadded<ReadState>,
   writers: Box<[CachePadded<WriterStripe>; WRITE_STRIPES]>,
   stamps_disabled: AtomicBool,
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
         writers: Box::new(std::array::from_fn(|_| CachePadded::default())),
         stamps_disabled: AtomicBool::new(false),
      }
   }

   #[inline(always)]
   pub(crate) fn reading(&self) -> bool {
      READING.get() == self.readers.id
   }

   pub(crate) fn stamp(&self) -> Option<ReadStamp> {
      let mut versions = [0; WRITE_STRIPES];
      for (v, stripe) in versions.iter_mut().zip(self.writers.iter()) {
         let done = stripe.finished.load(Ordering::SeqCst);
         *v = stripe.started.load(Ordering::SeqCst);
         if *v != done {
            return None;
         }
      }
      (!self.stamps_disabled.load(Ordering::SeqCst)).then_some(ReadStamp {
         id: self.readers.id,
         versions,
      })
   }

   #[inline]
   pub(crate) fn matches(&self, stamp: &ReadStamp) -> bool {
      stamp.id == self.readers.id
         && self
            .writers
            .iter()
            .zip(stamp.versions.iter())
            .all(|(s, v)| s.started.load(Ordering::SeqCst) == *v)
         && !self.stamps_disabled.load(Ordering::SeqCst)
   }

   pub(crate) fn read(&self) -> ReadEpochGuard<'_> {
      assert_eq!(READING.get(), 0, "nested read epoch");
      assert!(
         WRITING.get().0 != self.readers.id
            && !OTHER_WRITES.with_borrow(|others| others.iter().any(|w| w.0 == self.readers.id)),
         "read epoch inside a write epoch"
      );
      self.readers.count.fetch_add(1, Ordering::SeqCst);
      let backoff = crossbeam_utils::Backoff::new();
      for stripe in self.writers.iter() {
         while stripe.active() {
            backoff.snooze();
         }
      }
      READING.set(self.readers.id);
      ReadEpochGuard {
         access: self,
         _thread: PhantomData,
      }
   }

   #[inline]
   pub(crate) fn try_read(&self) -> Option<ReadEpochGuard<'_>> {
      if READING.get() != 0 {
         return None;
      }
      // Dekker handshake against writer registration.
      self.readers.count.fetch_add(1, Ordering::SeqCst);
      if self.writers.iter().any(|stripe| stripe.active()) {
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
      assert!(!self.reading(), "mutation inside a protected read batch");
      let current = WRITING.get();
      if current.0 == self.readers.id {
         WRITING.set((current.0, current.1 + 1));
      } else if current.0 == 0
         || !OTHER_WRITES.with_borrow_mut(|others| {
            if let Some(slot) = others.iter_mut().find(|w| w.0 == self.readers.id) {
               slot.1 += 1;
               return true;
            }
            others.push(current);
            false
         })
      {
         let stripe = &self.writers[stripe()];
         loop {
            if stripe.started.fetch_add(1, Ordering::SeqCst) >= u64::MAX / 2 {
               self.stamps_disabled.store(true, Ordering::SeqCst);
            }
            if self.readers.count.load(Ordering::SeqCst) == 0 {
               break;
            }
            // Withdraw before waiting so an admitted snapshot can drain active writers.
            stripe.finished.fetch_add(1, Ordering::SeqCst);
            let backoff = crossbeam_utils::Backoff::new();
            while self.readers.count.load(Ordering::Acquire) != 0 {
               backoff.snooze();
            }
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
            WRITING.set(OTHER_WRITES.with_borrow_mut(|others| others.pop().unwrap_or_default()));
            true
         }
      } else {
         OTHER_WRITES.with_borrow_mut(|others| {
            let i = others
               .iter()
               .position(|w| w.0 == self.access.readers.id)
               .expect("unregistered writer");
            others[i].1 -= 1;
            if others[i].1 != 0 {
               return false;
            }
            others.swap_remove(i);
            true
         })
      };
      if last {
         self.access.writers[stripe()]
            .finished
            .fetch_add(1, Ordering::SeqCst);
      }
   }
}

impl WriteEpochGuard<'_> {
   pub(crate) fn guards(&self, access: &Access) -> bool {
      std::ptr::eq(self.access, access)
   }
}
