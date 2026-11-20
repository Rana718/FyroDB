use super::*;

/// Writers increment per-worker counters; a fence holder sets the flag
/// and waits for all counters to drain before proceeding.
pub(super) struct WriteFence {
   /// In-flight write count per worker. Uncontended: one worker owns each.
   pub(super) in_flight: Box<[CachePadded<AtomicUsize>]>,
   /// Set while a fence holder is draining or holding the barrier.
   pub(super) engaged: AtomicBool,
   pub(super) gate: parking_lot::Mutex<()>,
}

impl WriteFence {
   pub(super) fn new(workers: usize) -> Self {
      Self {
         in_flight: (0..workers.max(1))
            .map(|_| CachePadded::new(AtomicUsize::new(0)))
            .collect::<Vec<_>>()
            .into_boxed_slice(),
         engaged: AtomicBool::new(false),
         gate: parking_lot::Mutex::new(()),
      }
   }
}

/// Held for the duration of one write command.
pub struct WriteTicket<'a> {
   pub(super) slot: Option<&'a AtomicUsize>,
   pub(super) _gate: Option<parking_lot::MutexGuard<'a, ()>>,
}

impl Drop for WriteTicket<'_> {
   #[inline(always)]
   fn drop(&mut self) {
      if let Some(slot) = self.slot {
         slot.fetch_sub(1, Ordering::Release);
      }
   }
}

/// Held while a bulk cluster operation needs a stable view of the keyspace.
pub struct WriteFenceGuard<'a> {
   pub(super) fence: &'a WriteFence,
   pub(super) _gate: parking_lot::MutexGuard<'a, ()>,
}

impl Drop for WriteFenceGuard<'_> {
   fn drop(&mut self) {
      self.fence.engaged.store(false, Ordering::Release);
   }
}

impl Store {
   #[inline]
   pub fn begin_write(&self, worker: usize) -> WriteTicket<'_> {
      let fence = &self.cluster_write_fence;
      let slot = match fence.in_flight.get(worker) {
         Some(slot) => &**slot,
         None => {
            return WriteTicket {
               slot: None,
               _gate: Some(fence.gate.lock()),
            };
         }
      };

      slot.fetch_add(1, Ordering::SeqCst);
      if !fence.engaged.load(Ordering::SeqCst) {
         return WriteTicket {
            slot: Some(slot),
            _gate: None,
         };
      }
      slot.fetch_sub(1, Ordering::Release);
      WriteTicket {
         slot: None,
         _gate: Some(fence.gate.lock()),
      }
   }

   pub fn drain_writes(&self) {
      let _guard = self.cluster_write_guard();
   }

   /// Slot migration and snapshot bootstrap need a stable keyspace,
   /// not writer exclusion.
   pub fn cluster_write_guard(&self) -> WriteFenceGuard<'_> {
      let fence = &self.cluster_write_fence;
      let gate = fence.gate.lock();
      fence.engaged.store(true, Ordering::SeqCst);
      for slot in fence.in_flight.iter() {
         while slot.load(Ordering::SeqCst) != 0 {
            std::hint::spin_loop();
         }
      }
      WriteFenceGuard { fence, _gate: gate }
   }
}

#[cfg(test)]
mod tests {
   use super::*;

   #[test]
   fn overlapping_fence_requests_each_keep_the_fence_engaged() {
      let store = Store::with_config_workers(1, 16, 2);
      let start = std::sync::Barrier::new(2);
      std::thread::scope(|scope| {
         for _ in 0..2 {
            scope.spawn(|| {
               start.wait();
               for _ in 0..500 {
                  let _guard = store.cluster_write_guard();
                  std::thread::yield_now();
                  assert!(store.cluster_write_fence.engaged.load(Ordering::SeqCst));
               }
            });
         }
      });
   }
}
