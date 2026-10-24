use super::*;

/// Writers increment per-worker counters; a fence holder sets the flag
/// and waits for all counters to drain before proceeding.
pub(super) struct WriteFence {
   /// In-flight write count per worker. Uncontended: one worker owns each.
   pub(super) in_flight: Box<[CachePadded<AtomicUsize>]>,
   /// Set while a fence holder is draining or holding the barrier.
   pub(super) engaged: AtomicBool,
   pub(super) gate: Mutex<()>,
}

impl WriteFence {
   pub(super) fn new(workers: usize) -> Self {
      Self {
         in_flight: (0..workers.max(1))
            .map(|_| CachePadded::new(AtomicUsize::new(0)))
            .collect::<Vec<_>>()
            .into_boxed_slice(),
         engaged: AtomicBool::new(false),
         gate: Mutex::new(()),
      }
   }
}

/// Held for the duration of one write command.
pub struct WriteTicket<'a> {
   pub(super) slot: Option<&'a AtomicUsize>,
   pub(super) _gate: Option<std::sync::MutexGuard<'a, ()>>,
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
   pub(super) _gate: std::sync::MutexGuard<'a, ()>,
}

impl Drop for WriteFenceGuard<'_> {
   fn drop(&mut self) {
      self.fence.engaged.store(false, Ordering::Release);
   }
}

impl Store {
   /// Both sides of the fence use SeqCst: the writer's counter step and the
   /// holder's flag step form a dekker handshake, and a weaker store/load pair
   /// on the arming side would let both proceed when the store is still in
   /// flight. The uncontended cost is a locked RMW plus one load per write.
   #[inline]
   pub fn begin_write(&self, worker: usize) -> WriteTicket<'_> {
      let fence = &self.cluster_write_fence;
      let slot = match fence.in_flight.get(worker) {
         Some(slot) => &**slot,
         // Out-of-range worker index: fall back to the barrier rather than
         // executing unfenced.
         None => {
            return WriteTicket {
               slot: None,
               _gate: Some(fence.gate.lock().unwrap_or_else(|e| e.into_inner())),
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
         _gate: Some(fence.gate.lock().unwrap_or_else(|e| e.into_inner())),
      }
   }

   /// Arm the fence and wait for in-flight writes to drain, then disarm it.
   /// Migration only needs this one-time drain: once the slot is marked
   /// migrating, new writes to it are ASK-redirected and never land locally.
   pub fn drain_writes(&self) {
      let _guard = self.cluster_write_guard();
   }

   /// Slot migration and snapshot bootstrap need a stable keyspace,
   /// not writer exclusion.
   pub fn cluster_write_guard(&self) -> WriteFenceGuard<'_> {
      let fence = &self.cluster_write_fence;
      fence.engaged.store(true, Ordering::SeqCst);
      let gate = fence.gate.lock().unwrap_or_else(|e| e.into_inner());
      for slot in fence.in_flight.iter() {
         while slot.load(Ordering::SeqCst) != 0 {
            std::hint::spin_loop();
         }
      }
      WriteFenceGuard { fence, _gate: gate }
   }
}
