use foldhash::fast::FixedState;
use parking_lot::{Condvar, Mutex};
use std::collections::HashMap;
use std::sync::Arc;

pub(super) const FILL_WAIT_MS: u64 = 50;
pub(super) const INFLIGHT_SHARDS: usize = 16;

pub(super) type InflightShard = Mutex<HashMap<String, Arc<FillWait>, FixedState>>;

#[derive(Default)]
pub(super) struct FillWait {
   pub(super) done: Mutex<bool>,
   pub(super) cond: Condvar,
}

// Marks a cache fill as in progress so concurrent misses for the same key wait
// for one upstream round trip instead of stampeding it.
pub struct FillGuard {
   pub(super) key: String,
   pub(super) slot: Arc<FillWait>,
   pub(super) inflight: Arc<Box<[InflightShard]>>,
}

impl Drop for FillGuard {
   fn drop(&mut self) {
      {
         let idx = inflight_shard_idx(&self.key);
         let mut shard = self.inflight[idx].lock();
         if shard
            .get(&self.key)
            .is_some_and(|slot| Arc::ptr_eq(slot, &self.slot))
         {
            shard.remove(&self.key);
         }
      }
      *self.slot.done.lock() = true;
      self.slot.cond.notify_all();
   }
}

pub(super) fn inflight_shard_idx(key: &str) -> usize {
   use std::hash::{BuildHasher, Hasher};
   let mut hasher = FixedState::with_seed(0).build_hasher();
   hasher.write(key.as_bytes());
   (hasher.finish() as usize) & (INFLIGHT_SHARDS - 1)
}
