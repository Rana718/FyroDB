use crossbeam_utils::CachePadded;
use parking_lot::Mutex;
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;

/// Delivers a retry to the worker that owns a parked connection.
pub trait Wake: Send + Sync {
   fn wake_retry(&self, token: usize, uid: u64);
}

#[derive(Clone)]
pub struct Waiter {
   pub wake: Arc<dyn Wake>,
   pub token: usize,
   pub uid: u64,
}

type WaiterBucket = CachePadded<Mutex<HashMap<String, VecDeque<Waiter>>>>;
const KEY_SHARDS: usize = 256;

fn key_shard(key: &str) -> usize {
   // foldhash-free cheap mixer: sharding only needs to spread, not hash
   let mut h = 0x9E37_79B9_7F4A_7C15u64;
   for b in key.as_bytes() {
      h = (h ^ (*b as u64)).wrapping_mul(0x100_0000_01b3);
   }
   (h >> 32) as usize % KEY_SHARDS
}

pub struct BlockRegistry {
   /// 256 cache-padded shards to eliminate contention during concurrent blocking ops.
   keys: Box<[WaiterBucket]>,
}

impl Default for BlockRegistry {
   fn default() -> Self {
      Self {
         keys: (0..KEY_SHARDS)
            .map(|_| CachePadded::new(Mutex::new(HashMap::new())))
            .collect::<Vec<_>>()
            .into_boxed_slice(),
      }
   }
}

impl BlockRegistry {
   pub fn register(&self, keys: &[String], waiter: &Waiter) {
      for k in keys {
         let mut shard = self.keys[key_shard(k)].lock();
         if let Some(queue) = shard.get_mut(k) {
            if !queue.iter().any(|w| w.uid == waiter.uid) {
               queue.push_back(waiter.clone());
            }
         } else {
            shard.insert(k.clone(), VecDeque::from([waiter.clone()]));
         }
      }
   }

   pub fn unregister(&self, keys: &[String], uid: u64) {
      for key in keys {
         let mut shard = self.keys[key_shard(key)].lock();
         if let Some(queue) = shard.get_mut(key) {
            queue.retain(|w| w.uid != uid);
            if queue.is_empty() {
               shard.remove(key);
            }
         }
      }
   }

   pub fn signal(&self, key: &str) {
      self.signal_many(key, usize::MAX);
   }

   pub(super) fn signal_many(&self, key: &str, count: usize) {
      let waiters: smallvec::SmallVec<[Waiter; 1]> = {
         let mut shard = self.keys[key_shard(key)].lock();
         let Some(queue) = shard.get_mut(key) else {
            return;
         };
         let n = count.min(queue.len());
         let waiters = queue.iter().take(n).cloned().collect();
         queue.rotate_left(n);
         waiters
      };
      for w in waiters {
         w.wake.wake_retry(w.token, w.uid);
      }
   }

   pub fn blocked_clients(&self) -> usize {
      let mut uids: Vec<u64> = Vec::new();
      for shard in &self.keys {
         let map = shard.lock();
         for list in map.values() {
            uids.extend(list.iter().map(|w| w.uid));
         }
      }
      uids.sort_unstable();
      uids.dedup();
      uids.len()
   }
}

#[cfg(test)]
mod tests {
   use super::*;
   use std::sync::Mutex as StdMutex;

   #[derive(Default)]
   struct Rec(StdMutex<Vec<(usize, u64)>>);
   impl Wake for Rec {
      fn wake_retry(&self, token: usize, uid: u64) {
         self.0.lock().unwrap().push((token, uid));
      }
   }

   #[test]
   fn signal_reaches_registered_waiters_until_unregistered() {
      let reg = BlockRegistry::default();
      let rec = Arc::new(Rec::default());
      let w = Waiter {
         wake: rec.clone(),
         token: 7,
         uid: 42,
      };
      let keys = vec!["a".to_string(), "b".to_string()];
      reg.register(&keys, &w);
      reg.register(&keys, &w);
      assert_eq!(reg.blocked_clients(), 1);
      reg.signal("b");
      reg.signal("zzz");
      assert_eq!(*rec.0.lock().unwrap(), vec![(7, 42)]);
      reg.unregister(&keys, 42);
      reg.signal("a");
      assert_eq!(rec.0.lock().unwrap().len(), 1);
   }
}
