use parking_lot::Mutex;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

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

const KEY_SHARDS: usize = 16;

fn key_shard(key: &str) -> usize {
   // foldhash-free cheap mixer: sharding only needs to spread, not hash
   let mut h = 0x9E37_79B9_7F4A_7C15u64;
   for b in key.as_bytes() {
      h = (h ^ (*b as u64)).wrapping_mul(0x100_0000_01b3);
   }
   (h >> 32) as usize % KEY_SHARDS
}

pub struct BlockRegistry {
   /// Registered (key, waiter) pairs; lets `signal` skip the lock.
   waiting: AtomicUsize,
   /// Sharded by key hash (audit §12.2): BLPOP-family wakeups on distinct
   /// keys no longer serialize on one mutex. Same memory as before.
   keys: [Mutex<HashMap<String, Vec<Waiter>>>; KEY_SHARDS],
}

impl Default for BlockRegistry {
   fn default() -> Self {
      Self {
         waiting: AtomicUsize::new(0),
         keys: std::array::from_fn(|_| Mutex::new(HashMap::new())),
      }
   }
}

impl BlockRegistry {
   pub fn register(&self, keys: &[String], waiter: &Waiter) {
      // one lock per shard: register all keys of a command that land in
      // the same shard under a single acquisition
      for k in keys {
         let mut map = self.keys[key_shard(k)].lock();
         let list = map.entry(k.clone()).or_default();
         if !list.iter().any(|w| w.uid == waiter.uid) {
            list.push(waiter.clone());
            self.waiting.fetch_add(1, Ordering::Relaxed);
         }
      }
   }

   pub fn unregister(&self, keys: &[String], uid: u64) {
      for k in keys {
         let mut map = self.keys[key_shard(k)].lock();
         if let Some(list) = map.get_mut(k) {
            let before = list.len();
            list.retain(|w| w.uid != uid);
            self
               .waiting
               .fetch_sub(before - list.len(), Ordering::Relaxed);
            if list.is_empty() {
               map.remove(k);
            }
         }
      }
   }

   /// A write made `key` possibly satisfiable: wake its waiters, oldest
   /// first (each worker retries in wake order).
   #[inline]
   pub fn signal(&self, key: &str) {
      if self.waiting.load(Ordering::Relaxed) == 0 {
         return;
      }
      self.signal_slow(key);
   }

   #[cold]
   fn signal_slow(&self, key: &str) {
      let map = self.keys[key_shard(key)].lock();
      if let Some(list) = map.get(key) {
         for w in list {
            w.wake.wake_retry(w.token, w.uid);
         }
      }
   }

   pub fn blocked_clients(&self) -> usize {
      let mut uids: Vec<u64> = Vec::new();
      for shard in &self.keys {
         let map = shard.lock();
         uids.extend(map.values().flatten().map(|w| w.uid));
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
      assert_eq!(reg.waiting.load(Ordering::Relaxed), 0);
      reg.signal("a");
      assert_eq!(rec.0.lock().unwrap().len(), 1);
   }
}
