use crossbeam_utils::CachePadded;
use parking_lot::Mutex;
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering, fence};

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
   /// Queued (key, waiter) registrations. Lets every push skip the shard
   /// lock and lookup while nobody is blocked, which is nearly always.
   queued: CachePadded<AtomicUsize>,
}

impl Default for BlockRegistry {
   fn default() -> Self {
      Self {
         keys: (0..KEY_SHARDS)
            .map(|_| CachePadded::new(Mutex::new(HashMap::new())))
            .collect::<Vec<_>>()
            .into_boxed_slice(),
         queued: CachePadded::new(AtomicUsize::new(0)),
      }
   }
}

impl BlockRegistry {
   /// The caller must re-attempt its operation after this returns: together
   /// with the check in `signal_many` that closes the race with a push that
   /// lands while the waiter registers.
   pub fn register(&self, keys: &[String], waiter: &Waiter) {
      for k in keys {
         let mut shard = self.keys[key_shard(k)].lock();
         if let Some(queue) = shard.get_mut(k) {
            if !queue.iter().any(|w| w.uid == waiter.uid) {
               queue.push_back(waiter.clone());
               self.queued.fetch_add(1, Ordering::SeqCst);
            }
         } else {
            shard.insert(k.clone(), VecDeque::from([waiter.clone()]));
            self.queued.fetch_add(1, Ordering::SeqCst);
         }
      }
      // Order the registration before the caller's re-attempt reads the data
      // (pairs with the fence in `signal_many`).
      fence(Ordering::SeqCst);
   }

   pub fn unregister(&self, keys: &[String], uid: u64) {
      for key in keys {
         let mut shard = self.keys[key_shard(key)].lock();
         if let Some(queue) = shard.get_mut(key) {
            let before = queue.len();
            queue.retain(|w| w.uid != uid);
            let removed = before - queue.len();
            if removed != 0 {
               self.queued.fetch_sub(removed, Ordering::Relaxed);
            }
            if queue.is_empty() {
               shard.remove(key);
            }
         }
      }
   }

   pub fn signal(&self, key: &str) {
      self.signal_many(key, usize::MAX);
   }

   /// Called after the push is published. Either this sees a registration and
   /// takes the shard lock (which orders it after the waiter's insert), or the
   /// waiter's re-attempt after `register` sees the pushed data.
   pub(super) fn signal_many(&self, key: &str, count: usize) {
      if self.queued.load(Ordering::Relaxed) == 0 {
         return;
      }
      fence(Ordering::SeqCst);
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
   use parking_lot::Mutex;

   #[derive(Default)]
   struct Rec(Mutex<Vec<(usize, u64)>>);
   impl Wake for Rec {
      fn wake_retry(&self, token: usize, uid: u64) {
         self.0.lock().push((token, uid));
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
      assert_eq!(*rec.0.lock(), vec![(7, 42)]);
      reg.unregister(&keys, 42);
      reg.signal("a");
      assert_eq!(rec.0.lock().len(), 1);
   }

   #[test]
   fn the_registration_count_tracks_queued_waiters() {
      let reg = BlockRegistry::default();
      let rec = Arc::new(Rec::default());
      let waiter = |uid| Waiter {
         wake: rec.clone(),
         token: 1,
         uid,
      };
      let keys = vec!["a".to_string(), "b".to_string()];
      reg.register(&keys, &waiter(1));
      reg.register(&keys, &waiter(1));
      reg.register(&keys[..1], &waiter(2));
      assert_eq!(reg.queued.load(Ordering::Relaxed), 3);
      reg.unregister(&keys, 1);
      assert_eq!(reg.queued.load(Ordering::Relaxed), 1);
      reg.unregister(&keys, 2);
      reg.unregister(&keys, 2);
      assert_eq!(reg.queued.load(Ordering::Relaxed), 0);
      reg.signal("a");
      assert!(rec.0.lock().is_empty());
   }

   /// A push that lands while a waiter registers is never lost: either the
   /// push sees the registration and wakes it, or the waiter's re-check
   /// after `register` sees the pushed item.
   #[test]
   fn a_push_racing_a_registration_is_never_lost() {
      use std::sync::atomic::AtomicBool;
      for _ in 0..2_000 {
         let reg = BlockRegistry::default();
         let rec = Arc::new(Rec::default());
         let item = AtomicBool::new(false);
         let keys = vec!["q".to_string()];
         let served = std::thread::scope(|s| {
            s.spawn(|| {
               item.store(true, Ordering::Release);
               reg.signal_many("q", 1);
            });
            let waiter = Waiter {
               wake: rec.clone(),
               token: 1,
               uid: 9,
            };
            reg.register(&keys, &waiter);
            item.load(Ordering::Acquire)
         });
         let woken = !rec.0.lock().is_empty();
         assert!(
            served || woken,
            "push between first attempt and registration was lost"
         );
      }
   }
}
