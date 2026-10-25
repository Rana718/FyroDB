use super::util::now_ms;
use foldhash::fast::FixedState;
use parking_lot::RwLock;
use std::collections::HashMap;
use std::hash::{BuildHasher, Hasher};

pub(super) type MemoryShard = RwLock<HashMap<String, (String, u64), FixedState>>;

pub trait DbCacheStorage: Send + Sync {
   fn get(&self, key: &str) -> Option<String>;
   fn set(&self, key: &str, value: &str, ttl_ms: u64);
   fn del(&self, key: &str) -> bool;

   fn exists(&self, key: &str) -> bool {
      self.get(key).is_some()
   }

   fn scan_prefix(&self, prefix: &str) -> Vec<(String, Option<String>)> {
      let _ = prefix;
      Vec::new()
   }

   // Returns true when the storage is local and cannot have keys deleted
   // through a separate RESP port. Skips the per-hit existence check.
   fn is_local(&self) -> bool {
      false
   }
}

pub struct MemoryStorage {
   shards: Box<[MemoryShard]>,
}

impl Default for MemoryStorage {
   fn default() -> Self {
      let mut shards = Vec::with_capacity(64);
      for _ in 0..64 {
         shards.push(RwLock::new(HashMap::with_hasher(FixedState::with_seed(0))));
      }
      Self {
         shards: shards.into_boxed_slice(),
      }
   }
}

impl MemoryStorage {
   fn shard(&self, key: &str) -> &MemoryShard {
      let mut h = FixedState::with_seed(0).build_hasher();
      h.write(key.as_bytes());
      &self.shards[(h.finish() as usize) & 63]
   }
}

impl DbCacheStorage for MemoryStorage {
   fn get(&self, key: &str) -> Option<String> {
      let now = now_ms();
      let guard = self.shard(key).read();
      let (val, exp) = guard.get(key)?;
      if *exp != 0 && *exp < now {
         None
      } else {
         Some(val.clone())
      }
   }

   fn set(&self, key: &str, value: &str, ttl_ms: u64) {
      let exp = if ttl_ms > 0 { now_ms() + ttl_ms } else { 0 };
      self
         .shard(key)
         .write()
         .insert(key.to_string(), (value.to_string(), exp));
   }

   fn del(&self, key: &str) -> bool {
      self.shard(key).write().remove(key).is_some()
   }

   fn exists(&self, key: &str) -> bool {
      let now = now_ms();
      self
         .shard(key)
         .read()
         .get(key)
         .is_some_and(|(_, exp)| *exp == 0 || *exp >= now)
   }

   fn scan_prefix(&self, prefix: &str) -> Vec<(String, Option<String>)> {
      let now = now_ms();
      self
         .shards
         .iter()
         .flat_map(|shard| {
            shard
               .read()
               .iter()
               .filter(|(k, (_, exp))| k.starts_with(prefix) && (*exp == 0 || *exp >= now))
               .map(|(k, (v, _))| (k.clone(), Some(v.clone())))
               .collect::<Vec<_>>()
         })
         .collect()
   }

   fn is_local(&self) -> bool {
      true
   }
}
