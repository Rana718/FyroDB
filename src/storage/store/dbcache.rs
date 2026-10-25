use super::Store;
use fyro_dbcache::DbCacheStorage;

impl DbCacheStorage for Store {
   fn get(&self, key: &str) -> Option<String> {
      self.get(key)
   }

   fn set(&self, key: &str, value: &str, ttl_ms: u64) {
      let expires_ms = if ttl_ms > 0 {
         crate::storage::value::now_ms() + ttl_ms
      } else {
         0
      };
      self.set_string(key, value, expires_ms);
   }

   fn del(&self, key: &str) -> bool {
      self.del(key)
   }

   fn exists(&self, key: &str) -> bool {
      self.exists(key)
   }

   fn is_local(&self) -> bool {
      true
   }

   fn scan_prefix(&self, prefix: &str) -> Vec<(String, Option<String>)> {
      let mut entries = Vec::new();
      self.data.for_each(|key, val| {
         if !key.starts_with(prefix) || val.is_expired() {
            return;
         }
         let value = val.value.as_string().map(|s| s.to_string());
         entries.push((key.to_string(), value));
      });
      entries
   }
}
