use super::Store;
use fyro_dbcache::DbCacheStorage;

impl DbCacheStorage for Store {
   fn get(&self, key: &str) -> Option<String> {
      let _guard = self.command_guard(usize::MAX);
      self.get(key)
   }

   fn set(&self, key: &str, value: &str, ttl_ms: u64) {
      let _guard = self.command_guard(usize::MAX);
      let expires_ms = if ttl_ms > 0 {
         crate::storage::value::now_ms() + ttl_ms
      } else {
         0
      };
      self.set_string(key, value, expires_ms);
   }

   fn del(&self, key: &str) -> bool {
      let _guard = self.command_guard(usize::MAX);
      self.del(key)
   }

   fn exists(&self, key: &str) -> bool {
      let _guard = self.command_guard(usize::MAX);
      self.exists(key)
   }

   fn is_local(&self) -> bool {
      true
   }

   fn scan_prefix(&self, prefix: &str) -> Vec<(String, Option<String>)> {
      let _guard = self.command_guard(usize::MAX);
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
