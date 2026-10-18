#![allow(dead_code)]
use fyro_db::storage::{store::Store, value::StoreValue};
use std::sync::Arc;
use std::time::{Duration, Instant};

pub fn store() -> Arc<Store> {
   Arc::new(Store::new())
}

pub fn set_str(s: &Store, key: &str, val: &str) {
   s.set(key.to_string(), StoreValue::string(val.to_string()));
}

pub fn set_expiring(s: &Store, key: &str, val: &str, secs: u64) {
   s.set(
      key.to_string(),
      StoreValue::string_with_expiry(
         val.to_string(),
         Some(Instant::now() + Duration::from_secs(secs)),
      ),
   );
}

pub fn set_expired(s: &Store, key: &str, val: &str) {
   s.set(
      key.to_string(),
      StoreValue::string_with_expiry(val.to_string(), Some(Instant::now())),
   );
}
