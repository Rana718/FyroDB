use super::Store;
use crate::search::hooks::HookObserver;
use crate::storage::value::now_ms;
use arc_swap::ArcSwap;
use customhash::WriteObserver;
use foldhash::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

pub(super) struct WatchRegistry {
   /// ArcSwap snapshot of watched keys for lock-free read on writes.
   keys: ArcSwap<HashMap<String, Arc<AtomicU64>>>,
   observing: Arc<customhash::Flag>,
}

pub struct WatchedKey {
   key: String,
   registry: Arc<WatchRegistry>,
   version: Arc<AtomicU64>,
   initial: u64,
   expires_ms: u64,
}

impl WatchedKey {
   pub fn changed(&self) -> bool {
      self.version.load(Ordering::Acquire) != self.initial
         || (self.expires_ms != 0 && now_ms() >= self.expires_ms)
   }
}

impl Drop for WatchedKey {
   fn drop(&mut self) {
      // Copy-on-write publish if this was the last watcher for the key.
      let mut keys = self.registry.keys.load_full();
      if Arc::strong_count(&self.version) == 2 {
         let map = Arc::make_mut(&mut keys);
         map.remove(&self.key);
         let empty = map.is_empty();
         self.registry.keys.store(keys);
         if empty {
            self.registry.observing.set_watching(false);
         }
      }
   }
}

impl WatchRegistry {
   pub(super) fn new(observing: Arc<customhash::Flag>) -> Self {
      Self {
         keys: ArcSwap::default(),
         observing,
      }
   }
}

pub(super) struct StoreObserver {
   pub(super) search: HookObserver,
   pub(super) watches: Arc<WatchRegistry>,
}

impl WriteObserver for StoreObserver {
   fn on_write(&self, key: &str) {
      if self.watches.observing.is_watching()
         && let Some(version) = self.watches.keys.load().get(key)
      {
         version.fetch_add(1, Ordering::Release);
      }
      if self.search.0.is_active() {
         self.search.on_write(key);
      }
   }

   fn on_clear(&self) {
      for version in self.watches.keys.load().values() {
         version.fetch_add(1, Ordering::Release);
      }
      self.search.on_clear();
   }
}

impl Store {
   pub fn watch_key(&self, key: &str) -> WatchedKey {
      let _guard = self.atomic_command_guard();
      let expires_ms = self.data.get_ref(key).map_or(0, |v| v.expires_ms);
      let expires_ms = if expires_ms != 0 && now_ms() >= expires_ms {
         self.del(key);
         0
      } else {
         expires_ms
      };
      let mut keys = self.watches.keys.load_full();
      let map = Arc::make_mut(&mut keys);
      let first = map.is_empty();
      let version = Arc::clone(map.entry(key.to_owned()).or_default());
      let initial = version.load(Ordering::Acquire);
      self.watches.keys.store(keys);
      if first {
         self.watches.observing.set_watching(true);
      }
      WatchedKey {
         key: key.to_owned(),
         registry: Arc::clone(&self.watches),
         version,
         initial,
         expires_ms,
      }
   }
}
