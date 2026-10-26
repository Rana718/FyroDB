use std::sync::atomic::{AtomicBool, AtomicU64};

#[derive(Default)]
pub struct DbCacheMetrics {
   pub hits: AtomicU64,
   pub misses: AtomicU64,
   pub fills: AtomicU64,
   pub fill_races: AtomicU64,
   pub invalidated_entries: AtomicU64,
   pub capacity_evictions: AtomicU64,
   pub expired_pruned: AtomicU64,
   pub external_invalidations: AtomicU64,
   pub coalesced_waits: AtomicU64,
   pub bus_messages_sent: AtomicU64,
   pub bus_messages_received: AtomicU64,
   pub bus_connected: AtomicBool,
   pub pool_idle: AtomicU64,
}

#[derive(Debug, Clone, Copy)]
pub struct CacheLimits {
   pub max_keys: usize,
   pub ttl_jitter_pct: u64,
   pub sync_interval_ms: u64,
}

impl Default for CacheLimits {
   fn default() -> Self {
      Self {
         max_keys: 100_000,
         ttl_jitter_pct: 10,
         sync_interval_ms: 5_000,
      }
   }
}
