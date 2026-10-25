mod catalog;
mod encoded;
mod inflight;
mod metrics;
mod storage;
mod tracker;
pub(crate) mod util;

pub use catalog::CatalogSnapshot;
pub use encoded::EncodedResult;
pub use inflight::FillGuard;
pub use metrics::{CacheLimits, DbCacheMetrics};
pub use storage::{DbCacheStorage, MemoryStorage};
pub use tracker::TableCounters;
pub use util::{mix64, now_ms};

use crate::query::{
   ParsedQuery, ParsedQueryCache, compute_fingerprint_with_params_base, make_cache_key,
};
use catalog::CatalogState;
use foldhash::fast::FixedState;
use inflight::{FILL_WAIT_MS, FillWait, INFLIGHT_SHARDS, InflightShard, inflight_shard_idx};
use parking_lot::RwLock;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use tracker::{ExternalSync, TableTracker, index_key, index_prefix, table_from_marker};

const ENCODED_SHARDS: usize = 64;
const CATALOG_REFRESH_MIN_MS: u64 = 1_000;
const CATALOG_REFRESH_RETRY_MS: u64 = 5_000;
const PRUNE_INTERVAL_MS: u64 = 60_000;

type EncodedShard = RwLock<HashMap<String, Arc<EncodedResult>, FixedState>>;

pub struct DbCacheManager {
   storage: Arc<dyn DbCacheStorage>,
   tracker: RwLock<TableTracker>,
   encoded: Box<[EncodedShard]>,
   parsed: ParsedQueryCache,
   catalog: CatalogState,
   metrics: DbCacheMetrics,
   limits: CacheLimits,
   sync: ExternalSync,
   last_prune_ms: AtomicU64,
   inflight: Arc<Box<[InflightShard]>>,
}

impl DbCacheManager {
   pub fn new(storage: Arc<dyn DbCacheStorage>) -> Self {
      Self::with_limits(storage, CacheLimits::default())
   }

   pub fn with_limits(storage: Arc<dyn DbCacheStorage>, limits: CacheLimits) -> Self {
      let mut encoded = Vec::with_capacity(ENCODED_SHARDS);
      for _ in 0..ENCODED_SHARDS {
         encoded.push(RwLock::new(HashMap::with_hasher(FixedState::with_seed(0))));
      }
      let mut inflight_shards: Vec<InflightShard> = Vec::with_capacity(INFLIGHT_SHARDS);
      for _ in 0..INFLIGHT_SHARDS {
         inflight_shards.push(parking_lot::Mutex::new(HashMap::with_hasher(
            FixedState::with_seed(0),
         )));
      }
      Self {
         storage,
         tracker: RwLock::new(TableTracker::default()),
         encoded: encoded.into_boxed_slice(),
         parsed: ParsedQueryCache::default(),
         catalog: CatalogState::new(),
         metrics: DbCacheMetrics::default(),
         limits,
         sync: ExternalSync::default(),
         last_prune_ms: AtomicU64::new(now_ms()),
         inflight: Arc::new(inflight_shards.into_boxed_slice()),
      }
   }

   pub fn limits(&self) -> CacheLimits {
      self.limits
   }

   pub fn metrics(&self) -> &DbCacheMetrics {
      &self.metrics
   }

   // Parses a statement at most once per process, regardless of client count.
   pub fn analyze(&self, sql: &str) -> Arc<ParsedQuery> {
      self.parsed.get_or_parse(sql)
   }

   // Prepared-statement fingerprint combining SQL text and bound values.
   pub fn analyze_bound(
      &self,
      sql: &str,
      params: &[Option<Vec<u8>>],
      param_formats: &[i16],
   ) -> (Arc<ParsedQuery>, u64) {
      let parsed = self.analyze(sql);
      if params.is_empty() {
         return (Arc::clone(&parsed), parsed.fingerprint);
      }
      let fingerprint =
         compute_fingerprint_with_params_base(parsed.fingerprint, params, param_formats);
      (parsed, fingerprint)
   }

   pub fn parsed_cache_len(&self) -> usize {
      self.parsed.len()
   }

   // ------------------------------------------------------------- catalog

   pub fn catalog_generation(&self) -> u64 {
      self.catalog.generation.load(Ordering::Acquire)
   }

   pub fn mark_catalog_stale(&self) {
      self.catalog.stale.store(true, Ordering::Release);
   }

   pub fn catalog_ready(&self) -> bool {
      self.catalog.snapshot.read().is_some()
   }

   pub fn catalog_refresh_due(&self) -> bool {
      let snapshot = self.catalog.snapshot.read();
      if snapshot.is_some() && !self.catalog.stale.load(Ordering::Acquire) {
         return false;
      }
      let min_interval = if snapshot.is_some() {
         CATALOG_REFRESH_MIN_MS
      } else {
         CATALOG_REFRESH_RETRY_MS
      };
      let elapsed = now_ms().saturating_sub(self.catalog.last_attempt_ms.load(Ordering::Relaxed));
      elapsed >= min_interval
   }

   pub fn try_begin_catalog_refresh(&self) -> bool {
      if !self.catalog_refresh_due() {
         return false;
      }
      if self
         .catalog
         .refreshing
         .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
         .is_err()
      {
         return false;
      }
      self
         .catalog
         .last_attempt_ms
         .store(now_ms(), Ordering::Relaxed);
      true
   }

   pub fn install_catalog(&self, snapshot: CatalogSnapshot) {
      *self.catalog.snapshot.write() = Some(Arc::new(snapshot));
      self.catalog.generation.fetch_add(1, Ordering::AcqRel);
      self.catalog.stale.store(false, Ordering::Release);
      self.catalog.refreshing.store(false, Ordering::Release);
      self
         .catalog
         .last_refresh_ms
         .store(now_ms(), Ordering::Relaxed);
   }

   pub fn catalog_refresh_failed(&self) {
      self.catalog.refreshing.store(false, Ordering::Release);
      self
         .catalog
         .last_attempt_ms
         .store(now_ms(), Ordering::Relaxed);
   }

   pub fn is_cacheable(&self, parsed: &ParsedQuery) -> bool {
      if !parsed.is_cacheable {
         return false;
      }
      let snapshot = self.catalog.snapshot.read();
      match snapshot.as_ref() {
         Some(snapshot) if !self.catalog.stale.load(Ordering::Acquire) => {
            !parsed
               .functions
               .iter()
               .any(|name| snapshot.is_volatile(name))
               && !snapshot.references_volatile_view(&parsed.tables)
         }
         Some(_) => parsed.functions.is_empty(),
         None => true,
      }
   }

   pub fn expanded_tables(&self, parsed: &ParsedQuery) -> Vec<String> {
      let snapshot = self.catalog.snapshot.read();
      match snapshot.as_ref() {
         Some(snapshot) => snapshot.expand(&parsed.tables),
         None => parsed.tables.clone(),
      }
   }

   // -------------------------------------------------------------- cache

   fn encoded_shard(&self, key: &str) -> &EncodedShard {
      use std::hash::{BuildHasher, Hasher};
      let mut hasher = FixedState::with_seed(0).build_hasher();
      hasher.write(key.as_bytes());
      &self.encoded[(hasher.finish() as usize) & (ENCODED_SHARDS - 1)]
   }

   pub fn cache_key(&self, prefix: &str, table: &str, fingerprint: u64) -> String {
      make_cache_key(prefix, table, fingerprint)
   }

   pub fn get_encoded(
      &self,
      prefix: &str,
      table: &str,
      fingerprint: u64,
   ) -> Option<Arc<EncodedResult>> {
      let key = make_cache_key(prefix, table, fingerprint);
      let shard = self.encoded_shard(&key);

      let entry = { shard.read().get(&key).cloned() }?;
      let now = now_ms();
      let expired = entry.expires_at_ms != 0 && entry.expires_at_ms < now;
      if expired || (!self.storage.is_local() && !self.storage.exists(&key)) {
         shard.write().remove(&key);
         return None;
      }
      self.metrics.hits.fetch_add(1, Ordering::Relaxed);
      Some(entry)
   }

   pub fn get_cached_query(
      &self,
      prefix: &str,
      table: &str,
      fingerprint: u64,
   ) -> Option<crate::row::DbResultSet> {
      let key = make_cache_key(prefix, table, fingerprint);
      let raw = self.storage.get(&key)?;
      crate::row::DbResultSet::from_json(&raw)
   }

   pub fn store_cached_query(
      &self,
      prefix: &str,
      tables: &[String],
      fingerprint: u64,
      result: &crate::row::DbResultSet,
      ttl_ms: u64,
   ) {
      self.store_cached_query_versioned(prefix, tables, fingerprint, result, ttl_ms, &[]);
   }

   pub fn table_versions(&self, tables: &[String]) -> Vec<u64> {
      let tracker = self.tracker.read();
      tables.iter().map(|table| tracker.epoch(table)).collect()
   }

   pub fn store_cached_query_versioned(
      &self,
      prefix: &str,
      tables: &[String],
      fingerprint: u64,
      result: &crate::row::DbResultSet,
      ttl_ms: u64,
      versions: &[u64],
   ) -> bool {
      self.store_cached_query_jittered(prefix, tables, fingerprint, result, ttl_ms, 0, versions)
   }

   #[allow(clippy::too_many_arguments)]
   pub fn store_cached_query_jittered(
      &self,
      prefix: &str,
      tables: &[String],
      fingerprint: u64,
      result: &crate::row::DbResultSet,
      ttl_ms: u64,
      jitter_pct: u64,
      versions: &[u64],
   ) -> bool {
      let primary_table = tables.first().map(|s| s.as_str()).unwrap_or("default");
      let key = make_cache_key(prefix, primary_table, fingerprint);
      let ttl_ms = util::jittered_ttl(ttl_ms, jitter_pct, fingerprint);
      let json = result.to_json();
      let encoded = Arc::new(EncodedResult::from_result_set(result, ttl_ms));

      let mut tracker = self.tracker.write();
      if !versions.is_empty() {
         for (table, version) in tables.iter().zip(versions) {
            if tracker.epoch(table) != *version {
               self.metrics.fill_races.fetch_add(1, Ordering::Relaxed);
               return false;
            }
         }
      }

      self.storage.set(&key, &json, ttl_ms);
      self
         .encoded_shard(&key)
         .write()
         .insert(key.clone(), encoded);

      for table in tables {
         let marker = index_key(prefix, table, fingerprint);
         tracker.register(table, key.clone(), marker.clone());
         self.storage.set(&marker, &key, ttl_ms);
      }
      drop(tracker);

      self.metrics.fills.fetch_add(1, Ordering::Relaxed);
      self.enforce_capacity(prefix);
      true
   }

   pub fn remember_encoded(
      &self,
      prefix: &str,
      table: &str,
      fingerprint: u64,
      result: &crate::row::DbResultSet,
      ttl_ms: u64,
   ) {
      let key = make_cache_key(prefix, table, fingerprint);
      let encoded = Arc::new(EncodedResult::from_result_set(result, ttl_ms));
      self.encoded_shard(&key).write().insert(key, encoded);
   }

   pub fn invalidate_tables(&self, tables: &[String]) -> usize {
      let mut to_delete = Vec::new();
      let mut markers = Vec::new();
      {
         let mut tracker = self.tracker.write();
         tracker.bump(tables);
         let mut matched: HashSet<String, FixedState> =
            HashSet::with_hasher(FixedState::with_seed(0));
         for table in tables {
            for name in tracker.matching_tables(table) {
               if matched.insert(name.clone()) {
                  to_delete.extend(tracker.remove_table(&name));
               }
            }
         }
         for key in &to_delete {
            markers.extend(tracker.remove_markers(key));
         }
      }

      let mut count = 0;
      for key in to_delete {
         if self.storage.del(&key) {
            count += 1;
         }
         self.encoded_shard(&key).write().remove(&key);
      }
      for marker in markers {
         self.storage.del(&marker);
      }
      self
         .metrics
         .invalidated_entries
         .fetch_add(count as u64, Ordering::Relaxed);
      count
   }

   pub fn flush_all(&self, prefix: &str) -> usize {
      let mut removed = 0;
      for (key, _) in self.storage.scan_prefix(prefix) {
         if key.starts_with(prefix) && self.storage.del(&key) {
            removed += 1;
         }
      }
      self.tracker.write().clear();
      for shard in &self.encoded {
         shard.write().clear();
      }
      removed
   }

   pub fn rebuild_tracker(&self, prefix: &str) -> usize {
      let marker_prefix = index_prefix(prefix);
      let mut tracker = self.tracker.write();
      let mut count = 0;
      for (marker, value) in self.storage.scan_prefix(&marker_prefix) {
         let Some(cache_key) = value else {
            continue;
         };
         let Some(table) = table_from_marker(prefix, &marker).map(str::to_string) else {
            continue;
         };
         if self.storage.exists(&cache_key) {
            tracker.register(&table, cache_key, marker);
            count += 1;
         }
      }
      count
   }

   pub fn tracked_key_count(&self) -> usize {
      self.tracker.read().key_count()
   }

   pub fn tracked_table_names(&self) -> Vec<String> {
      self.tracker.read().tracked_table_names()
   }

   pub fn storage(&self) -> &Arc<dyn DbCacheStorage> {
      &self.storage
   }

   // ---------------------------------------------------- capacity / pruning

   fn enforce_capacity(&self, prefix: &str) -> usize {
      let max = self.limits.max_keys;
      if max == 0 {
         return 0;
      }
      let evicted: Vec<(String, Vec<String>)> = {
         let mut tracker = self.tracker.write();
         let total = tracker.key_count();
         if total <= max {
            return 0;
         }
         let excess = total - max;
         let mut keys = Vec::with_capacity(excess);
         while keys.len() < excess {
            match tracker.insert_order.pop_front() {
               Some(k) if tracker.contains_key(&k) => keys.push(k),
               Some(_) => {}
               None => break,
            }
         }
         keys
            .into_iter()
            .map(|key| {
               let markers = tracker.detach_key(prefix, &key);
               (key, markers)
            })
            .collect()
      };

      for (key, markers) in &evicted {
         self.storage.del(key);
         self.encoded_shard(key).write().remove(key);
         for marker in markers {
            self.storage.del(marker);
         }
      }
      self
         .metrics
         .capacity_evictions
         .fetch_add(evicted.len() as u64, Ordering::Relaxed);
      evicted.len()
   }

   pub fn prune_due(&self) -> bool {
      now_ms().saturating_sub(self.last_prune_ms.load(Ordering::Relaxed)) >= PRUNE_INTERVAL_MS
   }

   pub fn prune_expired(&self, prefix: &str) -> usize {
      if !self.prune_due() {
         return 0;
      }
      self.last_prune_ms.store(now_ms(), Ordering::Relaxed);
      let now = now_ms();
      let expired: Vec<String> = {
         let tracker = self.tracker.read();
         tracker
            .markers_keys()
            .filter(|key: &&String| {
               let shard = self.encoded_shard(key);
               shard
                  .read()
                  .get(*key)
                  .is_some_and(|entry| entry.expires_at_ms != 0 && entry.expires_at_ms < now)
            })
            .cloned()
            .collect()
      };
      if expired.is_empty() {
         return 0;
      }

      let mut removed = Vec::with_capacity(expired.len());
      {
         let mut tracker = self.tracker.write();
         for key in expired {
            let markers = tracker.detach_key(prefix, &key);
            removed.push((key, markers));
         }
      }
      for (key, markers) in &removed {
         self.encoded_shard(key).write().remove(key);
         for marker in markers {
            self.storage.del(marker);
         }
      }
      self
         .metrics
         .expired_pruned
         .fetch_add(removed.len() as u64, Ordering::Relaxed);
      removed.len()
   }

   // -------------------------------------------------------- external sync

   pub fn external_sync_due(&self) -> bool {
      let interval = self.limits.sync_interval_ms;
      if interval == 0 || self.tracked_key_count() == 0 {
         return false;
      }
      let now = now_ms();
      if now.saturating_sub(self.sync.last_poll_ms.load(Ordering::Relaxed)) < interval {
         return false;
      }
      if self.sync.in_progress.swap(true, Ordering::AcqRel) {
         let started = self.sync.poll_started_ms.load(Ordering::Relaxed);
         if now.saturating_sub(started) < 30_000 {
            return false;
         }
      }
      self.sync.poll_started_ms.store(now, Ordering::Relaxed);
      self.sync.last_poll_ms.store(now, Ordering::Relaxed);
      true
   }

   pub fn abort_external_sync(&self) {
      self.sync.in_progress.store(false, Ordering::Release);
   }

   pub fn apply_external_counters(&self, rows: Vec<(String, TableCounters)>) -> Vec<String> {
      let mut changed = Vec::new();
      {
         let mut snapshot = self.sync.snapshot.lock();
         for (table, counters) in rows {
            match snapshot.get(&table) {
               Some(previous) if *previous == counters => {}
               Some(_) => {
                  snapshot.insert(table.clone(), counters);
                  changed.push(table);
               }
               None => {
                  snapshot.insert(table, counters);
               }
            }
         }
      }
      self.sync.in_progress.store(false, Ordering::Release);
      if changed.is_empty() {
         return changed;
      }
      self
         .metrics
         .external_invalidations
         .fetch_add(changed.len() as u64, Ordering::Relaxed);
      self.invalidate_tables(&changed);
      changed
   }

   // ----------------------------------------------------------- coalescing

   pub fn begin_fill(&self, key: &str) -> Option<FillGuard> {
      let idx = inflight_shard_idx(key);
      let mut shard = self.inflight[idx].lock();
      if shard.contains_key(key) {
         return None;
      }
      let slot = Arc::new(FillWait::default());
      shard.insert(key.to_string(), Arc::clone(&slot));
      Some(FillGuard {
         key: key.to_string(),
         slot,
         inflight: Arc::clone(&self.inflight),
      })
   }

   pub fn wait_for_fill(&self, key: &str) -> bool {
      let idx = inflight_shard_idx(key);
      let slot = self.inflight[idx].lock().get(key).cloned();
      let Some(slot) = slot else {
         return false;
      };
      let mut done = slot.done.lock();
      if *done {
         return true;
      }
      self.metrics.coalesced_waits.fetch_add(1, Ordering::Relaxed);
      slot
         .cond
         .wait_for(&mut done, Duration::from_millis(FILL_WAIT_MS));
      *done
   }

   // --------------------------------------------------------------- stats

   pub fn stats(&self) -> Vec<(&'static str, u64)> {
      let encoded_entries: usize = self.encoded.iter().map(|shard| shard.read().len()).sum();
      let last_refresh = self.catalog.last_refresh_ms.load(Ordering::Relaxed);
      let catalog_age = if last_refresh == 0 {
         0
      } else {
         now_ms().saturating_sub(last_refresh)
      };
      vec![
         ("hits", self.metrics.hits.load(Ordering::Relaxed)),
         ("misses", self.metrics.misses.load(Ordering::Relaxed)),
         ("fills", self.metrics.fills.load(Ordering::Relaxed)),
         (
            "fill_races",
            self.metrics.fill_races.load(Ordering::Relaxed),
         ),
         (
            "invalidated_entries",
            self.metrics.invalidated_entries.load(Ordering::Relaxed),
         ),
         (
            "capacity_evictions",
            self.metrics.capacity_evictions.load(Ordering::Relaxed),
         ),
         (
            "expired_pruned",
            self.metrics.expired_pruned.load(Ordering::Relaxed),
         ),
         (
            "external_invalidations",
            self.metrics.external_invalidations.load(Ordering::Relaxed),
         ),
         (
            "coalesced_waits",
            self.metrics.coalesced_waits.load(Ordering::Relaxed),
         ),
         ("tracked_keys", self.tracked_key_count() as u64),
         ("encoded_entries", encoded_entries as u64),
         ("parsed_entries", self.parsed.len() as u64),
         ("catalog_generation", self.catalog_generation()),
         ("catalog_age_ms", catalog_age),
         ("max_keys", self.limits.max_keys as u64),
         ("ttl_jitter_pct", self.limits.ttl_jitter_pct),
         ("sync_interval_ms", self.limits.sync_interval_ms),
         (
            "bus_messages_sent",
            self.metrics.bus_messages_sent.load(Ordering::Relaxed),
         ),
         (
            "bus_messages_received",
            self.metrics.bus_messages_received.load(Ordering::Relaxed),
         ),
         (
            "bus_connected",
            u64::from(self.metrics.bus_connected.load(Ordering::Relaxed)),
         ),
         ("pool_idle", self.metrics.pool_idle.load(Ordering::Relaxed)),
      ]
   }
}
