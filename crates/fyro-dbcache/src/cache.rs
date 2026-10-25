use crate::protocol;
use crate::query::{
   ParsedQuery, ParsedQueryCache, compute_fingerprint_with_params, make_cache_key,
};
use crate::row::{ColumnInfo, DbResultSet, DbRow, DbValue};
use foldhash::fast::FixedState;
use parking_lot::{Condvar, Mutex, RwLock};
use std::collections::{HashMap, HashSet, VecDeque};
use std::hash::{BuildHasher, Hasher};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

const ENCODED_SHARDS: usize = 64;

type EncodedShard = RwLock<HashMap<String, Arc<EncodedResult>>>;
const CATALOG_REFRESH_MIN_MS: u64 = 1_000;
const CATALOG_REFRESH_RETRY_MS: u64 = 5_000;
const PRUNE_INTERVAL_MS: u64 = 60_000;
const FILL_WAIT_MS: u64 = 250;

// Storage backend interface implemented by FyroDB's core Store.
pub trait DbCacheStorage: Send + Sync {
   fn get(&self, key: &str) -> Option<String>;
   fn set(&self, key: &str, value: &str, ttl_ms: u64);
   fn del(&self, key: &str) -> bool;

   // Cheap liveness check used to keep the local encoded-response cache in
   // sync with keys deleted through the RESP port.
   fn exists(&self, key: &str) -> bool {
      self.get(key).is_some()
   }

   // Enumerates live key/value pairs under a prefix. Used for startup cache
   // flushing and table-index rebuilds.
   fn scan_prefix(&self, prefix: &str) -> Vec<(String, Option<String>)> {
      let _ = prefix;
      Vec::new()
   }
}

#[derive(Default)]
pub struct MemoryStorage {
   map: RwLock<HashMap<String, (String, u64)>>,
}

impl DbCacheStorage for MemoryStorage {
   fn get(&self, key: &str) -> Option<String> {
      let now = now_ms();
      let guard = self.map.read();
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
         .map
         .write()
         .insert(key.to_string(), (value.to_string(), exp));
   }

   fn del(&self, key: &str) -> bool {
      self.map.write().remove(key).is_some()
   }

   fn exists(&self, key: &str) -> bool {
      let now = now_ms();
      self
         .map
         .read()
         .get(key)
         .is_some_and(|(_, exp)| *exp == 0 || *exp >= now)
   }

   fn scan_prefix(&self, prefix: &str) -> Vec<(String, Option<String>)> {
      let now = now_ms();
      self
         .map
         .read()
         .iter()
         .filter(|(key, (_, exp))| key.starts_with(prefix) && (*exp == 0 || *exp >= now))
         .map(|(key, (value, _))| (key.clone(), Some(value.clone())))
         .collect()
   }
}

// Wire-ready encoding of a cached result: no JSON parsing or row encoding on
// the hot path, just memcpy into the response buffer.
pub struct EncodedResult {
   pub row_description: Vec<u8>,
   pub data_rows: Vec<u8>,
   pub command_complete: Vec<u8>,
   pub columns: Vec<ColumnInfo>,
   pub row_count: u32,
   pub expires_at_ms: u64,
}

impl EncodedResult {
   pub fn from_result_set(result: &DbResultSet, ttl_ms: u64) -> Self {
      let (row_description, data_rows, row_count) =
         protocol::encode_result(&result.columns, &result.rows);
      Self {
         row_description,
         data_rows,
         command_complete: protocol::encode_command_complete(&format!("SELECT {row_count}")),
         columns: result.columns.clone(),
         row_count,
         expires_at_ms: expiry_ms(ttl_ms),
      }
   }

   // Simple-query response: RowDescription, rows, CommandComplete, ReadyForQuery.
   pub fn write_simple_response(&self, out: &mut Vec<u8>) {
      out.extend_from_slice(&self.row_description);
      out.extend_from_slice(&self.data_rows);
      out.extend_from_slice(&self.command_complete);
      out.extend_from_slice(&protocol::READY_IDLE);
   }

   // Extended-protocol Execute response: rows plus CommandComplete.
   pub fn write_execute_response(&self, out: &mut Vec<u8>) {
      out.extend_from_slice(&self.data_rows);
      out.extend_from_slice(&self.command_complete);
   }
}

impl std::fmt::Debug for EncodedResult {
   fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
      f.debug_struct("EncodedResult")
         .field("columns", &self.columns)
         .field("row_count", &self.row_count)
         .field(
            "bytes",
            &(self.row_description.len() + self.data_rows.len()),
         )
         .finish()
   }
}

// Catalog metadata that decides whether a query result is shareable.
#[derive(Default)]
pub struct CatalogSnapshot {
   // Functions with provolatile 's' (stable) or 'v' (volatile), keyed by both
   // bare and schema-qualified names.
   volatile: HashSet<String>,
   // View or materialized view -> directly referenced relations.
   view_tables: HashMap<String, Vec<String>>,
   // Views whose definition calls a non-immutable function.
   volatile_views: HashSet<String>,
}

impl CatalogSnapshot {
   // Builds a snapshot from `(nspname, proname)` rows and
   // `(view, pg_get_viewdef, base relation)` rows.
   pub fn from_rows(function_rows: &[DbRow], view_rows: &[DbRow]) -> Self {
      let mut snapshot = CatalogSnapshot::default();
      for row in function_rows {
         let (Some(schema), Some(name)) = (row_text(row, 0), row_text(row, 1)) else {
            continue;
         };
         snapshot.volatile.insert(name.to_string());
         snapshot.volatile.insert(format!("{schema}.{name}"));
      }

      let mut definitions: HashMap<String, String> = HashMap::new();
      for row in view_rows {
         let Some(view) = row_text(row, 0) else {
            continue;
         };
         let view = view.to_ascii_lowercase();
         if let Some(definition) = row_text(row, 1) {
            definitions
               .entry(view.clone())
               .or_insert_with(|| definition.to_ascii_lowercase());
         }
         let Some(base) = row_text(row, 2) else {
            continue;
         };
         let base = base.to_ascii_lowercase();
         let mut bases = vec![base.clone()];
         if let Some((_, bare)) = base.rsplit_once('.') {
            bases.push(bare.to_string());
         }
         let entry = snapshot.view_tables.entry(view.clone()).or_default();
         for candidate in bases {
            if !entry.contains(&candidate) {
               entry.push(candidate);
            }
         }
      }

      let volatile_names: Vec<&String> = snapshot.volatile.iter().collect();
      for (view, definition) in &definitions {
         if volatile_names
            .iter()
            .any(|name| definition_mentions(definition, name))
         {
            snapshot.volatile_views.insert(view.clone());
         }
      }
      // Propagate volatility through nested views.
      for _ in 0..8 {
         let mut changed = false;
         for (view, bases) in &snapshot.view_tables {
            if snapshot.volatile_views.contains(view) {
               continue;
            }
            if bases.iter().any(|base| {
               let bare = base.rsplit('.').next().unwrap_or(base);
               snapshot.volatile_views.contains(base) || snapshot.volatile_views.contains(bare)
            }) {
               snapshot.volatile_views.insert(view.clone());
               changed = true;
            }
         }
         if !changed {
            break;
         }
      }
      snapshot
   }

   pub fn is_volatile(&self, name: &str) -> bool {
      self.volatile.contains(name)
         || self
            .volatile
            .contains(name.rsplit('.').next().unwrap_or(name))
   }

   pub fn references_volatile_view(&self, tables: &[String]) -> bool {
      tables.iter().any(|table| {
         let bare = table.rsplit('.').next().unwrap_or(table);
         self.volatile_views.contains(table) || self.volatile_views.contains(bare)
      })
   }

   // Expands view references into the base relations they read, up to a small
   // nesting depth. Used for invalidation registration only, never for keys.
   pub fn expand(&self, tables: &[String]) -> Vec<String> {
      let mut out: Vec<String> = Vec::with_capacity(tables.len());
      let mut queue: VecDeque<(String, usize)> = tables.iter().map(|t| (t.clone(), 0)).collect();
      while let Some((table, depth)) = queue.pop_front() {
         if out.contains(&table) {
            continue;
         }
         out.push(table.clone());
         if depth >= 8 {
            continue;
         }
         let bare = table.rsplit('.').next().unwrap_or(&table);
         let bases = self
            .view_tables
            .get(&table)
            .or_else(|| self.view_tables.get(bare));
         if let Some(bases) = bases {
            for base in bases {
               if !out.contains(base) {
                  queue.push_back((base.clone(), depth + 1));
               }
            }
         }
      }
      out
   }
}

fn row_text(row: &DbRow, index: usize) -> Option<&str> {
   match row.values.get(index) {
      Some(DbValue::Text(text)) => Some(text.as_str()),
      _ => None,
   }
}

// Matches `name(` while ignoring qualified prefixes like `pg_catalog.name(`.
fn definition_mentions(definition: &str, name: &str) -> bool {
   let needle = format!("{name}(");
   let mut from = 0;
   while let Some(offset) = definition[from..].find(&needle) {
      let position = from + offset;
      let qualified = position > 0 && definition.as_bytes()[position - 1] == b'.';
      if !qualified {
         return true;
      }
      from = position + needle.len();
   }
   false
}

#[derive(Default)]
struct CatalogState {
   snapshot: RwLock<Option<Arc<CatalogSnapshot>>>,
   generation: AtomicU64,
   stale: AtomicBool,
   refreshing: AtomicBool,
   last_attempt_ms: AtomicU64,
   last_refresh_ms: AtomicU64,
}

impl CatalogState {
   fn new() -> Self {
      Self {
         stale: AtomicBool::new(true),
         ..Self::default()
      }
   }
}

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
   // Idle authenticated upstream sessions currently parked in the pool.
   pub pool_idle: AtomicU64,
}

// Tunables that live with the manager so cross-protocol callers share them.
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

// Tracks table-to-cache-keys relations for granular write invalidations.
#[derive(Default)]
struct TableTracker {
   tables: HashMap<String, HashSet<String>>,
   // Cache key -> index marker keys, so invalidation can drop both. The number
   // of entries here is the number of tracked cache keys.
   markers: HashMap<String, Vec<String>>,
   // Bumped on every invalidation so a fill that raced with a write can be
   // discarded instead of resurrecting stale rows.
   epochs: HashMap<String, u64>,
}

impl TableTracker {
   fn register(&mut self, table: &str, key: String, marker: String) {
      self
         .tables
         .entry(table.to_string())
         .or_default()
         .insert(key.clone());
      let markers = self.markers.entry(key).or_default();
      if !markers.contains(&marker) {
         markers.push(marker);
      }
   }

   fn remove_markers(&mut self, key: &str) -> Vec<String> {
      let markers = self.markers.remove(key).unwrap_or_default();
      for marker in &markers {
         if let Some(table) = table_from_marker_any(marker)
            && let Some(set) = self.tables.get_mut(table)
         {
            set.remove(key);
            if set.is_empty() {
               self.tables.remove(table);
            }
         }
      }
      markers
   }

   fn epoch(&self, table: &str) -> u64 {
      self.epochs.get(table).copied().unwrap_or_else(|| {
         let bare = normalize_table_name(table);
         self.epochs.get(&bare).copied().unwrap_or(0)
      })
   }

   fn bump(&mut self, tables: &[String]) {
      for table in tables {
         let epoch = self.epochs.entry(table.clone()).or_insert(0);
         *epoch = epoch.wrapping_add(1);
         let bare = normalize_table_name(table);
         if bare != *table {
            let bare_epoch = self.epochs.entry(bare).or_insert(0);
            *bare_epoch = bare_epoch.wrapping_add(1);
         }
      }
   }

   fn remove_table(&mut self, table: &str) -> Vec<String> {
      self
         .tables
         .remove(table)
         .map(|keys| keys.into_iter().collect())
         .unwrap_or_default()
   }

   fn matching_tables(&self, name: &str) -> Vec<String> {
      let bare = normalize_table_name(name);
      self
         .tables
         .keys()
         .filter(|tracked| normalize_table_name(tracked) == bare)
         .cloned()
         .collect()
   }

   fn clear(&mut self) {
      self.tables.clear();
      self.markers.clear();
   }

   fn key_count(&self) -> usize {
      self.markers.len()
   }

   fn tracked_table_names(&self) -> Vec<String> {
      let mut set = HashSet::new();
      for table in self.tables.keys() {
         let norm = normalize_table_name(table);
         if !norm.is_empty() {
            set.insert(norm);
         }
      }
      let mut list: Vec<String> = set.into_iter().collect();
      list.sort();
      list
   }

   // Removes a key from the marker map and from every table set it belongs to.
   // Returns its index markers so the caller can delete them.
   fn detach_key(&mut self, prefix: &str, key: &str) -> Vec<String> {
      let markers = self.markers.remove(key).unwrap_or_default();
      for marker in &markers {
         if let Some(table) = table_from_marker(prefix, marker)
            && let Some(set) = self.tables.get_mut(table)
         {
            set.remove(key);
            if set.is_empty() {
               self.tables.remove(table);
            }
         }
      }
      markers
   }
}

#[derive(Clone, Copy, Default, PartialEq, Eq)]
pub struct TableCounters {
   pub inserts: u64,
   pub updates: u64,
   pub deletes: u64,
}

#[derive(Default)]
struct ExternalSync {
   last_poll_ms: AtomicU64,
   poll_started_ms: AtomicU64,
   in_progress: AtomicBool,
   snapshot: Mutex<HashMap<String, TableCounters>>,
}

#[derive(Default)]
struct FillWait {
   done: Mutex<bool>,
   cond: Condvar,
}

// Marks a cache fill as in progress so concurrent misses for the same key wait
// for one upstream round trip instead of stampeding it.
pub struct FillGuard {
   key: String,
   slot: Arc<FillWait>,
   inflight: Arc<Mutex<HashMap<String, Arc<FillWait>>>>,
}

impl Drop for FillGuard {
   fn drop(&mut self) {
      {
         let mut inflight = self.inflight.lock();
         if inflight
            .get(&self.key)
            .is_some_and(|slot| Arc::ptr_eq(slot, &self.slot))
         {
            inflight.remove(&self.key);
         }
      }
      *self.slot.done.lock() = true;
      self.slot.cond.notify_all();
   }
}

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
   inflight: Arc<Mutex<HashMap<String, Arc<FillWait>>>>,
}

impl DbCacheManager {
   pub fn new(storage: Arc<dyn DbCacheStorage>) -> Self {
      Self::with_limits(storage, CacheLimits::default())
   }

   pub fn with_limits(storage: Arc<dyn DbCacheStorage>, limits: CacheLimits) -> Self {
      let mut encoded = Vec::with_capacity(ENCODED_SHARDS);
      for _ in 0..ENCODED_SHARDS {
         encoded.push(RwLock::new(HashMap::new()));
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
         inflight: Arc::new(Mutex::new(HashMap::new())),
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
      let fingerprint = compute_fingerprint_with_params(&parsed.normalized, params, param_formats);
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

   // True when a session should refresh the catalog. `retry` distinguishes the
   // initial load from a periodic retry after a failure.
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

   // Claims the refresh so only one session queries the catalogs at a time.
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

   // Catalog-aware cache decision. Falls back to the AST blocklist when the
   // catalog has never loaded (for example without an upstream connection).
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

   // Table names used for invalidation registration. Views expand to their
   // base relations; cache keys keep using the original first table.
   pub fn expanded_tables(&self, parsed: &ParsedQuery) -> Vec<String> {
      let snapshot = self.catalog.snapshot.read();
      match snapshot.as_ref() {
         Some(snapshot) => snapshot.expand(&parsed.tables),
         None => parsed.tables.clone(),
      }
   }

   // -------------------------------------------------------------- cache

   fn encoded_shard(&self, key: &str) -> &RwLock<HashMap<String, Arc<EncodedResult>>> {
      let mut hasher = FixedState::with_seed(0).build_hasher();
      hasher.write(key.as_bytes());
      &self.encoded[(hasher.finish() as usize) & (ENCODED_SHARDS - 1)]
   }

   pub fn cache_key(&self, prefix: &str, table: &str, fingerprint: u64) -> String {
      make_cache_key(prefix, table, fingerprint)
   }

   // Fetches a pre-encoded response. Verifies the backing key still exists so
   // keys deleted through the RESP port can never be served stale.
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
      if (entry.expires_at_ms != 0 && entry.expires_at_ms < now) || !self.storage.exists(&key) {
         shard.write().remove(&key);
         return None;
      }
      self.metrics.hits.fetch_add(1, Ordering::Relaxed);
      Some(entry)
   }

   // Fetches and parses a cached query result from the shared store.
   pub fn get_cached_query(
      &self,
      prefix: &str,
      table: &str,
      fingerprint: u64,
   ) -> Option<DbResultSet> {
      let key = make_cache_key(prefix, table, fingerprint);
      let raw = self.storage.get(&key)?;
      DbResultSet::from_json(&raw)
   }

   // Serializes and stores a query result, registers table invalidation
   // aliases, and keeps a wire-ready copy for repeated hits.
   pub fn store_cached_query(
      &self,
      prefix: &str,
      tables: &[String],
      fingerprint: u64,
      result: &DbResultSet,
      ttl_ms: u64,
   ) {
      self.store_cached_query_versioned(prefix, tables, fingerprint, result, ttl_ms, &[]);
   }

   // Captures the current invalidation epoch of each table. Pass the result to
   // `store_cached_query_versioned` so a fill racing with a write is dropped.
   pub fn table_versions(&self, tables: &[String]) -> Vec<u64> {
      let tracker = self.tracker.read();
      tables.iter().map(|table| tracker.epoch(table)).collect()
   }

   // Stores a result only when none of the involved tables were invalidated
   // since `versions` was captured. The epoch check and the write happen under
   // the tracker lock, so an invalidation either drops the fill or removes the
   // freshly written entry.
   pub fn store_cached_query_versioned(
      &self,
      prefix: &str,
      tables: &[String],
      fingerprint: u64,
      result: &DbResultSet,
      ttl_ms: u64,
      versions: &[u64],
   ) -> bool {
      self.store_cached_query_jittered(prefix, tables, fingerprint, result, ttl_ms, 0, versions)
   }

   // Same as `store_cached_query_versioned`, with per-fill TTL jitter so
   // entries of the same age do not all expire at once.
   #[allow(clippy::too_many_arguments)]
   pub fn store_cached_query_jittered(
      &self,
      prefix: &str,
      tables: &[String],
      fingerprint: u64,
      result: &DbResultSet,
      ttl_ms: u64,
      jitter_pct: u64,
      versions: &[u64],
   ) -> bool {
      let primary_table = tables.first().map(|s| s.as_str()).unwrap_or("default");
      let key = make_cache_key(prefix, primary_table, fingerprint);
      let ttl_ms = jittered_ttl(ttl_ms, jitter_pct, fingerprint);
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

   // Rebuilds the wire cache for a key filled by another process/protocol.
   pub fn remember_encoded(
      &self,
      prefix: &str,
      table: &str,
      fingerprint: u64,
      result: &DbResultSet,
      ttl_ms: u64,
   ) {
      let key = make_cache_key(prefix, table, fingerprint);
      let encoded = Arc::new(EncodedResult::from_result_set(result, ttl_ms));
      self.encoded_shard(&key).write().insert(key, encoded);
   }

   // Evicts all cached queries referencing the specified tables.
   pub fn invalidate_tables(&self, tables: &[String]) -> usize {
      let mut to_delete = Vec::new();
      let mut markers = Vec::new();
      {
         let mut tracker = self.tracker.write();
         tracker.bump(tables);
         let mut matched: HashSet<String> = HashSet::new();
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

   // Drops every cache entry (and table index) under the configured prefix.
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

   // Rebuilds table invalidation state from persisted index markers.
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

   // Evicts arbitrary entries once the tracked key count exceeds the limit.
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
         let keys: Vec<String> = tracker.markers.keys().take(excess).cloned().collect();
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

   // Drops tracker entries whose encoded response already expired. Storage TTL
   // handles the backing key; this keeps invalidation metadata bounded.
   pub fn prune_expired(&self, prefix: &str) -> usize {
      if !self.prune_due() {
         return 0;
      }
      self.last_prune_ms.store(now_ms(), Ordering::Relaxed);
      let now = now_ms();
      let keys: Vec<String> = self.tracker.read().markers.keys().cloned().collect();
      let mut expired = Vec::new();
      for key in keys {
         let is_expired = {
            let shard = self.encoded_shard(&key);
            let guard = shard.read();
            guard
               .get(&key)
               .is_some_and(|entry| entry.expires_at_ms != 0 && entry.expires_at_ms < now)
         };
         if is_expired {
            expired.push(key);
         }
      }
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

   // Claims the next poll of the upstream write counters. Returns false while
   // another session is polling or the interval has not elapsed.
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
         // Recover from a session that died mid-poll.
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

   // Compares the latest `pg_stat_all_tables` counters with the previous poll
   // and invalidates tables that changed. The first poll only records a
   // baseline. Returns the tables that changed so peers can be told as well.
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

   // Elects this caller as the single loader for a cache key. Returns None when
   // another caller is already loading it.
   pub fn begin_fill(&self, key: &str) -> Option<FillGuard> {
      let mut inflight = self.inflight.lock();
      if inflight.contains_key(key) {
         return None;
      }
      let slot = Arc::new(FillWait::default());
      inflight.insert(key.to_string(), Arc::clone(&slot));
      Some(FillGuard {
         key: key.to_string(),
         slot,
         inflight: Arc::clone(&self.inflight),
      })
   }

   // Waits briefly for the current loader of `key`. Returns true when the
   // loader finished within the window.
   pub fn wait_for_fill(&self, key: &str) -> bool {
      let slot = self.inflight.lock().get(key).cloned();
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

fn index_prefix(prefix: &str) -> String {
   format!("{prefix}idx:")
}

fn index_key(prefix: &str, table: &str, fingerprint: u64) -> String {
   format!("{prefix}idx:{table}:{fingerprint:016x}")
}

// Extracts the table name from an index marker key.
fn table_from_marker<'a>(prefix: &str, marker: &'a str) -> Option<&'a str> {
   marker
      .strip_prefix(&index_prefix(prefix))?
      .rsplit_once(':')
      .map(|(table, _)| table)
}

fn table_from_marker_any<'a>(marker: &'a str) -> Option<&'a str> {
   let idx_part = marker.split("idx:").nth(1)?;
   idx_part.rsplit_once(':').map(|(table, _)| table)
}

fn normalize_table_name(name: &str) -> String {
   name
      .rsplit('.')
      .next()
      .unwrap_or(name)
      .trim_matches('"')
      .to_ascii_lowercase()
}

fn expiry_ms(ttl_ms: u64) -> u64 {
   if ttl_ms == 0 { 0 } else { now_ms() + ttl_ms }
}

// Jitter only ever extends the TTL so the configured value stays a lower bound.
fn jittered_ttl(ttl_ms: u64, jitter_pct: u64, seed: u64) -> u64 {
   if ttl_ms == 0 || jitter_pct == 0 {
      return ttl_ms;
   }
   let span = (ttl_ms / 100)
      .saturating_mul(jitter_pct.clamp(0, 50))
      .max(1);
   ttl_ms + (mix64(seed ^ now_ms()) % (span + 1))
}

pub(crate) fn mix64(mut value: u64) -> u64 {
   value ^= value >> 30;
   value = value.wrapping_mul(0xbf58_476d_1ce4_e5b9);
   value ^= value >> 27;
   value = value.wrapping_mul(0x94d0_49bb_1331_11eb);
   value ^ (value >> 31)
}

pub(crate) fn now_ms() -> u64 {
   SystemTime::now()
      .duration_since(UNIX_EPOCH)
      .map(|d| d.as_millis() as u64)
      .unwrap_or(0)
}
