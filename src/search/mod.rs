//! Search-module registry: index definitions, the vector-index and
//! filter-index caches, and search cursors.
//!
//! Layout: `schema` (types + SCHEMA parser), `distance` (SIMD kernels),
//! `graph` (HNSW/FLAT vector entries + builders), `filter` (attribute
//! postings). Everything is re-exported here so external paths
//! (`crate::search::X`) stay stable.


mod filter;
pub mod hooks;
pub mod query;
pub mod graph;
mod schema;

pub use fyro_vector::distance::{QueryDist, cosine_bytes, cosine_f32, distance, distance_bytes, ip_f32, l2_bytes, l2_f32};
pub use filter::{FilterIndex, FilterRead, Resolved};
pub use graph::GraphEntry;
pub use schema::{canonical_dtype, DistanceMetric, FieldDef, FieldType, IndexDef, StorageKind, VectorAttrs, parse_field_defs};

use crate::storage::store::Store;
use parking_lot::Mutex;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

pub const SEARCH_MODULE_VERSION: i64 = 20811;

/// RediSearch's default English stopwords: never indexed, dropped from queries.
pub const STOPWORDS: &[&str] = &[
    "a", "is", "the", "an", "and", "are", "as", "at", "be", "but", "by", "for", "if", "in", "into", "it", "no", "not",
    "of", "on", "or", "such", "that", "their", "then", "there", "these", "they", "this", "to", "was", "will", "with",
];

pub fn is_stopword(t: &str) -> bool {
    STOPWORDS.contains(&t)
}

/// Normalized query term (lowercase), or None for a stopword.
pub fn query_term(t: &str) -> Option<String> {
    let t = t.to_lowercase();
    (!is_stopword(&t)).then_some(t)
}

/// Text field tokenizer shared by the postings and the scan: split on
/// non-alphanumerics, Unicode lowercase, drop stopwords.
pub fn tokens(text: &str) -> impl Iterator<Item = String> + '_ {
    text.split(|c: char| !c.is_alphanumeric())
        .filter(|t| !t.is_empty())
        .map(|t| t.to_lowercase())
        .filter(|t| !is_stopword(t))
}

/// Size the vector pool from the environment before first use:
/// `FYRODB_SEARCH_THREADS` (default: all cores) and `FYRODB_SEARCH_CORES`
/// (CPU list such as `4-11,14` to pin pool threads to; default: unpinned).
pub fn configure_pool(cores: Option<Vec<usize>>) {
    let threads = std::env::var("FYRODB_SEARCH_THREADS")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|&t| t > 0)
        .or_else(|| cores.as_ref().map(Vec::len))
        .unwrap_or_else(num_cpus::get);
    fyro_vector::pool::configure(threads, cores);
}

#[derive(Default)]
struct IndexRegistry {
    by_name: FxHashMap<String, usize>,
    by_alias: FxHashMap<String, usize>,
    defs: Vec<Option<Arc<IndexDef>>>,
}

impl IndexRegistry {
    fn resolve(&self, name: &str) -> Option<usize> {
        let idx = self
            .by_name
            .get(name)
            .copied()
            .or_else(|| self.by_alias.get(name).copied())?;
        self.defs[idx].as_ref().map(|_| idx)
    }
}

type FxHashMap<K, V> = std::collections::HashMap<K, V, foldhash::fast::RandomState>;
type CursorRows = Vec<Vec<(String, Vec<u8>)>>;

pub struct SearchState {
    registry: Mutex<IndexRegistry>,
    version: AtomicU64,
    cursors: Mutex<CursorTable>,
    count_cache: Mutex<FxHashMap<String, (usize, u64)>>,
    graphs: Mutex<FxHashMap<(String, String), Arc<GraphEntry>>>,
    /// Per-index attribute postings, keyed by index name.
    filters: Mutex<FxHashMap<String, Arc<parking_lot::RwLock<FilterIndex>>>>,
    /// Serializes filter-index rebuilds (one scan per generation change).
    filter_build: Mutex<()>,
    /// (index, field) keys with a background builder in flight.
    building: Mutex<FxHashMap<(String, String), ()>>,
    /// Serializes delta applies: two concurrent queries on a stale entry
    /// used to race on duplicate node ids (and the split-RwLock graph could
    /// deadlock). A delta is cheap to wait for relative to its effect.
    graph_build: Mutex<()>,
    pub cursor_seq: AtomicU64,
    /// Dirty-key tracking fed by the keyspace write observer.
    pub(crate) hooks: Arc<hooks::SearchHooks>,
}

#[derive(Default)]
struct CursorTable {
    rows: FxHashMap<u64, CursorRows>,
    last_access: FxHashMap<u64, std::time::Instant>,
}

impl CursorTable {
    fn put(&mut self, id: u64, rows: CursorRows) {
        self.rows.insert(id, rows);
        self.last_access.insert(id, std::time::Instant::now());
        if self.rows.len() > 128 {
            self.evict_idle();
        }
    }

    fn take(&mut self, id: u64) -> Option<CursorRows> {
        self.last_access.remove(&id);
        self.rows.remove(&id)
    }

    fn evict_idle(&mut self) {
        let cutoff = std::time::Instant::now() - std::time::Duration::from_secs(300);
        let idle: Vec<u64> = self
            .last_access
            .iter()
            .filter(|(_, t)| **t < cutoff)
            .map(|(id, _)| *id)
            .collect();

        for id in idle {
            self.rows.remove(&id);
            self.last_access.remove(&id);
        }
    }
}

impl Default for SearchState {
    fn default() -> Self {
        Self::new()
    }
}
impl SearchState {
    pub fn new() -> Self {
        Self {
            registry: Mutex::new(IndexRegistry::default()),
            version: AtomicU64::new(0),
            cursors: Mutex::new(CursorTable::default()),
            count_cache: Mutex::default(),
            graphs: Mutex::default(),
            filters: Mutex::default(),
            filter_build: Mutex::default(),
            building: Mutex::default(),
            graph_build: Mutex::default(),
            cursor_seq: AtomicU64::new(1),
            hooks: Arc::default(),
        }
    }


    pub fn version(&self) -> u64 {
        self.version.load(Ordering::Acquire)
    }

    pub fn create(&self, def: IndexDef) -> Result<(), &'static str> {
        let mut reg = self.registry.lock();
        if reg.by_name.contains_key(&def.name) {
            return Err("ERR Index with same name already exists");
        }
        let idx = reg.defs.len();
        let name = def.name.clone();
        let def = Arc::new(def);
        self.hooks.register(&def);
        reg.defs.push(Some(def));
        reg.by_name.insert(name, idx);
        self.version.fetch_add(1, Ordering::Release);
        Ok(())
    }

    pub fn cached_doc_count(&self, name: &str, map_len: usize, recompute: impl FnOnce() -> u64) -> u64 {
        if let Some(&(len, count)) = self.count_cache.lock().get(name)
            && len == map_len
        {
            return count;
        }
        let count = recompute();
        self.count_cache.lock().insert(name.to_string(), (map_len, count));
        count
    }

    pub fn alter(&self, name: &str, fields: Vec<FieldDef>) -> Result<(), &'static str> {
        let mut reg = self.registry.lock();
        let idx = reg.resolve(name).ok_or("ERR Unknown index name")?;
        let old = reg.defs[idx].as_ref().unwrap();
        let mut def = IndexDef {
            name: old.name.clone(),
            prefixes: old.prefixes.clone(),
            storage: old.storage,
            fields: old.fields.clone(),
            created_at_ms: old.created_at_ms,
            num_docs: AtomicU64::new(0),
        };
        for f in fields {
            def.fields.retain(|e| e.name != f.name);
            def.fields.push(f);
        }
        let def = Arc::new(def);
        self.hooks.register(&def);
        reg.defs[idx] = Some(def);
        // Field changes can invalidate vector dims/metrics against built graphs.
        self.drop_graphs(name);
        self.version.fetch_add(1, Ordering::Release);
        Ok(())
    }

    pub fn alias_add(&self, alias: &str, target: &str, update: bool) -> Result<(), &'static str> {
        let mut reg = self.registry.lock();
        let idx = reg.resolve(target).ok_or("ERR Unknown index name for alias")?;
        if reg.by_name.contains_key(alias) {
            return Err("ERR Alias is already an index name");
        }
        if !update && reg.by_alias.contains_key(alias) {
            return Err("ERR Alias already exists");
        }
        reg.by_alias.insert(alias.to_string(), idx);
        self.version.fetch_add(1, Ordering::Release);
        Ok(())
    }

    pub fn alias_del(&self, alias: &str) -> Result<(), &'static str> {
        let mut reg = self.registry.lock();
        if reg.by_alias.remove(alias).is_none() {
            return Err("ERR Unknown alias");
        }
        self.version.fetch_add(1, Ordering::Release);
        Ok(())
    }

    pub fn drop_index(&self, name: &str) -> Result<(), &'static str> {
        let mut reg = self.registry.lock();
        let idx = reg.resolve(name).ok_or("ERR Unknown index name")?;
        let def = reg.defs[idx].take().unwrap();
        reg.by_name.remove(&def.name);
        reg.by_alias.retain(|_, v| *v != idx);
        drop(reg);
        self.hooks.unregister(&def.name);
        self.count_cache.lock().remove(name);
        self.drop_graphs(name);
        self.version.fetch_add(1, Ordering::Release);
        Ok(())
    }

    pub fn with_def<R>(&self, name: &str, f: impl FnOnce(&IndexDef) -> R) -> Option<R> {
        let reg = self.registry.lock();
        reg.resolve(name)
            .and_then(|i| reg.defs[i].as_ref().map(|d| f(d)))
    }

    pub fn exists(&self, name: &str) -> bool {
        let reg = self.registry.lock();
        reg.resolve(name).is_some()
    }

    pub fn list_names(&self) -> Vec<String> {
        let reg = self.registry.lock();
        reg.defs.iter().flatten().map(|d| d.name.clone()).collect()
    }

    pub fn alias_list(&self) -> Vec<String> {
        let reg = self.registry.lock();
        reg.by_alias.keys().cloned().collect()
    }

    pub fn cursor_create(&self) -> u64 {
        self.cursor_seq.fetch_add(1, Ordering::Relaxed)
    }

    pub fn cursor_put(&self, id: u64, rows: CursorRows) {
        if id == 0 {
            return;
        }
        self.cursors.lock().put(id, rows);
    }

    pub fn def_arc(&self, name: &str) -> Option<Arc<IndexDef>> {
        let reg = self.registry.lock();
        reg.resolve(name).and_then(|i| reg.defs[i].clone())
    }

    pub fn cursor_read(
        &self,
        id: u64,
        count: usize,
    ) -> Option<(CursorRows, u64)> {
        let mut cursors = self.cursors.lock();
        let rows = cursors.take(id)?;
        let page: Vec<_> = rows.iter().take(count).cloned().collect();
        let rest = &rows[count.min(rows.len())..];
        if rest.is_empty() {
            Some((page, 0))
        } else {
            cursors.put(id, rest.to_vec());
            Some((page, id))
        }
    }

    pub fn cursor_del(&self, id: u64) -> bool {
        self.cursors.lock().take(id).is_some()
    }
}

impl Store {
    pub fn search_state(&self) -> &SearchState {
        &self.search
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_flat_vector_schema() {
        let schema = vec![
            "title",
            "TEXT",
            "embedding",
            "VECTOR",
            "FLAT",
            "6",
            "TYPE",
            "FLOAT32",
            "DIM",
            "4",
            "DISTANCE_METRIC",
            "COSINE",
        ];
        let fields = parse_field_defs(&schema).unwrap();
        assert_eq!(fields.len(), 2);
        assert_eq!(fields[1].field_type, FieldType::Vector);
        let v = fields[1].vector.as_ref().unwrap();
        assert_eq!(v.dims, 4);
        assert_eq!(v.metric, DistanceMetric::Cosine);
    }
}
