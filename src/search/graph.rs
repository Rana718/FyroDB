use super::hooks::{DirtySet, Drained};
use super::FxHashMap;
use super::IndexDef;
use super::SearchState;
use super::VectorAttrs;
use crate::storage::store::Store;
use fyro_vector::hnsw::{GraphDump, HnswGraph};
use parking_lot::{Mutex, RwLock};
use std::sync::Arc;

type GraphSnapshot = Vec<(String, String, Vec<(usize, Arc<str>)>, Option<GraphDump>)>;

// publish a partial graph every N linked nodes so queries don't wait for full build
const PARTIAL_PUBLISH_STEP: usize = 20_000;

#[derive(Default)]
struct KeyMap {
    by_key: FxHashMap<Arc<str>, u32>,
    by_id: Vec<Option<Arc<str>>>,
    len: usize,
}

impl KeyMap {
    fn insert(&mut self, key: Arc<str>, id: u32) {
        let i = id as usize;
        if self.by_id.len() <= i {
            self.by_id.resize(i + 1, None);
        }
        if self.by_id[i].replace(Arc::clone(&key)).is_none() {
            self.len += 1;
        }
        self.by_key.insert(key, id);
    }

    fn remove(&mut self, key: &str) -> Option<u32> {
        let id = self.by_key.remove(key)?;
        if let Some(slot) = self.by_id.get_mut(id as usize)
            && slot.take().is_some()
        {
            self.len -= 1;
        }
        Some(id)
    }
}

static EPOCHS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);

fn next_epoch() -> u64 {
    EPOCHS.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
}

pub struct GraphEntry {
    pub graph: Arc<HnswGraph>,
    keys: RwLock<KeyMap>,
    apply: Mutex<()>,
    /// Changes whenever the key map does: caches derived from it (the
    /// filter index's doc→vector-id table) compare against this.
    epoch: std::sync::atomic::AtomicU64,
    /// HNSW edges are built (search via traversal); false for FLAT, whose
    /// contiguous vectors are queried by exact scan.
    linked: bool,
}

impl GraphEntry {
    fn new(graph: Arc<HnswGraph>, linked: bool) -> Self {
        Self { graph, keys: RwLock::new(KeyMap::default()), apply: Mutex::new(()), epoch: next_epoch().into(), linked }
    }

    pub fn epoch(&self) -> u64 {
        self.epoch.load(std::sync::atomic::Ordering::Acquire)
    }

    fn bump(&self) {
        self.epoch.store(next_epoch(), std::sync::atomic::Ordering::Release);
    }

    /// Vector ids for a sequence of keys under one lock (`u32::MAX` = the
    /// key has no vector in this index).
    pub fn ids_for_keys<'a>(&self, keys: impl Iterator<Item = Option<&'a str>>) -> Vec<u32> {
        let km = self.keys.read();
        keys.map(|k| k.and_then(|k| km.by_key.get(k).copied()).unwrap_or(u32::MAX)).collect()
    }

    pub fn key_for_id(&self, id: usize) -> Option<Arc<str>> {
        self.keys.read().by_id.get(id).and_then(|k| k.clone())
    }

    pub fn id_for_key(&self, key: &str) -> Option<usize> {
        self.keys.read().by_key.get(key).map(|&i| i as usize)
    }

    /// Indexed documents.
    pub fn len(&self) -> usize {
        self.keys.read().len
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// True when this entry's graph has navigable edges (HNSW).
    pub fn is_linked(&self) -> bool {
        self.linked
    }

    /// (id, key) of every indexed document, for persistence.
    pub fn keys_by_id(&self) -> Vec<(usize, Arc<str>)> {
        let km = self.keys.read();
        km.by_id.iter().enumerate().filter_map(|(i, k)| k.clone().map(|k| (i, k))).collect()
    }
}

/// The indexable vector of `key`, if it currently has one.
pub fn doc_vector(store: &Store, def: &IndexDef, field: &str, attrs: &VectorAttrs, key: &str) -> Option<Vec<f32>> {
    if !def.matches_key(key) {
        return None;
    }
    store.data.read_consistent(key, |val| {
        if val.is_expired() {
            return None;
        }
        let h = crate::storage::value::FyroDB::as_hash(&val.value)?;
        attrs.decode(h.get(field)?.as_bytes())
    })?
}

fn new_graph(attrs: &VectorAttrs) -> Arc<HnswGraph> {
    let quant = if attrs.algorithm == "HNSW" { attrs.quant } else { fyro_vector::Quant::None };
    Arc::new(HnswGraph::with_quant(attrs.metric, attrs.dims, attrs.m, attrs.ef_construction, quant))
}

/// RaBitQ codebook training sample size (every n-th vector of the scan).
const TRAIN_SAMPLE: usize = 16_384;

/// Full build: one keyspace scan preparing every vector (memcpy + norm),
/// then parallel linking on the pool, publishing the entry for partial
/// search every `PARTIAL_PUBLISH_STEP` linked nodes.
fn build_graph_entry(
    ss: &SearchState,
    store: &Store,
    def: &IndexDef,
    field: &str,
    attrs: &VectorAttrs,
    publish: &dyn Fn(&Arc<GraphEntry>),
) -> Arc<GraphEntry> {
    let linked = attrs.algorithm == "HNSW";
    let graph = new_graph(attrs);
    let entry = Arc::new(GraphEntry::new(Arc::clone(&graph), linked));
    // Reset BEFORE scanning: a write landing mid-scan is marked again and
    // applied by the first query after publish, so nothing is lost.
    if let Some(d) = ss.vector_dirty(&def.name, field) {
        d.vectors[field].reset();
    }
    let indexable = |k: &str, val: &crate::storage::value::StoreValue| -> Option<Vec<f32>> {
        if val.is_expired() || !def.matches_key(k) {
            return None;
        }
        let h = crate::storage::value::FyroDB::as_hash(&val.value)?;
        attrs.decode(h.get(field)?.as_bytes())
    };
    // Count first so the arena is sized exactly (filling it one id at a
    // time leaves up to half its capacity as growth slack), then prepare
    // inside the second scan without buffering every vector.
    let mut expected = 0usize;
    store.data.for_each(|k, val| {
        if !val.is_expired()
            && def.matches_key(k)
            && crate::storage::value::FyroDB::as_hash(&val.value)
                .and_then(|h| h.get(field))
                .is_some_and(|b| b.as_bytes().len() == attrs.blob_len())
        {
            expected += 1;
        }
    });
    graph.reserve(expected);
    if graph.quant() == fyro_vector::Quant::RaBitQ {
        let stride = expected.div_ceil(TRAIN_SAMPLE).max(1);
        let mut sample = Vec::with_capacity(expected.min(TRAIN_SAMPLE));
        let mut seen = 0usize;
        store.data.for_each(|k, val| {
            if let Some(v) = indexable(k, val) {
                if seen.is_multiple_of(stride) {
                    sample.push(v);
                }
                seen += 1;
            }
        });
        graph.train(&sample);
    }
    {
        let mut km = entry.keys.write();
        let mut vecs: Vec<(usize, Vec<f32>)> = Vec::with_capacity(expected);
        let mut next = 0usize;
        store.data.for_each(|k, val| {
            if let Some(v) = indexable(k, val) {
                km.insert(k.into(), next as u32);
                vecs.push((next, v));
                next += 1;
            }
        });
        let items: Vec<(usize, &[f32])> = vecs.iter().map(|(id, v)| (*id, v.as_slice())).collect();
        graph.prepare_batch(&items);
    }
    if !linked {
        return entry;
    }
    let ids: Vec<usize> = (0..graph.node_count()).collect();
    let mut last = 0usize;
    graph.build_links(&ids, fyro_vector::pool::global().threads(), &mut |done| {
        if done < ids.len() && done - last >= PARTIAL_PUBLISH_STEP {
            last = done;
            publish(&entry);
        }
    });
    if ids.len() < REORDER_MIN {
        return entry;
    }
    let (reordered, map) = graph.reordered();
    let out = Arc::new(GraphEntry::new(Arc::new(reordered), linked));
    {
        let old = entry.keys.read();
        let mut km = out.keys.write();
        for (id, key) in old.by_id.iter().enumerate() {
            if let Some(k) = key
                && let Some(&new) = map.get(id).filter(|&&n| n != u32::MAX)
            {
                km.insert(Arc::clone(k), new);
            }
        }
    }
    out
}

const REORDER_MIN: usize = 10_000;

impl SearchState {
    pub(super) fn vector_dirty(&self, index: &str, field: &str) -> Option<Arc<super::hooks::IndexDirty>> {
        self.hooks.get(index).filter(|d| d.vectors.contains_key(field))
    }

    /// Current vector index for (index, field), applying pending writes.
    /// None → no usable index yet (first build running or starting): the
    /// caller answers with the exact keyspace scan.
    pub fn vector_index_entry(
        &self,
        store: &Arc<Store>,
        def: &Arc<IndexDef>,
        field: &str,
    ) -> Option<Arc<GraphEntry>> {
        let attrs = def.field(field).and_then(|f| f.vector.as_ref())?.clone();
        let key = (def.name.clone(), field.to_string());
        let existing = self.graphs.read().get(&key).cloned();
        if self.building.lock().contains_key(&key) {
            return existing;
        }
        let Some(entry) = existing else {
            self.spawn_build(store, def, field, &attrs);
            return None;
        };
        let dirty = self.vector_dirty(&def.name, field)?;
        let dirty_set: &DirtySet = &dirty.vectors[field];
        // Fast path: no pending writes — skip the apply lock entirely.
        if dirty_set.is_clean() {
            return Some(entry);
        }
        let _apply = entry.apply.lock();
        // Re-check under lock (another query may have drained it).
        if dirty_set.is_clean() {
            drop(_apply);
            return Some(entry);
        }
        match dirty_set.drain() {
            Drained::Rebuild => {
                drop(_apply);
                self.spawn_build(store, def, field, &attrs);
                None
            }
            Drained::Keys(keys) => {
                apply_keys(store, def, field, &attrs, &entry, keys);
                drop(_apply);
                Some(entry)
            }
        }
    }

    /// Kick one background full build per (index, field) if none is
    /// running. Low priority on the vector pool: queries go first.
    pub fn spawn_build(&self, store: &Arc<Store>, def: &Arc<IndexDef>, field: &str, attrs: &VectorAttrs) {
        let key = (def.name.clone(), field.to_string());
        {
            let mut building = self.building.lock();
            if building.contains_key(&key) {
                return;
            }
            building.insert(key.clone(), ());
        }
        // A stale graph must not answer while its rebuild runs; queries use
        // the exact scan until the builder's first partial publish.
        self.graphs.write().remove(&key);
        let store = Arc::clone(store);
        let def = Arc::clone(def);
        let field_o = field.to_string();
        let attrs = attrs.clone();
        fyro_vector::pool::global().spawn_low(move || {
            let ss = store.search_state();
            let current = |ss: &SearchState| ss.def_arc(&key.0).is_some_and(|d| Arc::ptr_eq(&d, &def));
            let publish = |e: &Arc<GraphEntry>| {
                if current(ss) {
                    ss.graphs.write().insert(key.clone(), Arc::clone(e));
                }
            };
            let entry = build_graph_entry(ss, &store, &def, &field_o, &attrs, &publish);
            // Publish only if the index still exists with the same
            // definition: a DROPINDEX / re-CREATE mid-build must not
            // resurrect a stale graph.
            publish(&entry);
            ss.building.lock().remove(&key);
        });
    }

    pub fn prefetch_build(&self, store: &Arc<Store>, def: &Arc<IndexDef>) {
        self.start_background_apply(store);
        for f in &def.fields {
            if let Some(v) = &f.vector {
                self.spawn_build(store, def, &f.name, v);
            }
        }
    }

    /// After a snapshot load: build every vector field that was not
    /// restored from its persisted graph.
    pub fn rebuild_missing(&self, store: &Arc<Store>) {
        self.start_background_apply(store);
        for name in self.list_names() {
            let Some(def) = self.def_arc(&name) else { continue };
            for f in &def.fields {
                let Some(v) = &f.vector else { continue };
                let key = (def.name.clone(), f.name.clone());
                if !self.graphs.read().contains_key(&key) {
                    self.spawn_build(store, &def, &f.name, v);
                }
            }
        }
    }

    pub fn restore_graph(
        &self,
        store: &Store,
        index: &str,
        field: &str,
        nodes: Vec<(String, u32, u8, Vec<Vec<u32>>)>,
        entry_id: Option<u32>,
        centroids: Vec<Vec<f32>>,
    ) -> bool {
        let Some(def) = self.def_arc(index) else { return false };
        let Some(attrs) = def.field(field).and_then(|f| f.vector.clone()) else { return false };
        let graph = new_graph(&attrs);
        let linked = attrs.algorithm == "HNSW";
        let e = Arc::new(GraphEntry::new(Arc::clone(&graph), linked));
        let max_id = nodes.iter().map(|n| n.1 as usize + 1).max().unwrap_or(0);
        graph.reserve(max_id);
        graph.set_centroids(centroids);
        {
            let mut km = e.keys.write();
            for (key, id, level, links) in &nodes {
                let Some(v) = doc_vector(store, &def, field, &attrs, key) else { continue };
                if linked {
                    graph.restore_node(*id as usize, &v, *level as usize, links);
                } else {
                    graph.prepare(*id as usize, &v);
                }
                km.insert(key.as_str().into(), *id);
            }
        }
        if linked {
            graph.restore_entry(entry_id.map(|i| i as usize));
        }
        if let Some(d) = self.vector_dirty(index, field) {
            d.vectors[field].reset();
        }
        self.graphs.write().insert((index.to_string(), field.to_string()), e);
        true
    }

    /// Persistable view of every settled vector index: (index, field,
    /// id→key, dump). Entries still building are skipped (they rebuild on
    /// load). The dump compacts tombstones first.
    pub fn graphs_for_snapshot(&self) -> GraphSnapshot {
        let graphs: Vec<((String, String), Arc<GraphEntry>)> =
            self.graphs.read().iter().map(|(k, v)| (k.clone(), Arc::clone(v))).collect();
        let building = self.building.lock().clone();
        graphs
            .into_iter()
            .filter(|(k, _)| !building.contains_key(k))
            .map(|((index, field), e)| {
                let dump = e.is_linked().then(|| e.graph.dump());
                (index, field, e.keys_by_id(), dump)
            })
            .collect()
    }

    /// Delete the cached graph for one index (used by drop/alter paths).
    pub(super) fn drop_graphs(&self, index: &str) {
        self.graphs.write().retain(|(name, _), _| name != index);
        self.building.lock().retain(|(name, _), _| name != index);
        self.filters.lock().remove(index);
    }

    pub fn hnsw_key_id(&self, index: &str, field: &str, key: &str) -> Option<usize> {
        self.hnsw_entry(index, field)?.id_for_key(key)
    }

    pub fn hnsw_entry(&self, index: &str, field: &str) -> Option<Arc<GraphEntry>> {
        self.graphs.read().iter()
            .find(|((k_idx, k_fld), _)| k_idx.as_str() == index && k_fld.as_str() == field)
            .map(|(_, v)| Arc::clone(v))
    }

    /// RediSearch's `indexing` / `percent_indexed`: true while any vector
    /// field of `def` has a builder in flight, with the searchable share of
    /// its documents (the minimum across vector fields).
    pub fn index_progress(&self, def: &IndexDef, num_docs: u64) -> (bool, f64) {
        let mut indexing = false;
        let mut percent: f64 = 1.0;
        for f in def.fields.iter().filter(|f| f.vector.is_some()) {
            let key = (def.name.clone(), f.name.clone());
            if !self.building.lock().contains_key(&key) {
                continue;
            }
            indexing = true;
            let entry = self.graphs.read().get(&key).cloned();
            let done = entry.map_or(0, |e| e.graph.linked_len());
            let p = if num_docs == 0 { 0.0 } else { done as f64 / num_docs as f64 };
            percent = percent.min(p.min(1.0));
        }
        (indexing, if indexing { percent } else { 1.0 })
    }

    /// True when `index` has at least one settled HNSW graph with no
    /// pending dirty keys — safe and cheap to query inline on the worker.
    pub fn hnsw_graph_is_clean(&self, index: &str) -> bool {
        let Some(def) = self.def_arc(index) else { return false };
        let graphs = self.graphs.read();
        let building = self.building.lock();
        for f in def.fields.iter().filter(|f| f.vector.as_ref().is_some_and(|v| v.algorithm == "HNSW")) {
            let key = (def.name.clone(), f.name.clone());
            if building.contains_key(&key) {
                return false;
            }
            let Some(entry) = graphs.get(&key) else { return false };
            if !entry.is_linked() {
                return false;
            }
            if let Some(dirty) = self.vector_dirty(&def.name, &f.name)
                && !dirty.vectors[&f.name].is_clean() {
                    return false;
                }
        }
        true
    }

    /// Launch a background periodic task that drains dirty keys for all
    /// vector indexes every 50ms.  Reduces p99 query latency under mixed
    /// read/write workloads by moving the apply work off the query path.
    pub fn start_background_apply(&self, store: &Arc<Store>) {
        if self.bg_apply_running.swap(true, std::sync::atomic::Ordering::AcqRel) {
            return;
        }
        let store = Arc::clone(store);
        std::thread::Builder::new()
            .name("bg-apply".into())
            .spawn(move || {
                loop {
                    std::thread::sleep(std::time::Duration::from_millis(50));
                    let ss = store.search_state();
                    ss.apply_pending_dirty(&store);
                }
            })
            .ok();
    }

    fn apply_pending_dirty(&self, store: &Arc<Store>) {
        let entries: Vec<((String, String), Arc<GraphEntry>)> =
            self.graphs.read().iter().map(|(k, v)| (k.clone(), Arc::clone(v))).collect();
        for ((index, field), entry) in entries {
            if self.building.lock().contains_key(&(index.clone(), field.clone())) {
                continue;
            }
            let Some(dirty) = self.vector_dirty(&index, &field) else { continue };
            let dirty_set = &dirty.vectors[&field];
            if dirty_set.is_clean() {
                continue;
            }
            let Some(_apply) = entry.apply.try_lock() else { continue };
            if dirty_set.is_clean() {
                continue;
            }
            let Some(def) = self.def_arc(&index) else { continue };
            let Some(attrs) = def.field(&field).and_then(|f| f.vector.clone()) else { continue };
            match dirty_set.drain() {
                Drained::Rebuild => {
                    drop(_apply);
                    self.spawn_build(store, &def, &field, &attrs);
                }
                Drained::Keys(keys) => {
                    apply_keys(store, &def, &field, &attrs, &entry, keys);
                }
            }
        }
    }

    pub fn invalidate_graphs(&self) {
        self.graphs.write().clear();
        self.filters.lock().clear();
    }
}

/// Bring `entry` up to date for the written `keys`: unchanged vectors are
/// skipped, rewritten ones tombstone their old node and get a fresh one,
/// removed ones are tombstoned. New nodes link in parallel.
fn apply_keys(store: &Store, def: &IndexDef, field: &str, attrs: &VectorAttrs, entry: &GraphEntry, keys: Vec<Box<str>>) {
    let graph = &entry.graph;
    let mut adds: Vec<(Arc<str>, Vec<f32>)> = Vec::new();
    {
        let mut km = entry.keys.write();
        for key in keys {
            let current = doc_vector(store, def, field, attrs, &key);
            let old = km.by_key.get(&*key).copied();
            match (old, current) {
                (Some(id), Some(v)) if graph.vector_matches(id as usize, &v) => {}
                (old, current) => {
                    if let Some(id) = old {
                        graph.mark_deleted(id as usize);
                        km.remove(&key);
                        entry.bump();
                    }
                    if let Some(v) = current {
                        adds.push((Arc::from(key), v));
                    }
                }
            }
        }
    }
    if entry.linked {
        // Unlink tombstones before handing their ids out again.
        graph.compact_if_needed();
    }
    if adds.is_empty() {
        return;
    }
    let ids = graph.alloc_ids(adds.len());
    let items: Vec<(usize, &[f32])> = adds.iter().zip(&ids).map(|((_, v), &id)| (id, v.as_slice())).collect();
    graph.prepare_batch(&items);
    if entry.linked {
        graph.build_links(&ids, fyro_vector::pool::global().threads(), &mut |_| {});
    }
    let mut km = entry.keys.write();
    for ((k, _), &id) in adds.into_iter().zip(&ids) {
        km.insert(k, id as u32);
    }
    drop(km);
    entry.bump();
}
