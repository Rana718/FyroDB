//! Write hooks feeding incremental index maintenance.
//!
//! The keyspace notifies [`SearchHooks`] after every mutation while at
//! least one index exists. Each index keeps one [`DirtySet`] per consumer
//! (every vector field's graph, and the attribute postings); a write to a
//! key under the index's prefixes lands in all of them. Consumers drain
//! their set at query time and re-read just those keys, so an index stays
//! current in O(changed keys) instead of rescanning the keyspace whenever
//! anything, anywhere, was written.

use super::{FxHashMap, IndexDef};
use parking_lot::{Mutex, RwLock};
use std::collections::HashSet;
use std::sync::Arc;
use std::hash::BuildHasher;
use std::sync::atomic::{AtomicBool, Ordering};

type FxHashSet<K> = HashSet<K, foldhash::fast::RandomState>;

/// Dirty keys beyond this make a full rebuild cheaper than per-key deltas.
pub const DIRTY_OVERFLOW: usize = 250_000;
const SHARDS: usize = 16;

/// Keys written since the consumer last drained. Sharded so writers on
/// different workers rarely meet on one lock.
pub struct DirtySet {
    shards: Vec<Mutex<FxHashSet<Box<str>>>>,
    /// Too many keys (or a FLUSHALL): the consumer must rebuild.
    overflow: AtomicBool,
}

pub enum Drained {
    Keys(Vec<Box<str>>),
    Rebuild,
}

impl Default for DirtySet {
    fn default() -> Self {
        Self {
            shards: (0..SHARDS).map(|_| Mutex::new(FxHashSet::default())).collect(),
            overflow: AtomicBool::new(false),
        }
    }
}

impl DirtySet {
    fn shard(&self, key: &str) -> &Mutex<FxHashSet<Box<str>>> {
        let h = foldhash::fast::FixedState::default().hash_one(key);
        &self.shards[(h as usize) % SHARDS]
    }

    pub fn mark(&self, key: &str) {
        if self.overflow.load(Ordering::Relaxed) {
            return;
        }
        let mut s = self.shard(key).lock();
        if s.len() * SHARDS > DIRTY_OVERFLOW {
            drop(s);
            self.mark_all();
            return;
        }
        if !s.contains(key) {
            s.insert(key.into());
        }
    }

    pub fn mark_all(&self) {
        self.overflow.store(true, Ordering::Release);
        for s in &self.shards {
            s.lock().clear();
        }
    }

    pub fn is_clean(&self) -> bool {
        !self.overflow.load(Ordering::Acquire) && self.shards.iter().all(|s| s.lock().is_empty())
    }

    /// Take everything recorded so far.
    pub fn drain(&self) -> Drained {
        if self.overflow.swap(false, Ordering::AcqRel) {
            for s in &self.shards {
                s.lock().clear();
            }
            return Drained::Rebuild;
        }
        let mut out = Vec::new();
        for s in &self.shards {
            out.extend(s.lock().drain());
        }
        Drained::Keys(out)
    }

    /// Forget everything (a full build is about to snapshot the keyspace).
    pub fn reset(&self) {
        self.overflow.store(false, Ordering::Release);
        for s in &self.shards {
            s.lock().clear();
        }
    }
}

/// Dirty sets for one index: one per vector field plus the postings.
pub struct IndexDirty {
    pub def: Arc<IndexDef>,
    pub vectors: FxHashMap<String, DirtySet>,
    pub filter: DirtySet,
}

impl IndexDirty {
    fn mark(&self, key: &str) {
        for d in self.vectors.values() {
            d.mark(key);
        }
        self.filter.mark(key);
    }

    fn mark_all(&self) {
        for d in self.vectors.values() {
            d.mark_all();
        }
        self.filter.mark_all();
    }
}

#[derive(Default)]
pub struct SearchHooks {
    indexes: RwLock<Vec<Arc<IndexDirty>>>,
    active: AtomicBool,
    /// The keyspace's observing flag: on exactly while an index exists.
    observing: std::sync::OnceLock<Arc<AtomicBool>>,
}

impl SearchHooks {
    /// Track `def` (replacing any earlier registration of the same name).
    pub fn register(&self, def: &Arc<IndexDef>) -> Arc<IndexDirty> {
        let dirty = Arc::new(IndexDirty {
            def: Arc::clone(def),
            vectors: def
                .fields
                .iter()
                .filter(|f| f.vector.is_some())
                .map(|f| (f.name.clone(), DirtySet::default()))
                .collect(),
            filter: DirtySet::default(),
        });
        let mut list = self.indexes.write();
        list.retain(|d| d.def.name != def.name);
        list.push(Arc::clone(&dirty));
        self.set_active(true);
        dirty
    }

    /// Attach the keyspace's observing flag (store construction).
    pub fn attach(&self, flag: Arc<AtomicBool>) {
        let _ = self.observing.set(flag);
        self.set_active(!self.indexes.read().is_empty());
    }

    fn set_active(&self, on: bool) {
        self.active.store(on, Ordering::Release);
        if let Some(f) = self.observing.get() {
            f.store(on, Ordering::Release);
        }
    }

    pub fn unregister(&self, name: &str) {
        let mut list = self.indexes.write();
        list.retain(|d| d.def.name != name);
        let any = !list.is_empty();
        drop(list);
        self.set_active(any);
    }

    pub fn get(&self, name: &str) -> Option<Arc<IndexDirty>> {
        self.indexes.read().iter().find(|d| d.def.name == name).cloned()
    }

    pub fn is_active(&self) -> bool {
        self.active.load(Ordering::Acquire)
    }
}

/// Adapter installed as the keyspace's write observer.
pub struct HookObserver(pub Arc<SearchHooks>);

impl customhash::WriteObserver for HookObserver {
    fn on_write(&self, key: &str) {
        for d in self.0.indexes.read().iter() {
            if d.def.matches_key(key) {
                d.mark(key);
            }
        }
    }

    fn on_clear(&self) {
        for d in self.0.indexes.read().iter() {
            d.mark_all();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dirty_set_drains_marked_keys_once() {
        let d = DirtySet::default();
        d.mark("a");
        d.mark("b");
        d.mark("a");
        let Drained::Keys(mut keys) = d.drain() else { panic!("unexpected rebuild") };
        keys.sort();
        assert_eq!(keys, vec!["a".into(), "b".into()] as Vec<Box<str>>);
        assert!(d.is_clean());
        d.mark_all();
        assert!(matches!(d.drain(), Drained::Rebuild));
        assert!(d.is_clean());
    }
}
