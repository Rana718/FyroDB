use crate::row::{DbRow, DbValue};
use foldhash::fast::FixedState;
use parking_lot::RwLock;
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64};

// Catalog metadata that decides whether a query result is shareable.
pub struct CatalogSnapshot {
   volatile: HashSet<String, FixedState>,
   view_tables: HashMap<String, Vec<String>, FixedState>,
   volatile_views: HashSet<String, FixedState>,
}

impl Default for CatalogSnapshot {
   fn default() -> Self {
      Self {
         volatile: HashSet::with_hasher(FixedState::with_seed(0)),
         view_tables: HashMap::with_hasher(FixedState::with_seed(0)),
         volatile_views: HashSet::with_hasher(FixedState::with_seed(0)),
      }
   }
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

      let mut definitions: HashMap<String, String, FixedState> =
         HashMap::with_hasher(FixedState::with_seed(0));
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

      let needles: Vec<String> = snapshot.volatile.iter().map(|n| format!("{n}(")).collect();
      for (view, definition) in &definitions {
         if needles
            .iter()
            .any(|needle| definition_mentions(definition, needle))
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

   pub fn has_volatile_views(&self) -> bool {
      !self.volatile_views.is_empty()
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
      let mut seen: HashSet<String, FixedState> =
         HashSet::with_capacity_and_hasher(tables.len() * 4, FixedState::with_seed(0));
      let mut queue: VecDeque<(String, usize)> = tables.iter().map(|t| (t.clone(), 0)).collect();
      while let Some((table, depth)) = queue.pop_front() {
         if !seen.insert(table.clone()) {
            continue;
         }
         if depth < 8 {
            let bare = table.rsplit('.').next().unwrap_or(&table);
            let bases = self
               .view_tables
               .get(&table)
               .or_else(|| self.view_tables.get(bare));
            if let Some(bases) = bases {
               for base in bases {
                  if !seen.contains(base.as_str()) {
                     queue.push_back((base.clone(), depth + 1));
                  }
               }
            }
         }
         out.push(table);
      }
      out
   }
}

pub(super) struct CatalogState {
   pub(super) snapshot: RwLock<Option<Arc<CatalogSnapshot>>>,
   pub(super) generation: AtomicU64,
   pub(super) stale: AtomicBool,
   pub(super) refreshing: AtomicBool,
   pub(super) last_attempt_ms: AtomicU64,
   pub(super) last_refresh_ms: AtomicU64,
   // Set to true once any snapshot contains volatile views.  Used by
   // is_cacheable() to skip the RwLock entirely for function-free queries.
   pub(super) has_volatile_views: AtomicBool,
}

impl Default for CatalogState {
   fn default() -> Self {
      Self {
         snapshot: RwLock::new(None),
         generation: AtomicU64::new(0),
         stale: AtomicBool::new(false),
         refreshing: AtomicBool::new(false),
         last_attempt_ms: AtomicU64::new(0),
         last_refresh_ms: AtomicU64::new(0),
         has_volatile_views: AtomicBool::new(false),
      }
   }
}

impl CatalogState {
   pub(super) fn new() -> Self {
      Self {
         stale: AtomicBool::new(true),
         ..Self::default()
      }
   }
}

fn row_text(row: &DbRow, index: usize) -> Option<&str> {
   match row.values.get(index) {
      Some(DbValue::Text(text)) => Some(text.as_str()),
      _ => None,
   }
}

fn definition_mentions(definition: &str, needle: &str) -> bool {
   let mut from = 0;
   while let Some(offset) = definition[from..].find(needle) {
      let position = from + offset;
      let qualified = position > 0 && definition.as_bytes()[position - 1] == b'.';
      if !qualified {
         return true;
      }
      from = position + needle.len();
   }
   false
}
