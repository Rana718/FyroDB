use foldhash::fast::FixedState;
use parking_lot::Mutex;
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicU64};

#[derive(Clone, Copy, Default, PartialEq, Eq)]
pub struct TableCounters {
   pub inserts: u64,
   pub updates: u64,
   pub deletes: u64,
}

pub(super) struct TableTracker {
   tables: HashMap<String, HashSet<String, FixedState>, FixedState>,
   markers: HashMap<String, Vec<String>, FixedState>,
   epochs: HashMap<String, u64, FixedState>,
   pub(super) insert_order: VecDeque<String>,
}

impl Default for TableTracker {
   fn default() -> Self {
      Self {
         tables: HashMap::with_hasher(FixedState::with_seed(0)),
         markers: HashMap::with_hasher(FixedState::with_seed(0)),
         epochs: HashMap::with_hasher(FixedState::with_seed(0)),
         insert_order: VecDeque::new(),
      }
   }
}

impl TableTracker {
   pub(super) fn register(&mut self, table: &str, key: String, marker: String) {
      let is_new = !self.markers.contains_key(&key);
      self
         .tables
         .entry(table.to_string())
         .or_insert_with(|| HashSet::with_hasher(FixedState::with_seed(0)))
         .insert(key.clone());
      let markers = self.markers.entry(key.clone()).or_default();
      if !markers.contains(&marker) {
         markers.push(marker);
      }
      if is_new {
         self.insert_order.push_back(key);
      }
   }

   pub(super) fn remove_markers(&mut self, key: &str) -> Vec<String> {
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

   pub(super) fn epoch(&self, table: &str) -> u64 {
      self.epochs.get(table).copied().unwrap_or_else(|| {
         let bare = normalize_table_name(table);
         self.epochs.get(&bare).copied().unwrap_or(0)
      })
   }

   pub(super) fn bump(&mut self, tables: &[String]) {
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

   pub(super) fn remove_table(&mut self, table: &str) -> Vec<String> {
      self
         .tables
         .remove(table)
         .map(|keys| keys.into_iter().collect())
         .unwrap_or_default()
   }

   pub(super) fn matching_tables(&self, name: &str) -> Vec<String> {
      let bare = normalize_table_name(name);
      self
         .tables
         .keys()
         .filter(|tracked| normalize_table_name(tracked) == bare)
         .cloned()
         .collect()
   }

   pub(super) fn clear(&mut self) {
      self.tables.clear();
      self.markers.clear();
      self.insert_order.clear();
   }

   pub(super) fn key_count(&self) -> usize {
      self.markers.len()
   }

   pub(super) fn tracked_table_names(&self) -> Vec<String> {
      let mut set: HashSet<String, FixedState> = HashSet::with_hasher(FixedState::with_seed(0));
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
   pub(super) fn detach_key(&mut self, prefix: &str, key: &str) -> Vec<String> {
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

   pub(super) fn contains_key(&self, key: &str) -> bool {
      self.markers.contains_key(key)
   }

   pub(super) fn markers_keys(&self) -> impl Iterator<Item = &String> {
      self.markers.keys()
   }
}

pub(super) struct ExternalSync {
   pub(super) last_poll_ms: AtomicU64,
   pub(super) poll_started_ms: AtomicU64,
   pub(super) in_progress: AtomicBool,
   pub(super) snapshot: Mutex<HashMap<String, TableCounters, FixedState>>,
}

impl Default for ExternalSync {
   fn default() -> Self {
      Self {
         last_poll_ms: AtomicU64::new(0),
         poll_started_ms: AtomicU64::new(0),
         in_progress: AtomicBool::new(false),
         snapshot: Mutex::new(HashMap::with_hasher(FixedState::with_seed(0))),
      }
   }
}

pub(super) fn index_prefix(prefix: &str) -> String {
   format!("{prefix}idx:")
}

pub(super) fn index_key(prefix: &str, table: &str, fingerprint: u64) -> String {
   format!("{prefix}idx:{table}:{fingerprint:016x}")
}

// Extracts the table name from an index marker key.
pub(super) fn table_from_marker<'a>(prefix: &str, marker: &'a str) -> Option<&'a str> {
   marker
      .strip_prefix(&index_prefix(prefix))?
      .rsplit_once(':')
      .map(|(table, _)| table)
}

pub(super) fn table_from_marker_any(marker: &str) -> Option<&str> {
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
