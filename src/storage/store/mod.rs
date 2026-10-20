use super::value::StoreValue;
use crossbeam_utils::CachePadded;
use customhash::CustomMap;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};

mod cluster;
mod fence;
mod memory;
mod ttl;

use cluster::ClusterMetrics;
use fence::WriteFence;
pub use fence::{WriteFenceGuard, WriteTicket};
pub use memory::*;
use ttl::TtlCounters;

pub struct Store {
   pub(crate) data: CustomMap<StoreValue>,
   pub(crate) replication: Option<crate::cluster::ReplicationCoordinator>,

   pub(crate) connected_clients: AtomicUsize,
   ttl: TtlCounters,
   replica_applied_offset: AtomicU64,
   replica_installing: std::sync::atomic::AtomicBool,
   pub(crate) search: crate::search::SearchState,
   /// Connections parked on blocking commands, by key.
   pub blocking: crate::storage::blocking::BlockRegistry,

   pub cluster: Box<crate::cluster::ClusterConfig>,
   pub(crate) cluster_state: crate::cluster::ClusterState,
   cluster_write_fence: WriteFence,
   replica_meta_path: Mutex<Option<String>>,
   replica_identity: Mutex<Option<[u8; 16]>>,
   cluster_meta_path: Mutex<Option<String>>,
   metrics: Box<ClusterMetrics>,
}

impl Default for Store {
   fn default() -> Self {
      Self::new()
   }
}

impl Store {
   pub fn new() -> Self {
      Self::with_config(1, 1_000)
   }

   pub fn with_config(shards: usize, max_keys: usize) -> Self {
      Self::with_config_workers(shards, max_keys, num_cpus::get())
   }

   /// `workers` sizes the per-worker write-fence counters, so it must be at
   /// least the number of workers that will call `begin_write`.
   pub fn with_config_workers(shards: usize, max_keys: usize, workers: usize) -> Self {
      rust_zmalloc::configure_allocator();
      let cluster = crate::cluster::ClusterConfig::from_env()
         .unwrap_or_else(|error| panic!("invalid cluster configuration: {error}"));
      // An all-primary topology has no replication destination. Avoid
      // allocating and maintaining a mutation log that no peer can consume.
      let replication_enabled = cluster.local_node().is_some_and(|local| {
         local.role == crate::cluster::NodeRole::Primary
            && cluster.topology.nodes.iter().any(|node| {
               node.role == crate::cluster::NodeRole::Replica
                  && node.replica_of.as_deref() == Some(local.id.as_str())
            })
      });
      let replication_log_capacity = cluster.replication_log_capacity;
      let initial_topology = cluster.topology.clone();
      let failure_quorum = cluster.failure_quorum;
      let data = CustomMap::with_capacity(shards, max_keys);
      let search = crate::search::SearchState::new();
      // Incremental index maintenance: the map reports every write to the
      // search hooks, gated by a flag that is on only while an index exists.
      data.set_observer(Box::new(crate::search::hooks::HookObserver(
         std::sync::Arc::clone(&search.hooks),
      )));
      search.hooks.attach(data.observing_handle());
      Self {
         data,
         cluster: Box::new(cluster),
         replication: if replication_enabled {
            Some(crate::cluster::ReplicationCoordinator::new(
               replication_log_capacity,
            ))
         } else {
            None
         },
         connected_clients: AtomicUsize::new(0),
         ttl: TtlCounters::new(),
         replica_applied_offset: AtomicU64::new(0),
         replica_meta_path: Mutex::new(None),
         replica_identity: Mutex::new(None),
         cluster_meta_path: Mutex::new(None),
         cluster_state: crate::cluster::ClusterState::with_topology(
            failure_quorum,
            std::time::Duration::from_secs(30),
            initial_topology,
         ),
         replica_installing: std::sync::atomic::AtomicBool::new(false),
         search,
         cluster_write_fence: WriteFence::new(workers),
         metrics: Box::new(ClusterMetrics::new()),
         blocking: Default::default(),
      }
   }

   pub fn client_connected(&self) {
      self.connected_clients.fetch_add(1, Ordering::Relaxed);
   }

   pub fn client_disconnected(&self) {
      self.connected_clients.fetch_sub(1, Ordering::Relaxed);
   }

   pub fn connected_clients(&self) -> usize {
      self.connected_clients.load(Ordering::Relaxed)
   }

   pub fn map_shard_count(&self) -> usize {
      self.data.shard_count()
   }

   #[inline(always)]
   pub fn prefetch_batch<K: AsRef<[u8]>>(&self, keys: &[K], hashes: &mut [u64]) {
      self.data.prefetch_batch(keys, hashes);
   }

   #[inline(always)]
   pub fn prefetch_batch_entries<K: AsRef<[u8]>>(
      &self,
      keys: &[K],
      hashes: &mut [u64],
      entries: &mut [*const ()],
   ) {
      self.data.prefetch_batch_entries(keys, hashes, entries);
   }

   #[inline(always)]
   pub fn prefetch_slots<K: AsRef<[u8]>>(
      &self,
      keys: &[K],
      hashes: &mut [u64],
      tables: &mut [*const ()],
   ) {
      self.data.prefetch_slots(keys, hashes, tables);
   }

   #[inline(always)]
   pub fn prefetch_slots_write<K: AsRef<[u8]>>(
      &self,
      keys: &[K],
      hashes: &mut [u64],
      tables: &mut [*const ()],
   ) {
      self.data.prefetch_slots_write(keys, hashes, tables);
   }

   #[inline(always)]
   pub fn probe_and_prefetch_entries(
      &self,
      tables: &[*const ()],
      hashes: &[u64],
      entries: &mut [*const ()],
   ) {
      self
         .data
         .probe_and_prefetch_entries(tables, hashes, entries);
   }

   #[inline(always)]
   pub fn probe_and_prefetch_entries_write(
      &self,
      tables: &[*const ()],
      hashes: &[u64],
      entries: &mut [*const ()],
   ) {
      self
         .data
         .probe_and_prefetch_entries_write(tables, hashes, entries);
   }

   /// Single relaxed load, constant-false with no limit configured.
   #[inline(always)]
   pub fn at_key_capacity(&self) -> bool {
      self.data.is_full()
   }

   pub fn map_shard_slot_count(&self, shard: usize) -> usize {
      self.data.shard_slot_count(shard)
   }

   pub fn compact_shard(&self, shard: usize) {
      self.data.compact_shard(shard);
   }

   /// Reclaims oversized shard tables after workload shrinkage. The map's
   /// compaction gate serializes each shard and readers remain EBR-safe.
   pub fn compact_underutilized(&self) {
      for shard in 0..self.map_shard_count() {
         self.data.compact_shard(shard);
      }
      customhash::force_collect_quiescent();
   }

   /// Returns `(next_slot, capacity, rebuilt)`; `next_slot == capacity`
   /// means the shard is done.
   pub fn defragment_shard_range(
      &self,
      shard: usize,
      start_slot: usize,
      budget: usize,
   ) -> (usize, usize, usize) {
      self
         .data
         .defragment_shard_range(shard, start_slot, budget, |value| {
            value.compact_allocations()
         })
         .unwrap_or_default()
   }

   pub fn map_shard_layout_matches(&self, capacities: &[usize]) -> bool {
      self.data.shard_layout_matches(capacities)
   }
}

#[cfg(test)]
mod tests {
   use super::Store;
   use crate::storage::value::StoreValue;
   use std::sync::atomic::Ordering;
   use std::time::{Duration, Instant};

   #[test]
   fn replica_mutations_validate_slot_and_apply_without_logging() {
      let store = Store::with_config(1, 16);
      let mut record = crate::cluster::MutationRecord {
         offset: 1,
         slot: crate::cluster::hash_slot(b"key"),
         kind: crate::cluster::MutationKind::Set,
         key: b"key".to_vec(),
         value: b"value".to_vec(),
         expire_at_ms: None,
      };
      store.apply_replica_mutation(&record).unwrap();
      assert_eq!(store.get("key").as_deref(), Some("value"));
      assert!(store.replication.as_ref().is_none_or(|log| log.is_empty()));

      record.slot = crate::cluster::Slot(0);
      if record.slot == crate::cluster::hash_slot(b"key") {
         record.slot = crate::cluster::Slot(1);
      }
      assert_eq!(
         store.apply_replica_mutation(&record),
         Err(crate::cluster::ApplyError::WrongSlot)
      );
   }

   #[test]
   fn replica_replace_and_delete_keep_ttl_accounting_exact() {
      let store = Store::with_config(1, 16);
      let key = b"ttl-key";
      let slot = crate::cluster::hash_slot(key);
      let expiring = StoreValue {
         value: crate::storage::value::FyroDB::String(crate::storage::value::SmallStr::new("one")),
         expires_ms: u64::MAX,
      };
      let persistent = StoreValue::string("two".to_owned());

      let replace = |offset, value: &StoreValue| crate::cluster::MutationRecord {
         offset,
         slot,
         kind: crate::cluster::MutationKind::Replace,
         key: key.to_vec(),
         value: crate::storage::rdb::encode_single_value(value).unwrap(),
         expire_at_ms: None,
      };

      store
         .apply_replica_mutation(&replace(1, &expiring))
         .unwrap();
      assert_eq!(store.ttl_live(), 1);
      store
         .apply_replica_mutation(&replace(2, &expiring))
         .unwrap();
      assert_eq!(store.ttl_live(), 1);
      store
         .apply_replica_mutation(&replace(3, &persistent))
         .unwrap();
      assert_eq!(store.ttl_live(), 0);

      store
         .apply_replica_mutation(&replace(4, &expiring))
         .unwrap();
      let delete = crate::cluster::MutationRecord {
         offset: 5,
         slot,
         kind: crate::cluster::MutationKind::Delete,
         key: key.to_vec(),
         value: Vec::new(),
         expire_at_ms: None,
      };
      store.apply_replica_mutation(&delete).unwrap();
      assert_eq!(store.ttl_live(), 0);
   }

   #[test]
   fn ttl_scan_repairs_overcount_after_overwrite_and_delete() {
      let store = Store::with_config(1, 16);
      let expires_at = Some(Instant::now() + Duration::from_secs(60));

      store.set(
         "key".to_owned(),
         StoreValue::string_with_expiry("one".to_owned(), expires_at),
      );
      store.set(
         "key".to_owned(),
         StoreValue::string_with_expiry("two".to_owned(), expires_at),
      );
      assert_eq!(store.ttl_live(), 2);

      store.cleanup_expired();
      assert_eq!(store.ttl_live(), 1);

      assert!(store.del("key"));
      store.cleanup_expired();
      assert!(!store.has_ttl_keys());
   }

   #[test]
   fn stale_scan_cannot_hide_a_new_ttl() {
      let store = Store::with_config(1, 16);
      let generation = store.ttl_generation();

      store.add_ttl();
      store.finish_ttl_scan(generation, 0);

      assert!(store.has_ttl_keys());
   }

   #[test]
   fn ttl_decrement_saturates_at_zero() {
      let store = Store::with_config(1, 16);
      store.sub_ttl();
      assert!(!store.has_ttl_keys());
   }

   #[test]
   fn getex_updates_ttl_accounting() {
      let store = Store::with_config(1, 16);
      store.set("key".to_owned(), StoreValue::string("value".to_owned()));

      assert_eq!(store.getex_ms("key", u64::MAX), Some("value".to_owned()));
      assert!(store.has_ttl_keys());

      assert_eq!(store.getex_ms("key", 0), Some("value".to_owned()));
      assert!(!store.has_ttl_keys());
   }

   /// Replaced a process-wide mutex while keeping what migration and
   /// bootstrap need: no write sections while the guard is held.
   #[test]
   fn write_fence_excludes_in_flight_writes() {
      use std::sync::Arc;
      use std::sync::atomic::{AtomicBool, AtomicU64};

      const WORKERS: usize = 4;
      let store = Arc::new(Store::with_config_workers(1, usize::MAX, WORKERS));
      let writes = Arc::new(AtomicU64::new(0));
      let stop = Arc::new(AtomicBool::new(false));

      let mut handles = Vec::new();
      for worker in 0..WORKERS {
         let store = Arc::clone(&store);
         let writes = Arc::clone(&writes);
         let stop = Arc::clone(&stop);
         handles.push(std::thread::spawn(move || {
            while !stop.load(Ordering::Relaxed) {
               let _ticket = store.begin_write(worker);
               writes.fetch_add(1, Ordering::Relaxed);
            }
         }));
      }

      for _ in 0..50 {
         let guard = store.cluster_write_guard();
         let before = writes.load(Ordering::Relaxed);
         std::thread::sleep(std::time::Duration::from_millis(1));
         let after = writes.load(Ordering::Relaxed);
         drop(guard);
         assert_eq!(
            before, after,
            "a write section ran while the fence was engaged"
         );
         std::thread::yield_now();
      }

      stop.store(true, Ordering::Relaxed);
      for handle in handles {
         handle.join().unwrap();
      }
      assert!(
         writes.load(Ordering::Relaxed) > 0,
         "writers never made progress, so the fence proved nothing"
      );
   }

   /// An out-of-range worker index must fall back to the barrier rather than
   /// silently executing outside the fence.
   #[test]
   fn unknown_worker_index_still_gets_fenced() {
      let store = Store::with_config_workers(1, usize::MAX, 2);
      let ticket = store.begin_write(99);
      // The fence holder must not be able to proceed while this is alive.
      let fenced = std::thread::scope(|scope| {
         let handle = scope.spawn(|| {
            let _guard = store.cluster_write_guard();
         });
         std::thread::sleep(std::time::Duration::from_millis(20));
         let still_blocked = !handle.is_finished();
         drop(ticket);
         handle.join().unwrap();
         still_blocked
      });
      assert!(fenced, "fence holder ran while an unfenced write was open");
   }
}
