use super::*;

pub(super) struct ClusterMetrics {
   pub(super) peers_total: AtomicUsize,
   pub(super) peers_healthy: AtomicUsize,
   pub(super) peers_suspect: AtomicUsize,
   pub(super) queue_full: AtomicU64,
   pub(super) reconnects: AtomicU64,
   pub(super) snapshot_attempts: AtomicU64,
   pub(super) replication_lag_total: AtomicU64,
   pub(super) replication_lag_max: AtomicU64,
}

impl ClusterMetrics {
   pub(super) fn new() -> Self {
      Self {
         peers_total: AtomicUsize::new(0),
         peers_healthy: AtomicUsize::new(0),
         peers_suspect: AtomicUsize::new(0),
         queue_full: AtomicU64::new(0),
         reconnects: AtomicU64::new(0),
         snapshot_attempts: AtomicU64::new(0),
         replication_lag_total: AtomicU64::new(0),
         replication_lag_max: AtomicU64::new(0),
      }
   }
}

impl Store {
   pub fn cluster_state(&self) -> crate::cluster::ClusterState {
      self.cluster_state.clone()
   }

   #[inline(always)]
   pub fn cluster_state_ref(&self) -> &crate::cluster::ClusterState {
      &self.cluster_state
   }

   #[inline(always)]
   pub fn has_replication(&self) -> bool {
      self.replication.is_some() || crate::storage::persistence::aof::enabled()
   }

   #[inline]
   pub(crate) fn aof_capture(&self, key: &str) {
      if let Some(aof) = crate::storage::persistence::aof::get() {
         aof.record(key, || {
            self
               .data
               .get_ref(key)
               .and_then(|v| crate::storage::persistence::aof::encode_live(&v))
         });
      }
   }

   pub fn record_current_value(&self, key: &str) {
      self.aof_capture(key);
      let Some(log) = &self.replication else { return };
      let slot = crate::cluster::hash_slot(key.as_bytes());
      let Some(value) = self.data.get_ref(key).filter(|value| !value.is_expired()) else {
         let _ = log.append(crate::cluster::MutationRecord {
            offset: 0,
            slot,
            kind: crate::cluster::MutationKind::Delete,
            key: key.as_bytes().to_vec(),
            value: Vec::new(),
            expire_at_ms: None,
         });
         return;
      };
      let Ok(encoded) = crate::storage::rdb::encode_single_value(&value) else {
         return;
      };
      let _ = log.append(crate::cluster::MutationRecord::replace(
         slot,
         key.as_bytes().to_vec(),
         encoded,
         Some(value.expires_ms).filter(|expiry| *expiry != 0),
      ));
   }

   pub fn cluster_topology(&self) -> crate::cluster::Topology {
      self.cluster_state.topology()
   }

   pub fn cluster_topology_arc(&self) -> std::sync::Arc<crate::cluster::Topology> {
      self.cluster_state.topology_arc()
   }

   pub fn load_cluster_metadata(&self, rdb_path: &str) {
      let path = format!("{rdb_path}.cluster");
      if let Ok(Some((node_id, epoch))) = crate::storage::rdb::load_cluster_metadata(&path)
         && node_id == self.cluster.local_id
         && epoch > self.cluster_state.topology().epoch
      {
         let mut topology = self.cluster_state.topology();
         topology.epoch = epoch;
         for node in &mut topology.nodes {
            node.epoch = node.epoch.max(epoch);
         }
         let _ = self.cluster_state.replace_topology(topology);
      }
      *self.cluster_meta_path.lock().unwrap() = Some(path);
      let _ = self.persist_cluster_metadata();
      if let Some(log) = &self.replication {
         let journal_path = format!("{rdb_path}.repllog");
         let mut identity = [0u8; 16];
         use std::hash::{Hash, Hasher};
         let mut hasher = std::collections::hash_map::DefaultHasher::new();
         self.cluster.local_id.hash(&mut hasher);
         let digest = hasher.finish();
         identity[..8].copy_from_slice(&digest.to_le_bytes());
         identity[8..].copy_from_slice(&self.cluster_state.topology().epoch.to_le_bytes());
         if log.open_journal(&journal_path, identity).is_err() {
            let _ = std::fs::remove_file(&journal_path);
            let _ = log.open_journal(&journal_path, identity);
         }
      }
   }

   pub fn install_cluster_topology(&self, topology: crate::cluster::Topology) -> bool {
      if !self.cluster_state.replace_topology(topology.clone()) {
         return false;
      }
      let _ = self.persist_cluster_metadata();
      let _ = crate::cluster::save_nodes_conf(
         &self.cluster.nodes_config_file,
         &topology,
         &self.cluster.local_id,
      );
      true
   }

   fn persist_cluster_metadata(&self) -> std::io::Result<()> {
      let Some(path) = self.cluster_meta_path.lock().unwrap().clone() else {
         return Ok(());
      };
      crate::storage::rdb::save_cluster_metadata(
         &path,
         &self.cluster.local_id,
         self.cluster_state.topology().epoch,
      )
   }

   /// Apply an already-ordered primary mutation without appending it to the
   /// local replication log. Replica transport validates offsets separately.
   pub fn apply_replica_mutation(
      &self,
      record: &crate::cluster::MutationRecord,
   ) -> Result<(), crate::cluster::ApplyError> {
      let key =
         std::str::from_utf8(&record.key).map_err(|_| crate::cluster::ApplyError::InvalidKey)?;
      if crate::cluster::hash_slot(record.key.as_slice()) != record.slot {
         return Err(crate::cluster::ApplyError::WrongSlot);
      }
      match record.kind {
         crate::cluster::MutationKind::Replace => {
            let value = crate::storage::rdb::decode_single_value(&record.value)
               .map_err(|_| crate::cluster::ApplyError::InvalidValue)?;
            let old_has_ttl = self
               .data
               .get_ref(key)
               .is_some_and(|old| old.expires_ms != 0);
            if value.is_expired() {
               self.data.remove_no_clone(key);
               if old_has_ttl {
                  self.sub_ttl();
               }
            } else {
               let new_has_ttl = value.expires_ms != 0;
               self.data.insert_str(key, value);
               self.adjust_replaced_ttl(old_has_ttl, new_has_ttl);
            }
         }
         crate::cluster::MutationKind::Set => {
            let value = std::str::from_utf8(&record.value)
               .map_err(|_| crate::cluster::ApplyError::InvalidValue)?;
            let expires_ms = record.expire_at_ms.unwrap_or(0);
            let store_value = StoreValue {
               value: crate::storage::value::FyroDB::String(crate::storage::value::SmallStr::new(
                  value,
               )),
               expires_ms,
            };
            let old_has_ttl = self
               .data
               .get_ref(key)
               .is_some_and(|old| old.expires_ms != 0);
            if store_value.is_expired() {
               self.data.remove_no_clone(key);
               if old_has_ttl {
                  self.sub_ttl();
               }
            } else {
               self.data.set_str(key, store_value);
               self.adjust_replaced_ttl(old_has_ttl, expires_ms != 0);
            }
         }
         crate::cluster::MutationKind::Delete => {
            let old_has_ttl = self
               .data
               .get_ref(key)
               .is_some_and(|old| old.expires_ms != 0);
            self.data.remove_no_clone(key);
            if old_has_ttl {
               self.sub_ttl();
            }
         }
         crate::cluster::MutationKind::Expire => {
            let expires_ms = record
               .expire_at_ms
               .ok_or(crate::cluster::ApplyError::InvalidValue)?;
            let became_ttl = self
               .data
               .update_with(key, |value| {
                  let persistent = value.expires_ms == 0;
                  value.expires_ms = expires_ms;
                  persistent
               })
               .unwrap_or(false);
            if became_ttl {
               self.add_ttl();
            }
         }
      }
      Ok(())
   }

   #[inline]
   fn adjust_replaced_ttl(&self, old_has_ttl: bool, new_has_ttl: bool) {
      match (old_has_ttl, new_has_ttl) {
         (false, true) => self.add_ttl(),
         (true, false) => self.sub_ttl(),
         _ => {}
      }
   }

   pub fn replica_applied_offset(&self) -> u64 {
      self.replica_applied_offset.load(Ordering::Acquire)
   }

   pub fn replication_coordinator(&self) -> Option<crate::cluster::ReplicationCoordinator> {
      self.replication.clone()
   }

   pub fn replication_identity(&self) -> Option<[u8; 16]> {
      self.replication.as_ref().and_then(|log| log.identity())
   }

   pub fn set_replica_identity(&self, identity: [u8; 16]) {
      if self.replica_identity() != Some(identity) {
         self.replica_applied_offset.store(0, Ordering::Release);
         if let Some(path) = self.replica_meta_path.lock().unwrap().as_ref() {
            let _ = crate::storage::rdb::save_replication_metadata(
               path,
               self.cluster_state.topology().epoch,
               0,
            );
         }
      }
      *self.replica_identity.lock().unwrap() = Some(identity);
      if let Some(path) = self.replica_meta_path.lock().unwrap().as_ref() {
         let _ = std::fs::write(format!("{path}.id"), identity);
      }
   }

   pub fn replica_identity(&self) -> Option<[u8; 16]> {
      *self.replica_identity.lock().unwrap()
   }

   /// Visit live values in one hash slot. The callback receives an owned
   /// mutation record, keeping migration payloads bounded to one key.
   pub fn for_each_slot_record(
      &self,
      slot: crate::cluster::Slot,
      mut visit: impl FnMut(crate::cluster::MutationRecord),
   ) {
      self.data.for_each(|key, value| {
         if crate::cluster::hash_slot(key.as_bytes()) != slot || value.is_expired() {
            return;
         }
         if let Ok(encoded) = crate::storage::rdb::encode_single_value(value) {
            visit(crate::cluster::MutationRecord::replace(
               slot,
               key.as_bytes().to_vec(),
               encoded,
               Some(value.expires_ms).filter(|expiry| *expiry != 0),
            ));
         }
      });
   }

   pub fn remove_migrated_key(&self, key: &str) {
      let _ = self.data.remove_no_clone(key);
   }

   pub fn remove_slot_values(&self, slot: crate::cluster::Slot) -> usize {
      let mut removed = 0usize;
      self.data.retain(|key, value| {
         if crate::cluster::hash_slot(key.as_bytes()) != slot {
            return true;
         }
         if value.expires_ms != 0 {
            self.sub_ttl();
         }
         removed += 1;
         false
      });
      removed
   }

   pub fn count_keys_in_slot(&self, slot: crate::cluster::Slot) -> usize {
      self
         .scan_slot_parallel(usize::MAX, |key, value| {
            (!value.is_expired() && crate::cluster::hash_slot(key.as_bytes()) == slot).then_some(())
         })
         .len()
   }

   pub fn keys_in_slot(&self, slot: crate::cluster::Slot, limit: usize) -> Vec<String> {
      let mut keys = self.scan_slot_parallel(limit, |key, value| {
         if value.is_expired() || crate::cluster::hash_slot(key.as_bytes()) != slot {
            return None;
         }
         Some(key.to_owned())
      });
      keys.truncate(limit);
      keys
   }

   /// Shards are independent, so admin slot scans fan out across them instead
   /// of walking the whole keyspace on one thread. Each thread stops
   /// materializing items past `cap`, so a hot slot cannot blow up a bounded
   /// GETKEYSINSLOT into a full copy of the slot.
   fn scan_slot_parallel<T, F>(&self, cap: usize, scan: F) -> Vec<T>
   where
      T: Send,
      F: Fn(&str, &StoreValue) -> Option<T> + Sync,
   {
      const MAX_SCAN_THREADS: usize = 8;
      let shards = self.data.shard_count();
      if shards < 2 || cap == 0 {
         let mut found = Vec::new();
         self.data.for_each(|key, value| {
            if found.len() < cap
               && let Some(item) = scan(key, value)
            {
               found.push(item);
            }
         });
         return found;
      }
      let threads = std::thread::available_parallelism()
         .map_or(1, |count| count.get())
         .min(shards)
         .min(MAX_SCAN_THREADS);
      let per = shards.div_ceil(threads);
      std::thread::scope(|scope| {
         let mut handles = Vec::with_capacity(threads);
         for index in 0..threads {
            let from = index * per;
            let to = (from + per).min(shards);
            if from >= to {
               continue;
            }
            let data = &self.data;
            let scan = &scan;
            handles.push(scope.spawn(move || {
               let mut found = Vec::new();
               data.for_each_shard_range(from, to, |key, value| {
                  if found.len() < cap
                     && let Some(item) = scan(key, value)
                  {
                     found.push(item);
                  }
               });
               found
            }));
         }
         handles
            .into_iter()
            .flat_map(|handle| handle.join().expect("slot scan thread panicked"))
            .collect()
      })
   }

   /// Hot-path update: one atomic store. Callers checkpoint to disk with
   /// [`Store::persist_replica_offset`]; replays are idempotent, so a shorter
   /// checkpoint interval only costs re-application, not correctness.
   #[inline]
   pub fn set_replica_applied_offset(&self, offset: u64) {
      self.replica_applied_offset.store(offset, Ordering::Release);
   }

   pub fn persist_replica_offset(&self) {
      let offset = self.replica_applied_offset();
      if let Some(path) = self.replica_meta_path.lock().unwrap().as_deref() {
         let _ = crate::storage::rdb::save_replication_metadata(
            path,
            self.cluster_state.topology_arc().epoch,
            offset,
         );
      }
   }

   pub fn load_replication_metadata(&self, rdb_path: &str) {
      let path = format!("{rdb_path}.repl");
      if let Ok(Some((epoch, offset))) = crate::storage::rdb::load_replication_metadata(&path)
         && epoch == self.cluster.topology.epoch
      {
         self.replica_applied_offset.store(offset, Ordering::Release);
      }
      if let Ok(bytes) = std::fs::read(format!("{path}.id"))
         && bytes.len() == 16
      {
         *self.replica_identity.lock().unwrap() = Some(bytes.try_into().unwrap());
      }
      *self.replica_meta_path.lock().unwrap() = Some(path);
   }

   pub fn update_cluster_health(&self, total: usize, healthy: usize, suspect: usize) {
      self.metrics.peers_total.store(total, Ordering::Relaxed);
      self.metrics.peers_healthy.store(healthy, Ordering::Relaxed);
      self.metrics.peers_suspect.store(suspect, Ordering::Relaxed);
   }

   pub fn cluster_health_counts(&self) -> (usize, usize, usize) {
      (
         self.metrics.peers_total.load(Ordering::Relaxed),
         self.metrics.peers_healthy.load(Ordering::Relaxed),
         self.metrics.peers_suspect.load(Ordering::Relaxed),
      )
   }

   pub fn update_cluster_transport_metrics(&self, queue_full: u64, reconnects: u64) {
      self.metrics.queue_full.store(queue_full, Ordering::Relaxed);
      self.metrics.reconnects.store(reconnects, Ordering::Relaxed);
   }

   pub fn record_snapshot_attempt(&self) {
      self
         .metrics
         .snapshot_attempts
         .fetch_add(1, Ordering::Relaxed);
   }

   pub fn cluster_snapshot_attempts(&self) -> u64 {
      self.metrics.snapshot_attempts.load(Ordering::Relaxed)
   }

   pub fn update_cluster_replication_lag(&self, total: u64, max: u64) {
      self
         .metrics
         .replication_lag_total
         .store(total, Ordering::Relaxed);
      self
         .metrics
         .replication_lag_max
         .store(max, Ordering::Relaxed);
   }

   pub fn cluster_replication_lag(&self) -> (u64, u64) {
      (
         self.metrics.replication_lag_total.load(Ordering::Relaxed),
         self.metrics.replication_lag_max.load(Ordering::Relaxed),
      )
   }

   pub fn cluster_transport_metrics(&self) -> (u64, u64) {
      (
         self.metrics.queue_full.load(Ordering::Relaxed),
         self.metrics.reconnects.load(Ordering::Relaxed),
      )
   }

   pub fn replica_installing(&self) -> bool {
      self.replica_installing.load(Ordering::Acquire)
   }

   pub fn set_replica_installing(&self, installing: bool) {
      self.replica_installing.store(installing, Ordering::Release);
   }
}
