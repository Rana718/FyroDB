use super::value::StoreValue;
use customhash::CustomMap;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

pub struct Store {
    pub(crate) data: CustomMap<StoreValue>,
    pub cluster: crate::cluster::ClusterConfig,
    pub(crate) replication: Option<crate::cluster::ReplicationCoordinator>,
    pub(crate) connected_clients: AtomicUsize,
    pub(crate) ttl_count: AtomicUsize,
    ttl_generation: AtomicU64,
    replica_applied_offset: AtomicU64,
    replica_meta_path: Mutex<Option<String>>,
    replica_identity: Mutex<Option<[u8; 16]>>,
    cluster_meta_path: Mutex<Option<String>>,
    cluster_peers_total: AtomicUsize,
    cluster_peers_healthy: AtomicUsize,
    cluster_peers_suspect: AtomicUsize,
    cluster_queue_full: AtomicU64,
    cluster_reconnects: AtomicU64,
    cluster_snapshot_attempts: AtomicU64,
    cluster_replication_lag_total: AtomicU64,
    cluster_replication_lag_max: AtomicU64,
    pub(crate) int_create_lock: Mutex<()>,
    pub(crate) cluster_state: crate::cluster::ClusterState,
    replica_installing: std::sync::atomic::AtomicBool,
    cluster_write_gate: Mutex<()>,
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
        Self {
            data: CustomMap::with_capacity(shards, max_keys),
            cluster,
            replication: if replication_enabled {
                Some(crate::cluster::ReplicationCoordinator::new(
                    replication_log_capacity,
                ))
            } else {
                None
            },
            connected_clients: AtomicUsize::new(0),
            ttl_count: AtomicUsize::new(0),
            ttl_generation: AtomicU64::new(0),
            replica_applied_offset: AtomicU64::new(0),
            replica_meta_path: Mutex::new(None),
            replica_identity: Mutex::new(None),
            cluster_meta_path: Mutex::new(None),
            cluster_peers_total: AtomicUsize::new(0),
            cluster_peers_healthy: AtomicUsize::new(0),
            cluster_peers_suspect: AtomicUsize::new(0),
            cluster_queue_full: AtomicU64::new(0),
            cluster_reconnects: AtomicU64::new(0),
            cluster_snapshot_attempts: AtomicU64::new(0),
            cluster_replication_lag_total: AtomicU64::new(0),
            cluster_replication_lag_max: AtomicU64::new(0),
            int_create_lock: Mutex::new(()),
            cluster_state: crate::cluster::ClusterState::with_topology(
                failure_quorum,
                std::time::Duration::from_secs(30),
                initial_topology,
            ),
            replica_installing: std::sync::atomic::AtomicBool::new(false),
            cluster_write_gate: Mutex::new(()),
        }
    }

    pub fn cluster_state(&self) -> crate::cluster::ClusterState {
        self.cluster_state.clone()
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
        if !self.cluster_state.replace_topology(topology) {
            return false;
        }
        let _ = self.persist_cluster_metadata();
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

    pub fn defragment_values(&self, budget: usize) -> usize {
        let rebuilt = self
            .data
            .defragment_values(budget, |value| value.compact_allocations());
        customhash::force_collect_quiescent();
        rebuilt
    }

    pub fn map_shard_layout_matches(&self, capacities: &[usize]) -> bool {
        self.data.shard_layout_matches(capacities)
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
                    self.data.insert(key.to_owned(), value);
                    self.adjust_replaced_ttl(old_has_ttl, new_has_ttl);
                }
            }
            crate::cluster::MutationKind::Set => {
                let value = std::str::from_utf8(&record.value)
                    .map_err(|_| crate::cluster::ApplyError::InvalidValue)?;
                let expires_ms = record.expire_at_ms.unwrap_or(0);
                let store_value = StoreValue {
                    value: crate::storage::value::FyroDB::String(
                        crate::storage::value::SmallStr::new(value),
                    ),
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
                    self.data.set(key, store_value, || key.to_owned());
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

    pub fn record_current_value(&self, key: &str) {
        let Some(log) = &self.replication else { return };
        let Some(value) = self.data.get_ref(key) else {
            let _ = log.append(crate::cluster::MutationRecord {
                offset: 0,
                slot: crate::cluster::hash_slot(key.as_bytes()),
                kind: crate::cluster::MutationKind::Delete,
                key: key.as_bytes().to_vec(),
                value: Vec::new(),
                expire_at_ms: None,
            });
            return;
        };
        if value.is_expired() {
            let _ = log.append(crate::cluster::MutationRecord {
                offset: 0,
                slot: crate::cluster::hash_slot(key.as_bytes()),
                kind: crate::cluster::MutationKind::Delete,
                key: key.as_bytes().to_vec(),
                value: Vec::new(),
                expire_at_ms: None,
            });
            return;
        }
        let Ok(encoded) = crate::storage::rdb::encode_single_value(&value) else {
            return;
        };
        let _ = log.append(crate::cluster::MutationRecord::replace(
            crate::cluster::hash_slot(key.as_bytes()),
            key.as_bytes().to_vec(),
            encoded,
            Some(value.expires_ms).filter(|expiry| *expiry != 0),
        ));
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

    pub fn set_replica_applied_offset(&self, offset: u64) {
        self.replica_applied_offset.store(offset, Ordering::Release);
        if let Some(path) = self.replica_meta_path.lock().unwrap().as_deref() {
            let _ = crate::storage::rdb::save_replication_metadata(
                path,
                self.cluster.topology.epoch,
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
        self.cluster_peers_total.store(total, Ordering::Relaxed);
        self.cluster_peers_healthy.store(healthy, Ordering::Relaxed);
        self.cluster_peers_suspect.store(suspect, Ordering::Relaxed);
    }

    pub fn cluster_health_counts(&self) -> (usize, usize, usize) {
        (
            self.cluster_peers_total.load(Ordering::Relaxed),
            self.cluster_peers_healthy.load(Ordering::Relaxed),
            self.cluster_peers_suspect.load(Ordering::Relaxed),
        )
    }

    pub fn update_cluster_transport_metrics(&self, queue_full: u64, reconnects: u64) {
        self.cluster_queue_full.store(queue_full, Ordering::Relaxed);
        self.cluster_reconnects.store(reconnects, Ordering::Relaxed);
    }

    pub fn record_snapshot_attempt(&self) {
        self.cluster_snapshot_attempts
            .fetch_add(1, Ordering::Relaxed);
    }

    pub fn cluster_snapshot_attempts(&self) -> u64 {
        self.cluster_snapshot_attempts.load(Ordering::Relaxed)
    }

    pub fn update_cluster_replication_lag(&self, total: u64, max: u64) {
        self.cluster_replication_lag_total
            .store(total, Ordering::Relaxed);
        self.cluster_replication_lag_max
            .store(max, Ordering::Relaxed);
    }

    pub fn cluster_replication_lag(&self) -> (u64, u64) {
        (
            self.cluster_replication_lag_total.load(Ordering::Relaxed),
            self.cluster_replication_lag_max.load(Ordering::Relaxed),
        )
    }

    pub fn cluster_transport_metrics(&self) -> (u64, u64) {
        (
            self.cluster_queue_full.load(Ordering::Relaxed),
            self.cluster_reconnects.load(Ordering::Relaxed),
        )
    }

    pub fn replica_installing(&self) -> bool {
        self.replica_installing.load(Ordering::Acquire)
    }

    pub fn set_replica_installing(&self, installing: bool) {
        self.replica_installing.store(installing, Ordering::Release);
    }

    pub fn cluster_write_guard(&self) -> std::sync::MutexGuard<'_, ()> {
        self.cluster_write_gate
            .lock()
            .expect("cluster write gate poisoned")
    }

    #[inline]
    pub fn add_ttl(&self) {
        self.ttl_generation.fetch_add(1, Ordering::Release);
        self.ttl_count.fetch_add(1, Ordering::Relaxed);
    }

    #[inline]
    pub fn sub_ttl(&self) {
        let _ = self
            .ttl_count
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |count| {
                count.checked_sub(1)
            });
    }

    #[inline]
    pub fn has_ttl_keys(&self) -> bool {
        self.ttl_count.load(Ordering::Relaxed) > 0
    }

    pub fn reset_ttl_count(&self) {
        self.ttl_count.store(0, Ordering::Relaxed);
        self.ttl_generation.fetch_add(1, Ordering::Release);
    }

    #[inline]
    pub fn ttl_generation(&self) -> u64 {
        self.ttl_generation.load(Ordering::Acquire)
    }

    #[inline]
    pub fn finish_ttl_scan(&self, generation: u64, live_ttls: usize) {
        if self.ttl_generation.load(Ordering::Acquire) != generation {
            return;
        }
        let observed = self.ttl_count.load(Ordering::Acquire);
        if self.ttl_generation.load(Ordering::Acquire) != generation {
            return;
        }
        let _ = self.ttl_count.compare_exchange(
            observed,
            live_ttls,
            Ordering::Release,
            Ordering::Relaxed,
        );
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
            value: crate::storage::value::FyroDB::String(crate::storage::value::SmallStr::new(
                "one",
            )),
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
        assert_eq!(store.ttl_count.load(Ordering::Relaxed), 1);
        store
            .apply_replica_mutation(&replace(2, &expiring))
            .unwrap();
        assert_eq!(store.ttl_count.load(Ordering::Relaxed), 1);
        store
            .apply_replica_mutation(&replace(3, &persistent))
            .unwrap();
        assert_eq!(store.ttl_count.load(Ordering::Relaxed), 0);

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
        assert_eq!(store.ttl_count.load(Ordering::Relaxed), 0);
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
        assert_eq!(store.ttl_count.load(Ordering::Relaxed), 2);

        store.cleanup_expired();
        assert_eq!(store.ttl_count.load(Ordering::Relaxed), 1);

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
}

pub fn rss_bytes() -> usize {
    // mimalloc eagerly decommits pages (MADV_DONTNEED) so VmRSS accurately
    // reflects actual memory usage without transient inflation.
    proc_status_kb("VmRSS:").saturating_mul(1024)
}

pub fn data_memory_bytes() -> usize {
    rss_bytes()
}

/// Bytes currently allocated by the allocator.
pub fn allocated_bytes() -> usize {
    rust_zmalloc::used_memory()
}

pub fn peak_rss_bytes() -> usize {
    proc_status_kb("VmHWM:").saturating_mul(1024)
}

/// Ask the allocator to release unused pages back to the OS.
/// mimalloc eagerly decommits by default, so this is mostly a hint
/// after bulk-free operations like FLUSHALL.
pub fn purge_allocator() {
    rust_zmalloc::purge();
}

/// Purge only when fragmentation is material.
pub fn purge_allocator_if_fragmented() {
    let used = rust_zmalloc::used_memory();
    let rss = rss_bytes();
    // If RSS is more than 20% above used memory, trigger a purge
    if rss > used.saturating_add(used / 5) && rss.saturating_sub(used) >= 10 * 1024 * 1024 {
        rust_zmalloc::purge();
    }
}

pub fn cgroup_memory_bytes() -> usize {
    [
        "/sys/fs/cgroup/memory.current",
        "/sys/fs/cgroup/memory/memory.usage_in_bytes",
    ]
    .iter()
    .find_map(|path| std::fs::read_to_string(path).ok()?.trim().parse().ok())
    .unwrap_or(0)
}

fn proc_status_kb(name: &str) -> usize {
    std::fs::read_to_string("/proc/self/status")
        .ok()
        .and_then(|status| {
            status.lines().find_map(|line| {
                let value = line.strip_prefix(name)?;
                value.split_whitespace().next()?.parse().ok()
            })
        })
        .unwrap_or(0)
}
