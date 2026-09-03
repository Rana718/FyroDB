use super::value::StoreValue;
use crossbeam_utils::CachePadded;
use customhash::CustomMap;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};

struct ClusterMetrics {
    peers_total: AtomicUsize,
    peers_healthy: AtomicUsize,
    peers_suspect: AtomicUsize,
    queue_full: AtomicU64,
    reconnects: AtomicU64,
    snapshot_attempts: AtomicU64,
    replication_lag_total: AtomicU64,
    replication_lag_max: AtomicU64,
}

impl ClusterMetrics {
    fn new() -> Self {
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

/// Writers increment per-worker counters; a fence holder sets the flag
/// and waits for all counters to drain before proceeding.
struct WriteFence {
    /// In-flight write count per worker. Uncontended: one worker owns each.
    in_flight: Box<[CachePadded<AtomicUsize>]>,
    /// Set while a fence holder is draining or holding the barrier.
    engaged: AtomicBool,
    gate: Mutex<()>,
}

impl WriteFence {
    fn new(workers: usize) -> Self {
        Self {
            in_flight: (0..workers.max(1))
                .map(|_| CachePadded::new(AtomicUsize::new(0)))
                .collect::<Vec<_>>()
                .into_boxed_slice(),
            engaged: AtomicBool::new(false),
            gate: Mutex::new(()),
        }
    }
}

/// Held for the duration of one write command.
pub struct WriteTicket<'a> {
    slot: Option<&'a AtomicUsize>,
    _gate: Option<std::sync::MutexGuard<'a, ()>>,
}

impl Drop for WriteTicket<'_> {
    #[inline(always)]
    fn drop(&mut self) {
        if let Some(slot) = self.slot {
            slot.fetch_sub(1, Ordering::Release);
        }
    }
}

/// Held while a bulk cluster operation needs a stable view of the keyspace.
pub struct WriteFenceGuard<'a> {
    fence: &'a WriteFence,
    _gate: std::sync::MutexGuard<'a, ()>,
}

impl Drop for WriteFenceGuard<'_> {
    fn drop(&mut self) {
        self.fence.engaged.store(false, Ordering::Release);
    }
}

/// Counters striped by CPU; `adds` is monotonic and doubles as the
/// scan generation.
struct TtlCounters {
    adds: Box<[CachePadded<AtomicU64>]>,
    removes: Box<[CachePadded<AtomicU64>]>,
}

impl TtlCounters {
    fn new() -> Self {
        let stripes = ttl_stripes();
        Self {
            adds: (0..stripes)
                .map(|_| CachePadded::new(AtomicU64::new(0)))
                .collect::<Vec<_>>()
                .into_boxed_slice(),
            removes: (0..stripes)
                .map(|_| CachePadded::new(AtomicU64::new(0)))
                .collect::<Vec<_>>()
                .into_boxed_slice(),
        }
    }

    #[inline(always)]
    fn stripe(&self) -> usize {
        #[cfg(target_os = "linux")]
        {
            let cpu = unsafe { libc::sched_getcpu() };
            if cpu < 0 {
                0
            } else {
                cpu as usize % self.adds.len()
            }
        }
        #[cfg(not(target_os = "linux"))]
        {
            0
        }
    }

    #[inline]
    fn add(&self) {
        let index = self.stripe();
        unsafe { self.adds.get_unchecked(index) }.fetch_add(1, Ordering::Relaxed);
    }

    #[inline]
    fn remove(&self) {
        let index = self.stripe();
        unsafe { self.removes.get_unchecked(index) }.fetch_add(1, Ordering::Relaxed);
    }

    fn total_adds(&self) -> u64 {
        self.adds
            .iter()
            .map(|stripe| stripe.load(Ordering::Acquire))
            .fold(0u64, u64::wrapping_add)
    }

    fn total_removes(&self) -> u64 {
        self.removes
            .iter()
            .map(|stripe| stripe.load(Ordering::Acquire))
            .fold(0u64, u64::wrapping_add)
    }

    /// Live TTL keys, saturating at zero: `removes` can momentarily lead
    /// `adds` across stripes, and an over-count is repaired by the next scan.
    fn live(&self) -> usize {
        self.total_adds().saturating_sub(self.total_removes()) as usize
    }

    /// Force the live count to `live_ttls` by moving the removes counter.
    fn set_live(&self, live_ttls: usize) {
        let adds = self.total_adds();
        let target_removes = adds.saturating_sub(live_ttls as u64);
        let current = self.total_removes();
        if target_removes > current {
            self.removes[0].fetch_add(target_removes - current, Ordering::Release);
        } else if current > target_removes {
            self.removes[0].fetch_sub(current - target_removes, Ordering::Release);
        }
    }
}

fn ttl_stripes() -> usize {
    num_cpus::get().next_power_of_two().clamp(1, 64)
}

pub struct Store {
    pub(crate) data: CustomMap<StoreValue>,
    pub(crate) replication: Option<crate::cluster::ReplicationCoordinator>,

    pub(crate) connected_clients: AtomicUsize,
    ttl: TtlCounters,
    replica_applied_offset: AtomicU64,
    replica_installing: std::sync::atomic::AtomicBool,

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
            cluster_write_fence: WriteFence::new(workers),
            metrics: Box::new(ClusterMetrics::new()),
        }
    }

    pub fn cluster_state(&self) -> crate::cluster::ClusterState {
        self.cluster_state.clone()
    }

    #[inline(always)]
    pub fn cluster_state_ref(&self) -> &crate::cluster::ClusterState {
        &self.cluster_state
    }

    #[inline(always)]
    pub fn has_replication(&self) -> bool {
        self.replication.is_some()
    }

    #[inline(always)]
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
        self.data
            .defragment_shard_range(shard, start_slot, budget, |value| {
                value.compact_allocations()
            })
            .unwrap_or_default()
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
                    self.data.insert_str(key, value);
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
        let mut count = 0usize;
        self.data.for_each(|key, value| {
            if !value.is_expired() && crate::cluster::hash_slot(key.as_bytes()) == slot {
                count += 1;
            }
        });
        count
    }

    pub fn keys_in_slot(&self, slot: crate::cluster::Slot, limit: usize) -> Vec<String> {
        let mut keys = Vec::new();
        self.data.for_each(|key, value| {
            if keys.len() >= limit {
                return;
            }
            if !value.is_expired() && crate::cluster::hash_slot(key.as_bytes()) == slot {
                keys.push(key.to_owned());
            }
        });
        keys
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
        self.metrics
            .snapshot_attempts
            .fetch_add(1, Ordering::Relaxed);
    }

    pub fn cluster_snapshot_attempts(&self) -> u64 {
        self.metrics.snapshot_attempts.load(Ordering::Relaxed)
    }

    pub fn update_cluster_replication_lag(&self, total: u64, max: u64) {
        self.metrics
            .replication_lag_total
            .store(total, Ordering::Relaxed);
        self.metrics
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

/// Both sides of the fence use SeqCst, so the total order guarantees
/// writer and fence holder observe each other.
    #[inline]
    pub fn begin_write(&self, worker: usize) -> WriteTicket<'_> {
        let fence = &self.cluster_write_fence;
        let slot = match fence.in_flight.get(worker) {
            Some(slot) => &**slot,
            // Out-of-range worker index: fall back to the barrier rather than
            // executing unfenced.
            None => {
                return WriteTicket {
                    slot: None,
                    _gate: Some(fence.gate.lock().unwrap_or_else(|e| e.into_inner())),
                };
            }
        };

        slot.fetch_add(1, Ordering::SeqCst);
        if !fence.engaged.load(Ordering::SeqCst) {
            return WriteTicket {
                slot: Some(slot),
                _gate: None,
            };
        }
        slot.fetch_sub(1, Ordering::Release);
        WriteTicket {
            slot: None,
            _gate: Some(fence.gate.lock().unwrap_or_else(|e| e.into_inner())),
        }
    }

/// Slot migration and snapshot bootstrap need a stable keyspace,
/// not writer exclusion.
    pub fn cluster_write_guard(&self) -> WriteFenceGuard<'_> {
        let fence = &self.cluster_write_fence;
        fence.engaged.store(true, Ordering::SeqCst);
        let gate = fence.gate.lock().unwrap_or_else(|e| e.into_inner());
        for slot in fence.in_flight.iter() {
            while slot.load(Ordering::SeqCst) != 0 {
                std::hint::spin_loop();
            }
        }
        WriteFenceGuard { fence, _gate: gate }
    }

    /// One uncontended increment on this CPU's stripe.
    #[inline]
    pub fn add_ttl(&self) {
        self.ttl.add();
    }

    #[inline]
    pub fn sub_ttl(&self) {
        self.ttl.remove();
    }

    #[inline]
    pub fn has_ttl_keys(&self) -> bool {
        self.ttl.live() > 0
    }

    pub fn reset_ttl_count(&self) {
        self.ttl.set_live(0);
    }

/// Doubles as the scan generation: changes only when a TTL is added.
    #[inline]
    pub fn ttl_generation(&self) -> u64 {
        self.ttl.total_adds()
    }

    #[inline]
    pub fn finish_ttl_scan(&self, generation: u64, live_ttls: usize) {
        // A TTL created while the scan ran makes its tally stale; leave the
        // counter alone and let the next pass repair it.
        if self.ttl.total_adds() != generation {
            return;
        }
        self.ttl.set_live(live_ttls);
    }

    #[cfg(test)]
    fn ttl_live(&self) -> usize {
        self.ttl.live()
    }
}



pub fn rss_bytes() -> usize {
    rust_zmalloc::resident_memory()
}

pub fn data_memory_bytes() -> usize {
    rust_zmalloc::used_memory()
}

pub fn allocated_bytes() -> usize {
    rust_zmalloc::used_memory()
}

pub fn peak_rss_bytes() -> usize {
    proc_status_kb("VmHWM:").saturating_mul(1024)
}

pub fn purge_allocator() {
    rust_zmalloc::purge();
}

/// used_memory is live requested bytes, rss what the OS mapped; the
/// gap is real fragmentation.
pub fn purge_allocator_if_fragmented() {
    let used = rust_zmalloc::used_memory();
    let rss = rust_zmalloc::resident_memory();
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
