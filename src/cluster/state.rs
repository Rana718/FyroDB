use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;

use super::{FailureReport, FailureTracker};

#[derive(Clone)]
pub struct ClusterState {
    failures: Arc<Mutex<FailureTracker>>,
    topology: Arc<RwLock<Arc<super::Topology>>>,
    migrations: Arc<Mutex<std::collections::HashMap<u16, String>>>,
    imports: Arc<Mutex<std::collections::HashMap<u16, String>>>,
    has_imports: Arc<AtomicBool>,
    has_migrations: Arc<AtomicBool>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cluster::{NodeInfo, NodeRole, Slot, SlotRange, Topology};

    fn topology() -> Topology {
        Topology::new(
            1,
            vec![
                NodeInfo {
                    id: "p".into(),
                    address: "p:8000".into(),
                    cluster_address: "p:18000".into(),
                    role: NodeRole::Primary,
                    replica_of: None,
                    epoch: 1,
                    slots: vec![SlotRange::new(Slot(0), Slot(16383)).unwrap()],
                },
                NodeInfo {
                    id: "r".into(),
                    address: "r:8000".into(),
                    cluster_address: "r:18000".into(),
                    role: NodeRole::Replica,
                    replica_of: Some("p".into()),
                    epoch: 7,
                    slots: vec![],
                },
            ],
        )
    }

    #[test]
    fn quorum_promotes_replica_and_bumps_epoch() {
        let state = ClusterState::with_topology(2, Duration::from_secs(30), topology());
        state.record_failure(FailureReport {
            target_id: "p".into(),
            reporter_id: "a".into(),
            epoch: 1,
        });
        state.record_failure(FailureReport {
            target_id: "p".into(),
            reporter_id: "b".into(),
            epoch: 1,
        });
        let promoted = state.promote_replica("p", 1).unwrap();
        assert_eq!(promoted.epoch, 2);
        assert_eq!(promoted.owner(Slot(42)).unwrap().id, "r");
        assert_eq!(promoted.owner(Slot(42)).unwrap().role, NodeRole::Primary);
    }

    #[test]
    fn migration_returns_target_until_finished() {
        let state = ClusterState::with_topology(2, Duration::from_secs(30), topology());
        assert!(state.begin_slot_migration(Slot(42), "r".into()));
        assert_eq!(state.migrating_target(Slot(42)).as_deref(), Some("r"));
        state.finish_slot_migration(Slot(42));
        assert!(state.migrating_target(Slot(42)).is_none());
    }

    #[test]
    fn stale_topology_is_rejected() {
        let state = ClusterState::with_topology(2, Duration::from_secs(30), topology());
        assert!(!state.replace_topology(topology()));
        let mut newer = topology();
        newer.epoch = 2;
        assert!(state.replace_topology(newer));
        assert!(!state.replace_topology(topology()));
    }

    #[test]
    fn migration_commit_moves_only_one_slot() {
        let state = ClusterState::with_topology(2, Duration::from_secs(30), topology());
        assert!(state.begin_slot_migration(Slot(42), "r".into()));
        let committed = state.commit_slot_migration(Slot(42)).unwrap();
        assert_eq!(committed.owner(Slot(42)).unwrap().id, "r");
        assert_eq!(committed.owner(Slot(41)).unwrap().id, "p");
    }
}

impl ClusterState {
    pub fn new(quorum: usize, retention: Duration) -> Self {
        Self {
            failures: Arc::new(Mutex::new(FailureTracker::new(quorum, retention))),
            topology: Arc::new(RwLock::new(Arc::new(super::Topology::default()))),
            migrations: Arc::new(Mutex::new(std::collections::HashMap::new())),
            imports: Arc::new(Mutex::new(std::collections::HashMap::new())),
            has_imports: Arc::new(AtomicBool::new(false)),
            has_migrations: Arc::new(AtomicBool::new(false)),
        }
    }

    pub fn with_topology(quorum: usize, retention: Duration, topology: super::Topology) -> Self {
        Self {
            failures: Arc::new(Mutex::new(FailureTracker::new(quorum, retention))),
            topology: Arc::new(RwLock::new(Arc::new(topology))),
            migrations: Arc::new(Mutex::new(std::collections::HashMap::new())),
            imports: Arc::new(Mutex::new(std::collections::HashMap::new())),
            has_imports: Arc::new(AtomicBool::new(false)),
            has_migrations: Arc::new(AtomicBool::new(false)),
        }
    }

    pub fn topology(&self) -> super::Topology {
        self.topology.read().unwrap().as_ref().clone()
    }

    pub fn topology_arc(&self) -> Arc<super::Topology> {
        Arc::clone(&*self.topology.read().unwrap())
    }

    pub fn begin_slot_migration(&self, slot: super::Slot, target: String) -> bool {
        if target.is_empty() || target.len() > 256 {
            return false;
        }
        let topology = self.topology();
        if topology.owner(slot).is_none() || !topology.nodes.iter().any(|node| node.id == target) {
            return false;
        }
        self.migrations.lock().unwrap().insert(slot.value(), target);
        self.has_migrations.store(true, Ordering::Release);
        true
    }

    pub fn finish_slot_migration(&self, slot: super::Slot) {
        let mut guard = self.migrations.lock().unwrap();
        guard.remove(&slot.value());
        if guard.is_empty() {
            self.has_migrations.store(false, Ordering::Release);
        }
    }

    pub fn commit_slot_migration(&self, slot: super::Slot) -> Option<super::Topology> {
        let target = {
            let mut guard = self.migrations.lock().unwrap();
            let t = guard.remove(&slot.value())?;
            if guard.is_empty() {
                self.has_migrations.store(false, Ordering::Release);
            }
            t
        };
        let mut topology = self.topology.write().unwrap().as_ref().clone();
        let source_index = topology.nodes.iter().position(|node| {
            node.role == super::NodeRole::Primary
                && node.slots.iter().any(|range| range.contains(slot))
        })?;
        let target_index = topology.nodes.iter().position(|node| node.id == target)?;
        remove_slot(&mut topology.nodes[source_index].slots, slot);
        topology.nodes[target_index].role = super::NodeRole::Primary;
        topology.nodes[target_index].replica_of = None;
        topology.nodes[target_index]
            .slots
            .push(super::SlotRange::new(slot, slot)?);
        topology.epoch = topology.epoch.saturating_add(1);
        topology.nodes[source_index].epoch = topology.epoch;
        topology.nodes[target_index].epoch = topology.epoch;
        *self.topology.write().unwrap() = Arc::new(topology.clone());
        Some(topology)
    }

    pub fn migrating_target(&self, slot: super::Slot) -> Option<String> {
        if !self.has_migrations.load(Ordering::Acquire) {
            return None;
        }
        self.migrations.lock().unwrap().get(&slot.value()).cloned()
    }

    pub fn begin_slot_import(&self, slot: super::Slot, source: String) -> bool {
        if source.is_empty() || source.len() > 256 {
            return false;
        }
        self.imports.lock().unwrap().insert(slot.value(), source);
        self.has_imports.store(true, Ordering::Release);
        true
    }

    pub fn finish_slot_import(&self, slot: super::Slot) {
        let mut guard = self.imports.lock().unwrap();
        guard.remove(&slot.value());
        if guard.is_empty() {
            self.has_imports.store(false, Ordering::Release);
        }
    }

    pub fn is_importing(&self, slot: super::Slot) -> bool {
        if !self.has_imports.load(Ordering::Acquire) {
            return false;
        }
        self.imports.lock().unwrap().contains_key(&slot.value())
    }

    pub fn import_source(&self, slot: super::Slot) -> Option<String> {
        if !self.has_imports.load(Ordering::Acquire) {
            return None;
        }
        self.imports.lock().unwrap().get(&slot.value()).cloned()
    }

    pub fn replace_topology(&self, topology: super::Topology) -> bool {
        let mut current = self.topology.write().unwrap();
        if topology.epoch <= current.epoch || !topology.is_valid() {
            return false;
        }
        *current = Arc::new(topology);
        true
    }

    /// Promote the best replica for a quorum-confirmed failed primary.
    /// The replica with the greatest node epoch wins; ties are deterministic.
    pub fn promote_replica(&self, failed_id: &str, epoch: u64) -> Option<super::Topology> {
        let failures = self.failures.lock().unwrap();
        if failures.report_count(failed_id, epoch) < failures.quorum() {
            return None;
        }
        drop(failures);
        let mut topology = self.topology.write().unwrap().as_ref().clone();
        if topology.epoch != epoch {
            return None;
        }
        let failed_index = topology
            .nodes
            .iter()
            .position(|node| node.id == failed_id)?;
        if topology.nodes[failed_index].role != super::NodeRole::Primary {
            return None;
        }
        let replica_index = topology
            .nodes
            .iter()
            .enumerate()
            .filter(|(_, node)| {
                node.role == super::NodeRole::Replica
                    && node.replica_of.as_deref() == Some(failed_id)
            })
            .max_by_key(|(_, node)| (node.epoch, std::cmp::Reverse(node.id.as_str())))
            .map(|(index, _)| index)?;
        let slots = topology.nodes[failed_index].slots.clone();
        let next_epoch = epoch.saturating_add(1);
        topology.nodes[failed_index].slots.clear();
        topology.nodes[failed_index].epoch = next_epoch;
        topology.nodes[replica_index].role = super::NodeRole::Primary;
        topology.nodes[replica_index].replica_of = None;
        topology.nodes[replica_index].slots = slots;
        topology.nodes[replica_index].epoch = next_epoch;
        topology.epoch = next_epoch;
        *self.topology.write().unwrap() = Arc::new(topology.clone());
        Some(topology)
    }

    pub fn record_failure(&self, report: FailureReport) -> bool {
        self.failures.lock().unwrap().report(report)
    }

    pub fn failure_report_count(&self, target_id: &str, epoch: u64) -> usize {
        self.failures.lock().unwrap().report_count(target_id, epoch)
    }

    /// Add a new node to the topology (CLUSTER MEET).
    /// Returns the updated topology on success, None if node already exists.
    pub fn meet_node(&self, node: super::NodeInfo) -> Option<super::Topology> {
        let mut topology = self.topology.write().unwrap().as_ref().clone();
        if topology.nodes.iter().any(|n| n.id == node.id) {
            return None;
        }
        topology.epoch = topology.epoch.saturating_add(1);
        topology.nodes.push(node);
        let new_topo = Arc::new(topology.clone());
        *self.topology.write().unwrap() = new_topo;
        Some(topology)
    }

    /// Assign slots to a node (CLUSTER ADDSLOTS).
    /// Returns updated topology, or None if node not found or slots already owned.
    pub fn add_slots(
        &self,
        node_id: &str,
        slots: &[super::Slot],
    ) -> Option<super::Topology> {
        let mut topology = self.topology.write().unwrap().as_ref().clone();
        let idx = topology.nodes.iter().position(|n| n.id == node_id)?;
        for &slot in slots {
            if topology
                .nodes
                .iter()
                .any(|n| n.slots.iter().any(|r| r.contains(slot)))
            {
                return None;
            }
            topology.nodes[idx]
                .slots
                .push(super::SlotRange::new(slot, slot).unwrap());
        }
        topology.nodes[idx].slots.sort_by_key(|r| r.start.value());
        compact_ranges(&mut topology.nodes[idx].slots);
        topology.epoch = topology.epoch.saturating_add(1);
        topology.nodes[idx].epoch = topology.epoch;
        *self.topology.write().unwrap() = Arc::new(topology.clone());
        Some(topology)
    }

    /// Remove slots from a node (CLUSTER DELSLOTS).
    pub fn del_slots(
        &self,
        node_id: &str,
        slots: &[super::Slot],
    ) -> Option<super::Topology> {
        let mut topology = self.topology.write().unwrap().as_ref().clone();
        let idx = topology.nodes.iter().position(|n| n.id == node_id)?;
        for &slot in slots {
            remove_slot(&mut topology.nodes[idx].slots, slot);
        }
        topology.epoch = topology.epoch.saturating_add(1);
        topology.nodes[idx].epoch = topology.epoch;
        *self.topology.write().unwrap() = Arc::new(topology.clone());
        Some(topology)
    }

    /// Remove a node from the topology (CLUSTER FORGET).
    pub fn forget_node(&self, node_id: &str) -> Option<super::Topology> {
        let mut topology = self.topology.write().unwrap().as_ref().clone();
        let idx = topology.nodes.iter().position(|n| n.id == node_id)?;
        topology.nodes.remove(idx);
        topology.epoch = topology.epoch.saturating_add(1);
        *self.topology.write().unwrap() = Arc::new(topology.clone());
        Some(topology)
    }

    /// Reset this node's slot assignments (CLUSTER RESET HARD/SOFT).
    pub fn reset_slots(&self, node_id: &str) -> super::Topology {
        let mut topology = self.topology.write().unwrap().as_ref().clone();
        if let Some(node) = topology.nodes.iter_mut().find(|n| n.id == node_id) {
            node.slots.clear();
        }
        topology.epoch = topology.epoch.saturating_add(1);
        *self.topology.write().unwrap() = Arc::new(topology.clone());
        topology
    }
}

fn remove_slot(ranges: &mut Vec<super::SlotRange>, slot: super::Slot) {
    let mut replacement = Vec::with_capacity(ranges.len().saturating_add(1));
    for range in ranges.drain(..) {
        if !range.contains(slot) {
            replacement.push(range);
            continue;
        }
        if range.start < slot {
            replacement
                .push(super::SlotRange::new(range.start, super::Slot(slot.value() - 1)).unwrap());
        }
        if slot < range.end {
            replacement
                .push(super::SlotRange::new(super::Slot(slot.value() + 1), range.end).unwrap());
        }
    }
    *ranges = replacement;
}

fn compact_ranges(ranges: &mut Vec<super::SlotRange>) {
    if ranges.len() < 2 {
        return;
    }
    let mut out: Vec<super::SlotRange> = Vec::with_capacity(ranges.len());
    for r in ranges.drain(..) {
        if let Some(last) = out.last_mut()
            && last.end.value() + 1 == r.start.value() {
                last.end = r.end;
                continue;
            }
        out.push(r);
    }
    *ranges = out;
}
