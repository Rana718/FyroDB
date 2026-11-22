use arc_swap::ArcSwap;
use parking_lot::{Mutex, RwLock};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

use super::{FailureReport, FailureTracker};

struct PublishedTopology {
   topology: Arc<super::Topology>,
   routing: Arc<super::RoutingTable>,
}

#[derive(Clone)]
pub struct ClusterState {
   failures: Arc<Mutex<FailureTracker>>,
   published: Arc<RwLock<PublishedTopology>>,
   version: Arc<AtomicU64>,
   /// Epoch of the installed topology, readable without the lock: every
   /// outbound bus frame is stamped with it.
   epoch: Arc<AtomicU64>,
   migrations: Arc<ArcSwap<std::collections::HashMap<u16, String>>>,
   imports: Arc<ArcSwap<std::collections::HashMap<u16, String>>>,
   has_imports: Arc<AtomicBool>,
   has_migrations: Arc<AtomicBool>,
}

impl ClusterState {
   pub fn new(quorum: usize, retention: Duration) -> Self {
      Self::build(quorum, retention, super::Topology::default())
   }

   pub fn with_topology(quorum: usize, retention: Duration, topology: super::Topology) -> Self {
      Self::build(quorum, retention, topology)
   }

   fn build(quorum: usize, retention: Duration, topology: super::Topology) -> Self {
      let routing = super::RoutingTable::build(&topology);
      let epoch = topology.epoch;
      Self {
         failures: Arc::new(Mutex::new(FailureTracker::new(quorum, retention))),
         published: Arc::new(RwLock::new(PublishedTopology {
            topology: Arc::new(topology),
            routing: Arc::new(routing),
         })),
         version: Arc::new(AtomicU64::new(1)),
         epoch: Arc::new(AtomicU64::new(epoch)),
         migrations: Arc::new(ArcSwap::default()),
         imports: Arc::new(ArcSwap::default()),
         has_imports: Arc::new(AtomicBool::new(false)),
         has_migrations: Arc::new(AtomicBool::new(false)),
      }
   }

   pub fn topology(&self) -> super::Topology {
      self.topology_arc().as_ref().clone()
   }

   pub fn topology_arc(&self) -> Arc<super::Topology> {
      Arc::clone(&self.published.read().topology)
   }

   pub fn topology_and_routing_arc(&self) -> (Arc<super::Topology>, Arc<super::RoutingTable>) {
      let snapshot = self.published.read();
      (
         Arc::clone(&snapshot.topology),
         Arc::clone(&snapshot.routing),
      )
   }

   /// Epoch of the currently installed topology.
   #[inline]
   pub fn epoch(&self) -> u64 {
      self.epoch.load(Ordering::Acquire)
   }

   /// Version of the currently installed topology.
   #[inline(always)]
   pub fn topology_version(&self) -> u64 {
      self.version.load(Ordering::Acquire)
   }

   /// Version is checked before the lock: steady state is one Acquire load.
   /// The table rides along, so generations cannot be mixed.
   pub fn topology_if_newer(
      &self,
      cached: u64,
   ) -> Option<(u64, Arc<super::Topology>, Arc<super::RoutingTable>)> {
      if self.version.load(Ordering::Acquire) == cached {
         return None;
      }
      let snapshot = self.published.read();
      Some((
         self.topology_version(),
         Arc::clone(&snapshot.topology),
         Arc::clone(&snapshot.routing),
      ))
   }

   fn install_topology(&self, snapshot: &mut PublishedTopology, topology: Arc<super::Topology>) {
      snapshot.routing = Arc::new(super::RoutingTable::build(&topology));
      self.epoch.store(topology.epoch, Ordering::Release);
      snapshot.topology = topology;
      self.version.fetch_add(1, Ordering::Release);
   }

   pub fn begin_slot_migration(&self, slot: super::Slot, target: String) -> bool {
      if target.is_empty() || target.len() > 256 {
         return false;
      }
      let snapshot = self.published.write();
      let topology = &snapshot.topology;
      if topology.owner(slot).is_none() || !topology.nodes.iter().any(|node| node.id == target) {
         return false;
      }
      let mut map = self.migrations.load_full();
      Arc::make_mut(&mut map).insert(slot.value(), target);
      self.migrations.store(map);
      self.has_migrations.store(true, Ordering::Release);
      true
   }

   pub fn finish_slot_migration(&self, slot: super::Slot) {
      let _update = self.published.write();
      self.clear_migration(slot);
   }

   fn clear_migration(&self, slot: super::Slot) {
      let mut map = self.migrations.load_full();
      let map_mut = Arc::make_mut(&mut map);
      map_mut.remove(&slot.value());
      let empty = map_mut.is_empty();
      self.migrations.store(map);
      if empty {
         self.has_migrations.store(false, Ordering::Release);
      }
   }

   pub fn commit_slot_migration(&self, slot: super::Slot) -> Option<super::Topology> {
      let mut snapshot = self.published.write();
      let target = self.migrations.load().get(&slot.value()).cloned()?;
      let mut topology = snapshot.topology.as_ref().clone();
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
      self.install_topology(&mut snapshot, Arc::new(topology.clone()));
      self.clear_migration(slot);
      Some(topology)
   }

   pub fn migrating_target(&self, slot: super::Slot) -> Option<String> {
      if !self.has_migrations.load(Ordering::Acquire) {
         return None;
      }
      self.migrations.load().get(&slot.value()).cloned()
   }

   pub fn begin_slot_import(&self, slot: super::Slot, source: String) -> bool {
      if source.is_empty() || source.len() > 256 {
         return false;
      }
      let _update = self.published.write();
      let mut map = self.imports.load_full();
      Arc::make_mut(&mut map).insert(slot.value(), source);
      self.imports.store(map);
      self.has_imports.store(true, Ordering::Release);
      true
   }

   pub fn finish_slot_import(&self, slot: super::Slot) {
      let _update = self.published.write();
      let mut map = self.imports.load_full();
      let map_mut = Arc::make_mut(&mut map);
      map_mut.remove(&slot.value());
      let empty = map_mut.is_empty();
      self.imports.store(map);
      if empty {
         self.has_imports.store(false, Ordering::Release);
      }
   }

   pub fn is_importing(&self, slot: super::Slot) -> bool {
      if !self.has_imports.load(Ordering::Acquire) {
         return false;
      }
      self.imports.load().contains_key(&slot.value())
   }

   pub fn import_source(&self, slot: super::Slot) -> Option<String> {
      if !self.has_imports.load(Ordering::Acquire) {
         return None;
      }
      self.imports.load().get(&slot.value()).cloned()
   }

   /// Installs a topology learned from a peer or from disk. A newer epoch
   /// replaces ours; a different table at the same epoch means two nodes
   /// changed it concurrently, so both versions are merged (see
   /// `Topology::merge_concurrent`) instead of leaving the nodes split.
   /// Returns whether the installed topology changed.
   pub fn replace_topology(&self, topology: super::Topology) -> bool {
      if !topology.is_valid() {
         return false;
      }
      let mut snapshot = self.published.write();
      let current = snapshot.topology.as_ref();
      let next = if topology.epoch > current.epoch {
         topology
      } else if topology.epoch == current.epoch && topology.nodes != current.nodes {
         let merged = super::Topology::merge_concurrent(current, &topology);
         if !merged.is_valid() {
            return false;
         }
         merged
      } else {
         return false;
      };
      self.install_topology(&mut snapshot, Arc::new(next));
      true
   }

   /// Promote the best replica for a quorum-confirmed failed primary.
   /// The replica with the greatest node epoch wins; ties are deterministic.
   pub fn promote_replica(&self, failed_id: &str, epoch: u64) -> Option<super::Topology> {
      let failures = self.failures.lock();
      if failures.report_count(failed_id, epoch) < failures.quorum() {
         return None;
      }
      drop(failures);
      let mut snapshot = self.published.write();
      let mut topology = snapshot.topology.as_ref().clone();
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
            node.role == super::NodeRole::Replica && node.replica_of.as_deref() == Some(failed_id)
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
      self.install_topology(&mut snapshot, Arc::new(topology.clone()));
      Some(topology)
   }

   pub fn record_failure(&self, report: FailureReport) -> bool {
      self.failures.lock().report(report)
   }

   pub fn failure_report_count(&self, target_id: &str, epoch: u64) -> usize {
      self.failures.lock().report_count(target_id, epoch)
   }

   /// SETSLOT NODE must also land on non-source nodes; the old add_slots
   /// fallback caused MOVED loops. None if target unknown or already owner.
   pub fn assign_slot(&self, slot: super::Slot, target: &str) -> Option<super::Topology> {
      let mut snapshot = self.published.write();
      let mut topology = snapshot.topology.as_ref().clone();
      let target_index = topology.nodes.iter().position(|node| node.id == target)?;
      if topology.nodes[target_index]
         .slots
         .iter()
         .any(|range| range.contains(slot))
      {
         return None;
      }

      for node in topology.nodes.iter_mut() {
         if node.slots.iter().any(|range| range.contains(slot)) {
            remove_slot(&mut node.slots, slot);
         }
      }

      topology.epoch = topology.epoch.saturating_add(1);
      let epoch = topology.epoch;
      let target_node = &mut topology.nodes[target_index];
      target_node.role = super::NodeRole::Primary;
      target_node.replica_of = None;
      target_node.slots.push(super::SlotRange::new(slot, slot)?);
      target_node.slots.sort_by_key(|range| range.start.value());
      compact_ranges(&mut target_node.slots);
      target_node.epoch = epoch;
      self.install_topology(&mut snapshot, Arc::new(topology.clone()));
      Some(topology)
   }

   /// Add a new node to the topology (CLUSTER MEET).
   /// Returns the updated topology on success, None if node already exists.
   pub fn meet_node(&self, node: super::NodeInfo) -> Option<super::Topology> {
      let mut snapshot = self.published.write();
      let mut topology = snapshot.topology.as_ref().clone();
      if topology.nodes.iter().any(|n| n.id == node.id) {
         return None;
      }
      topology.epoch = topology.epoch.saturating_add(1);
      let mut node = node;
      // Stamped with the change's epoch, so a concurrent merge keeps it.
      node.epoch = topology.epoch;
      topology.nodes.push(node);
      let new_topo = Arc::new(topology.clone());
      self.install_topology(&mut snapshot, new_topo);
      Some(topology)
   }

   /// Assign slots to a node (CLUSTER ADDSLOTS).
   /// Returns updated topology, or None if node not found or slots already owned.
   pub fn add_slots(&self, node_id: &str, slots: &[super::Slot]) -> Option<super::Topology> {
      let mut snapshot = self.published.write();
      let mut topology = snapshot.topology.as_ref().clone();
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
      self.install_topology(&mut snapshot, Arc::new(topology.clone()));
      Some(topology)
   }

   /// Remove slots from a node (CLUSTER DELSLOTS).
   pub fn del_slots(&self, node_id: &str, slots: &[super::Slot]) -> Option<super::Topology> {
      let mut snapshot = self.published.write();
      let mut topology = snapshot.topology.as_ref().clone();
      let idx = topology.nodes.iter().position(|n| n.id == node_id)?;
      for &slot in slots {
         remove_slot(&mut topology.nodes[idx].slots, slot);
      }
      topology.epoch = topology.epoch.saturating_add(1);
      topology.nodes[idx].epoch = topology.epoch;
      self.install_topology(&mut snapshot, Arc::new(topology.clone()));
      Some(topology)
   }

   /// Remove a node from the topology (CLUSTER FORGET).
   pub fn forget_node(&self, node_id: &str) -> Option<super::Topology> {
      let mut snapshot = self.published.write();
      let mut topology = snapshot.topology.as_ref().clone();
      let idx = topology.nodes.iter().position(|n| n.id == node_id)?;
      topology.nodes.remove(idx);
      topology.epoch = topology.epoch.saturating_add(1);
      self.install_topology(&mut snapshot, Arc::new(topology.clone()));
      Some(topology)
   }

   /// Reset this node's slot assignments (CLUSTER RESET HARD/SOFT).
   pub fn reset_slots(&self, node_id: &str) -> super::Topology {
      let mut snapshot = self.published.write();
      let mut topology = snapshot.topology.as_ref().clone();
      topology.epoch = topology.epoch.saturating_add(1);
      let epoch = topology.epoch;
      if let Some(node) = topology.nodes.iter_mut().find(|n| n.id == node_id) {
         node.slots.clear();
         // Stamped so a concurrent merge prefers this (empty) entry over an
         // older one that still lists the released slots.
         node.epoch = epoch;
      }
      self.install_topology(&mut snapshot, Arc::new(topology.clone()));
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
         replacement.push(super::SlotRange::new(super::Slot(slot.value() + 1), range.end).unwrap());
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
         && last.end.value() + 1 == r.start.value()
      {
         last.end = r.end;
         continue;
      }
      out.push(r);
   }
   *ranges = out;
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
   fn concurrent_slot_assignments_publish_matching_owner_tables() {
      let state = ClusterState::with_topology(2, Duration::from_secs(30), topology());
      std::thread::scope(|scope| {
         for start in [0, 32] {
            let state = &state;
            scope.spawn(move || {
               for n in start..start + 32 {
                  assert!(state.assign_slot(Slot(n), "r").is_some());
                  let (_, topology, routing) = state.topology_if_newer(0).unwrap();
                  for slot in [Slot(0), Slot(32), Slot(n)] {
                     let owner = routing.owner_index(slot).unwrap();
                     assert_eq!(topology.nodes[owner].id, topology.owner(slot).unwrap().id);
                  }
               }
            });
         }
      });
      let topology = state.topology();
      assert!((0..64).all(|n| topology.owner(Slot(n)).unwrap().id == "r"));
   }

   #[test]
   fn removing_and_restoring_slot_boundaries_preserves_the_live_route_table() {
      for cut in [0, 1, 8192, 16382, 16383] {
         let state = ClusterState::with_topology(2, Duration::from_secs(30), topology());
         for target in ["r", "p"] {
            state.assign_slot(Slot(cut), target).unwrap();
            let (topology, routing) = state.topology_and_routing_arc();
            for key_slot in 0..crate::cluster::HASH_SLOTS {
               let slot = Slot(key_slot);
               let id = if key_slot == cut { target } else { "p" };
               assert_eq!(topology.owner(slot).unwrap().id, id);
               assert_eq!(topology.nodes[routing.owner_index(slot).unwrap()].id, id);
            }
         }
         assert_eq!(
            state.topology().nodes[0].slots,
            [SlotRange::new(Slot(0), Slot(16383)).unwrap()]
         );
      }
   }

   #[test]
   fn a_concurrent_change_at_the_same_epoch_is_merged_not_ignored() {
      let node = |id: &str, ranges: &[(u16, u16)]| NodeInfo {
         id: id.into(),
         address: format!("{id}:8000"),
         cluster_address: format!("{id}:18000"),
         role: NodeRole::Primary,
         replica_of: None,
         epoch: 1,
         slots: ranges
            .iter()
            .map(|&(start, end)| SlotRange::new(Slot(start), Slot(end)).unwrap())
            .collect(),
      };
      let base = Topology::new(
         1,
         vec![node("p", &[(0, 16383)]), node("q", &[]), node("r", &[])],
      );
      let state = ClusterState::with_topology(2, Duration::from_secs(30), base.clone());
      // This node hands slot 100 to q while another node hands slot 7 to r.
      let local = state.assign_slot(Slot(100), "q").unwrap();
      let remote = ClusterState::with_topology(2, Duration::from_secs(30), base)
         .assign_slot(Slot(7), "r")
         .unwrap();
      assert_eq!(local.epoch, remote.epoch);

      assert!(
         state.replace_topology(remote.clone()),
         "same epoch, different table"
      );
      let merged = state.topology();
      assert_eq!(merged.epoch, local.epoch + 1);
      assert_eq!(state.epoch(), merged.epoch);
      assert_eq!(merged.owner(Slot(100)).unwrap().id, "q");
      assert_eq!(merged.owner(Slot(7)).unwrap().id, "r");
      assert_eq!(merged.owner(Slot(8)).unwrap().id, "p");
      assert!(merged.is_complete());
      assert!(
         !state.replace_topology(merged.clone()),
         "re-delivery is a no-op"
      );

      // The other node merges the same pair and lands on the same table.
      let other = ClusterState::with_topology(2, Duration::from_secs(30), remote);
      assert!(other.replace_topology(local));
      assert_eq!(other.topology(), merged);
   }

   fn three_primaries() -> Topology {
      let node = |id: &str, ranges: &[(u16, u16)]| NodeInfo {
         id: id.into(),
         address: format!("{id}:8000"),
         cluster_address: format!("{id}:18000"),
         role: NodeRole::Primary,
         replica_of: None,
         epoch: 1,
         slots: ranges
            .iter()
            .map(|&(start, end)| SlotRange::new(Slot(start), Slot(end)).unwrap())
            .collect(),
      };
      Topology::new(
         1,
         vec![
            node("p", &[(0, 9999)]),
            node("q", &[(10000, 16000)]),
            node("x", &[]),
         ],
      )
   }

   /// Applies `left` on one node and `right` on another from the same start,
   /// then lets the first node learn the second's version.
   fn race(left: impl Fn(&ClusterState), right: impl Fn(&ClusterState)) -> Topology {
      let a = ClusterState::with_topology(2, Duration::from_secs(30), three_primaries());
      let b = ClusterState::with_topology(2, Duration::from_secs(30), three_primaries());
      left(&a);
      right(&b);
      assert_eq!(
         a.topology().epoch,
         b.topology().epoch,
         "a concurrent change"
      );
      assert!(a.replace_topology(b.topology()));
      // Order independence: merging the other way gives the same table.
      let c = ClusterState::with_topology(2, Duration::from_secs(30), three_primaries());
      right(&c);
      let mut left_view = three_primaries();
      let d = ClusterState::with_topology(2, Duration::from_secs(30), left_view.clone());
      left(&d);
      left_view = d.topology();
      assert!(c.replace_topology(left_view));
      assert_eq!(a.topology(), c.topology());
      a.topology()
   }

   #[test]
   fn a_concurrent_merge_keeps_a_forget() {
      let merged = race(
         |s| assert!(s.forget_node("x").is_some()),
         |s| assert!(s.add_slots("q", &[Slot(16001)]).is_some()),
      );
      assert!(
         !merged.nodes.iter().any(|n| n.id == "x"),
         "forgotten node came back"
      );
      assert_eq!(merged.owner(Slot(16001)).unwrap().id, "q");
   }

   #[test]
   fn a_concurrent_merge_keeps_a_meet_and_a_delslots() {
      let newcomer = NodeInfo {
         id: "y".into(),
         address: "y:8000".into(),
         cluster_address: "y:18000".into(),
         role: NodeRole::Primary,
         replica_of: None,
         epoch: 0,
         slots: Vec::new(),
      };
      let merged = race(
         |s| assert!(s.meet_node(newcomer.clone()).is_some()),
         |s| assert!(s.del_slots("p", &[Slot(5)]).is_some()),
      );
      assert!(
         merged.nodes.iter().any(|n| n.id == "y"),
         "a node met concurrently is kept"
      );
      assert!(merged.owner(Slot(5)).is_none(), "released slot came back");
      assert_eq!(merged.owner(Slot(6)).unwrap().id, "p");
   }

   #[test]
   fn a_concurrent_merge_keeps_a_reset() {
      let merged = race(
         |s| {
            s.reset_slots("q");
         },
         |s| assert!(s.add_slots("x", &[Slot(16100)]).is_some()),
      );
      assert!(
         merged.owner(Slot(10000)).is_none(),
         "reset node's slots came back"
      );
      assert_eq!(merged.owner(Slot(16100)).unwrap().id, "x");
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

   /// A missed bump pins a connection to a stale topology.
   #[test]
   fn every_topology_install_advances_the_version() {
      let state = ClusterState::with_topology(2, Duration::from_secs(30), topology());
      let mut seen = state.topology_version();

      let mut expect_bump = |label: &str, state: &ClusterState| {
         let now = state.topology_version();
         assert!(now > seen, "{label} did not advance the topology version");
         seen = now;
      };

      let mut newer = topology();
      newer.epoch = 5;
      assert!(state.replace_topology(newer));
      expect_bump("replace_topology", &state);

      assert!(state.begin_slot_migration(Slot(42), "r".into()));
      assert!(state.commit_slot_migration(Slot(42)).is_some());
      expect_bump("commit_slot_migration", &state);

      assert!(state.del_slots("p", &[Slot(7)]).is_some());
      expect_bump("del_slots", &state);

      assert!(state.add_slots("p", &[Slot(7)]).is_some());
      expect_bump("add_slots", &state);

      assert!(
         state
            .meet_node(NodeInfo {
               id: "n".into(),
               address: "n:8000".into(),
               cluster_address: "n:18000".into(),
               role: NodeRole::Primary,
               replica_of: None,
               epoch: 1,
               slots: vec![],
            })
            .is_some()
      );
      expect_bump("meet_node", &state);

      assert!(state.forget_node("n").is_some());
      expect_bump("forget_node", &state);

      state.reset_slots("p");
      expect_bump("reset_slots", &state);
   }

   /// Old add_slots fallback refused still-owned slots: MOVED loops.
   #[test]
   fn assign_slot_takes_the_slot_from_its_current_owner() {
      let state = ClusterState::with_topology(2, Duration::from_secs(30), topology());
      let before_version = state.topology_version();
      let before_epoch = state.topology().epoch;

      let moved = state
         .assign_slot(Slot(6399), "r")
         .expect("target is a known node that does not own the slot");

      assert_eq!(moved.owner(Slot(6399)).unwrap().id, "r");
      assert_eq!(moved.owner(Slot(6398)).unwrap().id, "p");
      assert_eq!(moved.owner(Slot(6400)).unwrap().id, "p");
      assert_eq!(
         moved.nodes.iter().find(|n| n.id == "r").unwrap().role,
         NodeRole::Primary
      );
      assert!(moved.epoch > before_epoch);
      assert!(state.topology_version() > before_version);

      // Idempotent: re-applying it is a no-op rather than a duplicate range.
      assert!(state.assign_slot(Slot(6399), "r").is_none());
      assert!(state.assign_slot(Slot(6399), "does-not-exist").is_none());
   }

   #[test]
   fn assign_slot_leaves_the_rest_of_the_keyspace_owned() {
      let state = ClusterState::with_topology(2, Duration::from_secs(30), topology());
      assert!(state.assign_slot(Slot(0), "r").is_some());
      let after = state.topology();
      for slot in [1u16, 500, 6399, 16383] {
         assert!(
            after.owner(Slot(slot)).is_some(),
            "slot {slot} lost its owner"
         );
      }
      assert_eq!(after.owner(Slot(0)).unwrap().id, "r");
   }

   #[test]
   fn a_rejected_topology_leaves_the_version_alone() {
      let state = ClusterState::with_topology(2, Duration::from_secs(30), topology());
      let before = state.topology_version();
      assert!(!state.replace_topology(topology()), "stale epoch");
      assert_eq!(state.topology_version(), before);
   }

   #[test]
   fn cached_readers_only_refetch_after_a_change() {
      let state = ClusterState::with_topology(2, Duration::from_secs(30), topology());
      let (version, snapshot, routing) = state
         .topology_if_newer(0)
         .expect("first fetch always returns a snapshot");
      assert!(state.topology_if_newer(version).is_none());

      let mut newer = topology();
      newer.epoch = 9;
      assert!(state.replace_topology(newer));

      let (next_version, next_snapshot, next_routing) = state
         .topology_if_newer(version)
         .expect("an install must invalidate the cache");
      assert!(next_version > version);
      assert_eq!(snapshot.epoch, 1);
      assert_eq!(next_snapshot.epoch, 9);

      // The table must describe the topology it was handed out with, or a
      // reader would index the wrong generation's node list.
      for (topology, table) in [(&snapshot, &routing), (&next_snapshot, &next_routing)] {
         for raw in [0u16, 42, 5461, 16383] {
            let slot = Slot(raw);
            assert_eq!(
               topology.owner(slot).map(|node| node.id.as_str()),
               table
                  .owner_index(slot)
                  .map(|index| topology.nodes[index].id.as_str())
            );
         }
      }
   }
}
