use super::{HASH_SLOTS, Slot, SlotRange};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NodeRole {
   Primary,
   Replica,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeInfo {
   pub id: String,
   pub address: String,
   pub cluster_address: String,
   pub role: NodeRole,
   pub replica_of: Option<String>,
   pub epoch: u64,
   pub slots: Vec<SlotRange>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Topology {
   pub epoch: u64,
   pub nodes: Vec<NodeInfo>,
}

impl Topology {
   pub fn new(epoch: u64, nodes: Vec<NodeInfo>) -> Self {
      Self { epoch, nodes }
   }

   pub fn owner(&self, slot: Slot) -> Option<&NodeInfo> {
      self
         .nodes
         .iter()
         .filter(|node| node.role == NodeRole::Primary)
         .find(|node| node.slots.iter().copied().any(|range| range.contains(slot)))
   }

   pub fn is_complete(&self) -> bool {
      // One bit per Redis hash slot: 16,384 bits = 2 KiB on the stack.
      // Cluster INFO can call this frequently, so avoid a heap allocation.
      let mut covered = [0u64; HASH_SLOTS as usize / 64];
      let mut count = 0usize;
      for node in self
         .nodes
         .iter()
         .filter(|node| node.role == NodeRole::Primary)
      {
         for range in &node.slots {
            for slot in range.start.0..=range.end.0 {
               let index = slot as usize;
               let word = index / 64;
               let bit = 1u64 << (index % 64);
               if covered[word] & bit != 0 {
                  return false;
               }
               covered[word] |= bit;
               count += 1;
            }
         }
      }
      count == HASH_SLOTS as usize
   }

   /// Reconciles two different topologies that claim the same epoch, which
   /// happens when two nodes apply a change concurrently. The result does
   /// not depend on argument order, so every node that sees both versions
   /// converges on the same table one epoch later.
   ///
   /// Per node id the entry with the higher (node epoch, content hash) wins;
   /// a slot claimed by two primaries goes to the higher-ranked claimant,
   /// mirroring how Redis settles slot claims by config epoch.
   pub fn merge_concurrent(a: &Topology, b: &Topology) -> Topology {
      // A node in only one view either joined there during the concurrent
      // change (MEET stamps it with that epoch) or predates it and was
      // removed in the other view (FORGET). Only the former survives, so a
      // concurrent change elsewhere cannot bring a forgotten node back.
      let changed_at = a.epoch.min(b.epoch);
      let in_both =
         |id: &str| a.nodes.iter().any(|n| n.id == id) && b.nodes.iter().any(|n| n.id == id);
      let mut nodes: Vec<NodeInfo> = Vec::with_capacity(a.nodes.len().max(b.nodes.len()));
      for node in a.nodes.iter().chain(&b.nodes) {
         if node.epoch < changed_at && !in_both(&node.id) {
            continue;
         }
         match nodes.iter_mut().find(|existing| existing.id == node.id) {
            Some(existing) => {
               if node_rank(node) > node_rank(existing) {
                  *existing = node.clone();
               }
            }
            None => nodes.push(node.clone()),
         }
      }

      let mut order: Vec<usize> = (0..nodes.len()).collect();
      order.sort_by_key(|&index| std::cmp::Reverse(node_rank(&nodes[index])));
      let mut claimed = [0u64; HASH_SLOTS as usize / 64];
      for index in order {
         let node = &mut nodes[index];
         if node.role != NodeRole::Primary {
            continue;
         }
         let mut kept: Vec<SlotRange> = Vec::with_capacity(node.slots.len());
         for range in &node.slots {
            let mut run: Option<(u16, u16)> = None;
            for slot in range.start.0..=range.end.0.min(HASH_SLOTS - 1) {
               let (word, bit) = (slot as usize / 64, 1u64 << (slot % 64));
               if claimed[word] & bit == 0 {
                  claimed[word] |= bit;
                  run = match run {
                     Some((start, end)) if end + 1 == slot => Some((start, slot)),
                     Some((start, end)) => {
                        kept.extend(SlotRange::new(Slot(start), Slot(end)));
                        Some((slot, slot))
                     }
                     None => Some((slot, slot)),
                  };
               }
            }
            if let Some((start, end)) = run {
               kept.extend(SlotRange::new(Slot(start), Slot(end)));
            }
         }
         node.slots = kept;
      }

      nodes.sort_by(|x, y| x.id.cmp(&y.id));
      Topology::new(a.epoch.max(b.epoch).saturating_add(1), nodes)
   }

   pub fn is_valid(&self) -> bool {
      self.nodes.iter().all(|node| {
         node.id.len() <= 256
            && node.address.len() <= 512
            && node.cluster_address.len() <= 512
            && node.slots.iter().all(|range| range.start <= range.end)
      }) && {
         let mut seen = std::collections::HashSet::new();
         self.nodes.iter().all(|node| seen.insert(node.id.as_str()))
      }
   }
}

/// Total order used to settle concurrent claims: node epoch first, then a
/// stable content hash so equal epochs still break the tie identically on
/// every node (and across builds, unlike `DefaultHasher`).
fn node_rank(node: &NodeInfo) -> (u64, u64) {
   let mut hash = 0xcbf2_9ce4_8422_2325u64;
   let mut feed = |bytes: &[u8]| {
      for &byte in bytes.iter().chain(&[0xff]) {
         hash = (hash ^ u64::from(byte)).wrapping_mul(0x1000_0000_01b3);
      }
   };
   feed(node.id.as_bytes());
   feed(node.address.as_bytes());
   feed(node.cluster_address.as_bytes());
   feed(&[node.role as u8]);
   feed(node.replica_of.as_deref().unwrap_or("").as_bytes());
   for range in &node.slots {
      feed(&range.start.0.to_be_bytes());
      feed(&range.end.0.to_be_bytes());
   }
   (node.epoch, hash)
}

#[cfg(test)]
mod tests {
   use super::*;

   fn primary(id: &str, epoch: u64, ranges: &[(u16, u16)]) -> NodeInfo {
      NodeInfo {
         id: id.into(),
         address: format!("{id}:8000"),
         cluster_address: format!("{id}:18000"),
         role: NodeRole::Primary,
         replica_of: None,
         epoch,
         slots: ranges
            .iter()
            .map(|&(start, end)| SlotRange::new(Slot(start), Slot(end)).unwrap())
            .collect(),
      }
   }

   #[test]
   fn concurrent_slot_additions_on_different_nodes_both_survive_a_merge() {
      // a and b each ran ADDSLOTS for their own half from the same epoch 1.
      let on_a = Topology::new(2, vec![primary("a", 2, &[(0, 8191)]), primary("b", 1, &[])]);
      let on_b = Topology::new(
         2,
         vec![primary("a", 1, &[]), primary("b", 2, &[(8192, 16383)])],
      );
      let merged = Topology::merge_concurrent(&on_a, &on_b);
      assert_eq!(
         merged,
         Topology::merge_concurrent(&on_b, &on_a),
         "merge is order independent"
      );
      assert_eq!(merged.epoch, 3);
      assert!(merged.is_complete());
      assert_eq!(merged.owner(Slot(10)).unwrap().id, "a");
      assert_eq!(merged.owner(Slot(9000)).unwrap().id, "b");
   }

   #[test]
   fn a_doubly_claimed_slot_goes_to_the_higher_epoch_claimant() {
      // a still holds 0-100 in one view; b was assigned slot 50 later (epoch 3).
      let old = Topology::new(3, vec![primary("a", 1, &[(0, 100)]), primary("b", 1, &[])]);
      let new = Topology::new(
         3,
         vec![primary("a", 1, &[(0, 100)]), primary("b", 3, &[(50, 50)])],
      );
      let merged = Topology::merge_concurrent(&old, &new);
      assert_eq!(merged.owner(Slot(50)).unwrap().id, "b");
      assert_eq!(merged.owner(Slot(49)).unwrap().id, "a");
      assert_eq!(merged.owner(Slot(51)).unwrap().id, "a");
      let a = merged.nodes.iter().find(|n| n.id == "a").unwrap();
      assert_eq!(
         a.slots.len(),
         2,
         "a's range is split around the reassigned slot"
      );
      assert_eq!(merged, Topology::merge_concurrent(&new, &old));
   }

   #[test]
   fn resolves_primary_owner_and_complete_coverage() {
      let node = |id: &str, start, end| NodeInfo {
         id: id.into(),
         address: format!("{id}:8000"),
         cluster_address: format!("{id}:18000"),
         role: NodeRole::Primary,
         replica_of: None,
         epoch: 1,
         slots: vec![SlotRange::new(Slot(start), Slot(end)).unwrap()],
      };
      let topology = Topology::new(1, vec![node("a", 0, 8191), node("b", 8192, 16383)]);
      assert_eq!(topology.owner(Slot(9000)).unwrap().id, "b");
      assert!(topology.is_complete());
   }
}

/// Redis-style flat slots[16384] (32 KiB) shared via Arc: owner lookups
/// do not scan range lists on the command path.
pub struct RoutingTable {
   /// Index into `Topology::nodes`, or `UNASSIGNED`.
   owners: Box<[u16]>,
}

/// Sentinel for a slot no primary claims.
const UNASSIGNED: u16 = u16::MAX;

impl RoutingTable {
   pub fn build(topology: &Topology) -> Self {
      let mut owners = vec![UNASSIGNED; HASH_SLOTS as usize];
      let last = owners.len() - 1;
      for (index, node) in topology.nodes.iter().enumerate() {
         if node.role != NodeRole::Primary {
            continue;
         }
         // A node index that cannot be represented would silently alias
         // another node; leave those slots unassigned instead.
         let Ok(encoded) = u16::try_from(index) else {
            continue;
         };
         if encoded == UNASSIGNED {
            continue;
         }
         for range in &node.slots {
            let start = (range.start.value() as usize).min(last);
            let end = (range.end.value() as usize).min(last);
            for slot in &mut owners[start..=end] {
               // First primary wins, matching `Topology::owner`'s `find`.
               if *slot == UNASSIGNED {
                  *slot = encoded;
               }
            }
         }
      }
      Self {
         owners: owners.into_boxed_slice(),
      }
   }

   /// Index into `Topology::nodes` for the primary serving `slot`.
   #[inline(always)]
   pub fn owner_index(&self, slot: Slot) -> Option<usize> {
      match self.owners.get(slot.value() as usize).copied() {
         Some(UNASSIGNED) | None => None,
         Some(index) => Some(index as usize),
      }
   }
}

impl Default for RoutingTable {
   fn default() -> Self {
      Self::build(&Topology::default())
   }
}

#[cfg(test)]
mod routing_table_tests {
   use super::*;

   fn topology() -> Topology {
      Topology::new(
         1,
         vec![
            NodeInfo {
               id: "a".into(),
               address: "a:8000".into(),
               cluster_address: "a:18000".into(),
               role: NodeRole::Primary,
               replica_of: None,
               epoch: 1,
               slots: vec![SlotRange::new(Slot(0), Slot(5461)).unwrap()],
            },
            NodeInfo {
               id: "r".into(),
               address: "r:8000".into(),
               cluster_address: "r:18000".into(),
               role: NodeRole::Replica,
               replica_of: Some("a".into()),
               epoch: 1,
               slots: vec![SlotRange::new(Slot(0), Slot(5461)).unwrap()],
            },
            NodeInfo {
               id: "b".into(),
               address: "b:8000".into(),
               cluster_address: "b:18000".into(),
               role: NodeRole::Primary,
               replica_of: None,
               epoch: 1,
               slots: vec![SlotRange::new(Slot(5462), Slot(16383)).unwrap()],
            },
         ],
      )
   }

   /// The table must agree with `Topology::owner` on every slot, since it
   /// replaces it on the command path.
   #[test]
   fn table_matches_the_scanning_lookup_for_every_slot() {
      let topology = topology();
      let table = RoutingTable::build(&topology);
      for raw in 0..HASH_SLOTS {
         let slot = Slot(raw);
         let scanned = topology.owner(slot).map(|node| node.id.as_str());
         let looked_up = table
            .owner_index(slot)
            .map(|index| topology.nodes[index].id.as_str());
         assert_eq!(scanned, looked_up, "slot {raw}");
      }
   }

   #[test]
   fn replicas_never_own_a_slot() {
      let topology = topology();
      let table = RoutingTable::build(&topology);
      for raw in 0..HASH_SLOTS {
         if let Some(index) = table.owner_index(Slot(raw)) {
            assert_eq!(topology.nodes[index].role, NodeRole::Primary, "slot {raw}");
         }
      }
   }

   #[test]
   fn unassigned_slots_have_no_owner() {
      let mut topology = topology();
      topology.nodes[2].slots = vec![SlotRange::new(Slot(5462), Slot(9000)).unwrap()];
      let table = RoutingTable::build(&topology);
      assert!(table.owner_index(Slot(9000)).is_some());
      assert!(table.owner_index(Slot(9001)).is_none());
      assert!(table.owner_index(Slot(HASH_SLOTS - 1)).is_none());
   }

   #[test]
   fn an_empty_topology_owns_nothing() {
      let table = RoutingTable::default();
      for raw in [0u16, 1, 8000, HASH_SLOTS - 1] {
         assert!(table.owner_index(Slot(raw)).is_none());
      }
   }
}
