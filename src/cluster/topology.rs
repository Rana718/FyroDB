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

#[derive(Debug, Clone, Default)]
pub struct Topology {
    pub epoch: u64,
    pub nodes: Vec<NodeInfo>,
}

impl Topology {
    pub fn new(epoch: u64, nodes: Vec<NodeInfo>) -> Self {
        Self { epoch, nodes }
    }

    pub fn owner(&self, slot: Slot) -> Option<&NodeInfo> {
        self.nodes
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

#[cfg(test)]
mod tests {
    use super::*;

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
