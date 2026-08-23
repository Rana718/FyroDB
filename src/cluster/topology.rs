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
            epoch: 1,
            slots: vec![SlotRange::new(Slot(start), Slot(end)).unwrap()],
        };
        let topology = Topology::new(1, vec![node("a", 0, 8191), node("b", 8192, 16383)]);
        assert_eq!(topology.owner(Slot(9000)).unwrap().id, "b");
        assert!(topology.is_complete());
    }
}
