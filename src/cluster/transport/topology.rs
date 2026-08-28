use crate::cluster::{NodeInfo, NodeRole, Slot, SlotRange, Topology};

const MAX_NODES: usize = 4096;
const MAX_STRING: usize = u16::MAX as usize;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TopologyCodecError {
    Truncated,
    Invalid,
    TooLarge,
}

pub fn encode_topology(topology: &Topology) -> Result<Vec<u8>, TopologyCodecError> {
    if topology.nodes.len() > MAX_NODES {
        return Err(TopologyCodecError::TooLarge);
    }
    let mut out = Vec::new();
    out.extend_from_slice(&topology.epoch.to_be_bytes());
    out.extend_from_slice(&(topology.nodes.len() as u32).to_be_bytes());
    for node in &topology.nodes {
        put_string(&mut out, &node.id)?;
        put_string(&mut out, &node.address)?;
        put_string(&mut out, &node.cluster_address)?;
        out.push(match node.role {
            NodeRole::Primary => 0,
            NodeRole::Replica => 1,
        });
        put_string(&mut out, node.replica_of.as_deref().unwrap_or(""))?;
        out.extend_from_slice(&node.epoch.to_be_bytes());
        if node.slots.len() > u16::MAX as usize {
            return Err(TopologyCodecError::TooLarge);
        }
        out.extend_from_slice(&(node.slots.len() as u16).to_be_bytes());
        for range in &node.slots {
            out.extend_from_slice(&range.start.value().to_be_bytes());
            out.extend_from_slice(&range.end.value().to_be_bytes());
        }
    }
    Ok(out)
}

pub fn decode_topology(bytes: &[u8]) -> Result<Topology, TopologyCodecError> {
    let mut input = bytes;
    let epoch = take_u64(&mut input)?;
    let count = take_u32(&mut input)? as usize;
    if count > MAX_NODES {
        return Err(TopologyCodecError::TooLarge);
    }
    let mut nodes = Vec::with_capacity(count);
    for _ in 0..count {
        let id = take_string(&mut input)?;
        let address = take_string(&mut input)?;
        let cluster_address = take_string(&mut input)?;
        let role = match take(&mut input, 1)?[0] {
            0 => NodeRole::Primary,
            1 => NodeRole::Replica,
            _ => return Err(TopologyCodecError::Invalid),
        };
        let replica_of = match take_string(&mut input)? {
            value if value.is_empty() => None,
            value => Some(value),
        };
        let node_epoch = take_u64(&mut input)?;
        let range_count = take_u16(&mut input)? as usize;
        let mut slots = Vec::with_capacity(range_count);
        for _ in 0..range_count {
            let start = Slot::new(take_u16(&mut input)?).ok_or(TopologyCodecError::Invalid)?;
            let end = Slot::new(take_u16(&mut input)?).ok_or(TopologyCodecError::Invalid)?;
            slots.push(SlotRange::new(start, end).ok_or(TopologyCodecError::Invalid)?);
        }
        nodes.push(NodeInfo {
            id,
            address,
            cluster_address,
            role,
            replica_of,
            epoch: node_epoch,
            slots,
        });
    }
    if !input.is_empty() {
        return Err(TopologyCodecError::Invalid);
    }
    Ok(Topology::new(epoch, nodes))
}

fn put_string(out: &mut Vec<u8>, value: &str) -> Result<(), TopologyCodecError> {
    if value.len() > MAX_STRING {
        return Err(TopologyCodecError::TooLarge);
    }
    out.extend_from_slice(&(value.len() as u16).to_be_bytes());
    out.extend_from_slice(value.as_bytes());
    Ok(())
}

fn take<'a>(input: &mut &'a [u8], count: usize) -> Result<&'a [u8], TopologyCodecError> {
    if input.len() < count {
        return Err(TopologyCodecError::Truncated);
    }
    let (value, rest) = input.split_at(count);
    *input = rest;
    Ok(value)
}
fn take_u16(input: &mut &[u8]) -> Result<u16, TopologyCodecError> {
    Ok(u16::from_be_bytes(take(input, 2)?.try_into().unwrap()))
}
fn take_u32(input: &mut &[u8]) -> Result<u32, TopologyCodecError> {
    Ok(u32::from_be_bytes(take(input, 4)?.try_into().unwrap()))
}
fn take_u64(input: &mut &[u8]) -> Result<u64, TopologyCodecError> {
    Ok(u64::from_be_bytes(take(input, 8)?.try_into().unwrap()))
}
fn take_string(input: &mut &[u8]) -> Result<String, TopologyCodecError> {
    let length = take_u16(input)? as usize;
    String::from_utf8(take(input, length)?.to_vec()).map_err(|_| TopologyCodecError::Invalid)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn topology_round_trip_and_validation() {
        let topology = Topology::new(
            9,
            vec![NodeInfo {
                id: "a".into(),
                address: "10.0.0.1:8000".into(),
                cluster_address: "10.0.0.1:18000".into(),
                role: NodeRole::Primary,
                replica_of: None,
                epoch: 8,
                slots: vec![SlotRange::new(Slot(0), Slot(100)).unwrap()],
            }],
        );
        let encoded = encode_topology(&topology).unwrap();
        let decoded = decode_topology(&encoded).unwrap();
        assert_eq!(decoded.epoch, 9);
        assert_eq!(decoded.nodes, topology.nodes);
        assert!(matches!(
            decode_topology(&encoded[..encoded.len() - 1]),
            Err(TopologyCodecError::Truncated)
        ));
    }
}
