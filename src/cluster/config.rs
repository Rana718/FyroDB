use super::{HASH_SLOTS, NodeInfo, NodeRole, Slot, SlotRange, Topology};
use std::time::Duration;

#[derive(Debug, Clone)]
pub struct ClusterConfig {
    pub enabled: bool,
    pub local_id: String,
    pub listen_address: String,
    pub topology: Topology,
    pub peer_queue_capacity: usize,
    pub heartbeat_interval: Duration,
    pub suspect_timeout: Duration,
    pub failure_quorum: usize,
    pub max_inbound_peers: usize,
    pub auth_token: Option<String>,
    pub replication_log_capacity: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClusterConfigError(pub String);

impl std::fmt::Display for ClusterConfigError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for ClusterConfigError {}

impl ClusterConfig {
    pub fn disabled() -> Self {
        Self {
            enabled: false,
            local_id: String::new(),
            listen_address: String::new(),
            topology: Topology::default(),
            peer_queue_capacity: 1024,
            heartbeat_interval: Duration::from_secs(2),
            suspect_timeout: Duration::from_secs(6),
            failure_quorum: 2,
            max_inbound_peers: 1024,
            auth_token: None,
            replication_log_capacity: 100_000,
        }
    }

    pub fn from_env() -> Result<Self, ClusterConfigError> {
        let enabled = std::env::var("FYRODB_CLUSTER_ENABLED")
            .ok()
            .is_some_and(|value| {
                matches!(value.to_ascii_lowercase().as_str(), "1" | "true" | "yes")
            });
        if !enabled {
            return Ok(Self::disabled());
        }

        let local_id = std::env::var("FYRODB_NODE_ID").unwrap_or_else(|_| default_node_id());
        let bind = std::env::var("FYRODB_BIND").unwrap_or_else(|_| "127.0.0.1".into());
        let client_port = env_port("FYRODB_PORT", 8000)?;
        let cluster_port = env_port("FYRODB_CLUSTER_PORT", 18000)?;
        let listen_address = format!("{bind}:{cluster_port}");
        let advertised = std::env::var("FYRODB_ADVERTISE_ADDR").unwrap_or(bind);
        let heartbeat_interval = env_duration("FYRODB_CLUSTER_HEARTBEAT_MS", 2000)?;
        let suspect_timeout = env_duration("FYRODB_CLUSTER_SUSPECT_MS", 6000)?;
        let peer_queue_capacity = env_usize("FYRODB_CLUSTER_QUEUE_CAPACITY", 1024)?.max(1);
        let failure_quorum = env_usize("FYRODB_CLUSTER_FAILURE_QUORUM", 2)?.max(1);
        let max_inbound_peers = env_usize("FYRODB_CLUSTER_MAX_INBOUND", 1024)?.max(1);
        let auth_token = std::env::var("FYRODB_CLUSTER_AUTH")
            .ok()
            .filter(|v| !v.is_empty());
        let replication_log_capacity =
            env_usize("FYRODB_REPLICATION_LOG_CAPACITY", 100_000)?.max(1);
        if suspect_timeout <= heartbeat_interval {
            return Err(ClusterConfigError(
                "FYRODB_CLUSTER_SUSPECT_MS must exceed FYRODB_CLUSTER_HEARTBEAT_MS".into(),
            ));
        }
        let topology = if let Ok(spec) = std::env::var("FYRODB_CLUSTER_NODES") {
            parse_nodes(&spec)?
        } else {
            let slots = match std::env::var("FYRODB_CLUSTER_SLOTS") {
                Ok(value) => parse_slot_ranges(&value)?,
                Err(_) => vec![SlotRange::new(Slot(0), Slot(HASH_SLOTS - 1)).unwrap()],
            };
            Topology::new(
                1,
                vec![NodeInfo {
                    id: local_id.clone(),
                    address: format!("{advertised}:{client_port}"),
                    cluster_address: format!("{advertised}:{cluster_port}"),
                    role: NodeRole::Primary,
                    replica_of: None,
                    epoch: 1,
                    slots,
                }],
            )
        };
        if !topology.nodes.iter().any(|node| node.id == local_id) {
            return Err(ClusterConfigError(format!(
                "FYRODB_NODE_ID {local_id} is not present in FYRODB_CLUSTER_NODES"
            )));
        }
        validate_primary_ranges(&topology)?;
        Ok(Self {
            enabled: true,
            local_id,
            listen_address,
            topology,
            peer_queue_capacity,
            heartbeat_interval,
            suspect_timeout,
            failure_quorum,
            max_inbound_peers,
            auth_token,
            replication_log_capacity,
        })
    }

    pub fn local_node(&self) -> Option<&NodeInfo> {
        self.topology
            .nodes
            .iter()
            .find(|node| node.id == self.local_id)
    }
}

fn env_port(name: &str, default: u16) -> Result<u16, ClusterConfigError> {
    match std::env::var(name) {
        Ok(value) => value
            .parse()
            .map_err(|_| ClusterConfigError(format!("invalid {name}: {value}"))),
        Err(_) => Ok(default),
    }
}

fn env_usize(name: &str, default: usize) -> Result<usize, ClusterConfigError> {
    match std::env::var(name) {
        Ok(value) => value
            .parse()
            .map_err(|_| ClusterConfigError(format!("invalid {name}: {value}"))),
        Err(_) => Ok(default),
    }
}

fn env_duration(name: &str, default_ms: u64) -> Result<Duration, ClusterConfigError> {
    Ok(Duration::from_millis(
        env_usize(name, default_ms as usize)? as u64
    ))
}

fn parse_slot_ranges(value: &str) -> Result<Vec<SlotRange>, ClusterConfigError> {
    let mut ranges = Vec::new();
    for raw in value
        .split(',')
        .map(str::trim)
        .filter(|part| !part.is_empty())
    {
        let (start, end) = raw.split_once('-').unwrap_or((raw, raw));
        let start: u16 = start
            .parse()
            .map_err(|_| ClusterConfigError(format!("invalid slot range: {raw}")))?;
        let end: u16 = end
            .parse()
            .map_err(|_| ClusterConfigError(format!("invalid slot range: {raw}")))?;
        let start = Slot::new(start)
            .ok_or_else(|| ClusterConfigError(format!("slot out of range: {raw}")))?;
        let end = Slot::new(end)
            .ok_or_else(|| ClusterConfigError(format!("slot out of range: {raw}")))?;
        ranges.push(
            SlotRange::new(start, end)
                .ok_or_else(|| ClusterConfigError(format!("reversed slot range: {raw}")))?,
        );
    }
    if ranges.is_empty() {
        return Err(ClusterConfigError("FYRODB_CLUSTER_SLOTS is empty".into()));
    }
    Ok(ranges)
}

/// Parse `id|client_addr|cluster_addr|slots[|role]` records separated by semicolons.
/// Role is `primary` (default) or `replica:<primary-id>`. Replicas use `-` for slots.
fn parse_nodes(value: &str) -> Result<Topology, ClusterConfigError> {
    let mut nodes = Vec::new();
    for raw in value
        .split(';')
        .map(str::trim)
        .filter(|record| !record.is_empty())
    {
        let fields: Vec<_> = raw.split('|').collect();
        if !(4..=5).contains(&fields.len())
            || fields[..3].iter().any(|field| field.trim().is_empty())
        {
            return Err(ClusterConfigError(format!(
                "invalid cluster node record: {raw}"
            )));
        }
        if nodes.iter().any(|node: &NodeInfo| node.id == fields[0]) {
            return Err(ClusterConfigError(format!(
                "duplicate cluster node id: {}",
                fields[0]
            )));
        }
        let (role, replica_of, slots) = match fields.get(4).copied().unwrap_or("primary") {
            role if role.eq_ignore_ascii_case("primary") => {
                (NodeRole::Primary, None, parse_slot_ranges(fields[3])?)
            }
            role if role.to_ascii_lowercase().starts_with("replica:") => {
                let primary = role.split_once(':').map(|(_, id)| id.trim()).unwrap_or("");
                if primary.is_empty() || fields[3].trim() != "-" {
                    return Err(ClusterConfigError(format!(
                        "replica must reference a primary and use '-' slots: {raw}"
                    )));
                }
                (NodeRole::Replica, Some(primary.to_owned()), Vec::new())
            }
            _ => return Err(ClusterConfigError(format!("invalid node role: {raw}"))),
        };
        nodes.push(NodeInfo {
            id: fields[0].into(),
            address: fields[1].into(),
            cluster_address: fields[2].into(),
            role,
            replica_of,
            epoch: 1,
            slots,
        });
    }
    if nodes.is_empty() {
        return Err(ClusterConfigError("FYRODB_CLUSTER_NODES is empty".into()));
    }
    for node in nodes.iter().filter(|node| node.role == NodeRole::Replica) {
        let primary = node.replica_of.as_deref().unwrap();
        if !nodes
            .iter()
            .any(|candidate| candidate.id == primary && candidate.role == NodeRole::Primary)
        {
            return Err(ClusterConfigError(format!(
                "replica {} references unknown primary {primary}",
                node.id
            )));
        }
    }
    Ok(Topology::new(1, nodes))
}

fn validate_primary_ranges(topology: &Topology) -> Result<(), ClusterConfigError> {
    let mut owners = vec![None::<&str>; HASH_SLOTS as usize];
    for node in topology
        .nodes
        .iter()
        .filter(|node| node.role == NodeRole::Primary)
    {
        for range in &node.slots {
            for slot in range.start.value()..=range.end.value() {
                if let Some(existing) = owners[slot as usize] {
                    return Err(ClusterConfigError(format!(
                        "slot {slot} is assigned to both {existing} and {}",
                        node.id
                    )));
                }
                owners[slot as usize] = Some(&node.id);
            }
        }
    }
    Ok(())
}

fn default_node_id() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    format!("{nanos:040x}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_static_multi_node_topology() {
        let topology = parse_nodes(
            "a|10.0.0.1:8000|10.0.0.1:18000|0-8191;b|10.0.0.2:8000|10.0.0.2:18000|8192-16383",
        )
        .unwrap();
        assert_eq!(topology.nodes.len(), 2);
        assert!(topology.is_complete());
        validate_primary_ranges(&topology).unwrap();
    }

    #[test]
    fn rejects_overlapping_ranges() {
        let topology = parse_nodes("a|a:8000|a:18000|0-9000;b|b:8000|b:18000|9000-16383").unwrap();
        assert!(validate_primary_ranges(&topology).is_err());
    }

    #[test]
    fn parses_and_validates_replica_roles() {
        let topology =
            parse_nodes("a|a:8000|a:18000|0-16383|primary;r|r:8000|r:18000|-|replica:a").unwrap();
        let replica = topology.nodes.iter().find(|node| node.id == "r").unwrap();
        assert_eq!(replica.role, NodeRole::Replica);
        assert_eq!(replica.replica_of.as_deref(), Some("a"));
        assert!(replica.slots.is_empty());
        assert!(topology.is_complete());
    }

    #[test]
    fn rejects_replica_with_unknown_primary_or_slots() {
        assert!(parse_nodes("r|r:8000|r:18000|-|replica:missing").is_err());
        assert!(parse_nodes("a|a:8000|a:18000|0-16383;r|r:8000|r:18000|0-1|replica:a").is_err());
    }
}
