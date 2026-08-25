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
    /// Cached at startup — avoids a linear node scan on every write command.
    pub is_replica: bool,
    /// Path where cluster topology is auto-saved (nodes.conf style).
    pub nodes_config_file: String,
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
            is_replica: false,
            nodes_config_file: "fyrodb-nodes.conf".to_string(),
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
        let nodes_config_file = std::env::var("FYRODB_CLUSTER_CONFIG_FILE")
            .unwrap_or_else(|_| "fyrodb-nodes.conf".to_string());

        // Load topology from nodes.conf if it exists, otherwise start as
        // a single unconfigured node — use CLUSTER MEET + CLUSTER ADDSLOTS
        // (or redis-cli --cluster create) to wire the cluster together.
        let topology = if let Some(t) = load_nodes_conf(&nodes_config_file, &local_id) {
            t
        } else {
            Topology::new(
                1,
                vec![NodeInfo {
                    id: local_id.clone(),
                    address: format!("{advertised}:{client_port}"),
                    cluster_address: format!("{advertised}:{cluster_port}"),
                    role: NodeRole::Primary,
                    replica_of: None,
                    epoch: 1,
                    slots: Vec::new(),
                }],
            )
        };
        validate_primary_ranges(&topology)?;
        let is_replica = topology
            .nodes
            .iter()
            .any(|node| node.id == local_id && node.role == NodeRole::Replica);
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
            is_replica,
            nodes_config_file,
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

/// Load topology from a nodes.conf file (Redis-style auto-save format).
/// Each line: `<id> <addr>@<cluster_addr> <flags> <master> <epoch> connected <slots...>`
/// Returns None if the file doesn't exist or is unparseable.
pub fn load_nodes_conf(path: &str, local_id: &str) -> Option<Topology> {
    let content = std::fs::read_to_string(path).ok()?;
    let mut nodes = Vec::new();
    let mut epoch = 1u64;
    for line in content.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let parts: Vec<&str> = line.split_whitespace().collect();
        if parts.len() < 7 {
            continue;
        }
        let id = parts[0];
        let addrs = parts[1];
        let flags = parts[2];
        let master_field = parts[3];
        let node_epoch: u64 = parts[4].parse().unwrap_or(1);
        epoch = epoch.max(node_epoch);
        let (address, cluster_address) = addrs
            .split_once('@')
            .map(|(a, b)| (a.to_owned(), b.to_owned()))
            .unwrap_or_else(|| (addrs.to_owned(), addrs.to_owned()));
        let is_replica = flags.contains("slave") || flags.contains("replica");
        let replica_of = if is_replica && master_field != "-" {
            Some(master_field.to_owned())
        } else {
            None
        };
        let role = if is_replica {
            NodeRole::Replica
        } else {
            NodeRole::Primary
        };
        let slots = if !is_replica {
            parts[7..]
                .iter()
                .filter_map(|s| {
                    if let Some((start, end)) = s.split_once('-') {
                        let s: u16 = start.parse().ok()?;
                        let e: u16 = end.parse().ok()?;
                        SlotRange::new(Slot::new(s)?, Slot::new(e)?)
                    } else {
                        let n: u16 = s.parse().ok()?;
                        let slot = Slot::new(n)?;
                        SlotRange::new(slot, slot)
                    }
                })
                .collect()
        } else {
            Vec::new()
        };
        nodes.push(NodeInfo {
            id: id.to_owned(),
            address,
            cluster_address,
            role,
            replica_of,
            epoch: node_epoch,
            slots,
        });
    }
    if nodes.is_empty() || !nodes.iter().any(|n| n.id == local_id) {
        return None;
    }
    Some(Topology::new(epoch, nodes))
}

/// Save topology to nodes.conf in Redis-compatible format.
pub fn save_nodes_conf(path: &str, topology: &Topology, local_id: &str) -> std::io::Result<()> {
    use std::fmt::Write as FmtWrite;
    let mut content = String::new();
    for node in &topology.nodes {
        let flags = if node.id == local_id {
            if node.role == NodeRole::Replica {
                "myself,slave"
            } else {
                "myself,master"
            }
        } else if node.role == NodeRole::Replica {
            "slave"
        } else {
            "master"
        };
        let master = node.replica_of.as_deref().unwrap_or("-");
        let slots: Vec<String> = node
            .slots
            .iter()
            .map(|r| {
                if r.start == r.end {
                    r.start.value().to_string()
                } else {
                    format!("{}-{}", r.start.value(), r.end.value())
                }
            })
            .collect();
        let _ = writeln!(
            content,
            "{} {}@{} {} {} {} 0 connected {}",
            node.id,
            node.address,
            node.cluster_address,
            flags,
            master,
            node.epoch,
            slots.join(" ")
        );
    }
    let tmp = format!("{path}.tmp");
    std::fs::write(&tmp, &content)?;
    std::fs::rename(tmp, path)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn nodes_conf_round_trip() {
        let topology = Topology::new(
            3,
            vec![
                NodeInfo {
                    id: "node-a".into(),
                    address: "10.0.0.1:8000".into(),
                    cluster_address: "10.0.0.1:18000".into(),
                    role: NodeRole::Primary,
                    replica_of: None,
                    epoch: 3,
                    slots: vec![SlotRange::new(Slot(0), Slot(8191)).unwrap()],
                },
                NodeInfo {
                    id: "node-b".into(),
                    address: "10.0.0.2:8000".into(),
                    cluster_address: "10.0.0.2:18000".into(),
                    role: NodeRole::Primary,
                    replica_of: None,
                    epoch: 3,
                    slots: vec![SlotRange::new(Slot(8192), Slot(16383)).unwrap()],
                },
            ],
        );
        let path = std::env::temp_dir()
            .join(format!("fyrodb-test-nodes-{}.conf", std::process::id()));
        let path = path.to_str().unwrap();
        save_nodes_conf(path, &topology, "node-a").unwrap();
        let loaded = load_nodes_conf(path, "node-a").unwrap();
        assert_eq!(loaded.nodes.len(), 2);
        assert!(loaded.is_complete());
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn validate_primary_ranges_rejects_overlaps() {
        let topology = Topology::new(
            1,
            vec![
                NodeInfo {
                    id: "a".into(),
                    address: "a:8000".into(),
                    cluster_address: "a:18000".into(),
                    role: NodeRole::Primary,
                    replica_of: None,
                    epoch: 1,
                    slots: vec![SlotRange::new(Slot(0), Slot(9000)).unwrap()],
                },
                NodeInfo {
                    id: "b".into(),
                    address: "b:8000".into(),
                    cluster_address: "b:18000".into(),
                    role: NodeRole::Primary,
                    replica_of: None,
                    epoch: 1,
                    slots: vec![SlotRange::new(Slot(9000), Slot(16383)).unwrap()],
                },
            ],
        );
        assert!(validate_primary_ranges(&topology).is_err());
    }
}
