use super::{ClusterConfig, Slot, hash_slot};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RouteDecision<'a> {
    Local,
    Moved { slot: Slot, address: &'a str },
    MovedOwned { slot: Slot, address: String },
    Ask { slot: Slot, address: String },
    CrossSlot,
    Unassigned(Slot),
}

pub fn route_command<'a>(
    cluster: &'a ClusterConfig,
    command: &[u8],
    args: &[&[u8]],
) -> RouteDecision<'a> {
    route_command_with_topology(cluster, &cluster.topology, command, args)
}

pub fn route_command_with_topology<'a>(
    cluster: &'a ClusterConfig,
    topology: &'a super::Topology,
    command: &[u8],
    args: &[&[u8]],
) -> RouteDecision<'a> {
    if !cluster.enabled {
        return RouteDecision::Local;
    }
    let Some(pattern) = key_pattern(command) else {
        return RouteDecision::Local;
    };
    let mut indices = pattern.indices(args.len());
    let Some(first_index) = indices.next() else {
        return RouteDecision::Local;
    };
    let slot = hash_slot(args[first_index]);
    if indices.any(|index| hash_slot(args[index]) != slot) {
        return RouteDecision::CrossSlot;
    }
    match topology.owner(slot) {
        Some(owner) if owner.id == cluster.local_id => RouteDecision::Local,
        Some(owner) => RouteDecision::Moved {
            slot,
            address: &owner.address,
        },
        None => RouteDecision::Unassigned(slot),
    }
}

pub fn route_command_with_state<'a>(
    cluster: &'a ClusterConfig,
    state: &super::ClusterState,
    command: &[u8],
    args: &[&[u8]],
) -> RouteDecision<'a> {
    route_command_with_state_import(cluster, state, command, args, false)
}

pub fn route_command_with_state_import<'a>(
    cluster: &'a ClusterConfig,
    state: &super::ClusterState,
    command: &[u8],
    args: &[&[u8]],
    allow_import: bool,
) -> RouteDecision<'a> {
    let topology = state.topology_arc();
    route_command_with_snapshot(cluster, state, topology.as_ref(), command, args, allow_import)
}

/// Route against a topology snapshot the caller already holds.
///
/// The per-command variant above has to take the topology `RwLock` and clone an
/// `Arc` — two contended atomic read-modify-writes shared by every worker. A
/// connection that caches its snapshot and revalidates it against
/// `ClusterState::topology_version` can call this instead and pay one relaxed
/// load in the steady state.
///
/// Every redirect it produces owns its address, so the result borrows neither
/// the config nor the snapshot.
pub fn route_command_with_snapshot(
    cluster: &ClusterConfig,
    state: &super::ClusterState,
    topology: &super::Topology,
    command: &[u8],
    args: &[&[u8]],
    allow_import: bool,
) -> RouteDecision<'static> {
    let pattern = key_pattern(command);
    let decision = route_command_with_topology_owned(cluster, topology, command, args);
    if allow_import
        && let Some(pat) = pattern
        && let Some(index) = pat.indices(args.len()).next()
        && state.is_importing(hash_slot(args[index]))
    {
        return RouteDecision::Local;
    }
    match decision {
        RouteDecision::Local => {}
        other => return other,
    }
    let Some(pat) = pattern else {
        return RouteDecision::Local;
    };
    let Some(index) = pat.indices(args.len()).next() else {
        return RouteDecision::Local;
    };
    let slot = hash_slot(args[index]);
    state
        .migrating_target(slot)
        .and_then(|target| {
            topology
                .nodes
                .iter()
                .find(|node| node.id == target)
                .map(|node| RouteDecision::Ask {
                    slot,
                    address: node.address.clone(),
                })
        })
        .unwrap_or(RouteDecision::Local)
}

/// Same routing as `route_command_with_topology`, but any redirect address is
/// owned so the decision does not borrow the snapshot.
fn route_command_with_topology_owned(
    cluster: &ClusterConfig,
    topology: &super::Topology,
    command: &[u8],
    args: &[&[u8]],
) -> RouteDecision<'static> {
    if !cluster.enabled {
        return RouteDecision::Local;
    }
    let Some(pattern) = key_pattern(command) else {
        return RouteDecision::Local;
    };
    let mut indices = pattern.indices(args.len());
    let Some(first_index) = indices.next() else {
        return RouteDecision::Local;
    };
    let slot = hash_slot(args[first_index]);
    if indices.any(|index| hash_slot(args[index]) != slot) {
        return RouteDecision::CrossSlot;
    }
    match topology.owner(slot) {
        Some(owner) if owner.id == cluster.local_id => RouteDecision::Local,
        Some(owner) => RouteDecision::MovedOwned {
            slot,
            address: owner.address.clone(),
        },
        None => RouteDecision::Unassigned(slot),
    }
}

#[derive(Clone, Copy)]
enum KeyPattern {
    All,
    EveryOther,
    FirstTwo,
    First,
}

impl KeyPattern {
    fn indices(self, len: usize) -> impl Iterator<Item = usize> {
        let (take, step) = match self {
            Self::All => (len, 1),
            Self::EveryOther => (len, 2),
            Self::FirstTwo => (len.min(2), 1),
            Self::First => (len.min(1), 1),
        };
        (0..take).step_by(step)
    }
}

fn key_pattern(command: &[u8]) -> Option<KeyPattern> {
    if command.eq_ignore_ascii_case(b"MGET")
        || command.eq_ignore_ascii_case(b"DEL")
        || command.eq_ignore_ascii_case(b"UNLINK")
        || command.eq_ignore_ascii_case(b"EXISTS")
        || command.eq_ignore_ascii_case(b"TOUCH")
    {
        return Some(KeyPattern::All);
    }
    if command.eq_ignore_ascii_case(b"MSET") || command.eq_ignore_ascii_case(b"MSETNX") {
        return Some(KeyPattern::EveryOther);
    }
    if command.eq_ignore_ascii_case(b"RENAME")
        || command.eq_ignore_ascii_case(b"RENAMENX")
        || command.eq_ignore_ascii_case(b"COPY")
        || command.eq_ignore_ascii_case(b"SMOVE")
        || command.eq_ignore_ascii_case(b"LMOVE")
        || command.eq_ignore_ascii_case(b"RPOPLPUSH")
    {
        return Some(KeyPattern::FirstTwo);
    }
    if is_single_key(command) {
        return Some(KeyPattern::First);
    }
    None
}

/// Uses a (len, first_byte) match so the compiler emits a jump table
pub fn is_write_command(command: &[u8]) -> bool {
    let Some(&first) = command.first() else {
        return false;
    };
    match (command.len(), first.to_ascii_uppercase()) {
        // len=3
        (3, b'S') => command.eq_ignore_ascii_case(b"SET"),
        (3, b'D') => command.eq_ignore_ascii_case(b"DEL"),
        // len=4
        (4, b'I') => command.eq_ignore_ascii_case(b"INCR"),
        (4, b'D') => command.eq_ignore_ascii_case(b"DECR"),
        (4, b'C') => command.eq_ignore_ascii_case(b"COPY"),
        (4, b'M') => command.eq_ignore_ascii_case(b"MSET"),
        (4, b'H') => command.eq_ignore_ascii_case(b"HSET") || command.eq_ignore_ascii_case(b"HDEL"),
        (4, b'L') => {
            command.eq_ignore_ascii_case(b"LPOP")
                || command.eq_ignore_ascii_case(b"LSET")
                || command.eq_ignore_ascii_case(b"LREM")
        }
        (4, b'R') => {
            command.eq_ignore_ascii_case(b"RPOP") || command.eq_ignore_ascii_case(b"RPUSH")
        }
        (4, b'S') => {
            command.eq_ignore_ascii_case(b"SADD")
                || command.eq_ignore_ascii_case(b"SREM")
                || command.eq_ignore_ascii_case(b"SPOP")
                || command.eq_ignore_ascii_case(b"SORT")
        }
        (4, b'Z') => command.eq_ignore_ascii_case(b"ZADD") || command.eq_ignore_ascii_case(b"ZREM"),
        (4, b'X') => {
            command.eq_ignore_ascii_case(b"XADD")
                || command.eq_ignore_ascii_case(b"XDEL")
                || command.eq_ignore_ascii_case(b"XACK")
        }
        (4, b'P') => command.eq_ignore_ascii_case(b"PFADD"),
        // len=5
        (5, b'S') => {
            command.eq_ignore_ascii_case(b"SETNX")
                || command.eq_ignore_ascii_case(b"SETEX")
                || command.eq_ignore_ascii_case(b"SMOVE")
        }
        (5, b'L') => {
            command.eq_ignore_ascii_case(b"LPUSH")
                || command.eq_ignore_ascii_case(b"LMOVE")
                || command.eq_ignore_ascii_case(b"LTRIM")
        }
        (5, b'R') => command.eq_ignore_ascii_case(b"RPUSH"),
        (5, b'M') => command.eq_ignore_ascii_case(b"MSETNX"),
        (5, b'X') => command.eq_ignore_ascii_case(b"XTRIM"),
        (5, b'B') => command.eq_ignore_ascii_case(b"BITOP"),
        (5, b'Z') => command.eq_ignore_ascii_case(b"ZINCRBY"),
        // len=6
        (6, b'A') => command.eq_ignore_ascii_case(b"APPEND"),
        (6, b'U') => command.eq_ignore_ascii_case(b"UNLINK"),
        (6, b'G') => {
            command.eq_ignore_ascii_case(b"GETDEL")
                || command.eq_ignore_ascii_case(b"GETSET")
                || command.eq_ignore_ascii_case(b"GETEX")
                || command.eq_ignore_ascii_case(b"GEOADD")
        }
        (6, b'P') => {
            command.eq_ignore_ascii_case(b"PSETEX")
                || command.eq_ignore_ascii_case(b"PERSIST")
                || command.eq_ignore_ascii_case(b"PFADD")
        }
        (6, b'H') => {
            command.eq_ignore_ascii_case(b"HSETNX") || command.eq_ignore_ascii_case(b"HMSET")
        }
        (6, b'S') => command.eq_ignore_ascii_case(b"SETBIT"),
        (6, b'Z') => command.eq_ignore_ascii_case(b"ZINCRBY"),
        (6, b'X') => command.eq_ignore_ascii_case(b"XGROUP"),
        // len=7
        (7, b'E') => command.eq_ignore_ascii_case(b"EXPIRE"),
        (7, b'R') => command.eq_ignore_ascii_case(b"RENAME"),
        (7, b'P') => {
            command.eq_ignore_ascii_case(b"PEXPIRE") || command.eq_ignore_ascii_case(b"PFMERGE")
        }
        (7, b'S') => command.eq_ignore_ascii_case(b"SETRANGE"),
        (7, b'H') => command.eq_ignore_ascii_case(b"HINCRBY"),
        (7, b'L') => command.eq_ignore_ascii_case(b"LINSERT"),
        (7, b'B') => {
            command.eq_ignore_ascii_case(b"BLPOP")
                || command.eq_ignore_ascii_case(b"BRPOP")
                || command.eq_ignore_ascii_case(b"BITFIELD")
        }
        (7, b'Z') => {
            command.eq_ignore_ascii_case(b"ZPOPMIN") || command.eq_ignore_ascii_case(b"ZPOPMAX")
        }
        (7, b'F') => {
            command.eq_ignore_ascii_case(b"FLUSHDB") || command.eq_ignore_ascii_case(b"FLUSHALL")
        }
        (7, b'I') => command.eq_ignore_ascii_case(b"INCRBY"),
        (7, b'D') => command.eq_ignore_ascii_case(b"DECRBY"),
        (7, b'X') => command.eq_ignore_ascii_case(b"XGROUP"),
        // len=8
        (8, b'E') => command.eq_ignore_ascii_case(b"EXPIREAT"),
        (8, b'R') => command.eq_ignore_ascii_case(b"RENAMENX"),
        (8, b'P') => command.eq_ignore_ascii_case(b"PEXPIREAT"),
        (8, b'B') => command.eq_ignore_ascii_case(b"BITFIELD"),
        (8, b'F') => command.eq_ignore_ascii_case(b"FLUSHALL"),
        (8, b'I') => command.eq_ignore_ascii_case(b"INCRBYFLOAT"),
        // len=9+
        (9, b'R') => command.eq_ignore_ascii_case(b"RPOPLPUSH"),
        (9, b'Z') => {
            command.eq_ignore_ascii_case(b"ZPOPMIN")
                || command.eq_ignore_ascii_case(b"ZPOPMAX")
                || command.eq_ignore_ascii_case(b"ZINCRBY")
                || command.eq_ignore_ascii_case(b"ZRANGESTORE")
        }
        (9, b'B') => {
            command.eq_ignore_ascii_case(b"BZPOPMIN")
                || command.eq_ignore_ascii_case(b"BZPOPMAX")
                || command.eq_ignore_ascii_case(b"BLMOVE")
        }
        (9, b'H') => command.eq_ignore_ascii_case(b"HINCRBYFLOAT"),
        (10, b'S') => {
            command.eq_ignore_ascii_case(b"SDIFFSTORE")
                || command.eq_ignore_ascii_case(b"ZDIFFSTORE")
        }
        (11, b'Z') => {
            command.eq_ignore_ascii_case(b"ZUNIONSTORE")
                || command.eq_ignore_ascii_case(b"ZINTERSTORE")
        }
        (11, b'S') => {
            command.eq_ignore_ascii_case(b"SUNIONSTORE")
                || command.eq_ignore_ascii_case(b"SINTERSTORE")
        }
        (14, b'G') => command.eq_ignore_ascii_case(b"GEOSEARCHSTORE"),
        // JSON commands
        (8, b'J') => {
            command.eq_ignore_ascii_case(b"JSON.SET") || command.eq_ignore_ascii_case(b"JSON.DEL")
        }
        (10, b'J') => command.eq_ignore_ascii_case(b"JSON.CLEAR"),
        (11, b'J') => {
            command.eq_ignore_ascii_case(b"JSON.ARRTRIM")
                || command.eq_ignore_ascii_case(b"JSON.FORGET")
                || command.eq_ignore_ascii_case(b"JSON.TOGGLE")
                || command.eq_ignore_ascii_case(b"JSON.ARRPOP")
        }
        (12, b'J') => command.eq_ignore_ascii_case(b"JSON.ARRTRIM"),
        (14, b'J') => {
            command.eq_ignore_ascii_case(b"JSON.NUMINCRBY")
                || command.eq_ignore_ascii_case(b"JSON.NUMMULTBY")
                || command.eq_ignore_ascii_case(b"JSON.STRAPPEND")
                || command.eq_ignore_ascii_case(b"JSON.ARRAPPEND")
                || command.eq_ignore_ascii_case(b"JSON.ARRINSERT")
        }
        _ => false,
    }
}

fn is_single_key(command: &[u8]) -> bool {
    let Some(&first) = command.first() else {
        return false;
    };
    match (command.len(), first.to_ascii_uppercase()) {
        // len=3
        (3, b'G') => command.eq_ignore_ascii_case(b"GET"),
        (3, b'S') => command.eq_ignore_ascii_case(b"SET"),
        (3, b'T') => command.eq_ignore_ascii_case(b"TTL"),
        (3, b'D') => command.eq_ignore_ascii_case(b"DEL"),
        // len=4
        (4, b'I') => command.eq_ignore_ascii_case(b"INCR"),
        (4, b'D') => command.eq_ignore_ascii_case(b"DECR"),
        (4, b'T') => command.eq_ignore_ascii_case(b"TYPE"),
        (4, b'P') => command.eq_ignore_ascii_case(b"PTTL"),
        (4, b'H') => {
            command.eq_ignore_ascii_case(b"HSET")
                || command.eq_ignore_ascii_case(b"HGET")
                || command.eq_ignore_ascii_case(b"HDEL")
                || command.eq_ignore_ascii_case(b"HLEN")
        }
        (4, b'L') => {
            command.eq_ignore_ascii_case(b"LPOP")
                || command.eq_ignore_ascii_case(b"LLEN")
                || command.eq_ignore_ascii_case(b"LSET")
                || command.eq_ignore_ascii_case(b"LREM")
        }
        (4, b'R') => {
            command.eq_ignore_ascii_case(b"RPOP") || command.eq_ignore_ascii_case(b"RPUSH")
        }
        (4, b'S') => {
            command.eq_ignore_ascii_case(b"SADD")
                || command.eq_ignore_ascii_case(b"SREM")
                || command.eq_ignore_ascii_case(b"SPOP")
                || command.eq_ignore_ascii_case(b"SCARD")
        }
        (4, b'Z') => {
            command.eq_ignore_ascii_case(b"ZADD")
                || command.eq_ignore_ascii_case(b"ZREM")
                || command.eq_ignore_ascii_case(b"ZCARD")
        }
        (4, b'X') => {
            command.eq_ignore_ascii_case(b"XADD")
                || command.eq_ignore_ascii_case(b"XLEN")
                || command.eq_ignore_ascii_case(b"XDEL")
        }
        // len=5
        (5, b'S') => {
            command.eq_ignore_ascii_case(b"SETNX") || command.eq_ignore_ascii_case(b"SETEX")
        }
        (5, b'L') => {
            command.eq_ignore_ascii_case(b"LPUSH") || command.eq_ignore_ascii_case(b"LPOS")
        }
        (5, b'R') => command.eq_ignore_ascii_case(b"RPUSH"),
        (5, b'Z') => {
            command.eq_ignore_ascii_case(b"ZSCORE")
                || command.eq_ignore_ascii_case(b"ZRANK")
                || command.eq_ignore_ascii_case(b"ZCOUNT")
                || command.eq_ignore_ascii_case(b"ZRANGE")
        }
        (5, b'P') => command.eq_ignore_ascii_case(b"PFADD"),
        // len=6
        (6, b'A') => command.eq_ignore_ascii_case(b"APPEND"),
        (6, b'G') => {
            command.eq_ignore_ascii_case(b"GETSET")
                || command.eq_ignore_ascii_case(b"GETEX")
                || command.eq_ignore_ascii_case(b"GETBIT")
                || command.eq_ignore_ascii_case(b"GEOPOS")
        }
        (6, b'P') => {
            command.eq_ignore_ascii_case(b"PSETEX") || command.eq_ignore_ascii_case(b"PERSIST")
        }
        (6, b'S') => {
            command.eq_ignore_ascii_case(b"STRLEN") || command.eq_ignore_ascii_case(b"SETBIT")
        }
        (6, b'H') => {
            command.eq_ignore_ascii_case(b"HMGET")
                || command.eq_ignore_ascii_case(b"HMSET")
                || command.eq_ignore_ascii_case(b"HKEYS")
                || command.eq_ignore_ascii_case(b"HVALS")
                || command.eq_ignore_ascii_case(b"HEXISTS")
        }
        (6, b'L') => {
            command.eq_ignore_ascii_case(b"LINDEX")
                || command.eq_ignore_ascii_case(b"LRANGE")
                || command.eq_ignore_ascii_case(b"LTRIM")
        }
        (6, b'Z') => {
            command.eq_ignore_ascii_case(b"ZSCORE")
                || command.eq_ignore_ascii_case(b"ZRANGE")
                || command.eq_ignore_ascii_case(b"ZCOUNT")
                || command.eq_ignore_ascii_case(b"ZRANK")
        }
        (6, b'X') => command.eq_ignore_ascii_case(b"XRANGE"),
        // len=7
        (7, b'E') => command.eq_ignore_ascii_case(b"EXPIRE"),
        (7, b'G') => {
            command.eq_ignore_ascii_case(b"GEODIST") || command.eq_ignore_ascii_case(b"GEOHASH")
        }
        (7, b'P') => {
            command.eq_ignore_ascii_case(b"PEXPIRE") || command.eq_ignore_ascii_case(b"PFCOUNT")
        }
        // len=8
        (8, b'E') => command.eq_ignore_ascii_case(b"EXPIREAT"),
        (8, b'G') => {
            command.eq_ignore_ascii_case(b"GETRANGE")
                || command.eq_ignore_ascii_case(b"GETDEL")
                || command.eq_ignore_ascii_case(b"GEOADD")
        }
        (8, b'S') => {
            command.eq_ignore_ascii_case(b"SETRANGE") || command.eq_ignore_ascii_case(b"SMEMBERS")
        }
        (8, b'P') => command.eq_ignore_ascii_case(b"PEXPIREAT"),
        (8, b'Z') => {
            command.eq_ignore_ascii_case(b"ZPOPMIN")
                || command.eq_ignore_ascii_case(b"ZPOPMAX")
                || command.eq_ignore_ascii_case(b"ZREVRANK")
        }
        (8, b'B') => {
            command.eq_ignore_ascii_case(b"BITCOUNT") || command.eq_ignore_ascii_case(b"BITPOS")
        }
        (8, b'J') => {
            command.eq_ignore_ascii_case(b"JSON.SET")
                || command.eq_ignore_ascii_case(b"JSON.GET")
                || command.eq_ignore_ascii_case(b"JSON.DEL")
        }
        (8, b'X') => command.eq_ignore_ascii_case(b"XREVRANGE"),
        // len=9
        (9, b'S') => {
            command.eq_ignore_ascii_case(b"SISMEMBER")
                || command.eq_ignore_ascii_case(b"SRANDMEMBER")
        }
        (9, b'Z') => {
            command.eq_ignore_ascii_case(b"ZINCRBY") || command.eq_ignore_ascii_case(b"ZREVRANGE")
        }
        (9, b'G') => {
            command.eq_ignore_ascii_case(b"GEOSEARCH") || command.eq_ignore_ascii_case(b"GEOHASH")
        }
        (9, b'J') => command.eq_ignore_ascii_case(b"JSON.TYPE"),
        (9, b'X') => command.eq_ignore_ascii_case(b"XREVRANGE"),
        // len=10
        (10, b'S') => command.eq_ignore_ascii_case(b"SMISMEMBER"),
        (10, b'P') => command.eq_ignore_ascii_case(b"PFCOUNT"),
        // len=11
        (11, b'S') => command.eq_ignore_ascii_case(b"SRANDMEMBER"),
        (11, b'I') => command.eq_ignore_ascii_case(b"INCRBYFLOAT"),
        // len=12
        (12, b'H') => command.eq_ignore_ascii_case(b"HINCRBYFLOAT"),
        // len=13
        (13, b'Z') => command.eq_ignore_ascii_case(b"ZRANGEBYSCORE"),
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cluster::{NodeInfo, NodeRole, SlotRange, Topology};

    fn config() -> ClusterConfig {
        let local_slot = hash_slot(b"local");
        let remote_slot = hash_slot(b"remote");
        ClusterConfig {
            enabled: true,
            local_id: "a".into(),
            listen_address: "127.0.0.1:18000".into(),
            peer_queue_capacity: 16,
            heartbeat_interval: std::time::Duration::from_secs(2),
            suspect_timeout: std::time::Duration::from_secs(6),
            failure_quorum: 2,
            max_inbound_peers: 16,
            auth_token: None,
            replication_log_capacity: 16,
            is_replica: false,
            nodes_config_file: String::new(),
            topology: Topology::new(
                1,
                vec![
                    NodeInfo {
                        id: "a".into(),
                        address: "a:8000".into(),
                        cluster_address: "a:18000".into(),
                        role: NodeRole::Primary,
                        replica_of: None,
                        epoch: 1,
                        slots: vec![SlotRange::new(local_slot, local_slot).unwrap()],
                    },
                    NodeInfo {
                        id: "b".into(),
                        address: "b:8000".into(),
                        cluster_address: "b:18000".into(),
                        role: NodeRole::Primary,
                        replica_of: None,
                        epoch: 1,
                        slots: vec![SlotRange::new(remote_slot, remote_slot).unwrap()],
                    },
                ],
            ),
        }
    }

    #[test]
    fn routes_local_remote_and_cross_slot() {
        let config = config();
        assert_eq!(
            route_command(&config, b"GET", &[b"local"]),
            RouteDecision::Local
        );
        assert!(matches!(
            route_command(&config, b"GET", &[b"remote"]),
            RouteDecision::Moved { .. }
        ));
        assert_eq!(
            route_command(&config, b"MGET", &[b"local", b"remote"]),
            RouteDecision::CrossSlot
        );
        assert_eq!(
            route_command(&config, b"MGET", &[b"a{same}", b"b{same}"]),
            RouteDecision::Unassigned(hash_slot(b"{same}"))
        );
    }

    #[test]
    fn classifies_replica_write_commands_without_marking_reads() {
        assert!(is_write_command(b"set"));
        assert!(is_write_command(b"JSON.ARRAPPEND"));
        assert!(is_write_command(b"FLUSHALL"));
        assert!(!is_write_command(b"GET"));
        assert!(!is_write_command(b"INFO"));
        assert!(!is_write_command(b"PUBLISH"));
    }
}
