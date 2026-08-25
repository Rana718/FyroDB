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
    let decision = route_command_with_topology(cluster, topology.as_ref(), command, args);
    if allow_import
        && let Some(pattern) = key_pattern(command)
        && let Some(index) = pattern.indices(args.len()).next()
        && state.is_importing(hash_slot(args[index]))
    {
        return RouteDecision::Local;
    }
    match decision {
        RouteDecision::Moved { slot, address } => {
            return RouteDecision::MovedOwned {
                slot,
                address: address.to_owned(),
            };
        }
        RouteDecision::CrossSlot => return RouteDecision::CrossSlot,
        RouteDecision::Unassigned(slot) => return RouteDecision::Unassigned(slot),
        RouteDecision::Ask { slot, address } => return RouteDecision::Ask { slot, address },
        RouteDecision::MovedOwned { slot, address } => {
            return RouteDecision::MovedOwned { slot, address };
        }
        RouteDecision::Local => {}
    }
    let Some(pattern) = key_pattern(command) else {
        return RouteDecision::Local;
    };
    let Some(index) = pattern.indices(args.len()).next() else {
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

/// Commands that can change the replicated keyspace. This stays byte based so
/// the connection hot path does not allocate or require UTF-8 conversion.
pub fn is_write_command(command: &[u8]) -> bool {
    const COMMANDS: &[&[u8]] = &[
        b"FLUSH",
        b"FLUSHALL",
        b"FLUSHDB",
        b"SORT",
        b"SET",
        b"SETNX",
        b"SETEX",
        b"PSETEX",
        b"GETDEL",
        b"GETSET",
        b"GETEX",
        b"MSET",
        b"MSETNX",
        b"INCR",
        b"DECR",
        b"INCRBY",
        b"DECRBY",
        b"INCRBYFLOAT",
        b"APPEND",
        b"SETRANGE",
        b"DEL",
        b"UNLINK",
        b"EXPIRE",
        b"PEXPIRE",
        b"EXPIREAT",
        b"PERSIST",
        b"RENAME",
        b"RENAMENX",
        b"COPY",
        b"HSET",
        b"HSETNX",
        b"HMSET",
        b"HDEL",
        b"HINCRBY",
        b"HINCRBYFLOAT",
        b"LPUSH",
        b"RPUSH",
        b"LPOP",
        b"RPOP",
        b"LSET",
        b"LTRIM",
        b"LREM",
        b"LINSERT",
        b"LMOVE",
        b"RPOPLPUSH",
        b"BLPOP",
        b"BRPOP",
        b"BLMOVE",
        b"SADD",
        b"SREM",
        b"SPOP",
        b"SMOVE",
        b"SUNIONSTORE",
        b"SINTERSTORE",
        b"SDIFFSTORE",
        b"ZADD",
        b"ZREM",
        b"ZINCRBY",
        b"ZRANGESTORE",
        b"ZPOPMIN",
        b"ZPOPMAX",
        b"BZPOPMIN",
        b"BZPOPMAX",
        b"ZUNIONSTORE",
        b"ZINTERSTORE",
        b"ZDIFFSTORE",
        b"SETBIT",
        b"BITOP",
        b"BITFIELD",
        b"PFADD",
        b"PFMERGE",
        b"JSON.SET",
        b"JSON.DEL",
        b"JSON.FORGET",
        b"JSON.NUMINCRBY",
        b"JSON.NUMMULTBY",
        b"JSON.STRAPPEND",
        b"JSON.ARRAPPEND",
        b"JSON.ARRINSERT",
        b"JSON.ARRPOP",
        b"JSON.ARRTRIM",
        b"JSON.TOGGLE",
        b"JSON.CLEAR",
        b"GEOADD",
        b"GEOSEARCHSTORE",
        b"XADD",
        b"XTRIM",
        b"XDEL",
        b"XGROUP",
        b"XACK",
    ];
    COMMANDS
        .iter()
        .any(|known| command.eq_ignore_ascii_case(known))
}

fn is_single_key(command: &[u8]) -> bool {
    const COMMANDS: &[&[u8]] = &[
        b"GET",
        b"SET",
        b"SETNX",
        b"SETEX",
        b"PSETEX",
        b"GETDEL",
        b"GETSET",
        b"GETEX",
        b"INCR",
        b"DECR",
        b"INCRBY",
        b"DECRBY",
        b"INCRBYFLOAT",
        b"APPEND",
        b"STRLEN",
        b"GETRANGE",
        b"SETRANGE",
        b"TYPE",
        b"TTL",
        b"PTTL",
        b"EXPIRE",
        b"PEXPIRE",
        b"EXPIREAT",
        b"PERSIST",
        b"HSET",
        b"HGET",
        b"HMGET",
        b"HMSET",
        b"HGETALL",
        b"HDEL",
        b"HEXISTS",
        b"HLEN",
        b"HKEYS",
        b"HVALS",
        b"HINCRBY",
        b"HINCRBYFLOAT",
        b"LPUSH",
        b"RPUSH",
        b"LPOP",
        b"RPOP",
        b"LLEN",
        b"LINDEX",
        b"LSET",
        b"LRANGE",
        b"LTRIM",
        b"LREM",
        b"LINSERT",
        b"LPOS",
        b"SADD",
        b"SREM",
        b"SISMEMBER",
        b"SMISMEMBER",
        b"SMEMBERS",
        b"SCARD",
        b"SPOP",
        b"SRANDMEMBER",
        b"ZADD",
        b"ZREM",
        b"ZSCORE",
        b"ZMSCORE",
        b"ZRANK",
        b"ZREVRANK",
        b"ZCARD",
        b"ZCOUNT",
        b"ZINCRBY",
        b"ZRANGE",
        b"ZREVRANGE",
        b"ZRANGEBYSCORE",
        b"ZPOPMIN",
        b"ZPOPMAX",
        b"SETBIT",
        b"GETBIT",
        b"BITCOUNT",
        b"BITPOS",
        b"PFADD",
        b"PFCOUNT",
        b"JSON.SET",
        b"JSON.GET",
        b"JSON.DEL",
        b"JSON.TYPE",
        b"XADD",
        b"XLEN",
        b"XRANGE",
        b"XREVRANGE",
        b"XDEL",
        b"GEOADD",
        b"GEOPOS",
        b"GEODIST",
        b"GEOHASH",
        b"GEOSEARCH",
    ];
    COMMANDS
        .iter()
        .any(|known| command.eq_ignore_ascii_case(known))
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
