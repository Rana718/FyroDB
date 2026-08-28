use crate::parse_int;
use crate::storage::rdb;
use crate::storage::store::Store;
use crate::utils::resp;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

static BGSAVE_IN_PROGRESS: AtomicBool = AtomicBool::new(false);

pub fn ping(parts: &[&str], out: &mut Vec<u8>) {
    match parts {
        [_] => out.extend_from_slice(resp::PONG),
        [_, msg] => resp::write_bulk(out, msg),
        _ => resp::write_wrong_args(out, "ping"),
    }
}

pub fn echo(parts: &[&str], out: &mut Vec<u8>) {
    match parts {
        [_, msg] => resp::write_bulk(out, msg),
        _ => resp::write_wrong_args(out, "echo"),
    }
}

pub fn info(store: &Store, out: &mut Vec<u8>) {
    resp::write_bulk(out, &store.info());
}

pub fn flush(store: &Store, out: &mut Vec<u8>) {
    store.flush();
    resp::write_ok(out);
}

pub fn dbsize(store: &Store, out: &mut Vec<u8>) {
    resp::write_integer(out, store.dbsize() as i64);
}

pub fn type_of(parts: &[&str], store: &Store, out: &mut Vec<u8>) {
    match parts {
        [_, key] => resp::write_simple(out, store.type_of(key)),
        _ => resp::write_wrong_args(out, "type"),
    }
}

pub fn bgsave(store: &Arc<Store>, out: &mut Vec<u8>) {
    if BGSAVE_IN_PROGRESS.swap(true, Ordering::AcqRel) {
        return resp::write_err(out, "Background save already in progress");
    }
    let store = Arc::clone(store);
    let path = std::env::var("FYRODB_RDB_PATH").unwrap_or_else(|_| "fyrodb.rdb".to_string());
    std::thread::Builder::new()
        .name("fyrodb-bgsave".into())
        .spawn(move || {
            match rdb::save(&store, &path) {
                Ok(()) => eprintln!("[rdb] BGSAVE complete"),
                Err(e) => eprintln!("[rdb] BGSAVE error: {e}"),
            }
            BGSAVE_IN_PROGRESS.store(false, Ordering::Release);
        })
        .ok();
    resp::write_simple(out, "Background saving started");
}

pub fn time(_parts: &[&str], out: &mut Vec<u8>) {
    use std::time::{SystemTime, UNIX_EPOCH};
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    let secs = now.as_secs().to_string();
    let micros = (now.subsec_micros()).to_string();
    resp::write_array_header(out, 2);
    resp::write_bulk(out, &secs);
    resp::write_bulk(out, &micros);
}

pub fn flushall(store: &Store, out: &mut Vec<u8>) {
    store.flush();
    resp::write_ok(out);
}

pub fn flushdb(store: &Store, out: &mut Vec<u8>) {
    store.flush();
    resp::write_ok(out);
}

pub fn save(store: &Arc<Store>, out: &mut Vec<u8>) {
    let path = std::env::var("FYRODB_RDB_PATH").unwrap_or_else(|_| "fyrodb.rdb".to_string());
    match crate::storage::rdb::save(store, &path) {
        Ok(()) => resp::write_ok(out),
        Err(e) => resp::write_err(out, &format!("save failed: {e}")),
    }
}

pub fn lastsave(_parts: &[&str], out: &mut Vec<u8>) {
    use std::time::{SystemTime, UNIX_EPOCH};
    let ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    resp::write_integer(out, ts as i64);
}

pub fn command_cmd(parts: &[&str], out: &mut Vec<u8>) {
    match parts {
        [_] => {
            resp::write_array_header(out, 0);
        }
        [_, sub] if sub.eq_ignore_ascii_case("COUNT") => {
            resp::write_integer(out, 120);
        }
        [_, sub, ..] if sub.eq_ignore_ascii_case("INFO") => {
            resp::write_array_header(out, 0);
        }
        [_, sub, ..] if sub.eq_ignore_ascii_case("DOCS") => {
            resp::write_array_header(out, 0);
        }
        _ => resp::write_array_header(out, 0),
    }
}

pub fn cluster_cmd(parts: &[&str], store: &Store, out: &mut Vec<u8>) {
    let cluster = &store.cluster;
    if !cluster.enabled {
        return resp::write_err(out, "This instance has cluster support disabled");
    }

    let sub = match parts.get(1) {
        Some(s) => *s,
        None => return resp::write_wrong_args(out, "cluster"),
    };

    if sub.eq_ignore_ascii_case("MYID") {
        return resp::write_bulk(out, &cluster.local_id);
    }

    if sub.eq_ignore_ascii_case("INFO") {
        let topology = store.cluster_topology();
        let assigned: usize = topology
            .nodes
            .iter()
            .filter(|n| n.role == crate::cluster::NodeRole::Primary)
            .flat_map(|n| n.slots.iter())
            .map(|r| usize::from(r.end.value() - r.start.value()) + 1)
            .sum();
        let state_str = if topology.is_complete() { "ok" } else { "fail" };
        let total_nodes = topology.nodes.len();
        let primary_count = topology
            .nodes
            .iter()
            .filter(|n| n.role == crate::cluster::NodeRole::Primary)
            .count();
        let my_epoch = topology
            .nodes
            .iter()
            .find(|n| n.id == cluster.local_id)
            .map_or(0, |n| n.epoch);
        let info = format!(
            "cluster_enabled:1\r\ncluster_state:{state_str}\r\ncluster_slots_assigned:{assigned}\r\ncluster_slots_ok:{assigned}\r\ncluster_slots_pfail:0\r\ncluster_slots_fail:0\r\ncluster_known_nodes:{total_nodes}\r\ncluster_size:{primary_count}\r\ncluster_current_epoch:{}\r\ncluster_my_epoch:{my_epoch}\r\ntotal_cluster_links_established:0\r\n",
            topology.epoch,
        );
        return resp::write_bulk(out, &info);
    }

    if sub.eq_ignore_ascii_case("NODES") {
        let topology = store.cluster_topology();
        let mut text = String::new();
        for node in &topology.nodes {
            let flags = match (node.id == cluster.local_id, node.role == crate::cluster::NodeRole::Replica) {
                (true, true) => "myself,slave",
                (true, false) => "myself,master",
                (false, true) => "slave",
                (false, false) => "master",
            };
            let master = node.replica_of.as_deref().unwrap_or("-");
            let slots: Vec<String> = node.slots.iter().map(|r| {
                if r.start == r.end {
                    r.start.value().to_string()
                } else {
                    format!("{}-{}", r.start.value(), r.end.value())
                }
            }).collect();
            text.push_str(&format!(
                "{} {}@{} {} {} {} 0 {} connected {}\n",
                node.id, node.address, node.cluster_address,
                flags, master, node.epoch, node.epoch,
                slots.join(" ")
            ));
        }
        return resp::write_bulk(out, &text);
    }

    if sub.eq_ignore_ascii_case("SLOTS") {
        return write_cluster_slots(cluster, out);
    }

    if sub.eq_ignore_ascii_case("SHARDS") {
        return write_cluster_shards(store, out);
    }

    if sub.eq_ignore_ascii_case("MEET") {
        // CLUSTER MEET <ip> <port> [cluster-port]
        let (ip, port) = match parts {
            [_, _, ip, port, ..] => (*ip, *port),
            _ => return resp::write_wrong_args(out, "cluster meet"),
        };
        let client_port: u16 = match port.parse() {
            Ok(p) => p,
            Err(_) => return resp::write_err(out, "invalid port"),
        };
        let cluster_port: u16 = parts
            .get(4)
            .and_then(|p| p.parse().ok())
            .unwrap_or(client_port + 10000);
        let address = format!("{ip}:{client_port}");
        let cluster_address = format!("{ip}:{cluster_port}");
        let new_id = format!("{ip}:{client_port}");
        let new_node = crate::cluster::NodeInfo {
            id: new_id.clone(),
            address,
            cluster_address,
            role: crate::cluster::NodeRole::Primary,
            replica_of: None,
            epoch: 1,
            slots: Vec::new(),
        };
        if let Some(topology) = store.cluster_state_ref().meet_node(new_node) {
            let _ = crate::cluster::save_nodes_conf(
                &cluster.nodes_config_file,
                &topology,
                &cluster.local_id,
            );
        }
        return resp::write_ok(out);
    }

    if sub.eq_ignore_ascii_case("ADDSLOTS") {
        if parts.len() < 3 {
            return resp::write_wrong_args(out, "cluster addslots");
        }
        let mut slots = Vec::new();
        for raw in &parts[2..] {
            match raw.parse::<u16>() {
                Ok(n) => match crate::cluster::Slot::new(n) {
                    Some(s) => slots.push(s),
                    None => return resp::write_err(out, "Invalid slot number"),
                },
                Err(_) => return resp::write_err(out, "Invalid slot number"),
            }
        }
        match store.cluster_state_ref().add_slots(&cluster.local_id, &slots) {
            Some(topology) => {
                let _ = crate::cluster::save_nodes_conf(
                    &cluster.nodes_config_file,
                    &topology,
                    &cluster.local_id,
                );
                return resp::write_ok(out);
            }
            None => return resp::write_err(out, "Slot already assigned or node not found"),
        }
    }

    if sub.eq_ignore_ascii_case("ADDSLOTSRANGE") {
        if parts.len() < 4 || !(parts.len() - 2).is_multiple_of(2) {
            return resp::write_wrong_args(out, "cluster addslotsrange");
        }
        let mut slots = Vec::new();
        let mut i = 2;
        while i + 1 < parts.len() {
            let start: u16 = match parts[i].parse() {
                Ok(n) => n,
                Err(_) => return resp::write_err(out, "Invalid slot number"),
            };
            let end: u16 = match parts[i + 1].parse() {
                Ok(n) => n,
                Err(_) => return resp::write_err(out, "Invalid slot number"),
            };
            for n in start..=end {
                match crate::cluster::Slot::new(n) {
                    Some(s) => slots.push(s),
                    None => return resp::write_err(out, "Invalid slot number"),
                }
            }
            i += 2;
        }
        match store.cluster_state_ref().add_slots(&cluster.local_id, &slots) {
            Some(topology) => {
                let _ = crate::cluster::save_nodes_conf(
                    &cluster.nodes_config_file,
                    &topology,
                    &cluster.local_id,
                );
                return resp::write_ok(out);
            }
            None => return resp::write_err(out, "Slot already assigned or node not found"),
        }
    }

    if sub.eq_ignore_ascii_case("DELSLOTS") {
        if parts.len() < 3 {
            return resp::write_wrong_args(out, "cluster delslots");
        }
        let mut slots = Vec::new();
        for raw in &parts[2..] {
            match raw.parse::<u16>() {
                Ok(n) => match crate::cluster::Slot::new(n) {
                    Some(s) => slots.push(s),
                    None => return resp::write_err(out, "Invalid slot number"),
                },
                Err(_) => return resp::write_err(out, "Invalid slot number"),
            }
        }
        if let Some(topology) =
            store.cluster_state_ref().del_slots(&cluster.local_id, &slots)
        {
            let _ = crate::cluster::save_nodes_conf(
                &cluster.nodes_config_file,
                &topology,
                &cluster.local_id,
            );
        }
        return resp::write_ok(out);
    }

    if sub.eq_ignore_ascii_case("DELSLOTSRANGE") {
        if parts.len() < 4 || !(parts.len() - 2).is_multiple_of(2) {
            return resp::write_wrong_args(out, "cluster delslotsrange");
        }
        let mut slots = Vec::new();
        let mut i = 2;
        while i + 1 < parts.len() {
            let start: u16 = match parts[i].parse() {
                Ok(n) => n,
                Err(_) => return resp::write_err(out, "Invalid slot number"),
            };
            let end: u16 = match parts[i + 1].parse() {
                Ok(n) => n,
                Err(_) => return resp::write_err(out, "Invalid slot number"),
            };
            for n in start..=end {
                if let Some(s) = crate::cluster::Slot::new(n) {
                    slots.push(s);
                }
            }
            i += 2;
        }
        if let Some(topology) =
            store.cluster_state_ref().del_slots(&cluster.local_id, &slots)
        {
            let _ = crate::cluster::save_nodes_conf(
                &cluster.nodes_config_file,
                &topology,
                &cluster.local_id,
            );
        }
        return resp::write_ok(out);
    }

    if sub.eq_ignore_ascii_case("FORGET") {
        let node_id = match parts.get(2) {
            Some(id) => *id,
            None => return resp::write_wrong_args(out, "cluster forget"),
        };
        if node_id == cluster.local_id {
            return resp::write_err(out, "Can't forget myself");
        }
        if let Some(topology) = store.cluster_state_ref().forget_node(node_id) {
            let _ = crate::cluster::save_nodes_conf(
                &cluster.nodes_config_file,
                &topology,
                &cluster.local_id,
            );
        }
        return resp::write_ok(out);
    }

    if sub.eq_ignore_ascii_case("RESET") {
        let hard = parts.get(2).is_some_and(|m| m.eq_ignore_ascii_case("HARD"));
        if hard {
            store.flush();
        }
        let topology = store.cluster_state_ref().reset_slots(&cluster.local_id);
        let _ = crate::cluster::save_nodes_conf(
            &cluster.nodes_config_file,
            &topology,
            &cluster.local_id,
        );
        return resp::write_ok(out);
    }

    if sub.eq_ignore_ascii_case("KEYSLOT") {
        let key = match parts.get(2) {
            Some(k) => *k,
            None => return resp::write_wrong_args(out, "cluster keyslot"),
        };
        let slot = crate::cluster::hash_slot(key.as_bytes());
        return resp::write_integer(out, slot.value() as i64);
    }

    if sub.eq_ignore_ascii_case("COUNTKEYSINSLOT") {
        let slot_n: u16 = match parts.get(2).and_then(|s| s.parse().ok()) {
            Some(n) => n,
            None => return resp::write_wrong_args(out, "cluster countkeysinslot"),
        };
        let slot = match crate::cluster::Slot::new(slot_n) {
            Some(s) => s,
            None => return resp::write_err(out, "Invalid slot number"),
        };
        let count = store.count_keys_in_slot(slot);
        return resp::write_integer(out, count as i64);
    }

    if sub.eq_ignore_ascii_case("GETKEYSINSLOT") {
        let slot_n: u16 = match parts.get(2).and_then(|s| s.parse().ok()) {
            Some(n) => n,
            None => return resp::write_wrong_args(out, "cluster getkeysinslot"),
        };
        let count: usize = match parts.get(3).and_then(|s| s.parse().ok()) {
            Some(n) => n,
            None => return resp::write_wrong_args(out, "cluster getkeysinslot"),
        };
        let slot = match crate::cluster::Slot::new(slot_n) {
            Some(s) => s,
            None => return resp::write_err(out, "Invalid slot number"),
        };
        let keys = store.keys_in_slot(slot, count);
        resp::write_array_header(out, keys.len());
        for k in &keys {
            resp::write_bulk(out, k);
        }
        return;
    }

    if sub.eq_ignore_ascii_case("REPLICATE") {
        let primary_id = match parts.get(2) {
            Some(id) => *id,
            None => return resp::write_wrong_args(out, "cluster replicate"),
        };
        let topology = store.cluster_topology();
        if !topology.nodes.iter().any(|n| n.id == primary_id && n.role == crate::cluster::NodeRole::Primary) {
            return resp::write_err(out, "Unknown node or not a primary");
        }
        return resp::write_ok(out);
    }

    if sub.eq_ignore_ascii_case("SETSLOT") {
        // CLUSTER SETSLOT <slot> IMPORTING|MIGRATING|STABLE|NODE <node-id>
        let slot_n: u16 = match parts.get(2).and_then(|s| s.parse().ok()) {
            Some(n) => n,
            None => return resp::write_wrong_args(out, "cluster setslot"),
        };
        let slot = match crate::cluster::Slot::new(slot_n) {
            Some(s) => s,
            None => return resp::write_err(out, "Invalid slot number"),
        };
        let state_arg = match parts.get(3) {
            Some(s) => *s,
            None => return resp::write_wrong_args(out, "cluster setslot"),
        };
        if state_arg.eq_ignore_ascii_case("STABLE") {
            store.cluster_state_ref().finish_slot_migration(slot);
            store.cluster_state_ref().finish_slot_import(slot);
        } else if state_arg.eq_ignore_ascii_case("IMPORTING") {
            let source = parts.get(4).copied().unwrap_or("");
            store.cluster_state_ref().begin_slot_import(slot, source.to_owned());
        } else if state_arg.eq_ignore_ascii_case("MIGRATING") {
            let target = parts.get(4).copied().unwrap_or("");
            store.cluster_state_ref().begin_slot_migration(slot, target.to_owned());
        } else if state_arg.eq_ignore_ascii_case("NODE") {
            let target = match parts.get(4) {
                Some(id) => *id,
                None => return resp::write_wrong_args(out, "cluster setslot"),
            };
            if let Some(topology) = store.cluster_state_ref().commit_slot_migration(slot) {
                let _ = crate::cluster::save_nodes_conf(
                    &cluster.nodes_config_file,
                    &topology,
                    &cluster.local_id,
                );
                let _ = store.install_cluster_topology(topology);
            } else if let Some(topology) = store.cluster_state_ref().assign_slot(slot, target) {
                // Non-source nodes still need to record the new owner,
                // otherwise they keep redirecting the slot to the old one.
                let _ = crate::cluster::save_nodes_conf(
                    &cluster.nodes_config_file,
                    &topology,
                    &cluster.local_id,
                );
                let _ = store.install_cluster_topology(topology);
            }
        }
        return resp::write_ok(out);
    }

    if sub.eq_ignore_ascii_case("FAILOVER") {
        return resp::write_ok(out);
    }

    if sub.eq_ignore_ascii_case("SAVECONFIG") {
        let topology = store.cluster_topology();
        match crate::cluster::save_nodes_conf(
            &cluster.nodes_config_file,
            &topology,
            &cluster.local_id,
        ) {
            Ok(()) => return resp::write_ok(out),
            Err(e) => return resp::write_err(out, &format!("save failed: {e}")),
        }
    }

    if sub.eq_ignore_ascii_case("FLUSHSLOTS") {
        let topology = store.cluster_state_ref().reset_slots(&cluster.local_id);
        let _ = crate::cluster::save_nodes_conf(
            &cluster.nodes_config_file,
            &topology,
            &cluster.local_id,
        );
        return resp::write_ok(out);
    }

    resp::write_err(out, "Unknown CLUSTER subcommand or wrong number of arguments")
}

fn write_cluster_slots(cluster: &crate::cluster::ClusterConfig, out: &mut Vec<u8>) {
    let ranges: Vec<_> = cluster
        .topology
        .nodes
        .iter()
        .filter(|node| node.role == crate::cluster::NodeRole::Primary)
        .flat_map(|node| node.slots.iter().map(move |range| (node, range)))
        .collect();
    resp::write_array_header(out, ranges.len());
    for (node, range) in ranges {
        let (host, port) = split_advertised_address(&node.address);
        resp::write_array_header(out, 3);
        resp::write_integer(out, range.start.value() as i64);
        resp::write_integer(out, range.end.value() as i64);
        resp::write_array_header(out, 3);
        resp::write_bulk(out, host);
        resp::write_integer(out, port as i64);
        resp::write_bulk(out, &node.id);
    }
}

fn write_cluster_shards(store: &Store, out: &mut Vec<u8>) {
    let topology = store.cluster_topology();
    let primaries: Vec<_> = topology
        .nodes
        .iter()
        .filter(|n| n.role == crate::cluster::NodeRole::Primary)
        .collect();
    resp::write_array_header(out, primaries.len());
    for node in primaries {
        let replicas: Vec<_> = topology
            .nodes
            .iter()
            .filter(|n| n.replica_of.as_deref() == Some(node.id.as_str()))
            .collect();
        let slots_flat: Vec<u16> = node
            .slots
            .iter()
            .flat_map(|r| [r.start.value(), r.end.value()])
            .collect();
        resp::write_array_header(out, 2);
        resp::write_bulk(out, "slots");
        resp::write_array_header(out, slots_flat.len());
        for s in &slots_flat {
            resp::write_integer(out, *s as i64);
        }
        resp::write_bulk(out, "nodes");
        resp::write_array_header(out, 1 + replicas.len());
        for n in std::iter::once(node).chain(replicas) {
            let (host, port) = split_advertised_address(&n.address);
            resp::write_array_header(out, 4);
            resp::write_bulk(out, "id");
            resp::write_bulk(out, &n.id);
            resp::write_bulk(out, "endpoint");
            resp::write_bulk(out, host);
            resp::write_bulk(out, "port");
            resp::write_integer(out, port as i64);
            resp::write_bulk(out, "role");
            resp::write_bulk(out, if n.role == crate::cluster::NodeRole::Replica { "replica" } else { "master" });
        }
    }
}

fn split_advertised_address(address: &str) -> (&str, u16) {
    address
        .rsplit_once(':')
        .and_then(|(host, port)| port.parse().ok().map(|port| (host, port)))
        .unwrap_or((address, 0))
}

pub fn quit(out: &mut Vec<u8>) {
    resp::write_ok(out);
}

pub fn hello(_parts: &[&str], out: &mut Vec<u8>) {
    let proto = _parts
        .get(1)
        .and_then(|p| p.parse::<u32>().ok())
        .unwrap_or(2);
    if proto == 3 {
        crate::utils::resp3::write_hello_resp3(out);
    } else {
        resp::write_array_header(out, 14);
        resp::write_bulk(out, "server");
        resp::write_bulk(out, "fyrodb");
        resp::write_bulk(out, "version");
        resp::write_bulk(out, env!("CARGO_PKG_VERSION"));
        resp::write_bulk(out, "proto");
        resp::write_integer(out, 2);
        resp::write_bulk(out, "id");
        resp::write_integer(out, 1);
        resp::write_bulk(out, "mode");
        resp::write_bulk(out, "standalone");
        resp::write_bulk(out, "role");
        resp::write_bulk(out, "master");
        resp::write_bulk(out, "modules");
        resp::write_array_header(out, 0);
    }
}

pub fn select(parts: &[&str], out: &mut Vec<u8>) {
    match parts {
        [_, idx] => match idx.parse::<u32>() {
            Ok(0) => resp::write_ok(out),
            Ok(_) => resp::write_err(out, "FyroDB only supports DB 0"),
            Err(_) => resp::write_err(out, "value is not an integer or out of range"),
        },
        _ => resp::write_wrong_args(out, "select"),
    }
}

pub fn auth(_parts: &[&str], out: &mut Vec<u8>) {
    resp::write_ok(out);
}

pub fn reset(out: &mut Vec<u8>) {
    resp::write_simple(out, "RESET");
}

pub fn client_cmd(parts: &[&str], out: &mut Vec<u8>) {
    match parts {
        [_, sub] if sub.eq_ignore_ascii_case("ID") => resp::write_integer(out, 1),
        [_, sub] if sub.eq_ignore_ascii_case("GETNAME") => resp::write_nil(out),
        [_, sub, ..] if sub.eq_ignore_ascii_case("SETNAME") => resp::write_ok(out),
        [_, sub] if sub.eq_ignore_ascii_case("LIST") => {
            resp::write_bulk(out, "id=1 fd=0 name= db=0 cmd=client\r\n")
        }
        [_, sub] if sub.eq_ignore_ascii_case("INFO") => {
            resp::write_bulk(out, "id=1 fd=0 name= db=0 cmd=client\r\n")
        }
        [_, sub, ..] if sub.eq_ignore_ascii_case("KILL") => resp::write_ok(out),
        [_, sub, ..] if sub.eq_ignore_ascii_case("TRACKING") => resp::write_ok(out),
        [_, sub, ..] if sub.eq_ignore_ascii_case("CACHING") => resp::write_ok(out),
        _ => resp::write_ok(out),
    }
}

pub fn config_cmd(parts: &[&str], out: &mut Vec<u8>) {
    match parts {
        [_, sub, ..] if sub.eq_ignore_ascii_case("GET") => {
            resp::write_array_header(out, 0);
        }
        [_, sub, ..] if sub.eq_ignore_ascii_case("SET") => {
            resp::write_ok(out);
        }
        [_, sub] if sub.eq_ignore_ascii_case("RESETSTAT") => {
            resp::write_ok(out);
        }
        _ => resp::write_ok(out),
    }
}

pub fn slowlog_cmd(parts: &[&str], out: &mut Vec<u8>) {
    match parts {
        [_, sub] if sub.eq_ignore_ascii_case("LEN") => resp::write_integer(out, 0),
        [_, sub] if sub.eq_ignore_ascii_case("RESET") => resp::write_ok(out),
        [_, sub, ..] if sub.eq_ignore_ascii_case("GET") => {
            resp::write_array_header(out, 0);
        }
        _ => resp::write_ok(out),
    }
}

pub fn acl_cmd(parts: &[&str], out: &mut Vec<u8>) {
    match parts {
        [_, sub] if sub.eq_ignore_ascii_case("WHOAMI") => resp::write_bulk(out, "default"),
        [_, sub] if sub.eq_ignore_ascii_case("LIST") => {
            resp::write_array_header(out, 1);
            resp::write_bulk(out, "user default on ~* &* +@all");
        }
        _ => resp::write_ok(out),
    }
}

pub fn object_cmd(parts: &[&str], store: &Store, out: &mut Vec<u8>) {
    match parts {
        [_, sub, key] if sub.eq_ignore_ascii_case("ENCODING") => match store.data.get_ref(key) {
            None => resp::write_err(out, "no such key"),
            Some(e) if e.is_expired() => resp::write_err(out, "no such key"),
            Some(e) => {
                let encoding = match &e.value {
                    crate::storage::value::FyroDB::String(s) => {
                        if s.parse::<i64>().is_ok() {
                            "int"
                        } else {
                            "embstr"
                        }
                    }
                    crate::storage::value::FyroDB::Hash(h) => {
                        if h.len() <= 128 {
                            "listpack"
                        } else {
                            "hashtable"
                        }
                    }
                    crate::storage::value::FyroDB::List(l) => {
                        if l.len() <= 128 {
                            "listpack"
                        } else {
                            "quicklist"
                        }
                    }
                    crate::storage::value::FyroDB::Set(s) => {
                        if s.len() <= 128 {
                            "listpack"
                        } else {
                            "hashtable"
                        }
                    }
                    crate::storage::value::FyroDB::ZSet(z) => {
                        if z.len() <= 128 {
                            "listpack"
                        } else {
                            "skiplist"
                        }
                    }
                    crate::storage::value::FyroDB::Json(_) => "raw",
                    crate::storage::value::FyroDB::Stream(_) => "stream",
                };
                resp::write_bulk(out, encoding);
            }
        },
        [_, sub, _key] if sub.eq_ignore_ascii_case("REFCOUNT") => {
            resp::write_integer(out, 1);
        }
        [_, sub, _key] if sub.eq_ignore_ascii_case("IDLETIME") => {
            resp::write_integer(out, 0);
        }
        [_, sub, _key] if sub.eq_ignore_ascii_case("FREQ") => {
            resp::write_integer(out, 0);
        }
        [_, sub] if sub.eq_ignore_ascii_case("HELP") => {
            resp::write_array_header(out, 0);
        }
        _ => resp::write_wrong_args(out, "object"),
    }
}

pub fn sort_cmd(parts: &[&str], store: &Store, out: &mut Vec<u8>) {
    let [_, key, rest @ ..] = parts else {
        return resp::write_wrong_args(out, "sort");
    };
    let mut alpha = false;
    let mut desc = false;
    let mut limit_offset = 0usize;
    let mut limit_count = 0usize;
    let mut store_dst: Option<&str> = None;
    let mut i = 0;

    while i < rest.len() {
        if rest[i].eq_ignore_ascii_case("ALPHA") {
            alpha = true;
        } else if rest[i].eq_ignore_ascii_case("DESC") {
            desc = true;
        } else if rest[i].eq_ignore_ascii_case("ASC") {
            desc = false;
        } else if rest[i].eq_ignore_ascii_case("LIMIT") {
            if i + 2 >= rest.len() {
                return resp::write_err(out, "syntax error");
            }
            limit_offset = parse_int!(out, rest[i + 1], usize);
            limit_count = parse_int!(out, rest[i + 2], usize);
            i += 2;
        } else if rest[i].eq_ignore_ascii_case("STORE") {
            i += 1;
            if i >= rest.len() {
                return resp::write_err(out, "syntax error");
            }
            store_dst = Some(rest[i]);
        } else if rest[i].eq_ignore_ascii_case("BY") || rest[i].eq_ignore_ascii_case("GET") {
            i += 1;
        }
        i += 1;
    }

    let mut items: Vec<String> = match store.data.get_ref(key) {
        None => vec![],
        Some(e) if e.is_expired() => vec![],
        Some(e) => match &e.value {
            crate::storage::value::FyroDB::List(l) => {
                l.deque().iter().map(|v| v.to_string()).collect()
            }
            crate::storage::value::FyroDB::Set(s) => s.iter().map(|m| m.to_string()).collect(),
            crate::storage::value::FyroDB::ZSet(z) => z.members().map(|m| m.to_string()).collect(),
            _ => {
                return resp::write_err(out, "WRONGTYPE");
            }
        },
    };

    if alpha {
        items.sort();
    } else {
        items.sort_by(|a, b| {
            let fa = a.parse::<f64>().unwrap_or(0.0);
            let fb = b.parse::<f64>().unwrap_or(0.0);
            fa.partial_cmp(&fb).unwrap_or(std::cmp::Ordering::Equal)
        });
    }

    if desc {
        items.reverse();
    }

    if limit_count > 0 {
        items = items
            .into_iter()
            .skip(limit_offset)
            .take(limit_count)
            .collect();
    }

    if let Some(dst) = store_dst {
        let len = items.len();
        let l: std::collections::VecDeque<String> = items.into_iter().collect();
        store
            .data
            .insert(dst.to_string(), crate::storage::value::StoreValue::list(l));
        resp::write_integer(out, len as i64);
    } else {
        resp::write_array(out, &items);
    }
}
