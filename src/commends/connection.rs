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
    match parts {
        [_, sub] if sub.eq_ignore_ascii_case("MYID") => resp::write_bulk(out, &cluster.local_id),
        [_, sub] if sub.eq_ignore_ascii_case("INFO") => {
            let assigned: usize = cluster
                .topology
                .nodes
                .iter()
                .filter(|node| node.role == crate::cluster::NodeRole::Primary)
                .flat_map(|node| node.slots.iter())
                .map(|range| usize::from(range.end.value() - range.start.value()) + 1)
                .sum();
            let state = if cluster.topology.is_complete() {
                "ok"
            } else {
                "fail"
            };
            resp::write_bulk(
                out,
                &format!(
                    "cluster_state:{state}\r\ncluster_slots_assigned:{assigned}\r\ncluster_slots_ok:{assigned}\r\ncluster_slots_pfail:0\r\ncluster_slots_fail:0\r\ncluster_known_nodes:{}\r\ncluster_size:{}\r\ncluster_current_epoch:{}\r\ncluster_my_epoch:{}\r\n",
                    cluster.topology.nodes.len(),
                    cluster
                        .topology
                        .nodes
                        .iter()
                        .filter(|node| node.role == crate::cluster::NodeRole::Primary)
                        .count(),
                    cluster.topology.epoch,
                    cluster.local_node().map_or(0, |node| node.epoch),
                ),
            );
        }
        [_, sub] if sub.eq_ignore_ascii_case("NODES") => {
            let mut text = String::new();
            for node in &cluster.topology.nodes {
                let flags = if node.id == cluster.local_id {
                    "myself,master"
                } else {
                    "master"
                };
                let slots = node
                    .slots
                    .iter()
                    .map(|range| {
                        if range.start == range.end {
                            range.start.value().to_string()
                        } else {
                            format!("{}-{}", range.start.value(), range.end.value())
                        }
                    })
                    .collect::<Vec<_>>()
                    .join(" ");
                text.push_str(&format!(
                    "{} {}@{} {} - 0 0 {} connected {}\n",
                    node.id, node.address, node.cluster_address, flags, node.epoch, slots
                ));
            }
            resp::write_bulk(out, &text);
        }
        [_, sub] if sub.eq_ignore_ascii_case("SLOTS") => write_cluster_slots(cluster, out),
        _ => resp::write_err(
            out,
            "Unknown CLUSTER subcommand or wrong number of arguments",
        ),
    }
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
        resp::write_bulk(out, "0.1.0");
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
