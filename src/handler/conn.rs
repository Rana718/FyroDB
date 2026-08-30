use foldhash::HashSet;
use mio::net::TcpStream;
use std::io::{self, Read, Write};
use std::sync::Arc;

use crate::pubsub::{PubSub, SubSlot, WorkerNotifier};
use crate::storage::store::Store;
use crate::utils::parser::{ParseResult, RespParser};

use super::dispatch::dispatch;
use super::subscription::do_full_unsubscribe;

const SUB_WRITE_BATCH_BYTES: usize = 256 * 1024;
const RETAINED_WRITE_BUFFER: usize = 32 * 1024;

pub enum ConnMode {
    Normal,
    Subscribed {
        slot: Arc<SubSlot>,
        channels: HashSet<String>,
        patterns: HashSet<String>,
    },
}

pub struct Conn<'a> {
    pub stream: TcpStream,
    pub parser: RespParser,
    pub store: &'a Arc<Store>,
    pub pubsub: &'a PubSub,
    pub write_offset: usize,
    pub mode: ConnMode,
    pub token: usize,
    pub notifier: &'a Arc<WorkerNotifier>,
    pub auth_required: Option<&'a str>,
    pub authenticated: bool,
    pub asking: bool,
    pub writable_registered: bool,
    pub worker: usize,
    topology_cache: Option<(
        u64,
        Arc<crate::cluster::Topology>,
        Arc<crate::cluster::RoutingTable>,
    )>,
}

impl<'a> Conn<'a> {
    pub fn new(
        stream: TcpStream,
        store: &'a Arc<Store>,
        pubsub: &'a PubSub,
        token: usize,
        notifier: &'a Arc<WorkerNotifier>,
        auth: Option<&'a str>,
        worker: usize,
    ) -> Self {
        store.client_connected();
        let authenticated = auth.is_none();
        Self {
            stream,
            parser: RespParser::new(),
            store,
            pubsub,
            write_offset: 0,
            mode: ConnMode::Normal,
            token,
            notifier,
            auth_required: auth,
            authenticated,
            asking: false,
            writable_registered: false,
            worker,
            topology_cache: None,
        }
    }

    /// Bring the cached topology up to date. One `Acquire` load in the steady
    /// state; only a version change pays for the lock and `Arc` clones.
    #[inline]
    fn refresh_topology(&mut self) {
        let state = self.store.cluster_state_ref();
        match self.topology_cache.as_ref().map(|(version, ..)| *version) {
            Some(version) => {
                if let Some(fresh) = state.topology_if_newer(version) {
                    self.topology_cache = Some(fresh);
                }
            }
            None => {
                self.topology_cache = state.topology_if_newer(0);
            }
        }
    }

    #[inline]
    fn cached_topology(&self) -> (&crate::cluster::Topology, &crate::cluster::RoutingTable) {
        let (_, topology, routing) = self
            .topology_cache
            .as_ref()
            .expect("refresh_topology must run first");
        (topology.as_ref(), routing.as_ref())
    }

    pub fn do_read(&mut self) -> bool {
        loop {
            let buf = self.parser.read_buf();
            match self.stream.read(buf) {
                Ok(0) => return false,
                Ok(n) => self.parser.did_fill(n),
                Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => break,
                Err(_) => return false,
            }
        }

        loop {
            match self.parser.parse_one() {
                ParseResult::Complete => {
                    let raw_ptr = self.parser.parts_raw.as_ptr();
                    let raw_len = self.parser.parts_raw.len();
                    let raw = unsafe { std::slice::from_raw_parts(raw_ptr, raw_len) };
                    dispatch_raw(self, raw);
                }
                ParseResult::Incomplete => break,
                ParseResult::Error => return false,
            }
        }

        self.parser.release_read_buffer();

        true
    }

    pub fn do_write(&mut self) -> bool {
        if let ConnMode::Subscribed { ref slot, .. } = self.mode
            && self.parser.wbuf.len() < SUB_WRITE_BATCH_BYTES
        {
            slot.drain_into_limit(&mut self.parser.wbuf, SUB_WRITE_BATCH_BYTES);
        }

        if self.parser.wbuf.is_empty() {
            return true;
        }

        loop {
            match self.stream.write(&self.parser.wbuf[self.write_offset..]) {
                Ok(0) => return false,
                Ok(n) => {
                    self.write_offset += n;
                    if self.write_offset >= self.parser.wbuf.len() {
                        self.parser.wbuf.clear();
                        self.write_offset = 0;
                        if self.parser.wbuf.capacity() > RETAINED_WRITE_BUFFER {
                            self.parser.wbuf.shrink_to(RETAINED_WRITE_BUFFER);
                        }
                        return true;
                    }
                }
                Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
                    if self.write_offset > 0 {
                        self.parser.wbuf.drain(..self.write_offset);
                        self.write_offset = 0;
                    }
                    return true;
                }
                Err(_) => return false,
            }
        }
    }

    pub fn has_pending_write(&self) -> bool {
        !self.parser.wbuf.is_empty()
            || matches!(&self.mode, ConnMode::Subscribed { slot, .. } if slot.has_pending())
    }
}

impl Drop for Conn<'_> {
    fn drop(&mut self) {
        do_full_unsubscribe(self);
        self.store.client_disconnected();
    }
}

#[inline(always)]
unsafe fn part_bytes<'a>(part: (*const u8, usize)) -> &'a [u8] {
    unsafe { std::slice::from_raw_parts(part.0, part.1) }
}

#[inline(always)]
fn part_str<'a>(out: &mut Vec<u8>, part: (*const u8, usize)) -> Option<&'a str> {
    let bytes: &'a [u8] = unsafe { part_bytes(part) };
    match std::str::from_utf8(bytes) {
        Ok(s) => Some(s),
        Err(_) => {
            crate::utils::resp::write_err(out, "invalid UTF-8 in request");
            None
        }
    }
}

#[inline(always)]
fn dispatch_raw(conn: &mut Conn<'_>, raw: &[(*const u8, usize)]) {
    if raw.is_empty() {
        return;
    }

    let cmd_len = raw[0].1;
    let cmd: &[u8] = unsafe { part_bytes(raw[0]) };

    if !conn.authenticated {
        handle_unauth(conn, raw, cmd, cmd_len);
        return;
    }

    // Only pay for the write gate when cluster is actually enabled and the
    // command is a write.
    let cluster_is_write = conn.store.cluster.enabled && crate::cluster::is_write_command(cmd);

    if conn.store.at_key_capacity() && crate::storage::capacity::is_denyoom_command(cmd) {
        conn.parser
            .wbuf
            .extend_from_slice(crate::storage::capacity::OOM_REPLY);
        return;
    }

    let store = conn.store;
    let _cluster_write_guard = if cluster_is_write {
        Some(store.begin_write(conn.worker))
    } else {
        None
    };

    if conn.store.cluster.enabled {
        if conn.store.replica_installing() {
            conn.parser
                .wbuf
                .extend_from_slice(b"-CLUSTERDOWN Replica snapshot is installing\r\n");
            return;
        }
        if conn.store.cluster.is_replica && cluster_is_write {
            conn.parser
                .wbuf
                .extend_from_slice(b"-READONLY You can't write against a read only replica.\r\n");
            return;
        }

        conn.refresh_topology();
        let asking = std::mem::take(&mut conn.asking);

        // Only commands that need several keys checked for cross-slot
        // violations pay for materializing an argument slice array; keyless
        // commands and the single-key majority skip it entirely.
        let decision = match crate::cluster::routing_scope(cmd) {
            crate::cluster::RoutingScope::Keyless => crate::cluster::RouteDecision::Local,
            crate::cluster::RoutingScope::FirstKey => match raw.get(1) {
                Some(&part) => {
                    let (topology, routing) = conn.cached_topology();
                    crate::cluster::route_single_key(
                        &conn.store.cluster,
                        conn.store.cluster_state_ref(),
                        topology,
                        routing,
                        unsafe { part_bytes(part) },
                        asking,
                    )
                }
                None => crate::cluster::RouteDecision::Local,
            },
            crate::cluster::RoutingScope::ManyKeys => {
                const STACK_ARGS: usize = 32;
                let mut stack_args = [&[][..]; STACK_ARGS];
                if raw.len().saturating_sub(1) > STACK_ARGS {
                    let args: Vec<&[u8]> = raw[1..]
                        .iter()
                        .map(|&part| unsafe { part_bytes(part) })
                        .collect();
                    route_cached(conn, cmd, &args, asking)
                } else {
                    for (index, part) in raw[1..].iter().enumerate() {
                        stack_args[index] = unsafe { part_bytes(*part) };
                    }
                    let args = &stack_args[..raw.len() - 1];
                    route_cached(conn, cmd, args, asking)
                }
            }
        };
        match decision {
            crate::cluster::RouteDecision::Local => {}
            other => {
                write_route_decision(&mut conn.parser.wbuf, other);
                return;
            }
        }
    }

    if cmd_len == 3 {
        if cmd.eq_ignore_ascii_case(b"SET") && raw.len() >= 3 {
            let out = &mut conn.parser.wbuf;
            let Some(key) = part_str(out, raw[1]) else {
                return;
            };
            let Some(value) = part_str(out, raw[2]) else {
                return;
            };
            if raw.len() == 3 {
                conn.store.set_string(key, value, 0);
                if conn.store.has_replication() {
                    conn.store.record_current_value(key);
                }
                conn.parser.wbuf.extend_from_slice(b"+OK\r\n");
                return;
            }
        } else if cmd.eq_ignore_ascii_case(b"GET") && raw.len() == 2 {
            let out = &mut conn.parser.wbuf;
            let Some(key) = part_str(out, raw[1]) else {
                return;
            };
            if !conn.store.get_to_buf(key, &mut conn.parser.wbuf) {
                conn.parser.wbuf.extend_from_slice(b"$-1\r\n");
            }
            return;
        } else if cmd.eq_ignore_ascii_case(b"DEL") && raw.len() == 2 {
            let out = &mut conn.parser.wbuf;
            let Some(key) = part_str(out, raw[1]) else {
                return;
            };
            if conn.store.del(key) {
                conn.parser.wbuf.extend_from_slice(b":1\r\n");
            } else {
                conn.parser.wbuf.extend_from_slice(b":0\r\n");
            }
            return;
        }
    } else if cmd_len == 4 {
        if cmd.eq_ignore_ascii_case(b"INCR") && raw.len() == 2 {
            let out = &mut conn.parser.wbuf;
            let Some(key) = part_str(out, raw[1]) else {
                return;
            };
            match conn.store.incr(key) {
                Ok(n) => {
                    if conn.store.has_replication() {
                        conn.store.record_current_value(key);
                    }
                    crate::utils::resp::write_integer(&mut conn.parser.wbuf, n)
                }
                Err(e) => crate::utils::resp::write_err(&mut conn.parser.wbuf, e),
            }
            return;
        } else if cmd.eq_ignore_ascii_case(b"RPOP") && raw.len() == 2 {
            let out = &mut conn.parser.wbuf;
            let Some(key) = part_str(out, raw[1]) else {
                return;
            };
            match conn.store.pop_one_to_buf(key, true, &mut conn.parser.wbuf) {
                Ok(true) => {
                    if conn.store.has_replication() {
                        conn.store.record_current_value(key);
                    }
                }
                Ok(false) => conn.parser.wbuf.extend_from_slice(b"$-1\r\n"),
                Err(_) => crate::utils::resp::write_wrong_type(&mut conn.parser.wbuf),
            }
            return;
        } else if cmd.eq_ignore_ascii_case(b"LPOP") && raw.len() == 2 {
            let out = &mut conn.parser.wbuf;
            let Some(key) = part_str(out, raw[1]) else {
                return;
            };
            match conn.store.pop_one_to_buf(key, false, &mut conn.parser.wbuf) {
                Ok(true) => {
                    if conn.store.has_replication() {
                        conn.store.record_current_value(key);
                    }
                }
                Ok(false) => conn.parser.wbuf.extend_from_slice(b"$-1\r\n"),
                Err(_) => crate::utils::resp::write_wrong_type(&mut conn.parser.wbuf),
            }
            return;
        } else if cmd.eq_ignore_ascii_case(b"SADD") && raw.len() >= 3 {
            let out = &mut conn.parser.wbuf;
            let Some(key) = part_str(out, raw[1]) else {
                return;
            };
            if raw.len() == 3 {
                let Some(member) = part_str(out, raw[2]) else {
                    return;
                };
                match conn.store.sadd(key, &[member]) {
                    Ok(n) => {
                        if conn.store.has_replication() {
                            conn.store.record_current_value(key);
                        }
                        crate::utils::resp::write_integer(&mut conn.parser.wbuf, n as i64)
                    }
                    Err(_) => crate::utils::resp::write_wrong_type(&mut conn.parser.wbuf),
                }
                return;
            }
        }
    } else if cmd_len == 5 && cmd.eq_ignore_ascii_case(b"LPUSH") && raw.len() == 3 {
        let out = &mut conn.parser.wbuf;
        let Some(key) = part_str(out, raw[1]) else {
            return;
        };
        let Some(value) = part_str(out, raw[2]) else {
            return;
        };
        match conn.store.lpush(key, &[value]) {
            Ok(n) => {
                if conn.store.has_replication() {
                    conn.store.record_current_value(key);
                }
                crate::utils::resp::write_integer(&mut conn.parser.wbuf, n as i64)
            }
            Err(_) => crate::utils::resp::write_wrong_type(&mut conn.parser.wbuf),
        }
        return;
    }

    const STACK_CAP: usize = 32;
    if raw.len() <= STACK_CAP {
        let mut arr = [""; STACK_CAP];
        for (i, &part) in raw.iter().enumerate() {
            let Some(s) = part_str(&mut conn.parser.wbuf, part) else {
                return;
            };
            arr[i] = s;
        }
        dispatch(conn, &arr[..raw.len()]);
    } else {
        let mut parts: Vec<&str> = Vec::with_capacity(raw.len());
        for &part in raw.iter() {
            let Some(s) = part_str(&mut conn.parser.wbuf, part) else {
                return;
            };
            parts.push(s);
        }
        dispatch(conn, &parts);
    }
}

/// Route one command against this connection's cached topology snapshot.
#[inline(always)]
fn route_cached(
    conn: &Conn<'_>,
    cmd: &[u8],
    args: &[&[u8]],
    asking: bool,
) -> crate::cluster::RouteDecision<'static> {
    let (topology, routing) = conn.cached_topology();
    crate::cluster::route_command_with_snapshot(
        &conn.store.cluster,
        conn.store.cluster_state_ref(),
        topology,
        routing,
        cmd,
        args,
        asking,
    )
}

/// Write a non-Local routing decision into the output buffer.
#[cold]
#[inline(never)]
fn write_route_decision(out: &mut Vec<u8>, decision: crate::cluster::RouteDecision<'_>) {
    match decision {
        crate::cluster::RouteDecision::Moved { slot, address } => {
            out.extend_from_slice(b"-MOVED ");
            crate::utils::resp::write_usize(out, slot.value() as usize);
            out.push(b' ');
            out.extend_from_slice(address.as_bytes());
            out.extend_from_slice(b"\r\n");
        }
        crate::cluster::RouteDecision::MovedOwned { slot, address } => {
            out.extend_from_slice(b"-MOVED ");
            crate::utils::resp::write_usize(out, slot.value() as usize);
            out.push(b' ');
            out.extend_from_slice(address.as_bytes());
            out.extend_from_slice(b"\r\n");
        }
        crate::cluster::RouteDecision::Ask { slot, address } => {
            out.extend_from_slice(b"-ASK ");
            crate::utils::resp::write_usize(out, slot.value() as usize);
            out.push(b' ');
            out.extend_from_slice(address.as_bytes());
            out.extend_from_slice(b"\r\n");
        }
        crate::cluster::RouteDecision::CrossSlot => {
            out.extend_from_slice(b"-CROSSSLOT Keys in request don't hash to the same slot\r\n");
        }
        crate::cluster::RouteDecision::Unassigned(_) => {
            out.extend_from_slice(b"-CLUSTERDOWN Hash slot not served\r\n");
        }
        crate::cluster::RouteDecision::Local => {}
    }
}

#[cold]
#[inline(never)]
fn handle_unauth(conn: &mut Conn<'_>, raw: &[(*const u8, usize)], cmd: &[u8], cmd_len: usize) {
    if cmd_len == 4 && cmd.eq_ignore_ascii_case(b"AUTH") {
        if raw.len() >= 2 {
            let Some(pass) = part_str(&mut conn.parser.wbuf, raw[1]) else {
                return;
            };
            if let Some(expected) = conn.auth_required {
                if constant_time_eq(pass.as_bytes(), expected.as_bytes()) {
                    conn.authenticated = true;
                    conn.parser.wbuf.extend_from_slice(b"+OK\r\n");
                } else {
                    conn.parser
                        .wbuf
                        .extend_from_slice(b"-WRONGPASS invalid username-password pair\r\n");
                }
            } else {
                conn.authenticated = true;
                conn.parser.wbuf.extend_from_slice(b"+OK\r\n");
            }
        } else {
            crate::utils::resp::write_wrong_args(&mut conn.parser.wbuf, "auth");
        }
    } else if cmd_len == 4 && cmd.eq_ignore_ascii_case(b"PING") {
        conn.parser.wbuf.extend_from_slice(b"+PONG\r\n");
    } else if cmd_len == 4 && cmd.eq_ignore_ascii_case(b"QUIT") {
        conn.parser.wbuf.extend_from_slice(b"+OK\r\n");
    } else {
        conn.parser
            .wbuf
            .extend_from_slice(b"-NOAUTH Authentication required.\r\n");
    }
}

#[inline(never)]
fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff = 0u8;
    for (x, y) in a.iter().zip(b.iter()) {
        diff |= x ^ y;
    }
    diff == 0
}
