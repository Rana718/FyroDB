use foldhash::HashSet;
use mio::net::TcpStream;
use std::io::{self, Read, Write};
use std::sync::Arc;

use crate::pubsub::{PubSub, SubSlot, WorkerNotifier};
use crate::storage::store::Store;
use crate::utils::parser::{ParseResult, RespParser};

use super::dispatch::dispatch;
use super::subscription::do_full_unsubscribe;

pub(crate) const SUB_WRITE_BATCH_BYTES: usize = 256 * 1024;
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
                    if self.authenticated
                        && !self.store.cluster.enabled
                        && !self.store.at_key_capacity()
                        && is_coalescible_queue(raw)
                    {
                        match dispatch_queue_run(self, raw) {
                            QueueRunOutcome::Done => continue,
                            QueueRunOutcome::ConnError => return false,
                        }
                    }
                    if self.authenticated
                        && !self.store.cluster.enabled
                        && !self.store.at_key_capacity()
                        && is_set_expire_pair(raw)
                    {
                        match dispatch_set_expire_pair(self, raw) {
                            PairOutcome::Done => continue,
                            PairOutcome::FallThrough => {}
                            PairOutcome::ConnError => return false,
                        }
                    }
                    if self.authenticated && !self.store.cluster.enabled && is_publish_run(raw) {
                        match dispatch_publish_run(self, raw) {
                            QueueRunOutcome::Done => continue,
                            QueueRunOutcome::ConnError => return false,
                        }
                    }
                    if self.authenticated
                        && !self.store.cluster.enabled
                        && !self.store.at_key_capacity()
                        && is_write_run(raw)
                    {
                        match dispatch_write_run(self, raw) {
                            QueueRunOutcome::Done => continue,
                            QueueRunOutcome::ConnError => return false,
                        }
                    }
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

// The argument slice is materialized only for multi-key commands;
// keyless and single-key commands skip it.
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
    } else if cmd_len == 6 && cmd.eq_ignore_ascii_case(b"EXPIRE") && raw.len() == 3 {
        let out = &mut conn.parser.wbuf;
        let Some(key) = part_str(out, raw[1]) else {
            return;
        };
        let Some(secs) = part_str(out, raw[2]) else {
            return;
        };
        // Bad input and out-of-range overflow produce the same error the
        // generic path reports, so the fast arm stays protocol-identical.
        let reply = match secs
            .parse::<u64>()
            .ok()
            .and_then(crate::storage::value::expiry_from_secs)
        {
            Some(exp) => {
                let ok = conn.store.expire_ms(key, exp);
                if conn.store.has_replication() {
                    conn.store.record_current_value(key);
                }
                if ok {
                    crate::utils::resp::ONE
                } else {
                    crate::utils::resp::ZERO
                }
            }
            None => b"-ERR invalid expire time in 'expire' command\r\n" as &[u8],
        };
        conn.parser.wbuf.extend_from_slice(reply);
        return;
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

/// Max same-key queue commands per coalesced run (one lock acquisition).
const QUEUE_RUN_MAX: usize = 512;

enum PairOutcome {
    Done,
    FallThrough,
    ConnError,
}

/// Is `raw` a plain `SET key value` that could open a coalesced
/// `SET k v` + `EXPIRE k t` pair?
#[inline(always)]
fn is_set_expire_pair(raw: &[(*const u8, usize)]) -> bool {
    raw.len() == 3
        && unsafe { part_bytes(raw[0]) }.len() == 3
        && unsafe { part_bytes(raw[0]) }.eq_ignore_ascii_case(b"SET")
}

fn dispatch_set_expire_pair(conn: &mut Conn<'_>, set: &[(*const u8, usize)]) -> PairOutcome {
    let (key, value): (&str, &str);
    {
        let out = &mut conn.parser.wbuf;
        let Some(k) = part_str(out, set[1]) else {
            return PairOutcome::ConnError;
        };
        let Some(v) = part_str(out, set[2]) else {
            return PairOutcome::ConnError;
        };
        key = k;
        value = v;
    }

// Zero-copy peek: the EXPIRE header is fixed-width, so a prefix check
// on the unparsed remainder decides without consuming.
    const EXPIRE_PREFIX: &[u8] = b"*3\r\n$6\r\nEXPIRE\r\n";
    if !conn
        .parser
        .peek_remaining(EXPIRE_PREFIX.len())
        .starts_with(EXPIRE_PREFIX)
    {
        return PairOutcome::FallThrough;
    }

    // Consume and parse the EXPIRE command.
    match conn.parser.parse_one() {
        ParseResult::Complete => {}
        _ => return PairOutcome::FallThrough,
    }
    let raw_ptr = conn.parser.parts_raw.as_ptr();
    let raw_len = conn.parser.parts_raw.len();
    let expire = unsafe { std::slice::from_raw_parts(raw_ptr, raw_len) };
    if expire.len() != 3 || !unsafe { part_bytes(expire[0]) }.eq_ignore_ascii_case(b"EXPIRE") {
        conn.store.set_string(key, value, 0);
        if conn.store.has_replication() {
            conn.store.record_current_value(key);
        }
        conn.parser.wbuf.extend_from_slice(b"+OK\r\n");
        dispatch_raw(conn, expire);
        return PairOutcome::Done;
    }
    if unsafe { part_bytes(expire[1]) } != key.as_bytes() {
        // EXPIRE for a different key: both go through normal dispatch.
        conn.store.set_string(key, value, 0);
        if conn.store.has_replication() {
            conn.store.record_current_value(key);
        }
        conn.parser.wbuf.extend_from_slice(b"+OK\r\n");
        dispatch_raw(conn, expire);
        return PairOutcome::Done;
    }
    let secs_str: &str;
    {
        let out = &mut conn.parser.wbuf;
        let Some(s) = part_str(out, expire[2]) else {
            return PairOutcome::ConnError;
        };
        secs_str = s;
    }
    let Some(exp) = secs_str
        .parse::<u64>()
        .ok()
        .and_then(crate::storage::value::expiry_from_secs)
    else {
        // Invalid TTL: the generic path reports the error identically.
        conn.store.set_string(key, value, 0);
        if conn.store.has_replication() {
            conn.store.record_current_value(key);
        }
        conn.parser.wbuf.extend_from_slice(b"+OK\r\n");
        dispatch_raw(conn, expire);
        return PairOutcome::Done;
    };

    // The pair: one lock, atomic value+TTL.
    conn.store.set_string(key, value, exp);
    if conn.store.has_replication() {
        conn.store.record_current_value(key);
    }
    conn.parser.wbuf.extend_from_slice(b"+OK\r\n:1\r\n");
    PairOutcome::Done
}

/// Is `raw` a `PUBLISH channel message` that could open a batched run?
#[inline(always)]
fn is_publish_run(raw: &[(*const u8, usize)]) -> bool {
    raw.len() == 3
        && unsafe { part_bytes(raw[0]) }.len() == 7
        && unsafe { part_bytes(raw[0]) }.eq_ignore_ascii_case(b"PUBLISH")
}

fn dispatch_publish_run(conn: &mut Conn<'_>, first: &[(*const u8, usize)]) -> QueueRunOutcome {
    // Validate the channel the same way the single-command path would.
    let out = &mut conn.parser.wbuf;
    let Some(channel) = part_str(out, first[1]) else {
        return QueueRunOutcome::ConnError;
    };
    let channel_bytes = channel.as_bytes();

// first aliases parts_raw (overwritten by each parse_one): copy the
// message now; allocate only once a real run forms.
    let first_msg = first[2];
    let mut parts: Vec<(*const u8, usize)> = Vec::new();
    let mut collected_first = false;
    let mut cmds = 1usize;

    while cmds < QUEUE_RUN_MAX {
        match conn.parser.parse_one() {
            ParseResult::Complete => {
                let raw_ptr = conn.parser.parts_raw.as_ptr();
                let raw_len = conn.parser.parts_raw.len();
                let raw = unsafe { std::slice::from_raw_parts(raw_ptr, raw_len) };
                let cmd = unsafe { part_bytes(raw[0]) };
                let same = raw.len() == 3
                    && cmd.len() == 7
                    && cmd.eq_ignore_ascii_case(b"PUBLISH")
                    && unsafe { part_bytes(raw[1]) } == channel_bytes;
                if !same {
                    let tail: &[(*const u8, usize)] = if collected_first {
                        &parts
                    } else {
                        std::slice::from_ref(&first_msg)
                    };
                    let outcome = match execute_publish_run(conn, channel, cmds, tail) {
                        Ok(()) => QueueRunOutcome::Done,
                        Err(()) => QueueRunOutcome::ConnError,
                    };
                    if matches!(outcome, QueueRunOutcome::Done) {
                        dispatch_raw(conn, raw);
                    }
                    return outcome;
                }
                if !collected_first {
                    parts.push(first_msg);
                    collected_first = true;
                }
                parts.push(raw[2]);
                cmds += 1;
            }
            ParseResult::Incomplete => break,
            ParseResult::Error => {
                let tail: &[(*const u8, usize)] = if collected_first {
                    &parts
                } else {
                    std::slice::from_ref(&first_msg)
                };
                return match execute_publish_run(conn, channel, cmds, tail) {
                    Ok(()) => QueueRunOutcome::Done,
                    Err(()) => QueueRunOutcome::ConnError,
                };
            }
        }
    }

    let tail: &[(*const u8, usize)] = if collected_first {
        &parts
    } else {
        std::slice::from_ref(&first_msg)
    };
    match execute_publish_run(conn, channel, cmds, tail) {
        Ok(()) => QueueRunOutcome::Done,
        Err(()) => QueueRunOutcome::ConnError,
    }
}

fn execute_publish_run(
    conn: &mut Conn<'_>,
    channel: &str,
    cmds: usize,
    parts: &[(*const u8, usize)],
) -> Result<(), ()> {
    let out = &mut conn.parser.wbuf;
    let mut messages: Vec<&str> = Vec::with_capacity(parts.len());
    for &part in parts {
        let Some(m) = part_str(out, part) else {
            return Err(());
        };
        messages.push(m);
    }
    let n = conn.pubsub.publish_batch(channel, &messages);
    for _ in 0..cmds {
        crate::utils::resp::write_integer(out, n as i64);
    }
    Ok(())
}

/// Same-key write whose pipelined runs execute under one lock, with
/// per-command replies identical to sequential execution.
#[derive(Clone, Copy, PartialEq)]
enum WriteOp {
    /// `SET k v`: only the last value is observable; replies are all `+OK`.
    Set,
    /// `INCR k`: replies are the consecutive counter values.
    Incr,
    /// `HSET k f v`: reply per command is the "field added" flag.
    Hset,
    /// `SADD k m`: reply per command is the "member added" flag.
    Sadd,
    /// `ZADD k score m` (plain, no modifiers): reply is "member added".
    Zadd,
}

impl WriteOp {
    #[inline(always)]
    fn parse(cmd: &[u8]) -> Option<WriteOp> {
        match cmd.len() {
            3 if cmd.eq_ignore_ascii_case(b"SET") => Some(WriteOp::Set),
            4 if cmd.eq_ignore_ascii_case(b"INCR") => Some(WriteOp::Incr),
            4 if cmd.eq_ignore_ascii_case(b"HSET") => Some(WriteOp::Hset),
            4 if cmd.eq_ignore_ascii_case(b"SADD") => Some(WriteOp::Sadd),
            4 if cmd.eq_ignore_ascii_case(b"ZADD") => Some(WriteOp::Zadd),
            _ => None,
        }
    }

    /// Total RESP parts of one command, key included.
    #[inline(always)]
    fn arity(self) -> usize {
        match self {
            WriteOp::Set | WriteOp::Sadd => 3,
            WriteOp::Incr => 2,
            WriteOp::Hset | WriteOp::Zadd => 4,
        }
    }
}

#[inline(always)]
fn is_write_run(raw: &[(*const u8, usize)]) -> bool {
    if raw.is_empty() {
        return false;
    }
    let cmd = unsafe { part_bytes(raw[0]) };
    WriteOp::parse(cmd).is_some_and(|op| raw.len() == op.arity())
}

/// Execute a same-key write run with one lock acquisition; any
/// non-matching command ends the run and dispatches normally.
fn dispatch_write_run(conn: &mut Conn<'_>, first: &[(*const u8, usize)]) -> QueueRunOutcome {
    let first_cmd = unsafe { part_bytes(first[0]) };
    let op = WriteOp::parse(first_cmd).expect("is_write_run checked the first command");

    let out = &mut conn.parser.wbuf;
    let Some(key) = part_str(out, first[1]) else {
        return QueueRunOutcome::ConnError;
    };
    let key_bytes = key.as_bytes();

// first aliases parts_raw (overwritten per parse_one): copy tail values
// to the stack now; allocate only once a real run forms.
    let tail_len = op.arity() - 2;
    let mut first_tail: [(*const u8, usize); 2] = [(std::ptr::null(), 0); 2];
    for (i, &p) in first[2..].iter().enumerate() {
        first_tail[i] = p;
    }
    let mut parts: Vec<(*const u8, usize)> = Vec::new();
    let mut collected_first = false;
    let mut cmds = 1usize;

    while cmds < QUEUE_RUN_MAX {
        match conn.parser.parse_one() {
            ParseResult::Complete => {
                let raw_ptr = conn.parser.parts_raw.as_ptr();
                let raw_len = conn.parser.parts_raw.len();
                let raw = unsafe { std::slice::from_raw_parts(raw_ptr, raw_len) };
                let cmd = unsafe { part_bytes(raw[0]) };
                let same = WriteOp::parse(cmd) == Some(op)
                    && raw.len() == op.arity()
                    && unsafe { part_bytes(raw[1]) } == key_bytes;
                if !same {
// Dispatch the already-consumed trailing command inline, or it drops.
                    let tail: &[(*const u8, usize)] = if collected_first {
                        &parts
                    } else {
                        &first_tail[..tail_len]
                    };
                    let outcome = match execute_write_run(conn, key, op, cmds, tail) {
                        Ok(()) => QueueRunOutcome::Done,
                        Err(()) => QueueRunOutcome::ConnError,
                    };
                    if matches!(outcome, QueueRunOutcome::Done) {
                        dispatch_raw(conn, raw);
                    }
                    return outcome;
                }
                if !collected_first {
                    parts.extend_from_slice(&first_tail[..tail_len]);
                    collected_first = true;
                }
                parts.extend_from_slice(&raw[2..]);
                cmds += 1;
            }
            ParseResult::Incomplete => break,
            ParseResult::Error => {
                let tail: &[(*const u8, usize)] = if collected_first {
                    &parts
                } else {
                    &first_tail[..tail_len]
                };
                return match execute_write_run(conn, key, op, cmds, tail) {
                    Ok(()) => QueueRunOutcome::Done,
                    Err(()) => QueueRunOutcome::ConnError,
                };
            }
        }
    }

    let tail: &[(*const u8, usize)] = if collected_first {
        &parts
    } else {
        &first_tail[..tail_len]
    };
    match execute_write_run(conn, key, op, cmds, tail) {
        Ok(()) => QueueRunOutcome::Done,
        Err(()) => QueueRunOutcome::ConnError,
    }
}

/// Run one coalesced write batch; Err(()) closes the connection,
/// matching the single-command paths on invalid UTF-8.
fn execute_write_run(
    conn: &mut Conn<'_>,
    key: &str,
    op: WriteOp,
    cmds: usize,
    parts: &[(*const u8, usize)],
) -> Result<(), ()> {
    let replication = conn.store.has_replication();

    match op {
        WriteOp::Set => {
// Sequential SETs fully overwrite: one call reproduces the final
// state. Plain SET clears TTL (set_string uses zero TTL).
            let last = parts[parts.len() - 1];
            let out = &mut conn.parser.wbuf;
            let Some(value) = part_str(out, last) else {
                return Err(());
            };
            conn.store.set_string(key, value, 0);
            for _ in 0..cmds {
                conn.parser.wbuf.extend_from_slice(b"+OK\r\n");
            }
            if replication {
                for _ in 0..cmds {
                    conn.store.record_current_value(key);
                }
            }
        }
        WriteOp::Incr => {
// One incrby: replies are cur+1..=cur+K. Overflow needs the sequential
// fallback (partial application differs from one checked_add).
            match conn.store.incrby(key, cmds as i64) {
                Ok(final_value) => {
                    let out = &mut conn.parser.wbuf;
                    for i in 1..=cmds as i64 {
                        crate::utils::resp::write_integer(out, final_value - (cmds as i64 - i));
                    }
                }
                Err("increment or decrement would overflow") => {
                    for _ in 0..cmds {
                        match conn.store.incr(key) {
                            Ok(n) => {
                                crate::utils::resp::write_integer(&mut conn.parser.wbuf, n)
                            }
                            Err(e) => {
                                crate::utils::resp::write_err(&mut conn.parser.wbuf, e)
                            }
                        }
                    }
                }
                Err(e) => {
                    // Wrong-type / non-integer value: every sequential
                    // command fails identically without touching state.
                    let out = &mut conn.parser.wbuf;
                    for _ in 0..cmds {
                        crate::utils::resp::write_err(out, e);
                    }
                }
            }
            if replication {
                for _ in 0..cmds {
                    conn.store.record_current_value(key);
                }
            }
        }
        WriteOp::Hset => {
            let out = &mut conn.parser.wbuf;
            let mut fields: Vec<(&str, &str)> = Vec::with_capacity(parts.len() / 2);
            for pair in parts.chunks(2) {
                let Some(f) = part_str(out, pair[0]) else {
                    return Err(());
                };
                let Some(v) = part_str(out, pair[1]) else {
                    return Err(());
                };
                fields.push((f, v));
            }
            match conn.store.hset_added_flags(key, &fields) {
                Ok(flags) => {
                    let out = &mut conn.parser.wbuf;
                    for added in flags {
                        crate::utils::resp::write_integer(out, added as i64);
                    }
                }
                Err(e) => {
                    let out = &mut conn.parser.wbuf;
                    for _ in 0..cmds {
                        crate::utils::resp::write_err(out, e);
                    }
                }
            }
            if replication {
                for _ in 0..cmds {
                    conn.store.record_current_value(key);
                }
            }
        }
        WriteOp::Sadd => {
            let out = &mut conn.parser.wbuf;
            let mut members: Vec<&str> = Vec::with_capacity(parts.len());
            for &part in parts {
                let Some(m) = part_str(out, part) else {
                    return Err(());
                };
                members.push(m);
            }
            match conn.store.sadd_added_flags(key, &members) {
                Ok(flags) => {
                    let out = &mut conn.parser.wbuf;
                    for added in flags {
                        crate::utils::resp::write_integer(out, added as i64);
                    }
                }
                Err(e) => {
                    let out = &mut conn.parser.wbuf;
                    for _ in 0..cmds {
                        crate::utils::resp::write_err(out, e);
                    }
                }
            }
            if replication {
                for _ in 0..cmds {
                    conn.store.record_current_value(key);
                }
            }
        }
        WriteOp::Zadd => {
// A bad score fails only its command: sequential fallback keeps the
// reply stream identical.
            let out = &mut conn.parser.wbuf;
            let mut members: Vec<(f64, &str)> = Vec::with_capacity(parts.len() / 2);
            let mut all_valid = true;
            for pair in parts.chunks(2) {
                let Some(s) = part_str(out, pair[0]) else {
                    return Err(());
                };
                let Some(m) = part_str(out, pair[1]) else {
                    return Err(());
                };
                match s.parse::<f64>() {
                    Ok(score) => members.push((score, m)),
                    Err(_) => {
                        all_valid = false;
                        break;
                    }
                }
            }
            if !all_valid {
                for pair in parts.chunks(2) {
                    let score = std::str::from_utf8(unsafe { part_bytes(pair[0]) })
                        .ok()
                        .and_then(|s| s.parse::<f64>().ok());
                    match score {
                        Some(score) => {
                            let member = std::str::from_utf8(unsafe { part_bytes(pair[1]) })
                                .map_err(|_| ())?;
                            match conn.store.zadd(
                                key,
                                &[(score, member)],
                                crate::storage::zset::ZAddOptions::default(),
                            ) {
                                Ok(n) => crate::utils::resp::write_integer(
                                    &mut conn.parser.wbuf,
                                    n as i64,
                                ),
                                Err(e) => {
                                    crate::utils::resp::write_err(&mut conn.parser.wbuf, e)
                                }
                            }
                        }
                        None => {
                            conn.parser
                                .wbuf
                                .extend_from_slice(b"-ERR value is not a float\r\n");
                        }
                    }
                    if replication {
                        conn.store.record_current_value(key);
                    }
                }
                return Ok(());
            }
            match conn.store.zadd_added_flags(key, &members) {
                Ok(flags) => {
                    let out = &mut conn.parser.wbuf;
                    for added in flags {
                        crate::utils::resp::write_integer(out, added as i64);
                    }
                }
                Err(e) => {
                    let out = &mut conn.parser.wbuf;
                    for _ in 0..cmds {
                        crate::utils::resp::write_err(out, e);
                    }
                }
            }
            if replication {
                for _ in 0..cmds {
                    conn.store.record_current_value(key);
                }
            }
        }
    }
    Ok(())
}

/// A queue command eligible for run coalescing.
#[derive(Clone, Copy, PartialEq)]
enum QueueOp {
    PushFront,
    PushBack,
    PopFront,
    PopBack,
}

impl QueueOp {
    #[inline(always)]
    fn parse(cmd: &[u8]) -> Option<QueueOp> {
        if cmd.len() != 5 {
            return None;
        }
        match cmd {
            b"LPUSH" => Some(QueueOp::PushFront),
            b"RPUSH" => Some(QueueOp::PushBack),
            _ => None,
        }
    }

    #[inline(always)]
    fn parse_pop(cmd: &[u8]) -> Option<QueueOp> {
        if cmd.len() != 4 {
            return None;
        }
        match cmd {
            b"LPOP" => Some(QueueOp::PopFront),
            b"RPOP" => Some(QueueOp::PopBack),
            _ => None,
        }
    }

    #[inline(always)]
    fn is_pop(self) -> bool {
        matches!(self, QueueOp::PopFront | QueueOp::PopBack)
    }
}

/// Does the first parsed command open a coalescible run? (Fast pre-filter:
/// exact uppercase 3-arg LPUSH/RPUSH or 2-arg LPOP/RPOP.)
#[inline(always)]
fn is_coalescible_queue(raw: &[(*const u8, usize)]) -> bool {
    if raw.len() != 3 && raw.len() != 2 {
        return false;
    }
    let cmd = unsafe { part_bytes(raw[0]) };
    if let Some(op) = QueueOp::parse(cmd) {
        return !op.is_pop() && raw.len() == 3;
    }
    QueueOp::parse_pop(cmd).is_some() && raw.len() == 2
}

enum QueueRunOutcome {
    Done,
    ConnError,
}

fn dispatch_queue_run(conn: &mut Conn<'_>, first: &[(*const u8, usize)]) -> QueueRunOutcome {
    let first_cmd = unsafe { part_bytes(first[0]) };
    let op = QueueOp::parse(first_cmd)
        .or_else(|| QueueOp::parse_pop(first_cmd))
        .expect("is_coalescible_queue checked the first command");

    let key_bytes: &[u8] = unsafe { part_bytes(first[1]) };
    let Some(key) = std::str::from_utf8(key_bytes).ok() else {
        crate::utils::resp::write_err(&mut conn.parser.wbuf, "invalid UTF-8 in request");
        return QueueRunOutcome::ConnError;
    };

// first aliases parts_raw (overwritten per parse_one): copy the value
// now; allocate only once a real run forms. Pops read no value.
    let first_value = if op.is_pop() {
        (std::ptr::null(), 0)
    } else {
        first[2]
    };
    let mut parts: Vec<(*const u8, usize)> = Vec::new();
    let mut collected_first = false;
    let mut cmds = 1usize;

    while cmds < QUEUE_RUN_MAX {
        match conn.parser.parse_one() {
            ParseResult::Complete => {
                let raw_ptr = conn.parser.parts_raw.as_ptr();
                let raw_len = conn.parser.parts_raw.len();
                let raw = unsafe { std::slice::from_raw_parts(raw_ptr, raw_len) };
                let cmd = unsafe { part_bytes(raw[0]) };
                let same_shape = match op {
                    QueueOp::PushFront | QueueOp::PushBack => {
                        raw.len() == 3 && QueueOp::parse(cmd) == Some(op)
                    }
                    QueueOp::PopFront | QueueOp::PopBack => {
                        raw.len() == 2 && QueueOp::parse_pop(cmd) == Some(op)
                    }
                };
                if !same_shape || unsafe { part_bytes(raw[1]) } != key_bytes {
                    let tail: &[(*const u8, usize)] = if collected_first {
                        &parts
                    } else if op.is_pop() {
                        &[]
                    } else {
                        std::slice::from_ref(&first_value)
                    };
                    let outcome = match execute_queue_run(conn, key, op, cmds, tail) {
                        Ok(()) => QueueRunOutcome::Done,
                        Err(()) => QueueRunOutcome::ConnError,
                    };
                    if matches!(outcome, QueueRunOutcome::Done) {
                        dispatch_raw(conn, raw);
                    }
                    return outcome;
                }
                if !op.is_pop() {
                    if !collected_first {
                        parts.push(first_value);
                        collected_first = true;
                    }
                    parts.push(raw[2]);
                }
                cmds += 1;
            }
            ParseResult::Incomplete => break,
            ParseResult::Error => {
                let tail: &[(*const u8, usize)] = if collected_first {
                    &parts
                } else if op.is_pop() {
                    &[]
                } else {
                    std::slice::from_ref(&first_value)
                };
                return match execute_queue_run(conn, key, op, cmds, tail) {
                    Ok(()) => QueueRunOutcome::Done,
                    Err(()) => QueueRunOutcome::ConnError,
                };
            }
        }
    }

    let tail: &[(*const u8, usize)] = if collected_first {
        &parts
    } else if op.is_pop() {
        &[]
    } else {
        std::slice::from_ref(&first_value)
    };
    match execute_queue_run(conn, key, op, cmds, tail) {
        Ok(()) => QueueRunOutcome::Done,
        Err(()) => QueueRunOutcome::ConnError,
    }
}

/// Run one coalesced batch; Err(()) closes the connection, matching
/// the single-command path on invalid UTF-8.
fn execute_queue_run(
    conn: &mut Conn<'_>,
    key: &str,
    op: QueueOp,
    cmds: usize,
    parts: &[(*const u8, usize)],
) -> Result<(), ()> {
    let out = &mut conn.parser.wbuf;
    let store = conn.store;

    match op {
        QueueOp::PushFront | QueueOp::PushBack => {
            let mut values: Vec<&str> = Vec::with_capacity(parts.len());
            for &part in parts {
                let Some(v) = part_str(out, part) else {
                    return Err(());
                };
                values.push(v);
            }
            let result = if op == QueueOp::PushFront {
                store.lpush(key, &values)
            } else {
                store.rpush(key, &values)
            };
            match result {
                Ok(final_len) => {
                    let start = final_len.saturating_sub(values.len());
                    for i in 0..values.len() {
                        crate::utils::resp::write_integer(out, (start + i + 1) as i64);
                    }
                }
                Err(_) => {
                    for _ in values {
                        crate::utils::resp::write_wrong_type(out);
                    }
                }
            }
        }
        QueueOp::PopFront | QueueOp::PopBack => {
            let popped = if op == QueueOp::PopFront {
                store.lpop(key, cmds)
            } else {
                store.rpop(key, cmds)
            };
            match popped {
                Ok(values) => {
                    for i in 0..cmds {
                        match values.get(i) {
                            Some(v) => crate::utils::resp::write_bulk(out, v),
                            None => out.extend_from_slice(b"$-1\r\n"),
                        }
                    }
                }
                // Single-pop semantics on a wrong-type key: one error per
                // command, not nils.
                Err(_) => {
                    for _ in 0..cmds {
                        crate::utils::resp::write_wrong_type(out);
                    }
                }
            }
        }
    }

    if conn.store.has_replication() {
        for _ in 0..cmds {
            conn.store.record_current_value(key);
        }
    }
    Ok(())
}
