//! Parked connections.
//!
//! A command that cannot finish inline parks its connection instead of
//! stalling the worker: either it runs as a job on the vector pool (heavy
//! FT.SEARCH, index builds) and its reply comes back through the worker's
//! completion queue, or it is a blocking command (BLPOP, BLMOVE,
//! BZPOPMIN, …) waiting for a key to be written or its timeout to fire.
//!
//! While parked the connection keeps reading (so a closed socket is still
//! noticed) but parses nothing: pipelined commands behind the parked one
//! wait in the read buffer, preserving reply order. Unparking resumes the
//! parse loop from where it stopped.

use super::*;
use crate::pubsub::Completion;
use crate::storage::blocking::Waiter;
use crate::utils::resp;
use std::cell::RefCell;
use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

static NEXT_UID: AtomicU64 = AtomicU64::new(1);

/// Process-unique connection id: tokens are recycled, uids never are, so
/// a late completion can't reach the wrong client.
pub(crate) fn next_uid() -> u64 {
    NEXT_UID.fetch_add(1, Ordering::Relaxed)
}

pub enum Parked {
    /// Waiting for an off-worker job's reply.
    Job,
    /// Blocking command waiting on keys.
    Blocking(Box<Blocked>),
}

pub struct Blocked {
    pub op: Box<dyn BlockingOp>,
    pub keys: Vec<String>,
    pub deadline: Option<Instant>,
}

pub enum Attempt {
    /// Reply (or error) written.
    Served,
    /// Nothing available yet.
    Wait,
}

/// A blocking command, re-run on every signal until it serves or times out.
pub trait BlockingOp: Send {
    fn attempt(&mut self, store: &Arc<Store>, out: &mut Vec<u8>) -> Attempt;
    /// Keys whose writes can satisfy this command.
    fn keys(&self) -> Vec<String>;
    fn timeout_reply(&self, out: &mut Vec<u8>);
}

thread_local! {
    /// Per-worker deadlines of blocked connections: (deadline, token, uid).
    static TIMERS: RefCell<BinaryHeap<Reverse<(Instant, usize, u64)>>> =
        const { RefCell::new(BinaryHeap::new()) };
}

/// Earliest pending deadline on this worker.
pub fn next_deadline() -> Option<Instant> {
    TIMERS.with(|t| t.borrow().peek().map(|Reverse((d, ..))| *d))
}

/// Pop every deadline at or before `now` as (token, uid).
pub fn take_expired(now: Instant, out: &mut Vec<(usize, u64)>) {
    TIMERS.with(|t| {
        let mut t = t.borrow_mut();
        while let Some(Reverse((d, token, uid))) = t.peek().copied() {
            if d > now {
                break;
            }
            t.pop();
            out.push((token, uid));
        }
    });
}

/// Sends an error reply if a job is dropped without completing (panic
/// inside the job), so its connection can never stay parked forever.
struct ReplyGuard {
    notifier: Arc<WorkerNotifier>,
    token: usize,
    uid: u64,
    done: bool,
}

impl ReplyGuard {
    fn send(mut self, bytes: Vec<u8>) {
        self.done = true;
        self.notifier.complete(Completion::Reply { token: self.token, uid: self.uid, bytes });
    }
}

impl Drop for ReplyGuard {
    fn drop(&mut self) {
        if !self.done {
            self.notifier.complete(Completion::Reply {
                token: self.token,
                uid: self.uid,
                bytes: b"-ERR internal error while executing command\r\n".to_vec(),
            });
        }
    }
}

impl<'a> Conn<'a> {
    #[inline]
    pub fn is_parked(&self) -> bool {
        self.park.is_some()
    }

    /// Run `job` on the vector pool and park until its reply arrives. The
    /// job writes RESP into the buffer it is given.
    pub(crate) fn submit(&mut self, job: impl FnOnce(&mut Vec<u8>) + Send + 'static) {
        self.park = Some(Parked::Job);
        let guard = ReplyGuard {
            notifier: Arc::clone(self.notifier),
            token: self.token,
            uid: self.uid,
            done: false,
        };
        fyro_vector::pool::global().spawn(move || {
            let mut out = Vec::with_capacity(256);
            job(&mut out);
            guard.send(out);
        });
    }

    /// Try a blocking command now; park the connection if it must wait.
    pub(crate) fn start_blocking(&mut self, mut op: Box<dyn BlockingOp>, timeout: Option<Duration>) {
        let store = self.store;
        if let Attempt::Served = op.attempt(store, &mut self.parser.wbuf) {
            return;
        }
        let keys = op.keys();
        let waiter = Waiter { wake: Arc::clone(self.notifier) as _, token: self.token, uid: self.uid };
        store.blocking.register(&keys, &waiter);
        // Register-then-retry: a write that landed between the first
        // attempt and the registration would otherwise be missed.
        if let Attempt::Served = op.attempt(store, &mut self.parser.wbuf) {
            store.blocking.unregister(&keys, self.uid);
            return;
        }
        let deadline = timeout.map(|t| Instant::now() + t);
        if let Some(d) = deadline {
            TIMERS.with(|t| t.borrow_mut().push(Reverse((d, self.token, self.uid))));
        }
        self.park = Some(Parked::Blocking(Box::new(Blocked { op, keys, deadline })));
    }

    /// Apply a completion delivered to this worker. Returns false when the
    /// connection should close.
    pub fn on_completion(&mut self, c: Completion) -> bool {
        match c {
            Completion::Reply { uid, bytes, .. } => {
                if uid != self.uid || !matches!(self.park, Some(Parked::Job)) {
                    return true;
                }
                self.parser.wbuf.extend_from_slice(&bytes);
                self.park = None;
                self.process_buffered()
            }
            Completion::Retry { uid, .. } => {
                if uid != self.uid {
                    return true;
                }
                let store = self.store;
                let served = match &mut self.park {
                    Some(Parked::Blocking(b)) => {
                        matches!(b.op.attempt(store, &mut self.parser.wbuf), Attempt::Served)
                    }
                    _ => return true,
                };
                if !served {
                    return true;
                }
                self.unblock();
                self.process_buffered()
            }
        }
    }

    /// A deadline fired for this worker. Returns false when the connection
    /// should close.
    pub fn on_timeout(&mut self, uid: u64, now: Instant) -> bool {
        if uid != self.uid {
            return true;
        }
        let expired = matches!(
            &self.park,
            Some(Parked::Blocking(b)) if b.deadline.is_some_and(|d| d <= now)
        );
        if !expired {
            return true;
        }
        if let Some(Parked::Blocking(b)) = &self.park {
            b.op.timeout_reply(&mut self.parser.wbuf);
        }
        self.unblock();
        self.process_buffered()
    }

    /// Leave the blocking state and drop this connection's key waiters.
    pub(super) fn unblock(&mut self) {
        if let Some(Parked::Blocking(b)) = self.park.take() {
            self.store.blocking.unregister(&b.keys, self.uid);
        }
    }
}

// ── Blocking commands ──────────────────────────────────────────────────────

/// Parse a Redis blocking timeout (seconds, may be fractional; 0 = forever).
pub(crate) fn parse_timeout(s: &str) -> Result<Option<Duration>, &'static str> {
    let secs: f64 = s.parse().map_err(|_| "ERR timeout is not a float or out of range")?;
    if !secs.is_finite() {
        return Err("ERR timeout is not a float or out of range");
    }
    if secs < 0.0 {
        return Err("ERR timeout is negative");
    }
    Ok((secs > 0.0).then(|| Duration::from_secs_f64(secs)))
}

struct Pop {
    keys: Vec<String>,
    right: bool,
    scratch: String,
}

impl BlockingOp for Pop {
    fn attempt(&mut self, store: &Arc<Store>, out: &mut Vec<u8>) -> Attempt {
        for k in &self.keys {
            match store.pop_one_to_scratch(k, self.right, &mut self.scratch) {
                Ok(true) => {
                    if store.has_replication() {
                        store.record_current_value(k);
                    }
                    resp::write_array_header(out, 2);
                    resp::write_bulk(out, k);
                    resp::write_bulk(out, &self.scratch);
                    return Attempt::Served;
                }
                Ok(false) => {}
                Err(_) => {
                    resp::write_wrong_type(out);
                    return Attempt::Served;
                }
            }
        }
        Attempt::Wait
    }
    fn keys(&self) -> Vec<String> {
        self.keys.clone()
    }
    fn timeout_reply(&self, out: &mut Vec<u8>) {
        out.extend_from_slice(b"*-1\r\n");
    }
}

struct Move {
    src: String,
    dst: String,
    left_src: bool,
    left_dst: bool,
}

impl BlockingOp for Move {
    fn attempt(&mut self, store: &Arc<Store>, out: &mut Vec<u8>) -> Attempt {
        match store.lmove(&self.src, &self.dst, self.left_src, self.left_dst) {
            Ok(Some(v)) => {
                if store.has_replication() {
                    store.record_current_value(&self.src);
                    store.record_current_value(&self.dst);
                }
                resp::write_bulk(out, &v);
                Attempt::Served
            }
            Ok(None) => Attempt::Wait,
            Err(e) => {
                resp::write_store_err(out, e);
                Attempt::Served
            }
        }
    }
    fn keys(&self) -> Vec<String> {
        vec![self.src.clone()]
    }
    fn timeout_reply(&self, out: &mut Vec<u8>) {
        out.extend_from_slice(b"$-1\r\n");
    }
}

struct ZPop {
    keys: Vec<String>,
    max: bool,
}

impl BlockingOp for ZPop {
    fn attempt(&mut self, store: &Arc<Store>, out: &mut Vec<u8>) -> Attempt {
        for k in &self.keys {
            let popped = if self.max { store.zpopmax(k, 1) } else { store.zpopmin(k, 1) };
            match popped {
                Ok(items) if !items.is_empty() => {
                    if store.has_replication() {
                        store.record_current_value(k);
                    }
                    resp::write_array_header(out, 3);
                    resp::write_bulk(out, k);
                    resp::write_bulk(out, &items[0].0);
                    resp::write_bulk(out, &crate::utils::util::format_float(items[0].1));
                    return Attempt::Served;
                }
                Ok(_) => {}
                Err(_) => {
                    resp::write_wrong_type(out);
                    return Attempt::Served;
                }
            }
        }
        Attempt::Wait
    }
    fn keys(&self) -> Vec<String> {
        self.keys.clone()
    }
    fn timeout_reply(&self, out: &mut Vec<u8>) {
        out.extend_from_slice(b"*-1\r\n");
    }
}

struct XRead {
    req: crate::commands::stream::XReadReq,
}

impl BlockingOp for XRead {
    fn attempt(&mut self, store: &Arc<Store>, out: &mut Vec<u8>) -> Attempt {
        if !self.req.serve(store, out, false) {
            return Attempt::Wait;
        }
        if self.req.group.is_some() && store.has_replication() {
            for k in &self.req.keys {
                store.record_current_value(k);
            }
        }
        Attempt::Served
    }
    fn keys(&self) -> Vec<String> {
        self.req.keys.clone()
    }
    fn timeout_reply(&self, out: &mut Vec<u8>) {
        out.extend_from_slice(b"*-1\r\n");
    }
}

pub(super) fn is_blocking_stream_read(cmd: &[u8], raw: &[(*const u8, usize)]) -> bool {
    if !(cmd.eq_ignore_ascii_case(b"XREAD") || cmd.eq_ignore_ascii_case(b"XREADGROUP")) {
        return false;
    }
    for &p in &raw[1..] {
        let a = unsafe { part_bytes(p) };
        if a.eq_ignore_ascii_case(b"STREAMS") {
            return false;
        }
        if a.eq_ignore_ascii_case(b"BLOCK") {
            return true;
        }
    }
    false
}

pub(super) fn dispatch_stream_read(conn: &mut Conn<'_>, raw: &[(*const u8, usize)]) {
    let mut args: Vec<&str> = Vec::with_capacity(raw.len());
    for &p in raw {
        match std::str::from_utf8(unsafe { part_bytes(p) }) {
            Ok(s) => args.push(s),
            Err(_) => return resp::write_err(&mut conn.parser.wbuf, "invalid UTF-8 in request"),
        }
    }
    let grouped = args[0].eq_ignore_ascii_case("XREADGROUP");
    let mut req = match crate::commands::stream::parse_xread(&args, grouped) {
        Ok(r) => r,
        Err(e) => return resp::write_store_err(&mut conn.parser.wbuf, &e),
    };
    req.resolve_dollar(conn.store);
    let timeout = req.block.filter(|ms| *ms > 0).map(Duration::from_millis);
    let history = grouped && req.ids.iter().any(|id| id != ">");
    if history {
        req.serve(conn.store, &mut conn.parser.wbuf, true);
        if conn.store.has_replication() {
            for k in &req.keys {
                conn.store.record_current_value(k);
            }
        }
        return;
    }
    conn.start_blocking(Box::new(XRead { req }), timeout);
}

/// Commands this module blocks for; everything else dispatches normally.
pub(super) fn is_blocking_cmd(cmd: &[u8]) -> bool {
    matches!(cmd.len(), 5 | 6 | 8 | 10)
        && (cmd.eq_ignore_ascii_case(b"BLPOP")
            || cmd.eq_ignore_ascii_case(b"BRPOP")
            || cmd.eq_ignore_ascii_case(b"BLMOVE")
            || cmd.eq_ignore_ascii_case(b"BZPOPMIN")
            || cmd.eq_ignore_ascii_case(b"BZPOPMAX")
            || cmd.eq_ignore_ascii_case(b"BRPOPLPUSH"))
}

/// Parse and start one blocking list/zset command from raw parts.
pub(super) fn dispatch_blocking(conn: &mut Conn<'_>, raw: &[(*const u8, usize)]) {
    let mut args: Vec<String> = Vec::with_capacity(raw.len());
    for &p in raw {
        match std::str::from_utf8(unsafe { part_bytes(p) }) {
            Ok(s) => args.push(s.to_string()),
            Err(_) => return resp::write_err(&mut conn.parser.wbuf, "invalid UTF-8 in request"),
        }
    }
    let name = args[0].to_ascii_uppercase();
    let out = &mut conn.parser.wbuf;
    let wrong = |out: &mut Vec<u8>| resp::write_wrong_args(out, &name.to_ascii_lowercase());
    let (op, timeout): (Box<dyn BlockingOp>, &str) = match name.as_str() {
        "BLPOP" | "BRPOP" | "BZPOPMIN" | "BZPOPMAX" => {
            if args.len() < 3 {
                return wrong(out);
            }
            let keys = args[1..args.len() - 1].to_vec();
            let op: Box<dyn BlockingOp> = match name.as_str() {
                "BLPOP" => Box::new(Pop { keys, right: false, scratch: String::new() }),
                "BRPOP" => Box::new(Pop { keys, right: true, scratch: String::new() }),
                "BZPOPMIN" => Box::new(ZPop { keys, max: false }),
                _ => Box::new(ZPop { keys, max: true }),
            };
            (op, args.last().unwrap())
        }
        "BLMOVE" => {
            if args.len() != 6 {
                return wrong(out);
            }
            let side = |s: &str| match s.to_ascii_uppercase().as_str() {
                "LEFT" => Some(true),
                "RIGHT" => Some(false),
                _ => None,
            };
            let (Some(left_src), Some(left_dst)) = (side(&args[3]), side(&args[4])) else {
                return resp::write_store_err(out, "ERR syntax error");
            };
            let op = Box::new(Move { src: args[1].clone(), dst: args[2].clone(), left_src, left_dst });
            (op, &args[5])
        }
        "BRPOPLPUSH" => {
            if args.len() != 4 {
                return wrong(out);
            }
            let op = Box::new(Move { src: args[1].clone(), dst: args[2].clone(), left_src: false, left_dst: true });
            (op, &args[3])
        }
        _ => return wrong(out),
    };
    match parse_timeout(timeout) {
        Ok(t) => conn.start_blocking(op, t),
        Err(e) => resp::write_store_err(out, e),
    }
}

#[cfg(test)]
mod tests {
    use super::parse_timeout;
    use std::time::Duration;

    #[test]
    fn timeouts_follow_redis_rules() {
        assert_eq!(parse_timeout("0"), Ok(None));
        assert_eq!(parse_timeout("0.25"), Ok(Some(Duration::from_millis(250))));
        assert_eq!(parse_timeout("2"), Ok(Some(Duration::from_secs(2))));
        assert_eq!(parse_timeout("-1"), Err("ERR timeout is negative"));
        assert!(parse_timeout("abc").is_err());
        assert!(parse_timeout("inf").is_err());
    }
}
