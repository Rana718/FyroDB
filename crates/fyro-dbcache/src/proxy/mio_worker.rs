// Non-blocking mio-based proxy worker.
//
// One ProxyWorker thread runs a single mio Poll, multiplexing many proxy
// connections without blocking a FyroDB worker thread.
//
// Fast path (no upstream, cache hit): fully inline in the mio loop.
// Slow path (startup with upstream): handed to a bounded blocking sub-pool
//   via session::handle_client; completion wakes the Poll via a Waker.

use mio::net::{TcpListener, TcpStream};
use mio::{Events, Interest, Poll, Token, Waker};
use std::collections::HashMap;
use std::io::{self, Read, Write};
use std::sync::{Arc, mpsc};

use foldhash::fast::FixedState;

use crate::bus::InvalidationBus;
use crate::cache::{DbCacheManager, EncodedResult};
use crate::config::DbCacheConfig;
use crate::pool::UpstreamPool;
use crate::protocol;
use crate::query::{ParsedQuery, QueryKind, compute_fingerprint};

const LISTENER_TK: Token = Token(0);
const WAKER_TK: Token = Token(1);
const CONN_BASE: usize = 2;

const READ_CAP: usize = 32 * 1024;
const FLUSH_THRESH: usize = 32 * 1024;
const MAX_NAMED: usize = 512;
const PORTAL_SUSPENDED: [u8; 5] = [b's', 0, 0, 0, 4];

// ────────────────────────────────────── read buffer

struct ReadBuf {
    buf: Vec<u8>,
    consumed: usize,
}

impl ReadBuf {
    fn new() -> Self {
        Self { buf: Vec::with_capacity(READ_CAP), consumed: 0 }
    }

    fn fill(&mut self, stream: &mut TcpStream) -> io::Result<()> {
        let mut tmp = [0u8; 8192];
        loop {
            match stream.read(&mut tmp) {
                Ok(0) => return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "eof")),
                Ok(n) => self.buf.extend_from_slice(&tmp[..n]),
                Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => return Ok(()),
                Err(e) => return Err(e),
            }
        }
    }

    fn available(&self) -> &[u8] {
        &self.buf[self.consumed..]
    }

    fn take_startup_frame(&mut self) -> Option<Vec<u8>> {
        let avail = self.available();
        if avail.len() < 4 {
            return None;
        }
        let total_len = u32::from_be_bytes([avail[0], avail[1], avail[2], avail[3]]) as usize;
        if total_len < 8 || total_len > (1 << 20) {
            return None;
        }
        if avail.len() < total_len {
            return None;
        }
        let frame = avail[..total_len].to_vec();
        self.consumed += total_len;
        self.compact();
        Some(frame)
    }

    fn take_regular_frame(&mut self) -> Option<(u8, Vec<u8>)> {
        let avail = self.available();
        if avail.len() < 5 {
            return None;
        }
        let msg_type = avail[0];
        let len = i32::from_be_bytes([avail[1], avail[2], avail[3], avail[4]]);
        if len < 4 || len as usize > (64 << 20) {
            return None;
        }
        let body_len = len as usize - 4;
        let total = 5 + body_len;
        if avail.len() < total {
            return None;
        }
        let body = avail[5..total].to_vec();
        self.consumed += total;
        self.compact();
        Some((msg_type, body))
    }

    fn has_more(&self) -> bool {
        self.buf.len() > self.consumed
    }

    fn compact(&mut self) {
        if self.consumed > 4096 && self.consumed >= self.buf.len() / 2 {
            self.buf.drain(..self.consumed);
            self.consumed = 0;
        }
    }
}

// ────────────────────────────────────── write buffer

struct WriteBuf {
    buf: Vec<u8>,
    pos: usize,
}

impl WriteBuf {
    fn new() -> Self {
        Self { buf: Vec::with_capacity(FLUSH_THRESH + 512), pos: 0 }
    }

    fn extend(&mut self, data: &[u8]) {
        self.buf.extend_from_slice(data);
    }

    fn is_empty(&self) -> bool {
        self.pos >= self.buf.len()
    }

    fn pending_len(&self) -> usize {
        self.buf.len().saturating_sub(self.pos)
    }

    fn flush_to(&mut self, stream: &mut TcpStream) -> io::Result<bool> {
        while self.pos < self.buf.len() {
            match stream.write(&self.buf[self.pos..]) {
                Ok(0) => return Err(io::Error::new(io::ErrorKind::WriteZero, "write zero")),
                Ok(n) => self.pos += n,
                Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => return Ok(false),
                Err(e) => return Err(e),
            }
        }
        self.buf.clear();
        self.pos = 0;
        Ok(true)
    }
}

// ────────────────────────────────────── session state (post-auth)

struct StmtEntry {
    sql: String,
    param_oids: Vec<u32>,
    #[allow(dead_code)] // retained for upstream relay path
    parse_frame: Vec<u8>,
    row_description: Option<Vec<u8>>,
}

struct PortalEntry {
    statement: String,
    parsed: Arc<ParsedQuery>,
    fingerprint: u64,
    text_results: bool,
    #[allow(dead_code)] // retained for upstream relay path
    bind_frame: Vec<u8>,
    cursor: usize,
    finished: bool,
}

struct ActiveState {
    mgr: Arc<DbCacheManager>,
    config: Arc<DbCacheConfig>,
    in_transaction: bool,
    transaction_failed: bool,
    transaction_writes: Vec<String>,
    statements: HashMap<String, StmtEntry, FixedState>,
    portals: HashMap<String, PortalEntry, FixedState>,
    cycle_error: bool,
    hot_sql_fp: u64,
    hot_parsed: Option<Arc<ParsedQuery>>,
}

impl ActiveState {
    fn new(mgr: Arc<DbCacheManager>, config: Arc<DbCacheConfig>) -> Self {
        Self {
            mgr,
            config,
            in_transaction: false,
            transaction_failed: false,
            transaction_writes: Vec::new(),
            statements: HashMap::with_hasher(FixedState::with_seed(0)),
            portals: HashMap::with_hasher(FixedState::with_seed(0)),
            cycle_error: false,
            hot_sql_fp: 0,
            hot_parsed: None,
        }
    }

    fn effective_fp(&self, base: u64) -> u64 {
        // context_hash is always 0 in the no-upstream mio path (Trusted state).
        base
    }

    fn lookup_encoded(&self, parsed: &ParsedQuery, fp: u64) -> Option<Arc<EncodedResult>> {
        let primary = parsed.tables.first().map(String::as_str).unwrap_or("default");
        if let Some(e) = self.mgr.get_encoded(&self.config.key_prefix, primary, fp) {
            return Some(e);
        }
        for table in parsed.tables.iter().skip(1) {
            if let Some(e) = self.mgr.get_encoded(&self.config.key_prefix, table, fp) {
                return Some(e);
            }
        }
        None
    }

    fn lookup_json_and_promote(&self, parsed: &ParsedQuery, fp: u64) -> Option<crate::row::DbResultSet> {
        for table in &parsed.tables {
            if let Some(r) = self.mgr.get_cached_query(&self.config.key_prefix, table, fp) {
                self.mgr.remember_encoded(&self.config.key_prefix, table, fp, &r, self.config.default_ttl_ms);
                return Some(r);
            }
        }
        None
    }

    fn queue_ready(&self, out: &mut Vec<u8>) {
        let status = if self.transaction_failed { b'E' } else if self.in_transaction { b'T' } else { b'I' };
        out.extend_from_slice(&protocol::encode_ready(status));
    }

    fn queue_error(&mut self, out: &mut Vec<u8>, code: &str, msg: &str) {
        out.extend_from_slice(&protocol::encode_error("ERROR", code, msg));
        if self.in_transaction {
            self.transaction_failed = true;
        }
    }

    // Returns true = handled locally, false = needs upstream (offload).
    fn handle_query(&mut self, out: &mut Vec<u8>, body: &[u8]) -> bool {
        let sql_bytes = body.strip_suffix(b"\0").unwrap_or(body);
        let sql = std::str::from_utf8(sql_bytes).unwrap_or("").trim();

        if sql.is_empty() {
            protocol::put_empty_query(out);
            self.queue_ready(out);
            return true;
        }

        let sql_fp = compute_fingerprint(sql);
        let parsed = match &self.hot_parsed {
            Some(p) if self.hot_sql_fp == sql_fp => Arc::clone(p),
            _ => {
                let p = self.mgr.analyze(sql);
                self.hot_sql_fp = sql_fp;
                self.hot_parsed = Some(Arc::clone(&p));
                p
            }
        };

        match parsed.kind {
            QueryKind::Begin => {
                self.in_transaction = true;
                self.transaction_writes.clear();
            }
            QueryKind::Commit => {
                self.in_transaction = false;
                self.transaction_failed = false;
            }
            QueryKind::Rollback => {
                self.in_transaction = false;
                self.transaction_failed = false;
                self.transaction_writes.clear();
            }
            _ => {}
        }

        let fp = self.effective_fp(parsed.fingerprint);
        let cacheable = parsed.is_cacheable
            && parsed.kind == QueryKind::Select
            && !self.in_transaction
            && self.mgr.is_cacheable(&parsed);

        if cacheable {
            if let Some(entry) = self.lookup_encoded(&parsed, fp) {
                entry.write_simple_response(out);
                return true;
            }
            if let Some(result) = self.lookup_json_and_promote(&parsed, fp) {
                if let Some(entry) = self.lookup_encoded(&parsed, fp) {
                    entry.write_simple_response(out);
                    return true;
                }
                let tag = format!("SELECT {}", result.rows.len());
                let _ = protocol::write_result_set(out, &result, &tag);
                return true;
            }
            self.mgr.metrics().misses.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            // cache miss — no upstream in this path, return error
        }

        // Local fallback for non-SELECT kinds
        match parsed.kind {
            QueryKind::Begin | QueryKind::Commit | QueryKind::Rollback | QueryKind::Set => {
                let tag = match parsed.kind {
                    QueryKind::Begin => "BEGIN",
                    QueryKind::Commit => "COMMIT",
                    QueryKind::Rollback => "ROLLBACK",
                    _ => "SET",
                };
                out.extend_from_slice(&protocol::encode_command_complete(tag));
            }
            _ => {
                self.queue_error(out, "08006", "no upstream database configured for cache miss");
            }
        }
        self.queue_ready(out);
        true
    }

    fn handle_parse(&mut self, out: &mut Vec<u8>, raw_frame: Vec<u8>, body: &[u8]) -> bool {
        if self.cycle_error {
            return true;
        }
        if !self.statements.is_empty()
            && self.statements.len() >= MAX_NAMED
            && !self.statements.contains_key(body.split(|&b| b == 0).next().and_then(|s| std::str::from_utf8(s).ok()).unwrap_or(""))
        {
            self.queue_error(out, "53400", "too many prepared statements");
            self.cycle_error = true;
            return true;
        }

        let mut pos = 0usize;
        let name = take_cstr(body, &mut pos).unwrap_or("").to_string();
        let sql = take_cstr(body, &mut pos).unwrap_or("").to_string();
        let count = rd_i16(body, pos).max(0) as usize;
        pos += 2;
        let mut param_oids = Vec::with_capacity(count.min(256));
        for _ in 0..count.min(256) {
            if pos + 4 <= body.len() {
                param_oids.push(u32::from_be_bytes([body[pos], body[pos+1], body[pos+2], body[pos+3]]));
                pos += 4;
            }
        }

        if !name.is_empty() && !self.statements.contains_key(&name) && self.statements.len() >= MAX_NAMED {
            self.queue_error(out, "53400", "too many prepared statements");
            self.cycle_error = true;
            return true;
        }

        let parsed = self.mgr.analyze(&sql);
        let locally_servable = parsed.is_cacheable
            && parsed.kind == QueryKind::Select
            && !self.in_transaction
            && self.mgr.is_cacheable(&parsed);

        if !locally_servable {
            // For non-locally-servable: best effort — insert and send ParseComplete.
            // If the portal has a cache hit at Execute time, it works.
            // If not, we'll error at Execute.
        }

        self.statements.insert(name.clone(), StmtEntry {
            sql,
            param_oids,
            parse_frame: raw_frame,
            row_description: None,
        });

        protocol::put_parse_complete(out);
        true
    }

    fn handle_bind(&mut self, out: &mut Vec<u8>, raw_frame: Vec<u8>, body: &[u8]) -> bool {
        if self.cycle_error {
            return true;
        }

        let mut pos = 0usize;
        let portal = take_cstr(body, &mut pos).unwrap_or("").to_string();
        let statement = take_cstr(body, &mut pos).unwrap_or("").to_string();

        if !portal.is_empty() && !self.portals.contains_key(&portal) && self.portals.len() >= MAX_NAMED {
            self.queue_error(out, "53400", "too many open portals");
            self.cycle_error = true;
            return true;
        }

        let Some(stmt) = self.statements.get(&statement) else {
            self.queue_error(out, "26000", "prepared statement does not exist");
            self.cycle_error = true;
            return true;
        };
        let sql = stmt.sql.clone();

        let fmt_count = rd_i16(body, pos).max(0) as usize;
        pos += 2;
        let mut param_formats = Vec::with_capacity(fmt_count.min(256));
        for _ in 0..fmt_count.min(256) {
            if pos + 2 <= body.len() {
                param_formats.push(i16::from_be_bytes([body[pos], body[pos+1]]));
                pos += 2;
            }
        }

        let param_count = rd_i16(body, pos).max(0) as usize;
        pos += 2;
        let mut params: Vec<Option<Vec<u8>>> = Vec::with_capacity(param_count.min(256));
        for _ in 0..param_count.min(256) {
            if pos + 4 > body.len() { break; }
            let len = i32::from_be_bytes([body[pos], body[pos+1], body[pos+2], body[pos+3]]);
            pos += 4;
            if len < 0 {
                params.push(None);
            } else {
                let l = len as usize;
                if pos + l <= body.len() {
                    params.push(Some(body[pos..pos+l].to_vec()));
                    pos += l;
                } else {
                    params.push(None);
                }
            }
        }

        let res_count = rd_i16(body, pos).max(0) as usize;
        pos += 2;
        let mut result_formats = Vec::with_capacity(res_count.min(16));
        for _ in 0..res_count.min(16) {
            if pos + 2 <= body.len() {
                result_formats.push(i16::from_be_bytes([body[pos], body[pos+1]]));
                pos += 2;
            }
        }

        let text_results = result_formats.is_empty() || result_formats.iter().all(|&f| f == 0);
        let (parsed, base_fp) = self.mgr.analyze_bound(&sql, &params, &param_formats);

        self.portals.insert(portal, PortalEntry {
            statement,
            parsed,
            fingerprint: base_fp,
            text_results,
            bind_frame: raw_frame,
            cursor: 0,
            finished: false,
        });

        protocol::put_bind_complete(out);
        true
    }

    fn handle_execute(&mut self, out: &mut Vec<u8>, body: &[u8]) -> bool {
        if self.cycle_error {
            return true;
        }

        let mut pos = 0usize;
        let portal_name = take_cstr(body, &mut pos).unwrap_or("").to_string();
        let max_rows = if pos + 4 <= body.len() {
            i32::from_be_bytes([body[pos], body[pos+1], body[pos+2], body[pos+3]])
        } else { 0 };

        let Some(portal) = self.portals.get(&portal_name) else {
            self.queue_error(out, "34000", "portal does not exist");
            self.cycle_error = true;
            return true;
        };

        let parsed = Arc::clone(&portal.parsed);
        let base_fp = portal.fingerprint;
        let text_results = portal.text_results;
        let fp = self.effective_fp(base_fp);

        let cacheable = parsed.is_cacheable
            && parsed.kind == QueryKind::Select
            && !self.in_transaction
            && text_results
            && self.mgr.is_cacheable(&parsed);

        if cacheable {
            if let Some(entry) = self.lookup_encoded(&parsed, fp) {
                let portal_mut = self.portals.get_mut(&portal_name).unwrap();
                serve_encoded_from_portal(out, portal_mut, &entry, max_rows);
                return true;
            }
            if let Some(result) = self.lookup_json_and_promote(&parsed, fp) {
                if let Some(entry) = self.lookup_encoded(&parsed, fp) {
                    let portal_mut = self.portals.get_mut(&portal_name).unwrap();
                    serve_encoded_from_portal(out, portal_mut, &entry, max_rows);
                    return true;
                }
                let tag = format!("SELECT {}", result.rows.len());
                let _ = protocol::write_result_set(out, &result, &tag);
                if let Some(p) = self.portals.get_mut(&portal_name) {
                    p.finished = true;
                }
                return true;
            }
            self.mgr.metrics().misses.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }

        // Local fallback for non-select queries via extended protocol
        match parsed.kind {
            QueryKind::Begin => {
                self.in_transaction = true;
                self.transaction_writes.clear();
                out.extend_from_slice(&protocol::encode_command_complete("BEGIN"));
                if let Some(p) = self.portals.get_mut(&portal_name) { p.finished = true; }
            }
            QueryKind::Commit => {
                self.in_transaction = false;
                self.transaction_failed = false;
                self.transaction_writes.clear();
                out.extend_from_slice(&protocol::encode_command_complete("COMMIT"));
                if let Some(p) = self.portals.get_mut(&portal_name) { p.finished = true; }
            }
            QueryKind::Rollback => {
                self.in_transaction = false;
                self.transaction_failed = false;
                self.transaction_writes.clear();
                out.extend_from_slice(&protocol::encode_command_complete("ROLLBACK"));
                if let Some(p) = self.portals.get_mut(&portal_name) { p.finished = true; }
            }
            _ => {
                self.queue_error(out, "08006", "no upstream database configured for cache miss");
                self.cycle_error = true;
            }
        }
        true
    }

    fn handle_sync(&mut self, out: &mut Vec<u8>) {
        self.cycle_error = false;
        self.queue_ready(out);
    }

    fn handle_describe(&mut self, out: &mut Vec<u8>, body: &[u8]) {
        if self.cycle_error || body.is_empty() {
            return;
        }
        let kind = body[0];
        let mut pos = 1usize;
        let name = take_cstr(body, &mut pos).unwrap_or("").to_string();

        match kind {
            b'S' => {
                let Some(stmt) = self.statements.get(&name) else {
                    self.queue_error(out, "26000", "prepared statement does not exist");
                    self.cycle_error = true;
                    return;
                };
                let oids = stmt.param_oids.clone();
                let rd = stmt.row_description.clone();
                protocol::put_parameter_description(out, &oids);
                if let Some(row_desc) = rd {
                    out.extend_from_slice(&row_desc);
                } else {
                    protocol::put_no_data(out);
                }
            }
            b'P' => {
                // For cache-only portals we don't have upstream row description;
                // send NoData which is safe for the client to handle.
                protocol::put_no_data(out);
            }
            _ => {
                self.queue_error(out, "08P01", "invalid describe type");
                self.cycle_error = true;
            }
        }
    }

    fn handle_close(&mut self, out: &mut Vec<u8>, body: &[u8]) {
        if body.is_empty() {
            return;
        }
        let kind = body[0];
        let mut pos = 1usize;
        let name = take_cstr(body, &mut pos).unwrap_or("").to_string();
        match kind {
            b'S' => {
                self.statements.remove(&name);
                self.portals.retain(|_, p| p.statement != name);
            }
            b'P' => {
                self.portals.remove(&name);
            }
            _ => {}
        }
        protocol::put_close_complete(out);
    }
}

fn serve_encoded_from_portal(out: &mut Vec<u8>, portal: &mut PortalEntry, entry: &EncodedResult, max_rows: i32) {
    let limit = if max_rows > 0 { max_rows as usize } else { usize::MAX };
    write_encoded_rows(out, &mut portal.cursor, entry, limit);
    let exhausted = next_data_frame_end(entry.data_rows(), portal.cursor).is_none();
    if max_rows > 0 && !exhausted {
        out.extend_from_slice(&PORTAL_SUSPENDED);
    } else {
        out.extend_from_slice(entry.command_complete());
        portal.cursor = entry.data_rows().len();
        portal.finished = true;
    }
}

fn write_encoded_rows(out: &mut Vec<u8>, cursor: &mut usize, entry: &EncodedResult, limit: usize) {
    let data = entry.data_rows();
    if limit == usize::MAX && *cursor == 0 {
        out.extend_from_slice(data);
        *cursor = data.len();
        return;
    }
    let mut sent = 0usize;
    let mut offset = *cursor;
    while sent < limit {
        let Some(next) = next_data_frame_end(data, offset) else { break; };
        out.extend_from_slice(&data[offset..next]);
        offset = next;
        sent += 1;
    }
    *cursor = offset;
}

fn next_data_frame_end(data: &[u8], offset: usize) -> Option<usize> {
    if offset + 5 > data.len() { return None; }
    let len = i32::from_be_bytes([data[offset+1], data[offset+2], data[offset+3], data[offset+4]]);
    if len < 4 { return None; }
    let total = 1 + len as usize;
    if offset + total > data.len() { return None; }
    Some(offset + total)
}

fn take_cstr<'a>(buf: &'a [u8], pos: &mut usize) -> Option<&'a str> {
    let start = *pos;
    if start >= buf.len() { return None; }
    let rel = buf[start..].iter().position(|&b| b == 0)?;
    let end = start + rel;
    *pos = end + 1;
    std::str::from_utf8(&buf[start..end]).ok()
}

fn rd_i16(buf: &[u8], pos: usize) -> i16 {
    if pos + 2 <= buf.len() {
        i16::from_be_bytes([buf[pos], buf[pos+1]])
    } else {
        0
    }
}

// ────────────────────────────────────── connection state

enum ConnState {
    Startup,
    Offloaded,
    Active(Box<ActiveState>),
    #[allow(dead_code)]
    Dead,
}

struct ProxyConn {
    stream: TcpStream,
    state: ConnState,
    rbuf: ReadBuf,
    wbuf: WriteBuf,
    wants_write: bool,
}

impl ProxyConn {
    fn new(stream: TcpStream) -> Self {
        Self {
            stream,
            state: ConnState::Startup,
            rbuf: ReadBuf::new(),
            wbuf: WriteBuf::new(),
            wants_write: false,
        }
    }
}

// ────────────────────────────────────── blocking sub-pool

pub(super) struct BlockingPool {
    tx: mpsc::SyncSender<PoolJob>,
}

struct PoolJob {
    conn_id: usize,
    stream: std::net::TcpStream,
    config: Arc<DbCacheConfig>,
    mgr: Arc<DbCacheManager>,
    pool: Arc<UpstreamPool>,
    bus: Option<Arc<InvalidationBus>>,
    done_tx: mpsc::SyncSender<usize>,
    waker: Arc<Waker>,
}

impl BlockingPool {
    pub(super) fn start(thread_count: usize) -> Arc<Self> {
        let (job_tx, job_rx) = mpsc::sync_channel::<PoolJob>(thread_count * 8);
        let job_rx = Arc::new(std::sync::Mutex::new(job_rx));
        for i in 0..thread_count {
            let rx = Arc::clone(&job_rx);
            std::thread::Builder::new()
                .name(format!("fyrodb-dbblocking-{i}"))
                .stack_size(super::session::CLIENT_STACK)
                .spawn(move || loop {
                    let job = match rx.lock().unwrap().recv() {
                        Ok(j) => j,
                        Err(_) => return,
                    };
                    let _ = super::session::handle_client(
                        job.stream,
                        (*job.config).clone(),
                        job.mgr,
                        job.pool,
                        job.bus,
                    );
                    let _ = job.done_tx.try_send(job.conn_id);
                    let _ = job.waker.wake();
                })
                .unwrap();
        }
        Arc::new(BlockingPool { tx: job_tx })
    }

    pub(super) fn submit(
        &self,
        conn_id: usize,
        stream: std::net::TcpStream,
        config: Arc<DbCacheConfig>,
        mgr: Arc<DbCacheManager>,
        pool: Arc<UpstreamPool>,
        bus: Option<Arc<InvalidationBus>>,
        done_tx: mpsc::SyncSender<usize>,
        waker: Arc<Waker>,
    ) -> bool {
        self.tx.try_send(PoolJob {
            conn_id,
            stream,
            config,
            mgr,
            pool,
            bus,
            done_tx,
            waker,
        }).is_ok()
    }
}

// ────────────────────────────────────── worker entry point

pub(super) fn run_proxy_worker(
    mut listener: TcpListener,
    mgr: Arc<DbCacheManager>,
    config: Arc<DbCacheConfig>,
    pool: Arc<UpstreamPool>,
    bus: Option<Arc<InvalidationBus>>,
    blocking_pool: Arc<BlockingPool>,
    done_rx: mpsc::Receiver<usize>,
    done_tx: mpsc::SyncSender<usize>,
) {
    let mut poll = Poll::new().unwrap();
    let mut events = Events::with_capacity(512);

    let waker = Arc::new(Waker::new(poll.registry(), WAKER_TK).unwrap());

    poll.registry()
        .register(&mut listener, LISTENER_TK, Interest::READABLE)
        .unwrap();

    let mut conns: Vec<Option<ProxyConn>> = Vec::new();
    let mut free: Vec<usize> = Vec::new();
    let mut next_id: usize = 0;
    let mut write_pending: Vec<usize> = Vec::new();

    loop {
        match poll.poll(&mut events, None) {
            Ok(_) => {}
            Err(ref e) if e.kind() == io::ErrorKind::Interrupted => continue,
            Err(_) => continue,
        }

        for event in events.iter() {
            match event.token() {
                LISTENER_TK => {
                    do_accept(
                        &mut listener,
                        poll.registry(),
                        &mut conns,
                        &mut free,
                        &mut next_id,
                    );
                }
                WAKER_TK => {
                    while let Ok(id) = done_rx.try_recv() {
                        free_slot(&mut conns, poll.registry(), &mut free, id);
                    }
                }
                token => {
                    let id = token.0.wrapping_sub(CONN_BASE);
                    let close = dispatch_event(
                        &mut conns,
                        id,
                        event,
                        &mgr,
                        &config,
                        &pool,
                        &bus,
                        &blocking_pool,
                        &done_tx,
                        &waker,
                    );
                    if close {
                        free_slot(&mut conns, poll.registry(), &mut free, id);
                    } else if let Some(Some(conn)) = conns.get(id) {
                        if !conn.wbuf.is_empty() {
                            write_pending.push(id);
                        }
                    }
                }
            }
        }

        for id in write_pending.drain(..) {
            sync_write_interest(poll.registry(), &mut conns, id);
        }
    }
}

fn dispatch_event(
    conns: &mut Vec<Option<ProxyConn>>,
    id: usize,
    event: &mio::event::Event,
    mgr: &Arc<DbCacheManager>,
    config: &Arc<DbCacheConfig>,
    pool: &Arc<UpstreamPool>,
    bus: &Option<Arc<InvalidationBus>>,
    blocking_pool: &Arc<BlockingPool>,
    done_tx: &mpsc::SyncSender<usize>,
    waker: &Arc<Waker>,
) -> bool {
    let Some(Some(conn)) = conns.get_mut(id) else {
        return false;
    };

    if event.is_readable() {
        // Fill read buffer
        if let Err(e) = conn.rbuf.fill(&mut conn.stream) {
            if e.kind() != io::ErrorKind::WouldBlock {
                return true;
            }
        }

        let keep = match conn.state {
            ConnState::Startup => {
                handle_startup_event(conn, id, mgr, config, pool, bus, blocking_pool, done_tx, waker)
            }
            ConnState::Active(_) => {
                handle_active_event(conn)
            }
            ConnState::Offloaded | ConnState::Dead => false,
        };
        if !keep {
            return true;
        }
    }

    if event.is_writable() {
        if let Some(Some(conn)) = conns.get_mut(id) {
            match conn.wbuf.flush_to(&mut conn.stream) {
                Ok(_) => {}
                Err(_) => return true,
            }
        }
    }

    false
}

fn handle_startup_event(
    conn: &mut ProxyConn,
    id: usize,
    mgr: &Arc<DbCacheManager>,
    config: &Arc<DbCacheConfig>,
    pool: &Arc<UpstreamPool>,
    bus: &Option<Arc<InvalidationBus>>,
    blocking_pool: &Arc<BlockingPool>,
    done_tx: &mpsc::SyncSender<usize>,
    waker: &Arc<Waker>,
) -> bool {
    loop {
        let Some(frame) = conn.rbuf.take_startup_frame() else {
            return true; // need more data
        };
        if frame.len() < 8 {
            return false;
        }
        let code = i32::from_be_bytes([frame[4], frame[5], frame[6], frame[7]]);

        if code == protocol::SSL_REQUEST_CODE || code == protocol::GSSENC_REQUEST_CODE {
            conn.wbuf.extend(b"N");
            let _ = conn.wbuf.flush_to(&mut conn.stream);
            continue;
        }
        if code == protocol::CANCEL_REQUEST_CODE {
            return false;
        }
        if (code >> 16) != 3 {
            return false;
        }

        // Real Startup message.
        if config.upstream_addr.is_some() {
            return offload_conn(conn, id, frame, config, mgr, pool, bus, blocking_pool, done_tx, waker);
        }

        // No upstream: send AuthOk + params + ReadyForQuery inline.
        let mut tmp = Vec::new();
        if protocol::write_startup_ok(&mut tmp).is_err() {
            return false;
        }
        conn.wbuf.extend(&tmp);
        conn.state = ConnState::Active(Box::new(ActiveState::new(
            Arc::clone(mgr),
            Arc::clone(config),
        )));
        let _ = conn.wbuf.flush_to(&mut conn.stream);
        return true;
    }
}

#[cfg(unix)]
fn offload_conn(
    conn: &mut ProxyConn,
    id: usize,
    _startup_frame: Vec<u8>,
    config: &Arc<DbCacheConfig>,
    mgr: &Arc<DbCacheManager>,
    pool: &Arc<UpstreamPool>,
    bus: &Option<Arc<InvalidationBus>>,
    blocking_pool: &Arc<BlockingPool>,
    done_tx: &mpsc::SyncSender<usize>,
    waker: &Arc<Waker>,
) -> bool {
    use std::os::unix::io::FromRawFd;

    // We cannot move conn.stream directly because we only have &mut ProxyConn.
    // Swap it out using a raw fd trick: dup the fd, create a new std TcpStream,
    // mark the mio stream as offloaded by replacing state.
    let raw_fd = {
        let fd = std::os::unix::io::AsRawFd::as_raw_fd(&conn.stream);
        libc_dup(fd)
    };
    if raw_fd < 0 {
        return false;
    }
    let std_stream = unsafe { std::net::TcpStream::from_raw_fd(raw_fd) };
    if std_stream.set_nonblocking(false).is_err() {
        return false;
    }
    conn.state = ConnState::Offloaded;
    blocking_pool.submit(
        id,
        std_stream,
        Arc::clone(config),
        Arc::clone(mgr),
        Arc::clone(pool),
        bus.clone(),
        done_tx.clone(),
        Arc::clone(waker),
    );
    true
}

#[cfg(not(unix))]
fn offload_conn(
    conn: &mut ProxyConn,
    _id: usize,
    _startup_frame: Vec<u8>,
    _config: &Arc<DbCacheConfig>,
    _mgr: &Arc<DbCacheManager>,
    _pool: &Arc<UpstreamPool>,
    _bus: &Option<Arc<InvalidationBus>>,
    _blocking_pool: &Arc<BlockingPool>,
    _done_tx: &mpsc::SyncSender<usize>,
    _waker: &Arc<Waker>,
) -> bool {
    conn.state = ConnState::Dead;
    false
}

#[cfg(unix)]
fn libc_dup(fd: std::os::unix::io::RawFd) -> std::os::unix::io::RawFd {
    unsafe extern "C" {
        fn dup(fd: i32) -> i32;
    }
    unsafe { dup(fd) }
}

fn handle_active_event(conn: &mut ProxyConn) -> bool {
    // Collect all available complete frames first (avoids borrow conflicts
    // between conn.rbuf, conn.state, and conn.wbuf).
    let mut frames: Vec<(u8, Vec<u8>)> = Vec::new();
    while let Some(frame) = conn.rbuf.take_regular_frame() {
        frames.push(frame);
    }
    let rbuf_drained = !conn.rbuf.has_more();

    // Extract active state (borrows conn.state)
    let active = match &mut conn.state {
        ConnState::Active(a) => a.as_mut(),
        _ => return false,
    };

    // Accumulate output into a local buffer to avoid simultaneous borrow of
    // conn.state (via active) and conn.wbuf.
    let mut out = Vec::new();
    let mut should_terminate = false;
    let mut flush_now = false;

    for (msg_type, body) in frames {
        match msg_type {
            b'Q' => {
                active.handle_query(&mut out, &body);
                flush_now = true; // simple query always flushes
            }
            b'P' => {
                let mut raw = Vec::with_capacity(5 + body.len());
                raw.push(b'P');
                raw.extend_from_slice(&((body.len() as i32 + 4).to_be_bytes()));
                raw.extend_from_slice(&body);
                active.handle_parse(&mut out, raw, &body);
            }
            b'B' => {
                let mut raw = Vec::with_capacity(5 + body.len());
                raw.push(b'B');
                raw.extend_from_slice(&((body.len() as i32 + 4).to_be_bytes()));
                raw.extend_from_slice(&body);
                active.handle_bind(&mut out, raw, &body);
            }
            b'E' => {
                active.handle_execute(&mut out, &body);
            }
            b'S' => {
                active.handle_sync(&mut out);
                flush_now = true;
            }
            b'H' => {
                flush_now = true;
            }
            b'X' => {
                should_terminate = true;
                break;
            }
            b'D' => {
                active.handle_describe(&mut out, &body);
            }
            b'C' => {
                active.handle_close(&mut out, &body);
            }
            _ => {}
        }
    }

    // active borrow ends here — now we can freely use conn.wbuf / conn.stream
    let _ = active; // end borrow of conn.state before using conn.wbuf

    conn.wbuf.extend(&out);

    let do_flush = should_terminate
        || flush_now
        || rbuf_drained
        || conn.wbuf.pending_len() >= FLUSH_THRESH;
    if do_flush {
        let _ = conn.wbuf.flush_to(&mut conn.stream);
    }

    !should_terminate
}

fn do_accept(
    listener: &mut TcpListener,
    registry: &mio::Registry,
    conns: &mut Vec<Option<ProxyConn>>,
    free: &mut Vec<usize>,
    next_id: &mut usize,
) {
    loop {
        match listener.accept() {
            Ok((mut stream, _)) => {
                let _ = socket2::SockRef::from(&stream).set_tcp_nodelay(true);
                #[cfg(target_os = "linux")]
                let _ = socket2::SockRef::from(&stream).set_tcp_quickack(true);
                let id = alloc_slot(free, next_id, conns);
                if registry
                    .register(&mut stream, Token(id + CONN_BASE), Interest::READABLE)
                    .is_ok()
                {
                    conns[id] = Some(ProxyConn::new(stream));
                }
            }
            Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => break,
            Err(_) => break,
        }
    }
}

fn sync_write_interest(
    registry: &mio::Registry,
    conns: &mut Vec<Option<ProxyConn>>,
    id: usize,
) {
    let Some(Some(conn)) = conns.get_mut(id) else { return; };
    let wants = !conn.wbuf.is_empty();
    if wants == conn.wants_write { return; }
    let interest = if wants {
        Interest::READABLE | Interest::WRITABLE
    } else {
        Interest::READABLE
    };
    if registry.reregister(&mut conn.stream, Token(id + CONN_BASE), interest).is_ok() {
        conn.wants_write = wants;
    }
}

fn free_slot(conns: &mut Vec<Option<ProxyConn>>, registry: &mio::Registry, free: &mut Vec<usize>, id: usize) {
    if let Some(slot) = conns.get_mut(id) && let Some(mut conn) = slot.take() {
        let _ = registry.deregister(&mut conn.stream);
        free.push(id);
    }
}

fn alloc_slot(free: &mut Vec<usize>, next: &mut usize, conns: &mut Vec<Option<ProxyConn>>) -> usize {
    if let Some(id) = free.pop() { return id; }
    let id = *next;
    *next += 1;
    if id >= conns.len() {
        conns.resize_with(id + 1, || None);
    }
    id
}
