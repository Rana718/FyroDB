// Non-blocking mio proxy worker.
// Multiplexes connections with inline cache hits and a blocking sub-pool.

use mio::net::{TcpListener, TcpStream};
use mio::{Events, Interest, Poll, Token};
use std::collections::HashMap;
use std::io::{self, Read, Write};
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;

use foldhash::fast::FixedState;

use crate::bus::InvalidationBus;
use crate::cache::{DbCacheManager, EncodedResult, coarse_now_ms};
use crate::config::DbCacheConfig;
use crate::pool::UpstreamPool;
use crate::protocol;
use crate::query::{ParsedQuery, QueryKind, compute_fingerprint};

use super::{ConnectionPermit, reject_too_many, try_admit};

const LISTENER_TK: Token = Token(0);
const CONN_BASE: usize = 1;

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
      Self {
         buf: Vec::with_capacity(READ_CAP),
         consumed: 0,
      }
   }

   fn fill(&mut self, stream: &mut TcpStream) -> io::Result<()> {
      if self.consumed > 16384 && self.consumed >= self.buf.len() / 2 {
         self.buf.drain(..self.consumed);
         self.consumed = 0;
      } else if self.consumed == self.buf.len() {
         self.buf.clear();
         self.consumed = 0;
      }

      loop {
         let start = self.buf.len();
         if self.buf.capacity() - start < 65536 {
            self.buf.reserve(65536);
         }
         let spare = self.buf.spare_capacity_mut();
         let slice =
            unsafe { std::slice::from_raw_parts_mut(spare.as_mut_ptr() as *mut u8, spare.len()) };
         match stream.read(slice) {
            Ok(0) => return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "eof")),
            Ok(n) => unsafe {
               self.buf.set_len(start + n);
            },
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
      if !(8..=(1 << 20)).contains(&total_len) {
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

   #[allow(dead_code)]
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

   #[allow(dead_code)]
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
      Self {
         buf: Vec::with_capacity(128 * 1024),
         pos: 0,
      }
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
   parsed: Arc<ParsedQuery>,
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
   cached_encoded: Option<Arc<EncodedResult>>,
   /// `DbCacheManager::generation` read before `cached_encoded` was looked up.
   cached_generation: u64,
}

/// Connection-local shortcut to a cached result. Only valid while the
/// manager's generation still equals `generation`: before, these copies only
/// checked the TTL, so they kept serving a result after a write invalidated it.
#[derive(Clone)]
struct HotQuerySlot {
   fp: u64,
   parsed: Arc<ParsedQuery>,
   encoded: Option<Arc<EncodedResult>>,
   generation: u64,
}

const HOT_SLOTS_CAP: usize = 64;

struct ActiveState {
   mgr: Arc<DbCacheManager>,
   config: Arc<DbCacheConfig>,
   in_transaction: bool,
   transaction_failed: bool,
   transaction_writes: Vec<String>,
   unnamed_stmt: Option<StmtEntry>,
   unnamed_portal: Option<PortalEntry>,
   statements: HashMap<String, StmtEntry, FixedState>,
   portals: HashMap<String, PortalEntry, FixedState>,
   cycle_error: bool,
   hot_slots: [Option<HotQuerySlot>; HOT_SLOTS_CAP],
}

impl ActiveState {
   fn new(mgr: Arc<DbCacheManager>, config: Arc<DbCacheConfig>) -> Self {
      const NONE_SLOT: Option<HotQuerySlot> = None;
      Self {
         mgr,
         config,
         in_transaction: false,
         transaction_failed: false,
         transaction_writes: Vec::new(),
         unnamed_stmt: None,
         unnamed_portal: None,
         statements: HashMap::with_hasher(FixedState::with_seed(0)),
         portals: HashMap::with_hasher(FixedState::with_seed(0)),
         cycle_error: false,
         hot_slots: [NONE_SLOT; HOT_SLOTS_CAP],
      }
   }

   #[inline]
   fn get_statement(&self, name: &str) -> Option<&StmtEntry> {
      if name.is_empty() {
         self.unnamed_stmt.as_ref()
      } else {
         self.statements.get(name)
      }
   }

   #[inline]
   fn get_portal_mut(&mut self, name: &str) -> Option<&mut PortalEntry> {
      if name.is_empty() {
         self.unnamed_portal.as_mut()
      } else {
         self.portals.get_mut(name)
      }
   }

   fn effective_fp(&self, base: u64) -> u64 {
      // context_hash is always 0 in the no-upstream mio path (Trusted state).
      base
   }

   fn lookup_encoded(&self, parsed: &ParsedQuery, fp: u64) -> Option<Arc<EncodedResult>> {
      let primary = parsed
         .tables
         .first()
         .map(String::as_str)
         .unwrap_or("default");
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

   fn lookup_json_and_promote(
      &self,
      parsed: &ParsedQuery,
      fp: u64,
   ) -> Option<Arc<crate::row::DbResultSet>> {
      self.mgr.load_stored(
         &self.config.key_prefix,
         &parsed.tables,
         &self.mgr.expanded_tables(parsed),
         fp,
      )
   }

   #[inline]
   fn queue_ready(&self, out: &mut Vec<u8>) {
      let status = if self.transaction_failed {
         b'E'
      } else if self.in_transaction {
         b'T'
      } else {
         b'I'
      };
      protocol::put_ready_for_query(out, status);
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
      let slot_idx = (sql_fp as usize) & (HOT_SLOTS_CAP - 1);

      let generation = self.mgr.generation();
      if !self.in_transaction
         && let Some(slot) = &self.hot_slots[slot_idx]
         && slot.fp == sql_fp
         && slot.generation == generation
         && let Some(entry) = &slot.encoded
      {
         let now = coarse_now_ms();
         if !entry.is_expired(now) {
            entry.write_simple_response(out);
            return true;
         }
      }

      let parsed = if let Some(slot) = &self.hot_slots[slot_idx]
         && slot.fp == sql_fp
      {
         Arc::clone(&slot.parsed)
      } else {
         self.mgr.analyze(sql)
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
            self.hot_slots[slot_idx] = Some(HotQuerySlot {
               fp: sql_fp,
               parsed: Arc::clone(&parsed),
               encoded: Some(Arc::clone(&entry)),
               generation,
            });
            entry.write_simple_response(out);
            return true;
         }
         if let Some(result) = self.lookup_json_and_promote(&parsed, fp) {
            let generation = self.mgr.generation();
            if let Some(entry) = self.lookup_encoded(&parsed, fp) {
               self.hot_slots[slot_idx] = Some(HotQuerySlot {
                  fp: sql_fp,
                  parsed: Arc::clone(&parsed),
                  encoded: Some(Arc::clone(&entry)),
                  generation,
               });
               entry.write_simple_response(out);
               return true;
            }
            let tag = format!("SELECT {}", result.rows.len());
            let _ = protocol::write_result_set(out, &result, &tag);
            return true;
         }
         self
            .mgr
            .metrics()
            .misses
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
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
            protocol::put_command_complete(out, tag);
            self.hot_slots[slot_idx] = Some(HotQuerySlot {
               fp: sql_fp,
               parsed: Arc::clone(&parsed),
               encoded: None,
               generation,
            });
         }
         _ => {
            self.queue_error(
               out,
               "08006",
               "no upstream database configured for cache miss",
            );
         }
      }
      self.queue_ready(out);
      true
   }

   fn handle_parse(&mut self, out: &mut Vec<u8>, body: &[u8]) -> bool {
      if self.cycle_error {
         return true;
      }
      let mut pos = 0usize;
      let name = take_cstr(body, &mut pos).unwrap_or("");
      let sql = take_cstr(body, &mut pos).unwrap_or("");
      let count = rd_i16(body, pos).max(0) as usize;
      pos += 2;
      let mut param_oids = Vec::with_capacity(count.min(256));
      for _ in 0..count.min(256) {
         if pos + 4 <= body.len() {
            param_oids.push(u32::from_be_bytes([
               body[pos],
               body[pos + 1],
               body[pos + 2],
               body[pos + 3],
            ]));
            pos += 4;
         }
      }

      if !name.is_empty()
         && !self.statements.contains_key(name)
         && self.statements.len() >= MAX_NAMED
      {
         self.queue_error(out, "53400", "too many prepared statements");
         self.cycle_error = true;
         return true;
      }

      let parsed = self.mgr.analyze(sql);

      let entry = StmtEntry {
         sql: sql.to_string(),
         param_oids,
         parsed,
         parse_frame: Vec::new(),
         row_description: None,
      };

      if name.is_empty() {
         self.unnamed_stmt = Some(entry);
      } else {
         self.statements.insert(name.to_string(), entry);
      }

      protocol::put_parse_complete(out);
      true
   }

   fn handle_bind(&mut self, out: &mut Vec<u8>, body: &[u8]) -> bool {
      if self.cycle_error {
         return true;
      }

      let mut pos = 0usize;
      let portal = take_cstr(body, &mut pos).unwrap_or("");
      let statement = take_cstr(body, &mut pos).unwrap_or("");

      if !portal.is_empty() && !self.portals.contains_key(portal) && self.portals.len() >= MAX_NAMED
      {
         self.queue_error(out, "53400", "too many open portals");
         self.cycle_error = true;
         return true;
      }

      // Fast path: repeated bind on the same portal with valid cached entry
      let generation = self.mgr.generation();
      if !self.in_transaction
         && let Some(entry) = self.get_portal_mut(portal)
         && entry.statement == statement
         && entry.cached_generation == generation
         && let Some(ref cached) = entry.cached_encoded
      {
         let now = coarse_now_ms();
         if !cached.is_expired(now) {
            entry.cursor = 0;
            entry.finished = false;
            protocol::put_bind_complete(out);
            return true;
         }
      }

      let Some(stmt) = self.get_statement(statement) else {
         self.queue_error(out, "26000", "prepared statement does not exist");
         self.cycle_error = true;
         return true;
      };

      let fmt_count = rd_i16(body, pos).max(0) as usize;
      pos += 2;
      let mut param_formats: [i16; 16] = [0; 16];
      let mut heap_formats;
      let p_formats: &[i16] = if fmt_count <= 16 {
         for fmt in param_formats.iter_mut().take(fmt_count) {
            if pos + 2 <= body.len() {
               *fmt = i16::from_be_bytes([body[pos], body[pos + 1]]);
               pos += 2;
            }
         }
         &param_formats[..fmt_count]
      } else {
         heap_formats = Vec::with_capacity(fmt_count.min(256));
         for _ in 0..fmt_count.min(256) {
            if pos + 2 <= body.len() {
               heap_formats.push(i16::from_be_bytes([body[pos], body[pos + 1]]));
               pos += 2;
            }
         }
         &heap_formats
      };

      let param_count = rd_i16(body, pos).max(0) as usize;
      pos += 2;
      let mut params_stack: [Option<&[u8]>; 16] = [None; 16];
      let mut heap_params;
      let params_slice: &[Option<&[u8]>] = if param_count <= 16 {
         for param in params_stack.iter_mut().take(param_count) {
            if pos + 4 > body.len() {
               break;
            }
            let len = i32::from_be_bytes([body[pos], body[pos + 1], body[pos + 2], body[pos + 3]]);
            pos += 4;
            if len >= 0 {
               let l = len as usize;
               if pos + l <= body.len() {
                  *param = Some(&body[pos..pos + l]);
                  pos += l;
               }
            }
         }
         &params_stack[..param_count]
      } else {
         heap_params = Vec::with_capacity(param_count.min(256));
         for _ in 0..param_count.min(256) {
            if pos + 4 > body.len() {
               break;
            }
            let len = i32::from_be_bytes([body[pos], body[pos + 1], body[pos + 2], body[pos + 3]]);
            pos += 4;
            if len < 0 {
               heap_params.push(None);
            } else {
               let l = len as usize;
               if pos + l <= body.len() {
                  heap_params.push(Some(&body[pos..pos + l]));
                  pos += l;
               } else {
                  heap_params.push(None);
               }
            }
         }
         &heap_params
      };

      let res_count = rd_i16(body, pos).max(0) as usize;
      pos += 2;
      let mut text_results = true;
      for _ in 0..res_count.min(16) {
         if pos + 2 <= body.len() {
            if i16::from_be_bytes([body[pos], body[pos + 1]]) != 0 {
               text_results = false;
            }
            pos += 2;
         }
      }

      let (parsed, base_fp) = if params_slice.is_empty() {
         (Arc::clone(&stmt.parsed), stmt.parsed.fingerprint)
      } else {
         let fp = crate::query::compute_fingerprint_with_params_slice(
            stmt.parsed.fingerprint,
            params_slice,
            p_formats,
         );
         (Arc::clone(&stmt.parsed), fp)
      };

      let cacheable = parsed.is_cacheable
         && parsed.kind == QueryKind::Select
         && !self.in_transaction
         && text_results
         && self.mgr.is_cacheable(&parsed);
      let cached_encoded = if cacheable {
         self.lookup_encoded(&parsed, base_fp)
      } else {
         None
      };

      if portal.is_empty() {
         if let Some(entry) = &mut self.unnamed_portal {
            entry.statement.clear();
            entry.statement.push_str(statement);
            entry.parsed = parsed;
            entry.fingerprint = base_fp;
            entry.text_results = text_results;
            entry.cursor = 0;
            entry.finished = false;
            entry.cached_encoded = cached_encoded;
            entry.cached_generation = generation;
         } else {
            self.unnamed_portal = Some(PortalEntry {
               statement: statement.to_string(),
               parsed,
               fingerprint: base_fp,
               text_results,
               bind_frame: Vec::new(),
               cursor: 0,
               finished: false,
               cached_encoded,
               cached_generation: generation,
            });
         }
      } else if let Some(entry) = self.portals.get_mut(portal) {
         entry.statement.clear();
         entry.statement.push_str(statement);
         entry.parsed = parsed;
         entry.fingerprint = base_fp;
         entry.text_results = text_results;
         entry.cursor = 0;
         entry.finished = false;
         entry.cached_encoded = cached_encoded;
         entry.cached_generation = generation;
      } else {
         self.portals.insert(
            portal.to_string(),
            PortalEntry {
               statement: statement.to_string(),
               parsed,
               fingerprint: base_fp,
               text_results,
               bind_frame: Vec::new(),
               cursor: 0,
               finished: false,
               cached_encoded,
               cached_generation: generation,
            },
         );
      }

      protocol::put_bind_complete(out);
      true
   }

   fn handle_execute(&mut self, out: &mut Vec<u8>, body: &[u8]) -> bool {
      if self.cycle_error {
         return true;
      }

      let mut pos = 0usize;
      let portal_name = take_cstr(body, &mut pos).unwrap_or("");
      let max_rows = if pos + 4 <= body.len() {
         i32::from_be_bytes([body[pos], body[pos + 1], body[pos + 2], body[pos + 3]])
      } else {
         0
      };

      let generation = self.mgr.generation();
      let Some(portal) = self.get_portal_mut(portal_name) else {
         self.queue_error(out, "34000", "portal does not exist");
         self.cycle_error = true;
         return true;
      };

      if portal.cached_generation == generation
         && let Some(entry) = &portal.cached_encoded
      {
         let now = coarse_now_ms();
         if !entry.is_expired(now) {
            if max_rows <= 0 && portal.cursor == 0 {
               entry.write_execute_response(out);
               portal.cursor = entry.data_rows().len();
               portal.finished = true;
               return true;
            }
            let entry_clone = Arc::clone(entry);
            serve_encoded_from_portal(out, portal, &entry_clone, max_rows);
            return true;
         }
      }

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
            let portal_mut = self.get_portal_mut(portal_name).unwrap();
            portal_mut.cached_encoded = Some(Arc::clone(&entry));
            portal_mut.cached_generation = generation;
            serve_encoded_from_portal(out, portal_mut, &entry, max_rows);
            return true;
         }
         if let Some(result) = self.lookup_json_and_promote(&parsed, fp) {
            let generation = self.mgr.generation();
            if let Some(entry) = self.lookup_encoded(&parsed, fp) {
               let portal_mut = self.get_portal_mut(portal_name).unwrap();
               portal_mut.cached_encoded = Some(Arc::clone(&entry));
               portal_mut.cached_generation = generation;
               serve_encoded_from_portal(out, portal_mut, &entry, max_rows);
               return true;
            }
            let tag = format!("SELECT {}", result.rows.len());
            let _ = protocol::write_result_set(out, &result, &tag);
            if let Some(p) = self.get_portal_mut(portal_name) {
               p.finished = true;
            }
            return true;
         }
         self
            .mgr
            .metrics()
            .misses
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
      }

      // Local fallback for non-select queries via extended protocol
      match parsed.kind {
         QueryKind::Begin => {
            self.in_transaction = true;
            self.transaction_writes.clear();
            protocol::put_command_complete(out, "BEGIN");
            if let Some(p) = self.get_portal_mut(portal_name) {
               p.finished = true;
            }
         }
         QueryKind::Commit => {
            self.in_transaction = false;
            self.transaction_failed = false;
            self.transaction_writes.clear();
            protocol::put_command_complete(out, "COMMIT");
            if let Some(p) = self.get_portal_mut(portal_name) {
               p.finished = true;
            }
         }
         QueryKind::Rollback => {
            self.in_transaction = false;
            self.transaction_failed = false;
            self.transaction_writes.clear();
            protocol::put_command_complete(out, "ROLLBACK");
            if let Some(p) = self.get_portal_mut(portal_name) {
               p.finished = true;
            }
         }
         _ => {
            self.queue_error(
               out,
               "08006",
               "no upstream database configured for cache miss",
            );
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
      let name = take_cstr(body, &mut pos).unwrap_or("");

      // Describe used to answer NoData for every SELECT and then send rows on
      // Execute, which clients reject as a protocol violation. Columns come
      // from the cached result now; with no cached copy there is nothing
      // truthful to say without an upstream, so it is an error, as in the
      // blocking session.
      match kind {
         b'S' => {
            let Some(stmt) = self.get_statement(name) else {
               self.queue_error(out, "26000", "prepared statement does not exist");
               self.cycle_error = true;
               return;
            };
            let row_description = if stmt.parsed.kind != QueryKind::Select {
               Some(None)
            } else if let Some(known) = &stmt.row_description {
               Some(Some(known.clone()))
            } else if stmt.param_oids.is_empty() && !stmt.sql.contains('$') {
               let parsed = Arc::clone(&stmt.parsed);
               self
                  .lookup_encoded(&parsed, self.effective_fp(parsed.fingerprint))
                  .map(|entry| Some(entry.row_description().to_vec()))
            } else {
               None
            };
            let Some(row_description) = row_description else {
               self.queue_error(out, "08006", "no upstream database configured");
               self.cycle_error = true;
               return;
            };
            let oids = self
               .get_statement(name)
               .map(|stmt| stmt.param_oids.clone())
               .unwrap_or_default();
            protocol::put_parameter_description(out, &oids);
            match row_description {
               Some(row_description) => out.extend_from_slice(&row_description),
               None => protocol::put_no_data(out),
            }
         }
         b'P' => {
            let generation = self.mgr.generation();
            let Some(portal) = self.get_portal_mut(name) else {
               self.queue_error(out, "34000", "portal does not exist");
               self.cycle_error = true;
               return;
            };
            if portal.parsed.kind != QueryKind::Select {
               protocol::put_no_data(out);
               return;
            }
            let now = coarse_now_ms();
            let cached = portal
               .cached_encoded
               .as_ref()
               .filter(|entry| portal.cached_generation == generation && !entry.is_expired(now))
               .cloned();
            let (parsed, fingerprint, text_results) = (
               Arc::clone(&portal.parsed),
               portal.fingerprint,
               portal.text_results,
            );
            let entry = cached.or_else(|| {
               (text_results && self.mgr.is_cacheable(&parsed))
                  .then(|| self.lookup_encoded(&parsed, self.effective_fp(fingerprint)))
                  .flatten()
            });
            match entry {
               Some(entry) => out.extend_from_slice(entry.row_description()),
               None => {
                  self.queue_error(out, "08006", "no upstream database configured");
                  self.cycle_error = true;
               }
            }
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
      let name = take_cstr(body, &mut pos).unwrap_or("");
      match kind {
         b'S' => {
            if name.is_empty() {
               self.unnamed_stmt = None;
               if let Some(p) = &self.unnamed_portal
                  && p.statement.is_empty()
               {
                  self.unnamed_portal = None;
               }
            } else {
               self.statements.remove(name);
               self.portals.retain(|_, p| p.statement != name);
               if let Some(p) = &self.unnamed_portal
                  && p.statement == name
               {
                  self.unnamed_portal = None;
               }
            }
         }
         b'P' => {
            if name.is_empty() {
               self.unnamed_portal = None;
            } else {
               self.portals.remove(name);
            }
         }
         _ => {}
      }
      protocol::put_close_complete(out);
   }
}

fn serve_encoded_from_portal(
   out: &mut Vec<u8>,
   portal: &mut PortalEntry,
   entry: &EncodedResult,
   max_rows: i32,
) {
   let limit = if max_rows > 0 {
      max_rows as usize
   } else {
      usize::MAX
   };
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
      let Some(next) = next_data_frame_end(data, offset) else {
         break;
      };
      out.extend_from_slice(&data[offset..next]);
      offset = next;
      sent += 1;
   }
   *cursor = offset;
}

fn next_data_frame_end(data: &[u8], offset: usize) -> Option<usize> {
   if offset + 5 > data.len() {
      return None;
   }
   let len = i32::from_be_bytes([
      data[offset + 1],
      data[offset + 2],
      data[offset + 3],
      data[offset + 4],
   ]);
   if len < 4 {
      return None;
   }
   let total = 1 + len as usize;
   if offset + total > data.len() {
      return None;
   }
   Some(offset + total)
}

#[inline]
fn take_cstr<'a>(buf: &'a [u8], pos: &mut usize) -> Option<&'a str> {
   let start = *pos;
   if start >= buf.len() {
      return None;
   }
   if buf[start] == 0 {
      *pos = start + 1;
      return Some("");
   }
   let rel = buf[start..].iter().position(|&b| b == 0)?;
   let end = start + rel;
   *pos = end + 1;
   std::str::from_utf8(&buf[start..end]).ok()
}

fn rd_i16(buf: &[u8], pos: usize) -> i16 {
   if pos + 2 <= buf.len() {
      i16::from_be_bytes([buf[pos], buf[pos + 1]])
   } else {
      0
   }
}

// ────────────────────────────────────── connection state

enum ConnState {
   Startup,
   Active(Box<ActiveState>),
}

struct ProxyConn {
   stream: TcpStream,
   state: ConnState,
   rbuf: ReadBuf,
   wbuf: WriteBuf,
   wants_write: bool,
   permit: ConnectionPermit,
}

impl ProxyConn {
   fn new(stream: TcpStream, permit: ConnectionPermit) -> Self {
      Self {
         stream,
         state: ConnState::Startup,
         rbuf: ReadBuf::new(),
         wbuf: WriteBuf::new(),
         wants_write: false,
         permit,
      }
   }
}

/// What the event loop does with a connection after handling an event.
enum Next {
   Keep,
   Close,
   /// Hand the connection to a blocking upstream session; carries the
   /// startup message already read off the socket.
   Offload(Vec<u8>),
}

/// Shared by every worker of one listener: the proxy-wide connection count.
pub(super) struct WorkerShared {
   pub(super) mgr: Arc<DbCacheManager>,
   pub(super) config: Arc<DbCacheConfig>,
   pub(super) pool: Arc<UpstreamPool>,
   pub(super) bus: Option<Arc<InvalidationBus>>,
   pub(super) active: Arc<AtomicUsize>,
}

// ────────────────────────────────────── worker entry point

pub(super) fn run_proxy_worker(mut listener: TcpListener, shared: WorkerShared) {
   let mut poll = Poll::new().unwrap();
   let mut events = Events::with_capacity(512);

   poll
      .registry()
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
         if event.token() == LISTENER_TK {
            do_accept(
               &mut listener,
               poll.registry(),
               &mut conns,
               &mut free,
               &mut next_id,
               &shared,
            );
            continue;
         }
         let id = event.token().0.wrapping_sub(CONN_BASE);
         match dispatch_event(&mut conns, id, event, &shared) {
            Next::Close => free_slot(&mut conns, poll.registry(), &mut free, id),
            Next::Offload(startup) => {
               offload_conn(&mut conns, poll.registry(), &mut free, id, startup, &shared)
            }
            Next::Keep => {
               if let Some(Some(conn)) = conns.get(id)
                  && !conn.wbuf.is_empty()
               {
                  write_pending.push(id);
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
   conns: &mut [Option<ProxyConn>],
   id: usize,
   event: &mio::event::Event,
   shared: &WorkerShared,
) -> Next {
   let Some(Some(conn)) = conns.get_mut(id) else {
      return Next::Keep;
   };

   if event.is_readable() {
      if let Err(e) = conn.rbuf.fill(&mut conn.stream)
         && e.kind() != io::ErrorKind::WouldBlock
      {
         return Next::Close;
      }
      let next = match conn.state {
         ConnState::Startup => handle_startup_event(conn, shared),
         ConnState::Active(_) => {
            if handle_active_event(conn) {
               Next::Keep
            } else {
               Next::Close
            }
         }
      };
      if !matches!(next, Next::Keep) {
         return next;
      }
   }

   if event.is_writable() && conn.wbuf.flush_to(&mut conn.stream).is_err() {
      return Next::Close;
   }
   Next::Keep
}

fn handle_startup_event(conn: &mut ProxyConn, shared: &WorkerShared) -> Next {
   loop {
      let Some(frame) = conn.rbuf.take_startup_frame() else {
         return Next::Keep; // need more data
      };
      if frame.len() < 8 {
         return Next::Close;
      }
      let code = i32::from_be_bytes([frame[4], frame[5], frame[6], frame[7]]);

      if code == protocol::SSL_REQUEST_CODE || code == protocol::GSSENC_REQUEST_CODE {
         conn.wbuf.extend(b"N");
         let _ = conn.wbuf.flush_to(&mut conn.stream);
         continue;
      }
      if code == protocol::CANCEL_REQUEST_CODE {
         return Next::Close;
      }
      if (code >> 16) != 3 {
         return Next::Close;
      }

      // Real Startup message. Upstream sessions (auth relay, misses) are
      // blocking, so they move to their own thread.
      if shared.config.upstream_addr.is_some()
         && shared.config.mode == crate::config::DbProxyMode::Dual
      {
         return Next::Offload(frame);
      }

      // No upstream: send AuthOk + params + ReadyForQuery inline.
      if protocol::write_startup_ok(&mut conn.wbuf.buf).is_err() {
         return Next::Close;
      }
      conn.state = ConnState::Active(Box::new(ActiveState::new(
         Arc::clone(&shared.mgr),
         Arc::clone(&shared.config),
      )));
      let _ = conn.wbuf.flush_to(&mut conn.stream);
      return Next::Keep;
   }
}

/// Moves a connection out of the event loop into a blocking session thread.
///
/// The previous version `dup`ed the fd and left the original registered:
/// O_NONBLOCK is shared by both descriptors, so switching the copy to
/// blocking also made the event loop's reads block, and both sides then
/// raced for the client's bytes. It also dropped the startup message it had
/// already consumed, and gave up silently when its fixed pool was full. Now
/// the socket is deregistered and owned by the session alone, and every byte
/// already read is replayed into it.
#[cfg(unix)]
fn offload_conn(
   conns: &mut [Option<ProxyConn>],
   registry: &mio::Registry,
   free: &mut Vec<usize>,
   id: usize,
   startup: Vec<u8>,
   shared: &WorkerShared,
) {
   let Some(mut conn) = conns.get_mut(id).and_then(Option::take) else {
      return;
   };
   let _ = registry.deregister(&mut conn.stream);
   free.push(id);

   let mut prefix = startup;
   prefix.extend_from_slice(conn.rbuf.available());
   let unsent = conn.wbuf.buf[conn.wbuf.pos.min(conn.wbuf.buf.len())..].to_vec();
   let permit = conn.permit;
   let mut stream = into_std(conn.stream);
   if stream.set_nonblocking(false).is_err() || stream.write_all(&unsent).is_err() {
      return;
   }

   let config = (*shared.config).clone();
   let mgr = Arc::clone(&shared.mgr);
   let pool = Arc::clone(&shared.pool);
   let bus = shared.bus.clone();
   let spawned =
      super::threads::spawn_session("fyrodb-dbconn", super::session::CLIENT_STACK, move || {
         let _permit = permit;
         if let Err(e) =
            super::session::handle_client_with_prefix(stream, prefix, config, mgr, pool, bus)
            && e.kind() != io::ErrorKind::UnexpectedEof
         {
            eprintln!("fyrodb-dbproxy client error: {e}");
         }
      });
   if let Err(e) = spawned {
      eprintln!("fyrodb-dbproxy: spawn failed: {e}");
   }
}

#[cfg(not(unix))]
fn offload_conn(
   conns: &mut [Option<ProxyConn>],
   registry: &mio::Registry,
   free: &mut Vec<usize>,
   id: usize,
   _startup: Vec<u8>,
   _shared: &WorkerShared,
) {
   free_slot(conns, registry, free, id);
}

fn handle_active_event(conn: &mut ProxyConn) -> bool {
   let active = match &mut conn.state {
      ConnState::Active(a) => a.as_mut(),
      _ => return false,
   };
   let rbuf = &mut conn.rbuf;
   let wbuf = &mut conn.wbuf;

   let mut pos = rbuf.consumed;
   let len = rbuf.buf.len();
   let mut flush_now = false;
   let mut should_terminate = false;

   while pos + 5 <= len {
      let msg_type = rbuf.buf[pos];
      let msg_len = i32::from_be_bytes([
         rbuf.buf[pos + 1],
         rbuf.buf[pos + 2],
         rbuf.buf[pos + 3],
         rbuf.buf[pos + 4],
      ]);
      if msg_len < 4 || msg_len as usize > (64 << 20) {
         return false;
      }
      let body_len = msg_len as usize - 4;
      let total = 5 + body_len;
      if pos + total > len {
         break;
      }
      let body = &rbuf.buf[pos + 5..pos + total];
      pos += total;

      match msg_type {
         b'Q' => {
            active.handle_query(&mut wbuf.buf, body);
            flush_now = true; // simple query always flushes
         }
         b'P' => {
            active.handle_parse(&mut wbuf.buf, body);
         }
         b'B' => {
            active.handle_bind(&mut wbuf.buf, body);
         }
         b'E' => {
            active.handle_execute(&mut wbuf.buf, body);
         }
         b'S' => {
            active.handle_sync(&mut wbuf.buf);
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
            active.handle_describe(&mut wbuf.buf, body);
         }
         b'C' => {
            active.handle_close(&mut wbuf.buf, body);
         }
         _ => {}
      }
   }

   rbuf.consumed = pos;
   let rbuf_drained = rbuf.consumed == rbuf.buf.len();
   if rbuf_drained {
      rbuf.buf.clear();
      rbuf.consumed = 0;
   } else if rbuf.consumed > 16384 && rbuf.consumed >= rbuf.buf.len() / 2 {
      rbuf.buf.drain(..rbuf.consumed);
      rbuf.consumed = 0;
   }

   let do_flush =
      should_terminate || flush_now || rbuf_drained || wbuf.pending_len() >= FLUSH_THRESH;
   if do_flush {
      let _ = wbuf.flush_to(&mut conn.stream);
   }

   !should_terminate
}

fn do_accept(
   listener: &mut TcpListener,
   registry: &mio::Registry,
   conns: &mut Vec<Option<ProxyConn>>,
   free: &mut Vec<usize>,
   next_id: &mut usize,
   shared: &WorkerShared,
) {
   loop {
      match listener.accept() {
         Ok((mut stream, _)) => {
            let Some(permit) = try_admit(&shared.active, shared.config.max_connections) else {
               reject_too_many(into_std(stream));
               continue;
            };
            let _ = socket2::SockRef::from(&stream).set_tcp_nodelay(true);
            let _ = socket2::SockRef::from(&stream).set_send_buffer_size(512 * 1024);
            let _ = socket2::SockRef::from(&stream).set_recv_buffer_size(512 * 1024);
            #[cfg(target_os = "linux")]
            let _ = socket2::SockRef::from(&stream).set_tcp_quickack(true);
            let id = alloc_slot(free, next_id, conns);
            if registry
               .register(&mut stream, Token(id + CONN_BASE), Interest::READABLE)
               .is_ok()
            {
               conns[id] = Some(ProxyConn::new(stream, permit));
            } else {
               free.push(id);
            }
         }
         Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => break,
         Err(_) => break,
      }
   }
}

/// Takes a socket out of mio's ownership (it must not be registered).
#[cfg(unix)]
fn into_std(stream: TcpStream) -> std::net::TcpStream {
   use std::os::unix::io::{FromRawFd, IntoRawFd};
   // SAFETY: `into_raw_fd` released mio's ownership; this is the only owner.
   unsafe { std::net::TcpStream::from_raw_fd(stream.into_raw_fd()) }
}

#[cfg(windows)]
fn into_std(stream: TcpStream) -> std::net::TcpStream {
   use std::os::windows::io::{FromRawSocket, IntoRawSocket};
   // SAFETY: `into_raw_socket` released mio's ownership; this is the only owner.
   unsafe { std::net::TcpStream::from_raw_socket(stream.into_raw_socket()) }
}

fn sync_write_interest(registry: &mio::Registry, conns: &mut [Option<ProxyConn>], id: usize) {
   let Some(Some(conn)) = conns.get_mut(id) else {
      return;
   };
   let wants = !conn.wbuf.is_empty();
   if wants == conn.wants_write {
      return;
   }
   let interest = if wants {
      Interest::READABLE | Interest::WRITABLE
   } else {
      Interest::READABLE
   };
   if registry
      .reregister(&mut conn.stream, Token(id + CONN_BASE), interest)
      .is_ok()
   {
      conn.wants_write = wants;
   }
}

fn free_slot(
   conns: &mut [Option<ProxyConn>],
   registry: &mio::Registry,
   free: &mut Vec<usize>,
   id: usize,
) {
   if let Some(slot) = conns.get_mut(id)
      && let Some(mut conn) = slot.take()
   {
      let _ = registry.deregister(&mut conn.stream);
      free.push(id);
   }
}

fn alloc_slot(
   free: &mut Vec<usize>,
   next: &mut usize,
   conns: &mut Vec<Option<ProxyConn>>,
) -> usize {
   if let Some(id) = free.pop() {
      return id;
   }
   let id = *next;
   *next += 1;
   if id >= conns.len() {
      conns.resize_with(id + 1, || None);
   }
   id
}
