use crate::auth::{self, ClientAuthError};
use crate::bus::InvalidationBus;
use crate::cache::{DbCacheManager, EncodedResult, mix64};
use crate::config::{DbCacheConfig, DbProxyMode};
use crate::pool::{PoolKey, UpstreamPool};
use crate::protocol::{self, FrameReader, FrontendMessage};
use crate::query::{ParsedQuery, QueryKind, SessionChange, compute_fingerprint};
use crate::row::DbValue;
use crate::upstream::{HandshakeError, RelayOutcome, ReplyStop, UpstreamConnection};
use foldhash::fast::FixedState;
use std::collections::HashMap;
use std::io::{self, BufReader, Write};
use std::net::TcpStream;
use std::sync::Arc;

pub(super) const CLIENT_STACK: usize = 512 * 1024;
const CLIENT_READ_BUF: usize = 32 * 1024;
const RELAY_FLUSH_BYTES: usize = 32 * 1024;
const PORTAL_SUSPENDED: [u8; 5] = [b's', 0, 0, 0, 4];
const MAX_CONTEXT_SETS: u32 = 64;
const MAX_NAMED_STATEMENTS: usize = 512;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ContextState {
   Trusted,
   NeedsProbe,
   Untrusted,
}

struct ClientSink<'a> {
   out: &'a mut Vec<u8>,
   client: &'a mut TcpStream,
}

impl Write for ClientSink<'_> {
   fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
      self.out.extend_from_slice(buf);
      if self.out.len() >= RELAY_FLUSH_BYTES {
         self.client.write_all(self.out)?;
         self.out.clear();
      }
      Ok(buf.len())
   }

   fn flush(&mut self) -> io::Result<()> {
      self.client.write_all(self.out)?;
      self.out.clear();
      self.client.flush()
   }
}

pub(super) fn handle_client(
   client: TcpStream,
   config: DbCacheConfig,
   cache_mgr: Arc<DbCacheManager>,
   pool: Arc<UpstreamPool>,
   bus: Option<Arc<InvalidationBus>>,
) -> io::Result<()> {
   client.set_nodelay(true)?;
   let read_stream = client.try_clone()?;
   let mut session = Session {
      client,
      reader: FrameReader::new(BufReader::with_capacity(CLIENT_READ_BUF, read_stream)),
      config,
      mgr: cache_mgr,
      pool,
      bus,
      upstream: None,
      pool_key: None,
      upstream_poolable: false,
      client_authenticated: false,
      in_transaction: false,
      transaction_failed: false,
      transaction_writes: Vec::new(),
      cycle_forwarded: false,
      cycle_error: false,
      statements: HashMap::with_hasher(FixedState::with_seed(0)),
      portals: HashMap::with_hasher(FixedState::with_seed(0)),
      out: Vec::with_capacity(16 * 1024),
      context_hash: 0,
      context_state: ContextState::Untrusted,
      context_sets: 0,
   };
   session.run()
}

struct StatementState {
   sql: String,
   param_oids: Vec<u32>,
   parse_frame: Vec<u8>,
   row_description: Option<Vec<u8>>,
   forwarded: bool,
}

struct PortalState {
   statement: String,
   parsed: Arc<ParsedQuery>,
   fingerprint: u64,
   text_results: bool,
   bind_frame: Vec<u8>,
   bind_forwarded: bool,
   cursor: usize,
   finished: bool,
   row_description: Option<Vec<u8>>,
}

struct Session {
   client: TcpStream,
   reader: FrameReader<BufReader<TcpStream>>,
   config: DbCacheConfig,
   mgr: Arc<DbCacheManager>,
   pool: Arc<UpstreamPool>,
   bus: Option<Arc<InvalidationBus>>,
   upstream: Option<UpstreamConnection>,
   pool_key: Option<PoolKey>,
   upstream_poolable: bool,
   client_authenticated: bool,
   in_transaction: bool,
   transaction_failed: bool,
   transaction_writes: Vec<String>,
   cycle_forwarded: bool,
   cycle_error: bool,
   statements: HashMap<String, StatementState, FixedState>,
   portals: HashMap<String, PortalState, FixedState>,
   out: Vec<u8>,
   context_hash: u64,
   context_state: ContextState,
   context_sets: u32,
}

impl Session {
   fn run(&mut self) -> io::Result<()> {
      let result = self.run_inner();
      self.reclaim_upstream();
      result
   }

   fn run_inner(&mut self) -> io::Result<()> {
      if !self.startup()? {
         return Ok(());
      }

      loop {
         let msg = match self.reader.read_frontend(false) {
            Ok(msg) => msg,
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => return Ok(()),
            Err(e) if e.kind() == io::ErrorKind::ConnectionReset => return Ok(()),
            Err(e) => return Err(e),
         };

         self.maintain_idle();

         match msg {
            FrontendMessage::Query(sql) => self.handle_query(&sql)?,
            FrontendMessage::Parse {
               name,
               sql,
               param_oids,
            } => self.handle_parse(name, sql, param_oids)?,
            FrontendMessage::Bind {
               portal,
               statement,
               param_formats,
               params,
               result_formats,
            } => self.handle_bind(portal, statement, param_formats, params, result_formats)?,
            FrontendMessage::Describe { kind, name } => self.handle_describe(kind, &name)?,
            FrontendMessage::Execute { portal, max_rows } => {
               self.handle_execute(&portal, max_rows)?
            }
            FrontendMessage::Sync => self.handle_sync()?,
            FrontendMessage::Flush => self.flush_out()?,
            FrontendMessage::Close { kind, name } => self.handle_close(kind, &name)?,
            FrontendMessage::Terminate => {
               self.flush_out()?;
               return Ok(());
            }
            FrontendMessage::Unknown(_) => {
               let frame = self.reader.last_frame().to_vec();
               if self.upstream.is_some() {
                  self.flush_out()?;
                  if let Some(upstream) = self.upstream.as_mut() {
                     upstream.send_raw(&frame)?;
                  }
               }
            }
            FrontendMessage::SslRequest | FrontendMessage::GssEncRequest => {
               self.client.write_all(b"N")?;
            }
            FrontendMessage::CancelRequest { .. } | FrontendMessage::Startup { .. } => {
               self.queue_error("08P01", "unexpected startup message");
               self.flush_out()?;
               return Ok(());
            }
         }
      }
   }

   // ------------------------------------------------------------- startup

   fn startup(&mut self) -> io::Result<bool> {
      loop {
         let msg = match self.reader.read_frontend(true) {
            Ok(msg) => msg,
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => return Ok(false),
            Err(e) if e.kind() == io::ErrorKind::ConnectionReset => return Ok(false),
            Err(e) => return Err(e),
         };

         match msg {
            FrontendMessage::SslRequest | FrontendMessage::GssEncRequest => {
               self.client.write_all(b"N")?;
               self.client.flush()?;
            }
            FrontendMessage::CancelRequest { .. } => {
               if let Some(target) = self.upstream_target() {
                  crate::upstream::send_cancel(target, self.reader.last_frame());
               }
               return Ok(false);
            }
            FrontendMessage::Startup {
               ref user,
               ref database,
               ref params,
            } => {
               if let Some((_, value)) = params
                  .iter()
                  .find(|(key, _)| key.eq_ignore_ascii_case("replication"))
                  && !matches!(value.as_str(), "false" | "0" | "off")
               {
                  protocol::write_fatal_response(
                     &mut self.client,
                     "0A000",
                     "replication connections are not supported through the dbcache proxy",
                  )?;
                  return Ok(false);
               }

               match self.upstream_target().map(str::to_string) {
                  Some(target) => {
                     let parsed_target = crate::config::UpstreamTarget::parse(&target);
                     let effective_db = if let Some(ref target_db) = parsed_target.database {
                        if database.is_empty() || database == user {
                           target_db.clone()
                        } else {
                           database.clone()
                        }
                     } else {
                        database.clone()
                     };
                     let startup_frame = if effective_db != *database {
                        let mut updated = params.clone();
                        if let Some(db_entry) = updated
                           .iter_mut()
                           .find(|(k, _)| k.eq_ignore_ascii_case("database"))
                        {
                           db_entry.1 = effective_db.clone();
                        } else {
                           updated.push(("database".to_string(), effective_db.clone()));
                        }
                        protocol::encode_startup(&updated)
                     } else {
                        self.reader.last_frame().to_vec()
                     };
                     let pool_key = PoolKey::from_startup(params, user, &effective_db);
                     let pool_eligible = crate::pool::startup_allows_pooling(params);
                     let application_name = crate::pool::application_name(params);
                     let password = self
                        .config
                        .auth_users
                        .password(user)
                        .map(str::to_string)
                        .or_else(|| parsed_target.password.clone());
                     self.client_authenticated = password.is_some();
                     if self.config.trust_auth && self.client_authenticated {
                        self.client.write_all(&[b'R', 0, 0, 0, 8, 0, 0, 0, 0])?;
                        self.client.flush()?;
                     } else if let Some(password) = password.as_deref() {
                        match auth::authenticate_client(
                           &mut self.reader,
                           &mut self.client,
                           password,
                        ) {
                           Ok(()) => {}
                           Err(ClientAuthError::Rejected) => {
                              protocol::write_fatal_response(
                                 &mut self.client,
                                 "28P01",
                                 &format!("password authentication failed for user \"{user}\""),
                              )?;
                              return Ok(false);
                           }
                           Err(ClientAuthError::Io(e)) => return Err(e),
                        }
                     }

                     let usable = if pool_eligible {
                        self
                           .pool
                           .checkout(&pool_key, !self.client_authenticated, application_name)
                     } else {
                        None
                     };
                     self.mgr.metrics().pool_idle.store(
                        self.pool.idle_count() as u64,
                        std::sync::atomic::Ordering::Relaxed,
                     );
                     if let Some(upstream) = usable {
                        upstream.write_replay(&mut self.client, !self.client_authenticated)?;
                        self.upstream = Some(upstream);
                        self.upstream_poolable = true;
                     } else if self.client_authenticated {
                        let password = password.as_deref().unwrap_or_default();
                        match UpstreamConnection::connect_authenticated(
                           &target,
                           &startup_frame,
                           user,
                           password,
                        ) {
                           Ok(upstream) => {
                              upstream.write_replay(&mut self.client, false)?;
                              self.upstream = Some(upstream);
                              self.upstream_poolable = true;
                           }
                           Err(error) => {
                              protocol::write_fatal_response(
                                 &mut self.client,
                                 "08006",
                                 &format!("could not authenticate to upstream database: {error}"),
                              )?;
                              return Ok(false);
                           }
                        }
                     } else {
                        match UpstreamConnection::handshake(
                           &target,
                           &mut self.client,
                           &mut self.reader,
                           &startup_frame,
                        ) {
                           Ok(upstream) => {
                              self.upstream_poolable =
                                 pool_eligible && !upstream.required_password();
                              self.upstream = Some(upstream);
                           }
                           Err(HandshakeError::Rejected) => return Ok(false),
                           Err(HandshakeError::Io(e)) => {
                              protocol::write_fatal_response(
                                 &mut self.client,
                                 "08006",
                                 &format!("could not connect to upstream database: {e}"),
                              )?;
                              return Ok(false);
                           }
                        }
                     }
                     if pool_eligible && self.upstream_poolable {
                        self.pool_key = Some(pool_key);
                     } else {
                        self.upstream_poolable = false;
                     }
                     self.context_state = ContextState::NeedsProbe;
                     let _ = self.probe_context();
                     let _ = self.refresh_catalog();
                  }
                  None => {
                     protocol::write_startup_ok(&mut self.client)?;
                     self.context_hash = 0;
                     self.context_state = ContextState::Trusted;
                  }
               }
               return Ok(true);
            }
            FrontendMessage::Terminate => return Ok(false),
            _ => {
               protocol::write_fatal_response(
                  &mut self.client,
                  "08P01",
                  "expected startup message",
               )?;
               return Ok(false);
            }
         }
      }
   }

   // --------------------------------------------------------- simple query

   fn handle_query(&mut self, sql: &str) -> io::Result<()> {
      let trimmed = sql.trim();
      if trimmed.is_empty() {
         protocol::put_empty_query(&mut self.out);
         self.queue_ready();
         return self.flush_out();
      }

      let parsed = self.mgr.analyze(trimmed);
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

      let effective = self.effective_fingerprint(parsed.fingerprint);
      let cacheable = parsed.is_cacheable
         && parsed.kind == QueryKind::Select
         && !self.in_transaction
         && effective.is_some()
         && self.mgr.is_cacheable(&parsed)
         && self.catalog_allows(&parsed);
      let fingerprint = effective.unwrap_or(parsed.fingerprint);

      if cacheable {
         if let Some(entry) = self.lookup_encoded(&parsed, fingerprint) {
            entry.write_simple_response(&mut self.out);
            return self.flush_out();
         }
         if let Some(result) = self.lookup_json(&parsed, fingerprint) {
            if let Some(entry) = self.lookup_encoded(&parsed, fingerprint) {
               entry.write_simple_response(&mut self.out);
               return self.flush_out();
            }
            let tag = format!("SELECT {}", result.rows.len());
            protocol::write_result_set(&mut self.out, &result, &tag)?;
            return self.flush_out();
         }
         self
            .mgr
            .metrics()
            .misses
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
      }

      if self.upstream.is_none() {
         return self.local_query_fallback(&parsed);
      }

      let mut fill_guard = None;
      if cacheable && let Some(key) = self.fill_key(&parsed, fingerprint) {
         match self.mgr.begin_fill(&key) {
            Some(guard) => fill_guard = Some(guard),
            None => {
               self.mgr.wait_for_fill(&key);
               if let Some(entry) = self.lookup_encoded(&parsed, fingerprint) {
                  entry.write_simple_response(&mut self.out);
                  return self.flush_out();
               }
               fill_guard = self.mgr.begin_fill(&key);
            }
         }
      }
      let _fill_guard = fill_guard;

      self.flush_out()?;
      let tables = cacheable.then(|| self.mgr.expanded_tables(&parsed));
      let versions = tables
         .as_ref()
         .map(|tables| self.mgr.table_versions(tables));
      let buffer_tail = cacheable || parsed.writes || !self.transaction_writes.is_empty();
      let outcome = {
         let Some(upstream) = self.upstream.as_mut() else {
            unreachable!("upstream checked above");
         };
         if buffer_tail {
            let mut sink = ClientSink {
               out: &mut self.out,
               client: &mut self.client,
            };
            upstream.forward_query(trimmed, &mut sink, cacheable)?
         } else {
            upstream.forward_query(trimmed, &mut self.client, false)?
         }
      };
      self.update_status(&outcome);
      if !outcome.error {
         if parsed.schema_change {
            self.flush_catalog_entries();
         }
         self.apply_session_changes(&parsed);
      }
      if !outcome.error && parsed.writes {
         self.invalidate_for(&parsed);
      }
      if parsed.kind == QueryKind::Commit {
         self.invalidate_committed();
      }

      if outcome.copy_in {
         self.flush_out()?;
         let result = self.pump_copy_in(ReplyStop::Sync);
         if !self.cycle_error && parsed.writes {
            self.invalidate_for(&parsed);
         }
         return result;
      }

      if let (Some(versions), Some(tables)) = (versions, tables.as_deref())
         && !outcome.error
         && !outcome.capture_overflow
         && !outcome.copy_out
         && let Some(row_description) = outcome.row_description.as_deref()
      {
         self.fill_cache(
            tables,
            fingerprint,
            row_description,
            &outcome.data_rows,
            &versions,
         );
      }
      self.flush_out()
   }

   fn local_query_fallback(&mut self, parsed: &ParsedQuery) -> io::Result<()> {
      let tag = match parsed.kind {
         QueryKind::Begin => "BEGIN",
         QueryKind::Commit => "COMMIT",
         QueryKind::Rollback => "ROLLBACK",
         QueryKind::Set => "SET",
         QueryKind::Select => {
            self.queue_error("08006", "no upstream database configured for cache miss");
            self.queue_ready();
            return self.flush_out();
         }
         _ => {
            self.queue_error("08006", "no upstream database configured");
            self.queue_ready();
            return self.flush_out();
         }
      };
      self
         .out
         .extend_from_slice(&protocol::encode_command_complete(tag));
      if matches!(parsed.kind, QueryKind::Commit | QueryKind::Rollback) {
         self.transaction_writes.clear();
      }
      self.queue_ready();
      self.flush_out()
   }

   // ----------------------------------------------------- extended protocol

   fn handle_parse(&mut self, name: String, sql: String, param_oids: Vec<u32>) -> io::Result<()> {
      if self.cycle_error {
         return Ok(());
      }
      if !name.is_empty()
         && !self.statements.contains_key(&name)
         && self.statements.len() >= MAX_NAMED_STATEMENTS
      {
         self.queue_error("53400", "too many prepared statements");
         self.cycle_error = true;
         return Ok(());
      }

      let parsed = self.mgr.analyze(&sql);
      let locally_servable = parsed.is_cacheable
         && parsed.kind == QueryKind::Select
         && !self.in_transaction
         && self.effective_fingerprint(parsed.fingerprint).is_some()
         && self.mgr.is_cacheable(&parsed)
         && self.catalog_allows(&parsed);
      let parse_frame = self.reader.last_frame().to_vec();

      self.statements.insert(
         name.clone(),
         StatementState {
            sql,
            param_oids,
            parse_frame,
            row_description: None,
            forwarded: false,
         },
      );

      if locally_servable {
         protocol::put_parse_complete(&mut self.out);
         return Ok(());
      }

      if self.upstream.is_none() {
         self.statements.remove(&name);
         self.queue_error("08006", "no upstream database configured");
         self.cycle_error = true;
         return Ok(());
      }

      self.send_parse(&name, true)?;
      Ok(())
   }

   fn send_parse(&mut self, name: &str, relay_success: bool) -> io::Result<bool> {
      let Some(statement) = self.statements.get(name) else {
         return Ok(false);
      };
      if statement.forwarded {
         return Ok(true);
      }
      let frame = statement.parse_frame.clone();

      self.flush_out()?;
      let Some(upstream) = self.upstream.as_mut() else {
         return Ok(false);
      };
      upstream.send_and_flush(&frame)?;
      self.cycle_forwarded = true;

      if relay_success {
         let outcome = upstream.relay_until(&mut self.client, ReplyStop::Parse, true)?;
         if outcome.error {
            self.cycle_error = true;
            return Ok(false);
         }
         if let Some(statement) = self.statements.get_mut(name) {
            statement.forwarded = true;
            statement.row_description = outcome.row_description;
         }
         Ok(true)
      } else {
         let mut scratch = Vec::new();
         let outcome = upstream.relay_until(&mut scratch, ReplyStop::Parse, true)?;
         if outcome.error {
            self.client.write_all(&scratch)?;
            self.cycle_error = true;
            return Ok(false);
         }
         if let Some(statement) = self.statements.get_mut(name) {
            statement.forwarded = true;
            if outcome.row_description.is_some() {
               statement.row_description = outcome.row_description;
            }
         }
         Ok(true)
      }
   }

   fn handle_bind(
      &mut self,
      portal: String,
      statement: String,
      param_formats: Vec<i16>,
      params: Vec<Option<Vec<u8>>>,
      result_formats: Vec<i16>,
   ) -> io::Result<()> {
      if self.cycle_error {
         return Ok(());
      }
      if !portal.is_empty()
         && !self.portals.contains_key(&portal)
         && self.portals.len() >= MAX_NAMED_STATEMENTS
      {
         self.queue_error("53400", "too many open portals");
         self.cycle_error = true;
         return Ok(());
      }

      let Some(stmt) = self.statements.get(&statement) else {
         self.queue_error("26000", "prepared statement does not exist");
         self.cycle_error = true;
         return Ok(());
      };
      let sql = stmt.sql.clone();
      let (parsed, base_fingerprint) = self.mgr.analyze_bound(&sql, &params, &param_formats);
      let text_results =
         result_formats.is_empty() || result_formats.iter().all(|format| *format == 0);
      let effective = self.effective_fingerprint(base_fingerprint);
      let cacheable = parsed.is_cacheable
         && parsed.kind == QueryKind::Select
         && !self.in_transaction
         && text_results
         && effective.is_some()
         && self.mgr.is_cacheable(&parsed)
         && self.catalog_allows(&parsed);
      let hit = cacheable
         && self
            .lookup_encoded(&parsed, effective.unwrap_or(base_fingerprint))
            .is_some();

      let bind_frame = self.reader.last_frame().to_vec();
      let mut state = PortalState {
         statement: statement.clone(),
         parsed,
         fingerprint: base_fingerprint,
         text_results,
         bind_frame,
         bind_forwarded: false,
         cursor: 0,
         finished: false,
         row_description: None,
      };

      if hit || self.upstream.is_none() {
         protocol::put_bind_complete(&mut self.out);
         self.portals.insert(portal, state);
         return Ok(());
      }

      if !self.statements.get(&statement).is_some_and(|s| s.forwarded)
         && !self.send_parse(&statement, false)?
      {
         return Ok(());
      }

      self.flush_out()?;
      let Some(upstream) = self.upstream.as_mut() else {
         return Ok(());
      };
      upstream.send_and_flush(&state.bind_frame)?;
      self.cycle_forwarded = true;

      let mut scratch = Vec::new();
      let outcome = upstream.relay_until(&mut scratch, ReplyStop::Bind, true)?;
      if outcome.error {
         self.client.write_all(&scratch)?;
         self.cycle_error = true;
         self.portals.insert(portal, state);
         return Ok(());
      }
      state.bind_forwarded = true;
      if let Some(row_description) = outcome.row_description {
         state.row_description = Some(row_description);
      }
      self.portals.insert(portal, state);
      self.client.write_all(&scratch)?;
      Ok(())
   }

   fn handle_describe(&mut self, kind: u8, name: &str) -> io::Result<()> {
      if self.cycle_error {
         return Ok(());
      }

      match kind {
         b'S' => {
            let Some(statement) = self.statements.get(name) else {
               self.queue_error("26000", "prepared statement does not exist");
               self.cycle_error = true;
               return Ok(());
            };
            let param_oids = statement.param_oids.clone();
            let sql_has_params = statement.sql.contains('$');
            let row_description = statement.row_description.clone();
            let forwarded = statement.forwarded;

            if let Some(row_description) = row_description
               && (!sql_has_params || !param_oids.is_empty())
            {
               protocol::put_parameter_description(&mut self.out, &param_oids);
               self.out.extend_from_slice(&row_description);
               return Ok(());
            }

            if self.upstream.is_none() {
               self.queue_error("08006", "no upstream database configured");
               self.cycle_error = true;
               return Ok(());
            }
            if !forwarded && !self.send_parse(name, false)? {
               return Ok(());
            }
            let frame = self.reader.last_frame().to_vec();
            self.forward_describe(b'S', name, &frame)?
         }
         b'P' => {
            let Some(portal) = self.portals.get(name) else {
               self.queue_error("34000", "portal does not exist");
               self.cycle_error = true;
               return Ok(());
            };
            let parsed = Arc::clone(&portal.parsed);
            let base_fingerprint = portal.fingerprint;
            let text_results = portal.text_results;
            let bind_forwarded = portal.bind_forwarded;
            let statement = portal.statement.clone();

            let fingerprint = self.effective_fingerprint(base_fingerprint);
            if text_results
               && !self.in_transaction
               && fingerprint.is_some()
               && self.mgr.is_cacheable(&parsed)
               && self.catalog_allows(&parsed)
               && let Some(entry) =
                  self.lookup_encoded(&parsed, fingerprint.unwrap_or(base_fingerprint))
            {
               self.out.extend_from_slice(entry.row_description());
               if let Some(portal) = self.portals.get_mut(name) {
                  portal.row_description = Some(entry.row_description().to_vec());
               }
               return Ok(());
            }

            if self.upstream.is_none() {
               self.queue_error("08006", "no upstream database configured");
               self.cycle_error = true;
               return Ok(());
            }
            if !bind_forwarded {
               if !self.statements.get(&statement).is_some_and(|s| s.forwarded)
                  && !self.send_parse(&statement, false)?
               {
                  return Ok(());
               }
               if !self.forward_bind(name)? {
                  return Ok(());
               }
            }
            let frame = self.reader.last_frame().to_vec();
            self.forward_describe(b'P', name, &frame)?
         }
         _ => {
            if self.upstream.is_none() {
               self.queue_error("08006", "no upstream database configured");
               self.cycle_error = true;
               return Ok(());
            }
            let frame = self.reader.last_frame().to_vec();
            self.forward_describe(kind, name, &frame)?;
         }
      }

      Ok(())
   }

   fn forward_describe(&mut self, kind: u8, name: &str, frame: &[u8]) -> io::Result<()> {
      self.flush_out()?;
      let Some(upstream) = self.upstream.as_mut() else {
         return Ok(());
      };
      upstream.send_and_flush(frame)?;
      self.cycle_forwarded = true;

      let outcome = upstream.relay_until(&mut self.client, ReplyStop::Describe, true)?;
      if outcome.error {
         self.cycle_error = true;
      }
      if let Some(row_description) = outcome.row_description {
         match kind {
            b'S' => {
               if let Some(statement) = self.statements.get_mut(name) {
                  statement.row_description = Some(row_description);
               }
            }
            b'P' => {
               if let Some(portal) = self.portals.get_mut(name) {
                  portal.row_description = Some(row_description);
               }
            }
            _ => {}
         }
      }
      Ok(())
   }

   fn forward_bind(&mut self, portal: &str) -> io::Result<bool> {
      let Some(state) = self.portals.get(portal) else {
         return Ok(false);
      };
      let bind_frame = state.bind_frame.clone();

      self.flush_out()?;
      let Some(upstream) = self.upstream.as_mut() else {
         return Ok(false);
      };
      upstream.send_and_flush(&bind_frame)?;
      self.cycle_forwarded = true;

      let mut scratch = Vec::new();
      let outcome = upstream.relay_until(&mut scratch, ReplyStop::Bind, true)?;
      if outcome.error {
         self.client.write_all(&scratch)?;
         self.cycle_error = true;
         return Ok(false);
      }
      if let Some(state) = self.portals.get_mut(portal) {
         state.bind_forwarded = true;
      }
      Ok(true)
   }

   fn handle_execute(&mut self, portal_name: &str, max_rows: i32) -> io::Result<()> {
      if self.cycle_error {
         return Ok(());
      }

      let Some(portal) = self.portals.get(portal_name) else {
         self.queue_error("34000", "portal does not exist");
         self.cycle_error = true;
         return Ok(());
      };
      let parsed = Arc::clone(&portal.parsed);
      let base_fingerprint = portal.fingerprint;
      let text_results = portal.text_results;

      let initial = self.effective_fingerprint(base_fingerprint);
      let cacheable = parsed.is_cacheable
         && parsed.kind == QueryKind::Select
         && !self.in_transaction
         && text_results
         && initial.is_some()
         && self.mgr.is_cacheable(&parsed)
         && self.catalog_allows(&parsed);
      if cacheable {
         let fingerprint = initial.unwrap_or(base_fingerprint);
         if let Some(entry) = self.lookup_encoded(&parsed, fingerprint) {
            if let Some(portal) = self.portals.get_mut(portal_name) {
               Self::serve_encoded_into(&mut self.out, portal, &entry, max_rows);
            }
            return Ok(());
         }
         self
            .mgr
            .metrics()
            .misses
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
      }

      if self.upstream.is_none() {
         self.queue_error("08006", "no upstream database configured for cache miss");
         self.cycle_error = true;
         return Ok(());
      }

      match parsed.kind {
         QueryKind::Begin => {
            self.in_transaction = true;
            self.transaction_writes.clear();
         }
         QueryKind::Rollback => {
            self.in_transaction = false;
            self.transaction_failed = false;
            self.transaction_writes.clear();
         }
         _ => {}
      }

      let statement = self
         .portals
         .get(portal_name)
         .map(|p| p.statement.clone())
         .unwrap_or_default();
      let bind_forwarded = self
         .portals
         .get(portal_name)
         .is_some_and(|p| p.bind_forwarded);
      if !bind_forwarded {
         if !self.statements.get(&statement).is_some_and(|s| s.forwarded)
            && !self.send_parse(&statement, false)?
         {
            return Ok(());
         }
         if !self.forward_bind(portal_name)? {
            return Ok(());
         }
      }

      let wants_cache = parsed.is_cacheable
         && parsed.kind == QueryKind::Select
         && !self.in_transaction
         && text_results
         && self.effective_fingerprint(base_fingerprint).is_some()
         && self.mgr.is_cacheable(&parsed)
         && self.catalog_allows(&parsed);
      let fingerprint = self
         .effective_fingerprint(base_fingerprint)
         .unwrap_or(base_fingerprint);
      if wants_cache
         && self
            .statements
            .get(&statement)
            .is_some_and(|s| s.row_description.is_none())
      {
         self.capture_statement_row_description(&statement)?;
      }

      let mut fill_guard = None;
      if wants_cache && let Some(key) = self.fill_key(&parsed, fingerprint) {
         match self.mgr.begin_fill(&key) {
            Some(guard) => fill_guard = Some(guard),
            None => {
               self.mgr.wait_for_fill(&key);
               if let Some(entry) = self.lookup_encoded(&parsed, fingerprint) {
                  if let Some(portal) = self.portals.get_mut(portal_name) {
                     Self::serve_encoded_into(&mut self.out, portal, &entry, max_rows);
                  }
                  return Ok(());
               }
               fill_guard = self.mgr.begin_fill(&key);
            }
         }
      }
      let _fill_guard = fill_guard;

      let tables = wants_cache.then(|| self.mgr.expanded_tables(&parsed));
      let versions = tables
         .as_ref()
         .map(|tables| self.mgr.table_versions(tables));
      let execute_frame = self.reader.last_frame().to_vec();
      self.flush_out()?;
      let buffer_tail = wants_cache || parsed.kind == QueryKind::Commit;
      let outcome = {
         let Some(upstream) = self.upstream.as_mut() else {
            return Ok(());
         };
         upstream.send_and_flush(&execute_frame)?;
         self.cycle_forwarded = true;
         if buffer_tail {
            let mut sink = ClientSink {
               out: &mut self.out,
               client: &mut self.client,
            };
            upstream.relay_until(&mut sink, ReplyStop::Execute, wants_cache)?
         } else {
            upstream.relay_until(&mut self.client, ReplyStop::Execute, true)?
         }
      };
      if outcome.error {
         self.cycle_error = true;
      } else {
         if parsed.schema_change {
            self.flush_catalog_entries();
         }
         self.apply_session_changes(&parsed);
         if parsed.writes {
            self.invalidate_for(&parsed);
         }
      }
      if parsed.kind == QueryKind::Commit && !outcome.error {
         self.in_transaction = false;
         self.transaction_failed = false;
         self.invalidate_committed();
      }
      if outcome.copy_in {
         self.portals.remove(portal_name);
         self.cycle_error = false;
         self.flush_out()?;
         let result = self.pump_copy_in(ReplyStop::Execute);
         if !self.cycle_error && parsed.writes {
            self.invalidate_for(&parsed);
         }
         return result;
      }

      let command_is_select = outcome
         .command_tag
         .as_deref()
         .is_some_and(|tag| tag.starts_with("SELECT"));

      if let Some(portal) = self.portals.get_mut(portal_name) {
         portal.finished = outcome.command_tag.is_some() && !outcome.suspended;
         if outcome.row_description.is_some() {
            portal.row_description = outcome.row_description.clone();
         }
      }

      let store_result = wants_cache
         && !outcome.error
         && !outcome.suspended
         && max_rows <= 0
         && !outcome.capture_overflow
         && !outcome.copy_out
         && command_is_select;
      if store_result {
         let row_description = {
            let statement_rd = self
               .statements
               .get(&statement)
               .and_then(|s| s.row_description.clone());
            let portal_rd = self
               .portals
               .get(portal_name)
               .and_then(|p| p.row_description.clone());
            statement_rd.or(portal_rd).or(outcome.row_description)
         };
         if let (Some(row_description), Some(versions), Some(tables)) =
            (row_description, versions, tables.as_deref())
         {
            self.fill_cache(
               tables,
               fingerprint,
               &row_description,
               &outcome.data_rows,
               &versions,
            );
         }
      }
      self.flush_out()
   }

   fn capture_statement_row_description(&mut self, statement: &str) -> io::Result<bool> {
      if self
         .statements
         .get(statement)
         .is_none_or(|s| s.row_description.is_some())
      {
         return Ok(true);
      }

      let mut body = Vec::with_capacity(statement.len() + 2);
      body.push(b'S');
      body.extend_from_slice(statement.as_bytes());
      body.push(0);
      let mut frame = Vec::with_capacity(body.len() + 5);
      protocol::put_message(&mut frame, b'D', &body);

      self.flush_out()?;
      let Some(upstream) = self.upstream.as_mut() else {
         return Ok(false);
      };
      upstream.send_and_flush(&frame)?;
      self.cycle_forwarded = true;

      let mut scratch = Vec::new();
      let outcome = upstream.relay_until(&mut scratch, ReplyStop::Describe, true)?;
      if outcome.error {
         self.client.write_all(&scratch)?;
         self.cycle_error = true;
         return Ok(false);
      }
      let captured = outcome.row_description.is_some();
      if let Some(statement) = self.statements.get_mut(statement) {
         statement.row_description = outcome.row_description;
      }
      Ok(captured)
   }

   fn serve_encoded_into(
      out: &mut Vec<u8>,
      portal: &mut PortalState,
      entry: &EncodedResult,
      max_rows: i32,
   ) {
      let limit = if max_rows > 0 {
         max_rows as usize
      } else {
         usize::MAX
      };
      Self::write_encoded_rows_into(out, &mut portal.cursor, entry, limit);
      let exhausted = next_data_frame(entry.data_rows(), portal.cursor).is_none();
      if max_rows > 0 && !exhausted {
         out.extend_from_slice(&PORTAL_SUSPENDED);
      } else {
         out.extend_from_slice(entry.command_complete());
         portal.cursor = entry.data_rows().len();
         portal.finished = true;
      }
   }

   fn write_encoded_rows_into(
      out: &mut Vec<u8>,
      cursor: &mut usize,
      entry: &EncodedResult,
      limit: usize,
   ) -> usize {
      let data = entry.data_rows();
      if limit == usize::MAX && *cursor == 0 {
         out.reserve(data.len() + entry.command_complete().len());
         out.extend_from_slice(data);
         *cursor = data.len();
         return entry.row_count as usize;
      }
      let mut sent = 0usize;
      let mut offset = *cursor;
      while sent < limit {
         let Some((next, frame)) = next_data_frame(data, offset) else {
            break;
         };
         out.extend_from_slice(frame);
         offset = next;
         sent += 1;
      }
      *cursor = offset;
      sent
   }

   // --------------------------------------------------------------- sync

   fn handle_sync(&mut self) -> io::Result<()> {
      let forwarded = self.cycle_forwarded;
      self.cycle_error = false;
      self.cycle_forwarded = false;

      if forwarded {
         let frame = self.reader.last_frame().to_vec();
         self.flush_out()?;
         let Some(upstream) = self.upstream.as_mut() else {
            self.queue_ready();
            return self.flush_out();
         };
         upstream.send_raw(&frame)?;
         upstream.flush()?;
         if self.transaction_writes.is_empty() {
            let outcome = upstream.relay_until(&mut self.client, ReplyStop::Sync, false)?;
            self.update_status(&outcome);
         } else {
            let mut tail = Vec::new();
            let outcome = upstream.relay_until(&mut tail, ReplyStop::Sync, false)?;
            self.update_status(&outcome);
            self.client.write_all(&tail)?;
         }
         Ok(())
      } else {
         self.queue_ready();
         self.flush_out()
      }
   }

   fn handle_close(&mut self, kind: u8, name: &str) -> io::Result<()> {
      if self.cycle_error {
         if kind == b'S' {
            self.statements.remove(name);
            self.portals.retain(|_, portal| portal.statement != name);
         } else {
            self.portals.remove(name);
         }
         return Ok(());
      }

      if kind == b'S' {
         let forwarded = self
            .statements
            .get(name)
            .is_some_and(|statement| statement.forwarded);
         if forwarded && self.upstream.is_some() {
            let frame = self.reader.last_frame().to_vec();
            self.forward_close(&frame)?;
         } else {
            protocol::put_close_complete(&mut self.out);
         }
         self.statements.remove(name);
         self.portals.retain(|_, portal| portal.statement != name);
      } else {
         let forwarded = self
            .portals
            .get(name)
            .is_some_and(|portal| portal.bind_forwarded);
         if forwarded && self.upstream.is_some() {
            let frame = self.reader.last_frame().to_vec();
            self.forward_close(&frame)?;
         } else {
            protocol::put_close_complete(&mut self.out);
         }
         self.portals.remove(name);
      }
      Ok(())
   }

   fn forward_close(&mut self, frame: &[u8]) -> io::Result<()> {
      self.flush_out()?;
      let Some(upstream) = self.upstream.as_mut() else {
         return Ok(());
      };
      upstream.send_and_flush(frame)?;
      self.cycle_forwarded = true;
      let outcome = upstream.relay_until(&mut self.client, ReplyStop::Close, false)?;
      if outcome.error {
         self.cycle_error = true;
      }
      Ok(())
   }

   // -------------------------------------------------------------- COPY IN

   fn pump_copy_in(&mut self, stop: ReplyStop) -> io::Result<()> {
      loop {
         let tag = match self.reader.read_raw() {
            Ok(tag) => tag,
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => return Ok(()),
            Err(e) if e.kind() == io::ErrorKind::ConnectionReset => return Ok(()),
            Err(e) => return Err(e),
         };
         if tag == b'X' {
            return Ok(());
         }
         let frame = self.reader.last_frame().to_vec();
         let Some(upstream) = self.upstream.as_mut() else {
            return Ok(());
         };
         upstream.send_raw(&frame)?;
         if tag == b'c' || tag == b'f' {
            if matches!(stop, ReplyStop::Execute) {
               upstream.request_flush()?;
            } else {
               upstream.flush()?;
            }
            break;
         }
      }

      self.flush_out()?;
      let Some(upstream) = self.upstream.as_mut() else {
         return Ok(());
      };
      let outcome = upstream.relay_until(&mut self.client, stop, false)?;
      self.update_status(&outcome);
      if matches!(stop, ReplyStop::Sync) {
         self.cycle_forwarded = false;
      }
      if outcome.error {
         self.cycle_error = true;
      }
      Ok(())
   }

   // ------------------------------------------------------------ helpers

   fn reclaim_upstream(&mut self) {
      let Some(upstream) = self.upstream.take() else {
         return;
      };
      let Some(key) = self.pool_key.take() else {
         return;
      };
      if !self.upstream_poolable || self.in_transaction || self.transaction_failed {
         return;
      }
      let mut upstream = upstream;
      if upstream.reset_session().is_err() {
         return;
      }
      self.pool.checkin(key, upstream);
      self.mgr.metrics().pool_idle.store(
         self.pool.idle_count() as u64,
         std::sync::atomic::Ordering::Relaxed,
      );
   }

   fn upstream_target(&self) -> Option<&str> {
      match self.config.mode {
         DbProxyMode::Dual => self.config.upstream_addr.as_deref(),
         DbProxyMode::CacheOnly => None,
      }
   }

   fn lookup_encoded(&self, parsed: &ParsedQuery, fingerprint: u64) -> Option<Arc<EncodedResult>> {
      let primary = parsed
         .tables
         .first()
         .map(String::as_str)
         .unwrap_or("default");
      if let Some(entry) = self
         .mgr
         .get_encoded(&self.config.key_prefix, primary, fingerprint)
      {
         return Some(entry);
      }
      for table in parsed.tables.iter().skip(1) {
         if let Some(entry) = self
            .mgr
            .get_encoded(&self.config.key_prefix, table, fingerprint)
         {
            return Some(entry);
         }
      }
      None
   }

   fn lookup_json(
      &self,
      parsed: &ParsedQuery,
      fingerprint: u64,
   ) -> Option<crate::row::DbResultSet> {
      for table in &parsed.tables {
         if let Some(result) =
            self
               .mgr
               .get_cached_query(&self.config.key_prefix, table, fingerprint)
         {
            self.mgr.remember_encoded(
               &self.config.key_prefix,
               table,
               fingerprint,
               &result,
               self.config.default_ttl_ms,
            );
            return Some(result);
         }
      }
      None
   }

   fn fill_cache(
      &self,
      tables: &[String],
      fingerprint: u64,
      row_description: &[u8],
      data_rows: &[u8],
      versions: &[u64],
   ) {
      let Some(body) = row_description.get(5..) else {
         return;
      };
      let Ok(columns) = crate::upstream::parse_row_description(body) else {
         return;
      };
      if columns.is_empty() {
         return;
      }

      let mut rows = Vec::new();
      let mut offset = 0usize;
      while let Some((next, frame)) = next_data_frame(data_rows, offset) {
         if let Ok(row) = crate::upstream::parse_data_row(&frame[5..]) {
            rows.push(row);
         }
         offset = next;
      }

      let result = crate::row::DbResultSet {
         columns,
         rows_affected: rows.len() as u64,
         rows,
      };
      self.mgr.store_cached_query_jittered(
         &self.config.key_prefix,
         tables,
         fingerprint,
         &result,
         self.config.default_ttl_ms,
         self.config.ttl_jitter_pct,
         versions,
      );
   }

   fn invalidate_for(&mut self, parsed: &ParsedQuery) {
      if !parsed.writes {
         return;
      }
      let tables = self.mgr.expanded_tables(parsed);
      if self.in_transaction {
         for table in &tables {
            if !self.transaction_writes.iter().any(|t| t == table) {
               self.transaction_writes.push(table.clone());
            }
         }
      }
      self.mgr.invalidate_tables(&tables);
      if let Some(bus) = &self.bus {
         bus.invalidate(&tables);
      }
   }

   fn invalidate_committed(&mut self) {
      if self.transaction_writes.is_empty() {
         return;
      }
      let written = std::mem::take(&mut self.transaction_writes);
      self.mgr.invalidate_tables(&written);
      if let Some(bus) = &self.bus {
         bus.invalidate(&written);
      }
   }

   fn flush_catalog_entries(&mut self) {
      self.mgr.mark_catalog_stale();
      self.mgr.flush_all(&self.config.key_prefix);
      if let Some(bus) = &self.bus {
         bus.flush();
      }
   }

   // ------------------------------------------------------ session context

   fn effective_fingerprint(&self, base: u64) -> Option<u64> {
      if self.context_state != ContextState::Trusted {
         return None;
      }
      Some(if self.context_hash == 0 {
         base
      } else {
         mix64(base ^ self.context_hash)
      })
   }

   fn catalog_allows(&self, parsed: &ParsedQuery) -> bool {
      self.upstream.is_none() || self.mgr.catalog_ready() || parsed.functions.is_empty()
   }

   fn fill_key(&self, parsed: &ParsedQuery, fingerprint: u64) -> Option<String> {
      let table = parsed
         .tables
         .first()
         .map(String::as_str)
         .unwrap_or("default");
      Some(
         self
            .mgr
            .cache_key(&self.config.key_prefix, table, fingerprint),
      )
   }

   fn probe_context(&mut self) -> bool {
      let Some(upstream) = self.upstream.as_mut() else {
         self.context_hash = 0;
         self.context_state = ContextState::Trusted;
         return true;
      };
      let Ok(result) = upstream.execute_select(super::CONTEXT_PROBE) else {
         self.context_state = ContextState::Untrusted;
         return false;
      };
      let Some(row) = result.rows.first() else {
         self.context_state = ContextState::Untrusted;
         return false;
      };

      let mut hash = 0x9e37_79b9_7f4a_7c15u64;
      for value in &row.values {
         hash = mix64(hash ^ value_fingerprint(value));
      }
      let hash = mix64(hash);
      self.context_hash = if hash == 0 { 1 } else { hash };
      self.context_state = ContextState::Trusted;
      self.context_sets = 0;
      true
   }

   fn apply_session_changes(&mut self, parsed: &ParsedQuery) {
      if self.upstream.is_none() {
         return;
      }
      if parsed.mutates_session {
         self.context_state = ContextState::NeedsProbe;
      }
      for change in &parsed.session_changes {
         match change {
            SessionChange::Set { statement, local } => {
               if *local || self.in_transaction {
                  self.context_state = ContextState::NeedsProbe;
               } else if self.context_state == ContextState::Trusted {
                  self.context_hash = mix64(self.context_hash ^ compute_fingerprint(statement));
                  if self.context_hash == 0 {
                     self.context_hash = 1;
                  }
                  self.context_sets = self.context_sets.saturating_add(1);
                  if self.context_sets > MAX_CONTEXT_SETS {
                     self.context_state = ContextState::NeedsProbe;
                  }
               }
            }
            SessionChange::Reset => self.context_state = ContextState::NeedsProbe,
            SessionChange::DiscardAll => {
               self.context_state = ContextState::NeedsProbe;
               self.statements.clear();
               self.portals.clear();
            }
         }
      }
   }

   // ------------------------------------------------------- upkeep (idle)

   fn maintain_idle(&mut self) {
      if self.cycle_forwarded
         || self.cycle_error
         || self.in_transaction
         || !self.out.is_empty()
         || self.upstream.is_none()
      {
         return;
      }
      if self.context_state != ContextState::Trusted {
         let _ = self.probe_context();
      }
      if self.mgr.catalog_refresh_due() {
         let _ = self.refresh_catalog();
      }
   }

   fn refresh_catalog(&mut self) -> bool {
      if !self.mgr.try_begin_catalog_refresh() {
         return self.mgr.catalog_ready();
      }
      let Some(upstream) = self.upstream.as_mut() else {
         self.mgr.catalog_refresh_failed();
         return false;
      };
      let functions = upstream.execute_select(super::CATALOG_FUNCTIONS_SQL);
      let views = upstream.execute_select(super::CATALOG_VIEWS_SQL);
      match (functions, views) {
         (Ok(functions), Ok(views)) => {
            self
               .mgr
               .install_catalog(crate::cache::CatalogSnapshot::from_rows(
                  &functions.rows,
                  &views.rows,
               ));
            true
         }
         _ => {
            self.mgr.catalog_refresh_failed();
            false
         }
      }
   }

   fn update_status(&mut self, outcome: &RelayOutcome) {
      match outcome.status {
         b'T' => {
            self.in_transaction = true;
         }
         b'E' => {
            self.in_transaction = true;
            self.transaction_failed = true;
         }
         b'I' => {
            self.in_transaction = false;
            self.transaction_failed = false;
            self.invalidate_committed();
         }
         _ => {}
      }
   }

   fn queue_ready(&mut self) {
      let status = if self.transaction_failed {
         b'E'
      } else if self.in_transaction {
         b'T'
      } else {
         b'I'
      };
      self.out.extend_from_slice(&protocol::encode_ready(status));
   }

   fn queue_error(&mut self, code: &str, message: &str) {
      self
         .out
         .extend_from_slice(&protocol::encode_error("ERROR", code, message));
      if self.in_transaction {
         self.transaction_failed = true;
      }
   }

   fn flush_out(&mut self) -> io::Result<()> {
      if !self.out.is_empty() {
         self.client.write_all(&self.out)?;
         self.client.flush()?;
         self.out.clear();
      }
      Ok(())
   }
}

fn next_data_frame(data: &[u8], offset: usize) -> Option<(usize, &[u8])> {
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
   Some((offset + total, &data[offset..offset + total]))
}

fn value_fingerprint(value: &DbValue) -> u64 {
   match value {
      DbValue::Null => 0x9e37_79b9_7f4a_7c15,
      DbValue::Bool(b) => compute_fingerprint(if *b { "true" } else { "false" }),
      DbValue::Text(text) => compute_fingerprint(text),
      DbValue::Int(n) => {
         let s = n.to_string();
         compute_fingerprint(&s)
      }
      DbValue::Float(f) => {
         let s = f.to_string();
         compute_fingerprint(&s)
      }
   }
}
