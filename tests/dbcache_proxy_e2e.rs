use fyro_db::storage::store::Store;
use fyro_dbcache::protocol::{
   GSSENC_REQUEST_CODE, PROTOCOL_VERSION_3, SSL_REQUEST_CODE, encode_bind, encode_command_complete,
   encode_row_description, put_message,
};
use fyro_dbcache::query::parse_sql;
use fyro_dbcache::{
   AuthUsers, ColumnInfo, DbCacheConfig, DbCacheManager, DbCacheStorage, DbProxyMode, ScramClient,
   start_proxy_listener,
};
use std::collections::HashMap;
use std::io::{self, BufReader, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::thread;
use std::time::{Duration, Instant};

const ROWS: [(&str, &str); 2] = [("1", "alpha"), ("2", "beta")];

#[derive(Default)]
struct Stats {
   queries: AtomicUsize,
   parses: AtomicUsize,
   binds: AtomicUsize,
   executes: AtomicUsize,
   describes: AtomicUsize,
   sasl_rounds: AtomicUsize,
   connections: AtomicUsize,
   password_ok: AtomicBool,
   last_set: std::sync::Mutex<String>,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Auth {
   Cleartext,
   Scram,
}

// ------------------------------------------------------------ fake upstream

struct FakePg;

impl FakePg {
   fn start() -> (String, Arc<Stats>) {
      Self::start_with_auth(Auth::Cleartext)
   }

   fn start_scram() -> (String, Arc<Stats>) {
      Self::start_with_auth(Auth::Scram)
   }

   fn start_with_auth(auth: Auth) -> (String, Arc<Stats>) {
      let listener = TcpListener::bind("127.0.0.1:0").expect("bind fake pg");
      let addr = listener.local_addr().unwrap().to_string();
      let stats = Arc::new(Stats::default());
      let shared = Arc::clone(&stats);
      thread::Builder::new()
         .name("fake-pg".into())
         .spawn(move || {
            for stream in listener.incoming() {
               let Ok(stream) = stream else {
                  break;
               };
               shared.connections.fetch_add(1, Ordering::SeqCst);
               let stats = Arc::clone(&shared);
               thread::spawn(move || {
                  let _ = serve_conn(stream, stats, auth);
               });
            }
         })
         .expect("spawn fake pg");
      (addr, stats)
   }
}

fn serve_conn(stream: TcpStream, stats: Arc<Stats>, auth: Auth) -> io::Result<()> {
   let mut out = stream.try_clone()?;
   let mut reader = BufReader::new(stream);

   loop {
      let Some(payload) = read_initial(&mut reader) else {
         return Ok(());
      };
      let code = i32::from_be_bytes([payload[0], payload[1], payload[2], payload[3]]);
      if code == SSL_REQUEST_CODE || code == GSSENC_REQUEST_CODE {
         out.write_all(b"N")?;
         continue;
      }
      break;
   }

   match auth {
      // Cleartext authentication exercises the proxy's verbatim auth relay.
      Auth::Cleartext => {
         write_msg(&mut out, b'R', &3i32.to_be_bytes())?;
         let Some((tag, body)) = read_tagged(&mut reader) else {
            return Ok(());
         };
         if tag != b'p' {
            return Ok(());
         }
         if body.strip_suffix(&[0]) != Some(b"secret") {
            write_msg(
               &mut out,
               b'E',
               b"SERROR\0C28P01\0Mpassword authentication failed\0\0",
            )?;
            return Ok(());
         }
         stats.password_ok.store(true, Ordering::SeqCst);
      }
      // SASL: two client responses, then a server-only SASLFinal round.
      Auth::Scram => {
         let mut mechanisms = Vec::new();
         mechanisms.extend_from_slice(&10i32.to_be_bytes());
         mechanisms.extend_from_slice(b"SCRAM-SHA-256\x00\x00");
         write_msg(&mut out, b'R', &mechanisms)?;
         let Some((tag, _)) = read_tagged(&mut reader) else {
            return Ok(());
         };
         if tag != b'p' {
            return Ok(());
         }
         stats.sasl_rounds.fetch_add(1, Ordering::SeqCst);

         let mut server_first = Vec::new();
         server_first.extend_from_slice(&11i32.to_be_bytes());
         server_first.extend_from_slice(b"r=abc,s=xyz,i=4096");
         write_msg(&mut out, b'R', &server_first)?;
         let Some((tag, _)) = read_tagged(&mut reader) else {
            return Ok(());
         };
         if tag != b'p' {
            return Ok(());
         }
         stats.sasl_rounds.fetch_add(1, Ordering::SeqCst);

         let mut server_final = Vec::new();
         server_final.extend_from_slice(&12i32.to_be_bytes());
         server_final.extend_from_slice(b"v=proof");
         write_msg(&mut out, b'R', &server_final)?;
      }
   }

   write_msg(&mut out, b'R', &0i32.to_be_bytes())?;
   write_msg(&mut out, b'S', b"server_version\x0015.0\x00")?;
   write_msg(&mut out, b'Z', b"I")?;

   let mut statements: HashMap<String, String> = HashMap::new();
   let mut portals: HashMap<String, String> = HashMap::new();
   // Real PostgreSQL buffers extended-protocol replies until Flush or Sync.
   let mut pending: Vec<u8> = Vec::new();
   let mut in_txn = false;

   loop {
      let Some((tag, body)) = read_tagged(&mut reader) else {
         return Ok(());
      };
      match tag {
         b'Q' => {
            let sql = cstring(&body);
            let lower = sql.trim_start().to_ascii_lowercase();
            if lower.starts_with("set application_name") {
               *stats.last_set.lock().unwrap() = sql.clone();
            }
            if is_internal_query(&lower) {
               respond_internal(&mut out, &lower)?;
            } else {
               stats.queries.fetch_add(1, Ordering::SeqCst);
               respond_query(&mut out, &sql, true)?;
            }
            apply_txn_state(&mut in_txn, &sql);
            write_msg(&mut out, b'Z', if in_txn { b"T" } else { b"I" })?;
         }
         b'P' => {
            stats.parses.fetch_add(1, Ordering::SeqCst);
            let (name, sql) = split_two(&body);
            statements.insert(name, sql);
            push_msg(&mut pending, b'1', &[]);
         }
         b'B' => {
            stats.binds.fetch_add(1, Ordering::SeqCst);
            let (portal, statement) = split_two(&body);
            portals.insert(portal, statement);
            push_msg(&mut pending, b'2', &[]);
         }
         b'D' => {
            stats.describes.fetch_add(1, Ordering::SeqCst);
            let kind = body.first().copied().unwrap_or(b'S');
            let name = cstring(&body[1.min(body.len())..]);
            let statement = if kind == b'S' {
               name
            } else {
               portals.get(&name).cloned().unwrap_or_default()
            };
            let sql = statements.get(&statement).cloned().unwrap_or_default();
            let is_select = sql.trim_start().to_ascii_lowercase().starts_with("select");
            if kind == b'S' {
               push_msg(&mut pending, b't', &0i16.to_be_bytes());
            }
            if is_select {
               pending.extend_from_slice(&row_description_frame());
            } else {
               push_msg(&mut pending, b'n', &[]);
            }
         }
         b'E' => {
            stats.executes.fetch_add(1, Ordering::SeqCst);
            let portal = cstring(&body);
            let sql = portals
               .get(&portal)
               .and_then(|statement| statements.get(statement))
               .cloned()
               .unwrap_or_default();
            respond_query(&mut pending, &sql, false)?;
            apply_txn_state(&mut in_txn, &sql);
         }
         b'S' => {
            out.write_all(&pending)?;
            pending.clear();
            write_msg(&mut out, b'Z', if in_txn { b"T" } else { b"I" })?;
         }
         b'C' => push_msg(&mut pending, b'3', &[]),
         b'H' => {
            out.write_all(&pending)?;
            pending.clear();
         }
         b'X' => return Ok(()),
         _ => {}
      }
   }
}

fn push_msg(buf: &mut Vec<u8>, tag: u8, body: &[u8]) {
   buf.push(tag);
   buf.extend_from_slice(&((body.len() + 4) as i32).to_be_bytes());
   buf.extend_from_slice(body);
}

fn apply_txn_state(in_txn: &mut bool, sql: &str) {
   let lower = sql.trim_start().to_ascii_lowercase();
   if lower.starts_with("begin") {
      *in_txn = true;
   } else if lower.starts_with("commit") || lower.starts_with("rollback") {
      *in_txn = false;
   }
}

// The proxy issues context, catalog, and statistics queries on its own
// connection. They are answered here but never counted as client queries.
fn is_internal_query(lower: &str) -> bool {
   lower.contains("from pg_proc p")
      || lower.contains("from pg_rewrite r")
      || lower.contains("from pg_stat_all_tables")
      || lower.starts_with("select current_setting('search_path'")
}

fn respond_internal<W: Write>(out: &mut W, lower: &str) -> io::Result<()> {
   if lower.contains("from pg_proc") {
      out.write_all(&row_description_names(&["nspname", "proname"]))?;
      write_msg(
         out,
         b'D',
         &data_row_body(&[Some("pg_catalog"), Some("my_stable_fn")]),
      )?;
   } else if lower.contains("from pg_rewrite") {
      out.write_all(&row_description_names(&["relname", "definition", "base"]))?;
      write_msg(
         out,
         b'D',
         &data_row_body(&[
            Some("user_view"),
            Some("select id, username from users"),
            Some("users"),
         ]),
      )?;
   } else if lower.contains("from pg_stat_all_tables") {
      out.write_all(&row_description_names(&["relname", "ins", "upd", "del"]))?;
      write_msg(
         out,
         b'D',
         &data_row_body(&[Some("users"), Some("0"), Some("0"), Some("0")]),
      )?;
   } else {
      out.write_all(&row_description_frame())?;
      write_msg(
         out,
         b'D',
         &data_row_body(&[Some("public"), Some("postgres")]),
      )?;
   }
   out.write_all(&encode_command_complete("SELECT 1"))?;
   Ok(())
}

fn row_description_names(names: &[&str]) -> Vec<u8> {
   let columns: Vec<ColumnInfo> = names
      .iter()
      .map(|name| ColumnInfo {
         name: name.to_string(),
         type_oid: 25,
      })
      .collect();
   encode_row_description(&columns)
}

fn respond_query<W: Write>(out: &mut W, sql: &str, describe: bool) -> io::Result<()> {
   let lower = sql.trim_start().to_ascii_lowercase();
   if lower.starts_with("select") {
      if describe {
         out.write_all(&row_description_frame())?;
      }
      for (id, name) in ROWS {
         write_msg(out, b'D', &data_row_body(&[Some(id), Some(name)]))?;
      }
      out.write_all(&encode_command_complete("SELECT 2"))?;
      return Ok(());
   }

   let tag = if lower.starts_with("begin") {
      "BEGIN"
   } else if lower.starts_with("commit") {
      "COMMIT"
   } else if lower.starts_with("rollback") {
      "ROLLBACK"
   } else if lower.starts_with("update") {
      "UPDATE 1"
   } else if lower.starts_with("insert") {
      "INSERT 0 1"
   } else if lower.starts_with("delete") {
      "DELETE 1"
   } else if lower.is_empty() {
      write_msg(out, b'I', &[])?;
      return Ok(());
   } else {
      "OK"
   };
   out.write_all(&encode_command_complete(tag))?;
   Ok(())
}

fn row_description_frame() -> Vec<u8> {
   encode_row_description(&[
      ColumnInfo {
         name: "id".to_string(),
         type_oid: 23,
      },
      ColumnInfo {
         name: "username".to_string(),
         type_oid: 25,
      },
   ])
}

fn data_row_body(values: &[Option<&str>]) -> Vec<u8> {
   let mut body = Vec::new();
   body.extend_from_slice(&(values.len() as i16).to_be_bytes());
   for value in values {
      match value {
         Some(text) => {
            body.extend_from_slice(&(text.len() as i32).to_be_bytes());
            body.extend_from_slice(text.as_bytes());
         }
         None => body.extend_from_slice(&(-1i32).to_be_bytes()),
      }
   }
   body
}

// -------------------------------------------------------------- wire utils

fn read_initial<R: Read>(reader: &mut R) -> Option<Vec<u8>> {
   let mut len_buf = [0u8; 4];
   reader.read_exact(&mut len_buf).ok()?;
   let len = i32::from_be_bytes(len_buf);
   if len < 8 {
      return None;
   }
   let mut payload = vec![0u8; (len - 4) as usize];
   reader.read_exact(&mut payload).ok()?;
   Some(payload)
}

fn read_tagged<R: Read>(reader: &mut R) -> Option<(u8, Vec<u8>)> {
   let mut head = [0u8; 5];
   reader.read_exact(&mut head).ok()?;
   let len = i32::from_be_bytes([head[1], head[2], head[3], head[4]]);
   if len < 4 {
      return None;
   }
   let mut body = vec![0u8; (len - 4) as usize];
   reader.read_exact(&mut body).ok()?;
   Some((head[0], body))
}

fn write_msg<W: Write>(writer: &mut W, tag: u8, body: &[u8]) -> io::Result<()> {
   writer.write_all(&[tag])?;
   writer.write_all(&((body.len() + 4) as i32).to_be_bytes())?;
   writer.write_all(body)
}

fn cstring(body: &[u8]) -> String {
   let end = body.iter().position(|&b| b == 0).unwrap_or(body.len());
   String::from_utf8_lossy(&body[..end]).to_string()
}

fn split_two(body: &[u8]) -> (String, String) {
   let first_end = body.iter().position(|&b| b == 0).unwrap_or(body.len());
   let first = String::from_utf8_lossy(&body[..first_end]).to_string();
   let rest = body.get(first_end + 1..).unwrap_or(&[]);
   (first, cstring(rest))
}

fn free_port() -> u16 {
   TcpListener::bind("127.0.0.1:0")
      .expect("bind ephemeral")
      .local_addr()
      .unwrap()
      .port()
}

// ------------------------------------------------------------- test client

#[derive(Default)]
struct Reply {
   row_tags: Vec<u8>,
   tags: Vec<String>,
   rows: Vec<Vec<Option<String>>>,
   suspended: bool,
   error: Option<String>,
}

struct Client {
   stream: TcpStream,
   reader: BufReader<TcpStream>,
}

impl Client {
   fn connect(port: u16) -> Self {
      let mut stream = TcpStream::connect(("127.0.0.1", port)).expect("connect proxy");
      stream.set_nodelay(true).unwrap();
      stream
         .set_read_timeout(Some(std::time::Duration::from_secs(5)))
         .unwrap();
      let mut payload = Vec::new();
      payload.extend_from_slice(&PROTOCOL_VERSION_3.to_be_bytes());
      payload.extend_from_slice(b"user\0postgres\0database\0test\0\0");
      stream
         .write_all(&((payload.len() + 4) as i32).to_be_bytes())
         .unwrap();
      stream.write_all(&payload).unwrap();

      let mut reader = BufReader::new(stream.try_clone().unwrap());
      loop {
         let Some((tag, body)) = read_tagged(&mut reader) else {
            panic!("proxy closed during startup");
         };
         match tag {
            b'R' if body.len() >= 4 => {
               let code = i32::from_be_bytes([body[0], body[1], body[2], body[3]]);
               match code {
                  3 => write_msg(&mut stream, b'p', b"secret\0").unwrap(),
                  10 => {
                     let mut initial = Vec::new();
                     initial.extend_from_slice(b"SCRAM-SHA-256\0");
                     initial.extend_from_slice(&7i32.to_be_bytes());
                     initial.extend_from_slice(b"n,,n=,r");
                     write_msg(&mut stream, b'p', &initial).unwrap();
                  }
                  11 => write_msg(&mut stream, b'p', b"c=biws,r=abc,p=proof").unwrap(),
                  _ => {}
               }
            }
            b'E' => panic!("startup error: {}", error_message(&body)),
            b'Z' => break,
            _ => {}
         }
      }

      Self { stream, reader }
   }

   fn send(&mut self, tag: u8, body: &[u8]) {
      let mut frame = Vec::new();
      put_message(&mut frame, tag, body);
      self.stream.write_all(&frame).unwrap();
   }

   fn simple(&mut self, sql: &str) -> Reply {
      let mut body = Vec::from(sql.as_bytes());
      body.push(0);
      self.send(b'Q', &body);
      self.read_until_ready()
   }

   fn parse(&mut self, name: &str, sql: &str) {
      let mut body = Vec::new();
      body.extend_from_slice(name.as_bytes());
      body.push(0);
      body.extend_from_slice(sql.as_bytes());
      body.push(0);
      body.extend_from_slice(&0i16.to_be_bytes());
      self.send(b'P', &body);
   }

   fn bind(&mut self, portal: &str, statement: &str, params: &[Option<Vec<u8>>]) {
      let frame = encode_bind(portal, statement, &[], params, &[]);
      self.stream.write_all(&frame).unwrap();
   }

   fn describe_portal(&mut self, portal: &str) {
      let mut body = Vec::new();
      body.push(b'P');
      body.extend_from_slice(portal.as_bytes());
      body.push(0);
      self.send(b'D', &body);
   }

   fn execute(&mut self, portal: &str, max_rows: i32) {
      let mut body = Vec::new();
      body.extend_from_slice(portal.as_bytes());
      body.push(0);
      body.extend_from_slice(&max_rows.to_be_bytes());
      self.send(b'E', &body);
   }

   fn sync(&mut self) {
      self.send(b'S', &[]);
   }

   fn read_until_ready(&mut self) -> Reply {
      let mut reply = Reply::default();
      loop {
         let Some((tag, body)) = read_tagged(&mut self.reader) else {
            panic!("proxy closed before ReadyForQuery");
         };
         reply.row_tags.push(tag);
         match tag {
            b'D' => reply.rows.push(parse_data_row(&body)),
            b'C' => reply.tags.push(cstring(&body)),
            b's' => reply.suspended = true,
            b'E' => reply.error = Some(error_message(&body)),
            b'Z' => break,
            _ => {}
         }
      }
      reply
   }
}

// Real SCRAM-SHA-256 frontend login, including server-signature verification.
fn connect_scram(port: u16, user: &str, password: &str) -> Result<Client, String> {
   connect_scram_with_app(port, user, password, None)
}

fn connect_scram_with_app(
   port: u16,
   user: &str,
   password: &str,
   application_name: Option<&str>,
) -> Result<Client, String> {
   let mut stream = TcpStream::connect(("127.0.0.1", port)).map_err(|error| error.to_string())?;
   stream.set_nodelay(true).unwrap();
   stream
      .set_read_timeout(Some(Duration::from_secs(5)))
      .unwrap();
   let mut payload = Vec::new();
   payload.extend_from_slice(&PROTOCOL_VERSION_3.to_be_bytes());
   payload.extend_from_slice(format!("user\0{user}\0database\0test\0").as_bytes());
   if let Some(name) = application_name {
      payload.extend_from_slice(format!("application_name\0{name}\0").as_bytes());
   }
   payload.push(0);
   stream
      .write_all(&((payload.len() + 4) as i32).to_be_bytes())
      .map_err(|error| error.to_string())?;
   stream
      .write_all(&payload)
      .map_err(|error| error.to_string())?;

   let mut reader = BufReader::new(stream.try_clone().unwrap());
   let mut scram: Option<ScramClient> = None;
   loop {
      let Some((tag, body)) = read_tagged(&mut reader) else {
         return Err("proxy closed during startup".to_string());
      };
      match tag {
         b'R' if body.len() >= 4 => {
            let code = i32::from_be_bytes([body[0], body[1], body[2], body[3]]);
            match code {
               0 => {}
               3 => {
                  let mut message = password.as_bytes().to_vec();
                  message.push(0);
                  write_msg(&mut stream, b'p', &message).map_err(|error| error.to_string())?;
               }
               10 => {
                  if !ScramClient::mechanism_supported(&body[4..]) {
                     return Err("proxy offered no SCRAM-SHA-256".to_string());
                  }
                  let client = ScramClient::new().map_err(|error| error.to_string())?;
                  let initial = client.initial_response();
                  let mut message = Vec::new();
                  message.extend_from_slice(b"SCRAM-SHA-256\0");
                  message.extend_from_slice(&(initial.len() as i32).to_be_bytes());
                  message.extend_from_slice(initial.as_bytes());
                  write_msg(&mut stream, b'p', &message).map_err(|error| error.to_string())?;
                  scram = Some(client);
               }
               11 => {
                  let server_first =
                     std::str::from_utf8(&body[4..]).map_err(|error| error.to_string())?;
                  let client = scram
                     .as_mut()
                     .ok_or_else(|| "unexpected SASL continue".to_string())?;
                  let final_message = client
                     .finish(server_first, password)
                     .map_err(|error| error.to_string())?;
                  write_msg(&mut stream, b'p', final_message.as_bytes())
                     .map_err(|error| error.to_string())?;
               }
               12 => {
                  let server_final =
                     std::str::from_utf8(&body[4..]).map_err(|error| error.to_string())?;
                  let client = scram
                     .as_ref()
                     .ok_or_else(|| "unexpected SASL final".to_string())?;
                  if !client.verify_server_final(server_final) {
                     return Err("SCRAM server signature mismatch".to_string());
                  }
               }
               other => return Err(format!("unsupported auth request {other}")),
            }
         }
         b'E' => return Err(error_message(&body)),
         b'Z' => break,
         _ => {}
      }
   }

   Ok(Client { stream, reader })
}

fn parse_data_row(body: &[u8]) -> Vec<Option<String>> {
   if body.len() < 2 {
      return Vec::new();
   }
   let count = i16::from_be_bytes([body[0], body[1]]) as usize;
   let mut offset = 2;
   let mut values = Vec::with_capacity(count);
   for _ in 0..count {
      let len = i32::from_be_bytes([
         body[offset],
         body[offset + 1],
         body[offset + 2],
         body[offset + 3],
      ]);
      offset += 4;
      if len < 0 {
         values.push(None);
      } else {
         let len = len as usize;
         values.push(Some(
            String::from_utf8_lossy(&body[offset..offset + len]).to_string(),
         ));
         offset += len;
      }
   }
   values
}

fn error_message(body: &[u8]) -> String {
   let mut pos = 0;
   while pos < body.len() {
      let field = body[pos];
      pos += 1;
      if field == 0 {
         break;
      }
      let end = body[pos..].iter().position(|&b| b == 0).unwrap_or(0);
      if field == b'M' {
         return String::from_utf8_lossy(&body[pos..pos + end]).to_string();
      }
      pos += end + 1;
   }
   String::new()
}

fn start(upstream: &str) -> (u16, Arc<DbCacheManager>, Arc<Store>) {
   start_with(DbProxyMode::Dual, Some(upstream))
}

fn start_with(mode: DbProxyMode, upstream: Option<&str>) -> (u16, Arc<DbCacheManager>, Arc<Store>) {
   start_configured(mode, upstream, AuthUsers::default(), 8)
}

fn start_configured(
   mode: DbProxyMode,
   upstream: Option<&str>,
   auth_users: AuthUsers,
   pool_max_idle: usize,
) -> (u16, Arc<DbCacheManager>, Arc<Store>) {
   let store = Arc::new(Store::new());
   let manager = Arc::new(DbCacheManager::new(store.clone()));
   let port = free_port();
   let config = DbCacheConfig {
      enabled: true,
      mode,
      upstream_addr: upstream.map(str::to_string),
      listen_addr: format!("127.0.0.1:{port}"),
      default_ttl_ms: 60_000,
      key_prefix: "db:".to_string(),
      flush_on_start: true,
      sync_interval_ms: 0,
      cache_max_keys: 100_000,
      ttl_jitter_pct: 0,
      auth_users,
      pool_max_idle,
      ..DbCacheConfig::default()
   };
   start_proxy_listener(config, Arc::clone(&manager)).expect("start proxy");
   (port, manager, store)
}

// Cache keys now mix the session-context hash, so tests locate entries by
// their table prefix instead of recomputing the raw fingerprint.
fn cached_keys(store: &Store, table: &str) -> Vec<String> {
   let prefix = format!("db:{table}:");
   let mut keys: Vec<String> = store
      .scan_prefix(&prefix)
      .into_iter()
      .map(|(key, _)| key)
      .collect();
   keys.sort();
   keys
}

fn cached_key(store: &Store, table: &str) -> String {
   let mut keys = cached_keys(store, table);
   assert_eq!(
      keys.len(),
      1,
      "expected one cached key for {table}: {keys:?}"
   );
   keys.remove(0)
}

fn expected_rows() -> Vec<Vec<Option<String>>> {
   ROWS
      .iter()
      .map(|(id, name)| vec![Some(id.to_string()), Some(name.to_string())])
      .collect()
}

// ------------------------------------------------------------------- tests

#[test]
fn simple_query_cache_invalidation_and_shared_store() {
   let (upstream, stats) = FakePg::start();
   let (port, _manager, store) = start(&upstream);
   let mut client = Client::connect(port);

   assert!(stats.password_ok.load(Ordering::SeqCst));

   let sql = "select id, username from users where id = 42";
   let first = client.simple(sql);
   assert!(first.error.is_none());
   assert_eq!(first.rows, expected_rows());
   assert_eq!(first.tags, vec!["SELECT 2".to_string()]);
   assert_eq!(stats.queries.load(Ordering::SeqCst), 1);

   // Second identical query is served from the wire cache.
   let second = client.simple(sql);
   assert_eq!(second.rows, expected_rows());
   assert_eq!(stats.queries.load(Ordering::SeqCst), 1);

   // The cached rowset is visible through the shared store (Redis port).
   let key = cached_key(&store, "users");
   let shared = store.get(&key).expect("cache key in shared store");
   assert!(shared.contains("\"username\":\"alpha\""));

   // A write through the proxy invalidates the table.
   let update = client.simple("update users set username = 'renamed' where id = 42");
   assert!(update.error.is_none());
   assert!(store.get(&key).is_none());
   let third = client.simple(sql);
   assert_eq!(third.rows, expected_rows());
   // first SELECT + UPDATE + refill; the update itself is a forwarded query.
   assert_eq!(stats.queries.load(Ordering::SeqCst), 3);

   // Deleting the key through the Redis protocol must also evict the wire cache.
   assert!(store.del(&key));
   let fourth = client.simple(sql);
   assert_eq!(fourth.rows, expected_rows());
   assert_eq!(stats.queries.load(Ordering::SeqCst), 4);
   assert!(store.get(&key).is_some());
}

#[test]
fn prepared_statements_cache_cursors_and_invalidation() {
   let (upstream, stats) = FakePg::start();
   let (port, _manager, store) = start(&upstream);
   let mut client = Client::connect(port);

   let sql = "select id, username from users where id = $1";
   let param = Some(b"42".to_vec());
   let parsed = parse_sql(sql);
   assert!(parsed.is_cacheable);

   // First execution reaches the upstream: Parse, Bind, Describe (cache fill), Execute.
   client.parse("s1", sql);
   client.bind("p1", "s1", std::slice::from_ref(&param));
   client.execute("p1", 0);
   client.sync();
   let reply = client.read_until_ready();
   assert!(reply.error.is_none());
   assert_eq!(reply.row_tags, vec![b'1', b'2', b'D', b'D', b'C', b'Z']);
   assert_eq!(reply.rows, expected_rows());
   assert_eq!(stats.parses.load(Ordering::SeqCst), 1);
   assert_eq!(stats.binds.load(Ordering::SeqCst), 1);
   assert_eq!(stats.executes.load(Ordering::SeqCst), 1);
   let key = cached_key(&store, "users");
   assert!(store.get(&key).is_some());

   // A second binding with the same parameters never reaches the upstream.
   client.bind("p2", "s1", std::slice::from_ref(&param));
   client.execute("p2", 0);
   client.sync();
   let reply = client.read_until_ready();
   assert_eq!(reply.row_tags, vec![b'2', b'D', b'D', b'C', b'Z']);
   assert_eq!(reply.rows, expected_rows());
   assert_eq!(stats.parses.load(Ordering::SeqCst), 1);
   assert_eq!(stats.binds.load(Ordering::SeqCst), 1);
   assert_eq!(stats.executes.load(Ordering::SeqCst), 1);

   // Describing a cached portal is synthesized locally.
   client.describe_portal("p2");
   client.sync();
   let reply = client.read_until_ready();
   assert_eq!(reply.row_tags, vec![b'T', b'Z']);
   assert_eq!(stats.describes.load(Ordering::SeqCst), 1);

   // Row-limited Execute suspends the portal and resumes from the cache.
   client.bind("p3", "s1", std::slice::from_ref(&param));
   client.execute("p3", 1);
   client.sync();
   let reply = client.read_until_ready();
   assert_eq!(reply.row_tags, vec![b'2', b'D', b's', b'Z']);
   assert_eq!(reply.rows.len(), 1);
   assert!(reply.suspended);

   client.execute("p3", 1);
   client.sync();
   let reply = client.read_until_ready();
   assert_eq!(reply.row_tags, vec![b'D', b'C', b'Z']);
   assert_eq!(reply.rows.len(), 1);
   assert_eq!(reply.tags, vec!["SELECT 2".to_string()]);
   assert_eq!(stats.binds.load(Ordering::SeqCst), 1);
   assert_eq!(stats.executes.load(Ordering::SeqCst), 1);

   // Evict through the shared store, then the next execution refetches.
   assert!(store.del(&key));
   client.bind("p4", "s1", std::slice::from_ref(&param));
   client.execute("p4", 0);
   client.sync();
   let reply = client.read_until_ready();
   assert_eq!(reply.rows, expected_rows());
   assert_eq!(stats.binds.load(Ordering::SeqCst), 2);
   assert_eq!(stats.executes.load(Ordering::SeqCst), 2);
   assert!(store.get(&key).is_some());
}

#[test]
fn multi_statement_and_cte_writes_invalidate() {
   let (upstream, stats) = FakePg::start();
   let (port, _manager, store) = start(&upstream);
   let mut client = Client::connect(port);

   let sql = "select id, username from users where id = 42";
   assert_eq!(client.simple(sql).rows, expected_rows());
   let key = cached_key(&store, "users");
   assert!(store.get(&key).is_some());

   // The first statement is a SELECT, but the trailing UPDATE must still
   // invalidate the users table.
   let mixed = client.simple(
      "select id, username from users where id = 42; update users set username = 'q' where id = 42",
   );
   assert!(mixed.error.is_none());
   assert!(store.get(&key).is_none());

   assert_eq!(client.simple(sql).rows, expected_rows());
   assert!(store.get(&key).is_some());

   // A data-modifying CTE is never cached and invalidates on the way through.
   let queries_before = stats.queries.load(Ordering::SeqCst);
   let cte = client.simple("with gone as (delete from users returning id) select * from gone");
   assert!(cte.error.is_none());
   assert_eq!(stats.queries.load(Ordering::SeqCst), queries_before + 1);
   assert!(store.get(&key).is_none());

   // Repeating it must reach the upstream again, never the cache.
   let cte_again =
      client.simple("with gone as (delete from users returning id) select * from gone");
   assert!(cte_again.error.is_none());
   assert_eq!(stats.queries.load(Ordering::SeqCst), queries_before + 2);
}

#[test]
fn commit_reinvalidates_tables_written_in_transaction() {
   let (upstream, stats) = FakePg::start();
   let (port, _manager, store) = start(&upstream);
   let mut writer = Client::connect(port);
   let mut reader = Client::connect(port);

   let sql = "select id, username from users where id = 42";

   assert!(writer.simple("begin").error.is_none());
   assert!(
      writer
         .simple("update users set username = 'pending' where id = 42")
         .error
         .is_none()
   );
   assert!(cached_keys(&store, "users").is_empty());

   // Another session fills the cache with pre-commit rows while the
   // transaction is still open.
   assert_eq!(reader.simple(sql).rows, expected_rows());
   let key = cached_key(&store, "users");
   assert!(store.get(&key).is_some());
   let queries_before = stats.queries.load(Ordering::SeqCst);

   // COMMIT must evict that entry again.
   assert!(writer.simple("commit").error.is_none());
   assert!(store.get(&key).is_none());

   // The next read misses and refetches.
   assert_eq!(reader.simple(sql).rows, expected_rows());
   assert!(store.get(&key).is_some());
   assert_eq!(stats.queries.load(Ordering::SeqCst), queries_before + 2);

   // A rolled-back transaction must not evict entries filled while it was
   // open.
   assert!(writer.simple("begin").error.is_none());
   assert!(
      writer
         .simple("update users set username = 'discard' where id = 42")
         .error
         .is_none()
   );
   assert_eq!(reader.simple(sql).rows, expected_rows());
   assert!(store.get(&key).is_some());
   assert!(writer.simple("rollback").error.is_none());
   assert!(store.get(&key).is_some());
}

#[test]
fn extended_commit_reinvalidates_transaction_writes() {
   let (upstream, _stats) = FakePg::start();
   let (port, _manager, store) = start(&upstream);
   let mut writer = Client::connect(port);
   let mut reader = Client::connect(port);

   let sql = "select id, username from users where id = 42";

   writer.parse("sb", "begin");
   writer.bind("pb", "sb", &[]);
   writer.execute("pb", 0);
   writer.sync();
   assert!(writer.read_until_ready().error.is_none());

   writer.parse("su", "update users set username = 'pending' where id = 42");
   writer.bind("pu", "su", &[]);
   writer.execute("pu", 0);
   writer.sync();
   assert!(writer.read_until_ready().error.is_none());

   assert_eq!(reader.simple(sql).rows, expected_rows());
   let key = cached_key(&store, "users");
   assert!(store.get(&key).is_some());

   writer.parse("sc", "commit");
   writer.bind("pc", "sc", &[]);
   writer.execute("pc", 0);
   writer.sync();
   assert!(writer.read_until_ready().error.is_none());
   assert!(store.get(&key).is_none());

   assert_eq!(reader.simple(sql).rows, expected_rows());
   assert!(store.get(&key).is_some());
}

#[test]
fn scram_auth_is_relayed_without_deadlock() {
   let (upstream, stats) = FakePg::start_scram();
   let (port, _manager, _store) = start(&upstream);
   let mut client = Client::connect(port);

   assert_eq!(stats.sasl_rounds.load(Ordering::SeqCst), 2);
   assert_eq!(
      client
         .simple("select id, username from users where id = 1")
         .rows,
      expected_rows()
   );
   assert_eq!(stats.queries.load(Ordering::SeqCst), 1);
}

#[test]
fn cache_only_mode_never_contacts_upstream() {
   let (upstream, stats) = FakePg::start();
   let (port, _manager, _store) = start_with(DbProxyMode::CacheOnly, Some(&upstream));
   let mut client = Client::connect(port);

   assert!(!stats.password_ok.load(Ordering::SeqCst));

   for _ in 0..2 {
      let reply = client.simple("select id, username from users where id = 42");
      assert!(reply.error.is_some());
      assert!(reply.error.unwrap().contains("no upstream"));
   }

   let queries_before = stats.queries.load(Ordering::SeqCst);
   client.parse("s1", "select id, username from users where id = $1");
   client.bind("p1", "s1", &[Some(b"42".to_vec())]);
   client.execute("p1", 0);
   client.sync();
   let reply = client.read_until_ready();
   assert!(reply.error.is_some());
   assert_eq!(stats.queries.load(Ordering::SeqCst), queries_before);
   assert_eq!(stats.parses.load(Ordering::SeqCst), 0);
   assert_eq!(stats.binds.load(Ordering::SeqCst), 0);
   assert_eq!(stats.executes.load(Ordering::SeqCst), 0);
}

#[test]
fn extended_write_invalidates_all_cached_shapes() {
   let (upstream, stats) = FakePg::start();
   let (port, _manager, store) = start(&upstream);
   let mut client = Client::connect(port);

   let simple_sql = "select id, username from users where id = 42";
   let prepared_sql = "select id, username from users where id = $1";
   let param = Some(b"42".to_vec());

   assert_eq!(client.simple(simple_sql).rows, expected_rows());
   let simple_key = cached_key(&store, "users");
   client.parse("sp", prepared_sql);
   client.bind("pp", "sp", std::slice::from_ref(&param));
   client.execute("pp", 0);
   client.sync();
   assert_eq!(client.read_until_ready().rows, expected_rows());
   assert!(store.get(&simple_key).is_some());
   let prepared_key = cached_keys(&store, "users")
      .into_iter()
      .find(|key| *key != simple_key)
      .expect("prepared statement entry is cached");
   assert!(store.get(&prepared_key).is_some());

   // The generic UPDATE invalidates the users table for every cached shape.
   client.parse("su", "update users set username = 'z' where id = $1");
   client.bind("pu", "su", &[param]);
   client.execute("pu", 0);
   client.sync();
   let reply = client.read_until_ready();
   assert!(reply.error.is_none());
   assert_eq!(reply.tags, vec!["UPDATE 1".to_string()]);
   assert!(store.get(&simple_key).is_none());
   assert!(store.get(&prepared_key).is_none());

   let queries_before = stats.queries.load(Ordering::SeqCst);
   let refetched = client.simple(simple_sql);
   assert_eq!(refetched.rows, expected_rows());
   assert_eq!(stats.queries.load(Ordering::SeqCst), queries_before + 1);
   assert!(store.get(&simple_key).is_some());
}

#[test]
fn session_context_separates_cache_entries() {
   let (upstream, stats) = FakePg::start();
   let (port, _manager, store) = start(&upstream);
   let mut alice = Client::connect(port);
   let mut bob = Client::connect(port);
   let mut carol = Client::connect(port);
   let sql = "select id, username from users where id = 42";

   // Two sessions with identical settings share one cache entry.
   assert_eq!(alice.simple(sql).rows, expected_rows());
   assert_eq!(stats.queries.load(Ordering::SeqCst), 1);
   assert_eq!(bob.simple(sql).rows, expected_rows());
   assert_eq!(stats.queries.load(Ordering::SeqCst), 1);
   assert_eq!(cached_keys(&store, "users").len(), 1);

   // A session that changed search_path must not share that entry.
   assert!(carol.simple("set search_path to public").error.is_none());
   assert_eq!(carol.simple(sql).rows, expected_rows());
   assert_eq!(stats.queries.load(Ordering::SeqCst), 3);
   assert_eq!(cached_keys(&store, "users").len(), 2);

   // Repeating the query in the same session hits its own entry.
   assert_eq!(carol.simple(sql).rows, expected_rows());
   assert_eq!(stats.queries.load(Ordering::SeqCst), 3);

   // A different session with the same SET text shares carol's entry.
   let mut dave = Client::connect(port);
   assert!(dave.simple("set search_path to public").error.is_none());
   assert_eq!(dave.simple(sql).rows, expected_rows());
   assert_eq!(stats.queries.load(Ordering::SeqCst), 4);
   assert_eq!(cached_keys(&store, "users").len(), 2);
}

#[test]
fn catalog_marks_stable_functions_uncacheable() {
   let (upstream, stats) = FakePg::start();
   let (port, _manager, store) = start(&upstream);
   let mut client = Client::connect(port);

   // The fake catalog reports my_stable_fn as STABLE, so results must never
   // be shared even though the SQL parser alone would cache the query.
   let functional = "select id, username, my_stable_fn() from users";
   assert_eq!(client.simple(functional).rows, expected_rows());
   assert_eq!(client.simple(functional).rows, expected_rows());
   assert_eq!(stats.queries.load(Ordering::SeqCst), 2);
   assert!(cached_keys(&store, "users").is_empty());

   // Function-free queries keep caching normally.
   let plain = "select id, username from users where id = 42";
   assert_eq!(client.simple(plain).rows, expected_rows());
   assert_eq!(client.simple(plain).rows, expected_rows());
   assert_eq!(stats.queries.load(Ordering::SeqCst), 3);
   assert_eq!(cached_keys(&store, "users").len(), 1);
}

#[test]
fn view_dependencies_invalidate_with_base_table() {
   let (upstream, stats) = FakePg::start();
   let (port, _manager, store) = start(&upstream);
   let mut client = Client::connect(port);

   let sql = "select id, username from user_view";
   assert_eq!(client.simple(sql).rows, expected_rows());
   assert_eq!(stats.queries.load(Ordering::SeqCst), 1);
   let key = cached_key(&store, "user_view");
   assert!(store.get(&key).is_some());

   assert_eq!(client.simple(sql).rows, expected_rows());
   assert_eq!(stats.queries.load(Ordering::SeqCst), 1);

   // Writing to the view's base table evicts the view result.
   let update = client.simple("update users set username = 'via base' where id = 42");
   assert!(update.error.is_none());
   assert!(store.get(&key).is_none());

   assert_eq!(client.simple(sql).rows, expected_rows());
   assert_eq!(stats.queries.load(Ordering::SeqCst), 3);
   assert!(store.get(&key).is_some());
}

#[test]
fn proxy_terminates_scram_and_reuses_pooled_upstream() {
   let (upstream, stats) = FakePg::start_with_auth(Auth::Cleartext);
   let users = AuthUsers::from_pairs([("alice", "secret")]);
   let (port, manager, _store) = start_configured(DbProxyMode::Dual, Some(&upstream), users, 4);

   // The proxy verifies the proof itself, so a bad password never reaches the
   // origin.
   let error = match connect_scram(port, "alice", "wrong") {
      Ok(_) => panic!("bad password accepted"),
      Err(error) => error,
   };
   assert!(error.contains("password authentication failed"), "{error}");
   assert_eq!(stats.connections.load(Ordering::SeqCst), 0);

   let mut first =
      connect_scram_with_app(port, "alice", "secret", Some("first-app")).expect("first login");
   assert_eq!(
      first
         .simple("select id, username from users where id = 1")
         .rows,
      expected_rows()
   );
   assert_eq!(stats.connections.load(Ordering::SeqCst), 1);
   first.send(b'X', &[]);
   drop(first);

   // The upstream session is reset and checked in; the idle gauge tells the
   // test when that completed so the next login deterministically reuses it.
   let deadline = Instant::now() + Duration::from_secs(5);
   while Instant::now() < deadline && manager.metrics().pool_idle.load(Ordering::Relaxed) == 0 {
      thread::sleep(Duration::from_millis(10));
   }
   assert_eq!(manager.metrics().pool_idle.load(Ordering::Relaxed), 1);

   let mut second =
      connect_scram_with_app(port, "alice", "secret", Some("second's app")).expect("pooled login");
   assert_eq!(
      second
         .simple("select id, username from users where id = 2")
         .rows,
      expected_rows()
   );
   assert_eq!(
      stats.connections.load(Ordering::SeqCst),
      1,
      "the authenticated upstream session must be reused"
   );
   assert!(stats.password_ok.load(Ordering::SeqCst));
   // DISCARD ALL clears the startup setting, so the checkout re-applies the
   // new client's application_name on the reused session.
   assert_eq!(
      stats.last_set.lock().unwrap().as_str(),
      "SET application_name = E'second\\'s app'"
   );
}
