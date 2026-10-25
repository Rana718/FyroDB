use crate::config::{SslMode, UpstreamTarget};
use crate::protocol::{self, FrameReader};
use crate::row::{ColumnInfo, DbResultSet, DbRow, DbValue};
use parking_lot::Mutex;
use std::io::{self, BufReader, Read, Write};
use std::net::{TcpStream, ToSocketAddrs};
use std::sync::Arc;
use std::time::Duration;

const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(15);
const QUERY_TIMEOUT: Duration = Duration::from_secs(300);
const MAX_CAPTURE_BYTES: usize = 4 << 20;

#[derive(Debug)]
pub enum HandshakeError {
   Io(io::Error),
   Rejected,
}

impl From<io::Error> for HandshakeError {
   fn from(error: io::Error) -> Self {
      Self::Io(error)
   }
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum ReplyStop {
   Parse,
   Bind,
   Describe,
   Execute,
   Sync,
   Close,
   FunctionCall,
}

#[derive(Default)]
pub struct RelayOutcome {
   pub error: bool,
   pub status: u8,
   pub command_tag: Option<String>,
   pub row_description: Option<Vec<u8>>,
   pub data_rows: Vec<u8>,
   pub data_rows_bytes: usize,
   pub capture_overflow: bool,
   pub copy_in: bool,
   pub copy_out: bool,
   pub suspended: bool,
}

pub enum TransportStream {
   Plain(TcpStream),
   Tls(native_tls::TlsStream<TcpStream>),
}

impl TransportStream {
   pub fn set_read_timeout(&self, timeout: Option<Duration>) -> io::Result<()> {
      match self {
         Self::Plain(s) => s.set_read_timeout(timeout),
         Self::Tls(s) => s.get_ref().set_read_timeout(timeout),
      }
   }
}

impl Read for TransportStream {
   fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
      match self {
         Self::Plain(s) => s.read(buf),
         Self::Tls(s) => s.read(buf),
      }
   }
}

impl Write for TransportStream {
   fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
      match self {
         Self::Plain(s) => s.write(buf),
         Self::Tls(s) => s.write(buf),
      }
   }

   fn flush(&mut self) -> io::Result<()> {
      match self {
         Self::Plain(s) => s.flush(),
         Self::Tls(s) => s.flush(),
      }
   }
}

#[derive(Clone)]
pub struct SharedTransport(Arc<Mutex<TransportStream>>);

impl SharedTransport {
   pub fn set_read_timeout(&self, timeout: Option<Duration>) -> io::Result<()> {
      self.0.lock().set_read_timeout(timeout)
   }
}

impl Read for SharedTransport {
   fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
      self.0.lock().read(buf)
   }
}

impl Write for SharedTransport {
   fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
      self.0.lock().write(buf)
   }

   fn flush(&mut self) -> io::Result<()> {
      self.0.lock().flush()
   }
}

pub struct UpstreamConnection {
   writer: SharedTransport,
   reader: FrameReader<BufReader<SharedTransport>>,
   auth_ok: Vec<u8>,
   post_auth: Vec<u8>,
   required_password: bool,
}

impl UpstreamConnection {
   // Direct connect without frontend auth relaying.
   pub fn connect(target: &str) -> io::Result<Self> {
      let connection = Self::open(target)?;
      connection.writer.set_read_timeout(Some(QUERY_TIMEOUT))?;
      Ok(connection)
   }

   fn open(target: &str) -> io::Result<Self> {
      let parsed = UpstreamTarget::parse(target);
      let socket_addr = parsed
         .addr()
         .to_socket_addrs()?
         .next()
         .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "target address not found"))?;

      let mut tcp = TcpStream::connect_timeout(&socket_addr, Duration::from_secs(5))?;
      tcp.set_nodelay(true)?;

      let transport = if parsed.sslmode != SslMode::Disable {
         tcp.write_all(&protocol::SSL_REQUEST)?;
         tcp.flush()?;
         let mut resp = [0u8; 1];
         tcp.read_exact(&mut resp)?;
         if resp[0] == b'S' {
            let connector = native_tls::TlsConnector::builder()
               .build()
               .map_err(io::Error::other)?;
            let tls = connector
               .connect(&parsed.host, tcp)
               .map_err(|e| io::Error::new(io::ErrorKind::ConnectionRefused, e))?;
            TransportStream::Tls(tls)
         } else if parsed.sslmode == SslMode::Require {
            return Err(io::Error::new(
               io::ErrorKind::ConnectionRefused,
               "upstream rejected SSL connection (sslmode=require)",
            ));
         } else {
            TransportStream::Plain(tcp)
         }
      } else {
         TransportStream::Plain(tcp)
      };

      let shared = SharedTransport(Arc::new(Mutex::new(transport)));
      let reader = FrameReader::new(BufReader::with_capacity(32 * 1024, shared.clone()));
      Ok(Self {
         writer: shared,
         reader,
         auth_ok: Vec::new(),
         post_auth: Vec::new(),
         required_password: false,
      })
   }

   // Opens a session with the proxy's own credentials instead of relaying a
   // client exchange. This is what makes cross-session pooling possible.
   pub fn connect_authenticated(
      target: &str,
      startup_frame: &[u8],
      user: &str,
      password: &str,
   ) -> io::Result<Self> {
      let mut connection = Self::open(target)?;
      connection
         .writer
         .set_read_timeout(Some(HANDSHAKE_TIMEOUT))?;
      connection.writer.write_all(startup_frame)?;
      connection.writer.flush()?;
      connection.authenticate(user, password)?;
      connection.writer.set_read_timeout(Some(QUERY_TIMEOUT))?;
      Ok(connection)
   }

   fn authenticate(&mut self, user: &str, password: &str) -> io::Result<()> {
      let mut scram: Option<crate::auth::ScramClient> = None;
      loop {
         let tag = self.reader.read_raw()?;
         match tag {
            b'R' => {
               let body = self.reader.last_body().to_vec();
               let code = if body.len() >= 4 {
                  i32::from_be_bytes([body[0], body[1], body[2], body[3]])
               } else {
                  -1
               };
               match code {
                  0 => {
                     self.auth_ok.extend_from_slice(self.reader.last_frame());
                  }
                  3 => {
                     let mut message = password.as_bytes().to_vec();
                     message.push(0);
                     put_frontend(&mut self.writer, b'p', &message)?;
                  }
                  5 => {
                     let salt = body.get(4..8).ok_or_else(|| {
                        io::Error::new(io::ErrorKind::InvalidData, "invalid MD5 salt")
                     })?;
                     let mut message = crate::auth::md5_response(password, user, salt).into_bytes();
                     message.push(0);
                     put_frontend(&mut self.writer, b'p', &message)?;
                  }
                  10 => {
                     if !crate::auth::ScramClient::mechanism_supported(&body[4..]) {
                        return Err(io::Error::new(
                           io::ErrorKind::NotFound,
                           "upstream offered no supported SASL mechanism",
                        ));
                     }
                     let client = crate::auth::ScramClient::new()?;
                     let initial = client.initial_response();
                     let mut message = Vec::new();
                     message.extend_from_slice(crate::auth::SCRAM_MECHANISM.as_bytes());
                     message.push(0);
                     message.extend_from_slice(&(initial.len() as i32).to_be_bytes());
                     message.extend_from_slice(initial.as_bytes());
                     put_frontend(&mut self.writer, b'p', &message)?;
                     scram = Some(client);
                  }
                  11 => {
                     let client = scram
                        .as_mut()
                        .ok_or_else(|| io::Error::other("unexpected SASL continue"))?;
                     let server_first = std::str::from_utf8(&body[4..]).map_err(|_| {
                        io::Error::new(io::ErrorKind::InvalidData, "invalid SASL message")
                     })?;
                     let final_message = client.finish(server_first, password)?;
                     put_frontend(&mut self.writer, b'p', final_message.as_bytes())?;
                  }
                  12 => {
                     let server_final = std::str::from_utf8(&body[4..]).map_err(|_| {
                        io::Error::new(io::ErrorKind::InvalidData, "invalid SASL final")
                     })?;
                     if !scram
                        .as_ref()
                        .is_some_and(|client| client.verify_server_final(server_final))
                     {
                        return Err(io::Error::new(
                           io::ErrorKind::InvalidData,
                           "upstream SCRAM server signature mismatch",
                        ));
                     }
                  }
                  other => {
                     return Err(io::Error::other(format!(
                        "unsupported upstream authentication method {other}"
                     )));
                  }
               }
            }
            b'E' => {
               let message = error_message(self.reader.last_body());
               return Err(io::Error::new(
                  io::ErrorKind::PermissionDenied,
                  format!("upstream rejected the proxy credentials: {message}"),
               ));
            }
            b'S' | b'K' => self.post_auth.extend_from_slice(self.reader.last_frame()),
            b'Z' => {
               self.post_auth.extend_from_slice(self.reader.last_frame());
               return Ok(());
            }
            _ => {}
         }
      }
   }

   // True when the upstream demanded a password during the relayed handshake,
   // which means the connection can only be reused after re-authenticating.
   pub fn required_password(&self) -> bool {
      self.required_password
   }

   // Replays the origin's startup metadata to a new client. `include_auth_ok`
   // is false when the proxy already authenticated the client itself.
   pub fn write_replay<W: Write>(&self, writer: &mut W, include_auth_ok: bool) -> io::Result<()> {
      if include_auth_ok {
         writer.write_all(&self.auth_ok)?;
      }
      writer.write_all(&self.post_auth)?;
      writer.flush()
   }

   // Resets session state before returning a connection to the pool.
   pub fn reset_session(&mut self) -> io::Result<()> {
      let outcome = self.forward_query("DISCARD ALL", &mut Vec::new(), false)?;
      if outcome.error || outcome.status != b'I' {
         return Err(io::Error::other("upstream session reset failed"));
      }
      Ok(())
   }

   // Restores session settings that `DISCARD ALL` clears. Only settings the
   // pool key guarantees are re-appliable are permitted to reach this path.
   pub fn apply_session_params(&mut self, application_name: Option<&str>) -> io::Result<()> {
      let Some(name) = application_name else {
         return Ok(());
      };
      let mut escaped = String::with_capacity(name.len());
      for ch in name.chars() {
         if ch == '\\' || ch == '\'' {
            escaped.push('\\');
         }
         escaped.push(ch);
      }
      let outcome = self.forward_query(
         &format!("SET application_name = E'{escaped}'"),
         &mut Vec::new(),
         false,
      )?;
      if outcome.error || outcome.status != b'I' {
         return Err(io::Error::other("failed to restore session settings"));
      }
      Ok(())
   }

   // Cheap liveness probe for a pooled connection.
   pub fn ping(&mut self) -> io::Result<()> {
      let outcome = self.forward_query("", &mut Vec::new(), false)?;
      if outcome.error || outcome.status != b'I' {
         return Err(io::Error::other("pooled upstream connection is not idle"));
      }
      Ok(())
   }

   // Relays startup and authentication frames verbatim upstream.
   pub fn handshake<R: Read>(
      target: &str,
      client: &mut TcpStream,
      client_reader: &mut FrameReader<R>,
      startup_frame: &[u8],
   ) -> Result<Self, HandshakeError> {
      let mut connection = Self::open(target)?;
      connection
         .writer
         .set_read_timeout(Some(HANDSHAKE_TIMEOUT))?;
      connection.writer.write_all(startup_frame)?;
      connection.writer.flush()?;

      let mut auth_ok = Vec::new();
      let mut post_auth = Vec::new();
      let mut required_password = false;
      loop {
         let tag = connection.reader.read_raw()?;
         match tag {
            b'R' => {
               let body = connection.reader.last_body();
               let code = if body.len() >= 4 {
                  i32::from_be_bytes([body[0], body[1], body[2], body[3]])
               } else {
                  -1
               };
               if code == 0 {
                  auth_ok.extend_from_slice(connection.reader.last_frame());
               }
               client.write_all(connection.reader.last_frame())?;
               client.flush()?;
               if !matches!(code, 0 | 12) {
                  required_password = true;
                  let client_tag = client_reader.read_raw()?;
                  if client_tag == b'X' {
                     return Err(HandshakeError::Rejected);
                  }
                  connection.writer.write_all(client_reader.last_frame())?;
                  connection.writer.flush()?;
               }
            }
            b'E' => {
               client.write_all(connection.reader.last_frame())?;
               client.flush()?;
               return Err(HandshakeError::Rejected);
            }
            b'Z' => {
               post_auth.extend_from_slice(connection.reader.last_frame());
               client.write_all(connection.reader.last_frame())?;
               client.flush()?;
               connection.writer.set_read_timeout(Some(QUERY_TIMEOUT))?;
               connection.auth_ok = auth_ok;
               connection.post_auth = post_auth;
               connection.required_password = required_password;
               return Ok(connection);
            }
            b'S' | b'K' => {
               post_auth.extend_from_slice(connection.reader.last_frame());
               client.write_all(connection.reader.last_frame())?;
            }
            _ => {
               client.write_all(connection.reader.last_frame())?;
            }
         }
      }
   }

   // Sends a raw frontend message upstream.
   pub fn send_raw(&mut self, frame: &[u8]) -> io::Result<()> {
      self.writer.write_all(frame)
   }

   // Sends message and flushes buffered replies from PostgreSQL backend.
   pub fn send_and_flush(&mut self, frame: &[u8]) -> io::Result<()> {
      self.writer.write_all(frame)?;
      self.request_flush()
   }

   // Asks the backend to release buffered replies without ending the cycle.
   pub fn request_flush(&mut self) -> io::Result<()> {
      self.writer.write_all(&protocol::FLUSH_REQUEST)?;
      self.writer.flush()
   }

   pub fn flush(&mut self) -> io::Result<()> {
      self.writer.flush()
   }

   // Forwards raw query text upstream and relays the response to sink.
   pub fn forward_raw<W: Write>(&mut self, sql: &str, sink: &mut W) -> io::Result<RelayOutcome> {
      self.forward_query(sql, sink, false)
   }

   // Sends a simple query and relays the response, accumulating writes into
   // large chunks instead of one syscall per backend message.
   pub fn forward_query<W: Write>(
      &mut self,
      sql: &str,
      sink: &mut W,
      capture: bool,
   ) -> io::Result<RelayOutcome> {
      let mut payload = Vec::with_capacity(sql.len() + 1);
      payload.extend_from_slice(sql.as_bytes());
      payload.push(0);
      let mut frame = Vec::with_capacity(payload.len() + 5);
      protocol::put_message(&mut frame, b'Q', &payload);
      self.writer.write_all(&frame)?;
      self.relay_until(sink, ReplyStop::Sync, capture)
   }

   // Relays backend messages until the reply for `stop` completes.
   pub fn relay_until<W: Write>(
      &mut self,
      sink: &mut W,
      stop: ReplyStop,
      capture: bool,
   ) -> io::Result<RelayOutcome> {
      let mut outcome = RelayOutcome {
         status: b'I',
         ..RelayOutcome::default()
      };
      let mut capturing = capture;

      loop {
         let tag = self.reader.read_raw()?;
         sink.write_all(self.reader.last_frame())?;

         let terminal = match tag {
            b'N' | b'A' | b'S' | b'K' => false,
            b'T' => {
               if capturing {
                  outcome.row_description = Some(self.reader.last_frame().to_vec());
               }
               matches!(stop, ReplyStop::Describe)
            }
            b'D' => {
               if capturing && !outcome.copy_out {
                  let f_len = self.reader.last_frame().len();
                  if outcome.data_rows_bytes + f_len > MAX_CAPTURE_BYTES {
                     capturing = false;
                     outcome.capture_overflow = true;
                     outcome.data_rows.clear();
                     outcome.data_rows_bytes = 0;
                  } else {
                     outcome
                        .data_rows
                        .extend_from_slice(self.reader.last_frame());
                     outcome.data_rows_bytes += f_len;
                  }
               }
               false
            }
            b'C' => {
               outcome.command_tag = Some(command_tag(self.reader.last_body()));
               matches!(stop, ReplyStop::Execute)
            }
            b'1' => matches!(stop, ReplyStop::Parse),
            b'2' => matches!(stop, ReplyStop::Bind),
            b'3' => matches!(stop, ReplyStop::Close),
            b'n' => matches!(stop, ReplyStop::Describe),
            b'I' => matches!(stop, ReplyStop::Execute),
            b's' => {
               outcome.suspended = true;
               matches!(stop, ReplyStop::Execute)
            }
            b'V' => matches!(stop, ReplyStop::FunctionCall),
            b'G' => {
               outcome.copy_in = true;
               true
            }
            b'H' => {
               outcome.capture_overflow = true;
               capturing = false;
               outcome.data_rows.clear();
               outcome.copy_out = true;
               false
            }
            b'E' => {
               outcome.error = true;
               !matches!(stop, ReplyStop::Sync)
            }
            b'Z' => {
               let body = self.reader.last_body();
               if body.len() == 1 {
                  outcome.status = body[0];
               }
               true
            }
            _ => false,
         };

         if terminal {
            break;
         }
      }

      Ok(outcome)
   }

   // Executes a cacheable SELECT and parses the returned text rowset.
   pub fn execute_select(&mut self, sql: &str) -> io::Result<DbResultSet> {
      let mut payload = Vec::with_capacity(sql.len() + 1);
      payload.extend_from_slice(sql.as_bytes());
      payload.push(0);
      let mut frame = Vec::with_capacity(payload.len() + 5);
      protocol::put_message(&mut frame, b'Q', &payload);
      self.writer.write_all(&frame)?;

      let mut columns = Vec::new();
      let mut rows = Vec::new();
      let mut failed = false;
      loop {
         let tag = self.reader.read_raw()?;
         match tag {
            b'T' => columns = parse_row_description(self.reader.last_body())?,
            b'D' => rows.push(parse_data_row(self.reader.last_body())?),
            b'E' => {
               // Drain the idle status that follows the error, otherwise the
               // next internal query would read a stale message.
               failed = true;
            }
            b'Z' => break,
            _ => {}
         }
      }
      if failed {
         return Err(io::Error::other("upstream query error"));
      }

      Ok(DbResultSet {
         columns,
         rows_affected: rows.len() as u64,
         rows,
      })
   }
}

pub fn send_cancel(target: &str, frame: &[u8]) {
   let addr = normalize_target(target);
   let Ok(mut addrs) = addr.to_socket_addrs() else {
      return;
   };
   let Some(socket_addr) = addrs.next() else {
      return;
   };
   if let Ok(mut stream) = TcpStream::connect_timeout(&socket_addr, Duration::from_secs(2)) {
      let _ = stream.write_all(frame);
      let _ = stream.flush();
   }
}

fn normalize_target(target: &str) -> String {
   UpstreamTarget::parse(target).addr()
}

fn command_tag(body: &[u8]) -> String {
   let tag = body.strip_suffix(&[0]).unwrap_or(body);
   std::str::from_utf8(tag).unwrap_or("").to_string()
}

fn put_frontend<W: Write>(writer: &mut W, tag: u8, body: &[u8]) -> io::Result<()> {
   writer.write_all(&[tag])?;
   writer.write_all(&((body.len() + 4) as i32).to_be_bytes())?;
   writer.write_all(body)?;
   writer.flush()
}

fn error_message(body: &[u8]) -> String {
   let mut pos = 0;
   while pos < body.len() {
      let field = body[pos];
      pos += 1;
      if field == 0 {
         break;
      }
      let Some(end) = body[pos..].iter().position(|byte| *byte == 0) else {
         break;
      };
      if field == b'M' {
         return std::str::from_utf8(&body[pos..pos + end])
            .unwrap_or("")
            .to_string();
      }
      pos += end + 1;
   }
   String::new()
}

pub fn parse_row_description(data: &[u8]) -> io::Result<Vec<ColumnInfo>> {
   if data.len() < 2 {
      return Ok(Vec::new());
   }
   let count = i16::from_be_bytes([data[0], data[1]]) as usize;
   let mut cols = Vec::with_capacity(count);
   let mut offset = 2;

   for _ in 0..count {
      let end = data[offset..]
         .iter()
         .position(|&b| b == 0)
         .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "invalid column name"))?;
      let name = std::str::from_utf8(&data[offset..offset + end])
         .unwrap_or("")
         .to_string();
      offset += end + 1 + 6; // name + null + table_oid (4) + col_attr (2)

      if offset + 4 > data.len() {
         break;
      }
      let type_oid = u32::from_be_bytes([
         data[offset],
         data[offset + 1],
         data[offset + 2],
         data[offset + 3],
      ]);
      offset += 12; // type_oid (4) + type_len (2) + type_mod (4) + format (2)
      cols.push(ColumnInfo { name, type_oid });
   }

   Ok(cols)
}

pub fn parse_data_row(data: &[u8]) -> io::Result<DbRow> {
   if data.len() < 2 {
      return Ok(DbRow { values: Vec::new() });
   }
   let count = i16::from_be_bytes([data[0], data[1]]) as usize;
   let mut values = Vec::with_capacity(count);
   let mut offset = 2;

   for _ in 0..count {
      if offset + 4 > data.len() {
         break;
      }
      let len = i32::from_be_bytes([
         data[offset],
         data[offset + 1],
         data[offset + 2],
         data[offset + 3],
      ]);
      offset += 4;

      if len < 0 {
         values.push(DbValue::Null);
      } else {
         let len = len as usize;
         if offset + len > data.len() {
            break;
         }
         let text = std::str::from_utf8(&data[offset..offset + len])
            .unwrap_or("")
            .to_string();
         offset += len;
         values.push(DbValue::Text(text));
      }
   }

   Ok(DbRow { values })
}
