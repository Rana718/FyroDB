use crate::protocol::{self, FrameReader};
use crate::row::{ColumnInfo, DbResultSet, DbRow, DbValue};
use std::io::{self, BufReader, Read, Write};
use std::net::{TcpStream, ToSocketAddrs};
use std::time::Duration;

const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(15);
const QUERY_TIMEOUT: Duration = Duration::from_secs(300);
const RELAY_FLUSH_BYTES: usize = 32 * 1024;
const MAX_CAPTURE_BYTES: usize = 4 << 20;

#[derive(Debug)]
pub enum HandshakeError {
   // The upstream could not be reached; nothing was sent to the client yet.
   Io(io::Error),
   // The upstream rejected the session and its error was already relayed.
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

// Result of relaying one backend message group. `data_rows` holds raw
// DataRow frames so a cache fill does not re-encode anything.
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

pub struct UpstreamConnection {
   writer: TcpStream,
   reader: FrameReader<BufReader<TcpStream>>,
   relay_buf: Vec<u8>,
}

impl UpstreamConnection {
   // Direct TCP connect without frontend auth relaying.
   pub fn connect(target: &str) -> io::Result<Self> {
      let addr = normalize_target(target);
      let socket_addr = addr
         .to_socket_addrs()?
         .next()
         .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "target address not found"))?;

      let writer = TcpStream::connect_timeout(&socket_addr, Duration::from_secs(5))?;
      writer.set_nodelay(true)?;
      let reader = FrameReader::new(BufReader::with_capacity(32 * 1024, writer.try_clone()?));
      Ok(Self {
         writer,
         reader,
         relay_buf: Vec::with_capacity(16 * 1024),
      })
   }

   // Opens the upstream connection and relays startup/authentication traffic
   // verbatim, so cleartext, MD5 and SCRAM auth all work unchanged.
   pub fn handshake<R: Read>(
      target: &str,
      client: &mut TcpStream,
      client_reader: &mut FrameReader<R>,
      startup_frame: &[u8],
   ) -> Result<Self, HandshakeError> {
      let addr = normalize_target(target);
      let socket_addr = addr
         .to_socket_addrs()?
         .next()
         .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "target address not found"))?;

      let mut writer = TcpStream::connect_timeout(&socket_addr, Duration::from_secs(5))?;
      writer.set_nodelay(true)?;
      writer.set_read_timeout(Some(HANDSHAKE_TIMEOUT))?;
      writer.write_all(startup_frame)?;
      writer.flush()?;

      let mut reader = FrameReader::new(BufReader::with_capacity(32 * 1024, writer.try_clone()?));
      loop {
         let tag = reader.read_raw()?;
         match tag {
            b'R' => {
               let body = reader.last_body();
               let code = if body.len() >= 4 {
                  i32::from_be_bytes([body[0], body[1], body[2], body[3]])
               } else {
                  -1
               };
               client.write_all(reader.last_frame())?;
               client.flush()?;
               // Codes 3 (cleartext), 5 (MD5), 10 (SASL initial) and 11
               // (SASL continue) expect a frontend reply. SASLFinal (12) and
               // the other mechanisms are server-to-client only.
               if matches!(code, 3 | 5 | 10 | 11) {
                  // Relay the client's password/SASL response untouched.
                  let client_tag = client_reader.read_raw()?;
                  if client_tag == b'X' {
                     return Err(HandshakeError::Rejected);
                  }
                  writer.write_all(client_reader.last_frame())?;
                  writer.flush()?;
               }
            }
            b'E' => {
               client.write_all(reader.last_frame())?;
               client.flush()?;
               return Err(HandshakeError::Rejected);
            }
            b'Z' => {
               client.write_all(reader.last_frame())?;
               client.flush()?;
               writer.set_read_timeout(Some(QUERY_TIMEOUT))?;
               return Ok(Self {
                  writer,
                  reader,
                  relay_buf: Vec::with_capacity(16 * 1024),
               });
            }
            _ => {
               client.write_all(reader.last_frame())?;
            }
         }
      }
   }

   // Sends a raw frontend message upstream.
   pub fn send_raw(&mut self, frame: &[u8]) -> io::Result<()> {
      self.writer.write_all(frame)
   }

   // Sends a message followed by Flush. PostgreSQL buffers extended-protocol
   // replies until Sync/Flush, so the proxy must request a flush before it can
   // read the reply for a single forwarded message.
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
         let frame = self.reader.last_frame();
         self.relay_buf.extend_from_slice(frame);
         if self.relay_buf.len() >= RELAY_FLUSH_BYTES {
            sink.write_all(&self.relay_buf)?;
            self.relay_buf.clear();
         }

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

      if !self.relay_buf.is_empty() {
         sink.write_all(&self.relay_buf)?;
         self.relay_buf.clear();
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
   if let Some(stripped) = target
      .strip_prefix("postgres://")
      .or_else(|| target.strip_prefix("postgresql://"))
   {
      let after_auth = stripped.rsplit('@').next().unwrap_or(stripped);
      let host_port = after_auth.split('/').next().unwrap_or(after_auth);
      if host_port.contains(':') {
         host_port.to_string()
      } else {
         format!("{host_port}:5432")
      }
   } else if !target.contains(':') {
      format!("{target}:5432")
   } else {
      target.to_string()
   }
}

fn command_tag(body: &[u8]) -> String {
   let tag = body.strip_suffix(&[0]).unwrap_or(body);
   String::from_utf8_lossy(tag).to_string()
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
      let name = String::from_utf8_lossy(&data[offset..offset + end]).to_string();
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
         let text = String::from_utf8_lossy(&data[offset..offset + len]).to_string();
         offset += len;
         values.push(DbValue::Text(text));
      }
   }

   Ok(DbRow { values })
}
