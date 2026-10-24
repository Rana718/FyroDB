use crate::row::{ColumnInfo, DbValue};
use std::io::{self, Read, Write};

pub const SSL_REQUEST_CODE: i32 = 80877103;
pub const GSSENC_REQUEST_CODE: i32 = 80877104;
pub const CANCEL_REQUEST_CODE: i32 = 80877102;
pub const PROTOCOL_VERSION_3: i32 = 196608;

const MAX_STARTUP_LEN: usize = 1 << 20;
const MAX_MESSAGE_LEN: usize = 64 << 20;

#[derive(Debug, PartialEq, Eq)]
pub enum FrontendMessage {
   SslRequest,
   GssEncRequest,
   CancelRequest {
      pid: i32,
      secret: i32,
   },
   Startup {
      user: String,
      database: String,
      params: Vec<(String, String)>,
   },
   Query(String),
   Parse {
      name: String,
      sql: String,
      param_oids: Vec<u32>,
   },
   Bind {
      portal: String,
      statement: String,
      param_formats: Vec<i16>,
      params: Vec<Option<Vec<u8>>>,
      result_formats: Vec<i16>,
   },
   Describe {
      kind: u8,
      name: String,
   },
   Execute {
      portal: String,
      max_rows: i32,
   },
   Sync,
   Flush,
   Close {
      kind: u8,
      name: String,
   },
   Terminate,
   Unknown(u8),
}

impl FrontendMessage {
   pub fn startup_param(&self, name: &str) -> Option<&str> {
      match self {
         Self::Startup { params, .. } => params
            .iter()
            .find(|(key, _)| key.eq_ignore_ascii_case(name))
            .map(|(_, value)| value.as_str()),
         _ => None,
      }
   }
}

// Buffered PostgreSQL frame reader. Keeps the last raw frame so callers can
// forward it verbatim without re-encoding.
pub struct FrameReader<R> {
   inner: R,
   frame: Vec<u8>,
}

impl<R: Read> FrameReader<R> {
   pub fn new(inner: R) -> Self {
      Self {
         inner,
         frame: Vec::with_capacity(8192),
      }
   }

   pub fn last_frame(&self) -> &[u8] {
      &self.frame
   }

   pub fn last_body(&self) -> &[u8] {
      if self.frame.len() >= 5 {
         &self.frame[5..]
      } else {
         &[]
      }
   }

   pub fn into_inner(self) -> R {
      self.inner
   }

   pub fn read_frontend(&mut self, initial: bool) -> io::Result<FrontendMessage> {
      if initial {
         self.read_initial()
      } else {
         self.read_regular()
      }
   }

   // Reads any tagged message and returns its tag.
   pub fn read_raw(&mut self) -> io::Result<u8> {
      self.frame.clear();
      self.frame.resize(5, 0);
      self.inner.read_exact(&mut self.frame[..5])?;
      let tag = self.frame[0];
      let len = i32::from_be_bytes([self.frame[1], self.frame[2], self.frame[3], self.frame[4]]);
      if len < 4 || len as usize > MAX_MESSAGE_LEN {
         return Err(invalid("invalid message length"));
      }
      self.frame.resize(1 + len as usize, 0);
      self.inner.read_exact(&mut self.frame[5..])?;
      Ok(tag)
   }

   fn read_initial(&mut self) -> io::Result<FrontendMessage> {
      self.frame.clear();
      self.frame.resize(4, 0);
      self.inner.read_exact(&mut self.frame[..4])?;
      let len = i32::from_be_bytes([self.frame[0], self.frame[1], self.frame[2], self.frame[3]]);
      if len < 8 || len as usize > MAX_STARTUP_LEN {
         return Err(invalid("invalid startup packet length"));
      }
      self.frame.resize(len as usize, 0);
      self.inner.read_exact(&mut self.frame[4..])?;

      let code = rd_i32_at(&self.frame, 4)?;
      match code {
         SSL_REQUEST_CODE => return Ok(FrontendMessage::SslRequest),
         GSSENC_REQUEST_CODE => return Ok(FrontendMessage::GssEncRequest),
         CANCEL_REQUEST_CODE => {
            if self.frame.len() < 16 {
               return Err(invalid("invalid cancel request"));
            }
            return Ok(FrontendMessage::CancelRequest {
               pid: rd_i32_at(&self.frame, 8)?,
               secret: rd_i32_at(&self.frame, 12)?,
            });
         }
         version if version >> 16 != 3 => {
            return Err(invalid("unsupported frontend protocol version"));
         }
         _ => {}
      }

      let mut params = Vec::new();
      let mut pos = 8;
      while pos < self.frame.len() {
         let key = take_cstr(&self.frame, &mut pos)?;
         if key.is_empty() {
            break;
         }
         let value = take_cstr(&self.frame, &mut pos)?;
         params.push((key.to_string(), value.to_string()));
      }

      let mut user = String::new();
      let mut database = String::new();
      for (key, value) in &params {
         if key.eq_ignore_ascii_case("user") {
            user.clone_from(value);
         } else if key.eq_ignore_ascii_case("database") {
            database.clone_from(value);
         }
      }

      Ok(FrontendMessage::Startup {
         user,
         database,
         params,
      })
   }

   fn read_regular(&mut self) -> io::Result<FrontendMessage> {
      self.frame.clear();
      self.frame.resize(5, 0);
      self.inner.read_exact(&mut self.frame[..5])?;
      let tag = self.frame[0];
      let len = i32::from_be_bytes([self.frame[1], self.frame[2], self.frame[3], self.frame[4]]);
      if len < 4 || len as usize > MAX_MESSAGE_LEN {
         return Err(invalid("invalid message length"));
      }
      self.frame.resize(1 + len as usize, 0);
      self.inner.read_exact(&mut self.frame[5..])?;
      let body = &self.frame[5..];

      match tag {
         b'Q' => {
            let sql = body.strip_suffix(&[0]).unwrap_or(body);
            Ok(FrontendMessage::Query(
               String::from_utf8_lossy(sql).to_string(),
            ))
         }
         b'P' => {
            let mut pos = 0;
            let name = take_cstr(body, &mut pos)?.to_string();
            let sql = take_cstr(body, &mut pos)?.to_string();
            let count = rd_i16(body, &mut pos)?.max(0) as usize;
            let mut param_oids = Vec::with_capacity(count);
            for _ in 0..count {
               param_oids.push(rd_u32(body, &mut pos)?);
            }
            Ok(FrontendMessage::Parse {
               name,
               sql,
               param_oids,
            })
         }
         b'B' => {
            let mut pos = 0;
            let portal = take_cstr(body, &mut pos)?.to_string();
            let statement = take_cstr(body, &mut pos)?.to_string();

            let fmt_count = rd_i16(body, &mut pos)?.max(0) as usize;
            let mut param_formats = Vec::with_capacity(fmt_count);
            for _ in 0..fmt_count {
               param_formats.push(rd_i16(body, &mut pos)?);
            }

            let param_count = rd_i16(body, &mut pos)?.max(0) as usize;
            let mut params = Vec::with_capacity(param_count);
            for _ in 0..param_count {
               let len = rd_i32(body, &mut pos)?;
               if len < 0 {
                  params.push(None);
               } else {
                  let len = len as usize;
                  if pos + len > body.len() {
                     return Err(invalid("invalid bind parameter"));
                  }
                  params.push(Some(body[pos..pos + len].to_vec()));
                  pos += len;
               }
            }

            let res_count = rd_i16(body, &mut pos)?.max(0) as usize;
            let mut result_formats = Vec::with_capacity(res_count);
            for _ in 0..res_count {
               result_formats.push(rd_i16(body, &mut pos)?);
            }

            Ok(FrontendMessage::Bind {
               portal,
               statement,
               param_formats,
               params,
               result_formats,
            })
         }
         b'D' | b'C' => {
            if body.is_empty() {
               return Err(invalid("invalid describe/close message"));
            }
            let kind = body[0];
            let mut pos = 1;
            let name = take_cstr(body, &mut pos)?.to_string();
            if tag == b'D' {
               Ok(FrontendMessage::Describe { kind, name })
            } else {
               Ok(FrontendMessage::Close { kind, name })
            }
         }
         b'E' => {
            let mut pos = 0;
            let portal = take_cstr(body, &mut pos)?.to_string();
            let max_rows = rd_i32(body, &mut pos)?;
            Ok(FrontendMessage::Execute { portal, max_rows })
         }
         b'S' => Ok(FrontendMessage::Sync),
         b'H' => Ok(FrontendMessage::Flush),
         b'X' => Ok(FrontendMessage::Terminate),
         other => Ok(FrontendMessage::Unknown(other)),
      }
   }
}

pub fn read_frontend_message<R: Read>(
   reader: &mut R,
   is_initial: bool,
) -> io::Result<FrontendMessage> {
   FrameReader::new(reader).read_frontend(is_initial)
}

fn invalid(message: &str) -> io::Error {
   io::Error::new(io::ErrorKind::InvalidData, message)
}

fn rd_i32_at(buf: &[u8], offset: usize) -> io::Result<i32> {
   if offset + 4 > buf.len() {
      return Err(invalid("truncated message"));
   }
   Ok(i32::from_be_bytes([
      buf[offset],
      buf[offset + 1],
      buf[offset + 2],
      buf[offset + 3],
   ]))
}

fn rd_i16(body: &[u8], pos: &mut usize) -> io::Result<i16> {
   if *pos + 2 > body.len() {
      return Err(invalid("truncated message"));
   }
   let value = i16::from_be_bytes([body[*pos], body[*pos + 1]]);
   *pos += 2;
   Ok(value)
}

fn rd_u32(body: &[u8], pos: &mut usize) -> io::Result<u32> {
   if *pos + 4 > body.len() {
      return Err(invalid("truncated message"));
   }
   let value = u32::from_be_bytes([body[*pos], body[*pos + 1], body[*pos + 2], body[*pos + 3]]);
   *pos += 4;
   Ok(value)
}

fn rd_i32(body: &[u8], pos: &mut usize) -> io::Result<i32> {
   Ok(rd_u32(body, pos)? as i32)
}

fn take_cstr<'a>(body: &'a [u8], pos: &mut usize) -> io::Result<&'a str> {
   let rest = body
      .get(*pos..)
      .ok_or_else(|| invalid("truncated message"))?;
   let end = rest
      .iter()
      .position(|&b| b == 0)
      .ok_or_else(|| invalid("unterminated string"))?;
   let value =
      std::str::from_utf8(&rest[..end]).map_err(|_| invalid("invalid utf-8 in message"))?;
   *pos += end + 1;
   Ok(value)
}

// ---------------------------------------------------------------- encoding

pub const READY_IDLE: [u8; 6] = [b'Z', 0, 0, 0, 5, b'I'];

// Frontend Flush: makes the backend release buffered extended-protocol
// replies without ending the cycle, which the proxy needs because it reads
// one reply at a time.
pub const FLUSH_REQUEST: [u8; 5] = [b'H', 0, 0, 0, 4];

#[inline]
pub fn put_message(buf: &mut Vec<u8>, tag: u8, body: &[u8]) {
   buf.reserve(body.len() + 5);
   buf.push(tag);
   buf.extend_from_slice(&((body.len() + 4) as i32).to_be_bytes());
   buf.extend_from_slice(body);
}

pub fn encode_row_description(columns: &[ColumnInfo]) -> Vec<u8> {
   let mut body = Vec::with_capacity(2 + columns.len() * 24);
   body.extend_from_slice(&(columns.len() as i16).to_be_bytes());
   for column in columns {
      body.extend_from_slice(column.name.as_bytes());
      body.push(0);
      body.extend_from_slice(&0i32.to_be_bytes());
      body.extend_from_slice(&0i16.to_be_bytes());
      body.extend_from_slice(&column.type_oid.to_be_bytes());
      body.extend_from_slice(&(-1i16).to_be_bytes());
      body.extend_from_slice(&(-1i32).to_be_bytes());
      body.extend_from_slice(&0i16.to_be_bytes());
   }
   let mut out = Vec::with_capacity(body.len() + 5);
   put_message(&mut out, b'T', &body);
   out
}

pub fn append_data_row(out: &mut Vec<u8>, values: &[DbValue]) {
   let start = out.len();
   out.push(b'D');
   out.extend_from_slice(&0i32.to_be_bytes());
   out.extend_from_slice(&(values.len() as i16).to_be_bytes());
   for value in values {
      match value {
         DbValue::Null => out.extend_from_slice(&(-1i32).to_be_bytes()),
         DbValue::Bool(b) => {
            out.extend_from_slice(&1i32.to_be_bytes());
            out.push(if *b { b't' } else { b'f' });
         }
         DbValue::Int(i) => {
            let mut buf = [0u8; 20];
            let text = format_i64(i, &mut buf);
            out.extend_from_slice(&(text.len() as i32).to_be_bytes());
            out.extend_from_slice(text);
         }
         DbValue::Float(f) => {
            let text = f.to_string();
            out.extend_from_slice(&(text.len() as i32).to_be_bytes());
            out.extend_from_slice(text.as_bytes());
         }
         DbValue::Text(s) => {
            out.extend_from_slice(&(s.len() as i32).to_be_bytes());
            out.extend_from_slice(s.as_bytes());
         }
      }
   }
   let len = (out.len() - start - 1) as i32;
   out[start + 1..start + 5].copy_from_slice(&len.to_be_bytes());
}

fn format_i64<'a>(value: &i64, buf: &'a mut [u8; 20]) -> &'a [u8] {
   use std::io::Write as _;
   let mut cursor = std::io::Cursor::new(&mut buf[..]);
   write!(&mut cursor, "{value}").expect("write int");
   let len = cursor.position() as usize;
   &buf[..len]
}

pub fn encode_result(
   columns: &[ColumnInfo],
   rows: &[crate::row::DbRow],
) -> (Vec<u8>, Vec<u8>, u32) {
   let row_description = if columns.is_empty() {
      Vec::new()
   } else {
      encode_row_description(columns)
   };
   let mut data_rows = Vec::with_capacity(rows.len() * 32);
   for row in rows {
      append_data_row(&mut data_rows, &row.values);
   }
   (row_description, data_rows, rows.len() as u32)
}

pub fn encode_command_complete(tag: &str) -> Vec<u8> {
   let mut body = Vec::with_capacity(tag.len() + 1);
   body.extend_from_slice(tag.as_bytes());
   body.push(0);
   let mut out = Vec::with_capacity(body.len() + 5);
   put_message(&mut out, b'C', &body);
   out
}

// Re-encodes a Bind message. Used when a cached Bind is deferred and the
// client later requests a row-limited Execute that must reach the origin.
pub fn encode_bind(
   portal: &str,
   statement: &str,
   param_formats: &[i16],
   params: &[Option<Vec<u8>>],
   result_formats: &[i16],
) -> Vec<u8> {
   let mut body = Vec::with_capacity(portal.len() + statement.len() + 16);
   body.extend_from_slice(portal.as_bytes());
   body.push(0);
   body.extend_from_slice(statement.as_bytes());
   body.push(0);

   body.extend_from_slice(&(param_formats.len() as i16).to_be_bytes());
   for format in param_formats {
      body.extend_from_slice(&format.to_be_bytes());
   }

   body.extend_from_slice(&(params.len() as i16).to_be_bytes());
   for param in params {
      match param {
         None => body.extend_from_slice(&(-1i32).to_be_bytes()),
         Some(bytes) => {
            body.extend_from_slice(&(bytes.len() as i32).to_be_bytes());
            body.extend_from_slice(bytes);
         }
      }
   }

   body.extend_from_slice(&(result_formats.len() as i16).to_be_bytes());
   for format in result_formats {
      body.extend_from_slice(&format.to_be_bytes());
   }

   let mut out = Vec::with_capacity(body.len() + 5);
   put_message(&mut out, b'B', &body);
   out
}

pub fn put_parse_complete(out: &mut Vec<u8>) {
   put_message(out, b'1', &[]);
}

pub fn put_bind_complete(out: &mut Vec<u8>) {
   put_message(out, b'2', &[]);
}

pub fn put_close_complete(out: &mut Vec<u8>) {
   put_message(out, b'3', &[]);
}

pub fn put_no_data(out: &mut Vec<u8>) {
   put_message(out, b'n', &[]);
}

pub fn put_empty_query(out: &mut Vec<u8>) {
   put_message(out, b'I', &[]);
}

pub fn put_parameter_description(out: &mut Vec<u8>, oids: &[u32]) {
   let mut body = Vec::with_capacity(2 + oids.len() * 4);
   body.extend_from_slice(&(oids.len() as i16).to_be_bytes());
   for oid in oids {
      body.extend_from_slice(&oid.to_be_bytes());
   }
   put_message(out, b't', &body);
}

pub fn encode_ready(status: u8) -> [u8; 6] {
   [b'Z', 0, 0, 0, 5, status]
}

pub fn encode_error(severity: &str, code: &str, message: &str) -> Vec<u8> {
   let mut body = Vec::with_capacity(message.len() + 32);
   body.push(b'S');
   body.extend_from_slice(severity.as_bytes());
   body.push(0);
   body.push(b'V');
   body.extend_from_slice(severity.as_bytes());
   body.push(0);
   body.push(b'C');
   body.extend_from_slice(code.as_bytes());
   body.push(0);
   body.push(b'M');
   body.extend_from_slice(message.as_bytes());
   body.push(0);
   body.push(0);
   let mut out = Vec::with_capacity(body.len() + 5);
   put_message(&mut out, b'E', &body);
   out
}

// ------------------------------------------------------------- compatibility

pub fn write_ssl_refusal<W: Write>(writer: &mut W) -> io::Result<()> {
   writer.write_all(b"N")?;
   writer.flush()
}

pub fn write_startup_ok<W: Write>(writer: &mut W) -> io::Result<()> {
   writer.write_all(&[b'R', 0, 0, 0, 8, 0, 0, 0, 0])?;
   write_parameter_status(writer, "server_version", "15.0 (FyroDB)")?;
   write_parameter_status(writer, "client_encoding", "UTF8")?;
   write_parameter_status(writer, "DateStyle", "ISO, MDY")?;
   write_ready_for_query(writer, b'I')?;
   writer.flush()
}

pub fn write_parameter_status<W: Write>(writer: &mut W, name: &str, val: &str) -> io::Result<()> {
   let mut payload = Vec::new();
   payload.extend_from_slice(name.as_bytes());
   payload.push(0);
   payload.extend_from_slice(val.as_bytes());
   payload.push(0);

   let len = (payload.len() + 4) as i32;
   writer.write_all(b"S")?;
   writer.write_all(&len.to_be_bytes())?;
   writer.write_all(&payload)
}

pub fn write_ready_for_query<W: Write>(writer: &mut W, status: u8) -> io::Result<()> {
   writer.write_all(&[b'Z', 0, 0, 0, 5, status])
}

pub fn write_command_complete<W: Write>(writer: &mut W, tag: &str) -> io::Result<()> {
   let mut body = Vec::with_capacity(tag.len() + 1);
   body.extend_from_slice(tag.as_bytes());
   body.push(0);
   writer.write_all(b"C")?;
   writer.write_all(&((body.len() + 4) as i32).to_be_bytes())?;
   writer.write_all(&body)
}

pub fn write_result_set<W: Write>(
   writer: &mut W,
   result: &crate::row::DbResultSet,
   tag: &str,
) -> io::Result<()> {
   if !result.columns.is_empty() {
      writer.write_all(&encode_row_description(&result.columns))?;
      let mut rows = Vec::with_capacity(result.rows.len() * 32);
      for row in &result.rows {
         append_data_row(&mut rows, &row.values);
      }
      writer.write_all(&rows)?;
   }
   write_command_complete(writer, tag)?;
   write_ready_for_query(writer, b'I')?;
   writer.flush()
}

pub fn write_error_response<W: Write>(writer: &mut W, message: &str) -> io::Result<()> {
   writer.write_all(&encode_error("ERROR", "XX000", message))?;
   write_ready_for_query(writer, b'I')?;
   writer.flush()
}

pub fn write_fatal_response<W: Write>(writer: &mut W, code: &str, message: &str) -> io::Result<()> {
   writer.write_all(&encode_error("FATAL", code, message))?;
   writer.flush()
}
