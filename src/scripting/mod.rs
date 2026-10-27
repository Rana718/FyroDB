use mlua::prelude::*;
use mlua::LuaSerdeExt;
use sha1::{Digest, Sha1};
use std::cell::Cell;
use std::sync::Arc;

use crate::storage::store::Store;
use crate::utils::resp;

const HEX_CHARS: &[u8; 16] = b"0123456789abcdef";
const STACK_ARGS_CAP: usize = 8;

#[inline(always)]
pub fn compute_sha1_hex_buf<'a>(data: &[u8], buf: &'a mut [u8; 40]) -> &'a str {
   let mut hasher = Sha1::new();
   hasher.update(data);
   let result = hasher.finalize();
   for (i, &b) in result.iter().enumerate() {
      buf[i * 2] = HEX_CHARS[(b >> 4) as usize];
      buf[i * 2 + 1] = HEX_CHARS[(b & 0x0f) as usize];
   }
   // SAFETY: HEX_CHARS are all ASCII '0'-'9', 'a'-'f'
   unsafe { std::str::from_utf8_unchecked(buf) }
}

#[inline]
pub fn compute_sha1_hex(data: &[u8]) -> String {
   let mut buf = [0u8; 40];
   compute_sha1_hex_buf(data, &mut buf).to_string()
}

#[inline(always)]
pub fn normalize_sha1<'a>(input: &'a str, buf: &'a mut [u8; 40]) -> &'a str {
   let bytes = input.as_bytes();
   if bytes.len() == 40 {
      let mut has_upper = false;
      for &b in bytes {
         if b.is_ascii_uppercase() {
            has_upper = true;
            break;
         }
      }
      if !has_upper {
         return input;
      }
      for (i, &b) in bytes.iter().enumerate() {
         buf[i] = b.to_ascii_lowercase();
      }
      // SAFETY: to_ascii_lowercase on ASCII hex characters yields valid ASCII/UTF-8
      unsafe { std::str::from_utf8_unchecked(buf) }
   } else {
      input
   }
}

#[inline(always)]
fn format_i64(n: i64, buf: &mut [u8; 32]) -> &str {
   if n == 0 {
      return "0";
   }
   let neg = n < 0;
   let mut u = if neg {
      (n as i128).unsigned_abs() as u64
   } else {
      n as u64
   };
   let mut idx = 32;
   while u > 0 {
      idx -= 1;
      buf[idx] = b'0' + (u % 10) as u8;
      u /= 10;
   }
   if neg {
      idx -= 1;
      buf[idx] = b'-';
   }
   // SAFETY: All bytes written are ASCII digits or '-'
   unsafe { std::str::from_utf8_unchecked(&buf[idx..32]) }
}

#[inline(always)]
fn format_f64(f: f64, buf: &mut [u8; 32]) -> &str {
   use std::io::Write;
   let mut cursor = std::io::Cursor::new(&mut buf[..]);
   let _ = write!(cursor, "{}", f);
   let pos = cursor.position() as usize;
   unsafe { std::str::from_utf8_unchecked(&buf[..pos]) }
}

struct ScratchBuffers {
   resp_buf: Vec<u8>,
   num_strings: Vec<String>,
}

impl ScratchBuffers {
   fn new() -> Self {
      Self {
         resp_buf: Vec::with_capacity(512),
         num_strings: Vec::with_capacity(8),
      }
   }

   #[inline(always)]
   fn prepare_for_cmd(&mut self) {
      if self.resp_buf.capacity() > 64 * 1024 {
         self.resp_buf = Vec::with_capacity(512);
      } else {
         self.resp_buf.clear();
      }
      if self.num_strings.capacity() > 32 {
         self.num_strings = Vec::with_capacity(8);
      } else {
         self.num_strings.clear();
      }
   }
}

thread_local! {
   static LUA: Lua = {
      let lua = Lua::new();
      setup_lua_env(&lua).expect("failed to setup lua environment");
      lua
   };
   static SCRIPTS_TABLE: std::cell::RefCell<Option<LuaTable>> = const { std::cell::RefCell::new(None) };
   static REUSABLE_KEYS: std::cell::RefCell<Option<LuaTable>> = const { std::cell::RefCell::new(None) };
   static REUSABLE_ARGV: std::cell::RefCell<Option<LuaTable>> = const { std::cell::RefCell::new(None) };
   static LAST_KEYS_LEN: Cell<usize> = const { Cell::new(0) };
   static LAST_ARGV_LEN: Cell<usize> = const { Cell::new(0) };
   static CURRENT_STORE: Cell<*const Arc<Store>> = const { Cell::new(std::ptr::null()) };
   static LOCAL_SCRIPT_EPOCH: Cell<u64> = const { Cell::new(0) };
   static SCRATCH: std::cell::RefCell<ScratchBuffers> = std::cell::RefCell::new(ScratchBuffers::new());
}

fn setup_lua_env(lua: &Lua) -> LuaResult<()> {
   // Configure Lua 5.4 generational garbage collection for minimal memory footprint and fast sweeps
   lua.gc_set_mode(LuaGcMode::Generational(LuaGcGenParams::default()));

   // Setup script cache table in registry
   let scripts_table = lua.create_table()?;
   lua.set_named_registry_value("_fyro_scripts", scripts_table.clone())?;
   SCRIPTS_TABLE.with_borrow_mut(|opt| *opt = Some(scripts_table));

   // Setup reusable KEYS and ARGV tables in registry and globals
   let keys_table = lua.create_table_with_capacity(8, 0)?;
   lua.globals().raw_set("KEYS", keys_table.clone())?;
   REUSABLE_KEYS.with_borrow_mut(|opt| *opt = Some(keys_table));

   let argv_table = lua.create_table_with_capacity(8, 0)?;
   lua.globals().raw_set("ARGV", argv_table.clone())?;
   REUSABLE_ARGV.with_borrow_mut(|opt| *opt = Some(argv_table));

   // Setup redis table
   let redis_table = lua.create_table()?;

   // redis.call
   let call_fn = lua.create_function(|lua, args: LuaMultiValue| {
      execute_redis_call(lua, args, false)
   })?;
   redis_table.set("call", call_fn)?;

   // redis.pcall
   let pcall_fn = lua.create_function(|lua, args: LuaMultiValue| {
      execute_redis_call(lua, args, true)
   })?;
   redis_table.set("pcall", pcall_fn)?;

   // redis.error_reply
   let error_reply_fn = lua.create_function(|lua, msg: String| {
      let table = lua.create_table_with_capacity(0, 1)?;
      table.raw_set("err", msg)?;
      Ok(table)
   })?;
   redis_table.set("error_reply", error_reply_fn)?;

   // redis.status_reply
   let status_reply_fn = lua.create_function(|lua, msg: String| {
      let table = lua.create_table_with_capacity(0, 1)?;
      table.raw_set("ok", msg)?;
      Ok(table)
   })?;
   redis_table.set("status_reply", status_reply_fn)?;

   // redis.sha1hex
   let sha1hex_fn = lua.create_function(|_lua, s: LuaString| {
      Ok(compute_sha1_hex(&s.as_bytes()))
   })?;
   redis_table.set("sha1hex", sha1hex_fn)?;

   // redis.log
   let log_fn = lua.create_function(|_lua, (_level, _msg): (i32, String)| {
      Ok(())
   })?;
   redis_table.set("log", log_fn)?;
   redis_table.set("LOG_DEBUG", 0)?;
   redis_table.set("LOG_VERBOSE", 1)?;
   redis_table.set("LOG_NOTICE", 2)?;
   redis_table.set("LOG_WARNING", 3)?;

   // redis.replicate_commands
   let rep_fn = lua.create_function(|_lua, ()| Ok(true))?;
   redis_table.set("replicate_commands", rep_fn)?;

   // redis.setresp
   let setresp_fn = lua.create_function(|_lua, _ver: i32| Ok(true))?;
   redis_table.set("setresp", setresp_fn)?;

   lua.globals().set("redis", redis_table)?;

   // Setup cjson table
   let cjson = lua.create_table()?;
   cjson.set(
      "encode",
      lua.create_function(|lua, val: LuaValue| {
         let json_val: serde_json::Value = lua.from_value(val)
            .map_err(|e| LuaError::RuntimeError(format!("cjson encode error: {e}")))?;
         serde_json::to_string(&json_val)
            .map_err(|e| LuaError::RuntimeError(format!("cjson encode error: {e}")))
      })?,
   )?;
   cjson.set(
      "decode",
      lua.create_function(|lua, s: String| {
         let json_val: serde_json::Value = serde_json::from_str(&s)
            .map_err(|e| LuaError::RuntimeError(format!("cjson decode error: {e}")))?;
         lua.to_value(&json_val)
      })?,
   )?;
   lua.globals().set("cjson", cjson)?;

   // Setup bit table for bitwise ops compatibility with Redis 5.1/5.2 scripts
   let bit = lua.create_table()?;
   bit.set(
      "band",
      lua.create_function(|_lua, (a, b): (i64, i64)| Ok(a & b))?,
   )?;
   bit.set(
      "bor",
      lua.create_function(|_lua, (a, b): (i64, i64)| Ok(a | b))?,
   )?;
   bit.set(
      "bxor",
      lua.create_function(|_lua, (a, b): (i64, i64)| Ok(a ^ b))?,
   )?;
   bit.set(
      "bnot",
      lua.create_function(|_lua, a: i64| Ok(!a))?,
   )?;
   bit.set(
      "lshift",
      lua.create_function(|_lua, (a, n): (i64, u32)| Ok(a << (n % 64)))?,
   )?;
   bit.set(
      "rshift",
      lua.create_function(|_lua, (a, n): (i64, u32)| Ok(((a as u64) >> (n % 64)) as i64))?,
   )?;
   lua.globals().set("bit", bit)?;

   Ok(())
}

fn execute_redis_call(
   lua: &Lua,
   args: LuaMultiValue,
   is_pcall: bool,
) -> LuaResult<LuaValue> {
   let store_ptr = CURRENT_STORE.get();
   if store_ptr.is_null() {
      return Err(LuaError::RuntimeError(
         "redis.call() invoked without active store".into(),
      ));
   }
   let store = unsafe { &*store_ptr };

   let args_len = args.len();
   if args_len == 0 {
      if is_pcall {
         let t = lua.create_table_with_capacity(0, 1)?;
         t.raw_set("err", "ERR wrong number of arguments for 'redis.call'")?;
         return Ok(LuaValue::Table(t));
      } else {
         return Err(LuaError::RuntimeError(
            "ERR wrong number of arguments for 'redis.call'".into(),
         ));
      }
   }

   // Ultra-fast path: 2 string arguments (covers GET key, INCR key, DEL key, etc.)
   if args_len == 2
      && let (Some(LuaValue::String(s0)), Some(LuaValue::String(s1))) = (args.front(), args.get(1))
   {
      let b0 = s0.to_str()?;
      let b1 = s1.to_str()?;
      let parts: [&str; 2] = [&b0, &b1];
      let cmd_bytes = b0.as_bytes();
      if cmd_bytes.len() == 4 && cmd_bytes.eq_ignore_ascii_case(b"EVAL") {
         let err_msg = "ERR This Redis command is not allowed from scripts";
         if is_pcall {
            let t = lua.create_table_with_capacity(0, 1)?;
            t.raw_set("err", err_msg)?;
            return Ok(LuaValue::Table(t));
         } else {
            return Err(LuaError::RuntimeError(err_msg.into()));
         }
      }
      return SCRATCH.with_borrow_mut(|scratch| {
         scratch.prepare_for_cmd();
         crate::commands::execute(&parts, store, &mut scratch.resp_buf);
         let mut slice = &scratch.resp_buf[..];
         parse_resp(lua, &mut slice, is_pcall)
      });
   }

   // Ultra-fast path: 3 string arguments (covers SET key val, HGET key field, etc.)
   if args_len == 3
      && let (Some(LuaValue::String(s0)), Some(LuaValue::String(s1)), Some(LuaValue::String(s2))) =
         (args.front(), args.get(1), args.get(2))
   {
      let b0 = s0.to_str()?;
      let b1 = s1.to_str()?;
      let b2 = s2.to_str()?;
      let parts: [&str; 3] = [&b0, &b1, &b2];
      let cmd_bytes = b0.as_bytes();
      let is_disallowed = match cmd_bytes.len() {
         4 => cmd_bytes.eq_ignore_ascii_case(b"EVAL"),
         6 => cmd_bytes.eq_ignore_ascii_case(b"SCRIPT"),
         7 => cmd_bytes.eq_ignore_ascii_case(b"EVALSHA"),
         _ => false,
      };
      if is_disallowed {
         let err_msg = "ERR This Redis command is not allowed from scripts";
         if is_pcall {
            let t = lua.create_table_with_capacity(0, 1)?;
            t.raw_set("err", err_msg)?;
            return Ok(LuaValue::Table(t));
         } else {
            return Err(LuaError::RuntimeError(err_msg.into()));
         }
      }
      return SCRATCH.with_borrow_mut(|scratch| {
         scratch.prepare_for_cmd();
         crate::commands::execute(&parts, store, &mut scratch.resp_buf);
         let mut slice = &scratch.resp_buf[..];
         parse_resp(lua, &mut slice, is_pcall)
      });
   }

   // Fast path: For up to STACK_ARGS_CAP arguments (covers 99.9% of Redis commands),
   // format and dispatch entirely on stack arrays without any heap allocation.
   if args_len <= STACK_ARGS_CAP {
      let mut stack_parts: [&str; STACK_ARGS_CAP] = [""; STACK_ARGS_CAP];
      let mut num_bufs = [[0u8; 32]; STACK_ARGS_CAP];
      let mut borrowed_strs: [Option<mlua::BorrowedStr>; STACK_ARGS_CAP] = [const { None }; STACK_ARGS_CAP];

      for (i, (val, buf)) in args.iter().zip(num_bufs.iter_mut()).enumerate() {
         match val {
            LuaValue::String(s) => {
               borrowed_strs[i] = Some(s.to_str()?);
            }
            LuaValue::Integer(n) => {
               stack_parts[i] = format_i64(*n, buf);
            }
            LuaValue::Number(f) => {
               stack_parts[i] = format_f64(*f, buf);
            }
            LuaValue::Boolean(b) => {
               stack_parts[i] = if *b { "1" } else { "0" };
            }
            LuaValue::Nil => {
               stack_parts[i] = "";
            }
            _ => {
               return Err(LuaError::RuntimeError(
                  "Invalid argument type for redis command".into(),
               ));
            }
         }
      }

      for i in 0..args_len {
         if let Some(ref b) = borrowed_strs[i] {
            stack_parts[i] = b;
         }
      }

      let parts = &stack_parts[..args_len];
      let first_bytes = parts[0].as_bytes();
      let is_disallowed = match first_bytes.len() {
         4 => first_bytes.eq_ignore_ascii_case(b"EVAL"),
         6 => first_bytes.eq_ignore_ascii_case(b"SCRIPT"),
         7 => first_bytes.eq_ignore_ascii_case(b"EVALSHA"),
         _ => false,
      };
      if is_disallowed {
         let err_msg = "ERR This Redis command is not allowed from scripts";
         if is_pcall {
            let t = lua.create_table_with_capacity(0, 1)?;
            t.raw_set("err", err_msg)?;
            return Ok(LuaValue::Table(t));
         } else {
            return Err(LuaError::RuntimeError(err_msg.into()));
         }
      }

      SCRATCH.with_borrow_mut(|scratch| {
         scratch.prepare_for_cmd();
         crate::commands::execute(parts, store, &mut scratch.resp_buf);
         let mut slice = &scratch.resp_buf[..];
         parse_resp(lua, &mut slice, is_pcall)
      })
   } else {
      // Fallback path for large commands with > STACK_ARGS_CAP arguments
      let mut borrowed_strs = Vec::with_capacity(args_len);

      SCRATCH.with_borrow_mut(|scratch| {
         scratch.prepare_for_cmd();

         for val in &args {
            match val {
               LuaValue::String(s) => {
                  let str_val = s.to_str()?;
                  borrowed_strs.push(str_val);
               }
               LuaValue::Integer(i) => {
                  scratch.num_strings.push(i.to_string());
               }
               LuaValue::Number(f) => {
                  scratch.num_strings.push(f.to_string());
               }
               LuaValue::Boolean(b) => {
                  scratch.num_strings.push(if *b { "1".into() } else { "0".into() });
               }
               LuaValue::Nil => {
                  scratch.num_strings.push(String::new());
               }
               _ => {
                  return Err(LuaError::RuntimeError(
                     "Invalid argument type for redis command".into(),
                  ));
               }
            }
         }

         let mut str_parts: Vec<&str> = Vec::with_capacity(args_len);
         let mut str_idx = 0;
         let mut num_idx = 0;
         for val in &args {
            if matches!(val, LuaValue::String(_)) {
               str_parts.push(&borrowed_strs[str_idx]);
               str_idx += 1;
            } else {
               str_parts.push(&scratch.num_strings[num_idx]);
               num_idx += 1;
            }
         }

         let first_bytes = str_parts[0].as_bytes();
         let is_disallowed = match first_bytes.len() {
            4 => first_bytes.eq_ignore_ascii_case(b"EVAL"),
            6 => first_bytes.eq_ignore_ascii_case(b"SCRIPT"),
            7 => first_bytes.eq_ignore_ascii_case(b"EVALSHA"),
            _ => false,
         };
         if is_disallowed {
            let err_msg = "ERR This Redis command is not allowed from scripts";
            if is_pcall {
               let t = lua.create_table_with_capacity(0, 1)?;
               t.raw_set("err", err_msg)?;
               return Ok(LuaValue::Table(t));
            } else {
               return Err(LuaError::RuntimeError(err_msg.into()));
            }
         }

         crate::commands::execute(&str_parts, store, &mut scratch.resp_buf);

         let mut slice = &scratch.resp_buf[..];
         parse_resp(lua, &mut slice, is_pcall)
      })
   }
}

#[inline(always)]
fn read_crlf_line<'a>(input: &mut &'a [u8]) -> Option<&'a [u8]> {
   let bytes = *input;
   let len = bytes.len();
   // Fast path for short headers/lines (e.g., "1\r\n", "OK\r\n", "42\r\n")
   if len >= 3 && bytes[1] == b'\r' && bytes[2] == b'\n' {
      let line = &bytes[..1];
      *input = &bytes[3..];
      return Some(line);
   }
   if len >= 4 && bytes[2] == b'\r' && bytes[3] == b'\n' {
      let line = &bytes[..2];
      *input = &bytes[4..];
      return Some(line);
   }
   if len >= 5 && bytes[3] == b'\r' && bytes[4] == b'\n' {
      let line = &bytes[..3];
      *input = &bytes[5..];
      return Some(line);
   }
   if let Some(pos) = memchr::memchr(b'\r', bytes)
      && pos + 1 < len
      && bytes[pos + 1] == b'\n'
   {
      let line = &bytes[..pos];
      *input = &bytes[pos + 2..];
      return Some(line);
   }
   None
}

#[inline(always)]
fn parse_i64_fast(bytes: &[u8]) -> Option<i64> {
   if bytes.len() == 1 && bytes[0].is_ascii_digit() {
      return Some((bytes[0] - b'0') as i64);
   }
   if bytes == b"-1" {
      return Some(-1);
   }
   if bytes.is_empty() {
      return None;
   }
   let (neg, digits) = match bytes[0] {
      b'-' => (true, &bytes[1..]),
      b'+' => (false, &bytes[1..]),
      _ => (false, bytes),
   };
   if digits.is_empty() {
      return None;
   }
   let mut n: i64 = 0;
   for &b in digits {
      if !b.is_ascii_digit() {
         return None;
      }
      n = n.checked_mul(10)?.checked_add((b - b'0') as i64)?;
   }
   Some(if neg { -n } else { n })
}

pub fn parse_resp(
   lua: &Lua,
   input: &mut &[u8],
   is_pcall: bool,
) -> LuaResult<LuaValue> {
   if input.is_empty() {
      return Ok(LuaValue::Nil);
   }

   let prefix = input[0];
   *input = &input[1..];

   match prefix {
      b'+' => {
         let line = read_crlf_line(input).ok_or_else(|| {
            LuaError::RuntimeError("unexpected end of status reply".into())
         })?;
         let t = lua.create_table_with_capacity(0, 1)?;
         if line == b"OK" {
            t.raw_set("ok", "OK")?;
         } else {
            let status = std::str::from_utf8(line).unwrap_or("");
            t.raw_set("ok", status)?;
         }
         Ok(LuaValue::Table(t))
      }
      b'-' => {
         let line = read_crlf_line(input).ok_or_else(|| {
            LuaError::RuntimeError("unexpected end of error reply".into())
         })?;
         let err_msg = std::str::from_utf8(line).unwrap_or("ERR unknown");
         if is_pcall {
            let t = lua.create_table_with_capacity(0, 1)?;
            t.raw_set("err", err_msg)?;
            Ok(LuaValue::Table(t))
         } else {
            Err(LuaError::RuntimeError(err_msg.to_string()))
         }
      }
      b':' => {
         let bytes = *input;
         if bytes.len() >= 3 && bytes[0].is_ascii_digit() && bytes[1] == b'\r' && bytes[2] == b'\n' {
            *input = &bytes[3..];
            return Ok(LuaValue::Integer((bytes[0] - b'0') as i64));
         }
         let line = read_crlf_line(input).ok_or_else(|| {
            LuaError::RuntimeError("unexpected end of integer reply".into())
         })?;
         let n = parse_i64_fast(line).unwrap_or(0);
         Ok(LuaValue::Integer(n))
      }
      b'$' => {
         let bytes = *input;
         // Fast path 1: Null bulk string "$-1\r\n"
         if bytes.len() >= 4 && bytes[0] == b'-' && bytes[1] == b'1' && bytes[2] == b'\r' && bytes[3] == b'\n' {
            *input = &bytes[4..];
            return Ok(LuaValue::Boolean(false));
         }
         // Fast path 2: Short bulk string with single-digit length "$<len>\r\n<data>\r\n"
         if bytes.len() >= 3 && bytes[0].is_ascii_digit() && bytes[1] == b'\r' && bytes[2] == b'\n' {
            let len = (bytes[0] - b'0') as usize;
            let total = 3 + len + 2;
            if bytes.len() >= total && bytes[3 + len] == b'\r' && bytes[3 + len + 1] == b'\n' {
               let data = &bytes[3..3 + len];
               *input = &bytes[total..];
               return Ok(LuaValue::String(lua.create_string(data)?));
            }
         }
         let line = read_crlf_line(input).ok_or_else(|| {
            LuaError::RuntimeError("unexpected end of bulk string header".into())
         })?;
         let len = parse_i64_fast(line).unwrap_or(-1);
         if len < 0 {
            // In Redis, null bulk string is boolean false in Lua
            Ok(LuaValue::Boolean(false))
         } else {
            let len = len as usize;
            if input.len() < len + 2 {
               return Err(LuaError::RuntimeError(
                  "unexpected end of bulk string data".into(),
               ));
            }
            let data = &input[..len];
            *input = &input[len + 2..]; // skip data and \r\n
            Ok(LuaValue::String(lua.create_string(data)?))
         }
      }
      b'*' => {
         let line = read_crlf_line(input).ok_or_else(|| {
            LuaError::RuntimeError("unexpected end of array header".into())
         })?;
         let count = parse_i64_fast(line).unwrap_or(-1);
         if count < 0 {
            Ok(LuaValue::Boolean(false))
         } else {
            let count_usize = count as usize;
            let t = lua.create_table_with_capacity(count_usize, 0)?;
            for i in 1..=count_usize {
               let elem = parse_resp(lua, input, is_pcall)?;
               t.raw_seti(i, elem)?;
            }
            Ok(LuaValue::Table(t))
         }
      }
      b'_' => {
         read_crlf_line(input);
         Ok(LuaValue::Boolean(false))
      }
      b'#' => {
         let line = read_crlf_line(input).ok_or_else(|| {
            LuaError::RuntimeError("unexpected end of boolean reply".into())
         })?;
         Ok(LuaValue::Boolean(line == b"t"))
      }
      b',' => {
         let line = read_crlf_line(input).ok_or_else(|| {
            LuaError::RuntimeError("unexpected end of float reply".into())
         })?;
         let f = std::str::from_utf8(line)
            .ok()
            .and_then(|s| s.parse::<f64>().ok())
            .unwrap_or(0.0);
         Ok(LuaValue::Number(f))
      }
      _ => {
         read_crlf_line(input);
         Ok(LuaValue::Nil)
      }
   }
}

pub fn lua_to_resp(val: LuaValue, out: &mut Vec<u8>) {
   match val {
      LuaValue::Nil => {
         resp::write_nil(out);
      }
      LuaValue::Boolean(false) => {
         resp::write_nil(out);
      }
      LuaValue::Boolean(true) => {
         resp::write_integer(out, 1);
      }
      LuaValue::Integer(i) => {
         resp::write_integer(out, i);
      }
      LuaValue::Number(f) => {
         resp::write_integer(out, f.trunc() as i64);
      }
      LuaValue::String(s) => {
         resp::write_bulk_bytes(out, &s.as_bytes());
      }
      LuaValue::Table(t) => {
         let len = t.raw_len();
         if len == 0 {
            // Check for error reply table: { err = "..." }
            if let Ok(LuaValue::String(s)) = t.raw_get::<LuaValue>("err") {
               let msg = s.to_str();
               let msg_ref = msg.as_deref().unwrap_or("ERR unknown");
               if msg_ref.starts_with("ERR") || msg_ref.starts_with("WRONGTYPE") || msg_ref.starts_with("NOSCRIPT") {
                  out.extend_from_slice(b"-");
                  out.extend_from_slice(msg_ref.as_bytes());
                  out.extend_from_slice(b"\r\n");
               } else {
                  out.extend_from_slice(b"-ERR ");
                  out.extend_from_slice(msg_ref.as_bytes());
                  out.extend_from_slice(b"\r\n");
               }
               return;
            }
            // Check for status reply table: { ok = "..." }
            if let Ok(LuaValue::String(s)) = t.raw_get::<LuaValue>("ok") {
               if let Ok(msg) = s.to_str() {
                  resp::write_simple(out, &msg);
               } else {
                  resp::write_simple(out, "OK");
               }
               return;
            }
         }
         // Array / sequence table
         resp::write_array_header(out, len);
         for i in 1..=len {
            if let Ok(elem) = t.raw_get(i) {
               lua_to_resp(elem, out);
            } else {
               resp::write_nil(out);
            }
         }
      }
      _ => {
         resp::write_nil(out);
      }
   }
}

struct ScopeGuard;

impl Drop for ScopeGuard {
   fn drop(&mut self) {
      CURRENT_STORE.set(std::ptr::null());
   }
}

pub fn eval_script(
   script: &str,
   keys: &[&str],
   argv: &[&str],
   store: &Arc<Store>,
   out: &mut Vec<u8>,
) {
   let mut buf = [0u8; 40];
   let sha1 = compute_sha1_hex_buf(script.as_bytes(), &mut buf);
   eval_internal(Some(script), sha1, keys, argv, store, out);
}

pub fn eval_sha(
   sha1: &str,
   keys: &[&str],
   argv: &[&str],
   store: &Arc<Store>,
   out: &mut Vec<u8>,
) -> bool {
   let mut buf = [0u8; 40];
   let norm_sha1 = normalize_sha1(sha1, &mut buf);
   eval_internal(None, norm_sha1, keys, argv, store, out)
}

fn eval_internal(
   script: Option<&str>,
   sha1: &str,
   keys: &[&str],
   argv: &[&str],
   store: &Arc<Store>,
   out: &mut Vec<u8>,
) -> bool {
   LUA.with(|lua| {
      // 1. Check epoch for SCRIPT FLUSH synchronization
      let store_epoch = store.script_epoch.load(std::sync::atomic::Ordering::Acquire);
      if LOCAL_SCRIPT_EPOCH.get() != store_epoch {
         SCRIPTS_TABLE.with_borrow_mut(|opt| {
            if let Some(t) = opt {
               let _ = t.clear();
            }
         });
         LOCAL_SCRIPT_EPOCH.set(store_epoch);
      }

      // 2. Fast path: Fetch precompiled function directly from thread-local registry without cloning
      let cached_func: Option<LuaFunction> = SCRIPTS_TABLE.with_borrow(|opt| {
         opt.as_ref().and_then(|t| t.raw_get(sha1).ok())
      });

      // 3. Fallback: If not cached in thread-local VM, compile or retrieve from Store
      let func: LuaFunction = match cached_func {
         Some(f) => f,
         None => {
            let scripts_table = match SCRIPTS_TABLE.with_borrow(|opt| opt.clone()) {
               Some(t) => t,
               None => {
                  let t = match lua.named_registry_value::<LuaTable>("_fyro_scripts") {
                     Ok(t) => t,
                     Err(_) => {
                        let t = match lua.create_table() {
                           Ok(t) => t,
                           Err(e) => {
                              resp::write_err(out, &format!("ERR failed to access script cache: {e}"));
                              return true;
                           }
                        };
                        let _ = lua.set_named_registry_value("_fyro_scripts", t.clone());
                        t
                     }
                  };
                  SCRIPTS_TABLE.with_borrow_mut(|opt| *opt = Some(t.clone()));
                  t
               }
            };

            let compile_res = if let Some(src) = script {
               store.ensure_script_cached(sha1, src);
               lua.load(src).into_function()
            } else {
               match store.with_script(sha1, |src| lua.load(src).into_function()) {
                  Some(res) => res,
                  None => return false,
               }
            };

            match compile_res {
               Ok(f) => {
                  let _ = scripts_table.raw_set(sha1, f.clone());
                  f
               }
               Err(err) => {
                  let err_msg = err.to_string();
                  out.extend_from_slice(b"-ERR Error compiling script: ");
                  out.extend_from_slice(err_msg.as_bytes());
                  out.extend_from_slice(b"\r\n");
                  return true;
               }
            }
         }
      };

      CURRENT_STORE.set(store as *const Arc<Store>);
      let _guard = ScopeGuard;

      // Populate KEYS table in-place without table cloning or allocations
      let keys_len = keys.len();
      let last_klen = LAST_KEYS_LEN.get();
      if keys_len > 0 || last_klen > 0 {
         REUSABLE_KEYS.with_borrow(|opt| {
            if let Some(ref k_table) = *opt {
               for (i, &k) in keys.iter().enumerate() {
                  let _ = k_table.raw_seti(i + 1, k);
               }
               for i in (keys_len + 1)..=last_klen.max(k_table.raw_len()) {
                  let _ = k_table.raw_seti(i, LuaValue::Nil);
               }
            }
         });
         LAST_KEYS_LEN.set(keys_len);
      }

      // Populate ARGV table in-place without table cloning or allocations
      let argv_len = argv.len();
      let last_alen = LAST_ARGV_LEN.get();
      if argv_len > 0 || last_alen > 0 {
         REUSABLE_ARGV.with_borrow(|opt| {
            if let Some(ref a_table) = *opt {
               for (i, &a) in argv.iter().enumerate() {
                  let _ = a_table.raw_seti(i + 1, a);
               }
               for i in (argv_len + 1)..=last_alen.max(a_table.raw_len()) {
                  let _ = a_table.raw_seti(i, LuaValue::Nil);
               }
            }
         });
         LAST_ARGV_LEN.set(argv_len);
      }

      // Execute the precompiled function
      let result: LuaResult<LuaValue> = func.call(());

      match result {
         Ok(val) => {
            lua_to_resp(val, out);
         }
         Err(err) => {
            let err_msg = err.to_string();
            out.extend_from_slice(b"-ERR Error running script: ");
            out.extend_from_slice(err_msg.as_bytes());
            out.extend_from_slice(b"\r\n");
         }
      }

      true
   })
}
