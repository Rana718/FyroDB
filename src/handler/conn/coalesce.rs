#![allow(clippy::chunks_exact_to_as_chunks, clippy::collapsible_if)]

use super::*;

/// Max same-key queue commands per coalesced run (one lock acquisition).
pub(super) const QUEUE_RUN_MAX: usize = 512;

pub(super) enum PairOutcome {
   Done,
   FallThrough,
   ConnError,
}

/// Is `raw` a plain `SET key value` that could open a coalesced
/// `SET k v` + `EXPIRE k t` pair?
#[inline(always)]
pub(super) fn is_set_expire_pair(conn: &Conn<'_>, raw: &[(*const u8, usize)]) -> bool {
   if raw.len() != 3 {
      return false;
   }
   const EXPIRE_PREFIX: &[u8] = b"*3\r\n$6\r\n";
   conn
      .parser
      .peek_remaining(EXPIRE_PREFIX.len())
      .starts_with(EXPIRE_PREFIX)
}

pub(super) fn dispatch_set_expire_pair(
   conn: &mut Conn<'_>,
   set: &[(*const u8, usize)],
) -> PairOutcome {
   // Zero-copy peek: the EXPIRE header is fixed-width, so a prefix check
   // on the unparsed remainder decides without consuming or doing UTF-8 parsing on set parts.
   const EXPIRE_PREFIX: &[u8] = b"*3\r\n$6\r\nEXPIRE\r\n";
   if !conn
      .parser
      .peek_remaining(EXPIRE_PREFIX.len())
      .starts_with(EXPIRE_PREFIX)
   {
      return PairOutcome::FallThrough;
   }

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

   // Consume and parse the EXPIRE command.
   match conn.parser.parse_one() {
      ParseResult::Complete => {}
      ParseResult::Incomplete => {
         // SET succeeded; EXPIRE was incomplete and remains in rbuf at start_pos for the next read/parse.
         conn.store.set_string(key, value, 0);
         if conn.store.has_replication() {
            conn.store.record_current_value(key);
         }
         conn.parser.wbuf.extend_from_slice(b"+OK\r\n");
         return PairOutcome::Done;
      }
      ParseResult::Error => return PairOutcome::ConnError,
   }
   let raw_ptr = conn.parser.parts_raw.as_ptr();
   let raw_len = conn.parser.parts_raw.len();
   let expire = unsafe { std::slice::from_raw_parts(raw_ptr, raw_len) };
   if expire.len() != 3 || cmd_id(unsafe { part_bytes(expire[0]) }) != CmdId::Expire {
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
   let exp = if secs_str == "60" {
      crate::storage::value::expiry_from_secs(60)
   } else {
      secs_str
         .parse::<u64>()
         .ok()
         .and_then(crate::storage::value::expiry_from_secs)
   };
   let Some(exp) = exp else {
      // Invalid TTL: the generic path reports the error identically.
      conn.store.set_string(key, value, 0);
      if conn.store.has_replication() {
         conn.store.record_current_value(key);
      }
      conn.parser.wbuf.extend_from_slice(b"+OK\r\n");
      dispatch_raw(conn, expire);
      return PairOutcome::Done;
   };

   let store = conn.store;
   let _write_epoch = store.data.write_epoch();

   // The pair: one lock, atomic value+TTL.
   let (hint, entry) = conn.hint_for(set[1]);
   conn.store.set_string_auto(key, hint, entry, value, exp);
   if conn.store.has_replication() {
      conn.store.record_current_value(key);
   }
   conn.parser.wbuf.extend_from_slice(b"+OK\r\n:1\r\n");

   let exp_60 = crate::storage::value::expiry_from_secs(60).unwrap();
   for _ in 0..128 {
      let avail = conn.parser.filled - conn.parser.pos;
      if avail < 48 {
         break;
      }
      let buf = unsafe { conn.parser.rbuf.as_ptr().add(conn.parser.pos) };
      if let Some((k_slice, v_slice)) = conn.parser.try_fast_set_expire_pair_at(buf, avail) {
         let (hint, entry) = conn.hint_for(k_slice);
         let out = &mut conn.parser.wbuf;
         let Some(k_str) = part_str(out, k_slice) else {
            return PairOutcome::ConnError;
         };
         let Some(v_str) = part_str(out, v_slice) else {
            return PairOutcome::ConnError;
         };
         conn
            .store
            .set_string_auto(k_str, hint, entry, v_str, exp_60);
         if conn.store.has_replication() {
            conn.store.record_current_value(k_str);
         }
         conn.parser.wbuf.extend_from_slice(b"+OK\r\n:1\r\n");
      } else {
         break;
      }
   }
   PairOutcome::Done
}

/// Is `raw` a `PUBLISH channel message` that could open a batched run?
/// The caller has already classified the command as `PUBLISH`.
#[inline(always)]
pub(super) fn is_publish_run(raw: &[(*const u8, usize)]) -> bool {
   raw.len() == 3
}

pub(super) fn dispatch_publish_run(
   conn: &mut Conn<'_>,
   first: &[(*const u8, usize)],
) -> QueueRunOutcome {
   // Validate the channel the same way the single-command path would.
   let out = &mut conn.parser.wbuf;
   let Some(channel) = part_str(out, first[1]) else {
      return QueueRunOutcome::ConnError;
   };
   let channel_bytes = channel.as_bytes();

   // first aliases parts_raw (overwritten by each parse_one): copy the
   // message now; allocate only once a real run forms.
   let first_msg = first[2];
   let mut parts = [(std::ptr::null::<u8>(), 0usize); QUEUE_RUN_MAX];
   parts[0] = first_msg;
   let mut cmds = 1usize;

   while cmds < QUEUE_RUN_MAX {
      let avail = conn.parser.filled - conn.parser.pos;
      if avail >= 24 {
         let buf = unsafe { conn.parser.rbuf.as_ptr().add(conn.parser.pos) };
         if let Some(msg_slice) = conn.parser.try_fast_publish_at(buf, avail, channel_bytes) {
            parts[cmds] = msg_slice;
            cmds += 1;
            continue;
         }
      }

      let next_start = conn.parser.pos;
      match conn.parser.parse_one() {
         ParseResult::Complete => {
            let raw_ptr = conn.parser.parts_raw.as_ptr();
            let raw_len = conn.parser.parts_raw.len();
            let raw = unsafe { std::slice::from_raw_parts(raw_ptr, raw_len) };
            let cmd = unsafe { part_bytes(raw[0]) };
            let same = raw.len() == 3
               && cmd_id(cmd) == CmdId::Publish
               && unsafe { part_bytes(raw[1]) } == channel_bytes;
            if !same {
               let outcome = match execute_publish_run(conn, channel, cmds, &parts[..cmds]) {
                  Ok(()) => QueueRunOutcome::Done,
                  Err(()) => QueueRunOutcome::ConnError,
               };
               if matches!(outcome, QueueRunOutcome::Done) {
                  conn.parser.pos = next_start;
               }
               return outcome;
            }
            parts[cmds] = raw[2];
            cmds += 1;
         }
         ParseResult::Incomplete => break,
         ParseResult::Error => {
            return match execute_publish_run(conn, channel, cmds, &parts[..cmds]) {
               Ok(()) => QueueRunOutcome::Done,
               Err(()) => QueueRunOutcome::ConnError,
            };
         }
      }
   }

   match execute_publish_run(conn, channel, cmds, &parts[..cmds]) {
      Ok(()) => QueueRunOutcome::Done,
      Err(()) => QueueRunOutcome::ConnError,
   }
}

pub(super) fn execute_publish_run(
   conn: &mut Conn<'_>,
   channel: &str,
   cmds: usize,
   parts: &[(*const u8, usize)],
) -> Result<(), ()> {
   let out = &mut conn.parser.wbuf;
   out.reserve(cmds * 8);
   let mut messages: [&[u8]; QUEUE_RUN_MAX] = [&[]; QUEUE_RUN_MAX];
   for (idx, &part) in parts.iter().enumerate() {
      messages[idx] = unsafe { part_bytes(part) };
   }
   let n = conn.pubsub.publish_batch_bytes(channel, &messages[..cmds]);
   let old_len = out.len();
   crate::utils::resp::write_integer(out, n as i64);
   if cmds > 1 {
      crate::utils::resp::repeat_reply(out, old_len, cmds);
   }
   Ok(())
}

/// Same-key write whose pipelined runs execute under one lock, with
/// per-command replies identical to sequential execution.
#[derive(Clone, Copy, PartialEq)]
pub(super) enum WriteOp {
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
   /// `PFADD k elem`: reply per command is the "register changed" flag.
   Pfadd,
   /// `GEOADD k lon lat member`: reply per command is the added count.
   Geoadd,
   /// `JSON.SET k path v`: only the last value is observable; replies are all `+OK`.
   JsonSet,
   /// `XADD k * f v`: reply per command is the stream ID.
   Xadd,
}

impl WriteOp {
   #[inline(always)]
   fn parse(cmd: &[u8]) -> Option<WriteOp> {
      Self::from_id(cmd_id(cmd))
   }

   #[inline(always)]
   fn from_id(id: CmdId) -> Option<WriteOp> {
      match id {
         CmdId::Set => Some(WriteOp::Set),
         CmdId::Incr => Some(WriteOp::Incr),
         CmdId::Hset => Some(WriteOp::Hset),
         CmdId::Sadd => Some(WriteOp::Sadd),
         CmdId::Zadd => Some(WriteOp::Zadd),
         CmdId::Xadd => Some(WriteOp::Xadd),
         CmdId::Pfadd => Some(WriteOp::Pfadd),
         CmdId::Geoadd => Some(WriteOp::Geoadd),
         CmdId::JsonSet => Some(WriteOp::JsonSet),
         _ => None,
      }
   }

   /// Total RESP parts of one command, key included.
   #[inline(always)]
   fn arity(self) -> usize {
      match self {
         WriteOp::Set | WriteOp::Sadd | WriteOp::Pfadd => 3,
         WriteOp::Incr => 2,
         WriteOp::Hset | WriteOp::Zadd | WriteOp::JsonSet => 4,
         WriteOp::Geoadd | WriteOp::Xadd => 5,
      }
   }
}

#[inline(always)]
pub(super) fn is_write_run(raw: &[(*const u8, usize)]) -> bool {
   if raw.is_empty() {
      return false;
   }
   let cmd = unsafe { part_bytes(raw[0]) };
   WriteOp::parse(cmd).is_some_and(|op| raw.len() == op.arity())
}

use crate::utils::resp::format_usize_digits;

#[inline(always)]
fn try_fast_tail(
   buf: *const u8,
   avail: usize,
   prefix_len: usize,
   tail_len: usize,
   out_parts: &mut [(*const u8, usize)],
) -> Option<usize> {
   let mut offset = prefix_len;
   for out_part in out_parts.iter_mut().take(tail_len) {
      if avail < offset + 4 {
         return None;
      }
      if unsafe { *buf.add(offset) } != b'$' {
         return None;
      }
      let (len, hdr_len) =
         unsafe { crate::utils::parser::parse_len_le4(buf.add(offset + 1), avail - (offset + 1)) }?;
      let data_offset = offset + 1 + hdr_len;
      let next_offset = data_offset + len + 2;
      if avail < next_offset {
         return None;
      }
      if unsafe { std::ptr::read_unaligned(buf.add(next_offset - 2) as *const u16) }
         != u16::from_ne_bytes(*b"\r\n")
      {
         return None;
      }
      *out_part = (unsafe { buf.add(data_offset) }, len);
      offset = next_offset;
   }
   Some(offset)
}

/// Execute a same-key write run with one lock acquisition; any
/// non-matching command ends the run and dispatches normally.
pub(super) fn dispatch_write_run(
   conn: &mut Conn<'_>,
   first: &[(*const u8, usize)],
) -> QueueRunOutcome {
   let first_cmd = unsafe { part_bytes(first[0]) };
   let op = WriteOp::parse(first_cmd).expect("is_write_run checked the first command");

   let out = &mut conn.parser.wbuf;
   let Some(key) = part_str(out, first[1]) else {
      return QueueRunOutcome::ConnError;
   };
   let key_bytes = key.as_bytes();

   let tail_len = op.arity() - 2;
   let mut parts: [(*const u8, usize); QUEUE_RUN_MAX * 3] =
      [(std::ptr::null(), 0); QUEUE_RUN_MAX * 3];
   for (i, &p) in first[2..].iter().enumerate() {
      parts[i] = p;
   }
   let mut cmds = 1usize;

   let mut prefix_buf = [0u8; 192];
   let mut prefix_len = 0;
   let can_fast = key_bytes.len() + first_cmd.len() + 32 <= prefix_buf.len() && tail_len <= 3;
   if can_fast {
      let arity_bytes: &[u8] = match op.arity() {
         2 => b"*2\r\n",
         3 => b"*3\r\n",
         4 => b"*4\r\n",
         5 => b"*5\r\n",
         _ => b"",
      };
      prefix_buf[..4].copy_from_slice(arity_bytes);
      prefix_len = 4;

      prefix_buf[prefix_len] = b'$';
      prefix_len += 1;
      let mut num_buf = [0u8; 20];
      let num_str = format_usize_digits(first_cmd.len(), &mut num_buf);
      prefix_buf[prefix_len..prefix_len + num_str.len()].copy_from_slice(num_str);
      prefix_len += num_str.len();
      prefix_buf[prefix_len..prefix_len + 2].copy_from_slice(b"\r\n");
      prefix_len += 2;
      prefix_buf[prefix_len..prefix_len + first_cmd.len()].copy_from_slice(first_cmd);
      prefix_len += first_cmd.len();
      prefix_buf[prefix_len..prefix_len + 2].copy_from_slice(b"\r\n");
      prefix_len += 2;

      prefix_buf[prefix_len] = b'$';
      prefix_len += 1;
      let num_str = format_usize_digits(key_bytes.len(), &mut num_buf);
      prefix_buf[prefix_len..prefix_len + num_str.len()].copy_from_slice(num_str);
      prefix_len += num_str.len();
      prefix_buf[prefix_len..prefix_len + 2].copy_from_slice(b"\r\n");
      prefix_len += 2;
      prefix_buf[prefix_len..prefix_len + key_bytes.len()].copy_from_slice(key_bytes);
      prefix_len += key_bytes.len();
      prefix_buf[prefix_len..prefix_len + 2].copy_from_slice(b"\r\n");
      prefix_len += 2;
   }

   let prefix = &prefix_buf[..prefix_len];
   while cmds < QUEUE_RUN_MAX {
      if can_fast {
         let avail = conn.parser.filled - conn.parser.pos;
         if avail >= prefix_len + tail_len * 4 {
            let buf = unsafe { conn.parser.rbuf.as_ptr().add(conn.parser.pos) };
            if unsafe { std::slice::from_raw_parts(buf, prefix_len) } == prefix {
               if let Some(parsed_len) = try_fast_tail(
                  buf,
                  avail,
                  prefix_len,
                  tail_len,
                  &mut parts[cmds * tail_len..],
               ) {
                  conn.parser.pos += parsed_len;
                  cmds += 1;
                  continue;
               }
            }
         }
      }

      let next_start = conn.parser.pos;
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
               let tail = &parts[..cmds * tail_len];
               let outcome = match execute_write_run(conn, key, op, cmds, tail) {
                  Ok(()) => QueueRunOutcome::Done,
                  Err(()) => QueueRunOutcome::ConnError,
               };
               if matches!(outcome, QueueRunOutcome::Done) {
                  conn.parser.pos = next_start;
               }
               return outcome;
            }
            let dst = &mut parts[cmds * tail_len..(cmds + 1) * tail_len];
            dst.copy_from_slice(&raw[2..]);
            cmds += 1;
         }
         ParseResult::Incomplete => break,
         ParseResult::Error => {
            let tail = &parts[..cmds * tail_len];
            return match execute_write_run(conn, key, op, cmds, tail) {
               Ok(()) => QueueRunOutcome::Done,
               Err(()) => QueueRunOutcome::ConnError,
            };
         }
      }
   }

   let tail = &parts[..cmds * tail_len];
   match execute_write_run(conn, key, op, cmds, tail) {
      Ok(()) => QueueRunOutcome::Done,
      Err(()) => QueueRunOutcome::ConnError,
   }
}

/// Run one coalesced write batch; Err(()) closes the connection,
/// matching the single-command paths on invalid UTF-8.
pub(super) fn execute_write_run(
   conn: &mut Conn<'_>,
   key: &str,
   op: WriteOp,
   cmds: usize,
   parts: &[(*const u8, usize)],
) -> Result<(), ()> {
   let store = conn.store;
   let _write_epoch = store.data.write_epoch();
   let replication = conn.store.has_replication();

   match op {
      WriteOp::Set => {
         // Sequential SETs fully overwrite: one call reproduces the final
         // state. Plain SET clears TTL. Bytes-native: no value scan.
         let last = parts[parts.len() - 1];
         let value = unsafe { part_bytes(last) };
         conn.store.set_string_auto_bytes(key, None, std::ptr::null(), value, 0);
         for _ in 0..cmds {
            conn.parser.wbuf.extend_from_slice(b"+OK\r\n");
         }
         if replication {
            // One capture per run: the batch mutation is already complete
            // above, so every loop iteration recorded the identical final
            // value; K-1 lookups, encodes and appends were pure waste.
            conn.store.record_current_value(key);
         }
      }
      WriteOp::Incr => {
         // One incrby: replies are cur+1..=cur+K. Overflow needs the sequential
         // fallback (partial application differs from one checked_add).
         match conn.store.incrby(key, cmds as i64) {
            Ok(final_value) => {
               let out = &mut conn.parser.wbuf;
               out.reserve(cmds * 24);
               for i in 1..=cmds as i64 {
                  crate::utils::resp::write_integer(out, final_value - (cmds as i64 - i));
               }
            }
            Err("increment or decrement would overflow") => {
               conn.parser.wbuf.reserve(cmds * 24);
               for _ in 0..cmds {
                  match conn.store.incr(key) {
                     Ok(n) => crate::utils::resp::write_integer(&mut conn.parser.wbuf, n),
                     Err(e) => crate::utils::resp::write_err(&mut conn.parser.wbuf, e),
                  }
               }
            }
            Err(e) => {
               let out = &mut conn.parser.wbuf;
               out.reserve(cmds * 32);
               for _ in 0..cmds {
                  crate::utils::resp::write_err(out, e);
               }
            }
         }
         if replication {
            // One capture per run: the batch mutation is already complete
            // above, so every loop iteration recorded the identical final
            // value; K-1 lookups, encodes and appends were pure waste.
            conn.store.record_current_value(key);
         }
      }
      WriteOp::Hset => {
         let mut fields: [(&str, &[u8]); QUEUE_RUN_MAX] = [("", &[]); QUEUE_RUN_MAX];
         for (i, pair) in parts.chunks_exact(2).enumerate() {
            let Some(f) = std::str::from_utf8(unsafe { part_bytes(pair[0]) }).ok() else {
               return Err(());
            };
            fields[i] = (f, unsafe { part_bytes(pair[1]) });
         }
         match conn.store.hset_bytes_added_flags(key, &fields[..cmds]) {
            Ok(flags) => {
               let out = &mut conn.parser.wbuf;
               out.reserve(flags.len() * 4);
               for added in flags {
                  crate::utils::resp::write_integer(out, added as i64);
               }
            }
            Err(e) => {
               let out = &mut conn.parser.wbuf;
               out.reserve(cmds * 32);
               for _ in 0..cmds {
                  crate::utils::resp::write_err(out, e);
               }
            }
         }
         if replication {
            // One capture per run: the batch mutation is already complete
            // above, so every loop iteration recorded the identical final
            // value; K-1 lookups, encodes and appends were pure waste.
            conn.store.record_current_value(key);
         }
      }
      WriteOp::Sadd => {
         let out = &mut conn.parser.wbuf;
         let mut members: [&str; QUEUE_RUN_MAX] = [""; QUEUE_RUN_MAX];
         for (i, &part) in parts.iter().enumerate() {
            let Some(m) = part_str(out, part) else {
               return Err(());
            };
            members[i] = m;
         }
         if let Err(e) = conn.store.sadd_added_flags_into(key, &members[..cmds], out) {
            out.reserve(cmds * 32);
            for _ in 0..cmds {
               crate::utils::resp::write_err(out, e);
            }
         }
         if replication {
            // One capture per run: the batch mutation is already complete
            // above, so every loop iteration recorded the identical final
            // value; K-1 lookups, encodes and appends were pure waste.
            conn.store.record_current_value(key);
         }
      }
      WriteOp::Pfadd => {
         let out = &mut conn.parser.wbuf;
         let mut elements: [&str; QUEUE_RUN_MAX] = [""; QUEUE_RUN_MAX];
         for (i, &part) in parts.iter().enumerate() {
            let Some(m) = part_str(out, part) else {
               return Err(());
            };
            elements[i] = m;
         }
         match conn.store.pfadd_run(key, &elements[..cmds]) {
            Ok(flags) => {
               let out = &mut conn.parser.wbuf;
               out.reserve(flags.len() * 4);
               for changed in flags {
                  crate::utils::resp::write_integer(out, changed as i64);
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
            // One capture per run: the batch mutation is already complete
            // above, so every loop iteration recorded the identical final
            // value; K-1 lookups, encodes and appends were pure waste.
            conn.store.record_current_value(key);
         }
      }
      WriteOp::Zadd => {
         // A bad score fails only its command: sequential fallback keeps the
         // reply stream identical.
         let out = &mut conn.parser.wbuf;
         let mut members: [(f64, &str); QUEUE_RUN_MAX] = [(0.0, ""); QUEUE_RUN_MAX];
         let mut all_valid = true;
         for (i, pair) in parts.chunks_exact(2).enumerate() {
            let score_b = unsafe { part_bytes(pair[0]) };
            let Some(score) = crate::utils::resp::fast_parse_f64_bytes(score_b) else {
               all_valid = false;
               break;
            };
            let Some(m) = part_str(out, pair[1]) else {
               return Err(());
            };
            members[i] = (score, m);
         }
         if !all_valid {
            for pair in parts.chunks_exact(2) {
               let score = crate::utils::resp::fast_parse_f64_bytes(unsafe { part_bytes(pair[0]) });
               match score {
                  Some(score) => {
                     let member =
                        std::str::from_utf8(unsafe { part_bytes(pair[1]) }).map_err(|_| ())?;
                     match conn.store.zadd(
                        key,
                        &[(score, member)],
                        crate::storage::zset::ZAddOptions::default(),
                     ) {
                        Ok(n) => crate::utils::resp::write_integer(&mut conn.parser.wbuf, n as i64),
                        Err(e) => crate::utils::resp::write_err(&mut conn.parser.wbuf, e),
                     }
                  }
                  None => {
                     conn
                        .parser
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
         if let Err(e) = conn.store.zadd_added_flags_into(key, &members[..cmds], out) {
            out.reserve(cmds * 32);
            for _ in 0..cmds {
               crate::utils::resp::write_err(out, e);
            }
         }
         if replication {
            // One capture per run: the batch mutation is already complete
            // above, so every loop iteration recorded the identical final
            // value; K-1 lookups, encodes and appends were pure waste.
            conn.store.record_current_value(key);
         }
      }
      WriteOp::Geoadd => {
         let out = &mut conn.parser.wbuf;
         out.reserve(cmds * 4);
         let mut members: [(f64, &str); QUEUE_RUN_MAX] = [(0.0, ""); QUEUE_RUN_MAX];
         let mut all_valid = true;
         for (i, chunk) in parts.chunks_exact(3).enumerate() {
            let lon_b = unsafe { part_bytes(chunk[0]) };
            let lat_b = unsafe { part_bytes(chunk[1]) };
            let Some(member) = part_str(out, chunk[2]) else {
               return Err(());
            };
            if let (Some(lon), Some(lat)) = (
               crate::utils::resp::fast_parse_f64_bytes(lon_b),
               crate::utils::resp::fast_parse_f64_bytes(lat_b),
            ) {
               if (-180.0..=180.0).contains(&lon) && (-85.05112878..=85.05112878).contains(&lat) {
                  let hash = crate::storage::geo::geohash_encode(lon, lat);
                  members[i] = (f64::from_bits(hash), member);
               } else {
                  all_valid = false;
                  break;
               }
            } else {
               all_valid = false;
               break;
            }
         }
         if all_valid {
            if let Err(e) = conn.store.zadd_added_flags_into(key, &members[..cmds], out) {
               for _ in 0..cmds {
                  crate::utils::resp::write_err(out, e);
               }
            }
         } else {
            for chunk in parts.chunks_exact(3) {
               let lon_b = unsafe { part_bytes(chunk[0]) };
               let lat_b = unsafe { part_bytes(chunk[1]) };
               let Some(member) = part_str(out, chunk[2]) else {
                  return Err(());
               };
               if let (Some(lon), Some(lat)) = (
                  crate::utils::resp::fast_parse_f64_bytes(lon_b),
                  crate::utils::resp::fast_parse_f64_bytes(lat_b),
               ) {
                  if (-180.0..=180.0).contains(&lon) && (-85.05112878..=85.05112878).contains(&lat)
                  {
                     match conn
                        .store
                        .geoadd(key, &[(lon, lat, member)], false, false, false)
                     {
                        Ok(n) => crate::utils::resp::write_integer(out, n as i64),
                        Err(e) => crate::utils::resp::write_store_err(out, e),
                     }
                  } else {
                     out.extend_from_slice(b"-ERR invalid longitude,latitude pair\r\n");
                  }
               } else {
                  out.extend_from_slice(b"-ERR value is not a valid float\r\n");
               }
            }
         }
         if replication {
            // One capture per run: the batch mutation is already complete
            // above, so every loop iteration recorded the identical final
            // value; K-1 lookups, encodes and appends were pure waste.
            conn.store.record_current_value(key);
         }
      }
      WriteOp::Xadd => {
         let out = &mut conn.parser.wbuf;
         out.reserve(cmds * 24);
         let mut entries: [(&str, &str, &str); QUEUE_RUN_MAX] = [("", "", ""); QUEUE_RUN_MAX];
         for (i, chunk) in parts.chunks_exact(3).enumerate() {
            let Some(id_str) = part_str(out, chunk[0]) else {
               return Err(());
            };
            let Some(f) = part_str(out, chunk[1]) else {
               return Err(());
            };
            let Some(v) = part_str(out, chunk[2]) else {
               return Err(());
            };
            entries[i] = (id_str, f, v);
         }
         let res = conn.store.xadd_batch(key, cmds, |s, i| {
            let (id_str, f, v) = entries[i];
            let spec = crate::storage::stream::IdSpec::parse(id_str)
               .ok_or(crate::storage::stream::ERR_INVALID_ID)?;
            let id = s.add(spec, &[(f, v)])?;
            id.write_to_resp(out);
            Ok(())
         });
         if let Err(e) = res {
            crate::utils::resp::write_err(out, e);
         }
         if replication {
            conn.store.record_current_value(key);
         }
      }
      WriteOp::JsonSet => {
         let out = &mut conn.parser.wbuf;
         out.reserve(cmds * 5);
         let last_chunk = &parts[parts.len() - 2..];
         let Some(path) = part_str(out, last_chunk[0]) else {
            return Err(());
         };
         let Some(value) = part_str(out, last_chunk[1]) else {
            return Err(());
         };
         match conn.store.json_set(key, path, value, false, false) {
            Ok(true) => {
               for _ in 0..cmds {
                  crate::utils::resp::write_ok(out);
               }
            }
            Ok(false) => {
               for _ in 0..cmds {
                  crate::utils::resp::write_nil(out);
               }
            }
            Err(e) => {
               for _ in 0..cmds {
                  crate::utils::resp::write_store_err(out, e);
               }
            }
         }
         if replication {
            // One capture per run: the batch mutation is already complete
            // above, so every loop iteration recorded the identical final
            // value; K-1 lookups, encodes and appends were pure waste.
            conn.store.record_current_value(key);
         }
      }
   }
   Ok(())
}

/// A queue command eligible for run coalescing.
#[derive(Clone, Copy, PartialEq)]
pub(super) enum QueueOp {
   PushFront,
   PushBack,
   PopFront,
   PopBack,
}

impl QueueOp {
   #[inline(always)]
   fn parse(cmd: &[u8]) -> Option<QueueOp> {
      match cmd_id(cmd) {
         CmdId::Lpush => Some(QueueOp::PushFront),
         CmdId::Rpush => Some(QueueOp::PushBack),
         _ => None,
      }
   }

   #[inline(always)]
   fn parse_pop(cmd: &[u8]) -> Option<QueueOp> {
      match cmd_id(cmd) {
         CmdId::Lpop => Some(QueueOp::PopFront),
         CmdId::Rpop => Some(QueueOp::PopBack),
         _ => None,
      }
   }

   #[inline(always)]
   fn from_id(id: CmdId) -> Option<QueueOp> {
      match id {
         CmdId::Lpush => Some(QueueOp::PushFront),
         CmdId::Rpush => Some(QueueOp::PushBack),
         CmdId::Lpop => Some(QueueOp::PopFront),
         CmdId::Rpop => Some(QueueOp::PopBack),
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
pub(super) fn is_coalescible_queue(raw: &[(*const u8, usize)], id: CmdId) -> bool {
   match id {
      CmdId::Lpush | CmdId::Rpush => raw.len() == 3,
      CmdId::Lpop | CmdId::Rpop => raw.len() == 2,
      _ => false,
   }
}

pub(super) enum QueueRunOutcome {
   Done,
   ConnError,
}

pub(super) fn dispatch_queue_run(
   conn: &mut Conn<'_>,
   first: &[(*const u8, usize)],
) -> QueueRunOutcome {
   let first_cmd = unsafe { part_bytes(first[0]) };
   let op =
      QueueOp::from_id(cmd_id(first_cmd)).expect("is_coalescible_queue checked the first command");

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
   let mut parts = [(std::ptr::null::<u8>(), 0usize); QUEUE_RUN_MAX];
   parts[0] = first_value;
   let mut cmds = 1usize;
   let tail_len = if op.is_pop() { 0 } else { 1 };

   let mut prefix_buf = [0u8; 192];
   let mut prefix_len = 0;
   let can_fast = key_bytes.len() + first_cmd.len() + 32 <= prefix_buf.len();
   if can_fast {
      let arity_bytes: &[u8] = if op.is_pop() { b"*2\r\n" } else { b"*3\r\n" };
      prefix_buf[..4].copy_from_slice(arity_bytes);
      prefix_len = 4;

      prefix_buf[prefix_len] = b'$';
      prefix_len += 1;
      let mut num_buf = [0u8; 20];
      let num_str = format_usize_digits(first_cmd.len(), &mut num_buf);
      prefix_buf[prefix_len..prefix_len + num_str.len()].copy_from_slice(num_str);
      prefix_len += num_str.len();
      prefix_buf[prefix_len..prefix_len + 2].copy_from_slice(b"\r\n");
      prefix_len += 2;
      prefix_buf[prefix_len..prefix_len + first_cmd.len()].copy_from_slice(first_cmd);
      prefix_len += first_cmd.len();
      prefix_buf[prefix_len..prefix_len + 2].copy_from_slice(b"\r\n");
      prefix_len += 2;

      prefix_buf[prefix_len] = b'$';
      prefix_len += 1;
      let num_str = format_usize_digits(key_bytes.len(), &mut num_buf);
      prefix_buf[prefix_len..prefix_len + num_str.len()].copy_from_slice(num_str);
      prefix_len += num_str.len();
      prefix_buf[prefix_len..prefix_len + 2].copy_from_slice(b"\r\n");
      prefix_len += 2;
      prefix_buf[prefix_len..prefix_len + key_bytes.len()].copy_from_slice(key_bytes);
      prefix_len += key_bytes.len();
      prefix_buf[prefix_len..prefix_len + 2].copy_from_slice(b"\r\n");
      prefix_len += 2;
   }

   let prefix = &prefix_buf[..prefix_len];
   while cmds < QUEUE_RUN_MAX {
      if can_fast {
         let avail = conn.parser.filled - conn.parser.pos;
         if avail >= prefix_len + tail_len * 4 {
            let buf = unsafe { conn.parser.rbuf.as_ptr().add(conn.parser.pos) };
            if unsafe { std::slice::from_raw_parts(buf, prefix_len) } == prefix {
               if let Some(parsed_len) = try_fast_tail(
                  buf,
                  avail,
                  prefix_len,
                  tail_len,
                  &mut parts[cmds..cmds + tail_len.max(1)],
               ) {
                  conn.parser.pos += parsed_len;
                  cmds += 1;
                  continue;
               }
            }
         }
      }

      let next_start = conn.parser.pos;
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
               let tail = if op.is_pop() { &[] } else { &parts[..cmds] };
               let outcome = match execute_queue_run(conn, key, op, cmds, tail) {
                  Ok(()) => QueueRunOutcome::Done,
                  Err(()) => QueueRunOutcome::ConnError,
               };
               if matches!(outcome, QueueRunOutcome::Done) {
                  conn.parser.pos = next_start;
               }
               return outcome;
            }
            if !op.is_pop() {
               parts[cmds] = raw[2];
            }
            cmds += 1;
         }
         ParseResult::Incomplete => break,
         ParseResult::Error => {
            let tail = if op.is_pop() { &[] } else { &parts[..cmds] };
            return match execute_queue_run(conn, key, op, cmds, tail) {
               Ok(()) => QueueRunOutcome::Done,
               Err(()) => QueueRunOutcome::ConnError,
            };
         }
      }
   }

   let tail = if op.is_pop() { &[] } else { &parts[..cmds] };
   match execute_queue_run(conn, key, op, cmds, tail) {
      Ok(()) => QueueRunOutcome::Done,
      Err(()) => QueueRunOutcome::ConnError,
   }
}

/// Run one coalesced batch; Err(()) closes the connection, matching
/// the single-command path on invalid UTF-8.
pub(super) fn execute_queue_run(
   conn: &mut Conn<'_>,
   key: &str,
   op: QueueOp,
   cmds: usize,
   parts: &[(*const u8, usize)],
) -> Result<(), ()> {
   let store = conn.store;
   let _write_epoch = store.data.write_epoch();
   let out = &mut conn.parser.wbuf;
   let store = conn.store;

   match op {
      QueueOp::PushFront | QueueOp::PushBack => {
         const STACK_VALUES: usize = 64;
         if parts.len() <= STACK_VALUES {
            let mut stack_vals = [""; STACK_VALUES];
            for (i, &part) in parts.iter().enumerate() {
               let Some(v) = part_str(out, part) else {
                  return Err(());
               };
               stack_vals[i] = v;
            }
            let values = &stack_vals[..parts.len()];
            let result = if op == QueueOp::PushFront {
               store.lpush(key, values)
            } else {
               store.rpush(key, values)
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
         } else {
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
      }
      QueueOp::PopFront | QueueOp::PopBack => {
         match store.pop_many_to_buf(key, cmds, op == QueueOp::PopBack, out) {
            Ok(n) => {
               for _ in n..cmds {
                  out.extend_from_slice(b"$-1\r\n");
               }
            }
            Err(_) => {
               for _ in 0..cmds {
                  crate::utils::resp::write_wrong_type(out);
               }
            }
         }
      }
   }

   if conn.store.has_replication() {
      // One capture per run (see execute_write_run): the push/pop batch is
      // complete, so per-iteration captures were identical duplicates.
      conn.store.record_current_value(key);
   }
   Ok(())
}
