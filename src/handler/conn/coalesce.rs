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
pub(super) fn is_set_expire_pair(raw: &[(*const u8, usize)]) -> bool {
   raw.len() == 3
      && unsafe { part_bytes(raw[0]) }.len() == 3
      && unsafe { part_bytes(raw[0]) }.eq_ignore_ascii_case(b"SET")
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
   if expire.len() != 3 || !unsafe { part_bytes(expire[0]) }.eq_ignore_ascii_case(b"EXPIRE") {
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
   let Some(exp) = secs_str
      .parse::<u64>()
      .ok()
      .and_then(crate::storage::value::expiry_from_secs)
   else {
      // Invalid TTL: the generic path reports the error identically.
      conn.store.set_string(key, value, 0);
      if conn.store.has_replication() {
         conn.store.record_current_value(key);
      }
      conn.parser.wbuf.extend_from_slice(b"+OK\r\n");
      dispatch_raw(conn, expire);
      return PairOutcome::Done;
   };

   // The pair: one lock, atomic value+TTL.
   conn.store.set_string(key, value, exp);
   if conn.store.has_replication() {
      conn.store.record_current_value(key);
   }
   conn.parser.wbuf.extend_from_slice(b"+OK\r\n:1\r\n");
   PairOutcome::Done
}

/// Is `raw` a `PUBLISH channel message` that could open a batched run?
#[inline(always)]
pub(super) fn is_publish_run(raw: &[(*const u8, usize)]) -> bool {
   raw.len() == 3
      && unsafe { part_bytes(raw[0]) }.len() == 7
      && unsafe { part_bytes(raw[0]) }.eq_ignore_ascii_case(b"PUBLISH")
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
   let mut parts: Vec<(*const u8, usize)> = Vec::new();
   let mut collected_first = false;
   let mut cmds = 1usize;

   while cmds < QUEUE_RUN_MAX {
      match conn.parser.parse_one() {
         ParseResult::Complete => {
            let raw_ptr = conn.parser.parts_raw.as_ptr();
            let raw_len = conn.parser.parts_raw.len();
            let raw = unsafe { std::slice::from_raw_parts(raw_ptr, raw_len) };
            let cmd = unsafe { part_bytes(raw[0]) };
            let same = raw.len() == 3
               && cmd.len() == 7
               && cmd.eq_ignore_ascii_case(b"PUBLISH")
               && unsafe { part_bytes(raw[1]) } == channel_bytes;
            if !same {
               let tail: &[(*const u8, usize)] = if collected_first {
                  &parts
               } else {
                  std::slice::from_ref(&first_msg)
               };
               let outcome = match execute_publish_run(conn, channel, cmds, tail) {
                  Ok(()) => QueueRunOutcome::Done,
                  Err(()) => QueueRunOutcome::ConnError,
               };
               if matches!(outcome, QueueRunOutcome::Done) {
                  dispatch_raw(conn, raw);
               }
               return outcome;
            }
            if !collected_first {
               parts.push(first_msg);
               collected_first = true;
            }
            parts.push(raw[2]);
            cmds += 1;
         }
         ParseResult::Incomplete => break,
         ParseResult::Error => {
            let tail: &[(*const u8, usize)] = if collected_first {
               &parts
            } else {
               std::slice::from_ref(&first_msg)
            };
            return match execute_publish_run(conn, channel, cmds, tail) {
               Ok(()) => QueueRunOutcome::Done,
               Err(()) => QueueRunOutcome::ConnError,
            };
         }
      }
   }

   let tail: &[(*const u8, usize)] = if collected_first {
      &parts
   } else {
      std::slice::from_ref(&first_msg)
   };
   match execute_publish_run(conn, channel, cmds, tail) {
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
   let mut messages: Vec<&str> = Vec::with_capacity(parts.len());
   for &part in parts {
      let Some(m) = part_str(out, part) else {
         return Err(());
      };
      messages.push(m);
   }
   let n = conn.pubsub.publish_batch(channel, &messages);
   for _ in 0..cmds {
      crate::utils::resp::write_integer(out, n as i64);
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
}

impl WriteOp {
   #[inline(always)]
   fn parse(cmd: &[u8]) -> Option<WriteOp> {
      match cmd.len() {
         3 if cmd.eq_ignore_ascii_case(b"SET") => Some(WriteOp::Set),
         4 if cmd.eq_ignore_ascii_case(b"INCR") => Some(WriteOp::Incr),
         4 if cmd.eq_ignore_ascii_case(b"HSET") => Some(WriteOp::Hset),
         4 if cmd.eq_ignore_ascii_case(b"SADD") => Some(WriteOp::Sadd),
         4 if cmd.eq_ignore_ascii_case(b"ZADD") => Some(WriteOp::Zadd),
         5 if cmd.eq_ignore_ascii_case(b"PFADD") => Some(WriteOp::Pfadd),
         _ => None,
      }
   }

   /// Total RESP parts of one command, key included.
   #[inline(always)]
   fn arity(self) -> usize {
      match self {
         WriteOp::Set | WriteOp::Sadd | WriteOp::Pfadd => 3,
         WriteOp::Incr => 2,
         WriteOp::Hset | WriteOp::Zadd => 4,
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

   // first aliases parts_raw (overwritten per parse_one): copy tail values
   // to the stack now; allocate only once a real run forms.
   let tail_len = op.arity() - 2;
   let mut first_tail: [(*const u8, usize); 2] = [(std::ptr::null(), 0); 2];
   for (i, &p) in first[2..].iter().enumerate() {
      first_tail[i] = p;
   }
   let mut parts: Vec<(*const u8, usize)> = Vec::new();
   let mut collected_first = false;
   let mut cmds = 1usize;

   while cmds < QUEUE_RUN_MAX {
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
               // Dispatch the already-consumed trailing command inline, or it drops.
               let tail: &[(*const u8, usize)] = if collected_first {
                  &parts
               } else {
                  &first_tail[..tail_len]
               };
               let outcome = match execute_write_run(conn, key, op, cmds, tail) {
                  Ok(()) => QueueRunOutcome::Done,
                  Err(()) => QueueRunOutcome::ConnError,
               };
               if matches!(outcome, QueueRunOutcome::Done) {
                  dispatch_raw(conn, raw);
               }
               return outcome;
            }
            if !collected_first {
               parts.extend_from_slice(&first_tail[..tail_len]);
               collected_first = true;
            }
            parts.extend_from_slice(&raw[2..]);
            cmds += 1;
         }
         ParseResult::Incomplete => break,
         ParseResult::Error => {
            let tail: &[(*const u8, usize)] = if collected_first {
               &parts
            } else {
               &first_tail[..tail_len]
            };
            return match execute_write_run(conn, key, op, cmds, tail) {
               Ok(()) => QueueRunOutcome::Done,
               Err(()) => QueueRunOutcome::ConnError,
            };
         }
      }
   }

   let tail: &[(*const u8, usize)] = if collected_first {
      &parts
   } else {
      &first_tail[..tail_len]
   };
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
   let replication = conn.store.has_replication();

   match op {
      WriteOp::Set => {
         // Sequential SETs fully overwrite: one call reproduces the final
         // state. Plain SET clears TTL (set_string uses zero TTL).
         let last = parts[parts.len() - 1];
         let out = &mut conn.parser.wbuf;
         let Some(value) = part_str(out, last) else {
            return Err(());
         };
         conn.store.set_string(key, value, 0);
         for _ in 0..cmds {
            conn.parser.wbuf.extend_from_slice(b"+OK\r\n");
         }
         if replication {
            for _ in 0..cmds {
               conn.store.record_current_value(key);
            }
         }
      }
      WriteOp::Incr => {
         // One incrby: replies are cur+1..=cur+K. Overflow needs the sequential
         // fallback (partial application differs from one checked_add).
         match conn.store.incrby(key, cmds as i64) {
            Ok(final_value) => {
               let out = &mut conn.parser.wbuf;
               for i in 1..=cmds as i64 {
                  crate::utils::resp::write_integer(out, final_value - (cmds as i64 - i));
               }
            }
            Err("increment or decrement would overflow") => {
               for _ in 0..cmds {
                  match conn.store.incr(key) {
                     Ok(n) => crate::utils::resp::write_integer(&mut conn.parser.wbuf, n),
                     Err(e) => crate::utils::resp::write_err(&mut conn.parser.wbuf, e),
                  }
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
            for _ in 0..cmds {
               conn.store.record_current_value(key);
            }
         }
      }
      WriteOp::Hset => {
         let mut fields: Vec<(&str, &[u8])> = Vec::with_capacity(parts.len() / 2);
         for pair in parts.chunks(2) {
            let Some(f) = std::str::from_utf8(unsafe { part_bytes(pair[0]) }).ok() else {
               return Err(());
            };
            fields.push((f, unsafe { part_bytes(pair[1]) }));
         }
         match conn.store.hset_bytes_added_flags(key, &fields) {
            Ok(flags) => {
               let out = &mut conn.parser.wbuf;
               for added in flags {
                  crate::utils::resp::write_integer(out, added as i64);
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
            for _ in 0..cmds {
               conn.store.record_current_value(key);
            }
         }
      }
      WriteOp::Sadd => {
         let out = &mut conn.parser.wbuf;
         let mut members: Vec<&str> = Vec::with_capacity(parts.len());
         for &part in parts {
            let Some(m) = part_str(out, part) else {
               return Err(());
            };
            members.push(m);
         }
         match conn.store.sadd_added_flags(key, &members) {
            Ok(flags) => {
               let out = &mut conn.parser.wbuf;
               for added in flags {
                  crate::utils::resp::write_integer(out, added as i64);
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
            for _ in 0..cmds {
               conn.store.record_current_value(key);
            }
         }
      }
      WriteOp::Pfadd => {
         let out = &mut conn.parser.wbuf;
         let mut elements: Vec<&str> = Vec::with_capacity(parts.len());
         for &part in parts {
            let Some(m) = part_str(out, part) else {
               return Err(());
            };
            elements.push(m);
         }
         match conn.store.pfadd_run(key, &elements) {
            Ok(flags) => {
               let out = &mut conn.parser.wbuf;
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
            for _ in 0..cmds {
               conn.store.record_current_value(key);
            }
         }
      }
      WriteOp::Zadd => {
         // A bad score fails only its command: sequential fallback keeps the
         // reply stream identical.
         let out = &mut conn.parser.wbuf;
         let mut members: Vec<(f64, &str)> = Vec::with_capacity(parts.len() / 2);
         let mut all_valid = true;
         for pair in parts.chunks(2) {
            let Some(s) = part_str(out, pair[0]) else {
               return Err(());
            };
            let Some(m) = part_str(out, pair[1]) else {
               return Err(());
            };
            match s.parse::<f64>() {
               Ok(score) => members.push((score, m)),
               Err(_) => {
                  all_valid = false;
                  break;
               }
            }
         }
         if !all_valid {
            for pair in parts.chunks(2) {
               let score = std::str::from_utf8(unsafe { part_bytes(pair[0]) })
                  .ok()
                  .and_then(|s| s.parse::<f64>().ok());
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
         match conn.store.zadd_added_flags(key, &members) {
            Ok(flags) => {
               let out = &mut conn.parser.wbuf;
               for added in flags {
                  crate::utils::resp::write_integer(out, added as i64);
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
            for _ in 0..cmds {
               conn.store.record_current_value(key);
            }
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
      if cmd.len() != 5 {
         return None;
      }
      match cmd {
         b"LPUSH" => Some(QueueOp::PushFront),
         b"RPUSH" => Some(QueueOp::PushBack),
         _ => None,
      }
   }

   #[inline(always)]
   fn parse_pop(cmd: &[u8]) -> Option<QueueOp> {
      if cmd.len() != 4 {
         return None;
      }
      match cmd {
         b"LPOP" => Some(QueueOp::PopFront),
         b"RPOP" => Some(QueueOp::PopBack),
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
pub(super) fn is_coalescible_queue(raw: &[(*const u8, usize)]) -> bool {
   if raw.len() != 3 && raw.len() != 2 {
      return false;
   }
   let cmd = unsafe { part_bytes(raw[0]) };
   if let Some(op) = QueueOp::parse(cmd) {
      return !op.is_pop() && raw.len() == 3;
   }
   QueueOp::parse_pop(cmd).is_some() && raw.len() == 2
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
   let op = QueueOp::parse(first_cmd)
      .or_else(|| QueueOp::parse_pop(first_cmd))
      .expect("is_coalescible_queue checked the first command");

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
   let mut parts: Vec<(*const u8, usize)> = Vec::new();
   let mut collected_first = false;
   let mut cmds = 1usize;

   while cmds < QUEUE_RUN_MAX {
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
               let tail: &[(*const u8, usize)] = if collected_first {
                  &parts
               } else if op.is_pop() {
                  &[]
               } else {
                  std::slice::from_ref(&first_value)
               };
               let outcome = match execute_queue_run(conn, key, op, cmds, tail) {
                  Ok(()) => QueueRunOutcome::Done,
                  Err(()) => QueueRunOutcome::ConnError,
               };
               if matches!(outcome, QueueRunOutcome::Done) {
                  dispatch_raw(conn, raw);
               }
               return outcome;
            }
            if !op.is_pop() {
               if !collected_first {
                  parts.push(first_value);
                  collected_first = true;
               }
               parts.push(raw[2]);
            }
            cmds += 1;
         }
         ParseResult::Incomplete => break,
         ParseResult::Error => {
            let tail: &[(*const u8, usize)] = if collected_first {
               &parts
            } else if op.is_pop() {
               &[]
            } else {
               std::slice::from_ref(&first_value)
            };
            return match execute_queue_run(conn, key, op, cmds, tail) {
               Ok(()) => QueueRunOutcome::Done,
               Err(()) => QueueRunOutcome::ConnError,
            };
         }
      }
   }

   let tail: &[(*const u8, usize)] = if collected_first {
      &parts
   } else if op.is_pop() {
      &[]
   } else {
      std::slice::from_ref(&first_value)
   };
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
      for _ in 0..cmds {
         conn.store.record_current_value(key);
      }
   }
   Ok(())
}
